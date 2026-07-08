#!/usr/bin/env python3
"""
Optimized batched standardisation UPDATEs/INSERTs for: rgd_udm_silver.allergies

Change TARGET_TABLE at the top to run against any allergies table.

Four sequential passes — each with checkpoint/resume:

  Pass 1 — WHERE allergen_code IS NOT NULL AND allergen_code <> '':
    SET allergen_name_std, allergen_code_std, allergen_coding_system_std
    LEFT JOIN pre-materialized snomed_max, rx_max, FDB NDC + RxNorm lookups

  Pass 2 — WHERE allergy_reaction_code IS NOT NULL AND allergy_reaction_code <> '':
    SET allergy_reaction_name_std, allergy_reaction_code_std, allergy_reaction_coding_system_std
    LEFT JOIN pre-materialized snomed_max on allergy_reaction_code = conceptId
    NULL (not 'NS') if no match — code lookup only

  Pass 3 — WHERE (allergy_reaction_name_std IS NULL OR allergy_reaction_name_std = 'NS')
              AND allergy_reaction_name IS NOT NULL AND allergy_reaction_name <> '':
    SET same 3 reaction std columns (COALESCE to 'NS' if no match)
    LEFT JOIN semantics.snomed ON LOWER(s.term) LIKE LOWER(a.allergy_reaction_name)

  Pass 4 — WHERE allergy_reaction_code LIKE '%,%' AND allergy_reaction_code IS NOT NULL
              AND allergy_reaction_code <> '':
    INSERT new rows for each comma-separated SNOMED code using pre-built reaction_map staging.
    Original rows are KEPT as-is. New rows get udm_inc_id = 0 (DEFAULT).
    PK staging rebuilt AFTER Pass 3 (depends on allergy_reaction_name_std being set).

    WARNING: If Pass 4 fails mid-run, partially inserted rows must be manually
    deleted before re-running. See cleanup SQL at the end.
    Delete: DELETE FROM {TARGET_TABLE}
            WHERE allergy_reaction_coding_system_std IN ('SNOMED','NS')
            AND udm_inc_id = 0;

Pre-materialized lookup tables (computed ONCE, reused across runs):
  - staging.allergy_std_snomed_{suffix}        (snomed_max CTE — indexed on conceptId)
  - staging.allergy_std_rx_max_{suffix}        (rx_max CTE — indexed on EVD_RXN_RXCUI)
  - staging.allergy_std_reaction_map_{suffix}  (JSON_TABLE comma-split reaction codes -> SNOMED)

Std columns added to target table if not present (with metadata lock guard).

Optimizations applied:
- All lookup tables pre-materialized once (not re-scanned per batch)
- Per-pass PK staging tables (filtered to eligible rows only)
- Pass 4 PK staging rebuilt after Pass 3 (depends on allergy_reaction_name_std output)
- Server-side boundary sampling (avoids loading all PKs into memory)
- Commit after every batch (frees undo/log space)
- Checkpoint/resume per pass — re-run skips completed passes
- Disabled InnoDB checks per-session for bulk update speed
- Progress bar via tqdm

Usage:
    python allergies_stand_opt.py
"""

import sys
import time
from datetime import datetime
import pymysql
from tqdm import tqdm
import os
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())

# ── Configuration ─────────────────────────────────────────────────────
DB_CONFIG = {
    "host":            os.environ.get("DB_INTERNAL_HOST"),
    "port":            3306,
    "user":            os.environ.get("DB_INTERNAL_USER"),
    "password":        os.environ.get("DB_INTERNAL_PASSWORD"),
    "database":        "rgd_udm_silver",
    "charset":         "utf8mb4",
    "connect_timeout": 30,
    "read_timeout":    21600,
    "write_timeout":   21600,
}

BATCH_SIZE = 50_000

# ── Change this to run against a different allergies table ────────────
TARGET_TABLE = "kinsula_leq.allergies"

# ─────────────────────────────────────────────────────────────────────
# Derive a short suffix from the table name for unique staging/checkpoint
# names — allows multiple tables to run without collision.
_TABLE_SUFFIX = TARGET_TABLE.replace(".", "_").replace("-", "_")

# Shared lookup tables (not dropped between runs — reused across all allergies tables)
STAGING_SNOMED        = f"staging.allergy_std_snomed_{_TABLE_SUFFIX}"        # snomed_max CTE (conceptId + latest_snomed_id)
STAGING_SNOMED_TERMS  = f"staging.allergy_std_snomed_terms_{_TABLE_SUFFIX}"  # snomed_max + term + term_lower (full lookup)
STAGING_RX_MAX        = f"staging.allergy_std_rx_max_{_TABLE_SUFFIX}"        # rx_max CTE
STAGING_ALLERGEN_KEYS = f"staging.allergy_std_keys_{_TABLE_SUFFIX}"          # pre-computed TRIM/REPLACE/LOWER per row

# Per-run PK staging tables
STAGING_PK_PASS1 = f"staging.allergy_std_pk1_{_TABLE_SUFFIX}"
STAGING_PK_PASS2 = f"staging.allergy_std_pk2_{_TABLE_SUFFIX}"
STAGING_PK_PASS3 = f"staging.allergy_std_pk3_{_TABLE_SUFFIX}"

CHECKPOINT_TABLE = f"staging.etl_checkpoint_allergy_std_f_{_TABLE_SUFFIX}"
CHECKPOINT_PASS1 = f"allergies.std.pass1.allergen_code_lookup.{_TABLE_SUFFIX}"
CHECKPOINT_PASS2 = f"allergies.std.pass2.reaction_code_lookup.{_TABLE_SUFFIX}"
CHECKPOINT_PASS3 = f"allergies.std.pass3.reaction_name_fallback.{_TABLE_SUFFIX}"

BATCH_KEY = "udm_inc_id"


# ── Helpers ───────────────────────────────────────────────────────────

def get_connection():
    return pymysql.connect(**DB_CONFIG)


def _table_exists(cur, full_table_name):
    schema, table = full_table_name.split(".")
    cur.execute(
        "SELECT COUNT(*) FROM information_schema.tables "
        "WHERE table_schema = %s AND table_name = %s",
        (schema, table),
    )
    return cur.fetchone()[0] > 0


def _col_exists(cur, full_table_name, col_name):
    schema, table = full_table_name.split(".")
    cur.execute(
        "SELECT COUNT(*) FROM information_schema.columns "
        "WHERE table_schema = %s AND table_name = %s AND column_name = %s",
        (schema, table, col_name),
    )
    return cur.fetchone()[0] > 0


def _safe_add_index(cur, conn, table, col, idx_name, prefix=50):
    """
    Add an index on `col` for `table`.
    - TEXT/BLOB columns require a prefix → try (prefix) first.
    - If Error 1089 (prefix > VARCHAR column length), fall back to no prefix.
    - Silently skips if the index already exists (Error 1061).
    """
    import pymysql
    for i, key_spec in enumerate((f"`{col}`({prefix})", f"`{col}`")):
        try:
            cur.execute(f"ALTER TABLE {table} ADD INDEX `{idx_name}` ({key_spec})")
            conn.commit()
            return
        except pymysql.err.OperationalError as e:
            code = e.args[0]
            if code == 1061:   # duplicate key name — already exists
                return
            if code == 1089 and i == 0:  # prefix > column length → retry without prefix
                continue
            raise              # unexpected error, or 1170 on no-prefix attempt → propagate


def _build_ranges(cur, staging_pk):
    cur.execute(f"SELECT COUNT(*) FROM {staging_pk}")
    total = cur.fetchone()[0]
    if total == 0:
        return [], 0

    cur.execute(f"""
        SELECT {BATCH_KEY}
        FROM (
            SELECT {BATCH_KEY},
                   ROW_NUMBER() OVER (ORDER BY {BATCH_KEY}) AS rn
            FROM {staging_pk}
        ) t
        WHERE (rn - 1) % {BATCH_SIZE} = 0
        ORDER BY {BATCH_KEY}
    """)
    boundaries = [row[0] for row in cur.fetchall() if row[0] is not None]

    cur.execute(f"SELECT MAX({BATCH_KEY}) FROM {staging_pk}")
    max_pk = int(cur.fetchone()[0])

    ranges = []
    for i, lo in enumerate(boundaries):
        hi = boundaries[i + 1] if i + 1 < len(boundaries) else max_pk + 1
        ranges.append((lo, hi))

    return ranges, total


# ── Batch UPDATE/INSERT builders ──────────────────────────────────────

def build_pass1(pk_lo, pk_hi):
    """
    Pass 1: UPDATE allergen_name_std, allergen_code_std, allergen_coding_system_std.

    Uses pre-computed keys (no TRIM/REPLACE at query time):
      ak.allergen_code_trimmed → STAGING_SNOMED_TERMS (SNOMED)
      ak.allergen_code_ndc     → FDB.RNDC14_NDC_MSTR  (NDC)
      ak.allergen_code_trimmed → STAGING_RX_MAX → REVDCS0 → REVDCD0 (RxNorm)
    All joins are direct equality on indexed columns.
    """
    return f"""
UPDATE {TARGET_TABLE} a
JOIN {STAGING_ALLERGEN_KEYS} ak
    ON a.{BATCH_KEY} = ak.{BATCH_KEY}
LEFT JOIN {STAGING_SNOMED_TERMS} st
    ON ak.allergen_code_trimmed = st.conceptId
LEFT JOIN FDB.RNDC14_NDC_MSTR ndc
    ON ak.allergen_code_ndc = ndc.NDC
LEFT JOIN {STAGING_RX_MAX} rxm
    ON rxm.EVD_RXN_RXCUI = ak.allergen_code_trimmed
LEFT JOIN FDB.REVDCS0_RXN_CONCEPT_SOURCE rx
    ON rx.EVD_RXN_RXCUI = ak.allergen_code_trimmed
    AND rx.EVD_RXN_CONCEPT_SOURCE_KEY = rxm.latest_rx_id
LEFT JOIN FDB.REVDCD0_RXN_CONCEPT_DESC rxdesc
    ON rx.EVD_RXN_CONCEPT_SOURCE_KEY = rxdesc.EVD_RXN_CONCEPT_SOURCE_KEY
SET
    a.allergen_name_std          = COALESCE(rxdesc.EVD_RXN_STR, ndc.BN, st.term, 'NS'),
    a.allergen_code_std          = COALESCE(rx.EVD_RXN_RXCUI, ndc.NDC, st.conceptId, 'NS'),
    a.allergen_coding_system_std = CASE
        WHEN rx.EVD_RXN_RXCUI IS NOT NULL AND ndc.NDC IS NULL AND st.conceptId IS NULL THEN 'RXNORM'
        WHEN ndc.NDC IS NOT NULL AND rx.EVD_RXN_RXCUI IS NULL AND st.conceptId IS NULL THEN 'NDC'
        WHEN st.conceptId IS NOT NULL AND rx.EVD_RXN_RXCUI IS NULL AND ndc.NDC IS NULL THEN 'SNOMED'
        ELSE 'NS'
    END
WHERE a.allergen_code IS NOT NULL
  AND a.allergen_code <> ''
  AND a.{BATCH_KEY} >= {pk_lo}
  AND a.{BATCH_KEY} <  {pk_hi}
"""


def build_pass2(pk_lo, pk_hi):
    """
    Pass 2: UPDATE allergy_reaction_name_std, allergy_reaction_code_std,
    allergy_reaction_coding_system_std via SNOMED lookup on reaction code.
    Sets NULL (not 'NS') if no match — Pass 3 will catch those rows.

    Uses ak.reaction_code_trimmed → STAGING_SNOMED_TERMS (direct equality, no TRIM at query time).
    """
    return f"""
UPDATE {TARGET_TABLE} a
JOIN {STAGING_ALLERGEN_KEYS} ak
    ON a.{BATCH_KEY} = ak.{BATCH_KEY}
LEFT JOIN {STAGING_SNOMED_TERMS} st
    ON ak.reaction_code_trimmed = st.conceptId
SET
    a.allergy_reaction_name_std          = st.term,
    a.allergy_reaction_code_std          = st.conceptId,
    a.allergy_reaction_coding_system_std = CASE
        WHEN st.conceptId IS NOT NULL THEN 'SNOMED'
        ELSE NULL
    END
WHERE a.allergy_reaction_code IS NOT NULL
  AND a.allergy_reaction_code <> ''
  AND a.{BATCH_KEY} >= {pk_lo}
  AND a.{BATCH_KEY} <  {pk_hi}
"""


def build_pass3(pk_lo, pk_hi):
    """
    Pass 3: UPDATE reaction std columns using name match as fallback.
    Only runs on rows where Pass 2 did not set a reaction standard (NULL or 'NS').
    COALESCE to 'NS' if no name match found.

    Uses ak.reaction_name_lower = st.term_lower — direct indexed equality join,
    no LOWER()/LIKE at query time.
    """
    return f"""
UPDATE {TARGET_TABLE} a
JOIN {STAGING_ALLERGEN_KEYS} ak
    ON a.{BATCH_KEY} = ak.{BATCH_KEY}
LEFT JOIN {STAGING_SNOMED_TERMS} st
    ON ak.reaction_name_lower = st.term_lower
SET
    a.allergy_reaction_name_std          = COALESCE(st.term, 'NS'),
    a.allergy_reaction_code_std          = COALESCE(st.conceptId, 'NS'),
    a.allergy_reaction_coding_system_std = CASE
        WHEN st.conceptId IS NOT NULL THEN 'SNOMED'
        ELSE 'NS'
    END
WHERE (a.allergy_reaction_name_std IS NULL OR a.allergy_reaction_name_std = 'NS')
  AND a.allergy_reaction_name IS NOT NULL
  AND a.allergy_reaction_name <> ''
  AND a.{BATCH_KEY} >= {pk_lo}
  AND a.{BATCH_KEY} <  {pk_hi}
"""


# ── Checkpoint ─────────────────────────────────────────────────────────

def is_done(conn, checkpoint_key):
    cur = conn.cursor()
    cur.execute(
        f"SELECT status FROM {CHECKPOINT_TABLE} WHERE source_key = %s",
        (checkpoint_key,),
    )
    row = cur.fetchone()
    cur.close()
    return row is not None and row[0] == "done"


def mark(conn, checkpoint_key, status, rows=0, error=None):
    cur = conn.cursor()
    cur.execute(f"""
        INSERT INTO {CHECKPOINT_TABLE}
            (source_key, status, rows_affected, started_at, completed_at, error_msg)
        VALUES (%s, %s, %s, NOW(), IF(%s = 'done', NOW(), NULL), %s)
        ON DUPLICATE KEY UPDATE
            status        = VALUES(status),
            rows_affected = VALUES(rows_affected),
            completed_at  = IF(VALUES(status) = 'done', NOW(), NULL),
            error_msg     = VALUES(error_msg)
    """, (checkpoint_key, status, rows, status, error))
    conn.commit()
    cur.close()


# ── DDL: ensure std columns exist ─────────────────────────────────────

def ensure_std_columns():
    std_cols = [
        ("allergen_name_std",                  "VARCHAR(500)"),
        ("allergen_code_std",                  "VARCHAR(500)"),
        ("allergen_coding_system_std",         "VARCHAR(500)"),
        ("allergy_reaction_name_std",          "VARCHAR(500)"),
        ("allergy_reaction_code_std",          "VARCHAR(500)"),
        ("allergy_reaction_coding_system_std", "VARCHAR(500)"),
    ]
    print(f"  Checking std columns on {TARGET_TABLE}...")
    ddl_conn  = get_connection()
    ddl_cur   = ddl_conn.cursor()
    ddl_cur.execute("SET lock_wait_timeout = 15")
    ddl_error = None
    added = []
    try:
        for col_name, col_type in std_cols:
            if not _col_exists(ddl_cur, TARGET_TABLE, col_name):
                print(f"    adding: {col_name} {col_type} ...")
                ddl_cur.execute(
                    f"ALTER TABLE {TARGET_TABLE} ADD COLUMN {col_name} {col_type} DEFAULT NULL"
                )
                ddl_conn.commit()
                added.append(col_name)
                print(f"    added: {col_name}")
            else:
                print(f"    exists: {col_name}")
    except Exception as exc:
        ddl_error = exc
        try:
            ddl_conn.rollback()
        except Exception:
            pass
    finally:
        try:
            ddl_cur.close()
        except Exception:
            pass
        try:
            ddl_conn.close()
        except Exception:
            pass

    if ddl_error:
        print(f"\n  ERROR: Could not add column — metadata lock on {TARGET_TABLE}.")
        print(f"  Find the blocker:")
        print(f"    SELECT id, user, state, info FROM information_schema.processlist")
        print(f"    WHERE state LIKE '%lock%' OR state LIKE '%wait%' ORDER BY time DESC;")
        print(f"  Then: KILL <id>;")
        print(f"\n  Original error: {ddl_error}")
        sys.exit(1)

    if added:
        print(f"    Columns added: {', '.join(added)}")
    else:
        print(f"    All std columns already present.")


# ── Index setup ────────────────────────────────────────────────────────

def _index_exists(cur, schema, table, column):
    cur.execute(
        "SELECT COUNT(*) FROM information_schema.statistics "
        "WHERE table_schema = %s AND table_name = %s "
        "  AND column_name = %s AND seq_in_index = 1",
        (schema, table, column),
    )
    return cur.fetchone()[0] > 0


def ensure_indexes(cur, conn):
    """
    Ensure all join/filter/batch columns on source and target tables are indexed.
    Skips silently if already present.
    """
    # (schema, table, column)
    needed = [
        # target table — batch key + join keys
        ("kinsula_leq",  "allergies",                   "udm_inc_id"),
        ("kinsula_leq",  "allergies",                   "allergen_code"),
        ("kinsula_leq",  "allergies",                   "allergy_reaction_code"),
        ("kinsula_leq",  "allergies",                   "allergy_reaction_name"),
        ("kinsula_leq",  "allergies",                   "allergy_reaction_name_std"),
        # FDB lookup tables
        ("FDB",          "RNDC14_NDC_MSTR",             "NDC"),
        ("FDB",          "REVDCS0_RXN_CONCEPT_SOURCE",  "EVD_RXN_RXCUI"),
        ("FDB",          "REVDCS0_RXN_CONCEPT_SOURCE",  "EVD_RXN_CONCEPT_SOURCE_KEY"),
        ("FDB",          "REVDCD0_RXN_CONCEPT_DESC",    "EVD_RXN_CONCEPT_SOURCE_KEY"),
        # semantics.snomed — join key
        ("semantics",    "snomed",                      "conceptId"),
    ]

    # Override schema for target if TARGET_TABLE differs from default
    _tgt_schema, _tgt_table = TARGET_TABLE.split(".", 1)
    adjusted = []
    for schema, table, col in needed:
        if schema == "kinsula_leq" and table == "allergies":
            adjusted.append((_tgt_schema, _tgt_table, col))
        else:
            adjusted.append((schema, table, col))

    print("  Checking source/target table indexes...")
    created = []
    for schema, table, col in adjusted:
        if not _index_exists(cur, schema, table, col):
            idx_name = f"idx_allergy_std_{col.lower()}"
            print(f"    creating index on {schema}.{table}({col})...")
            try:
                _safe_add_index(cur, conn, f"{schema}.{table}", col, idx_name)
                created.append(f"{table}.{col}")
            except Exception as e:
                print(f"    WARNING: could not create index on {schema}.{table}({col}): {e}")

    if created:
        print(f"    created {len(created)} index(es): {', '.join(created)}")
    else:
        print("    all indexes already present")


# ── Setup ──────────────────────────────────────────────────────────────

def setup_tables():
    ensure_std_columns()

    conn = get_connection()
    cur  = conn.cursor()

    # ── 0. Ensure indexes on all join/filter/batch columns ────────────
    ensure_indexes(cur, conn)

    # ── 1. snomed_max lookup (semantics.snomed — latest active per conceptId) ──
    print("  Materializing snomed_max lookup...")
    if not _table_exists(cur, STAGING_SNOMED):
        cur.execute(f"""
            CREATE TABLE {STAGING_SNOMED} AS
            SELECT
                conceptId,
                MAX(id) AS latest_snomed_id
            FROM semantics.snomed
            WHERE active = 1
            GROUP BY conceptId
        """)
        _safe_add_index(cur, conn, STAGING_SNOMED, "conceptId", "idx_conceptid")
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")
    cur.execute(f"SELECT COUNT(*) FROM {STAGING_SNOMED}")
    print(f"    {cur.fetchone()[0]:,} rows")

    # ── 2. rx_max lookup (FDB.REVDCS0_RXN_CONCEPT_SOURCE — latest per RXCUI) ──
    print("  Materializing rx_max lookup...")
    if not _table_exists(cur, STAGING_RX_MAX):
        cur.execute(f"""
            CREATE TABLE {STAGING_RX_MAX} AS
            SELECT
                EVD_RXN_RXCUI,
                MAX(EVD_RXN_CONCEPT_SOURCE_KEY) AS latest_rx_id
            FROM FDB.REVDCS0_RXN_CONCEPT_SOURCE
            GROUP BY EVD_RXN_RXCUI
        """)
        _safe_add_index(cur, conn, STAGING_RX_MAX, "EVD_RXN_RXCUI", "idx_rxcui")
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")
    cur.execute(f"SELECT COUNT(*) FROM {STAGING_RX_MAX}")
    print(f"    {cur.fetchone()[0]:,} rows")

    # ── 3. snomed_terms lookup (snomed_max + term + term_lower — full lookup table) ──
    #    Eliminates second JOIN back to semantics.snomed in all batch queries.
    #    Depends on STAGING_SNOMED already being created in step 1.
    print("  Materializing snomed_terms lookup (with term + term_lower)...")
    if not _table_exists(cur, STAGING_SNOMED_TERMS):
        cur.execute(f"""
            CREATE TABLE {STAGING_SNOMED_TERMS} AS
            SELECT
                sm.conceptId,
                sm.latest_snomed_id,
                s.term,
                LOWER(s.term) AS term_lower
            FROM {STAGING_SNOMED} sm
            JOIN semantics.snomed s
                ON sm.conceptId = s.conceptId AND s.id = sm.latest_snomed_id
        """)
        _safe_add_index(cur, conn, STAGING_SNOMED_TERMS, "conceptId",  "idx_conceptid")
        _safe_add_index(cur, conn, STAGING_SNOMED_TERMS, "term_lower", "idx_term_lower")
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")
    cur.execute(f"SELECT COUNT(*) FROM {STAGING_SNOMED_TERMS}")
    print(f"    {cur.fetchone()[0]:,} rows")

    # ── 4. Allergen keys lookup (pre-computed TRIM/REPLACE/LOWER per row) ────────
    #    Covers all three passes — computed once, avoids function calls in UPDATE JOINs.
    print("  Materializing allergen keys (pre-computed TRIM/REPLACE/LOWER)...")
    if not _table_exists(cur, STAGING_ALLERGEN_KEYS):
        cur.execute(f"""
            CREATE TABLE {STAGING_ALLERGEN_KEYS} AS
            SELECT
                {BATCH_KEY},
                TRIM(allergen_code)                    AS allergen_code_trimmed,
                REPLACE(TRIM(allergen_code), '_', '')  AS allergen_code_ndc,
                TRIM(allergy_reaction_code)            AS reaction_code_trimmed,
                LOWER(TRIM(allergy_reaction_name))     AS reaction_name_lower
            FROM {TARGET_TABLE}
            WHERE {BATCH_KEY} IS NOT NULL
        """)
        cur.execute(f"ALTER TABLE {STAGING_ALLERGEN_KEYS} ADD INDEX idx_pk ({BATCH_KEY})")
        _safe_add_index(cur, conn, STAGING_ALLERGEN_KEYS, "allergen_code_trimmed", "idx_allergen_trimmed")
        _safe_add_index(cur, conn, STAGING_ALLERGEN_KEYS, "allergen_code_ndc",     "idx_allergen_ndc")
        _safe_add_index(cur, conn, STAGING_ALLERGEN_KEYS, "reaction_code_trimmed", "idx_reaction_code")
        _safe_add_index(cur, conn, STAGING_ALLERGEN_KEYS, "reaction_name_lower",   "idx_reaction_name")
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")
    cur.execute(f"SELECT COUNT(*) FROM {STAGING_ALLERGEN_KEYS}")
    print(f"    {cur.fetchone()[0]:,} rows")

    # ── 6. PK staging — Pass 1: allergen_code IS NOT NULL AND <> '' ────────────
    print("  Creating PK staging for Pass 1 (allergen_code rows)...")
    if not _table_exists(cur, STAGING_PK_PASS1):
        cur.execute(f"""
            CREATE TABLE {STAGING_PK_PASS1} AS
            SELECT {BATCH_KEY}
            FROM {TARGET_TABLE}
            WHERE allergen_code IS NOT NULL
              AND allergen_code <> ''
              AND {BATCH_KEY} IS NOT NULL
        """)
        cur.execute(f"ALTER TABLE {STAGING_PK_PASS1} ADD INDEX idx_pk ({BATCH_KEY})")
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")
    ranges_p1, total_p1 = _build_ranges(cur, STAGING_PK_PASS1)
    print(f"    {total_p1:,} rows -> {len(ranges_p1)} batches")

    # ── 7. PK staging — Pass 2: allergy_reaction_code IS NOT NULL AND <> '' ───
    print("  Creating PK staging for Pass 2 (allergy_reaction_code rows)...")
    if not _table_exists(cur, STAGING_PK_PASS2):
        cur.execute(f"""
            CREATE TABLE {STAGING_PK_PASS2} AS
            SELECT {BATCH_KEY}
            FROM {TARGET_TABLE}
            WHERE allergy_reaction_code IS NOT NULL
              AND allergy_reaction_code <> ''
              AND {BATCH_KEY} IS NOT NULL
        """)
        cur.execute(f"ALTER TABLE {STAGING_PK_PASS2} ADD INDEX idx_pk ({BATCH_KEY})")
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")
    ranges_p2, total_p2 = _build_ranges(cur, STAGING_PK_PASS2)
    print(f"    {total_p2:,} rows -> {len(ranges_p2)} batches")

    # ── 8. PK staging — Pass 3: reaction name fallback rows ──────────────────
    #    (allergy_reaction_name_std IS NULL OR = 'NS') AND allergy_reaction_name present
    print("  Creating PK staging for Pass 3 (reaction name fallback rows)...")
    if not _table_exists(cur, STAGING_PK_PASS3):
        cur.execute(f"""
            CREATE TABLE {STAGING_PK_PASS3} AS
            SELECT {BATCH_KEY}
            FROM {TARGET_TABLE}
            WHERE (allergy_reaction_name_std IS NULL OR allergy_reaction_name_std = 'NS')
              AND allergy_reaction_name IS NOT NULL
              AND allergy_reaction_name <> ''
              AND {BATCH_KEY} IS NOT NULL
        """)
        cur.execute(f"ALTER TABLE {STAGING_PK_PASS3} ADD INDEX idx_pk ({BATCH_KEY})")
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")
    ranges_p3, total_p3 = _build_ranges(cur, STAGING_PK_PASS3)
    print(f"    {total_p3:,} rows -> {len(ranges_p3)} batches")

    # ── 9. Checkpoint table ────────────────────────────────────────────────────
    print("  Creating checkpoint table...")
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {CHECKPOINT_TABLE} (
            source_key    VARCHAR(200) NOT NULL PRIMARY KEY,
            status        ENUM('running','done','failed') NOT NULL DEFAULT 'running',
            rows_affected BIGINT      DEFAULT 0,
            started_at    DATETIME    DEFAULT NULL,
            completed_at  DATETIME    DEFAULT NULL,
            error_msg     TEXT        DEFAULT NULL
        )
    """)
    conn.commit()
    print("    ready")

    cur.close()
    conn.close()

    return {
        CHECKPOINT_PASS1: ranges_p1,
        CHECKPOINT_PASS2: ranges_p2,
        CHECKPOINT_PASS3: ranges_p3,
    }


# ── Runner ─────────────────────────────────────────────────────────────

def run_pass(checkpoint_key, build_fn, ranges, pbar):
    conn = get_connection()

    if is_done(conn, checkpoint_key):
        conn.close()
        pbar.update(len(ranges))
        return {"status": "skipped", "rows": 0, "secs": 0}

    mark(conn, checkpoint_key, "running")
    t0 = time.time()
    total_rows = 0

    try:
        cur = conn.cursor()
        cur.execute("SET unique_checks = 0")
        cur.execute("SET foreign_key_checks = 0")

        for pk_lo, pk_hi in ranges:
            sql = build_fn(pk_lo, pk_hi)
            cur.execute(sql)
            conn.commit()
            total_rows += cur.rowcount
            pbar.update(1)

        cur.execute("SET unique_checks = 1")
        cur.execute("SET foreign_key_checks = 1")
        cur.close()

        elapsed = round(time.time() - t0, 1)
        mark(conn, checkpoint_key, "done", total_rows)
        conn.close()
        return {"status": "done", "rows": total_rows, "secs": elapsed}

    except Exception as exc:
        elapsed = round(time.time() - t0, 1)
        mark(conn, checkpoint_key, "failed", total_rows, str(exc))
        try:
            conn.close()
        except Exception:
            pass
        return {"status": f"FAILED: {exc}", "rows": total_rows, "secs": elapsed}


# ── Main ───────────────────────────────────────────────────────────────

def main():
    print(f"\n{'='*70}", flush=True)
    print(f"  Allergies Standardisation UPDATE/INSERT — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  target     : {TARGET_TABLE}")
    print(f"  batch_key  : {BATCH_KEY}")
    print(f"  batch_size : {BATCH_SIZE:,}")
    print(f"  passes     : 3  (allergen code lookup | reaction code lookup | reaction name fallback)")
    print(f"{'='*70}\n", flush=True)

    print("  Connecting to database...")
    sys.stdout.flush()
    all_ranges = setup_tables()

    results    = {}
    any_failed = False

    all_passes = [
        (CHECKPOINT_PASS1, "Pass 1 — allergen code lookup UPDATE (allergen_code rows)",        build_pass1),
        (CHECKPOINT_PASS2, "Pass 2 — reaction code lookup UPDATE (allergy_reaction_code rows)", build_pass2),
        (CHECKPOINT_PASS3, "Pass 3 — reaction name fallback UPDATE (NULL/NS reaction rows)",   build_pass3),
    ]

    total_batches = sum(len(all_ranges.get(ck, [])) for ck, _, _ in all_passes)

    with tqdm(total=total_batches, desc="Passes 1-3", unit="batch") as pbar:
        for ck, label, build_fn in all_passes:
            ranges = all_ranges.get(ck, [])
            if not ranges:
                print(f"\n  [SKIP] {label} — no eligible rows")
                continue

            print(f"\n  Starting {label} ({len(ranges)} batches)...")
            result = run_pass(ck, build_fn, ranges, pbar)
            results[ck] = result

            if result["status"].startswith("FAILED"):
                print(f"\n  FAILED at {label}: {result['status']}")
                print("  Aborting remaining passes.")
                any_failed = True
                break

    # ── Summary ───────────────────────────────────────────────────────
    print(f"\n{'='*70}")
    print(f"  Per-pass summary:")
    total_rows = 0
    for ck, label, _ in all_passes:
        res    = results.get(ck, {"status": "not run", "rows": 0, "secs": 0})
        status = res["status"]
        rows   = res["rows"]
        secs   = res["secs"]
        if status == "done":
            tag = " DONE"; total_rows += rows
        elif status == "skipped":
            tag = " SKIP"
        elif status == "not run":
            tag = "  ---"
        else:
            tag = " FAIL"; any_failed = True
        print(f"  [{tag}] {label:<52}  {rows:>10,} rows  ({secs}s)")
        if status.startswith("FAILED"):
            print(f"         {status}")

    print(f"\n  Total rows affected: {total_rows:,}")
    print(f"{'='*70}")

    print(f"\n  Cleanup SQL (run after verifying data):")
    print(f"    -- Shared lookups (only drop when done with ALL allergies tables):")
    print(f"    -- DROP TABLE IF EXISTS {STAGING_SNOMED};")
    print(f"    -- DROP TABLE IF EXISTS {STAGING_SNOMED_TERMS};")
    print(f"    -- DROP TABLE IF EXISTS {STAGING_RX_MAX};")
    print(f"    -- DROP TABLE IF EXISTS {STAGING_ALLERGEN_KEYS};")
    print(f"    -- Per-run tables:")
    print(f"    DROP TABLE IF EXISTS {STAGING_PK_PASS1};")
    print(f"    DROP TABLE IF EXISTS {STAGING_PK_PASS2};")
    print(f"    DROP TABLE IF EXISTS {STAGING_PK_PASS3};")
    print(f"    DROP TABLE IF EXISTS {CHECKPOINT_TABLE};")

    if any_failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
