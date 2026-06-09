#!/usr/bin/env python3
"""
problemlist_stand_opt.py — Optimized batched standardisation UPDATEs for udm_staging.problemlist

4 sequential passes — each with checkpoint/resume:

  Pass 1 — ICD code lookup (icd_code IS NOT NULL rows):
    SET problem_desc_std
    JOIN semantics ICD9/ICD10 tables — all pre-materialized with REPLACE(code,'.','') pre-computed

  Pass 2 — SNOMED lookup (snomed_code IS NOT NULL rows):
    SET problem_desc_std
    JOIN semantics.snomed CTE collapsed into staging (one row per conceptId, latest active term)

  Pass 3 — SNOMED→ICD10 map (snomed_code IS NOT NULL rows):
    SET mapped_icd_code, mapped_icd_desc
    JOIN semantics.snomed_icd10_map — pre-materialized, filtered to PROPERLY CLASSIFIED

  Pass 4 — Comorbidity mapping (all rows with icd_code or snomed_code):
    SET charlson_comorbidity, elixhauser_comorbidity
    LIKE-pattern JOIN semantics.charlson_icd_map + semantics.elixhauser_icd_map

Pre-materialized lookup tables (computed ONCE, reused across all batches):
  - staging.prob_std_icd10    (icd10cm_with_parent — icd_code_clean + LONG_DESCRIPTION)
  - staging.prob_std_icd10f   (icd10_fixed          — icd_code_clean + LONG_DESCRIPTION)
  - staging.prob_std_icd9     (icd9cm_lookup         — icd_code_clean + LONG_DESCRIPTION)
  - staging.prob_std_icd9f    (icd9_fixed            — icd_code_clean + LONG_DESCRIPTION)
  - staging.prob_std_snomed   (snomed CTE collapsed  — one row per conceptId, latest term)
  - staging.prob_std_snomap   (snomed_icd10_map      — PROPERLY CLASSIFIED only)
  - staging.prob_std_charlson (charlson_icd_map)
  - staging.prob_std_elix     (elixhauser_icd_map)

Usage:
    python problemlist_stang_opt.py
"""

import sys
import time
from datetime import datetime
import pymysql
from tqdm import tqdm

# ── Configuration ─────────────────────────────────────────────────────
DB_CONFIG = {
    "host":            "ndai-dev-rds-instance.cwp60ymu4ko0.us-east-1.rds.amazonaws.com",
    "port":            3306,
    "user":            "Aalind",
    "password":        "A@L1nd@123",
    "database":        'tng_athena_one',
    "charset":         "utf8mb4",
    "connect_timeout": 30,
    "read_timeout":    21600,
    "write_timeout":   21600,
}

BATCH_SIZE = 50_000

TARGET_TABLE = "udm_staging.problemlist_rgd_v2"
BATCH_KEY    = "ndid"   # integer PK on problemlist table

_TABLE_SUFFIX = TARGET_TABLE.replace(".", "_").replace("-", "_")

# ── Shared lookup staging (reused across all passes) ──────────────────
STAGING_ICD10    = "staging.prob_std_icd10"
STAGING_ICD10F   = "staging.prob_std_icd10f"
STAGING_ICD9     = "staging.prob_std_icd9"
STAGING_ICD9F    = "staging.prob_std_icd9f"
STAGING_SNOMED   = "staging.prob_std_snomed"
STAGING_SNOMAP   = "staging.prob_std_snomap"
STAGING_CHARLSON      = "staging.prob_std_charlson"
STAGING_ELIX          = "staging.prob_std_elix"
STAGING_COMORBIDITY_MAP = f"staging.prob_std_comorbidity_map_{_TABLE_SUFFIX}"

# ── Per-run PK staging and checkpoint ────────────────────────────────
STAGING_PK_PASS1 = f"staging.prob_std_pk1_{_TABLE_SUFFIX}"
STAGING_PK_PASS2 = f"staging.prob_std_pk2_{_TABLE_SUFFIX}"
STAGING_PK_PASS3 = f"staging.prob_std_pk3_{_TABLE_SUFFIX}"
STAGING_PK_PASS4 = f"staging.prob_std_pk4_{_TABLE_SUFFIX}"
CHECKPOINT_TABLE = f"staging.etl_checkpoint_prob_std_{_TABLE_SUFFIX}"
CHECKPOINT_PASS1 = f"problemlist.std.pass1.icd_lookup.{_TABLE_SUFFIX}"
CHECKPOINT_PASS2 = f"problemlist.std.pass2.snomed_lookup.{_TABLE_SUFFIX}"
CHECKPOINT_PASS3 = f"problemlist.std.pass3.snomed_icd_map.{_TABLE_SUFFIX}"
CHECKPOINT_PASS4 = f"problemlist.std.pass4.comorbidity.{_TABLE_SUFFIX}"


# ── Helpers ───────────────────────────────────────────────────────────

def get_connection():
    return pymysql.connect(**DB_CONFIG)


def _table_exists(cur, full_table_name):
    schema, table = full_table_name.split(".", 1)
    cur.execute(
        "SELECT COUNT(*) FROM information_schema.tables "
        "WHERE table_schema = %s AND table_name = %s",
        (schema, table),
    )
    return cur.fetchone()[0] > 0


def _col_exists(cur, full_table_name, col_name):
    schema, table = full_table_name.split(".", 1)
    cur.execute(
        "SELECT COUNT(*) FROM information_schema.columns "
        "WHERE table_schema = %s AND table_name = %s AND column_name = %s",
        (schema, table, col_name),
    )
    return cur.fetchone()[0] > 0


def _index_exists(cur, schema, table, column):
    cur.execute("""
        SELECT COUNT(*) FROM information_schema.statistics
        WHERE table_schema = %s AND table_name = %s AND column_name = %s
    """, (schema, table, column))
    return cur.fetchone()[0] > 0


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


# ── Batch UPDATE builders ─────────────────────────────────────────────

def build_pass1(pk_lo, pk_hi):
    """Pass 1: SET problem_desc_std via ICD9/ICD10 code lookups.
    Joins through STAGING_PK_PASS1 to use pre-computed icd_code_clean
    (avoids UPPER/REPLACE per-row on every batch).
    """
    return f"""
UPDATE {TARGET_TABLE} p
JOIN {STAGING_PK_PASS1} pkc ON p.{BATCH_KEY} = pkc.{BATCH_KEY}
LEFT JOIN {STAGING_ICD10}  icd10  ON pkc.icd_code_clean = icd10.icd_code_clean
LEFT JOIN {STAGING_ICD10F} icd10f ON pkc.icd_code_clean = icd10f.icd_code_clean
LEFT JOIN {STAGING_ICD9}   icd9   ON pkc.icd_code_clean = icd9.icd_code_clean
LEFT JOIN {STAGING_ICD9F}  icd9f  ON pkc.icd_code_clean = icd9f.icd_code_clean
SET p.problem_desc_std = CASE
    WHEN COALESCE(icd9.LONG_DESCRIPTION, icd9f.LONG_DESCRIPTION) IS NOT NULL
         AND COALESCE(icd10.LONG_DESCRIPTION, icd10f.LONG_DESCRIPTION) IS NOT NULL
        THEN 'Matching both ICD-9 and ICD-10'
    ELSE COALESCE(
        icd10.LONG_DESCRIPTION,
        icd10f.LONG_DESCRIPTION,
        icd9.LONG_DESCRIPTION,
        icd9f.LONG_DESCRIPTION,
        'NS'
    )
END
WHERE p.icd_code IS NOT NULL
  AND TRIM(p.icd_code) != ''
  AND p.{BATCH_KEY} >= {pk_lo}
  AND p.{BATCH_KEY} <  {pk_hi}
"""


def build_pass2(pk_lo, pk_hi):
    """Pass 2: SET problem_desc_std via SNOMED latest-term lookup."""
    return f"""
UPDATE {TARGET_TABLE} p
LEFT JOIN {STAGING_SNOMED} s ON p.snomed_code = s.conceptId
SET p.problem_desc_std = COALESCE(s.term, 'NS')
WHERE p.snomed_code IS NOT NULL
  AND TRIM(p.snomed_code) != ''
  AND p.{BATCH_KEY} >= {pk_lo}
  AND p.{BATCH_KEY} <  {pk_hi}
"""


def build_pass3(pk_lo, pk_hi):
    """Pass 3: SET mapped_icd_code + mapped_icd_desc via SNOMED→ICD10 map."""
    return f"""
UPDATE {TARGET_TABLE} p
LEFT JOIN {STAGING_SNOMAP} m ON p.snomed_code = m.referencedcomponentid
SET p.mapped_icd_code = m.mapped_icd_code,
    p.mapped_icd_desc = m.mapped_icd_desc
WHERE p.snomed_code IS NOT NULL
  AND TRIM(p.snomed_code) != ''
  AND p.{BATCH_KEY} >= {pk_lo}
  AND p.{BATCH_KEY} <  {pk_hi}
"""


def build_pass4(pk_lo, pk_hi):
    """Pass 4: SET charlson_comorbidity + elixhauser_comorbidity.
    Uses STAGING_COMORBIDITY_MAP — LIKE join pre-computed ONCE on unique ICD codes,
    so batch UPDATE is a fast indexed equality join.
    """
    return f"""
UPDATE {TARGET_TABLE} p
JOIN {STAGING_PK_PASS4} pkc ON p.{BATCH_KEY} = pkc.{BATCH_KEY}
LEFT JOIN {STAGING_COMORBIDITY_MAP} cm ON pkc.icd_code_clean = cm.icd_code_clean
SET p.charlson_comorbidity   = cm.charlson_comorbidity,
    p.elixhauser_comorbidity = cm.elixhauser_comorbidity
WHERE (p.icd_code IS NOT NULL OR p.mapped_icd_code IS NOT NULL)
  AND p.{BATCH_KEY} >= {pk_lo}
  AND p.{BATCH_KEY} <  {pk_hi}
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
            (source_key, status, rows_updated, started_at, completed_at, error_msg)
        VALUES (%s, %s, %s, NOW(), IF(%s = 'done', NOW(), NULL), %s)
        ON DUPLICATE KEY UPDATE
            status       = VALUES(status),
            rows_updated = VALUES(rows_updated),
            completed_at = IF(VALUES(status) = 'done', NOW(), NULL),
            error_msg    = VALUES(error_msg)
    """, (checkpoint_key, status, rows, status, error))
    conn.commit()
    cur.close()


# ── DDL: ensure std columns exist ─────────────────────────────────────

_LONGTEXT_COLS  = {"problem_desc_std", "mapped_icd_desc"}
_LONGTEXT_TYPES = {"longtext"}


def ensure_std_columns():
    std_cols = [
        ("problem_desc_std",       "LONGTEXT"),
        ("mapped_icd_code",        "VARCHAR(50)"),
        ("mapped_icd_desc",        "LONGTEXT"),
        ("charlson_comorbidity",   "VARCHAR(255)"),
        ("elixhauser_comorbidity", "VARCHAR(255)"),
    ]
    print(f"  Checking std columns on {TARGET_TABLE}...", flush=True)
    ddl_conn = get_connection()
    ddl_cur  = ddl_conn.cursor()
    ddl_error = None
    added = []
    try:
        for col_name, col_type in std_cols:
            if not _col_exists(ddl_cur, TARGET_TABLE, col_name):
                print(f"    adding: {col_name} {col_type} ...", flush=True)
                ddl_cur.execute(
                    f"ALTER TABLE {TARGET_TABLE} ADD COLUMN {col_name} {col_type} DEFAULT NULL"
                )
                ddl_conn.commit()
                added.append(col_name)
                print(f"    added: {col_name}")
            elif col_name in _LONGTEXT_COLS:
                schema, table = TARGET_TABLE.split(".")
                ddl_cur.execute(
                    "SELECT DATA_TYPE FROM information_schema.columns "
                    "WHERE table_schema = %s AND table_name = %s AND column_name = %s",
                    (schema, table, col_name),
                )
                row = ddl_cur.fetchone()
                current_type = row[0].lower() if row else "longtext"
                if current_type not in _LONGTEXT_TYPES:
                    print(f"    widening: {col_name} ({current_type.upper()} → LONGTEXT)...", flush=True)
                    ddl_cur.execute(
                        f"ALTER TABLE {TARGET_TABLE} MODIFY COLUMN {col_name} LONGTEXT DEFAULT NULL"
                    )
                    ddl_conn.commit()
                    print(f"    widened: {col_name}")
                else:
                    print(f"    exists: {col_name} (LONGTEXT)")
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
        print(f"\n  ERROR: Could not alter {TARGET_TABLE} — likely a metadata lock.")
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


# ── Materialize one lookup table ──────────────────────────────────────

def _materialize(cur, conn, label, staging_tbl, create_sql, index_sql):
    print(f"  Materializing {label}...", flush=True)
    if not _table_exists(cur, staging_tbl):
        print("    creating...", flush=True)
        cur.execute(create_sql)
        cur.execute(index_sql)
        conn.commit()
        cur.execute(f"SELECT COUNT(*) FROM {staging_tbl}")
        n = cur.fetchone()[0]
        print(f"    {n:,} rows")
    else:
        cur.execute(f"SELECT COUNT(*) FROM {staging_tbl}")
        n = cur.fetchone()[0]
        print(f"    already exists, reusing  ({n:,} rows)")


# ── Setup ──────────────────────────────────────────────────────────────

def setup_tables():
    ensure_std_columns()

    conn = get_connection()
    cur  = conn.cursor()

    # ── 0. Ensure indexes on source/target tables BEFORE any staging queries ──
    print("  Checking source/target indexes...", flush=True)
    _tgt_schema, _tgt_table = TARGET_TABLE.split(".", 1)
    index_defs = [
        # (schema,       table,             column,            prefix)
        # prefix=None for VARCHAR; prefix=N only for TEXT/BLOB columns
        (_tgt_schema,   _tgt_table,        BATCH_KEY,         None),
        (_tgt_schema,   _tgt_table,        "icd_code",        None),
        (_tgt_schema,   _tgt_table,        "snomed_code",     None),
        ("semantics",   "snomed",          "conceptId",       None),
        ("semantics",   "snomed",          "active",          10),   # TEXT column — needs prefix
        ("semantics",   "snomed_icd10_map","mapcategoryname", 100),  # TEXT column — needs prefix
    ]
    for schema, table, col, prefix in index_defs:
        print(f"    {schema}.{table} ({col})...", end=" ", flush=True)
        if not _index_exists(cur, schema, table, col):
            col_spec = f"{col}({prefix})" if prefix else col
            print("missing — creating...", flush=True)
            cur.execute(f"CREATE INDEX idx_{col} ON {schema}.{table} ({col_spec})")
            conn.commit()
            print(f"      done")
        else:
            print("exists")

    # ── 1. ICD lookup tables — REPLACE('.','') pre-computed as icd_code_clean ──
    _materialize(cur, conn,
        "semantics.icd10cm_with_parent",
        STAGING_ICD10,
        f"""CREATE TABLE {STAGING_ICD10} AS
            SELECT REPLACE(diagnosis_code, '.', '') AS icd_code_clean,
                   LONG_DESCRIPTION
            FROM semantics.icd10cm_with_parent""",
        f"ALTER TABLE {STAGING_ICD10} ADD INDEX idx_icd_clean (icd_code_clean(20))"
    )

    _materialize(cur, conn,
        "semantics.icd10_fixed",
        STAGING_ICD10F,
        f"""CREATE TABLE {STAGING_ICD10F} AS
            SELECT REPLACE(code, '.', '') AS icd_code_clean,
                   LONG_DESCRIPTION
            FROM semantics.icd10_fixed""",
        f"ALTER TABLE {STAGING_ICD10F} ADD INDEX idx_icd_clean (icd_code_clean(20))"
    )

    _materialize(cur, conn,
        "semantics.icd9cm_lookup",
        STAGING_ICD9,
        f"""CREATE TABLE {STAGING_ICD9} AS
            SELECT REPLACE(diagnosis_code, '.', '') AS icd_code_clean,
                   LONG_DESCRIPTION
            FROM semantics.icd9cm_lookup""",
        f"ALTER TABLE {STAGING_ICD9} ADD INDEX idx_icd_clean (icd_code_clean(20))"
    )

    _materialize(cur, conn,
        "semantics.icd9_fixed",
        STAGING_ICD9F,
        f"""CREATE TABLE {STAGING_ICD9F} AS
            SELECT REPLACE(diagnosis_code, '.', '') AS icd_code_clean,
                   LONG_DESCRIPTION
            FROM semantics.icd9_fixed""",
        f"ALTER TABLE {STAGING_ICD9F} ADD INDEX idx_icd_clean (icd_code_clean(20))"
    )

    # ── 2. SNOMED — CTE collapsed: one row per conceptId, latest active term ──
    _materialize(cur, conn,
        "semantics.snomed (latest active term per conceptId)",
        STAGING_SNOMED,
        f"""CREATE TABLE {STAGING_SNOMED} AS
            SELECT s.conceptId, s.term
            FROM semantics.snomed s
            INNER JOIN (
                SELECT conceptId, MAX(id) AS latest_id
                FROM semantics.snomed
                WHERE active = 1
                GROUP BY conceptId
            ) sm ON s.conceptId = sm.conceptId AND s.id = sm.latest_id
            WHERE s.active = 1""",
        f"ALTER TABLE {STAGING_SNOMED} ADD INDEX idx_conceptId (conceptId(50))"
    )

    # ── 3. SNOMED→ICD10 map — PROPERLY CLASSIFIED only ───────────────
    _materialize(cur, conn,
        "semantics.snomed_icd10_map (PROPERLY CLASSIFIED)",
        STAGING_SNOMAP,
        f"""CREATE TABLE {STAGING_SNOMAP} AS
            SELECT referencedcomponentid,
                   REPLACE(maptarget, '?', '')  AS mapped_icd_code,
                   maptargetname                AS mapped_icd_desc
            FROM semantics.snomed_icd10_map
            WHERE mapcategoryname = 'MAP SOURCE CONCEPT IS PROPERLY CLASSIFIED'""",
        f"ALTER TABLE {STAGING_SNOMAP} ADD INDEX idx_refcomp (referencedcomponentid(50))"
    )

    # ── 4. Comorbidity maps — icd_pattern_clean pre-computed (avoids REPLACE per LIKE match) ──
    _materialize(cur, conn,
        "semantics.charlson_icd_map",
        STAGING_CHARLSON,
        f"""CREATE TABLE {STAGING_CHARLSON} AS
            SELECT charlson_comorbidity,
                   REPLACE(icd_pattern, '.', '') AS icd_pattern_clean
            FROM semantics.charlson_icd_map""",
        f"ALTER TABLE {STAGING_CHARLSON} ADD INDEX idx_pattern (icd_pattern_clean(50))"
    )

    _materialize(cur, conn,
        "semantics.elixhauser_icd_map",
        STAGING_ELIX,
        f"""CREATE TABLE {STAGING_ELIX} AS
            SELECT elixhauser_comorbidity,
                   REPLACE(icd_pattern, '.', '') AS icd_pattern_clean
            FROM semantics.elixhauser_icd_map""",
        f"ALTER TABLE {STAGING_ELIX} ADD INDEX idx_pattern (icd_pattern_clean(50))"
    )

    # ── 5. PK staging for passes 1-3 ─────────────────────────────────
    # Pass 1 includes pre-computed icd_code_clean = UPPER(REPLACE(icd_code,'.',''))
    # Pass 4 PK staging is created AFTER Pass 3 runs (mapped_icd_code must be populated first)

    print(f"  Creating PK staging for Pass 1 (icd_code IS NOT NULL)...", flush=True)
    if not _table_exists(cur, STAGING_PK_PASS1):
        print("    creating...", flush=True)
        cur.execute(f"""
            CREATE TABLE {STAGING_PK_PASS1} AS
            SELECT {BATCH_KEY},
                   UPPER(REPLACE(icd_code, '.', '')) AS icd_code_clean
            FROM {TARGET_TABLE}
            WHERE icd_code IS NOT NULL AND TRIM(icd_code) != '' AND {BATCH_KEY} IS NOT NULL
        """)
        cur.execute(f"ALTER TABLE {STAGING_PK_PASS1} ADD INDEX idx_pk ({BATCH_KEY})")
        cur.execute(f"ALTER TABLE {STAGING_PK_PASS1} ADD INDEX idx_clean (icd_code_clean(20))")
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")

    pk_configs_23 = [
        (STAGING_PK_PASS2, "Pass 2 (snomed_code IS NOT NULL)",
         f"WHERE snomed_code IS NOT NULL AND TRIM(snomed_code) != '' AND {BATCH_KEY} IS NOT NULL"),
        (STAGING_PK_PASS3, "Pass 3 (snomed_code IS NOT NULL)",
         f"WHERE snomed_code IS NOT NULL AND TRIM(snomed_code) != '' AND {BATCH_KEY} IS NOT NULL"),
    ]

    all_ranges = {}
    for staging_pk, label, where_clause in pk_configs_23:
        print(f"  Creating PK staging for {label}...", flush=True)
        if not _table_exists(cur, staging_pk):
            print("    creating...", flush=True)
            cur.execute(f"""
                CREATE TABLE {staging_pk} AS
                SELECT {BATCH_KEY} FROM {TARGET_TABLE}
                {where_clause}
            """)
            cur.execute(f"ALTER TABLE {staging_pk} ADD INDEX idx_pk ({BATCH_KEY})")
            conn.commit()
            print("    created")
        else:
            print("    already exists, reusing")

        ranges, total = _build_ranges(cur, staging_pk)
        print(f"    {total:,} rows → {len(ranges)} batches")
        all_ranges[label] = ranges

    # Pass 1 ranges
    ranges1, total1 = _build_ranges(cur, STAGING_PK_PASS1)
    print(f"    Pass 1: {total1:,} rows → {len(ranges1)} batches")
    all_ranges["Pass 1 (icd_code IS NOT NULL)"] = ranges1

    # ── 6. Checkpoint table ───────────────────────────────────────────
    print("  Creating checkpoint table...", flush=True)
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {CHECKPOINT_TABLE} (
            source_key   VARCHAR(200) NOT NULL PRIMARY KEY,
            status       ENUM('running','done','failed') NOT NULL DEFAULT 'running',
            rows_updated BIGINT      DEFAULT 0,
            started_at   DATETIME    DEFAULT NULL,
            completed_at DATETIME    DEFAULT NULL,
            error_msg    TEXT        DEFAULT NULL
        )
    """)
    conn.commit()
    print("    ready")

    cur.close()
    conn.close()

    return {
        CHECKPOINT_PASS1: all_ranges["Pass 1 (icd_code IS NOT NULL)"],
        CHECKPOINT_PASS2: all_ranges["Pass 2 (snomed_code IS NOT NULL)"],
        CHECKPOINT_PASS3: all_ranges["Pass 3 (snomed_code IS NOT NULL)"],
        # CHECKPOINT_PASS4 ranges built after Pass 3 via setup_pass4_pk()
    }


# ── Pass 4 PK staging (created AFTER Pass 3 populates mapped_icd_code) ──

def setup_pass4_pk():
    """Create Pass 4 PK staging after Pass 3 has populated mapped_icd_code.
    Pre-computes REPLACE(COALESCE(icd_code, mapped_icd_code), '.', '') as icd_code_clean.
    """
    conn = get_connection()
    cur  = conn.cursor()

    print(f"  Creating PK staging for Pass 4 (icd_code or mapped_icd_code)...", flush=True)
    if not _table_exists(cur, STAGING_PK_PASS4):
        print("    creating...", flush=True)
        cur.execute(f"""
            CREATE TABLE {STAGING_PK_PASS4} AS
            SELECT {BATCH_KEY},
                   REPLACE(COALESCE(icd_code, mapped_icd_code), '.', '') AS icd_code_clean
            FROM {TARGET_TABLE}
            WHERE (icd_code IS NOT NULL OR mapped_icd_code IS NOT NULL)
              AND {BATCH_KEY} IS NOT NULL
        """)
        cur.execute(f"ALTER TABLE {STAGING_PK_PASS4} ADD INDEX idx_pk ({BATCH_KEY})")
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")

    ranges, total = _build_ranges(cur, STAGING_PK_PASS4)
    print(f"    {total:,} rows → {len(ranges)} batches")

    # ── Build comorbidity map ONCE on unique ICD codes (replaces per-batch LIKE) ──
    print(f"  Building comorbidity map on unique ICD codes...", flush=True)
    if not _table_exists(cur, STAGING_COMORBIDITY_MAP):
        print("    running LIKE join on unique codes (one-time)...", flush=True)
        cur.execute(f"""
            CREATE TABLE {STAGING_COMORBIDITY_MAP} AS
            SELECT
                codes.icd_code_clean,
                MAX(ci.charlson_comorbidity)   AS charlson_comorbidity,
                MAX(ei.elixhauser_comorbidity) AS elixhauser_comorbidity
            FROM (
                SELECT DISTINCT icd_code_clean
                FROM {STAGING_PK_PASS4}
                WHERE icd_code_clean IS NOT NULL
            ) codes
            LEFT JOIN {STAGING_CHARLSON} ci ON codes.icd_code_clean LIKE ci.icd_pattern_clean
            LEFT JOIN {STAGING_ELIX}     ei ON codes.icd_code_clean LIKE ei.icd_pattern_clean
            GROUP BY codes.icd_code_clean
        """)
        cur.execute(f"ALTER TABLE {STAGING_COMORBIDITY_MAP} ADD INDEX idx_clean (icd_code_clean(20))")
        conn.commit()
        cur.execute(f"SELECT COUNT(*) FROM {STAGING_COMORBIDITY_MAP}")
        n = cur.fetchone()[0]
        print(f"    mapped {n:,} unique ICD codes")
    else:
        cur.execute(f"SELECT COUNT(*) FROM {STAGING_COMORBIDITY_MAP}")
        n = cur.fetchone()[0]
        print(f"    already exists, reusing  ({n:,} unique codes)")

    cur.close()
    conn.close()
    return ranges


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
            cur.execute(build_fn(pk_lo, pk_hi))
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
    print(f"  Problemlist Standardisation UPDATE — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  target     : {TARGET_TABLE}")
    print(f"  batch_key  : {BATCH_KEY}")
    print(f"  batch_size : {BATCH_SIZE:,}")
    print(f"  passes     : 4  (ICD lookup | SNOMED lookup | SNOMED→ICD map | Comorbidity)")
    print(f"{'='*70}\n", flush=True)

    all_ranges = setup_tables()

    passes_123 = [
        (CHECKPOINT_PASS1, "Pass 1 — ICD9/ICD10 code lookup   (icd_code rows)",    build_pass1),
        (CHECKPOINT_PASS2, "Pass 2 — SNOMED term lookup        (snomed_code rows)", build_pass2),
        (CHECKPOINT_PASS3, "Pass 3 — SNOMED→ICD10 map          (snomed_code rows)", build_pass3),
    ]
    passes_all = passes_123 + [
        (CHECKPOINT_PASS4, "Pass 4 — Charlson/Elixhauser map   (all coded rows)",   build_pass4),
    ]

    results = {}
    any_failed = False

    # Passes 1-3: ranges already known from setup
    total_batches_123 = sum(len(all_ranges.get(ck, [])) for ck, _, _ in passes_123)
    # Pass 4 batch count unknown until after Pass 3 — tqdm updated below

    with tqdm(total=total_batches_123, desc="Passes 1-3", unit="batch") as pbar:
        for ck, label, build_fn in passes_123:
            ranges = all_ranges.get(ck, [])
            if not ranges:
                print(f"\n  [SKIP] {label} — no eligible rows")
                continue

            print(f"\n  Starting {label} ({len(ranges)} batches)...", flush=True)
            result = run_pass(ck, build_fn, ranges, pbar)
            results[ck] = result

            if result["status"].startswith("FAILED"):
                print(f"\n  FAILED at {label}: {result['status']}")
                print("  Aborting remaining passes.")
                any_failed = True
                break

    if not any_failed:
        # Build Pass 4 PK staging now that mapped_icd_code is populated by Pass 3
        print(f"\n  Setting up Pass 4 PK staging...", flush=True)
        ranges4 = setup_pass4_pk()

        ck4, label4, build4 = passes_all[3]
        if not ranges4:
            print(f"\n  [SKIP] {label4} — no eligible rows")
        else:
            print(f"\n  Starting {label4} ({len(ranges4)} batches)...", flush=True)
            with tqdm(total=len(ranges4), desc="Pass 4", unit="batch") as pbar4:
                result4 = run_pass(ck4, build4, ranges4, pbar4)
            results[ck4] = result4
            if result4["status"].startswith("FAILED"):
                print(f"\n  FAILED at {label4}: {result4['status']}")
                any_failed = True

    print(f"\n{'='*70}")
    print(f"  Per-pass summary:")
    total_rows = 0
    for ck, label, _ in passes_all:
        res = results.get(ck, {"status": "not run", "rows": 0, "secs": 0})
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
        print(f"  [{tag}] {label:<55}  {rows:>10,} rows  ({secs}s)")
        if status.startswith("FAILED"):
            print(f"         {status}")

    print(f"\n  Total rows updated: {total_rows:,}")
    print(f"{'='*70}")

    print(f"\n  Cleanup SQL (run after verifying data):")
    print(f"    -- Shared lookups (only drop when fully done):")
    print(f"    -- DROP TABLE IF EXISTS {STAGING_ICD10};")
    print(f"    -- DROP TABLE IF EXISTS {STAGING_ICD10F};")
    print(f"    -- DROP TABLE IF EXISTS {STAGING_ICD9};")
    print(f"    -- DROP TABLE IF EXISTS {STAGING_ICD9F};")
    print(f"    -- DROP TABLE IF EXISTS {STAGING_SNOMED};")
    print(f"    -- DROP TABLE IF EXISTS {STAGING_SNOMAP};")
    print(f"    -- DROP TABLE IF EXISTS {STAGING_CHARLSON};")
    print(f"    -- DROP TABLE IF EXISTS {STAGING_ELIX};")
    print(f"    -- Per-run tables:")
    print(f"    DROP TABLE IF EXISTS {STAGING_PK_PASS1};")
    print(f"    DROP TABLE IF EXISTS {STAGING_PK_PASS2};")
    print(f"    DROP TABLE IF EXISTS {STAGING_PK_PASS3};")
    print(f"    DROP TABLE IF EXISTS {STAGING_PK_PASS4};")
    print(f"    DROP TABLE IF EXISTS {STAGING_COMORBIDITY_MAP};")
    print(f"    DROP TABLE IF EXISTS {CHECKPOINT_TABLE};")

    if any_failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
