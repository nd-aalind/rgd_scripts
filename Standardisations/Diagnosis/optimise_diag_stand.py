#!/usr/bin/env python3
"""
Optimized batched standardisation UPDATE for: rgd_udm_silver.diagnosis

Three sequential passes — each with checkpoint/resume:

  Pass 1 — All rows:
    SET diag_desc_std, diag_coding_system_std
    diag_desc_std = NULL (no code) | 'Matching both ICD-9 and ICD-10' | COALESCE(icd10, icd10f, icd9, icd9f, 'NS')
    diag_coding_system_std = NULL (no code) | 'ICD-10' | 'ICD-9' | 'Matching both ICD-9 and ICD-10' | 'NS'

  Pass 2 — WHERE diag_coding_system_std = 'Matching both ICD-9 and ICD-10':
    Reclassify V-codes and E-codes using length/RLIKE rules
    Updates diag_desc_std and diag_coding_system_std

  Pass 3 — All rows:
    SET primary_diagnosis_flag_std based on ehr_source_name + primary_diagnosis_flag

Pre-materialized lookup tables (computed ONCE from semantics schema, reused all batches):
  - staging.diag_std_icd10cm   (icd10cm_with_parent  → diagnosis_code_clean, LONG_DESCRIPTION)
  - staging.diag_std_icd10f    (icd10_fixed          → code_clean, LONG_DESCRIPTION)
  - staging.diag_std_icd9cm    (icd9cm_lookup        → diagnosis_code_clean, LONG_DESCRIPTION)
  - staging.diag_std_icd9f     (icd9_fixed           → diagnosis_code_clean, LONG_DESCRIPTION)

No temporary columns added to production tables — UPPER(REPLACE(diag_code, '.', ''))
is computed inline per batch.

Optimizations applied:
- All 4 semantic lookup tables pre-materialized once with indexed clean code columns
- Per-pass PK staging tables (filtered to eligible rows only)
- Server-side boundary sampling (avoids loading all PKs into memory)
- Commit after every batch (frees undo/log space)
- Checkpoint/resume per pass — re-run skips completed passes
- Disabled InnoDB checks per-session for bulk update speed
- Progress bar via tqdm

Usage:
    python optimise_diag_stand.py
"""

import sys
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
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

BATCH_SIZE  = 100_000
NUM_WORKERS = 3

TARGET_TABLE = "rgd_udm_silver.diagnosis"
SEMANTICS_DB = "semantics"

# ── Pre-materialized semantic lookup tables ───────────────────────────
STAGING_ICD10CM     = "staging.diag_std_icd10cm_fn_f"      # icd10cm_with_parent
STAGING_ICD10F      = "staging.diag_std_icd10f_fn_f"       # icd10_fixed
STAGING_ICD9CM      = "staging.diag_std_icd9cm_fn_f"       # icd9cm_lookup
STAGING_ICD9F       = "staging.diag_std_icd9f_fn_f"        # icd9_fixed
STAGING_SOURCE_CLEAN = "staging.diag_std_source_clean_fn_v1" # pre-computed clean codes from target
STAGING_MERGED       = "staging.diag_std_merged_fn_v1"        # pre-joined icd10+icd9 descriptions (1 JOIN per batch)

# ── Per-pass PK staging and checkpoint tables ─────────────────────────
STAGING_PK_PASS1 = "staging.diag_std_pk_pass1_fn_f"
STAGING_PK_PASS2 = "staging.diag_std_pk_pass2_fn_f"
STAGING_PK_PASS3 = "staging.diag_std_pk_pass3_fn_f"

CHECKPOINT_TABLE = "staging.etl_checkpoint_diag_standardisation_fn_f_v2"
CHECKPOINT_PASS1 = "diagnosis.standardisation.pass1_fn_f"
CHECKPOINT_PASS2 = "diagnosis.standardisation.pass2_fn_f"
CHECKPOINT_PASS3 = "diagnosis.standardisation.pass3_primary_flag_fn_f"

# ── Primary key column for batching ───────────────────────────────────
BATCH_KEY = "udm_inc_id"   # integer PK on rgd_udm_silver.diagnosis


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


# ── Batch UPDATE builders ─────────────────────────────────────────────

def build_pass1(pk_lo, pk_hi):
    """
    Pass 1: set diag_desc_std, diag_coding_system_std on all rows.
    2 JOINs: source_clean (PK, 47M rows) + code_merged (code, ~50-200K rows).
    'Matching both ICD-9 and ICD-10' flags ambiguous rows for Pass 2 reclassification.
    """
    return f"""
UPDATE {TARGET_TABLE} d
JOIN      {STAGING_SOURCE_CLEAN} sc ON d.{BATCH_KEY}      = sc.{BATCH_KEY}
LEFT JOIN {STAGING_MERGED}        m  ON sc.diag_code_clean = m.diag_code_clean
SET
    d.diag_desc_std = CASE
        WHEN sc.diag_code_clean IS NULL OR sc.diag_code_clean = '' THEN NULL
        WHEN m.icd9_desc IS NOT NULL AND m.icd10_desc IS NOT NULL
            THEN 'Matching both ICD-9 and ICD-10'
        ELSE COALESCE(m.icd10_desc, m.icd9_desc, 'NS')
    END,
    d.diag_coding_system_std = CASE
        WHEN sc.diag_code_clean IS NULL OR sc.diag_code_clean = '' THEN NULL
        WHEN m.icd10_desc IS NOT NULL AND m.icd9_desc IS NULL      THEN 'ICD-10'
        WHEN m.icd9_desc  IS NOT NULL AND m.icd10_desc IS NULL     THEN 'ICD-9'
        WHEN m.icd9_desc  IS NOT NULL AND m.icd10_desc IS NOT NULL THEN 'Matching both ICD-9 and ICD-10'
        ELSE 'NS'
    END
WHERE d.{BATCH_KEY} >= {pk_lo}
  AND d.{BATCH_KEY} < {pk_hi}
"""


def build_pass2(pk_lo, pk_hi):
    """
    Pass 2: V-code / E-code reclassification.
    Only runs on rows WHERE diag_coding_system_std = 'Matching both ICD-9 and ICD-10'.
    2 JOINs: source_clean (PK) + code_merged (code, ~50-200K rows).
    """
    return f"""
UPDATE {TARGET_TABLE} d
JOIN      {STAGING_SOURCE_CLEAN} sc ON d.{BATCH_KEY}      = sc.{BATCH_KEY}
LEFT JOIN {STAGING_MERGED}        m  ON sc.diag_code_clean = m.diag_code_clean
SET
    d.diag_desc_std = CASE
        WHEN sc.diag_code_clean LIKE 'V%' AND LENGTH(sc.diag_code_clean) > 5
            THEN m.icd10_desc
        WHEN sc.diag_code_upper RLIKE '^E[0-9]{{2}}(\\\\.[0-9]{{1,4}})?$'
            THEN m.icd10_desc
        WHEN sc.diag_code_clean LIKE 'V%' AND LENGTH(sc.diag_code_clean) <= 5
            THEN m.icd9_desc
        WHEN sc.diag_code_upper RLIKE '^E[0-9]{{3}}(\\\\.[0-9]+)?$'
            THEN m.icd9_desc
        ELSE 'NS'
    END,
    d.diag_coding_system_std = CASE
        WHEN sc.diag_code_clean LIKE 'V%' AND LENGTH(sc.diag_code_clean) > 5  THEN 'ICD-10'
        WHEN sc.diag_code_clean LIKE 'V%' AND LENGTH(sc.diag_code_clean) <= 5 THEN 'ICD-9'
        WHEN sc.diag_code_upper RLIKE '^E[0-9]{{3}}(\\\\.[0-9]+)?$'            THEN 'ICD-9'
        WHEN sc.diag_code_upper RLIKE '^E[0-9]{{2}}(\\\\.[0-9]{{1,4}})?$'      THEN 'ICD-10'
        ELSE 'NS'
    END
WHERE d.diag_coding_system_std = 'Matching both ICD-9 and ICD-10'
  AND d.{BATCH_KEY} >= {pk_lo}
  AND d.{BATCH_KEY} < {pk_hi}
"""


def build_pass3(pk_lo, pk_hi):
    """
    Pass 3: standardise primary_diagnosis_flag_std from ehr_source_name.
    - ECW:  1='Y', 0='N'
    - AthenaOne: 0='Y', >0='N'
    - Greenway / AthenaPractice: 1='Y', >1='N'
    - General fallback: Y/N string
    """
    # CAST to CHAR avoids error 1292 when primary_diagnosis_flag is a DOUBLE column —
    # comparing DOUBLE IN ('Y','N') causes MySQL to cast 'Y' to DOUBLE and fail.
    return f"""
UPDATE {TARGET_TABLE} d
SET
    d.primary_diagnosis_flag_std = CASE
        WHEN LOWER(d.ehr_source_name) = 'ecw'
             AND CAST(d.primary_diagnosis_flag AS CHAR) = '1' THEN 'Y'
        WHEN LOWER(d.ehr_source_name) = 'ecw'
             AND CAST(d.primary_diagnosis_flag AS CHAR) = '0' THEN 'N'
        WHEN LOWER(d.ehr_source_name) = 'athenaone'
             AND CAST(d.primary_diagnosis_flag AS CHAR) = '0' THEN 'Y'
        WHEN LOWER(d.ehr_source_name) = 'athenaone'
             AND CAST(d.primary_diagnosis_flag AS CHAR) REGEXP '^[0-9]'
             AND CAST(d.primary_diagnosis_flag AS UNSIGNED) > 0 THEN 'N'
        WHEN LOWER(d.ehr_source_name) IN ('greenway', 'athenapractice')
             AND CAST(d.primary_diagnosis_flag AS CHAR) = '1' THEN 'Y'
        WHEN LOWER(d.ehr_source_name) IN ('greenway', 'athenapractice')
             AND CAST(d.primary_diagnosis_flag AS CHAR) REGEXP '^[0-9]'
             AND CAST(d.primary_diagnosis_flag AS UNSIGNED) > 1 THEN 'N'
        WHEN CAST(d.primary_diagnosis_flag AS CHAR) IN ('y', 'Y') THEN 'Y'
        WHEN CAST(d.primary_diagnosis_flag AS CHAR) IN ('n', 'N') THEN 'N'
        WHEN d.primary_diagnosis_flag IS NULL THEN NULL
        ELSE 'NS'
    END
WHERE d.{BATCH_KEY} >= {pk_lo}
  AND d.{BATCH_KEY} < {pk_hi}
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


# ── Setup ──────────────────────────────────────────────────────────────

def _build_ranges(cur, staging_pk):
    """Compute batch boundary ranges from a PK staging table."""
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


def setup_tables():
    """
    0. Add std columns to target table if they don't exist.
    1. Pre-materialize 4 semantic lookup tables with clean code columns + indexes.
    2. Create PK staging tables for each pass (filtered to eligible rows).
    3. Create checkpoint table.
    4. Compute batch ranges per pass.
    Returns dict: checkpoint_key → list of (lo, hi) ranges.
    """
    conn = get_connection()
    cur  = conn.cursor()

    # ── 0. Ensure std columns exist on target table ───────────────────
    # Uses a dedicated short-lived connection with a 15-second lock_wait_timeout.
    # If another session holds a metadata lock on the table, this fails fast
    # with a clear error instead of hanging indefinitely.
    print("  Checking std columns on target table...")
    target_schema, target_table = TARGET_TABLE.split(".")
    std_columns = [
        ("diag_desc_std",              "TEXT"),
        ("diag_coding_system_std",     "VARCHAR(50)"),
        ("primary_diagnosis_flag_std", "VARCHAR(10)"),
    ]

    ddl_conn = get_connection()
    ddl_cur  = ddl_conn.cursor()
    # Fail fast — don't hang waiting for a metadata lock
    ddl_cur.execute("SET lock_wait_timeout = 15")

    columns_added = []
    ddl_error = None
    try:
        for col_name, col_type in std_columns:
            ddl_cur.execute(
                "SELECT COUNT(*) FROM information_schema.columns "
                "WHERE table_schema = %s AND table_name = %s AND column_name = %s",
                (target_schema, target_table, col_name),
            )
            if ddl_cur.fetchone()[0] == 0:
                print(f"    adding column: {col_name} {col_type} ...")
                ddl_cur.execute(
                    f"ALTER TABLE {TARGET_TABLE} ADD COLUMN {col_name} {col_type} DEFAULT NULL"
                )
                ddl_conn.commit()
                columns_added.append(col_name)
                print(f"    added: {col_name}")
            else:
                print(f"    already exists: {col_name}")
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
        print(f"\n  ERROR: Could not add column — metadata lock detected.")
        print(f"  Another session may have an open transaction on {TARGET_TABLE}.")
        print(f"  Run this to find blocking sessions:")
        print(f"    SELECT * FROM information_schema.processlist WHERE state LIKE '%lock%';")
        print(f"  Then KILL the blocking process ID, or wait for it to finish.")
        print(f"\n  Original error: {ddl_error}")
        sys.exit(1)

    if columns_added:
        print(f"  Columns added: {', '.join(columns_added)}")
    else:
        print("  All std columns already present.")

    # ── 1. Semantic lookup tables ─────────────────────────────────────
    # Store clean codes without dots — JOIN matches UPPER(REPLACE(diag_code,'.',''))
    lookups = [
        {
            "name":      STAGING_ICD10CM,
            "label":     "icd10cm_with_parent",
            "src_sql":   f"SELECT REPLACE(diagnosis_code, '.', '') AS diagnosis_code_clean, LONG_DESCRIPTION FROM {SEMANTICS_DB}.icd10cm_with_parent",
            "ddl":       f"CREATE TABLE {STAGING_ICD10CM} (diagnosis_code_clean VARCHAR(20), LONG_DESCRIPTION TEXT) ENGINE=InnoDB",
            "index_col": "diagnosis_code_clean",
        },
        {
            "name":      STAGING_ICD10F,
            "label":     "icd10_fixed",
            "src_sql":   f"SELECT REPLACE(code, '.', '') AS code_clean, LONG_DESCRIPTION FROM {SEMANTICS_DB}.icd10_fixed",
            "ddl":       f"CREATE TABLE {STAGING_ICD10F} (code_clean VARCHAR(20), LONG_DESCRIPTION TEXT) ENGINE=InnoDB",
            "index_col": "code_clean",
        },
        {
            "name":      STAGING_ICD9CM,
            "label":     "icd9cm_lookup",
            "src_sql":   f"SELECT REPLACE(diagnosis_code, '.', '') AS diagnosis_code_clean, LONG_DESCRIPTION FROM {SEMANTICS_DB}.icd9cm_lookup",
            "ddl":       f"CREATE TABLE {STAGING_ICD9CM} (diagnosis_code_clean VARCHAR(20), LONG_DESCRIPTION TEXT) ENGINE=InnoDB",
            "index_col": "diagnosis_code_clean",
        },
        {
            "name":      STAGING_ICD9F,
            "label":     "icd9_fixed",
            "src_sql":   f"SELECT REPLACE(diagnosis_code, '.', '') AS diagnosis_code_clean, LONG_DESCRIPTION FROM {SEMANTICS_DB}.icd9_fixed",
            "ddl":       f"CREATE TABLE {STAGING_ICD9F} (diagnosis_code_clean VARCHAR(20), LONG_DESCRIPTION TEXT) ENGINE=InnoDB",
            "index_col": "diagnosis_code_clean",
        },
    ]

    for lkp in lookups:
        print(f"  Materializing {lkp['label']} lookup...")
        if not _table_exists(cur, lkp["name"]):
            cur.execute(lkp["ddl"])
            cur.execute(f"INSERT INTO {lkp['name']} {lkp['src_sql']}")
            cur.execute(f"ALTER TABLE {lkp['name']} ADD INDEX idx_clean ({lkp['index_col']}(50))")
            conn.commit()
            print("    created")
        else:
            print("    already exists, reusing")
        cur.execute(f"SELECT COUNT(*) FROM {lkp['name']}")
        print(f"    {cur.fetchone()[0]:,} rows")

    # ── 2. Source clean-code staging ──────────────────────────────────
    # Pre-compute UPPER(REPLACE(diag_code,'.','')) and UPPER(diag_code) ONCE.
    # All batch UPDATEs join through this table — eliminates repeated inline string ops.
    print("  Materializing source clean-code staging...")
    # If table exists but is empty (partial from a previous failed run), drop and recreate.
    needs_create = False
    if not _table_exists(cur, STAGING_SOURCE_CLEAN):
        needs_create = True
    else:
        cur.execute(f"SELECT COUNT(*) FROM {STAGING_SOURCE_CLEAN}")
        existing_rows = cur.fetchone()[0]
        if existing_rows == 0:
            print("    found empty table (previous failed run) — dropping and recreating...")
            cur.execute(f"DROP TABLE {STAGING_SOURCE_CLEAN}")
            conn.commit()
            needs_create = True
        else:
            print(f"    already exists, reusing  ({existing_rows:,} rows)")

    if needs_create:
        sc_exc = None
        cur.execute("SET lock_wait_timeout = 30")
        try:
            # Explicitly InnoDB — avoids the staging schema MEMORY default.
            cur.execute(f"""
                CREATE TABLE {STAGING_SOURCE_CLEAN} (
                    {BATCH_KEY}        BIGINT,
                    diag_code_clean    CHAR(20),
                    diag_code_upper    CHAR(30)
                ) ENGINE=InnoDB ROW_FORMAT=DYNAMIC
            """)
            cur.execute(f"""
                INSERT INTO {STAGING_SOURCE_CLEAN}
                SELECT
                    {BATCH_KEY},
                    CAST(UPPER(REPLACE(TRIM(diag_code), '.', '')) AS CHAR(20)) AS diag_code_clean,
                    CAST(UPPER(diag_code) AS CHAR(30))                          AS diag_code_upper
                FROM {TARGET_TABLE}
                WHERE {BATCH_KEY} IS NOT NULL
            """)
            conn.commit()
            cur.execute(f"ALTER TABLE {STAGING_SOURCE_CLEAN} ADD INDEX idx_pk ({BATCH_KEY})")
            cur.execute(f"ALTER TABLE {STAGING_SOURCE_CLEAN} ADD INDEX idx_clean (diag_code_clean)")
            conn.commit()
            print("    created")
        except Exception as e:
            sc_exc = e
            try:
                conn.rollback()
            except Exception:
                pass
        cur.execute("SET lock_wait_timeout = DEFAULT")
        if sc_exc:
            print(f"\n  ERROR: Could not create source clean-code staging.")
            print(f"\n  Original error: {sc_exc}")
            cur.close()
            conn.close()
            sys.exit(1)

    cur.execute(f"SELECT COUNT(*) FROM {STAGING_SOURCE_CLEAN}")
    print(f"    {cur.fetchone()[0]:,} rows")

    # ── 3. Pre-merged ICD lookup (4 joins done ONCE here, not per batch) ─
    # Stores icd10_desc = COALESCE(icd10cm, icd10f) and icd9_desc = COALESCE(icd9cm, icd9f)
    # keyed by udm_inc_id — each batch UPDATE needs only 1 JOIN on the PK.
    print("  Materializing pre-merged ICD lookup (icd10_desc + icd9_desc per row)...")
    needs_merged = False
    if not _table_exists(cur, STAGING_MERGED):
        needs_merged = True
    else:
        cur.execute(f"SELECT COUNT(*) FROM {STAGING_MERGED}")
        if cur.fetchone()[0] == 0:
            print("    found empty table (previous failed run) — dropping and recreating...")
            cur.execute(f"DROP TABLE {STAGING_MERGED}")
            conn.commit()
            needs_merged = True
        else:
            print("    already exists, reusing")

    if needs_merged:
        # Join only DISTINCT codes — may be 50K-200K rows, not 47M.
        # Each batch UPDATE then does: target → source_clean (PK) → code_merged (code).
        cur.execute(f"""
            CREATE TABLE {STAGING_MERGED} (
                diag_code_clean  CHAR(20) NOT NULL,
                icd10_desc       TEXT,
                icd9_desc        TEXT
            ) ENGINE=InnoDB ROW_FORMAT=DYNAMIC
        """)
        cur.execute(f"""
            INSERT INTO {STAGING_MERGED}
            SELECT
                sc.diag_code_clean,
                COALESCE(icd10.LONG_DESCRIPTION, icd10f.LONG_DESCRIPTION) AS icd10_desc,
                COALESCE(icd9.LONG_DESCRIPTION,  icd9f.LONG_DESCRIPTION)  AS icd9_desc
            FROM (SELECT DISTINCT diag_code_clean FROM {STAGING_SOURCE_CLEAN}
                  WHERE diag_code_clean IS NOT NULL AND diag_code_clean != '') sc
            LEFT JOIN {STAGING_ICD10CM} icd10  ON sc.diag_code_clean = icd10.diagnosis_code_clean
            LEFT JOIN {STAGING_ICD10F}  icd10f ON sc.diag_code_clean = icd10f.code_clean
            LEFT JOIN {STAGING_ICD9CM}  icd9   ON sc.diag_code_clean = icd9.diagnosis_code_clean
            LEFT JOIN {STAGING_ICD9F}   icd9f  ON sc.diag_code_clean = icd9f.diagnosis_code_clean
        """)
        conn.commit()
        cur.execute(f"ALTER TABLE {STAGING_MERGED} ADD INDEX idx_clean (diag_code_clean(20))")
        conn.commit()
        print("    created")

    cur.execute(f"SELECT COUNT(*) FROM {STAGING_MERGED}")
    print(f"    {cur.fetchone()[0]:,} rows")

    # ── 4. Per-pass PK staging ─────────────────────────────────────────
    # Note: Pass 2 filter references diag_coding_system_std which is set in Pass 1.
    # Pass 2 staging must be created AFTER Pass 1 completes if re-running from scratch.
    # For simplicity we collect all three now; Pass 2 staging may have 0 rows before Pass 1.

    pass_staging = [
        {
            "key":    CHECKPOINT_PASS1,
            "label":  "Pass 1 (all rows)",
            "stg":    STAGING_PK_PASS1,
            "filter": f"{BATCH_KEY} IS NOT NULL",
        },
        {
            "key":    CHECKPOINT_PASS2,
            "label":  "Pass 2 (Matching both ICD-9 and ICD-10 rows)",
            "stg":    STAGING_PK_PASS2,
            "filter": f"diag_coding_system_std = 'Matching both ICD-9 and ICD-10' AND {BATCH_KEY} IS NOT NULL",
        },
        {
            "key":    CHECKPOINT_PASS3,
            "label":  "Pass 3 (primary_diagnosis_flag_std)",
            "stg":    STAGING_PK_PASS3,
            "filter": f"{BATCH_KEY} IS NOT NULL",
        },
    ]

    all_ranges = {}
    for ps in pass_staging:
        print(f"  Creating PK staging for {ps['label']}...")
        if not _table_exists(cur, ps["stg"]):
            cur.execute(f"""
                CREATE TABLE {ps['stg']} AS
                SELECT {BATCH_KEY}
                FROM {TARGET_TABLE}
                WHERE {ps['filter']}
            """)
            cur.execute(f"ALTER TABLE {ps['stg']} ADD INDEX idx_pk ({BATCH_KEY})")
            conn.commit()
            print("    created")
        else:
            print("    already exists, reusing")

        ranges, total = _build_ranges(cur, ps["stg"])
        print(f"    {total:,} rows → {len(ranges)} batches")
        all_ranges[ps["key"]] = ranges

    # ── 3. Checkpoint table ────────────────────────────────────────────
    print("  Creating checkpoint table...")
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {CHECKPOINT_TABLE} (
            source_key   VARCHAR(150) NOT NULL PRIMARY KEY,
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
    return all_ranges


# ── Runner ─────────────────────────────────────────────────────────────

def _run_batch(pk_lo, pk_hi, build_fn, max_retries=3):
    """Execute a single batch UPDATE in its own connection (thread-safe).
    Retries up to max_retries times on deadlock (errno 1213)."""
    for attempt in range(max_retries):
        conn = get_connection()
        try:
            cur = conn.cursor()
            cur.execute("SET unique_checks = 0")
            cur.execute("SET foreign_key_checks = 0")
            cur.execute(build_fn(pk_lo, pk_hi))
            conn.commit()
            rows = cur.rowcount
            cur.execute("SET unique_checks = 1")
            cur.execute("SET foreign_key_checks = 1")
            cur.close()
            return rows, None
        except Exception as exc:
            try:
                conn.rollback()
            except Exception:
                pass
            # Retry on deadlock with brief backoff
            if getattr(exc, 'args', (None,))[0] == 1213 and attempt < max_retries - 1:
                time.sleep(2 ** attempt)   # 1s, 2s backoff
                continue
            return 0, str(exc)
        finally:
            try:
                conn.close()
            except Exception:
                pass
    return 0, f"Deadlock persisted after {max_retries} retries"


def run_pass(checkpoint_key, build_fn, ranges, pbar):
    conn = get_connection()

    if is_done(conn, checkpoint_key):
        conn.close()
        pbar.update(len(ranges))
        return {"status": "skipped", "rows": 0, "secs": 0}

    mark(conn, checkpoint_key, "running")
    conn.close()

    t0         = time.time()
    total_rows = 0
    errors     = []

    with ThreadPoolExecutor(max_workers=NUM_WORKERS) as executor:
        futures = {
            executor.submit(_run_batch, lo, hi, build_fn): (lo, hi)
            for lo, hi in ranges
        }
        for future in as_completed(futures):
            rows, err = future.result()
            total_rows += rows
            if err:
                errors.append(err)
            pbar.update(1)

    elapsed = round(time.time() - t0, 1)
    conn = get_connection()
    if errors:
        mark(conn, checkpoint_key, "failed", total_rows, errors[0])
        conn.close()
        return {"status": f"FAILED: {errors[0]}", "rows": total_rows, "secs": elapsed}

    mark(conn, checkpoint_key, "done", total_rows)
    conn.close()
    return {"status": "done", "rows": total_rows, "secs": elapsed}


# ── Pass 2 rebuild ─────────────────────────────────────────────────────

def rebuild_pass2_staging():
    """
    Rebuild Pass 2 PK staging AFTER Pass 1 completes.
    Pass 1 sets diag_coding_system_std — Pass 2 staging needs those values.
    Only rebuilds if the table is empty (0 rows from initial setup before Pass 1 ran).
    """
    conn = get_connection()
    cur  = conn.cursor()

    cur.execute(f"SELECT COUNT(*) FROM {STAGING_PK_PASS2}")
    count = cur.fetchone()[0]

    if count == 0:
        print("  Rebuilding Pass 2 PK staging (now that Pass 1 has populated diag_coding_system_std)...")
        cur.execute(f"DROP TABLE IF EXISTS {STAGING_PK_PASS2}")
        cur.execute(f"""
            CREATE TABLE {STAGING_PK_PASS2} AS
            SELECT {BATCH_KEY}
            FROM {TARGET_TABLE}
            WHERE diag_coding_system_std = 'Matching both ICD-9 and ICD-10'
              AND {BATCH_KEY} IS NOT NULL
        """)
        cur.execute(f"ALTER TABLE {STAGING_PK_PASS2} ADD INDEX idx_pk ({BATCH_KEY})")
        conn.commit()

        ranges, total = _build_ranges(cur, STAGING_PK_PASS2)
        cur.close()
        conn.close()
        print(f"    {total:,} rows → {len(ranges)} batches")
        return ranges
    else:
        ranges, total = _build_ranges(cur, STAGING_PK_PASS2)
        cur.close()
        conn.close()
        return ranges


# ── Main ───────────────────────────────────────────────────────────────

def main():
    print(f"\n{'='*70}", flush=True)
    print(f"  Diagnosis Standardisation UPDATE — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  target     : {TARGET_TABLE}")
    print(f"  semantics  : {SEMANTICS_DB}.(icd10cm_with_parent | icd10_fixed | icd9cm_lookup | icd9_fixed)")
    print(f"  batch_key  : {BATCH_KEY}")
    print(f"  batch_size : {BATCH_SIZE:,}")
    print(f"  passes     : 3  (diag_desc_std + coding system | V/E-code fix | primary_flag)")
    print(f"  workers    : {NUM_WORKERS}  (parallel batches per pass)")
    print(f"{'='*70}\n", flush=True)

    print("  Connecting to database...")
    sys.stdout.flush()
    all_ranges = setup_tables()

    passes = [
        (CHECKPOINT_PASS1, "Pass 1 — ICD lookup (all rows)",                   build_pass1),
        (CHECKPOINT_PASS2, "Pass 2 — V/E-code fix (Matching both rows)",       build_pass2),
        (CHECKPOINT_PASS3, "Pass 3 — primary_diagnosis_flag_std (all rows)",   build_pass3),
    ]

    results = {}
    any_failed = False

    # Passes are sequential — Pass 2 depends on Pass 1 output
    total_batches = sum(len(all_ranges.get(ck, [])) for ck, _, _ in passes)
    with tqdm(total=total_batches, desc="Overall", unit="batch") as pbar:
        for ck, label, build_fn in passes:
            ranges = all_ranges.get(ck, [])

            # After Pass 1 finishes, rebuild Pass 2 staging if it was empty
            if ck == CHECKPOINT_PASS2 and not is_done(get_connection(), CHECKPOINT_PASS2):
                ranges = rebuild_pass2_staging()
                all_ranges[CHECKPOINT_PASS2] = ranges
                # Adjust pbar total for newly discovered Pass 2 rows
                pbar.total = sum(len(all_ranges.get(c, [])) for c, _, _ in passes)
                pbar.refresh()

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

    print(f"\n{'='*70}")
    print(f"  Per-pass summary:")
    total_rows = 0
    for ck, label, _ in passes:
        res = results.get(ck, {"status": "not run", "rows": 0, "secs": 0})
        status = res["status"]
        rows   = res["rows"]
        secs   = res["secs"]
        if status == "done":
            tag = " DONE"
            total_rows += rows
        elif status == "skipped":
            tag = " SKIP"
        elif status == "not run":
            tag = "  ---"
        else:
            tag = " FAIL"
            any_failed = True
        print(f"  [{tag}] {label:<52}  {rows:>10,} rows  ({secs}s)")
        if status.startswith("FAILED"):
            print(f"         {status}")

    print(f"\n  Total rows updated: {total_rows:,}")
    print(f"{'='*70}")

    print(f"\n  Cleanup SQL (run after verifying data):")
    print(f"    DROP TABLE IF EXISTS {STAGING_ICD10CM};")
    print(f"    DROP TABLE IF EXISTS {STAGING_ICD10F};")
    print(f"    DROP TABLE IF EXISTS {STAGING_ICD9CM};")
    print(f"    DROP TABLE IF EXISTS {STAGING_ICD9F};")
    print(f"    DROP TABLE IF EXISTS {STAGING_SOURCE_CLEAN};")
    print(f"    DROP TABLE IF EXISTS {STAGING_MERGED};")
    print(f"    DROP TABLE IF EXISTS {STAGING_PK_PASS1};")
    print(f"    DROP TABLE IF EXISTS {STAGING_PK_PASS2};")
    print(f"    DROP TABLE IF EXISTS {STAGING_PK_PASS3};")
    print(f"    DROP TABLE IF EXISTS {CHECKPOINT_TABLE};")
    print(f"    -- If upgrading from old two-column schema, also drop:")
    print(f"    -- ALTER TABLE {TARGET_TABLE} DROP COLUMN IF EXISTS icd10_desc_std;")
    print(f"    -- ALTER TABLE {TARGET_TABLE} DROP COLUMN IF EXISTS icd9_desc_std;")

    if any_failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
