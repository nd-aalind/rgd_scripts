#!/usr/bin/env python3
"""
Optimized batched standardisation UPDATE for: kinsula_leq.diagnosis

Three sequential passes — each with checkpoint/resume:

  Pass 1 — All rows:
    SET diag_desc_std, diag_coding_system_std
    diag_desc_std = NULL (no code) | 'Matching both ICD-9 and ICD-10' | COALESCE(icd10, icd9, 'NS')
    diag_coding_system_std = NULL (no code) | 'ICD-10' | 'ICD-9' | 'Matching both ICD-9 and ICD-10' | 'NS'

  Pass 2 — WHERE diag_coding_system_std = 'Matching both ICD-9 and ICD-10':
    Reclassify V-codes and E-codes using length/RLIKE rules.
    ELSE clause preserves existing diag_desc_std / diag_coding_system_std (not 'NS').
    PK staging rebuilt AFTER Pass 1 (depends on diag_coding_system_std being set).

  Pass 3 — All rows:
    SET primary_diagnosis_flag_std based on ehr_source_name + primary_diagnosis_flag
    - ecw:                '1'→Y, '0'→N
    - athenaone:          '0'→Y, ('1','9')→N
    - greenway/athenapractice: '1'→Y, ('0','9')→N
    - general fallback:   LOWER='y'→Y, LOWER='n'→N

Pre-materialized lookup tables (computed ONCE, reused across batches):
  - staging.diag_opt_icd10cm   (icd10cm_with_parent  → diagnosis_code_clean, LONG_DESCRIPTION)
  - staging.diag_opt_icd10f    (icd10_fixed          → code_clean, LONG_DESCRIPTION)
  - staging.diag_opt_icd9cm    (icd9cm_lookup        → diagnosis_code_clean, LONG_DESCRIPTION)
  - staging.diag_opt_icd9f     (icd9_fixed           → diagnosis_code_clean, LONG_DESCRIPTION)
  - staging.diag_opt_source_clean  (pre-computed UPPER(REPLACE(diag_code,'.','')) per row)
  - staging.diag_opt_merged        (distinct diag_code_clean → icd10_desc, icd9_desc)

Optimizations applied:
- 4 ICD lookup tables pre-materialized once with indexed clean code columns
- Merged code-level lookup (only distinct codes — ~28K rows — not 47M)
- Each batch UPDATE: 2 JOINs (PK + code) instead of 4 inline JOINs
- Parallel batch execution via ThreadPoolExecutor (4 workers)
- Deadlock retry with exponential backoff (up to 3 retries)
- Per-pass PK staging tables (filtered to eligible rows only)
- Pass 2 PK staging rebuilt after Pass 1
- Server-side boundary sampling (avoids loading all PKs into memory)
- Commit after every batch (frees undo/log space)
- Checkpoint/resume per pass — re-run skips completed passes
- Disabled InnoDB checks per-session for bulk update speed
- Progress bar via tqdm

Usage:
    python diag_optimise_new.py
"""

import sys
import time
import logging
import os
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import pymysql
from tqdm import tqdm
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())

# ── Logging setup ──────────────────────────────────────────────────────
os.makedirs("logs", exist_ok=True)
_log_file = os.path.join("logs", f"diagnosis_stand_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(_log_file, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)

# ── Configuration ─────────────────────────────────────────────────────
DB_CONFIG = {
    "host":            os.environ.get("DB_HOST"),
    "port":            3306,
    "user":            os.environ.get("DB_USER"),
    "password":        os.environ.get("DB_PASSWORD"),
    "database":        "udm_staging",
    "charset":         "utf8mb4",
    "connect_timeout": 30,
    "read_timeout":    1800,
    "write_timeout":   1800,
}

BATCH_SIZE  = 25_000
NUM_WORKERS = 2

TARGET_TABLE     = "rgd_udm_silver.diagnosis"
SEMANTICS_DB     = "semantics"
FILTER_FROM_DATE = "2026-06-18"   # only process rows WHERE createddatetime >= this date; set to None to process all rows

# ── Pre-materialized semantic lookup tables ───────────────────────────
STAGING_ICD10CM      = "staging.diag_opt_icd10cm1"       # icd10cm_with_parent
STAGING_ICD10F       = "staging.diag_opt_icd10f1"        # icd10_fixed
STAGING_ICD9CM       = "staging.diag_opt_icd9cm1"        # icd9cm_lookup
STAGING_ICD9F        = "staging.diag_opt_icd9f1"         # icd9_fixed
STAGING_SOURCE_CLEAN = "staging.diag_opt_source_clean1"  # pre-computed clean codes from target
STAGING_MERGED       = "staging.diag_opt_merged1"        # distinct code → icd10_desc, icd9_desc

# ── Per-pass PK staging and checkpoint tables ─────────────────────────
STAGING_PK_PASS1 = "staging.diag_opt_pk_pass12"
STAGING_PK_PASS2 = "staging.diag_opt_pk_pass23"
STAGING_PK_PASS3 = "staging.diag_opt_pk_pass34"

CHECKPOINT_TABLE = "staging.etl_checkpoint_diag_opt1"
CHECKPOINT_PASS1 = "diagnosis.opt.pass1"
CHECKPOINT_PASS2 = "diagnosis.opt.pass2"
CHECKPOINT_PASS3 = "diagnosis.opt.pass3_primary_flag"

# ── Primary key column for batching ───────────────────────────────────
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


# ── Batch UPDATE builders ─────────────────────────────────────────────

def build_pass1(pk_lo, pk_hi):
    """
    Pass 1: set diag_desc_std, diag_coding_system_std on all rows.
    2 JOINs: source_clean (PK) + merged (distinct code → icd10_desc, icd9_desc).
    'Matching both ICD-9 and ICD-10' flags ambiguous rows for Pass 2.
    """
    return f"""
UPDATE {TARGET_TABLE} d
JOIN      {STAGING_SOURCE_CLEAN} sc ON d.{BATCH_KEY}      = sc.{BATCH_KEY}
LEFT JOIN {STAGING_MERGED}        m  ON sc.diag_code_clean = m.diag_code_clean
SET
    d.diag_desc_std = CASE
        WHEN d.diag_code IS NULL OR d.diag_code = '' THEN NULL
        WHEN m.icd9_desc IS NOT NULL AND m.icd10_desc IS NOT NULL
            THEN 'Matching both ICD-9 and ICD-10'
        ELSE COALESCE(m.icd10_desc, m.icd9_desc, 'NS')
    END,
    d.diag_coding_system_std = CASE
        WHEN d.diag_code IS NULL OR d.diag_code = '' THEN NULL
        WHEN m.icd10_desc IS NOT NULL AND m.icd9_desc IS NULL      THEN 'ICD-10'
        WHEN m.icd9_desc  IS NOT NULL AND m.icd10_desc IS NULL     THEN 'ICD-9'
        WHEN m.icd9_desc  IS NOT NULL AND m.icd10_desc IS NOT NULL THEN 'Matching both ICD-9 and ICD-10'
        ELSE 'NS'
    END
WHERE (d.diag_desc_std IS NULL OR d.diag_coding_system_std IS NULL)
  AND d.{BATCH_KEY} >= {pk_lo}
  AND d.{BATCH_KEY} < {pk_hi}
"""


def build_pass2(pk_lo, pk_hi):
    """
    Pass 2: V-code / E-code reclassification.
    Only runs on rows WHERE diag_coding_system_std = 'Matching both ICD-9 and ICD-10'.
    ELSE clause preserves existing value (not 'NS') — matches diag_optimise.sql behavior.
    """
    return f"""
UPDATE {TARGET_TABLE} d
JOIN      {STAGING_SOURCE_CLEAN} sc ON d.{BATCH_KEY}      = sc.{BATCH_KEY}
LEFT JOIN {STAGING_MERGED}        m  ON sc.diag_code_clean = m.diag_code_clean
SET
    d.diag_desc_std = CASE
        WHEN sc.diag_code_clean LIKE 'V%' AND LENGTH(sc.diag_code_clean) > 5
            THEN m.icd10_desc
        WHEN sc.diag_code_clean RLIKE '^E[0-9]{{2}}(\\\\.[0-9]{{1,4}})?$'
            THEN m.icd10_desc
        WHEN sc.diag_code_clean LIKE 'V%' AND LENGTH(sc.diag_code_clean) <= 5
            THEN m.icd9_desc
        WHEN sc.diag_code_clean RLIKE '^E[0-9]{{3}}(\\\\.[0-9]+)?$'
            THEN m.icd9_desc
        ELSE d.diag_desc_std
    END,
    d.diag_coding_system_std = CASE
        WHEN sc.diag_code_clean LIKE 'V%' AND LENGTH(sc.diag_code_clean) > 5  THEN 'ICD-10'
        WHEN sc.diag_code_clean LIKE 'V%' AND LENGTH(sc.diag_code_clean) <= 5 THEN 'ICD-9'
        WHEN sc.diag_code_clean RLIKE '^E[0-9]{{3}}(\\\\.[0-9]+)?$'            THEN 'ICD-9'
        WHEN sc.diag_code_clean RLIKE '^E[0-9]{{2}}(\\\\.[0-9]{{1,4}})?$'      THEN 'ICD-10'
        ELSE d.diag_coding_system_std
    END
WHERE d.diag_coding_system_std = 'Matching both ICD-9 and ICD-10'
  AND d.{BATCH_KEY} >= {pk_lo}
  AND d.{BATCH_KEY} < {pk_hi}
"""


def build_pass3(pk_lo, pk_hi):
    """
    Pass 3: standardise primary_diagnosis_flag_std from ehr_source_name.
    - ecw:                    '1'→Y, '0'→N
    - athenaone:              '0'→Y, ('1','9')→N
    - greenway/athenapractice:'1'→Y, ('0','9')→N
    - general fallback:       LOWER='y'→Y, LOWER='n'→N
    Uses direct string comparisons (no CAST) matching diag_optimise.sql.
    """
    return f"""
UPDATE {TARGET_TABLE} d
SET
    d.primary_diagnosis_flag_std = CASE
        WHEN LOWER(d.ehr_source_name) = 'ecw'
             AND d.primary_diagnosis_flag = '1' THEN 'Y'
        WHEN LOWER(d.ehr_source_name) = 'ecw'
             AND d.primary_diagnosis_flag = '0' THEN 'N'
        WHEN LOWER(d.ehr_source_name) = 'athenaone'
             AND d.primary_diagnosis_flag = '0' THEN 'Y'
        WHEN LOWER(d.ehr_source_name) = 'athenaone'
             AND d.primary_diagnosis_flag IN ('1', '9') THEN 'N'
        WHEN LOWER(d.ehr_source_name) IN ('greenway', 'athenapractice')
             AND d.primary_diagnosis_flag = '1' THEN 'Y'
        WHEN LOWER(d.ehr_source_name) IN ('greenway', 'athenapractice')
             AND d.primary_diagnosis_flag IN ('0', '9') THEN 'N'
        WHEN LOWER(d.primary_diagnosis_flag) = 'y' THEN 'Y'
        WHEN LOWER(d.primary_diagnosis_flag) = 'n' THEN 'N'
        WHEN d.primary_diagnosis_flag IS NULL THEN NULL
        ELSE 'NS'
    END
WHERE d.primary_diagnosis_flag_std IS NULL
  AND d.{BATCH_KEY} >= {pk_lo}
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

def setup_tables():
    conn = get_connection()
    cur  = conn.cursor()

    # ── 0. Ensure std columns exist on target table ───────────────────
    logger.info("Checking std columns on %s ...", TARGET_TABLE)
    target_schema, target_table = TARGET_TABLE.split(".")
    std_columns = [
        ("diag_desc_std",              "TEXT"),
        ("diag_coding_system_std",     "VARCHAR(50)"),
        ("primary_diagnosis_flag_std", "VARCHAR(10)"),
    ]

    ddl_conn = get_connection()
    ddl_cur  = ddl_conn.cursor()
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
                logger.info("  Adding column: %s %s ...", col_name, col_type)
                ddl_cur.execute(
                    f"ALTER TABLE {TARGET_TABLE} ADD COLUMN {col_name} {col_type} DEFAULT NULL"
                )
                ddl_conn.commit()
                columns_added.append(col_name)
                logger.info("  Added: %s", col_name)
            else:
                logger.info("  Already exists: %s", col_name)
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
        logger.error("Could not add column — metadata lock detected.")
        logger.error(
            "Run: SELECT * FROM information_schema.processlist WHERE state LIKE '%%lock%%';\n"
            "Then KILL the blocking process ID."
        )
        logger.error("Original error: %s", ddl_error)
        sys.exit(1)

    if columns_added:
        logger.info("  Columns added: %s", ", ".join(columns_added))
    else:
        logger.info("  All std columns already present.")

    # ── 1. Semantic lookup tables ─────────────────────────────────────
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
        logger.info("Materializing %s lookup ...", lkp["label"])
        if not _table_exists(cur, lkp["name"]):
            cur.execute(lkp["ddl"])
            cur.execute(f"INSERT INTO {lkp['name']} {lkp['src_sql']}")
            cur.execute(f"ALTER TABLE {lkp['name']} ADD INDEX idx_clean ({lkp['index_col']})")
            conn.commit()
            logger.info("  Created %s", lkp["name"])
        else:
            logger.info("  Already exists, reusing %s", lkp["name"])
        cur.execute(f"SELECT COUNT(*) FROM {lkp['name']}")
        logger.info("  %s rows", f"{cur.fetchone()[0]:,}")

    # ── 2. Source clean-code staging ──────────────────────────────────
    logger.info("Materializing source clean-code staging (incremental — NULL _std rows only) ...")
    if _table_exists(cur, STAGING_SOURCE_CLEAN):
        cur.execute(f"DROP TABLE {STAGING_SOURCE_CLEAN}")
        conn.commit()

    sc_exc = None
    cur.execute("SET lock_wait_timeout = 30")
    try:
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
              AND (diag_desc_std IS NULL OR diag_coding_system_std IS NULL
                   OR primary_diagnosis_flag_std IS NULL)
              {f"AND created_datetime >= '{FILTER_FROM_DATE}'" if FILTER_FROM_DATE else ""}
        """)
        conn.commit()
        cur.execute(f"ALTER TABLE {STAGING_SOURCE_CLEAN} ADD INDEX idx_pk ({BATCH_KEY})")
        cur.execute(f"ALTER TABLE {STAGING_SOURCE_CLEAN} ADD INDEX idx_clean (diag_code_clean)")
        conn.commit()
        logger.info("  Created %s", STAGING_SOURCE_CLEAN)
    except Exception as e:
        sc_exc = e
        try:
            conn.rollback()
        except Exception:
            pass
    cur.execute("SET lock_wait_timeout = DEFAULT")
    if sc_exc:
        logger.error("Could not create source clean-code staging: %s", sc_exc)
        cur.close()
        conn.close()
        sys.exit(1)

    cur.execute(f"SELECT COUNT(*) FROM {STAGING_SOURCE_CLEAN}")
    logger.info("  %s rows in source clean-code staging", f"{cur.fetchone()[0]:,}")

    # ── 3. Pre-merged ICD lookup (distinct codes only — fast) ─────────
    logger.info("Materializing pre-merged ICD lookup (icd10_desc + icd9_desc per distinct code) ...")
    if _table_exists(cur, STAGING_MERGED):
        cur.execute(f"DROP TABLE {STAGING_MERGED}")
        conn.commit()

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
    logger.info("  Created %s", STAGING_MERGED)

    cur.execute(f"SELECT COUNT(*) FROM {STAGING_MERGED}")
    logger.info("  %s distinct codes in merged lookup", f"{cur.fetchone()[0]:,}")

    # ── 4. Per-pass PK staging ─────────────────────────────────────────
    _date_filter = f"AND created_datetime >= '{FILTER_FROM_DATE}'" if FILTER_FROM_DATE else ""

    pass_staging = [
        {
            "key":    CHECKPOINT_PASS1,
            "label":  "Pass 1 (NULL diag_desc_std / diag_coding_system_std rows)",
            "stg":    STAGING_PK_PASS1,
            "filter": f"(diag_desc_std IS NULL OR diag_coding_system_std IS NULL) AND {BATCH_KEY} IS NOT NULL {_date_filter}",
        },
        {
            "key":    CHECKPOINT_PASS2,
            "label":  "Pass 2 (Matching both ICD-9 and ICD-10 rows)",
            "stg":    STAGING_PK_PASS2,
            "filter": f"diag_coding_system_std = 'Matching both ICD-9 and ICD-10' AND {BATCH_KEY} IS NOT NULL {_date_filter}",
        },
        {
            "key":    CHECKPOINT_PASS3,
            "label":  "Pass 3 (NULL primary_diagnosis_flag_std rows)",
            "stg":    STAGING_PK_PASS3,
            "filter": f"primary_diagnosis_flag_std IS NULL AND {BATCH_KEY} IS NOT NULL {_date_filter}",
        },
    ]

    all_ranges = {}
    for ps in pass_staging:
        logger.info("Creating PK staging for %s ...", ps["label"])
        if _table_exists(cur, ps["stg"]):
            cur.execute(f"DROP TABLE {ps['stg']}")
            conn.commit()
        cur.execute(f"""
            CREATE TABLE {ps['stg']} AS
            SELECT {BATCH_KEY}
            FROM {TARGET_TABLE}
            WHERE {ps['filter']}
        """)
        cur.execute(f"ALTER TABLE {ps['stg']} ADD INDEX idx_pk ({BATCH_KEY})")
        conn.commit()

        ranges, total = _build_ranges(cur, ps["stg"])
        logger.info("  %s rows → %d batches", f"{total:,}", len(ranges))
        all_ranges[ps["key"]] = ranges

    # ── 5. Checkpoint table ────────────────────────────────────────────
    logger.info("Resetting checkpoint table ...")
    cur.execute(f"DROP TABLE IF EXISTS {CHECKPOINT_TABLE}")
    cur.execute(f"""
        CREATE TABLE {CHECKPOINT_TABLE} (
            source_key   VARCHAR(150) NOT NULL PRIMARY KEY,
            status       ENUM('running','done','failed') NOT NULL DEFAULT 'running',
            rows_updated BIGINT      DEFAULT 0,
            started_at   DATETIME    DEFAULT NULL,
            completed_at DATETIME    DEFAULT NULL,
            error_msg    TEXT        DEFAULT NULL
        )
    """)
    conn.commit()
    logger.info("  Checkpoint table ready")

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
            err_code = getattr(exc, 'args', (None,))[0]
            _retry_reasons = {1213: "Deadlock", 1205: "Lock wait timeout", 2013: "Connection lost"}
            if err_code in _retry_reasons and attempt < max_retries - 1:
                wait = 2 ** attempt
                logger.warning(
                    "%s on batch [%s, %s), attempt %d/%d — retrying in %ds",
                    _retry_reasons[err_code], pk_lo, pk_hi, attempt + 1, max_retries, wait,
                )
                time.sleep(wait)
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
        logger.info("  Skipped (already done): %s", checkpoint_key)
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
                logger.error("Batch error: %s", err)
                errors.append(err)
            pbar.update(1)

    elapsed = round(time.time() - t0, 1)
    conn = get_connection()
    if errors:
        mark(conn, checkpoint_key, "failed", total_rows, errors[0])
        conn.close()
        logger.error("  Pass failed after %ss: %s", elapsed, errors[0])
        return {"status": f"FAILED: {errors[0]}", "rows": total_rows, "secs": elapsed}

    mark(conn, checkpoint_key, "done", total_rows)
    conn.close()
    logger.info("  Done: %s rows updated in %ss", f"{total_rows:,}", elapsed)
    return {"status": "done", "rows": total_rows, "secs": elapsed}


# ── Pass 2 rebuild ─────────────────────────────────────────────────────

def rebuild_pass2_staging():
    """Rebuild Pass 2 PK staging AFTER Pass 1 sets diag_coding_system_std."""
    conn = get_connection()
    cur  = conn.cursor()

    cur.execute(f"SELECT COUNT(*) FROM {STAGING_PK_PASS2}")
    count = cur.fetchone()[0]

    if count == 0:
        logger.info("Rebuilding Pass 2 PK staging (now that Pass 1 has set diag_coding_system_std) ...")
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
        logger.info("  %s rows → %d batches", f"{total:,}", len(ranges))
        return ranges
    else:
        ranges, total = _build_ranges(cur, STAGING_PK_PASS2)
        cur.close()
        conn.close()
        return ranges


# ── Main ───────────────────────────────────────────────────────────────

def main():
    logger.info("=" * 70)
    logger.info("Diagnosis Optimised Standardisation — %s", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    logger.info("Log file  : %s", os.path.abspath(_log_file))
    logger.info("Target    : %s", TARGET_TABLE)
    logger.info("Semantics : %s.(icd10cm_with_parent | icd10_fixed | icd9cm_lookup | icd9_fixed)", SEMANTICS_DB)
    logger.info("Batch key : %s", BATCH_KEY)
    logger.info("Batch size: %s", f"{BATCH_SIZE:,}")
    logger.info("Passes    : 3  (diag_desc_std + coding system | V/E-code fix | primary_flag)")
    logger.info("Workers   : %d  (parallel batches per pass)", NUM_WORKERS)
    logger.info("Date filter: created_datetime >= %s", FILTER_FROM_DATE if FILTER_FROM_DATE else "(none — all rows)")
    logger.info("=" * 70)

    logger.info("Connecting to database ...")
    all_ranges = setup_tables()

    passes = [
        (CHECKPOINT_PASS1, "Pass 1 — ICD lookup (all rows)",                 build_pass1),
        (CHECKPOINT_PASS2, "Pass 2 — V/E-code fix (Matching both rows)",     build_pass2),
        (CHECKPOINT_PASS3, "Pass 3 — primary_diagnosis_flag_std (all rows)", build_pass3),
    ]

    results    = {}
    any_failed = False

    total_batches = sum(len(all_ranges.get(ck, [])) for ck, _, _ in passes)
    with tqdm(total=total_batches, desc="Overall", unit="batch") as pbar:
        for ck, label, build_fn in passes:
            ranges = all_ranges.get(ck, [])

            # After Pass 1 finishes, rebuild Pass 2 staging if it was empty
            if ck == CHECKPOINT_PASS2 and not is_done(get_connection(), CHECKPOINT_PASS2):
                ranges = rebuild_pass2_staging()
                all_ranges[CHECKPOINT_PASS2] = ranges
                pbar.total = sum(len(all_ranges.get(c, [])) for c, _, _ in passes)
                pbar.refresh()

            if not ranges:
                logger.info("[SKIP] %s — no eligible rows", label)
                continue

            logger.info("Starting %s (%d batches) ...", label, len(ranges))
            result = run_pass(ck, build_fn, ranges, pbar)
            results[ck] = result

            if result["status"].startswith("FAILED"):
                logger.error("FAILED at %s: %s", label, result["status"])
                logger.error("Aborting remaining passes.")
                any_failed = True
                break

    logger.info("=" * 70)
    logger.info("Per-pass summary:")
    total_rows = 0
    for ck, label, _ in passes:
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
        logger.info("  [%s] %-52s  %10s rows  (%ss)", tag, label, f"{rows:,}", secs)
        if status.startswith("FAILED"):
            logger.error("         %s", status)

    logger.info("Total rows updated: %s", f"{total_rows:,}")
    logger.info("=" * 70)

    logger.info("Cleanup SQL (run after verifying data):")
    for t in [STAGING_ICD10CM, STAGING_ICD10F, STAGING_ICD9CM, STAGING_ICD9F,
              STAGING_SOURCE_CLEAN, STAGING_MERGED,
              STAGING_PK_PASS1, STAGING_PK_PASS2, STAGING_PK_PASS3, CHECKPOINT_TABLE]:
        logger.info("  DROP TABLE IF EXISTS %s;", t)

    if any_failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
