#!/usr/bin/env python3
"""
Optimized batched standardisation UPDATEs for: vitals

Change TARGET_TABLE at the top to run against any vitals table.

Two sequential passes — each with checkpoint/resume:

  Pass 1 — All rows:
    SET vital_name_std, vital_code_std, vital_coding_system_std
    LEFT JOIN pre-materialized semantics.vitals_loinc lookup
    Match key: LOWER(REPLACE(TRIM(vital_name), ':', ''))
    Blood pressure rows where result has no '/' → vital_name_std = 'NS'

  Pass 2 — WHERE vital_name_std = 'Blood pressure (Split Required)':
    INSERT new rows for Systolic blood pressure and Diastolic blood pressure
    bp_clean = REGEXP_REPLACE to strip HTML + non-numeric chars, must match ^[0-9]+/[0-9]+$
    Original 'Blood pressure (Split Required)' rows are KEPT as-is.
    PK staging rebuilt AFTER Pass 1 (depends on vital_name_std being set).

    WARNING: If Pass 2 fails mid-run, partially inserted rows must be manually
    deleted before re-running. See cleanup SQL at the end.
    Delete: DELETE FROM {TARGET_TABLE} WHERE vital_name_std IN
            ('Systolic blood pressure','Diastolic blood pressure') AND udm_inc_id = 0;

Pre-materialized lookup tables (computed ONCE, reused across runs):
  - staging.vitals_std_loinc   (semantics.vitals_loinc — indexed on vital_name_clean)

Std columns added to target table if not present (with metadata lock guard).

Optimizations applied:
- vitals_loinc lookup pre-materialized once with pre-computed clean key column
- Per-pass PK staging tables (filtered to eligible rows only)
- Pass 2 PK staging rebuilt after Pass 1 (depends on vital_name_std output)
- Server-side boundary sampling (avoids loading all PKs into memory)
- Commit after every batch (frees undo/log space)
- Checkpoint/resume per pass — re-run skips completed passes
- Disabled InnoDB checks per-session for bulk update speed
- Progress bar via tqdm

Usage:
    python vitals_opt_stand.py
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
    "host":            os.environ.get("DB_HOST"),
    "port":            3306,
    "user":            os.environ.get("DB_USER"),
    "password":        os.environ.get("DB_PASSWORD"),
    "database":        'rgd_udm_silver',
    "charset":         "utf8mb4",
    "connect_timeout": 30,
    "read_timeout":    21600,
    "write_timeout":   21600,
}

BATCH_SIZE = 50_000

# ── Change this to run against a different vitals table ───────────────
TARGET_TABLE = "rgd_udm_staging.vitals_ecw_test"

# ─────────────────────────────────────────────────────────────────────
_TABLE_SUFFIX = TARGET_TABLE.replace(".", "_").replace("-", "_")

STAGING_LOINC          = "staging.vitals_std_loinc_f3"                        # shared across runs
STAGING_PK_PASS1       = f"staging.vitals_std_pk1_fn3_{_TABLE_SUFFIX}"
STAGING_PK_PASS2       = f"staging.vitals_std_pk2_fn3_{_TABLE_SUFFIX}"
STAGING_BP_CLEAN       = f"staging.vitals_std_bp_clean_fn3_{_TABLE_SUFFIX}"    # Pass 2: BP rows with vital_clean pre-computed
STAGING_PASS3_COMPUTED = f"staging.vitals_std_p3comp_fn3_{_TABLE_SUFFIX}"      # Pass 3: pre-computed vital_result_std/unit_std
CHECKPOINT_TABLE = f"staging.etl_checkpoint_vitals_std_fn3_{_TABLE_SUFFIX}"
CHECKPOINT_PASS1 = f"vitals.std.pass1.loinc_lookup_fn3.{_TABLE_SUFFIX}"
CHECKPOINT_PASS2 = f"vitals.std.pass2.bp_split_insert_fn3.{_TABLE_SUFFIX}"
CHECKPOINT_PASS3 = f"vitals.std.pass3.result_unit_std_fn3.{_TABLE_SUFFIX}"

BATCH_KEY = "ndid"
ROW_KEY   = "vital_id"   # actual per-row unique key — used by Pass 3 JOIN for correctness

# Populated during setup — columns of TARGET_TABLE that are copied verbatim in Pass 2 INSERT.
# Excludes std columns (those get explicit values) so the INSERT works on any target table.
_PASS2_COPY_COLS: list = []

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


def _index_exists(cur, schema, table, column):
    cur.execute(
        "SELECT COUNT(*) FROM information_schema.statistics "
        "WHERE table_schema = %s AND table_name = %s AND column_name = %s",
        (schema, table, column),
    )
    return cur.fetchone()[0] > 0


def _load_pass2_copy_cols(conn):
    """
    Populate _PASS2_COPY_COLS with every column in TARGET_TABLE except the five
    std columns (those receive explicit values in the Pass 2 INSERT).
    Called once during setup so build_pass2 works on any target table schema.
    """
    global _PASS2_COPY_COLS
    std = {'vital_name_std', 'vital_code_std', 'vital_coding_system_std',
           'vital_result_std', 'vital_unit_std'}
    schema, table = TARGET_TABLE.split(".")
    cur = conn.cursor()
    cur.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = %s AND table_name = %s "
        "ORDER BY ordinal_position",
        (schema, table),
    )
    _PASS2_COPY_COLS = [row[0] for row in cur.fetchall() if row[0] not in std]
    cur.close()
    print(f"  Pass 2 copy columns ({len(_PASS2_COPY_COLS)}): {', '.join(_PASS2_COPY_COLS)}")


def _build_ranges(cur, staging_pk, key_col=None):
    pk = key_col or BATCH_KEY
    cur.execute(f"SELECT COUNT(*) FROM {staging_pk}")
    total = cur.fetchone()[0]
    if total == 0:
        return [], 0

    cur.execute(f"""
        SELECT {pk}
        FROM (
            SELECT {pk},
                   ROW_NUMBER() OVER (ORDER BY {pk}) AS rn
            FROM {staging_pk}
        ) t
        WHERE (rn - 1) % {BATCH_SIZE} = 0
        ORDER BY {pk}
    """)
    boundaries = [row[0] for row in cur.fetchall() if row[0] is not None]

    cur.execute(f"SELECT MAX({pk}) FROM {staging_pk}")
    max_pk = int(cur.fetchone()[0])

    ranges = []
    for i, lo in enumerate(boundaries):
        hi = boundaries[i + 1] if i + 1 < len(boundaries) else max_pk + 1
        ranges.append((lo, hi))

    return ranges, total


# ── Batch UPDATE/INSERT builders ──────────────────────────────────────

def build_pass1(pk_lo, pk_hi):
    """Pass 1: UPDATE vital_name_std, vital_code_std, vital_coding_system_std via LOINC lookup."""
    return f"""
UPDATE {TARGET_TABLE} v
LEFT JOIN {STAGING_LOINC} l
    ON LOWER(REPLACE(TRIM(v.vital_name), ':', '')) = l.vital_name_clean
SET
    v.vital_name_std = CASE
        WHEN v.vital_name IS NULL OR TRIM(v.vital_name) = '' THEN NULL
        WHEN l.vital_name_std LIKE 'Blood pressure (Split Required)'
             AND REGEXP_REPLACE(
                     REGEXP_REPLACE(v.vital_result, '<[^>]*>', ''),
                     '[^0-9/]', ''
                 ) NOT REGEXP '^[0-9]+/[0-9]+$' THEN 'NS'
        WHEN l.vital_name_std IS NOT NULL THEN l.vital_name_std
        ELSE 'NS'
    END,
    v.vital_code_std = CASE
        WHEN v.vital_name IS NULL OR TRIM(v.vital_name) = '' THEN NULL
        WHEN l.vital_code_std IS NOT NULL THEN l.vital_code_std
        ELSE 'NS'
    END,
    v.vital_coding_system_std = CASE
        WHEN v.vital_name IS NULL OR TRIM(v.vital_name) = '' THEN NULL
        WHEN l.vital_coding_system_std IS NOT NULL THEN l.vital_coding_system_std
        ELSE 'NS'
    END
WHERE v.{BATCH_KEY} >= {pk_lo}
  AND v.{BATCH_KEY} <  {pk_hi}
"""


def build_pass2(pk_lo, pk_hi):
    """
    Pass 2: INSERT Systolic + Diastolic rows from pre-materialized STAGING_BP_CLEAN.
    STAGING_BP_CLEAN is built once in rebuild_pass2_staging (one scan of the 23M-row target),
    so each batch here is just a fast indexed range scan of the ~1.3M-row staging table.
    Previously the FROM subquery was repeated twice (UNION ALL), causing two full scans per batch.
    """
    copy_cols   = _PASS2_COPY_COLS
    col_list    = ", ".join(copy_cols)
    select_copy = ", ".join(f"v.{c}" for c in copy_cols)
    return f"""
INSERT INTO {TARGET_TABLE}
    ({col_list},
     vital_name_std, vital_code_std, vital_coding_system_std,
     vital_result_std, vital_unit_std)

SELECT {select_copy},
    'Systolic blood pressure', '8480-6', 'LOINC',
    TRIM(SUBSTRING_INDEX(v.vital_clean, '/', 1)), 'mmHg'
FROM {STAGING_BP_CLEAN} v
WHERE v.{BATCH_KEY} >= {pk_lo}
  AND v.{BATCH_KEY} <  {pk_hi}

UNION ALL

SELECT {select_copy},
    'Diastolic blood pressure', '8462-4', 'LOINC',
    TRIM(SUBSTRING_INDEX(v.vital_clean, '/', -1)), 'mmHg'
FROM {STAGING_BP_CLEAN} v
WHERE v.{BATCH_KEY} >= {pk_lo}
  AND v.{BATCH_KEY} <  {pk_hi}
"""



# Pass 3 pre-computation: INSERT into STAGING_PASS3_COMPUTED in ndid-range chunks.
# Chunked approach avoids the lock-timeout problem of a single giant CTAS on 21M rows.
# Tokens: __STAGING_P3C__, __TARGET__, __RK__ (vital_id), __BK__ (ndid), __LO__, __HI__.
# Uses '\\s+' (MySQL sees \s+) to correctly strip whitespace in normalized_height.
_PASS3_INSERT_SQL = r"""
INSERT INTO __STAGING_P3C__ (__RK__, vital_result_std, vital_unit_std)
SELECT
    __RK__,

    /* ── vital_result_std ─────────────────────────────────── */
    CASE
        /* 1. ft + in  (e.g. 5'11" or 5'11.5) */
        WHEN vital_name_std = 'Body height'
             AND normalized_height REGEXP "^[0-9]+'[0-9]+(\\.[0-9]+)?\"?$"
        THEN
            CAST(REGEXP_SUBSTR(normalized_height, '^[0-9]+') AS DOUBLE) * 12
          + CAST(REGEXP_SUBSTR(normalized_height, "(?<=')[0-9]+(\\.[0-9]+)?") AS DOUBLE)

        /* 2. ft + fraction  (e.g. 4'9 1/2") */
        WHEN vital_name_std = 'Body height'
             AND normalized_height REGEXP "^[0-9]+'[0-9]+[0-9]*/[0-9]+"
        THEN
            CAST(REGEXP_SUBSTR(normalized_height, '^[0-9]+') AS DOUBLE) * 12
          + CAST(REGEXP_SUBSTR(normalized_height, "(?<=')[0-9]+") AS DOUBLE)
          + (  CAST(REGEXP_SUBSTR(normalized_height, "[0-9]+(?=/)") AS DOUBLE)
             / CAST(REGEXP_SUBSTR(normalized_height, "(?<=/)[0-9]+") AS DOUBLE) )

        /* 3. feet only  (e.g. 5') */
        WHEN vital_name_std = 'Body height'
             AND normalized_height REGEXP "^[0-9]+'$"
        THEN CAST(REGEXP_SUBSTR(normalized_height, '[0-9]+') AS DOUBLE) * 12

        /* 4. inches only  (e.g. 66" or 74.5") */
        WHEN vital_name_std = 'Body height'
             AND normalized_height REGEXP '^[0-9]+(\\.[0-9]+)?\"$'
        THEN CAST(REGEXP_SUBSTR(normalized_height, '[0-9]+(\\.[0-9]+)?') AS DOUBLE)

        /* 5. fallback: bare number in cm range → convert to inches */
        WHEN vital_name_std = 'Body height'
             AND normalized_height REGEXP '^[0-9]+(\\.[0-9]+)?$'
             AND CAST(normalized_height AS DOUBLE) BETWEEN 100 AND 250
        THEN ROUND(CAST(normalized_height AS DOUBLE) / 2.54, 2)

        /* 6. height with explicit cm unit */
        WHEN vital_name_std = 'Body height'
             AND (LOWER(TRIM(vital_unit)) = 'cm' OR LOWER(vital_name) LIKE '%cm%')
            THEN ROUND(val / 2.54, 2)

            /* 7. height, no unit, value in cm range */
            WHEN vital_name_std = 'Body height'
                 AND (TRIM(vital_unit) = '' OR vital_unit IS NULL)
                 AND val BETWEEN 100 AND 250
            THEN ROUND(val / 2.54, 2)

            /* 8. weight in grams → lb */
            WHEN vital_name_std = 'Body weight'
                 AND (LOWER(vital_unit) = 'g' OR val > 1000)
            THEN ROUND(val / 453.59237, 2)

            /* 9. weight in kg → lb */
            WHEN vital_name_std = 'Body weight'
                 AND (LOWER(vital_unit) = 'kg' OR LOWER(vital_result) LIKE '%kg%' OR val < 80)
            THEN ROUND(val * 2.20462, 2)

            ELSE
                CASE
                    WHEN vital_name_std = 'Blood pressure (Split Required)' THEN 'NS'
                    ELSE val
                END
        END AS vital_result_std,

        /* ── vital_unit_std ───────────────────────────────────── */
        CASE
            WHEN vital_name_std = 'Body weight'
                 AND (   vital_unit IN ('g', 'kg', '[lb_av]')
                      OR val > 1000
                      OR LOWER(vital_result) LIKE '%kg%'
                      OR val < 80)
            THEN 'lb'

            WHEN vital_name_std = 'Body weight'
                 AND (TRIM(vital_unit) = '' OR vital_unit IS NULL)
                 AND val BETWEEN 150 AND 500
            THEN 'lb'

            WHEN vital_name_std = 'Weight-for-length Per age and sex'
                 AND vital_unit = '{percentile}'
            THEN 'percentile'

            WHEN vital_name_std = 'Body height'
                 AND (vital_unit IN ('[in_i]', 'cm') OR LOWER(vital_name) LIKE '%cm%')
            THEN 'in'

            WHEN vital_name_std = 'Body height'
                 AND cleaned_result REGEXP "('|ft|in|\")"
            THEN 'in'

            WHEN vital_name_std = 'Body height'
                 AND (TRIM(vital_unit) = '' OR vital_unit IS NULL)
                 AND val BETWEEN 100 AND 250
            THEN 'in'

            WHEN vital_name_std = 'Diastolic blood pressure'   THEN 'mmHg'
            WHEN vital_name_std = 'Systolic blood pressure'    THEN 'mmHg'

            WHEN vital_name_std = 'Body temperature'
                 AND vital_unit IN ('[degF]', 'F')
            THEN '°F'

            WHEN vital_name_std = 'Heart rate'                 THEN 'bpm'
            WHEN vital_name_std = 'Respiratory rate'           THEN 'breaths/min'

            WHEN vital_name_std = 'Body surface area'
                 AND vital_unit = 'm2'
            THEN 'm²'

            WHEN vital_name_std = 'Inhaled oxygen flow rate'   THEN 'L/min'

            WHEN vital_name_std = 'Inhaled oxygen concentration'
                 AND vital_unit = '%'
            THEN '%'

            WHEN vital_name_std = 'Oxygen saturation in Arterial blood by Pulse oximetry'
                 AND vital_unit = '%'
            THEN '%'

            ELSE vital_unit
        END AS vital_unit_std

    FROM (
        /* ── parsed_vitals: extract val + normalized_height ──── */
        SELECT
            __RK__,
            vital_name,
            vital_name_std,
            vital_result,
            vital_unit,
            cleaned_result,
            REGEXP_REPLACE(
                REPLACE(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(cleaned_result, 'feet|foot|ft', "'"),
                        'inches|inch|in', '"'
                    ),
                    "''", '"'
                ),
                '\\s+', ''
            ) AS normalized_height,
            CASE
                WHEN vital_name_std = 'Blood pressure (Split Required)'
                THEN 'NS'
                ELSE CAST(
                    NULLIF(
                        REGEXP_SUBSTR(
                            REGEXP_REPLACE(
                                LOWER(CASE
                                    WHEN vital_result REGEXP '<[^>]+>'
                                    THEN REGEXP_REPLACE(vital_result, '<[^>]*>', '')
                                    ELSE vital_result
                                END),
                                '[^0-9.]', ''
                            ),
                            '[0-9]+(\\.[0-9]+)?'
                        ), ''
                    ) AS DOUBLE
                )
            END AS val
        FROM (
            /* ── cleaned_vitals: strip HTML entities ─────────── */
            SELECT
                __RK__,
                vital_name,
                vital_name_std,
                vital_result,
                vital_unit,
                TRIM(LOWER(REGEXP_REPLACE(
                    REPLACE(REPLACE(vital_result, '&apos;', "'"), '&quot;', '"'),
                    '<[^>]*>', ''
                ))) AS cleaned_result
            FROM __TARGET__
            WHERE vital_name_std IS NOT NULL
              AND vital_name_std NOT IN ('Systolic blood pressure', 'Diastolic blood pressure')
              AND __BK__ >= __LO__
              AND __BK__ <  __HI__
        ) cleaned
    ) parsed
"""


def build_pass3(pk_lo, pk_hi):
    """
    Pass 3: UPDATE vital_result_std, vital_unit_std from pre-computed staging table.
    Simple indexed JOIN on ROW_KEY (vital_id) — correct one-to-one semantics,
    no per-batch REGEXP/CASE computation (all done once in rebuild_pass3_staging).
    """
    return f"""
UPDATE {TARGET_TABLE} v
JOIN {STAGING_PASS3_COMPUTED} c ON v.{ROW_KEY} = c.{ROW_KEY}
SET v.vital_result_std = c.vital_result_std,
    v.vital_unit_std   = c.vital_unit_std
WHERE c.{ROW_KEY} >= {pk_lo}
  AND c.{ROW_KEY} <  {pk_hi}
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
            status       = VALUES(status),
            rows_affected = VALUES(rows_affected),
            completed_at = IF(VALUES(status) = 'done', NOW(), NULL),
            error_msg    = VALUES(error_msg)
    """, (checkpoint_key, status, rows, status, error))
    conn.commit()
    cur.close()


# ── DDL: ensure std columns exist ─────────────────────────────────────

def ensure_std_columns():
    std_cols = [
        ("vital_name_std",         "VARCHAR(200)"),
        ("vital_code_std",         "VARCHAR(20)"),
        ("vital_coding_system_std","VARCHAR(20)"),
        ("vital_result_std",       "VARCHAR(500)"),
        ("vital_unit_std",         "VARCHAR(60)"),
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


# ── Setup ──────────────────────────────────────────────────────────────

def setup_tables():
    ensure_std_columns()

    conn = get_connection()
    _load_pass2_copy_cols(conn)   # must run after ensure_std_columns so std cols exist
    cur  = conn.cursor()
    cur.execute("SET SESSION lock_wait_timeout = 3600")
    cur.execute("SET SESSION innodb_lock_wait_timeout = 3600")

    # ── 1. LOINC lookup (semantics.vitals_loinc) ──────────────────────
    print("  Materializing semantics.vitals_loinc lookup...")
    if not _table_exists(cur, STAGING_LOINC):
        cur.execute(f"""
            CREATE TABLE {STAGING_LOINC} AS
            SELECT
                vital_name,
                vital_name_std,
                vital_code_std,
                vital_coding_system_std,
                LOWER(TRIM(vital_name)) AS vital_name_clean
            FROM semantics.vitals_loinc
        """)
        cur.execute(f"ALTER TABLE {STAGING_LOINC} ADD INDEX idx_clean (vital_name_clean(100))")
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")
    cur.execute(f"SELECT COUNT(*) FROM {STAGING_LOINC}")
    print(f"    {cur.fetchone()[0]:,} rows")

    # ── 2. PK staging — Pass 1: all rows ──────────────────────────────
    print("  Creating PK staging for Pass 1 (all rows)...")
    if not _table_exists(cur, STAGING_PK_PASS1):
        cur.execute(f"""
            CREATE TABLE {STAGING_PK_PASS1} AS
            SELECT {BATCH_KEY}
            FROM {TARGET_TABLE}
            WHERE {BATCH_KEY} IS NOT NULL
        """)
        cur.execute(f"ALTER TABLE {STAGING_PK_PASS1} ADD INDEX idx_pk ({BATCH_KEY})")
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")
    ranges_p1, total_p1 = _build_ranges(cur, STAGING_PK_PASS1)
    print(f"    {total_p1:,} rows → {len(ranges_p1)} batches")

    # ── 3. Pass 2 PK staging — deferred until after Pass 1 ────────────
    #    vital_name_std = 'Blood pressure (Split Required)' is only set after Pass 1
    ranges_p2 = []   # rebuilt in rebuild_pass2_staging()

    # ── 4. Checkpoint table ────────────────────────────────────────────
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

    # ── 5. Ensure required indexes exist on the target table ──────────────
    # ndid   — Pass 1/2/3 batch WHERE clauses
    # vital_name — Pass 1 JOIN key
    # vital_id   — Pass 3 JOIN key (STAGING_PASS3_COMPUTED → TARGET_TABLE)
    #              Without this, every Pass 3 batch does a full 23M-row scan.
    tgt_schema, tgt_table = TARGET_TABLE.split(".")
    for col, idx_name in [
        (BATCH_KEY, f"idx_{BATCH_KEY}"),
        ("vital_name", "idx_vital_name"),
        (ROW_KEY,   f"idx_{ROW_KEY}"),
    ]:
        if not _index_exists(cur, tgt_schema, tgt_table, col):
            length = "(100)" if col == "vital_name" else ""
            print(f"  Adding index {idx_name} on {TARGET_TABLE}({col}) — may take a few minutes on large tables...")
            cur.execute(f"ALTER TABLE {TARGET_TABLE} ADD INDEX {idx_name} ({col}{length})")
            conn.commit()
            print(f"    done")
        else:
            print(f"  Index on {TARGET_TABLE}({col}) already exists.")

    cur.close()
    conn.close()

    return {
        CHECKPOINT_PASS1: ranges_p1,
        CHECKPOINT_PASS2: ranges_p2,
        CHECKPOINT_PASS3: [],          # built after Pass 2 (depends on vital_name_std)
    }


def rebuild_pass2_staging(all_ranges):
    """
    Build Pass 2 staging tables after Pass 1 has set vital_name_std.
    STAGING_BP_CLEAN pre-computes vital_clean once (one scan of the 23M-row target),
    eliminating the double-scan that caused ~12s/batch in the old approach.
    """
    print("\n  Rebuilding Pass 2 staging (Blood pressure rows)...")
    conn = get_connection()
    cur  = conn.cursor()
    cur.execute("SET SESSION lock_wait_timeout = 3600")
    cur.execute("SET SESSION innodb_lock_wait_timeout = 3600")

    # PK staging: ndid values of eligible BP rows (for batch range calculation)
    if not _table_exists(cur, STAGING_PK_PASS2):
        cur.execute(f"""
            CREATE TABLE {STAGING_PK_PASS2} AS
            SELECT {BATCH_KEY}
            FROM {TARGET_TABLE}
            WHERE vital_name_std = 'Blood pressure (Split Required)'
              AND vital_result LIKE '%/%'
              AND REGEXP_REPLACE(
                      REGEXP_REPLACE(vital_result, '<[^>]*>', ''),
                      '[^0-9/]', ''
                  ) REGEXP '^[0-9]+/[0-9]+$'
              AND {BATCH_KEY} IS NOT NULL
        """)
        cur.execute(f"ALTER TABLE {STAGING_PK_PASS2} ADD INDEX idx_pk ({BATCH_KEY})")
        conn.commit()
        print("    PK staging created")
    else:
        print("    PK staging already exists, reusing")

    # BP clean staging: full BP rows with vital_clean pre-computed
    # build_pass2 reads from here instead of scanning TARGET_TABLE per batch
    if not _table_exists(cur, STAGING_BP_CLEAN):
        col_list = ", ".join(f"t.{c}" for c in _PASS2_COPY_COLS)
        print("    Building BP clean staging (one-time scan to pre-compute vital_clean)...")
        cur.execute(f"""
            CREATE TABLE {STAGING_BP_CLEAN} AS
            SELECT {col_list},
                REGEXP_REPLACE(
                    REGEXP_REPLACE(t.vital_result, '<[^>]*>', ''),
                    '[^0-9/]', ''
                ) AS vital_clean
            FROM {TARGET_TABLE} t
            WHERE t.vital_name_std = 'Blood pressure (Split Required)'
              AND t.vital_result LIKE '%/%'
              AND REGEXP_REPLACE(
                      REGEXP_REPLACE(t.vital_result, '<[^>]*>', ''),
                      '[^0-9/]', ''
                  ) REGEXP '^[0-9]+/[0-9]+$'
              AND t.{BATCH_KEY} IS NOT NULL
        """)
        cur.execute(f"ALTER TABLE {STAGING_BP_CLEAN} ADD INDEX idx_{BATCH_KEY} ({BATCH_KEY})")
        conn.commit()
        print("    BP clean staging created")
    else:
        print("    BP clean staging already exists, reusing")

    ranges_p2, total_p2 = _build_ranges(cur, STAGING_PK_PASS2)
    print(f"    {total_p2:,} rows → {len(ranges_p2)} batches")
    cur.close()
    conn.close()
    all_ranges[CHECKPOINT_PASS2] = ranges_p2
    return ranges_p2


def rebuild_pass3_staging(all_ranges):
    """
    Build Pass 3 staging after Pass 2.
    Pre-computes vital_result_std / vital_unit_std for all eligible rows into
    STAGING_PASS3_COMPUTED using short ndid-range INSERT chunks (avoids the lock-timeout
    that a single CTAS on 21M rows triggers under concurrent locks).
    Each batch UPDATE then does a simple indexed JOIN on ROW_KEY (vital_id).
    """
    print("\n  Rebuilding Pass 3 staging (pre-computing result/unit std)...")
    conn = get_connection()
    cur  = conn.cursor()
    cur.execute("SET SESSION lock_wait_timeout = 3600")
    cur.execute("SET SESSION innodb_lock_wait_timeout = 3600")

    if not _table_exists(cur, STAGING_PASS3_COMPUTED):
        # Create empty table first — avoids long CTAS transaction
        cur.execute(f"""
            CREATE TABLE {STAGING_PASS3_COMPUTED} (
                {ROW_KEY}        BIGINT       NOT NULL,
                vital_result_std VARCHAR(500) DEFAULT NULL,
                vital_unit_std   VARCHAR(60)  DEFAULT NULL
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """)
        conn.commit()

        # Reuse STAGING_PK_PASS1 ndid boundaries for chunked INSERT
        cur.execute(f"SELECT COUNT(*) FROM {STAGING_PK_PASS1}")
        total_ndids = cur.fetchone()[0]
        cur.execute(f"""
            SELECT {BATCH_KEY}
            FROM (
                SELECT {BATCH_KEY}, ROW_NUMBER() OVER (ORDER BY {BATCH_KEY}) AS rn
                FROM {STAGING_PK_PASS1}
            ) t WHERE (rn - 1) % {BATCH_SIZE} = 0
            ORDER BY {BATCH_KEY}
        """)
        bounds = [row[0] for row in cur.fetchall()]
        cur.execute(f"SELECT MAX({BATCH_KEY}) FROM {STAGING_PK_PASS1}")
        max_bk = int(cur.fetchone()[0])
        chunks = [(bounds[i], bounds[i + 1] if i + 1 < len(bounds) else max_bk + 1)
                  for i in range(len(bounds))]

        insert_template = (
            _PASS3_INSERT_SQL
            .replace("__STAGING_P3C__", STAGING_PASS3_COMPUTED)
            .replace("__TARGET__",      TARGET_TABLE)
            .replace("__RK__",          ROW_KEY)
            .replace("__BK__",          BATCH_KEY)
        )
        print(f"    Inserting pre-computed values in {len(chunks)} chunks...")
        for lo, hi in tqdm(chunks, desc="P3 precompute", unit="chunk"):
            sql = insert_template.replace("__LO__", str(lo)).replace("__HI__", str(hi))
            cur.execute(sql)
            conn.commit()

        cur.execute(f"ALTER TABLE {STAGING_PASS3_COMPUTED} ADD INDEX idx_{ROW_KEY} ({ROW_KEY})")
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")

    ranges_p3, total_p3 = _build_ranges(cur, STAGING_PASS3_COMPUTED, key_col=ROW_KEY)
    print(f"    {total_p3:,} rows → {len(ranges_p3)} batches")
    cur.close()
    conn.close()
    all_ranges[CHECKPOINT_PASS3] = ranges_p3
    return ranges_p3


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
        cur.execute("SET SESSION innodb_lock_wait_timeout = 3600")

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
    print(f"  Vitals Standardisation UPDATE — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  target     : {TARGET_TABLE}")
    print(f"  batch_key  : {BATCH_KEY}")
    print(f"  batch_size : {BATCH_SIZE:,}")
    print(f"  passes     : 3  (LOINC lookup UPDATE | BP split INSERT | result/unit std UPDATE)")
    print(f"{'='*70}\n", flush=True)

    print("  Connecting to database...")
    sys.stdout.flush()
    all_ranges = setup_tables()

    passes_1 = [
        (CHECKPOINT_PASS1, "Pass 1 — LOINC lookup UPDATE (all rows)", build_pass1),
    ]

    results    = {}
    any_failed = False

    # ── Pass 1 ────────────────────────────────────────────────────────
    total_batches_p1 = len(all_ranges.get(CHECKPOINT_PASS1, []))
    with tqdm(total=total_batches_p1, desc="Pass 1", unit="batch") as pbar:
        for ck, label, build_fn in passes_1:
            ranges = all_ranges.get(ck, [])
            if not ranges:
                print(f"\n  [SKIP] {label} — no eligible rows")
                continue
            print(f"\n  Starting {label} ({len(ranges)} batches)...")
            result = run_pass(ck, build_fn, ranges, pbar)
            results[ck] = result
            if result["status"].startswith("FAILED"):
                print(f"\n  FAILED at {label}: {result['status']}")
                any_failed = True

    # ── Pass 2 (deferred staging) ──────────────────────────────────────
    if not any_failed:
        if is_done(get_connection(), CHECKPOINT_PASS2):
            print(f"\n  [SKIP] Pass 2 — BP split INSERT — already done (checkpoint)")
            results[CHECKPOINT_PASS2] = {"status": "skipped", "rows": 0, "secs": 0}
        else:
            ranges_p2 = rebuild_pass2_staging(all_ranges)
            if not ranges_p2:
                print(f"\n  [SKIP] Pass 2 — BP split INSERT — no Blood pressure rows found")
            else:
                with tqdm(total=len(ranges_p2), desc="Pass 2", unit="batch") as pbar2:
                    print(f"\n  Starting Pass 2 — BP split INSERT ({len(ranges_p2)} batches)...")
                    result2 = run_pass(CHECKPOINT_PASS2, build_pass2, ranges_p2, pbar2)
                    results[CHECKPOINT_PASS2] = result2
                    if result2["status"].startswith("FAILED"):
                        print(f"\n  FAILED at Pass 2: {result2['status']}")
                        any_failed = True

    # ── Pass 3 (deferred staging — depends on Pass 1 + Pass 2 output) ──
    if not any_failed:
        if is_done(get_connection(), CHECKPOINT_PASS3):
            print(f"\n  [SKIP] Pass 3 — result/unit std UPDATE — already done (checkpoint)")
            results[CHECKPOINT_PASS3] = {"status": "skipped", "rows": 0, "secs": 0}
        else:
            ranges_p3 = rebuild_pass3_staging(all_ranges)
            if not ranges_p3:
                print(f"\n  [SKIP] Pass 3 — result/unit std UPDATE — no eligible rows found")
            else:
                with tqdm(total=len(ranges_p3), desc="Pass 3", unit="batch") as pbar3:
                    print(f"\n  Starting Pass 3 — result/unit std UPDATE ({len(ranges_p3)} batches)...")
                    result3 = run_pass(CHECKPOINT_PASS3, build_pass3, ranges_p3, pbar3)
                    results[CHECKPOINT_PASS3] = result3
                    if result3["status"].startswith("FAILED"):
                        print(f"\n  FAILED at Pass 3: {result3['status']}")
                        any_failed = True

    # ── Summary ───────────────────────────────────────────────────────
    all_passes = [
        (CHECKPOINT_PASS1, "Pass 1 — LOINC lookup UPDATE (all rows)"),
        (CHECKPOINT_PASS2, "Pass 2 — BP split INSERT (Blood pressure rows)"),
        (CHECKPOINT_PASS3, "Pass 3 — result/unit std UPDATE (all non-BP-split rows)"),
    ]
    print(f"\n{'='*70}")
    print(f"  Per-pass summary:")
    total_rows = 0
    for ck, label in all_passes:
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
    print(f"    -- Shared lookup (only drop when done with ALL vitals tables):")
    print(f"    -- DROP TABLE IF EXISTS {STAGING_LOINC};")
    print(f"    -- Per-run tables:")
    print(f"    DROP TABLE IF EXISTS {STAGING_PK_PASS1};")
    print(f"    DROP TABLE IF EXISTS {STAGING_PK_PASS2};")
    print(f"    DROP TABLE IF EXISTS {STAGING_BP_CLEAN};")
    print(f"    DROP TABLE IF EXISTS {STAGING_PASS3_COMPUTED};")
    print(f"    DROP TABLE IF EXISTS {CHECKPOINT_TABLE};")
    print(f"    -- If Pass 2 failed mid-run and you need to re-run, delete partial inserts first:")
    print(f"    -- DELETE FROM {TARGET_TABLE}")
    print(f"    --   WHERE vital_name_std IN ('Systolic blood pressure','Diastolic blood pressure')")
    print(f"    --   AND udm_inc_id = 0;")

    if any_failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
