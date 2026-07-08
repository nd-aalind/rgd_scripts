#!/usr/bin/env python3
"""
Optimized batched standardisation UPDATEs for: rgd_udm_silver.vitals_dedup

Two passes, each parallelized across workers via ThreadPoolExecutor:

  Pass 1 — All rows:
    SET vital_name_std, vital_code_std, vital_coding_system_std
    JOIN semantics.vitals_loinc — pre-materialized once

  Pass 2 — WHERE vital_name_std IS NOT NULL:
    SET vital_result_std, vital_unit_std
    Complex height/weight/unit conversion (no JOIN)
    PK staging built AFTER Pass 1 so newly-set rows are captured.

Workers split the batch list equally — no psid scoping needed.
Non-overlapping udm_inc_id ranges → zero lock contention between workers.

Optimizations:
- LOINC lookup pre-materialized once (171 rows, indexed)
- PK staging pre-filters eligible rows (one full scan only)
- Server-side boundary sampling (sparse-ID safe)
- Workers get non-overlapping ranges → no row-level lock contention
- Commit after every batch
- Checkpoint/resume per (pass, worker)
- InnoDB checks disabled per-session
- Progress bar via tqdm

Usage:
    python vitals_final_stand_opt.py
"""

import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
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
    "database":        "rgd_udm_silver",
    "charset":         "utf8mb4",
    "connect_timeout": 30,
    "read_timeout":    21600,
    "write_timeout":   21600,
}

BATCH_SIZE  = 50_000
MAX_WORKERS = 4        # parallel workers — tune based on DB server capacity

# ── Change this to run against a different vitals table ───────────────
TARGET_TABLE = "rgd_udm_silver.vitals_dedup"

# ─────────────────────────────────────────────────────────────────────
_TABLE_SUFFIX = TARGET_TABLE.replace(".", "_").replace("-", "_")

STAGING_LOINC    = "staging.vitals_std_loinc"
STAGING_PK_P1    = f"staging.vitals_std_pk1n_{_TABLE_SUFFIX}"
STAGING_PK_P2    = f"staging.vitals_std_pk2n_{_TABLE_SUFFIX}"
CHECKPOINT_TABLE = f"staging.etl_checkpoint_vitals_stdn_{_TABLE_SUFFIX}"

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


def _build_all_ranges(cur, staging_table):
    """Build all (lo, hi) batch ranges from a PK staging table."""
    cur.execute(f"SELECT COUNT(*) FROM {staging_table}")
    total = cur.fetchone()[0]
    if total == 0:
        return [], 0

    cur.execute(f"""
        SELECT {BATCH_KEY}
        FROM (
            SELECT {BATCH_KEY},
                   ROW_NUMBER() OVER (ORDER BY {BATCH_KEY}) AS rn
            FROM {staging_table}
        ) t
        WHERE (rn - 1) % {BATCH_SIZE} = 0
        ORDER BY {BATCH_KEY}
    """)
    boundaries = [row[0] for row in cur.fetchall() if row[0] is not None]

    cur.execute(f"SELECT MAX({BATCH_KEY}) FROM {staging_table}")
    max_pk = int(cur.fetchone()[0])

    ranges = []
    for i, lo in enumerate(boundaries):
        hi = boundaries[i + 1] if i + 1 < len(boundaries) else max_pk + 1
        ranges.append((lo, hi))

    return ranges, total


def _split_chunks(ranges, n):
    """Split ranges into n roughly equal chunks for workers."""
    size = (len(ranges) + n - 1) // n
    return [ranges[i: i + size] for i in range(0, len(ranges), size)]


# ── Batch UPDATE builders ─────────────────────────────────────────────

def build_pass1(pk_lo, pk_hi):
    """Pass 1: LOINC lookup — vital_name_std, vital_code_std, vital_coding_system_std."""
    return f"""
UPDATE {TARGET_TABLE} v
LEFT JOIN {STAGING_LOINC} l
       ON LOWER(REPLACE(TRIM(v.vital_name), ':', '')) = l.vital_name_key
SET
    v.vital_name_std = CASE
        WHEN v.vital_name IS NULL OR TRIM(v.vital_name) = '' THEN NULL
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
  AND v.{BATCH_KEY} < {pk_hi}
"""


# Pass 2 uses a raw string — avoids f-string escaping issues with
# MySQL regex patterns and quoted strings inside double-quoted strings.
# Tokens: __TARGET__, __BK__, __LO__, __HI__

_PASS2_SQL = r"""
UPDATE __TARGET__ v
JOIN (
    SELECT
        p.__BK__,

        CASE
            /* 1. ft + in (normal + decimal): 5'11" or 5'11.5" */
            WHEN p.vital_name_std = 'Body height'
                AND p.normalized_height REGEXP "^[0-9]+'[0-9]+(\.[0-9]+)?\"?$"
            THEN
                CAST(REGEXP_SUBSTR(p.normalized_height, '^[0-9]+') AS DOUBLE) * 12
                + CAST(REGEXP_SUBSTR(p.normalized_height, "(?<=')[0-9]+(\.[0-9]+)?") AS DOUBLE)

            /* 2. ft + fraction: 4'9 1/2" */
            WHEN p.vital_name_std = 'Body height'
                AND p.normalized_height REGEXP "^[0-9]+'[0-9]+[0-9]*/[0-9]+"
            THEN
                CAST(REGEXP_SUBSTR(p.normalized_height, '^[0-9]+') AS DOUBLE) * 12
                + CAST(REGEXP_SUBSTR(p.normalized_height, "(?<=')[0-9]+") AS DOUBLE)
                + (
                    CAST(REGEXP_SUBSTR(p.normalized_height, "[0-9]+(?=/)") AS DOUBLE)
                    / CAST(REGEXP_SUBSTR(p.normalized_height, "(?<=/)[0-9]+") AS DOUBLE)
                  )

            /* 3. only feet: 5' */
            WHEN p.vital_name_std = 'Body height'
                AND p.normalized_height REGEXP "^[0-9]+'$"
            THEN CAST(REGEXP_SUBSTR(p.normalized_height, '[0-9]+') AS DOUBLE) * 12

            /* 4. inches only: 66" or 74" */
            WHEN p.vital_name_std = 'Body height'
                AND p.normalized_height REGEXP '^[0-9]+(\.[0-9]+)?"$'
            THEN CAST(REGEXP_SUBSTR(p.normalized_height, '[0-9]+(\.[0-9]+)?') AS DOUBLE)

            /* 5. plain number in cm range → convert to inches */
            WHEN p.vital_name_std = 'Body height'
                AND p.normalized_height REGEXP '^[0-9]+(\.[0-9]+)?$'
                AND CAST(p.normalized_height AS DOUBLE) BETWEEN 100 AND 250
            THEN ROUND(CAST(p.normalized_height AS DOUBLE) / 2.54, 2)

            /* HEIGHT explicit cm unit */
            WHEN p.vital_name_std = 'Body height'
                AND (LOWER(TRIM(p.vital_unit)) = 'cm' OR LOWER(p.vital_name) LIKE '%cm%')
            THEN ROUND(p.val / 2.54, 2)

            /* HEIGHT no unit, value in cm range */
            WHEN p.vital_name_std = 'Body height'
                AND (TRIM(p.vital_unit) = '' OR p.vital_unit IS NULL)
                AND p.val BETWEEN 100 AND 250
            THEN ROUND(p.val / 2.54, 2)

            /* WEIGHT g → lb */
            WHEN p.vital_name_std = 'Body weight'
                AND (LOWER(p.vital_unit) = 'g' OR p.val > 1000)
            THEN ROUND(p.val / 453.59237, 2)

            /* WEIGHT kg → lb */
            WHEN p.vital_name_std = 'Body weight'
                AND (LOWER(p.vital_unit) = 'kg' OR LOWER(p.vital_result) LIKE '%kg%' OR p.val < 80)
            THEN ROUND(p.val * 2.20462, 2)

            ELSE
                CASE
                    WHEN p.vital_name_std = 'Blood pressure (Split Required)' THEN 'NS'
                    ELSE p.val
                END
        END AS vital_result_std,

        CASE
            WHEN p.vital_name_std = 'Body weight'
                AND (
                    p.vital_unit IN ('g', 'kg', '[lb_av]')
                    OR p.val > 1000
                    OR LOWER(p.vital_result) LIKE '%kg%'
                    OR p.val < 80
                )
            THEN 'lb'

            WHEN p.vital_name_std = 'Body weight'
                AND (TRIM(p.vital_unit) = '' OR p.vital_unit IS NULL)
                AND p.val BETWEEN 150 AND 500
            THEN 'lb'

            WHEN p.vital_name_std = 'Weight-for-length Per age and sex'
                AND p.vital_unit = '{percentile}'
            THEN 'percentile'

            WHEN p.vital_name_std = 'Body height'
                AND (p.vital_unit IN ('[in_i]', 'cm') OR LOWER(p.vital_name) LIKE '%cm%')
            THEN 'in'

            WHEN p.vital_name_std = 'Body height'
                AND p.cleaned_result REGEXP "('|ft|in|\")"
            THEN 'in'

            WHEN p.vital_name_std = 'Body height'
                AND (TRIM(p.vital_unit) = '' OR p.vital_unit IS NULL)
                AND p.val BETWEEN 100 AND 250
            THEN 'in'

            WHEN p.vital_name_std = 'Diastolic blood pressure' THEN 'mmHg'
            WHEN p.vital_name_std = 'Systolic blood pressure'  THEN 'mmHg'

            WHEN p.vital_name_std = 'Body temperature'
                AND p.vital_unit IN ('[degF]', 'F')
            THEN '°F'

            WHEN p.vital_name_std = 'Heart rate'           THEN 'bpm'
            WHEN p.vital_name_std = 'Respiratory rate'     THEN 'breaths/min'

            WHEN p.vital_name_std = 'Body surface area'
                AND p.vital_unit = 'm2'
            THEN 'm²'

            WHEN p.vital_name_std = 'Inhaled oxygen flow rate' THEN 'L/min'

            WHEN p.vital_name_std = 'Inhaled oxygen concentration'
                AND p.vital_unit = '%'
            THEN '%'

            WHEN p.vital_name_std = 'Oxygen saturation in Arterial blood by Pulse oximetry'
                AND p.vital_unit = '%'
            THEN '%'

            ELSE p.vital_unit
        END AS vital_unit_std

    FROM (
        SELECT
            c.__BK__,
            c.vital_name,
            c.vital_name_std,
            c.vital_result,
            c.vital_unit,
            c.cleaned_result,

            REGEXP_REPLACE(
                REPLACE(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(c.cleaned_result, 'feet|foot|ft', "'"),
                        'inches|inch|in', '"'
                    ),
                    "''", '"'
                ),
                '\\s+', ''
            ) AS normalized_height,

            CAST(
                NULLIF(
                    REGEXP_SUBSTR(
                        REGEXP_REPLACE(
                            LOWER(
                                CASE
                                    WHEN c.vital_result REGEXP '<[^>]+>'
                                        THEN REGEXP_REPLACE(c.vital_result, '<[^>]*>', '')
                                    ELSE c.vital_result
                                END
                            ),
                            '[^0-9.]', ''
                        ),
                        '[0-9]+(\.[0-9]+)?'
                    ),
                    ''
                ) AS DOUBLE
            ) AS val

        FROM (
            SELECT
                __BK__,
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
              AND __BK__ >= __LO__ AND __BK__ < __HI__
        ) c
    ) p
) comp ON v.__BK__ = comp.__BK__
SET
    v.vital_result_std = comp.vital_result_std,
    v.vital_unit_std   = comp.vital_unit_std
"""


def build_pass2(pk_lo, pk_hi):
    """Pass 2: height/weight/unit conversion — vital_result_std, vital_unit_std."""
    return (
        _PASS2_SQL
        .replace("__TARGET__", TARGET_TABLE)
        .replace("__BK__",     BATCH_KEY)
        .replace("__LO__",     str(pk_lo))
        .replace("__HI__",     str(pk_hi))
    )


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

def ensure_std_columns():
    std_cols = [
        ("vital_name_std",          "VARCHAR(200)"),
        ("vital_code_std",          "VARCHAR(50)"),
        ("vital_coding_system_std", "VARCHAR(50)"),
        ("vital_result_std",        "VARCHAR(500)"),
        ("vital_unit_std",          "VARCHAR(100)"),
    ]
    print(f"  Checking std columns on {TARGET_TABLE}...")
    ddl_conn = get_connection()
    ddl_cur  = ddl_conn.cursor()
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
    cur  = conn.cursor()

    # ── LOINC lookup ──────────────────────────────────────────────────
    print("  Materializing semantics.vitals_loinc lookup...")
    if not _table_exists(cur, STAGING_LOINC):
        cur.execute(f"""
            CREATE TABLE {STAGING_LOINC} AS
            SELECT
                vital_name,
                LOWER(TRIM(vital_name))   AS vital_name_key,
                vital_name_std,
                vital_code_std,
                vital_coding_system_std
            FROM semantics.vitals_loinc
        """)
        cur.execute(f"ALTER TABLE {STAGING_LOINC} ADD INDEX idx_name_key (vital_name_key(100))")
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")
    cur.execute(f"SELECT COUNT(*) FROM {STAGING_LOINC}")
    print(f"    {cur.fetchone()[0]:,} rows")

    # ── Pass 1 PK staging — one full scan, no psid filter ─────────────
    print("  Creating Pass 1 PK staging (all rows)...")
    if not _table_exists(cur, STAGING_PK_P1):
        cur.execute(f"""
            CREATE TABLE {STAGING_PK_P1} AS
            SELECT {BATCH_KEY}
            FROM {TARGET_TABLE}
            WHERE {BATCH_KEY} IS NOT NULL
        """)
        cur.execute(f"ALTER TABLE {STAGING_PK_P1} ADD INDEX idx_pk ({BATCH_KEY})")
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")
    ranges_p1, total_p1 = _build_all_ranges(cur, STAGING_PK_P1)
    print(f"    {total_p1:,} rows → {len(ranges_p1)} batches")

    # ── Checkpoint table ──────────────────────────────────────────────
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

    cur.close()
    conn.close()
    return ranges_p1, total_p1


def build_p2_staging():
    """Called AFTER Pass 1 — captures newly-set vital_name_std rows."""
    conn = get_connection()
    cur  = conn.cursor()
    print("  Creating Pass 2 PK staging (vital_name_std IS NOT NULL)...")
    if not _table_exists(cur, STAGING_PK_P2):
        cur.execute(f"""
            CREATE TABLE {STAGING_PK_P2} AS
            SELECT {BATCH_KEY}
            FROM {TARGET_TABLE}
            WHERE vital_name_std IS NOT NULL
              AND {BATCH_KEY} IS NOT NULL
        """)
        cur.execute(f"ALTER TABLE {STAGING_PK_P2} ADD INDEX idx_pk ({BATCH_KEY})")
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")
    ranges_p2, total_p2 = _build_all_ranges(cur, STAGING_PK_P2)
    print(f"    {total_p2:,} rows → {len(ranges_p2)} batches")
    cur.close()
    conn.close()
    return ranges_p2, total_p2


# ── Worker ─────────────────────────────────────────────────────────────

def run_worker(worker_id, pass_num, build_fn, ranges_chunk, pbar):
    """Process one chunk of batches. Each worker has its own DB connection."""
    ck_key = f"vitals.std.pass{pass_num}.worker{worker_id}.{_TABLE_SUFFIX}"
    conn   = get_connection()

    if is_done(conn, ck_key):
        conn.close()
        pbar.update(len(ranges_chunk))
        return {"worker": worker_id, "pass": pass_num,
                "status": "skipped", "rows": 0, "secs": 0}

    mark(conn, ck_key, "running")
    t0         = time.time()
    total_rows = 0

    try:
        cur = conn.cursor()
        cur.execute("SET unique_checks = 0")
        cur.execute("SET foreign_key_checks = 0")

        for pk_lo, pk_hi in ranges_chunk:
            sql = build_fn(pk_lo, pk_hi)
            cur.execute(sql)
            conn.commit()
            total_rows += cur.rowcount
            pbar.update(1)

        cur.execute("SET unique_checks = 1")
        cur.execute("SET foreign_key_checks = 1")
        cur.close()

        elapsed = round(time.time() - t0, 1)
        mark(conn, ck_key, "done", total_rows)
        conn.close()
        return {"worker": worker_id, "pass": pass_num,
                "status": "done", "rows": total_rows, "secs": elapsed}

    except Exception as exc:
        elapsed = round(time.time() - t0, 1)
        mark(conn, ck_key, "failed", total_rows, str(exc))
        try:
            conn.close()
        except Exception:
            pass
        return {"worker": worker_id, "pass": pass_num,
                "status": f"FAILED: {exc}", "rows": total_rows, "secs": elapsed}


def run_pass(pass_num, build_fn, all_ranges, label):
    """Split ranges into worker chunks and run in parallel."""
    chunks  = _split_chunks(all_ranges, MAX_WORKERS)
    results = []
    any_failed = False

    print(f"\n  ── {label} ──")
    print(f"     {len(all_ranges)} batches  ×  {BATCH_SIZE:,} rows/batch  →  {MAX_WORKERS} workers")

    with tqdm(total=len(all_ranges), desc=f"Pass {pass_num}", unit="batch") as pbar:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
            futures = {
                pool.submit(run_worker, i, pass_num, build_fn, chunks[i], pbar): i
                for i in range(len(chunks))
            }
            for future in as_completed(futures):
                res = future.result()
                results.append(res)
                if res["status"].startswith("FAILED"):
                    any_failed = True

    done    = sum(1 for r in results if r["status"] == "done")
    skipped = sum(1 for r in results if r["status"] == "skipped")
    rows    = sum(r["rows"] for r in results)
    print(f"     workers: {done} done, {skipped} skipped  |  rows updated: {rows:,}")

    return results, any_failed


# ── Main ───────────────────────────────────────────────────────────────

def main():
    print(f"\n{'='*70}", flush=True)
    print(f"  Vitals Standardisation UPDATE — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  target     : {TARGET_TABLE}")
    print(f"  batch_key  : {BATCH_KEY}")
    print(f"  batch_size : {BATCH_SIZE:,}")
    print(f"  workers    : {MAX_WORKERS}")
    print(f"  passes     : 2  (LOINC name/code lookup | result/unit conversion)")
    print(f"{'='*70}\n", flush=True)

    ranges_p1, total_p1 = setup_tables()

    if not ranges_p1:
        print("  No rows to process. Exiting.")
        return

    # ── Pass 1 ────────────────────────────────────────────────────────
    p1_results, p1_failed = run_pass(
        1, build_pass1, ranges_p1,
        f"Pass 1 — LOINC lookup  ({total_p1:,} rows)"
    )

    if p1_failed:
        print("\n  Pass 1 had failures — aborting.")
        sys.exit(1)

    # ── Pass 2 PK staging (built after Pass 1 sets vital_name_std) ────
    ranges_p2, total_p2 = build_p2_staging()

    if not ranges_p2:
        print("\n  Pass 2: no rows with vital_name_std set — skipping.")
        p2_results = []
    else:
        p2_results, p2_failed = run_pass(
            2, build_pass2, ranges_p2,
            f"Pass 2 — result/unit conversion  ({total_p2:,} rows)"
        )
        if p2_failed:
            print("\n  Pass 2 had failures.")
            sys.exit(1)

    # ── Summary ───────────────────────────────────────────────────────
    total_rows = sum(r["rows"] for r in p1_results + p2_results)
    print(f"\n{'='*70}")
    print(f"  Total rows updated : {total_rows:,}")
    print(f"  Target             : {TARGET_TABLE}")
    print(f"{'='*70}")

    print(f"\n  Cleanup SQL (run after verifying data):")
    print(f"    -- Shared lookup (only drop when all vitals tables are done):")
    print(f"    -- DROP TABLE IF EXISTS {STAGING_LOINC};")
    print(f"    DROP TABLE IF EXISTS {STAGING_PK_P1};")
    print(f"    DROP TABLE IF EXISTS {STAGING_PK_P2};")
    print(f"    DROP TABLE IF EXISTS {CHECKPOINT_TABLE};")


if __name__ == "__main__":
    main()