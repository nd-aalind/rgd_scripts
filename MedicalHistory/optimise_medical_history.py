#!/usr/bin/env python3
"""
Optimized ETL loader for: udm_staging.athenaone_medicalhistory

Source: {SOURCE_SCHEMA}.PATIENTPASTMEDICALHISTORY  (single table, no JOINs)
  Filter : nd_active_flag = 'Y'
  Batch  : PASTMEDICALHISTORYID (primary key)

To run for a different schema/psid, change SOURCE_SCHEMA and PSID below.

Optimizations applied:
- PK staging table pre-filters eligible rows (nd_active_flag = 'Y')
- Batch by actual primary key values (not arithmetic ranges — IDs can be sparse)
- Server-side boundary sampling (avoids loading millions of PKs into memory)
- Commit after every batch (frees undo/log space)
- Checkpoint/resume — re-run skips if already completed
- Disabled InnoDB checks per-session for bulk insert speed
- REGEXP {{n}} quantifiers correctly escaped inside f-strings
- Progress bar via tqdm

Usage:
    python optimise_medical_history.py
"""

import sys
import time
from datetime import datetime
import pymysql
from tqdm import tqdm

# ── Configuration ────────────────────────────────────────────────────
DB_CONFIG = {
    "host":            "172.16.2.42",
    "port":            3306,
    "user":            "nd-root-mysql",
    "password":        "kmsamd89undsd4",
    "database":        "tng_athena_one",
    "charset":         "utf8mb4",
    "connect_timeout": 30,
    "read_timeout":    21600,
    "write_timeout":   21600,
}

BATCH_SIZE = 50_000

# ── Change these two variables to run for a different schema/psid ────
SOURCE_SCHEMA = "tng_athena_one"   # e.g. "dcnd", "raleigh", "tng_athena_one", ...
PSID          = 2

DEST_TABLE       = "udm_staging.athenaone_medicalhistory"
STAGING_TABLE    = f"staging.tmp_mh_athenaone_staging_{SOURCE_SCHEMA}"
CHECKPOINT_TABLE = f"staging.etl_checkpoint_mh_athenaone_{SOURCE_SCHEMA}"
CHECKPOINT_KEY   = f"medicalhistory.insert.{SOURCE_SCHEMA}"

BATCH_KEY = "PASTMEDICALHISTORYID"


# ── Date CASE helper ─────────────────────────────────────────────────

def date_case(col):
    """
    Uses LEFT(col, 10) to extract the date prefix, then pattern-matches.
    LEFT handles YYYY-MM-DD HH:MM:SS by truncating to YYYY-MM-DD automatically.
    Handles: NULL, 'None', '', YYYY-MM-DD, YYYY-MM-DD HH:MM:SS, MM-DD-YYYY.
    {{4}}/{{2}} in f-strings produce {4}/{2} — correct MySQL REGEXP quantifiers.
    """
    return (
        f"CASE\n"
        f"        WHEN {col} IS NULL OR {col} IN ('None', '') THEN NULL\n"
        f"        WHEN LEFT({col}, 10) REGEXP '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}$'\n"
        f"            THEN STR_TO_DATE(LEFT({col}, 10), '%Y-%m-%d')\n"
        f"        WHEN LEFT({col}, 10) REGEXP '^[0-9]{{2}}-[0-9]{{2}}-[0-9]{{4}}$'\n"
        f"            THEN STR_TO_DATE(LEFT({col}, 10), '%m-%d-%Y')\n"
        f"        ELSE NULL\n"
        f"    END"
    )


# ── Batch INSERT builder ──────────────────────────────────────────────

def build_batch_insert(pk_lo, pk_hi):
    p = "p"
    return f"""
INSERT INTO {DEST_TABLE}
    (med_hist_id, ndid, eid, encounter_date, med_hist_date,
     hist_category, med_hist_category, med_hist_question, med_hist_value,
     created_datetime, created_by, updated_datetime, updated_by,
     ehr_source_name, source_path, data_type,
     psid, nd_extracted_date)
SELECT
    {p}.PASTMEDICALHISTORYID,
    {p}.CHARTID,
    NULL,
    NULL,
    {date_case(f'{p}.CREATEDDATETIME')},
    'MedicalHistory',
    {p}.PASTMEDICALHISTORYKEY,
    {p}.PASTMEDICALHISTORYQUESTION,
    {p}.PASTMEDICALHISTORYANSWER,
    CURRENT_DATE(),
    'ND',
    CURRENT_DATE(),
    'ND',
    'athenaone',
    'bronze_layer',
    'Structured',
    {PSID},
    {p}.nd_extracted_date
FROM {SOURCE_SCHEMA}.PATIENTPASTMEDICALHISTORY {p}
WHERE {p}.nd_active_flag = 'Y'
  AND {p}.pastmedicalhistorykey <> 'REVIEWED.PASTMEDICALHISTORY'
  AND {p}.deleteddatetime IS NULL
  AND {p}.{BATCH_KEY} >= {pk_lo} AND {p}.{BATCH_KEY} < {pk_hi}
"""


# ── Helpers ──────────────────────────────────────────────────────────

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


# ── Checkpoint ───────────────────────────────────────────────────────

def is_done(conn):
    cur = conn.cursor()
    cur.execute(
        f"SELECT status FROM {CHECKPOINT_TABLE} WHERE source_key = %s",
        (CHECKPOINT_KEY,),
    )
    row = cur.fetchone()
    cur.close()
    return row is not None and row[0] == "done"


def mark(conn, status, rows=0, error=None):
    cur = conn.cursor()
    cur.execute(f"""
        INSERT INTO {CHECKPOINT_TABLE}
            (source_key, status, rows_inserted, started_at, completed_at, error_msg)
        VALUES (%s, %s, %s, NOW(), IF(%s = 'done', NOW(), NULL), %s)
        ON DUPLICATE KEY UPDATE
            status        = VALUES(status),
            rows_inserted = VALUES(rows_inserted),
            completed_at  = IF(VALUES(status) = 'done', NOW(), NULL),
            error_msg     = VALUES(error_msg)
    """, (CHECKPOINT_KEY, status, rows, status, error))
    conn.commit()
    cur.close()


# ── Setup ────────────────────────────────────────────────────────────

def setup_tables():
    """Create PK staging, destination, checkpoint tables. Return batch ranges."""
    conn = get_connection()
    cur  = conn.cursor()

    # ── 1. PK staging table ──────────────────────────────────────────
    print("  Creating PK staging table...")
    if not _table_exists(cur, STAGING_TABLE):
        cur.execute(f"""
            CREATE TABLE {STAGING_TABLE} AS
            SELECT {BATCH_KEY}
            FROM {SOURCE_SCHEMA}.PATIENTPASTMEDICALHISTORY
            WHERE {BATCH_KEY} IS NOT NULL
              AND nd_active_flag = 'Y'
              AND pastmedicalhistorykey <> 'REVIEWED.PASTMEDICALHISTORY'
              AND deleteddatetime IS NULL
        """)
        cur.execute(f"ALTER TABLE {STAGING_TABLE} ADD INDEX idx_pk ({BATCH_KEY})")
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")

    cur.execute(f"SELECT COUNT(*) FROM {STAGING_TABLE}")
    total = cur.fetchone()[0]
    print(f"    {total:,} rows to insert")

    # ── 2. Destination table ─────────────────────────────────────────
    print("  Creating destination table...")
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {DEST_TABLE} (
            med_hist_id             BIGINT        DEFAULT NULL,
            ndid                    BIGINT        DEFAULT NULL,
            eid                     BIGINT        DEFAULT NULL,
            encounter_date          DATE          DEFAULT NULL,
            med_hist_date           DATE          DEFAULT NULL,
            hist_category           VARCHAR(100)  DEFAULT NULL,
            med_hist_category       VARCHAR(500)  DEFAULT NULL,
            med_hist_question       TEXT,
            med_hist_value          TEXT,
            med_hist_code           VARCHAR(50)   DEFAULT NULL,
            med_hist_coding_system  VARCHAR(50)   DEFAULT NULL,
            med_hist_notes          TEXT,
            data_source             VARCHAR(50)   DEFAULT NULL,
            created_datetime        DATETIME      DEFAULT NULL,
            created_by              VARCHAR(50)   DEFAULT NULL,
            updated_datetime        DATETIME      DEFAULT NULL,
            updated_by              VARCHAR(50)   DEFAULT NULL,
            ehr_source_name         VARCHAR(100)  DEFAULT NULL,
            source_path             VARCHAR(100)  DEFAULT NULL,
            data_type               VARCHAR(50)   DEFAULT NULL,
            psid                    INT           DEFAULT NULL,
            nd_extracted_date       DATE          DEFAULT NULL
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
    """)
    conn.commit()
    print("    ready")

    # ── 3. Checkpoint table ──────────────────────────────────────────
    print("  Creating checkpoint table...")
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {CHECKPOINT_TABLE} (
            source_key    VARCHAR(150) NOT NULL PRIMARY KEY,
            status        ENUM('running','done','failed') NOT NULL DEFAULT 'running',
            rows_inserted BIGINT      DEFAULT 0,
            started_at    DATETIME    DEFAULT NULL,
            completed_at  DATETIME    DEFAULT NULL,
            error_msg     TEXT        DEFAULT NULL
        )
    """)
    conn.commit()
    print("    ready")

    # ── 4. Batch boundary sampling ───────────────────────────────────
    print("  Computing batch boundaries...")
    sys.stdout.flush()

    if total == 0:
        cur.close()
        conn.close()
        return []

    cur.execute(f"""
        SELECT {BATCH_KEY}
        FROM (
            SELECT {BATCH_KEY},
                   ROW_NUMBER() OVER (ORDER BY {BATCH_KEY}) AS rn
            FROM {STAGING_TABLE}
        ) t
        WHERE (rn - 1) % {BATCH_SIZE} = 0
        ORDER BY {BATCH_KEY}
    """)
    boundaries = [row[0] for row in cur.fetchall() if row[0] is not None]

    cur.execute(f"SELECT MAX({BATCH_KEY}) FROM {STAGING_TABLE}")
    max_pk = int(cur.fetchone()[0])

    cur.close()
    conn.close()

    ranges = []
    for i, lo in enumerate(boundaries):
        hi = boundaries[i + 1] if i + 1 < len(boundaries) else max_pk + 1
        ranges.append((lo, hi))

    print(f"    {len(ranges)} batches of ~{BATCH_SIZE:,} rows each")
    return ranges


# ── Runner ───────────────────────────────────────────────────────────

def run_insert(ranges, pbar):
    conn = get_connection()

    if is_done(conn):
        conn.close()
        pbar.update(len(ranges))
        return {"status": "skipped", "rows": 0, "secs": 0}

    mark(conn, "running")
    t0 = time.time()
    total_rows = 0

    try:
        cur = conn.cursor()
        cur.execute("SET unique_checks = 0")
        cur.execute("SET foreign_key_checks = 0")

        for pk_lo, pk_hi in ranges:
            sql = build_batch_insert(pk_lo, pk_hi)
            cur.execute(sql)
            conn.commit()
            total_rows += cur.rowcount
            pbar.update(1)

        cur.execute("SET unique_checks = 1")
        cur.execute("SET foreign_key_checks = 1")
        cur.close()

        elapsed = round(time.time() - t0, 1)
        mark(conn, "done", total_rows)
        conn.close()
        return {"status": "done", "rows": total_rows, "secs": elapsed}

    except Exception as exc:
        elapsed = round(time.time() - t0, 1)
        mark(conn, "failed", total_rows, str(exc))
        try:
            conn.close()
        except Exception:
            pass
        return {"status": f"FAILED: {exc}", "rows": total_rows, "secs": elapsed}


# ── Main ─────────────────────────────────────────────────────────────

def main():
    print(f"\n{'='*70}", flush=True)
    print(f"  AthenaOne Medical History ETL — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  source     : {SOURCE_SCHEMA}.PATIENTPASTMEDICALHISTORY  (psid={PSID})")
    print(f"  dest       : {DEST_TABLE}")
    print(f"  staging    : {STAGING_TABLE}")
    print(f"  checkpoint : {CHECKPOINT_TABLE}")
    print(f"  batch_size : {BATCH_SIZE:,}")
    print(f"{'='*70}\n", flush=True)

    print("  Connecting to database...")
    sys.stdout.flush()
    ranges = setup_tables()

    if not ranges:
        print(f"\nNo eligible rows in {SOURCE_SCHEMA}.PATIENTPASTMEDICALHISTORY. Exiting.")
        return

    result = None
    with tqdm(total=len(ranges), desc="Overall", unit="batch") as pbar:
        result = run_insert(ranges, pbar)

    print()
    if result["status"] == "done":
        tag = " DONE"
    elif result["status"] == "skipped":
        tag = " SKIP"
    else:
        tag = " FAIL"
    print(f"  [{tag}] {SOURCE_SCHEMA}.PATIENTPASTMEDICALHISTORY  "
          f"{result['rows']:>10,} rows inserted  ({result['secs']}s)")

    print(f"\n{'='*70}")
    if result["status"].startswith("FAILED"):
        print(f"  FAILED: {result['status']}")
    else:
        print(f"  Status: {result['status']}  |  Total rows inserted: {result['rows']:,}")
    print(f"{'='*70}")

    print(f"\n  Cleanup SQL:")
    print(f"    DROP TABLE IF EXISTS {STAGING_TABLE};")
    print(f"    DROP TABLE IF EXISTS {CHECKPOINT_TABLE};")

    if result["status"].startswith("FAILED"):
        sys.exit(1)


if __name__ == "__main__":
    main()
