#!/usr/bin/env python3
"""
Optimized ETL loader for: udm_staging.medicalhistory_ecw
Source: eCW

Source: {SOURCE_SCHEMA}.encounterdata  LEFT JOIN enc
  Filter : LENGTH(encounterdata.pasthistory) > 1
  Batch  : encounterid (from encounterdata)

Pre-materialized lookup tables (computed ONCE, reused across all batches):
  - staging.mh_ecw_enc_{SOURCE_SCHEMA}   (enc, keyed on encounterid)

Key ECW patterns applied:
- enc.encounterid is VARCHAR; encounterdata.encounterid is BIGINT
  → JOIN uses: enc.encounterid = CAST(encounterdata.encounterid AS CHAR)
- enc has duplicate encounterid rows; filter nd_active_flag = 'Y' to get one row
- enc.date is DATETIME; wrapped in CAST(... AS CHAR) to avoid MySQL strict mode error 1292

Optimizations applied:
- enc lookup pre-materialized once (not re-scanned per batch)
- Batch by actual encounterid values (server-side boundary sampling)
- Commit after every batch (frees undo/log space)
- Checkpoint/resume — re-run skips if already completed
- Disabled InnoDB checks per-session for bulk insert speed
- REGEXP {{n}} quantifiers escaped as {{n}} inside f-strings
- Progress bar via tqdm

Usage:
    python opt_med_hist_ecw.py
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
    "database":        "arizona_staging",
    "charset":         "utf8mb4",
    "connect_timeout": 30,
    "read_timeout":    21600,
    "write_timeout":   21600,
}

BATCH_SIZE = 50_000

# ── Change these two variables to run for a different schema/psid ────
SOURCE_SCHEMA = "arizona_staging"   # e.g. "northwest", "texas", "fcn_latest", ...
PSID          = 14

DEST_TABLE       = "udm_staging.athenaone_medicalhistory"
STAGING_ENC      = f"staging.mh_ecw_enc1_{SOURCE_SCHEMA}"
STAGING_PK       = f"staging.tmp_mh_ecw_staging1_{SOURCE_SCHEMA}"
CHECKPOINT_TABLE = f"staging.etl_checkpoint_mh_ecw1_{SOURCE_SCHEMA}"
CHECKPOINT_KEY   = f"medicalhistory1.ecw.insert.{SOURCE_SCHEMA}"

BATCH_KEY = "encounterid"


# ── Date CASE helper (for enc.date DATETIME column) ───────────────────

def date_case_enc(col):
    """
    CASE for enc.date (DATETIME): wraps in CAST(... AS CHAR) to avoid
    MySQL strict mode error 1292 when compared with IN ('', 'None').
    Handles: NULL, empty, YYYY-MM-DD, YYYY-MM-DD HH:MM:SS, MM-DD-YYYY.
    {{4}}/{{2}} produce literal {4}/{2} — correct MySQL REGEXP quantifiers.
    """
    c = f"CAST({col} AS CHAR)"
    return (
        f"CASE\n"
        f"        WHEN {c} IS NULL OR {c} IN ('', 'None') THEN NULL\n"
        f"        WHEN {c} REGEXP '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}( [0-9]{{2}}:[0-9]{{2}}:[0-9]{{2}})?$'\n"
        f"            THEN DATE({c})\n"
        f"        WHEN {c} REGEXP '^[0-9]{{2}}-[0-9]{{2}}-[0-9]{{4}}$'\n"
        f"            THEN STR_TO_DATE({c}, '%m-%d-%Y')\n"
        f"        ELSE NULL\n"
        f"    END"
    )


# ── Batch INSERT builder ──────────────────────────────────────────────

def build_batch_insert(pk_lo, pk_hi):
    return f"""
INSERT INTO {DEST_TABLE}
    (med_hist_id, ndid, eid, encounter_date, med_hist_date,
     med_hist_category, med_hist_question, med_hist_value,
     med_hist_code, med_hist_coding_system, med_hist_notes,
     created_datetime, created_by, updated_datetime, updated_by,
     ehr_source_name, source_path, data_type,
     psid, nd_extracted_date)
SELECT
    NULL,
    CAST(enc.patientid AS SIGNED),
    CAST(ed.{BATCH_KEY} AS SIGNED),
    {date_case_enc('enc.date')},
    {date_case_enc('enc.date')},
    'Patient Medical History',
    NULL,
    ed.pasthistory,
    NULL,
    NULL,
    NULL,
    CURRENT_DATE(),
    'ND',
    CURRENT_DATE(),
    'ND',
    'ECW',
    'bronze_layer',
    'Structured',
    {PSID},
    ed.nd_extracted_date
FROM {SOURCE_SCHEMA}.encounterdata ed
LEFT JOIN {STAGING_ENC} enc
    ON enc.encounterid = CAST(ed.{BATCH_KEY} AS CHAR)
WHERE ed.nd_Activeflag = 'Y'
  AND LENGTH(ed.pasthistory) > 1
  AND CAST(ed.{BATCH_KEY} AS SIGNED) >= {pk_lo}
  AND CAST(ed.{BATCH_KEY} AS SIGNED) < {pk_hi}
"""


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


# ── Checkpoint ────────────────────────────────────────────────────────

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


# ── Setup ─────────────────────────────────────────────────────────────

def setup_tables():
    """Create enc lookup, PK staging, destination, checkpoint tables. Return batch ranges."""
    conn = get_connection()
    cur  = conn.cursor()

    # ── 1. Pre-materialized enc lookup ───────────────────────────────
    print("  Materializing enc lookup...")
    if not _table_exists(cur, STAGING_ENC):
        cur.execute(f"""
            CREATE TABLE {STAGING_ENC} AS
            SELECT patientid, encounterid, date
            FROM {SOURCE_SCHEMA}.enc
            WHERE nd_Activeflag = 'Y'
        """)
        cur.execute(f"ALTER TABLE {STAGING_ENC} ADD INDEX idx_enc (encounterid)")
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")
    cur.execute(f"SELECT COUNT(*) FROM {STAGING_ENC}")
    print(f"    {cur.fetchone()[0]:,} enc rows")

    # ── 2. PK staging table ───────────────────────────────────────────
    print("  Creating PK staging for encounterdata...")
    if not _table_exists(cur, STAGING_PK):
        cur.execute(f"""
            CREATE TABLE {STAGING_PK} AS
            SELECT CAST({BATCH_KEY} AS SIGNED) AS {BATCH_KEY}
            FROM {SOURCE_SCHEMA}.encounterdata
            WHERE {BATCH_KEY} IS NOT NULL
              AND nd_Activeflag = 'Y'
              AND LENGTH(pasthistory) > 1
        """)
        cur.execute(f"ALTER TABLE {STAGING_PK} ADD INDEX idx_pk ({BATCH_KEY})")
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")

    cur.execute(f"SELECT COUNT(*) FROM {STAGING_PK}")
    total = cur.fetchone()[0]
    print(f"    {total:,} rows to insert")

    # ── 3. Destination table ──────────────────────────────────────────
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

    # ── 4. Checkpoint table ───────────────────────────────────────────
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

    # ── 5. Batch boundary sampling ────────────────────────────────────
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
            FROM {STAGING_PK}
        ) t
        WHERE (rn - 1) % {BATCH_SIZE} = 0
        ORDER BY {BATCH_KEY}
    """)
    boundaries = [row[0] for row in cur.fetchall() if row[0] is not None]

    cur.execute(f"SELECT MAX({BATCH_KEY}) FROM {STAGING_PK}")
    max_pk = int(cur.fetchone()[0])

    cur.close()
    conn.close()

    ranges = []
    for i, lo in enumerate(boundaries):
        hi = boundaries[i + 1] if i + 1 < len(boundaries) else max_pk + 1
        ranges.append((lo, hi))

    print(f"    {len(ranges)} batches of ~{BATCH_SIZE:,} rows each")
    return ranges


# ── Runner ────────────────────────────────────────────────────────────

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


# ── Main ──────────────────────────────────────────────────────────────

def main():
    print(f"\n{'='*70}", flush=True)
    print(f"  eCW Medical History ETL — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  source     : {SOURCE_SCHEMA}.encounterdata  (psid={PSID})")
    print(f"  dest       : {DEST_TABLE}")
    print(f"  enc_lookup : {STAGING_ENC}")
    print(f"  staging    : {STAGING_PK}")
    print(f"  checkpoint : {CHECKPOINT_TABLE}")
    print(f"  batch_size : {BATCH_SIZE:,}")
    print(f"{'='*70}\n", flush=True)

    print("  Connecting to database...")
    sys.stdout.flush()
    ranges = setup_tables()

    if not ranges:
        print(f"\nNo eligible rows in {SOURCE_SCHEMA}.encounterdata. Exiting.")
        return

    result = None
    with tqdm(total=len(ranges), desc="encounterdata", unit="batch") as pbar:
        result = run_insert(ranges, pbar)

    print()
    if result["status"] == "done":
        tag = " DONE"
    elif result["status"] == "skipped":
        tag = " SKIP"
    else:
        tag = " FAIL"
    print(f"  [{tag}] {SOURCE_SCHEMA}.encounterdata  "
          f"{result['rows']:>10,} rows inserted  ({result['secs']}s)")

    print(f"\n{'='*70}")
    if result["status"].startswith("FAILED"):
        print(f"  FAILED: {result['status']}")
    else:
        print(f"  Status: {result['status']}  |  Total rows inserted: {result['rows']:,}")
    print(f"{'='*70}")

    print(f"\n  Cleanup SQL (run after verifying data):")
    print(f"    DROP TABLE IF EXISTS {STAGING_ENC};")
    print(f"    DROP TABLE IF EXISTS {STAGING_PK};")
    print(f"    DROP TABLE IF EXISTS {CHECKPOINT_TABLE};")

    if result["status"].startswith("FAILED"):
        sys.exit(1)


if __name__ == "__main__":
    main()
