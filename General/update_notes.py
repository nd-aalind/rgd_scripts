#!/usr/bin/env python3
"""
Optimized UPDATE: rgd_udm_silver.notes_part1.enc_start_date

Backfills enc_start_date (currently NULL) by joining with CLINICALENCOUNTER.

Pre-materialized lookup table (computed ONCE):
  - staging.upd_notes_ce_v1  (CLINICALENCOUNTER: clinicalencounterid → ENCOUNTERDATE)

Optimizations applied:
- CLINICALENCOUNTER pre-materialized once (not re-scanned per batch)
- PK staging pre-filters eligible NULL rows (distinct eids only)
- Batch by actual eid values (not arithmetic ranges — IDs can be sparse)
- Server-side boundary sampling (avoids loading all PKs into memory)
- Commit after every batch (frees undo/log space)
- Checkpoint/resume — re-run skips if already completed
- Disabled InnoDB checks per-session for bulk update speed
- Progress bar via tqdm

Usage:
    python update_notes.py
"""

import sys
import time
from datetime import datetime
import pymysql
from tqdm import tqdm

# ── Configuration ────────────────────────────────────────────────────
DB_CONFIG = {
    "host":            "ndai-dev-rds-instance.cwp60ymu4ko0.us-east-1.rds.amazonaws.com",
    "port":            3306,
    "user":            "Aalind",
    "password":        "A@L1nd@123",
    "database":        'rgd_udm_silver',
    "charset":         "utf8mb4",
    "connect_timeout": 30,
    "read_timeout":    21600,
    "write_timeout":   21600,
}

BATCH_SIZE = 50_000

# ── Change these to target a different table/schema/psid ─────────────
TARGET_TABLE = "rgd_udm_silver.notes_part1_lilly"
CE_SCHEMA    = "tng_athena_one"
PSID         = 2

STAGING_CE       = f"staging.upd_notes_ce_v4_{CE_SCHEMA}"
STAGING_TABLE    = f"staging.tmp_upd_notes_eid_v4_{CE_SCHEMA}"
CHECKPOINT_TABLE = f"staging.etl_checkpoint_upd_notes_v5_{CE_SCHEMA}"
CHECKPOINT_KEY   = "notes_part1.enc_start_date.update"

BATCH_KEY = "eid"


# ── Batch UPDATE builder ──────────────────────────────────────────────

def build_batch_update(pk_lo, pk_hi):
    return f"""
UPDATE {TARGET_TABLE} a
JOIN {STAGING_CE} b ON b.clinicalencounterid = a.eid
SET a.enc_start_date = b.enc_start_date
WHERE a.enc_start_date IS NULL
  AND a.psid = {PSID}
  AND a.{BATCH_KEY} >= {pk_lo} AND a.{BATCH_KEY} < {pk_hi}
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


def _index_exists(cur, full_table_name, index_name):
    schema, table = full_table_name.split(".")
    cur.execute(
        "SELECT COUNT(*) FROM information_schema.statistics "
        "WHERE table_schema = %s AND table_name = %s AND index_name = %s",
        (schema, table, index_name),
    )
    return cur.fetchone()[0] > 0


def ensure_indexes(cur, conn):
    """Check and create missing indexes on join/filter columns."""
    checks = [
        # (full_table,                    index_name,        column(s))
        (TARGET_TABLE,                    "idx_eid",         "eid"),
        (TARGET_TABLE,                    "idx_psid",        "psid"),
        (TARGET_TABLE,                    "idx_enc_start",   "enc_start_date"),
    ]
    for full_table, idx_name, cols in checks:
        if not _index_exists(cur, full_table, idx_name):
            print(f"    Creating index {idx_name} on {full_table}({cols})...")
            cur.execute(f"ALTER TABLE {full_table} ADD INDEX {idx_name} ({cols})")
            conn.commit()
            print(f"      created")
        else:
            print(f"    Index {idx_name} on {full_table} already exists")


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
            (source_key, status, rows_updated, started_at, completed_at, error_msg)
        VALUES (%s, %s, %s, NOW(), IF(%s = 'done', NOW(), NULL), %s)
        ON DUPLICATE KEY UPDATE
            status       = VALUES(status),
            rows_updated = VALUES(rows_updated),
            completed_at = IF(VALUES(status) = 'done', NOW(), NULL),
            error_msg    = VALUES(error_msg)
    """, (CHECKPOINT_KEY, status, rows, status, error))
    conn.commit()
    cur.close()


# ── Setup ────────────────────────────────────────────────────────────

def setup_tables():
    """Pre-materialize lookup + PK staging + checkpoint. Return batch ranges."""
    conn = get_connection()
    cur  = conn.cursor()

    # ── 0. Ensure indexes on target table join/filter columns ────────
    print("  Checking indexes on target table...")
    ensure_indexes(cur, conn)

    # ── 1. Pre-materialized CLINICALENCOUNTER lookup ──────────────────
    print("  Materializing CLINICALENCOUNTER lookup...")
    if not _table_exists(cur, STAGING_CE):
        cur.execute(f"""
            CREATE TABLE {STAGING_CE} AS
            SELECT
                clinicalencounterid,
                CASE
                    WHEN ENCOUNTERDATE IS NULL OR ENCOUNTERDATE IN ('', 'None') THEN NULL
                    WHEN ENCOUNTERDATE REGEXP '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}} [0-9]{{2}}:[0-9]{{2}}:[0-9]{{2}}$'
                        THEN DATE(ENCOUNTERDATE)
                    WHEN ENCOUNTERDATE REGEXP '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}$'
                        THEN STR_TO_DATE(ENCOUNTERDATE, '%Y-%m-%d')
                    WHEN ENCOUNTERDATE REGEXP '^[0-9]{{2}}/[0-9]{{2}}/[0-9]{{4}}$'
                        THEN STR_TO_DATE(ENCOUNTERDATE, '%m/%d/%Y')
                    WHEN ENCOUNTERDATE REGEXP '^[0-9]{{2}}-[0-9]{{2}}-[0-9]{{4}}$'
                        THEN STR_TO_DATE(ENCOUNTERDATE, '%m-%d-%Y')
                    ELSE NULL
                END AS enc_start_date
            FROM {CE_SCHEMA}.CLINICALENCOUNTER
        """)
        cur.execute(f"ALTER TABLE {STAGING_CE} ADD INDEX idx_ce (clinicalencounterid)")
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")
    cur.execute(f"SELECT COUNT(*) FROM {STAGING_CE}")
    print(f"    {cur.fetchone()[0]:,} CLINICALENCOUNTER rows")

    # ── 2. PK staging — distinct eids with enc_start_date IS NULL ────
    print(f"  Creating PK staging (enc_start_date IS NULL, psid={PSID})...")
    if not _table_exists(cur, STAGING_TABLE):
        cur.execute(f"""
            CREATE TABLE {STAGING_TABLE} AS
            SELECT DISTINCT eid
            FROM {TARGET_TABLE}
            WHERE enc_start_date IS NULL
              AND psid = {PSID}
              AND eid IS NOT NULL
        """)
        cur.execute(f"ALTER TABLE {STAGING_TABLE} ADD INDEX idx_eid (eid)")
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")

    cur.execute(f"SELECT COUNT(*) FROM {STAGING_TABLE}")
    total = cur.fetchone()[0]
    print(f"    {total:,} distinct eids to process")

    # ── 3. Checkpoint table ──────────────────────────────────────────
    print("  Creating checkpoint table...")
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {CHECKPOINT_TABLE} (
            source_key   VARCHAR(150) NOT NULL PRIMARY KEY,
            status       ENUM('running','done','failed') NOT NULL DEFAULT 'running',
            rows_updated BIGINT       DEFAULT 0,
            started_at   DATETIME     DEFAULT NULL,
            completed_at DATETIME     DEFAULT NULL,
            error_msg    TEXT         DEFAULT NULL
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

    print(f"    {len(ranges)} batches of ~{BATCH_SIZE:,} eids each")
    return ranges


# ── Runner ───────────────────────────────────────────────────────────

def run_update(ranges, pbar):
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
            sql = build_batch_update(pk_lo, pk_hi)
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
    print(f"  Update notes enc_start_date — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  target     : {TARGET_TABLE}  (psid={PSID})")
    print(f"  lookup     : {CE_SCHEMA}.CLINICALENCOUNTER")
    print(f"  staging    : {STAGING_TABLE}")
    print(f"  checkpoint : {CHECKPOINT_TABLE}")
    print(f"  batch_size : {BATCH_SIZE:,}")
    print(f"{'='*70}\n", flush=True)

    print("  Connecting to database...")
    sys.stdout.flush()
    ranges = setup_tables()

    if not ranges:
        print(f"\nNo eligible rows (enc_start_date IS NULL AND psid={PSID}). Exiting.")
        return

    result = None
    with tqdm(total=len(ranges), desc="Overall", unit="batch") as pbar:
        result = run_update(ranges, pbar)

    print()
    if result["status"] == "done":
        tag = " DONE"
    elif result["status"] == "skipped":
        tag = " SKIP"
    else:
        tag = " FAIL"
    print(f"  [{tag}] {TARGET_TABLE}  "
          f"{result['rows']:>10,} rows updated  ({result['secs']}s)")

    print(f"\n{'='*70}")
    if result["status"].startswith("FAILED"):
        print(f"  FAILED: {result['status']}")
    else:
        print(f"  Status: {result['status']}  |  Total rows updated: {result['rows']:,}")
    print(f"{'='*70}")

    print(f"\n  Cleanup SQL:")
    print(f"    DROP TABLE IF EXISTS {STAGING_CE};")
    print(f"    DROP TABLE IF EXISTS {STAGING_TABLE};")
    print(f"    DROP TABLE IF EXISTS {CHECKPOINT_TABLE};")

    if result["status"].startswith("FAILED"):
        sys.exit(1)


if __name__ == "__main__":
    main()
