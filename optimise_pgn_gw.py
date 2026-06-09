#!/usr/bin/env python3
"""
Optimized ETL loader for: udm_staging.ecw_progressnotes
Source: eCW (progressnotes_decryptfinal + enc)

Source: {SOURCE_SCHEMA}.progressnotes_decryptfinal a
        LEFT JOIN enc b ON a.encounterID = b.encounterID
  Batch  : encounterID (CAST AS SIGNED)

Pre-materialized lookup tables (computed ONCE, reused across all batches):
  - staging.pgn_gw_enc_{SOURCE_SCHEMA}  (enc filtered nd_active_flag='Y')

Key ECW patterns applied:
- enc.encounterID is VARCHAR; progressnotes_decryptfinal.encounterID is BIGINT
  → JOIN: enc.encounterID = CAST(a.encounterID AS CHAR)
- enc has duplicate encounterID rows; filter nd_active_flag = 'Y' for one row

Optimizations applied:
- enc lookup pre-materialized once (not re-scanned per batch)
- Batch by actual encounterID values (CAST AS SIGNED, server-side boundary sampling)
- Commit after every batch (frees undo/log space)
- Checkpoint/resume — re-run skips if already completed
- Disabled InnoDB checks per-session for bulk insert speed
- Progress bar via tqdm

Usage:
    python optimise_pgn_gw.py
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
    "database":        "staging",
    "charset":         "utf8mb4",
    "connect_timeout": 30,
    "read_timeout":    21600,
    "write_timeout":   21600,
}

BATCH_SIZE = 50_000

# ── Change these two variables to run for a different schema/psid ────
SOURCE_SCHEMA = "dent"   # e.g. "dent", "arizona_staging", ...
PSID          = 1

DEST_TABLE       = "udm_staging.ecw_progressnotes"
STAGING_ENC      = f"staging.pgn_gw_enc_{SOURCE_SCHEMA}"
STAGING_PK       = f"staging.tmp_pgn_gw_staging_{SOURCE_SCHEMA}"
CHECKPOINT_TABLE = f"staging.etl_checkpoint_pgn_gw_{SOURCE_SCHEMA}"
CHECKPOINT_KEY   = f"progressnotes.gw.insert.{SOURCE_SCHEMA}"

BATCH_KEY = "encounterID"


# ── Batch INSERT builder ──────────────────────────────────────────────

def build_batch_insert(pk_lo, pk_hi):
    return f"""
INSERT INTO {DEST_TABLE}
    (patientID, encounterID, summary, psid, nd_extracted_date)
SELECT
    CAST(b.patientID AS SIGNED),
    CAST(a.{BATCH_KEY} AS SIGNED),
    a.summary,
    {PSID},
    CURRENT_DATE()
FROM {SOURCE_SCHEMA}.progressnotes_decryptfinal a
LEFT JOIN {STAGING_ENC} b
    ON b.encounterID = CAST(a.{BATCH_KEY} AS CHAR)
WHERE CAST(a.{BATCH_KEY} AS SIGNED) >= {pk_lo}
  AND CAST(a.{BATCH_KEY} AS SIGNED) < {pk_hi}
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
    print("  Materializing enc lookup (nd_active_flag = 'Y')...")
    if not _table_exists(cur, STAGING_ENC):
        cur.execute(f"""
            CREATE TABLE {STAGING_ENC} AS
            SELECT patientID, encounterID
            FROM {SOURCE_SCHEMA}.enc
            WHERE nd_active_flag = 'Y'
        """)
        cur.execute(f"ALTER TABLE {STAGING_ENC} ADD INDEX idx_enc (encounterID)")
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")
    cur.execute(f"SELECT COUNT(*) FROM {STAGING_ENC}")
    print(f"    {cur.fetchone()[0]:,} enc rows")

    # ── 2. PK staging table ───────────────────────────────────────────
    print("  Creating PK staging for progressnotes_decryptfinal...")
    if not _table_exists(cur, STAGING_PK):
        cur.execute(f"""
            CREATE TABLE {STAGING_PK} AS
            SELECT CAST({BATCH_KEY} AS SIGNED) AS {BATCH_KEY}
            FROM {SOURCE_SCHEMA}.progressnotes_decryptfinal
            WHERE {BATCH_KEY} IS NOT NULL
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
            patientID        BIGINT       DEFAULT NULL,
            encounterID      BIGINT       DEFAULT NULL,
            summary          LONGTEXT,
            psid             INT          DEFAULT NULL,
            nd_extracted_date DATE        DEFAULT NULL
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
    print(f"  eCW Progress Notes ETL — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  source     : {SOURCE_SCHEMA}.progressnotes_decryptfinal  (psid={PSID})")
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
        print(f"\nNo eligible rows in {SOURCE_SCHEMA}.progressnotes_decryptfinal. Exiting.")
        return

    result = None
    with tqdm(total=len(ranges), desc="progressnotes", unit="batch") as pbar:
        result = run_insert(ranges, pbar)

    print()
    if result["status"] == "done":
        tag = " DONE"
    elif result["status"] == "skipped":
        tag = " SKIP"
    else:
        tag = " FAIL"
    print(f"  [{tag}] {SOURCE_SCHEMA}.progressnotes_decryptfinal  "
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
