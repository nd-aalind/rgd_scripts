#!/usr/bin/env python3
"""
Optimized deduplication CREATE TABLE for: deidentified_merged.dedup_patientinsurance

Reads from deidentified_merged.PATIENTINSURANCE and keeps only the latest record
(by nd_deidentification_datetime DESC) for each nd_auto_increment_id that has
duplicates (cnt > 1).

Window functions used:
  - ROW_NUMBER() OVER (PARTITION BY nd_auto_increment_id ORDER BY nd_deidentification_datetime DESC)
  - COUNT()      OVER (PARTITION BY nd_auto_increment_id)

Filter: cnt > 1 AND rn = 1

Batching strategy:
  - Batch by nd_auto_increment_id ranges (the PARTITION BY key).
  - This ensures every window function partition is fully contained within one
    batch, so ROW_NUMBER and COUNT values are always correct.
  - Target table is created empty first; then rows are INSERTed batch by batch.

Optimizations applied:
- Staging pre-materializes DISTINCT nd_auto_increment_id values (the partition key)
- Server-side boundary sampling (avoids loading all IDs into memory)
- Batch range added to inner subquery — window functions stay correct per partition
- Commit after every batch (frees undo/log space)
- Checkpoint/resume — re-run skips if already completed
- Disabled InnoDB checks per-session for bulk insert speed
- Progress bar via tqdm

Usage:
    python create_tab.py
"""

import sys
import time
from datetime import datetime
import pymysql
from tqdm import tqdm
import os
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())

# ── Configuration ────────────────────────────────────────────────────
DB_CONFIG = {
    "host":            os.environ.get("DB_INTERNAL_HOST"),
    "port":            3306,
    "user":            os.environ.get("DB_INTERNAL_USER"),
    "password":        os.environ.get("DB_INTERNAL_PASSWORD"),
    "database":        "deidentified_merged",
    "charset":         "utf8mb4",
    "connect_timeout": 30,
    "read_timeout":    21600,
    "write_timeout":   21600,
}

BATCH_SIZE = 50_000   # partition IDs per batch

SOURCE_TABLE     = "deidentified_merged.PATIENTINSURANCE"
TARGET_TABLE     = "deidentified_merged.dedup_patientinsurance"
STAGING_TABLE    = "staging.tmp_dedup_patins_staging"
CHECKPOINT_TABLE = "staging.etl_checkpoint_dedup_patins"

# Partition key — batching on this keeps window function partitions intact
BATCH_KEY = "nd_auto_increment_id"

CHECKPOINT_KEY = "dedup_patientinsurance.create"


# ── Helpers ──────────────────────────────────────────────────────────

def get_connection():
    """One connection per call."""
    return pymysql.connect(**DB_CONFIG)


def build_batch_insert(pk_lo, pk_hi):
    """
    Inserts deduplicated rows for nd_auto_increment_id in [pk_lo, pk_hi).
    The batch range is applied inside the inner subquery so that the window
    functions (ROW_NUMBER, COUNT) only see rows within the current partition
    range — producing correct rn and cnt values without scanning the full table.
    """
    return f"""
INSERT INTO {TARGET_TABLE}
SELECT *
FROM (
    SELECT t.*,
           ROW_NUMBER() OVER (
               PARTITION BY {BATCH_KEY}
               ORDER BY nd_deidentification_datetime DESC
           ) AS rn,
           COUNT(*) OVER (
               PARTITION BY {BATCH_KEY}
           ) AS cnt
    FROM {SOURCE_TABLE} t
    WHERE {BATCH_KEY} >= {pk_lo} AND {BATCH_KEY} < {pk_hi}
) x
WHERE cnt > 1
  AND rn = 1
"""


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
    """Create target, staging, and checkpoint tables. Return pk ranges."""
    conn = get_connection()
    cur = conn.cursor()

    # ── 1. Target table: create empty with correct schema ────────────
    print("  Creating target table (empty)...")
    cur.execute(
        "SELECT COUNT(*) FROM information_schema.tables "
        "WHERE table_schema = %s AND table_name = %s",
        (TARGET_TABLE.split(".")[0], TARGET_TABLE.split(".")[1]),
    )
    if cur.fetchone()[0] == 0:
        cur.execute(f"""
            CREATE TABLE {TARGET_TABLE}
            SELECT *
            FROM (
                SELECT t.*,
                       ROW_NUMBER() OVER (
                           PARTITION BY {BATCH_KEY}
                           ORDER BY nd_deidentification_datetime DESC
                       ) AS rn,
                       COUNT(*) OVER (
                           PARTITION BY {BATCH_KEY}
                       ) AS cnt
                FROM {SOURCE_TABLE} t
                WHERE 1 = 0
            ) x
            WHERE cnt > 1 AND rn = 1
        """)
        conn.commit()
        print("    created (empty)")
    else:
        print("    already exists, appending to it")

    # ── 2. Staging table: DISTINCT partition key values ──────────────
    print("  Creating staging table (distinct nd_auto_increment_id values)...")
    cur.execute(
        "SELECT COUNT(*) FROM information_schema.tables "
        "WHERE table_schema = %s AND table_name = %s",
        (STAGING_TABLE.split(".")[0], STAGING_TABLE.split(".")[1]),
    )
    if cur.fetchone()[0] == 0:
        cur.execute(f"""
            CREATE TABLE {STAGING_TABLE} AS
            SELECT DISTINCT {BATCH_KEY}
            FROM {SOURCE_TABLE}
            WHERE {BATCH_KEY} IS NOT NULL
        """)
        cur.execute(f"ALTER TABLE {STAGING_TABLE} ADD INDEX idx_pk ({BATCH_KEY})")
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")

    cur.execute(f"SELECT COUNT(*) FROM {STAGING_TABLE}")
    partition_count = cur.fetchone()[0]
    print(f"    {partition_count:,} distinct partition IDs")

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

    # ── 4. Compute batch ranges via server-side boundary sampling ────
    print("  Computing batch boundaries...")
    sys.stdout.flush()

    cur.execute(f"SELECT COUNT(*) FROM {STAGING_TABLE}")
    total = cur.fetchone()[0]

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

    print(f"    {len(ranges)} batches of ~{BATCH_SIZE:,} partition IDs each")
    return ranges


# ── Runner ───────────────────────────────────────────────────────────

def run_insert(ranges, pbar):
    """Execute the deduplication INSERT across all batches."""
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

        # Disable InnoDB checks for bulk insert speed (session-scoped only)
        cur.execute("SET unique_checks = 0")
        cur.execute("SET foreign_key_checks = 0")

        for pk_lo, pk_hi in ranges:
            sql = build_batch_insert(pk_lo, pk_hi)
            cur.execute(sql)
            conn.commit()
            total_rows += cur.rowcount
            pbar.update(1)

        # Re-enable checks
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
    print(f"  Dedup PATIENTINSURANCE CREATE TABLE — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  source     : {SOURCE_TABLE}")
    print(f"  target     : {TARGET_TABLE}")
    print(f"  partition  : {BATCH_KEY}")
    print(f"  filter     : cnt > 1 AND rn = 1  (latest record per duplicate group)")
    print(f"  staging    : {STAGING_TABLE}")
    print(f"  batch_size : {BATCH_SIZE:,} partition IDs")
    print(f"{'='*70}\n", flush=True)

    print("  Connecting to database...")
    sys.stdout.flush()
    ranges = setup_tables()
    if not ranges:
        print(f"\nNo rows found in {SOURCE_TABLE}. Exiting.")
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

    print(f"  [{tag}] {TARGET_TABLE:<42} {result['rows']:>10,} rows inserted  ({result['secs']}s)")

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
