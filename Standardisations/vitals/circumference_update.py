#!/usr/bin/env python3
"""
Optimized batched UPDATE for: rgd_udm_silver.vitals_dedup

Single pass:
  SET vital_result_std = ROUND(vital_result / 2.54, 2),
      vital_unit_std   = 'in'
  WHERE vital_name IN ('VITALS.HEADCIRCUMFERENCE', 'VITALS.WAISTCIRCUMFERENCE')
    AND vital_unit = 'cm'

Optimizations:
- PK staging pre-filters eligible rows (one full scan only)
- Server-side boundary sampling (sparse-ID safe)
- Workers get non-overlapping ranges → no row-level lock contention
- Commit after every batch
- Checkpoint/resume per worker
- InnoDB checks disabled per-session
- Progress bar via tqdm

Usage:
    python circumference_update.py
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

# ── Configuration ─────────────────────────────────────────────────────────────
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
MAX_WORKERS = 4

TARGET_TABLE     = "rgd_udm_silver.vitals_dedup"
BATCH_KEY        = "udm_inc_id"

_TABLE_SUFFIX    = TARGET_TABLE.replace(".", "_").replace("-", "_")
STAGING_PK       = f"staging.vitals_circumference_pk_{_TABLE_SUFFIX}"
CHECKPOINT_TABLE = f"staging.etl_checkpoint_circumference_{_TABLE_SUFFIX}"


# ── Helpers ───────────────────────────────────────────────────────────────────

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


def _build_all_ranges(cur, staging_table):
    """Build (lo, hi) batch ranges from a PK staging table."""
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


# ── Batch UPDATE builder ──────────────────────────────────────────────────────

def build_batch_update(pk_lo, pk_hi):
    return f"""
UPDATE {TARGET_TABLE}
SET
    vital_result_std = ROUND(vital_result / 2.54, 2),
    vital_unit_std   = 'in'
WHERE vital_name IN ('VITALS.HEADCIRCUMFERENCE', 'VITALS.WAISTCIRCUMFERENCE')
  AND vital_unit = 'cm'
  AND {BATCH_KEY} >= {pk_lo}
  AND {BATCH_KEY} <  {pk_hi}
"""


# ── Checkpoint ────────────────────────────────────────────────────────────────

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


# ── Setup ─────────────────────────────────────────────────────────────────────

def setup_tables():
    conn = get_connection()
    cur  = conn.cursor()

    # PK staging — pre-filter rows matching the WHERE condition
    print("  Creating PK staging (circumference rows with vital_unit = 'cm')...")
    if not _table_exists(cur, STAGING_PK):
        cur.execute(f"""
            CREATE TABLE {STAGING_PK} AS
            SELECT {BATCH_KEY}
            FROM {TARGET_TABLE}
            WHERE vital_name IN ('VITALS.HEADCIRCUMFERENCE', 'VITALS.WAISTCIRCUMFERENCE')
              AND vital_unit = 'cm'
              AND {BATCH_KEY} IS NOT NULL
        """)
        cur.execute(f"ALTER TABLE {STAGING_PK} ADD INDEX idx_pk ({BATCH_KEY})")
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")

    ranges, total = _build_all_ranges(cur, STAGING_PK)
    print(f"    {total:,} rows → {len(ranges)} batches")

    # Checkpoint table
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
    return ranges, total


# ── Worker ────────────────────────────────────────────────────────────────────

def run_worker(worker_id, ranges_chunk, pbar):
    ck_key = f"vitals.circumference.worker{worker_id}.{_TABLE_SUFFIX}"
    conn   = get_connection()

    if is_done(conn, ck_key):
        conn.close()
        pbar.update(len(ranges_chunk))
        return {"worker": worker_id, "status": "skipped", "rows": 0, "secs": 0}

    mark(conn, ck_key, "running")
    t0         = time.time()
    total_rows = 0

    try:
        cur = conn.cursor()
        cur.execute("SET unique_checks = 0")
        cur.execute("SET foreign_key_checks = 0")

        for pk_lo, pk_hi in ranges_chunk:
            sql = build_batch_update(pk_lo, pk_hi)
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
        return {"worker": worker_id, "status": "done", "rows": total_rows, "secs": elapsed}

    except Exception as exc:
        elapsed = round(time.time() - t0, 1)
        mark(conn, ck_key, "failed", total_rows, str(exc))
        try:
            conn.close()
        except Exception:
            pass
        return {"worker": worker_id, "status": f"FAILED: {exc}",
                "rows": total_rows, "secs": elapsed}


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print(f"\n{'='*70}", flush=True)
    print(f"  Circumference cm→in UPDATE — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  target     : {TARGET_TABLE}")
    print(f"  batch_key  : {BATCH_KEY}")
    print(f"  batch_size : {BATCH_SIZE:,}")
    print(f"  workers    : {MAX_WORKERS}")
    print(f"  filter     : vital_name IN (HEADCIRCUMFERENCE, WAISTCIRCUMFERENCE)")
    print(f"               AND vital_unit = 'cm'")
    print(f"{'='*70}\n", flush=True)

    all_ranges, total = setup_tables()

    if not all_ranges:
        print("  No rows to process. Exiting.")
        return

    chunks = _split_chunks(all_ranges, MAX_WORKERS)
    results = []

    print(f"\n  ── UPDATE pass ──")
    print(f"     {total:,} rows  |  {len(all_ranges)} batches  ×  {BATCH_SIZE:,}  →  {MAX_WORKERS} workers\n")

    with tqdm(total=len(all_ranges), desc="Circumference", unit="batch") as pbar:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
            futures = {
                pool.submit(run_worker, i, chunks[i], pbar): i
                for i in range(len(chunks))
            }
            for future in as_completed(futures):
                results.append(future.result())

    print()
    for r in sorted(results, key=lambda x: x["worker"]):
        tag = "DONE" if r["status"] == "done" \
              else "SKIP" if r["status"] == "skipped" \
              else "FAIL"
        print(f"  [W{r['worker']}] [{tag}]  {r['rows']:>10,} rows  ({r['secs']}s)")

    done    = sum(1 for r in results if r["status"] == "done")
    skipped = sum(1 for r in results if r["status"] == "skipped")
    failed  = [r for r in results if "FAILED" in str(r["status"])]
    total_updated = sum(r["rows"] for r in results)

    print(f"\n{'='*70}")
    print(f"  Done: {done}  Skipped: {skipped}  Failed: {len(failed)}  |  Total rows updated: {total_updated:,}")
    print(f"{'='*70}")

    print(f"\n  Cleanup SQL (run after verifying data):")
    print(f"    DROP TABLE IF EXISTS {STAGING_PK};")
    print(f"    DROP TABLE IF EXISTS {CHECKPOINT_TABLE};")

    if failed:
        print("\n  Failed workers:")
        for r in failed:
            print(f"    Worker {r['worker']}: {r['status']}")
        sys.exit(1)


if __name__ == "__main__":
    main()
