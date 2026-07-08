#!/usr/bin/env python3
"""
diag_gen_opt.py — Optimized batched UPDATE: udm_active_flag on diagnosis_dedup_v1

Source SQL: General/diag_gen_update.sql
Target table: rgd_udm_silver.diagnosis_dedup_v1

Single pass:
  SET udm_active_flag = 'Y' for the most recent udm_inc_id per udm_unq_id
  SET udm_active_flag = 'N' for all older duplicates

Strategy:
  Pre-materialize (udm_inc_id → new_flag) once using ROW_NUMBER OVER
  (PARTITION BY udm_unq_id ORDER BY udm_inc_id DESC). Batch UPDATE by
  udm_inc_id range joining the flagmap — window function runs once, not per batch.

Optimizations:
- Flagmap pre-materialized once (ROW_NUMBER runs once on full table, not per batch)
- Batching by actual udm_inc_id values (keyset pagination, sparse-ID safe)
- Parallel workers with non-overlapping ranges (zero lock contention)
- Checkpoint/resume per worker — re-run skips completed workers
- Commit after every batch (frees InnoDB undo log)
- InnoDB checks disabled per-session for bulk speed
- tqdm progress bar

Usage:
    python diag_gen_opt.py
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
MAX_WORKERS = 4

TARGET_TABLE     = "rgd_udm_silver.diagnosis_dedup_v1"
STAGING_MAP      = "staging.diag_gen_flagmap_v1"
STAGING_PK       = "staging.tmp_diag_gen_pk_v1"
CHECKPOINT_TABLE = "staging.etl_checkpoint_diag_gen_v1"
BATCH_KEY        = "udm_inc_id"


# ── Helpers ────────────────────────────────────────────────────────────

def get_connection():
    return pymysql.connect(**DB_CONFIG)


def _table_exists(cur, full_table_name):
    schema, table = full_table_name.split(".", 1)
    cur.execute(
        "SELECT COUNT(*) FROM information_schema.tables "
        "WHERE table_schema = %s AND table_name = %s",
        (schema, table),
    )
    return cur.fetchone()[0] > 0


def _col_exists(cur, full_table_name, col_name):
    schema, table = full_table_name.split(".", 1)
    cur.execute(
        "SELECT COUNT(*) FROM information_schema.columns "
        "WHERE table_schema = %s AND table_name = %s AND column_name = %s",
        (schema, table, col_name),
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
    size = (len(ranges) + n - 1) // n
    return [ranges[i: i + size] for i in range(0, len(ranges), size)]


# ── Checkpoint ─────────────────────────────────────────────────────────

def is_done(conn, ck_key):
    cur = conn.cursor()
    cur.execute(
        f"SELECT status FROM {CHECKPOINT_TABLE} WHERE source_key = %s",
        (ck_key,),
    )
    row = cur.fetchone()
    cur.close()
    return row is not None and row[0] == "done"


def mark(conn, ck_key, status, rows=0, error=None):
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
    """, (ck_key, status, rows, status, error))
    conn.commit()
    cur.close()


# ── Batch UPDATE builder ───────────────────────────────────────────────

def build_update(pk_lo, pk_hi):
    return f"""
UPDATE {TARGET_TABLE} p
JOIN {STAGING_MAP} m ON p.{BATCH_KEY} = m.{BATCH_KEY}
SET p.udm_active_flag = m.new_flag
WHERE p.{BATCH_KEY} >= {pk_lo}
  AND p.{BATCH_KEY} <  {pk_hi}
"""


# ── DDL: ensure udm_active_flag column exists ─────────────────────────

def ensure_active_flag_column():
    print(f"  Checking udm_active_flag column on {TARGET_TABLE}...")
    conn = get_connection()
    cur  = conn.cursor()
    err  = None
    try:
        if not _col_exists(cur, TARGET_TABLE, "udm_active_flag"):
            print("    adding: udm_active_flag CHAR(1) ...")
            cur.execute(
                f"ALTER TABLE {TARGET_TABLE} "
                f"ADD COLUMN udm_active_flag CHAR(1) DEFAULT NULL"
            )
            conn.commit()
            print("    added")
        else:
            print("    exists")
    except Exception as exc:
        err = exc
        try:
            conn.rollback()
        except Exception:
            pass
    finally:
        try:
            cur.close()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass

    if err:
        print(f"\n  ERROR: Could not add column — metadata lock on {TARGET_TABLE}.")
        print(f"  Find the blocker:")
        print(f"    SELECT id, user, state, info FROM information_schema.processlist")
        print(f"    WHERE state LIKE '%lock%' OR state LIKE '%wait%' ORDER BY time DESC;")
        print(f"  Then: KILL <id>;")
        print(f"\n  Original error: {err}")
        sys.exit(1)


# ── Setup ──────────────────────────────────────────────────────────────

def setup_tables():
    ensure_active_flag_column()

    conn = get_connection()
    cur  = conn.cursor()

    # ── Flagmap: pre-materialize (udm_inc_id → Y/N) ───────────────────
    # ROW_NUMBER OVER (PARTITION BY udm_unq_id ORDER BY udm_inc_id DESC)
    # runs once on the full table. Each udm_unq_id's most recent row gets 'Y',
    # all older duplicates get 'N'.
    print(f"  Pre-materializing active-flag map ({STAGING_MAP})...")
    if not _table_exists(cur, STAGING_MAP):
        print("    running ROW_NUMBER window function — may take several minutes...")
        cur.execute(f"""
            CREATE TABLE {STAGING_MAP} AS
            SELECT
                udm_inc_id,
                CASE WHEN rn = 1 THEN 'Y' ELSE 'N' END AS new_flag
            FROM (
                SELECT
                    udm_inc_id,
                    ROW_NUMBER() OVER (
                        PARTITION BY udm_unq_id
                        ORDER BY udm_inc_id DESC
                    ) AS rn
                FROM {TARGET_TABLE}
            ) x
        """)
        cur.execute(f"ALTER TABLE {STAGING_MAP} ADD INDEX idx_inc_id ({BATCH_KEY})")
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")

    cur.execute(f"SELECT COUNT(*) FROM {STAGING_MAP}")
    map_count = cur.fetchone()[0]
    cur.execute(f"SELECT SUM(new_flag = 'Y') FROM {STAGING_MAP}")
    active_count = cur.fetchone()[0] or 0
    print(f"    {map_count:,} rows  ({active_count:,} marked 'Y',  {map_count - active_count:,} marked 'N')")

    # ── PK staging — batch ranges for udm_inc_id ──────────────────────
    print(f"  Building PK staging for batch ranges ({STAGING_PK})...")
    if not _table_exists(cur, STAGING_PK):
        cur.execute(f"""
            CREATE TABLE {STAGING_PK} AS
            SELECT {BATCH_KEY}
            FROM {TARGET_TABLE}
            WHERE {BATCH_KEY} IS NOT NULL
        """)
        cur.execute(f"ALTER TABLE {STAGING_PK} ADD INDEX idx_pk ({BATCH_KEY})")
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")

    ranges, total = _build_all_ranges(cur, STAGING_PK)
    print(f"    {total:,} rows → {len(ranges)} batches of {BATCH_SIZE:,}")

    # ── Checkpoint table ──────────────────────────────────────────────
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {CHECKPOINT_TABLE} (
            source_key   VARCHAR(200) NOT NULL PRIMARY KEY,
            status       ENUM('running','done','failed') NOT NULL DEFAULT 'running',
            rows_updated BIGINT      DEFAULT 0,
            started_at   DATETIME    DEFAULT NULL,
            completed_at DATETIME    DEFAULT NULL,
            error_msg    TEXT        DEFAULT NULL
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
    """)
    conn.commit()

    cur.close()
    conn.close()
    return ranges, total


# ── Worker ─────────────────────────────────────────────────────────────

def run_worker(worker_id, ranges_chunk, pbar):
    ck_key = f"diag.gen.active_flag.worker{worker_id}"
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
        cur.execute("SET SESSION innodb_lock_wait_timeout = 3600")
        cur.execute("SET unique_checks = 0")
        cur.execute("SET foreign_key_checks = 0")

        for pk_lo, pk_hi in ranges_chunk:
            cur.execute(build_update(pk_lo, pk_hi))
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


# ── Main ───────────────────────────────────────────────────────────────

def main():
    print(f"\n{'='*70}", flush=True)
    print(f"  Diagnosis Active Flag UPDATE — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  target     : {TARGET_TABLE}")
    print(f"  flagmap    : {STAGING_MAP}")
    print(f"  checkpoint : {CHECKPOINT_TABLE}")
    print(f"  batch_size : {BATCH_SIZE:,}  |  workers : {MAX_WORKERS}")
    print(f"{'='*70}\n", flush=True)

    ranges, total = setup_tables()

    if not ranges:
        print("  No rows to process. Exiting.")
        return

    chunks = _split_chunks(ranges, MAX_WORKERS)

    print(f"\n  Starting UPDATE ({len(ranges)} batches, {MAX_WORKERS} workers)...", flush=True)

    results = []
    with tqdm(total=len(ranges), desc="Active flag UPDATE", unit="batch") as pbar:
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
        print(f"  [{tag}] worker {r['worker']}  {r['rows']:>10,} rows  ({r['secs']}s)")

    done          = sum(1 for r in results if r["status"] == "done")
    skipped       = sum(1 for r in results if r["status"] == "skipped")
    failed        = [r for r in results if "FAILED" in str(r["status"])]
    total_updated = sum(r["rows"] for r in results)

    print(f"\n{'='*70}")
    print(f"  Done: {done}  Skipped: {skipped}  Failed: {len(failed)}  |  Rows updated: {total_updated:,}")
    print(f"{'='*70}")

    print(f"\n  Cleanup SQL (run after verifying data):")
    print(f"    DROP TABLE IF EXISTS {STAGING_MAP};")
    print(f"    DROP TABLE IF EXISTS {STAGING_PK};")
    print(f"    DROP TABLE IF EXISTS {CHECKPOINT_TABLE};")

    if failed:
        print("\n  Failed workers:")
        for r in failed:
            print(f"    worker {r['worker']}: {r['status']}")
        sys.exit(1)


if __name__ == "__main__":
    main()
