#!/usr/bin/env python3
"""
Optimized batched UPDATE for: rgd_udm_silver.diagnosis_dedup_v1

Original logic:
  SET udm_active_flag = 'Y' for the row with the highest udm_inc_id
  per udm_unq_id group, 'N' for all others.
  (ROW_NUMBER() OVER PARTITION BY udm_unq_id ORDER BY udm_inc_id DESC)

Optimizations:
- Window function pre-computed once into a staging table (indexed on udm_inc_id)
- Batching by actual PK values — sparse-ID safe
- ThreadPoolExecutor with parallel workers over non-overlapping ranges
- Checkpoint/resume — re-run skips completed workers
- Commit after every batch
- InnoDB checks disabled per-session
- Dual logging: terminal (stdout) + timestamped log file

Usage:
    python update_active_flag_opt.py
    # On a VM (survives logout):
    nohup python update_active_flag_opt.py &
    tail -f update_active_flag_*.log
"""

import logging
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

TARGET_TABLE     = "rgd_udm_silver.vitals"
STAGING_TABLE    = "staging.vitals_dedup_active_flag_v1"
CHECKPOINT_TABLE = "staging.etl_checkpoint_update_active_flag_v1_vt"
BATCH_KEY        = "udm_inc_id"


# ── Logging setup ─────────────────────────────────────────────────────

def _setup_logging():
    ts       = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path = f"update_active_flag_{ts}.log"

    fmt = logging.Formatter(
        fmt="%(asctime)s  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setFormatter(fmt)
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)

    root = logging.getLogger()
    root.setLevel(logging.INFO)
    root.addHandler(fh)
    root.addHandler(sh)

    return log_path


logger = logging.getLogger("update_active_flag")


# ── Batch UPDATE builder ───────────────────────────────────────────────

def build_batch_update(pk_lo, pk_hi):
    return f"""
UPDATE {TARGET_TABLE} p
JOIN {STAGING_TABLE} t ON t.{BATCH_KEY} = p.{BATCH_KEY}
SET p.udm_active_flag = t.new_flag
WHERE p.{BATCH_KEY} >= {pk_lo} AND p.{BATCH_KEY} < {pk_hi}
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


def _index_exists(cur, full_table_name, index_name):
    schema, table = full_table_name.split(".")
    cur.execute(
        "SELECT COUNT(*) FROM information_schema.statistics "
        "WHERE table_schema = %s AND table_name = %s AND index_name = %s",
        (schema, table, index_name),
    )
    return cur.fetchone()[0] > 0


def _add_index(cur, conn, full_table_name, index_name, col_expr):
    if not _index_exists(cur, full_table_name, index_name):
        cur.execute(f"ALTER TABLE {full_table_name} ADD INDEX {index_name} ({col_expr})")
        conn.commit()


def _build_all_ranges(cur, staging_table):
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


# ── Checkpoint ────────────────────────────────────────────────────────

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


# ── Setup ─────────────────────────────────────────────────────────────

def setup_tables():
    conn = get_connection()
    cur  = conn.cursor()

    cur.execute("SET SESSION lock_wait_timeout = 3600")
    cur.execute("SET SESSION innodb_lock_wait_timeout = 3600")

    # 1. Materialize window function into staging table
    logger.info(f"Materializing active flag staging -> {STAGING_TABLE} ...")
    logger.info("  (ROW_NUMBER OVER PARTITION BY udm_unq_id_raw ORDER BY udm_inc_id DESC)")
    if not _table_exists(cur, STAGING_TABLE):
        cur.execute(f"""
            CREATE TABLE {STAGING_TABLE} AS
            SELECT
                udm_inc_id,
                CASE WHEN rn = 1 THEN 'Y' ELSE 'N' END AS new_flag
            FROM (
                SELECT
                    udm_inc_id,
                    ROW_NUMBER() OVER (
                        PARTITION BY udm_unq_id_raw
                        ORDER BY udm_inc_id DESC
                    ) AS rn
                FROM {TARGET_TABLE}
            ) x
        """)
        conn.commit()
        logger.info("  created")
    else:
        logger.info("  already exists, reusing")

    _add_index(cur, conn, STAGING_TABLE, "idx_pk", BATCH_KEY)

    cur.execute(f"SELECT COUNT(*) FROM {STAGING_TABLE}")
    total_rows = cur.fetchone()[0]
    cur.execute(f"SELECT SUM(new_flag = 'Y') FROM {STAGING_TABLE}")
    active_count = cur.fetchone()[0]
    logger.info(f"  {total_rows:,} total rows  |  {active_count:,} will be set to 'Y'")

    # 2. Build batch ranges
    all_ranges, total = _build_all_ranges(cur, STAGING_TABLE)
    logger.info(f"  {total:,} rows -> {len(all_ranges)} batches of {BATCH_SIZE:,}")

    # 3. Checkpoint table
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
    return all_ranges, total


# ── Worker ─────────────────────────────────────────────────────────────

def run_worker(worker_id, ranges_chunk, pbar):
    ck_key = f"update_active_flag.worker{worker_id}"
    conn   = get_connection()

    if is_done(conn, ck_key):
        conn.close()
        pbar.update(len(ranges_chunk))
        logger.info(f"  Worker {worker_id}: skipped (already done)")
        return {"worker": worker_id, "status": "skipped", "rows": 0, "secs": 0}

    logger.info(f"  Worker {worker_id}: starting ({len(ranges_chunk)} batches)")
    mark(conn, ck_key, "running")
    t0         = time.time()
    total_rows = 0
    log_every  = max(1, len(ranges_chunk) // 10)

    try:
        cur = conn.cursor()
        cur.execute("SET unique_checks = 0")
        cur.execute("SET foreign_key_checks = 0")

        for batch_num, (pk_lo, pk_hi) in enumerate(ranges_chunk, 1):
            sql = build_batch_update(pk_lo, pk_hi)
            cur.execute(sql)
            conn.commit()
            total_rows += cur.rowcount
            pbar.update(1)

            if batch_num % log_every == 0:
                pct = batch_num / len(ranges_chunk) * 100
                logger.info(
                    f"  Worker {worker_id}: {batch_num}/{len(ranges_chunk)} batches "
                    f"({pct:.0f}%)  rows so far: {total_rows:,}"
                )

        cur.execute("SET unique_checks = 1")
        cur.execute("SET foreign_key_checks = 1")
        cur.close()

        elapsed = round(time.time() - t0, 1)
        mark(conn, ck_key, "done", total_rows)
        conn.close()
        logger.info(f"  Worker {worker_id}: DONE  {total_rows:,} rows  ({elapsed}s)")
        return {"worker": worker_id, "status": "done", "rows": total_rows, "secs": elapsed}

    except Exception as exc:
        elapsed = round(time.time() - t0, 1)
        mark(conn, ck_key, "failed", total_rows, str(exc))
        logger.error(
            f"  Worker {worker_id}: FAILED after {elapsed}s  "
            f"rows so far: {total_rows:,}  error: {exc}"
        )
        try:
            conn.close()
        except Exception:
            pass
        return {"worker": worker_id, "status": f"FAILED: {exc}",
                "rows": total_rows, "secs": elapsed}


# ── Main ───────────────────────────────────────────────────────────────

def main():
    log_path = _setup_logging()

    logger.info("=" * 70)
    logger.info(f"Update Active Flag — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"  log file   : {log_path}")
    logger.info(f"  target     : {TARGET_TABLE}")
    logger.info(f"  staging    : {STAGING_TABLE}")
    logger.info(f"  checkpoint : {CHECKPOINT_TABLE}")
    logger.info(f"  batch_size : {BATCH_SIZE:,}  |  workers: {MAX_WORKERS}")
    logger.info("=" * 70)

    all_ranges, total = setup_tables()

    if not all_ranges:
        logger.info("No rows to process. Exiting.")
        return

    chunks = _split_chunks(all_ranges, MAX_WORKERS)
    results = []

    logger.info(f"{'─'*70}")
    logger.info(
        f"Starting UPDATE: {len(all_ranges)} batches  x  {BATCH_SIZE:,} rows/batch"
        f"  ->  {MAX_WORKERS} workers"
    )

    with tqdm(total=len(all_ranges), desc="Updating", unit="batch",
              file=sys.stderr) as pbar:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
            futures = {
                pool.submit(run_worker, i, chunks[i], pbar): i
                for i in range(len(chunks))
            }
            for future in as_completed(futures):
                results.append(future.result())

    done    = sum(1 for r in results if r["status"] == "done")
    skipped = sum(1 for r in results if r["status"] == "skipped")
    failed  = [r for r in results if "FAILED" in str(r["status"])]
    total_updated = sum(r["rows"] for r in results)

    logger.info("=" * 70)
    logger.info(
        f"Complete: {done} done, {skipped} skipped, {len(failed)} failed  |  "
        f"rows updated: {total_updated:,}"
    )
    for r in failed:
        logger.error(f"  [FAIL] worker {r['worker']}: {r['status']}")
    logger.info("=" * 70)

    logger.info("Cleanup SQL (run after verifying data):")
    logger.info(f"  DROP TABLE IF EXISTS {STAGING_TABLE};")
    logger.info(f"  DROP TABLE IF EXISTS {CHECKPOINT_TABLE};")

    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
