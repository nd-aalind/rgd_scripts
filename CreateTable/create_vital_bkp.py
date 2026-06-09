#!/usr/bin/env python3
"""
Optimized batched backup copy: rgd_udm_silver.vitals → staging.vital_rgd_bkp

Reproduces: CREATE TABLE staging.vital_rgd_bkp AS SELECT * FROM rgd_udm_silver.vitals

Optimizations applied:
- Destination table created with LIKE source (no full-table scan at creation)
- Boundary sampling directly from source (no PK staging table — avoids full-table copy)
- Batch by actual udm_inc_id values (sparse IDs — never arithmetic ranges)
- Commit after every batch (frees undo/log space)
- Checkpoint/resume — re-run skips if already done
- Disabled InnoDB checks per-session for bulk insert speed
- Progress bar via tqdm

Usage:
    python create_vital_bkp.py
"""

import sys
import time
from datetime import datetime
import pymysql
from tqdm import tqdm

# ── Configuration ─────────────────────────────────────────────────────
DB_CONFIG = {
    "host":            "172.16.2.42",
    "port":            3306,
    "user":            "nd-root-mysql",
    "password":        "kmsamd89undsd4",
    "database":        "rgd_udm_silver",
    "charset":         "utf8mb4",
    "connect_timeout": 30,
    "read_timeout":    21600,
    "write_timeout":   21600,
}

BATCH_SIZE = 50_000
BATCH_KEY  = "udm_inc_id"

SOURCE_TABLE     = "rgd_udm_silver.vitals"
DEST_TABLE       = "staging.vital_rgd_bkp_new"
CHECKPOINT_TABLE = "staging.etl_checkpoint_vital_bkp_n"
CHECKPOINT_KEY   = "staging.vital_rgd_bkp"


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


def _build_ranges(cur):
    cur.execute(f"SELECT COUNT(*) FROM {SOURCE_TABLE} WHERE {BATCH_KEY} IS NOT NULL")
    total = cur.fetchone()[0]
    if total == 0:
        return [], 0

    # Sample boundary PKs directly from source — avoids materializing all PKs
    cur.execute(f"""
        SELECT {BATCH_KEY}
        FROM (
            SELECT {BATCH_KEY},
                   ROW_NUMBER() OVER (ORDER BY {BATCH_KEY}) AS rn
            FROM {SOURCE_TABLE}
            WHERE {BATCH_KEY} IS NOT NULL
        ) t
        WHERE (rn - 1) % {BATCH_SIZE} = 0
        ORDER BY {BATCH_KEY}
    """)
    boundaries = [row[0] for row in cur.fetchall() if row[0] is not None]

    cur.execute(f"SELECT MAX({BATCH_KEY}) FROM {SOURCE_TABLE} WHERE {BATCH_KEY} IS NOT NULL")
    max_pk = int(cur.fetchone()[0])

    ranges = []
    for i, lo in enumerate(boundaries):
        hi = boundaries[i + 1] if i + 1 < len(boundaries) else max_pk + 1
        ranges.append((lo, hi))

    return ranges, total


# ── Batch INSERT builder ──────────────────────────────────────────────

def build_batch_insert(pk_lo, pk_hi):
    return f"""
INSERT INTO {DEST_TABLE}
SELECT *
FROM {SOURCE_TABLE}
WHERE {BATCH_KEY} >= {pk_lo}
  AND {BATCH_KEY} <  {pk_hi}
"""


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


# ── Setup ──────────────────────────────────────────────────────────────

def setup_tables():
    conn = get_connection()
    cur  = conn.cursor()

    # 1. Destination table
    print(f"  Creating destination table {DEST_TABLE}...")
    if not _table_exists(cur, DEST_TABLE):
        cur.execute(f"CREATE TABLE {DEST_TABLE} LIKE {SOURCE_TABLE}")
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")

    # 2. Checkpoint table
    print("  Creating checkpoint table...")
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {CHECKPOINT_TABLE} (
            source_key    VARCHAR(200) NOT NULL PRIMARY KEY,
            status        ENUM('running','done','failed') NOT NULL DEFAULT 'running',
            rows_inserted BIGINT      DEFAULT 0,
            started_at    DATETIME    DEFAULT NULL,
            completed_at  DATETIME    DEFAULT NULL,
            error_msg     TEXT        DEFAULT NULL
        )
    """)
    conn.commit()
    print("    ready")

    ranges, total = _build_ranges(cur)
    print(f"  {total:,} rows → {len(ranges)} batches of ~{BATCH_SIZE:,}")

    cur.close()
    conn.close()
    return ranges


# ── Runner ─────────────────────────────────────────────────────────────

def run(ranges, pbar):
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


# ── Main ───────────────────────────────────────────────────────────────

def main():
    print(f"\n{'='*70}", flush=True)
    print(f"  Vitals Backup Copy — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  source     : {SOURCE_TABLE}")
    print(f"  dest       : {DEST_TABLE}")
    print(f"  batch_key  : {BATCH_KEY}")
    print(f"  batch_size : {BATCH_SIZE:,}")
    print(f"{'='*70}\n", flush=True)

    print("  Connecting to database...")
    sys.stdout.flush()
    ranges = setup_tables()

    if not ranges:
        print("\n  No rows found in source table. Exiting.")
        return

    with tqdm(total=len(ranges), desc="Copying", unit="batch") as pbar:
        result = run(ranges, pbar)

    status = result["status"]
    rows   = result["rows"]
    secs   = result["secs"]
    tag = " DONE" if status == "done" else (" SKIP" if status == "skipped" else " FAIL")

    print(f"\n{'='*70}")
    print(f"  [{tag}] {DEST_TABLE}  {rows:>12,} rows  ({secs}s)")
    if status.startswith("FAILED"):
        print(f"         {status}")
    print(f"\n  Total rows copied: {rows:,}")
    print(f"{'='*70}")

    print(f"\n  Cleanup SQL (run after verifying data):")
    print(f"    DROP TABLE IF EXISTS {CHECKPOINT_TABLE};")
    print(f"    -- To drop backup:")
    print(f"    -- DROP TABLE IF EXISTS {DEST_TABLE};")

    if status.startswith("FAILED"):
        sys.exit(1)


if __name__ == "__main__":
    main()
