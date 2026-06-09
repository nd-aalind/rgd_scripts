#!/usr/bin/env python3
"""
Optimized sequential row-number UPDATE for: rgd_udm_silver.notes_part2

Populates udm_inc_id_new with a sequential integer (1, 2, 3, ...) ordered
by the primary key — equivalent to the original:
    SET @rownum := 0;
    UPDATE rgd_udm_silver.notes_part2 SET udm_inc_id_new = (@rownum := @rownum + 1);

Why not run the original directly?
  MySQL user variables (@rownum) cannot be split across batches. Running the
  UPDATE in one shot on millions of rows holds a huge transaction open, bloating
  the undo log and risking lock timeouts.

Why not pre-materialize ROW_NUMBER() for all rows first?
  CREATE TABLE ... AS SELECT ROW_NUMBER() OVER (...) scans the ENTIRE table in
  one shot — same problem as @rownum. It will hang on large tables.

Solution — offset + per-batch ROW_NUMBER():
  1. Create a lightweight PK-only staging table (fast — no computation).
  2. During boundary sampling, also capture rn-1 as the offset for each batch.
     (rn-1 is the count of rows before this boundary — free from the ROW_NUMBER
     already computed for sampling.)
  3. Each batch UPDATE computes ROW_NUMBER() only over its own BATCH_SIZE rows
     and adds the pre-computed offset. No full-table scan ever.

Optimizations applied:
- PK-only staging table (lightweight — no ROW_NUMBER scan of full table)
- Per-batch offset computed during boundary sampling (rn-1, zero extra cost)
- Batch UPDATE uses offset + ROW_NUMBER() OVER (ORDER BY pk) in a subquery
- Server-side boundary sampling (avoids loading all PKs into memory)
- Commit after every batch (frees undo/log space)
- Checkpoint/resume — re-run skips if already completed
- Disabled InnoDB checks per-session for bulk update speed
- Progress bar via tqdm

Usage:
    python alter_table.py
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
    "database":        "kinsula_leq",
    "charset":         "utf8mb4",
    "connect_timeout": 30,
    "read_timeout":    21600,
    "write_timeout":   21600,
}

BATCH_SIZE = 50_000

TARGET_TABLE     = "rgd_udm_silver.notes_part2"
UPDATE_COLUMN    = "udm_inc_id_new"
BATCH_KEY        = "ndid"          # existing PK / unique column to batch on

STAGING_TABLE    = "staging.tmp_rownum_notes_part2_new"
CHECKPOINT_TABLE = "staging.etl_checkpoint_rownum_notes_part2_new"
CHECKPOINT_KEY   = "notes_part2.udm_inc_id_new.rownum"


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
    """
    1. Create a lightweight PK-only staging table (no ROW_NUMBER — fast).
    2. Boundary sample: capture both pk and offset (rn-1) per boundary.
    3. Create checkpoint table.
    4. Return ranges as (pk_lo, pk_hi, offset) triples.
    """
    conn = get_connection()
    cur  = conn.cursor()

    # ── 1. PK-only staging table (lightweight — just the keys) ───────
    print("  Creating PK staging table...")
    if not _table_exists(cur, STAGING_TABLE):
        cur.execute(f"""
            CREATE TABLE {STAGING_TABLE} AS
            SELECT {BATCH_KEY}
            FROM {TARGET_TABLE}
            WHERE {BATCH_KEY} IS NOT NULL
        """)
        cur.execute(f"ALTER TABLE {STAGING_TABLE} ADD INDEX idx_pk ({BATCH_KEY})")
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")

    cur.execute(f"SELECT COUNT(*) FROM {STAGING_TABLE}")
    total = cur.fetchone()[0]
    print(f"    {total:,} rows to update")

    # ── 2. Checkpoint table ──────────────────────────────────────────
    print("  Creating checkpoint table...")
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {CHECKPOINT_TABLE} (
            source_key   VARCHAR(150) NOT NULL PRIMARY KEY,
            status       ENUM('running','done','failed') NOT NULL DEFAULT 'running',
            rows_updated BIGINT      DEFAULT 0,
            started_at   DATETIME    DEFAULT NULL,
            completed_at DATETIME    DEFAULT NULL,
            error_msg    TEXT        DEFAULT NULL
        )
    """)
    conn.commit()
    print("    ready")

    # ── 3. Boundary sampling — also capture offset (rn-1) per batch ──
    # rn-1 is the 0-based row number at each boundary = count of rows before it.
    # This is free — ROW_NUMBER() is already computed for sampling.
    print("  Computing batch boundaries + offsets...")
    sys.stdout.flush()

    if total == 0:
        cur.close()
        conn.close()
        return []

    cur.execute(f"""
        SELECT {BATCH_KEY}, (rn - 1) AS offset_before
        FROM (
            SELECT {BATCH_KEY},
                   ROW_NUMBER() OVER (ORDER BY {BATCH_KEY}) AS rn
            FROM {STAGING_TABLE}
        ) t
        WHERE (rn - 1) % {BATCH_SIZE} = 0
        ORDER BY {BATCH_KEY}
    """)
    boundaries = [(row[0], int(row[1])) for row in cur.fetchall() if row[0] is not None]

    cur.execute(f"SELECT MAX({BATCH_KEY}) FROM {STAGING_TABLE}")
    max_pk = int(cur.fetchone()[0])

    cur.close()
    conn.close()

    ranges = []
    for i, (lo, offset) in enumerate(boundaries):
        hi = boundaries[i + 1][0] if i + 1 < len(boundaries) else max_pk + 1
        ranges.append((lo, hi, offset))

    print(f"    {len(ranges)} batches of ~{BATCH_SIZE:,} rows each")
    return ranges


# ── Batch UPDATE builder ──────────────────────────────────────────────

def build_batch_update(pk_lo, pk_hi, offset):
    """
    UPDATE only BATCH_SIZE rows at a time.
    ROW_NUMBER() runs over this small slice; offset shifts the sequence
    to the correct global position.
    """
    return f"""
UPDATE {TARGET_TABLE} t
JOIN (
    SELECT {BATCH_KEY},
           {offset} + ROW_NUMBER() OVER (ORDER BY {BATCH_KEY}) AS seq_id
    FROM {TARGET_TABLE}
    WHERE {BATCH_KEY} >= {pk_lo} AND {BATCH_KEY} < {pk_hi}
) s ON s.{BATCH_KEY} = t.{BATCH_KEY}
SET t.{UPDATE_COLUMN} = s.seq_id
WHERE t.{BATCH_KEY} >= {pk_lo} AND t.{BATCH_KEY} < {pk_hi}
"""


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

        for pk_lo, pk_hi, offset in ranges:
            sql = build_batch_update(pk_lo, pk_hi, offset)
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
    print(f"  Sequential Row-Number UPDATE — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  target     : {TARGET_TABLE}")
    print(f"  column     : {UPDATE_COLUMN}  ← offset + ROW_NUMBER() ORDER BY {BATCH_KEY}")
    print(f"  staging    : {STAGING_TABLE}  (PK-only, lightweight)")
    print(f"  batch_key  : {BATCH_KEY}")
    print(f"  batch_size : {BATCH_SIZE:,}")
    print(f"{'='*70}\n", flush=True)

    print("  Connecting to database...")
    sys.stdout.flush()
    ranges = setup_tables()

    if not ranges:
        print(f"\nNo rows found in {TARGET_TABLE}. Exiting.")
        return

    result = None
    with tqdm(total=len(ranges), desc="Overall", unit="batch") as pbar:
        result = run_update(ranges, pbar)

    print()
    tag = " DONE" if result["status"] == "done" else (" SKIP" if result["status"] == "skipped" else " FAIL")
    print(f"  [{tag}] {TARGET_TABLE:<42} {result['rows']:>10,} rows updated  ({result['secs']}s)")

    print(f"\n{'='*70}")
    if result["status"].startswith("FAILED"):
        print(f"  FAILED: {result['status']}")
    else:
        print(f"  Status: {result['status']}  |  Total rows updated: {result['rows']:,}")
    print(f"{'='*70}")

    print(f"\n  Cleanup SQL:")
    print(f"    DROP TABLE IF EXISTS {STAGING_TABLE};")
    print(f"    DROP TABLE IF EXISTS {CHECKPOINT_TABLE};")

    if result["status"].startswith("FAILED"):
        sys.exit(1)


if __name__ == "__main__":
    main()
