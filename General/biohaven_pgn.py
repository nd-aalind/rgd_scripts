#!/usr/bin/env python3
"""
Optimized ETL loader for: biohaven.dent_progressnotes_20260423

Source:
    dent.progressnotes_decryptfinal a
    LEFT JOIN dent.enc b ON a.encounterID = b.encounterID
    INNER JOIN biohaven.pateint_list_21apr c ON b.patientid = c.patientid

Reproduces: CREATE TABLE biohaven.dent_progressnotes_20260423 AS SELECT a.* ...

Optimizations applied:
- Eligible encounterIDs pre-staged (filtered via INNER JOIN to patient list)
- Batch by actual encounterID values (sparse IDs — never arithmetic ranges)
- Index on join keys for source tables
- Checkpoint/resume — re-run skips completed sources
- Disabled InnoDB checks per-session for bulk insert speed
- Commit after every batch (frees undo/log space)
- Progress bar via tqdm

Usage:
    python biohaven_pgn.py
"""

import sys
import time
from datetime import datetime
import pymysql
from tqdm import tqdm
import os
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())

# ── Configuration ─────────────────────────────────────────────────────
DB_CONFIG = {
    "host":            os.environ.get("DB_INTERNAL_HOST"),
    "port":            3306,
    "user":            os.environ.get("DB_INTERNAL_USER"),
    "password":        os.environ.get("DB_INTERNAL_PASSWORD"),
    "database":        "dent",
    "charset":         "utf8mb4",
    "connect_timeout": 30,
    "read_timeout":    21600,
    "write_timeout":   21600,
}

BATCH_SIZE = 50_000

SOURCE_SCHEMA      = "dent"
PATIENT_LIST_TABLE = "biohaven.pateint_list_21apr"
DEST_TABLE         = "biohaven.dent_progressnotes_20260423"
STAGING_TABLE      = "staging.tmp_biohaven_pgn_enc"
CHECKPOINT_TABLE   = "staging.etl_checkpoint_biohaven_pgn1"
CHECKPOINT_KEY     = "biohaven.dent_progressnotes_20260423"

BATCH_KEY = "encounterID"


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


def _index_exists(cur, schema, table, column):
    cur.execute(
        "SELECT COUNT(*) FROM information_schema.statistics "
        "WHERE table_schema = %s AND table_name = %s AND column_name = %s",
        (schema, table, column),
    )
    return cur.fetchone()[0] > 0


def _build_ranges(cur):
    cur.execute(f"SELECT COUNT(*) FROM {STAGING_TABLE}")
    total = cur.fetchone()[0]
    if total == 0:
        return [], 0

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
    max_key = int(cur.fetchone()[0])

    ranges = []
    for i, lo in enumerate(boundaries):
        hi = boundaries[i + 1] if i + 1 < len(boundaries) else max_key + 1
        ranges.append((lo, hi))

    return ranges, total


# ── Batch INSERT builder ──────────────────────────────────────────────

def build_batch_insert(eid_lo, eid_hi):
    """
    Reproduces: SELECT a.* ... LEFT JOIN enc b ... INNER JOIN pateint_list c
    Batched on progressnotes_decryptfinal.encounterID.
    """
    return f"""
INSERT INTO {DEST_TABLE}
SELECT
    a.*
FROM {SOURCE_SCHEMA}.progressnotes_decryptfinal a
LEFT JOIN {SOURCE_SCHEMA}.enc b
    ON a.encounterID = b.encounterID
INNER JOIN {PATIENT_LIST_TABLE} c
    ON b.patientid = c.patientid
WHERE a.{BATCH_KEY} >= {eid_lo}
  AND a.{BATCH_KEY} <  {eid_hi}
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

    # ── 1. Ensure indexes on join keys ────────────────────────────────
    print("  Checking/creating source table indexes...")
    for schema, table, col in [
        (SOURCE_SCHEMA, "progressnotes_decryptfinal", "encounterID"),
        (SOURCE_SCHEMA, "enc",                        "encounterID"),
        (SOURCE_SCHEMA, "enc",                        "patientid"),
    ]:
        if not _index_exists(cur, schema, table, col):
            print(f"    Creating index on {schema}.{table}.{col} ...")
            try:
                cur.execute(f"CREATE INDEX idx_{col} ON {schema}.{table} ({col})")
                conn.commit()
                print(f"    Done")
            except Exception as e:
                print(f"    Warning: {e}")
        else:
            print(f"    {schema}.{table}.{col} — index exists")

    # ── 2. Staging table: eligible encounterIDs (filtered by patient list)
    print("  Creating staging table (eligible encounterIDs)...")
    if not _table_exists(cur, STAGING_TABLE):
        cur.execute(f"""
            CREATE TABLE {STAGING_TABLE} AS
            SELECT DISTINCT a.encounterID
            FROM {SOURCE_SCHEMA}.progressnotes_decryptfinal a
            LEFT JOIN {SOURCE_SCHEMA}.enc b
                ON a.encounterID = b.encounterID
            INNER JOIN {PATIENT_LIST_TABLE} c
                ON b.patientid = c.patientid
            WHERE a.encounterID IS NOT NULL
        """)
        cur.execute(f"ALTER TABLE {STAGING_TABLE} ADD INDEX idx_eid (encounterID)")
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")
    cur.execute(f"SELECT COUNT(*) FROM {STAGING_TABLE}")
    print(f"    {cur.fetchone()[0]:,} distinct eligible encounterIDs")

    # ── 3. Destination table ──────────────────────────────────────────
    print("  Creating destination table...")
    if not _table_exists(cur, DEST_TABLE):
        cur.execute(f"""
            CREATE TABLE {DEST_TABLE} LIKE {SOURCE_SCHEMA}.progressnotes_decryptfinal
        """)
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")

    # ── 4. Checkpoint table ───────────────────────────────────────────
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

    # ── 5. Compute batch ranges ───────────────────────────────────────
    print("  Computing batch boundaries...")
    sys.stdout.flush()
    ranges, total = _build_ranges(cur)
    print(f"    {total:,} rows → {len(ranges)} batches of ~{BATCH_SIZE:,}")

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

        for eid_lo, eid_hi in ranges:
            sql = build_batch_insert(eid_lo, eid_hi)
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
    print(f"  Biohaven Progress Notes ETL — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  source     : {SOURCE_SCHEMA}.progressnotes_decryptfinal")
    print(f"  patient list : {PATIENT_LIST_TABLE}")
    print(f"  dest       : {DEST_TABLE}")
    print(f"  batch_size : {BATCH_SIZE:,}")
    print(f"{'='*70}\n", flush=True)

    print("  Connecting to database...")
    sys.stdout.flush()
    ranges = setup_tables()

    if not ranges:
        print("\n  No eligible encounterIDs found. Exiting.")
        return

    with tqdm(total=len(ranges), desc="Inserting", unit="batch") as pbar:
        result = run(ranges, pbar)

    print(f"\n{'='*70}")
    status = result["status"]
    rows   = result["rows"]
    secs   = result["secs"]
    if status == "done":
        tag = " DONE"
    elif status == "skipped":
        tag = " SKIP"
    else:
        tag = " FAIL"
    print(f"  [{tag}] {DEST_TABLE}  {rows:>10,} rows  ({secs}s)")
    if status.startswith("FAILED"):
        print(f"         {status}")
    print(f"\n  Total rows inserted: {rows:,}")
    print(f"{'='*70}")

    print(f"\n  Cleanup SQL (run after verifying data):")
    print(f"    DROP TABLE IF EXISTS {STAGING_TABLE};")
    print(f"    DROP TABLE IF EXISTS {CHECKPOINT_TABLE};")
    print(f"    -- To drop destination:")
    print(f"    -- DROP TABLE IF EXISTS {DEST_TABLE};")

    if status.startswith("FAILED"):
        sys.exit(1)


if __name__ == "__main__":
    main()
