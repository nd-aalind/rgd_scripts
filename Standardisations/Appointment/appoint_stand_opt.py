#!/usr/bin/env python3
"""
appoint_stand_opt.py — Optimized batched standardisation UPDATE for appointment_name

Source SQL: Standardisations/Appointment/appoint_stand.sql
Target table: udm_staging.appointment_fn

Single pass:
  SET appointment_name_std
  14-step REGEXP_REPLACE chain pre-computed ONCE on DISTINCT appointment_name values.
  Batch UPDATE joins mapping table slices on appointment_name equality.

Batching strategy — name-group batches, NOT appointment_id ranges:
  The mapping table has only 3,051 distinct names. Each batch processes BATCH_NAMES
  names at a time via LIMIT/OFFSET on the map. This avoids any range scan or
  MIN/MAX query on the large appointment_fn table (which kills the process).

Optimizations:
- Mapping table pre-materialized once (chain runs on unique names only, not per row)
- Batch by name-groups from staging map — no large-table range scan during setup
- Parallel workers with non-overlapping name slices (zero lock contention)
- Checkpoint/resume per worker
- Commit after every batch
- InnoDB checks disabled per-session
- tqdm progress bar

Usage:
    python appoint_stand_opt.py
"""

import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import pymysql
from tqdm import tqdm

# ── Configuration ─────────────────────────────────────────────────────
DB_CONFIG = {
    "host":            "ndai-dev-rds-instance.cwp60ymu4ko0.us-east-1.rds.amazonaws.com",
    "port":            3306,
    "user":            "Aalind",
    "password":        "A@L1nd@123",
    "database":        "udm_staging",
    "charset":         "utf8mb4",
    "connect_timeout": 30,
    "read_timeout":    21600,
    "write_timeout":   21600,
}

# Number of distinct appointment_names processed per UPDATE batch.
# 3,051 names / 200 per batch = ~16 batches total.
BATCH_NAMES = 200
MAX_WORKERS = 4

TARGET_TABLE     = "udm_staging.appointment_fn_test"
STAGING_MAP      = "staging.apt_std_map_v2"
CHECKPOINT_TABLE = "staging.etl_checkpoint_apt_std_v2"


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


def _index_exists(cur, schema, table, column):
    cur.execute("""
        SELECT COUNT(*) FROM information_schema.statistics
        WHERE table_schema = %s AND table_name = %s AND column_name = %s
    """, (schema, table, column))
    return cur.fetchone()[0] > 0


def _split_chunks(items, n):
    size = (len(items) + n - 1) // n
    return [items[i: i + size] for i in range(0, len(items), size)]


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


# ── Batch UPDATE — joins a slice of the mapping table ─────────────────

def build_update(offset, limit):
    """
    UPDATE appointment_fn rows whose appointment_name matches the next `limit`
    names from the mapping table (starting at `offset`). The subquery on the
    tiny STAGING_MAP (3,051 rows) is fast; the join uses the appointment_name index.
    No WHERE filter on appointment_fn — any filter on an unindexed column forces a
    full table scan. The checkpoint guarantees each worker runs only once.
    """
    return f"""
UPDATE {TARGET_TABLE} a
JOIN (
    SELECT appointment_name, appointment_name_std
    FROM {STAGING_MAP}
    ORDER BY appointment_name
    LIMIT {limit} OFFSET {offset}
) m ON a.appointment_name = m.appointment_name
SET a.appointment_name_std = m.appointment_name_std
"""


# ── DDL: ensure std column exists ─────────────────────────────────────

def ensure_std_column():
    print(f"  Checking appointment_name_std column on {TARGET_TABLE}...")
    conn = get_connection()
    cur  = conn.cursor()
    err  = None
    try:
        if not _col_exists(cur, TARGET_TABLE, "appointment_name_std"):
            print("    adding: appointment_name_std VARCHAR(200) ...")
            cur.execute(
                f"ALTER TABLE {TARGET_TABLE} "
                f"ADD COLUMN appointment_name_std VARCHAR(200) DEFAULT NULL"
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


# ── Staging map SQL (raw string — preserves MySQL regex backslash escapes) ──
# 14-step REGEXP_REPLACE chain applied in order step 1 → step 14.
# Innermost call applies step 1 (fractions); outermost applies step 14 (trailing punctuation).

_STAGING_MAP_SQL = r"""
CREATE TABLE __MAP__ AS
SELECT DISTINCT
    appointment_name,
    CASE
        WHEN appointment_name IS NULL
             OR TRIM(appointment_name) = ''
        THEN NULL

        WHEN TRIM(appointment_name) REGEXP '^[0-9]+(\.[0-9]+)?$'
        THEN 'Other'

        ELSE COALESCE(
            NULLIF(
                TRIM(
                    REGEXP_REPLACE(
                    REGEXP_REPLACE(
                    REGEXP_REPLACE(
                    REGEXP_REPLACE(
                    REGEXP_REPLACE(
                    REGEXP_REPLACE(
                    REGEXP_REPLACE(
                    REGEXP_REPLACE(
                    REGEXP_REPLACE(
                    REGEXP_REPLACE(
                    REGEXP_REPLACE(
                    REGEXP_REPLACE(
                    REGEXP_REPLACE(
                    REGEXP_REPLACE(
                        appointment_name,
                        '[0-9]+/[0-9]+',
                        ''
                    ),
                    '(^|[^A-Za-z])[0-9]+\\.?[0-9]*\\s*(hours|hour|hrs|hr|h)\\s*or\\s*[0-9]+\\.?[0-9]*\\s*(hours|hour|hrs|hr|h)([^A-Za-z]|$)',
                    ''
                    ),
                    '^[0-9]+\\s*(MINUTE|MINUTES|MIN|MINS|HOUR|HOURS|HR|HRS)\\s+',
                    ''
                    ),
                    '(^|[^A-Za-z])[0-9]+\\.?[0-9]*\\s*(hours|hour|mins|minutes|hrs|hr|min|h)([^A-Za-z]|$)',
                    ''
                    ),
                    '[0-9]+\\.?[0-9]*\\s*(days|day|weeks|week|months|month|years|year)',
                    ''
                    ),
                    '\\([^)]*\\)',
                    ''
                    ),
                    '\\s*[-/]\\s*[0-9]+[\\s-]*$',
                    ''
                    ),
                    '[_\\s]+[0-9]+\\s*$',
                    ''
                    ),
                    '^[0-9\\.]+\\s+(?!\\+)',
                    ''
                    ),
                    ',',
                    ' '
                    ),
                    '\\s*-+\\s*$',
                    ''
                    ),
                    '\\s+',
                    ' '
                    ),
                    '^\\*+\\s*',
                    ''
                    ),
                    '[*._]+\\s*$',
                    ''
                )
                ),
                ''
            ),
            'Other'
        )
    END AS appointment_name_std
FROM __TABLE__
"""


# ── Setup ──────────────────────────────────────────────────────────────

def setup_tables():
    ensure_std_column()

    conn = get_connection()
    cur  = conn.cursor()

    # ── Mapping table ─────────────────────────────────────────────────
    print(f"  Pre-materializing appointment_name mapping ({STAGING_MAP})...")
    if not _table_exists(cur, STAGING_MAP):
        print("    running SELECT DISTINCT + 14-step REGEXP_REPLACE chain...")
        sql = (
            _STAGING_MAP_SQL
            .replace("__MAP__",   STAGING_MAP)
            .replace("__TABLE__", TARGET_TABLE)
        )
        cur.execute(sql)
        cur.execute(f"ALTER TABLE {STAGING_MAP} ADD INDEX idx_aname (appointment_name(200))")
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")

    cur.execute(f"SELECT COUNT(*) FROM {STAGING_MAP}")
    total_names = cur.fetchone()[0]
    print(f"    {total_names:,} distinct appointment names")

    # ── appointment_name index on target (for JOIN efficiency) ────────
    tgt_schema, tgt_table = TARGET_TABLE.split(".", 1)
    if not _index_exists(cur, tgt_schema, tgt_table, "appointment_name"):
        print(f"  Creating index idx_appointment_name on {TARGET_TABLE}(appointment_name)...")
        cur.execute(
            f"CREATE INDEX idx_appointment_name ON {TARGET_TABLE} (appointment_name(200))"
        )
        conn.commit()
        print("    done")
    else:
        print(f"  Index on {TARGET_TABLE}(appointment_name) exists")

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

    # ── Name-group batches: LIMIT/OFFSET on STAGING_MAP (tiny table) ──
    # No range scan on appointment_fn — avoids the MIN/MAX OOM kill.
    batches = [
        (offset, min(BATCH_NAMES, total_names - offset))
        for offset in range(0, total_names, BATCH_NAMES)
    ]
    print(f"    {total_names:,} names → {len(batches)} batches of {BATCH_NAMES} names each")

    cur.close()
    conn.close()
    return batches, total_names


# ── Worker ─────────────────────────────────────────────────────────────

def run_worker(worker_id, batch_chunk, pbar):
    ck_key = f"apt.std.worker{worker_id}"
    conn   = get_connection()

    if is_done(conn, ck_key):
        conn.close()
        pbar.update(len(batch_chunk))
        return {"worker": worker_id, "status": "skipped", "rows": 0, "secs": 0}

    mark(conn, ck_key, "running")
    t0         = time.time()
    total_rows = 0

    try:
        cur = conn.cursor()
        cur.execute("SET SESSION innodb_lock_wait_timeout = 3600")
        cur.execute("SET unique_checks = 0")
        cur.execute("SET foreign_key_checks = 0")

        for offset, limit in batch_chunk:
            cur.execute(build_update(offset, limit))
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
    print(f"  Appointment Standardisation UPDATE — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  target     : {TARGET_TABLE}")
    print(f"  map        : {STAGING_MAP}")
    print(f"  checkpoint : {CHECKPOINT_TABLE}")
    print(f"  batch_size : {BATCH_NAMES} names/batch  |  workers : {MAX_WORKERS}")
    print(f"{'='*70}\n", flush=True)

    batches, total_names = setup_tables()

    if not batches:
        print("  No names to process. Exiting.")
        return

    chunks = _split_chunks(batches, MAX_WORKERS)

    print(f"\n  Starting UPDATE ({len(batches)} batches, {MAX_WORKERS} workers)...", flush=True)

    results = []
    with tqdm(total=len(batches), desc="Apt standardisation", unit="batch") as pbar:
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
    print(f"    DROP TABLE IF EXISTS {CHECKPOINT_TABLE};")

    if failed:
        print("\n  Failed workers:")
        for r in failed:
            print(f"    worker {r['worker']}: {r['status']}")
        sys.exit(1)


if __name__ == "__main__":
    main()
