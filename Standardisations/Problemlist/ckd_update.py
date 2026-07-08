#!/usr/bin/env python3
"""
ckd_update.py — Optimized batched CKD comorbidity UPDATE for rgd_udm_silver.problemlist

Original SQL:
    UPDATE rgd_udm_silver.problemlist
    SET ckd_comorbidity = 'Chronic kidney disease (CKD)'
    WHERE LOWER(problem_desc)     LIKE '%chronic kidney%'
       OR LOWER(problem_desc_std) LIKE '%chronic kidney%'
       OR icd_code                LIKE 'N18%'
       OR mapped_icd_code         LIKE 'N18%';

The OR/LIKE scan is run ONCE upfront on all eligible rows → PKs stored in STAGING_PK.
Batches then use a fast integer equality join on ndid — no LIKE per batch.

Pre-materialized tables (computed ONCE):
  - staging.ckd_update_pk_v1_<tbl>   (ndids of matching rows, OR/LIKE done once)

Usage:
    python ckd_update.py
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

BATCH_SIZE   = 50_000
TARGET_TABLE = "rgd_udm_silver.problemlist"
BATCH_KEY    = "ndid"

_TABLE_SUFFIX = TARGET_TABLE.replace(".", "_").replace("-", "_")

STAGING_PK       = f"staging.ckd_update_pk_v1_{_TABLE_SUFFIX}"
CHECKPOINT_TABLE = f"staging.etl_checkpoint_ckd_update_v1_{_TABLE_SUFFIX}"
CHECKPOINT_KEY   = f"problemlist.ckd_update.v1.{_TABLE_SUFFIX}"


# ── Helpers ───────────────────────────────────────────────────────────

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


def _build_ranges(cur, staging_pk):
    cur.execute(f"SELECT COUNT(*) FROM {staging_pk}")
    total = cur.fetchone()[0]
    if total == 0:
        return [], 0

    cur.execute(f"""
        SELECT {BATCH_KEY}
        FROM (
            SELECT {BATCH_KEY},
                   ROW_NUMBER() OVER (ORDER BY {BATCH_KEY}) AS rn
            FROM {staging_pk}
        ) t
        WHERE (rn - 1) % {BATCH_SIZE} = 0
        ORDER BY {BATCH_KEY}
    """)
    boundaries = [row[0] for row in cur.fetchall() if row[0] is not None]

    cur.execute(f"SELECT MAX({BATCH_KEY}) FROM {staging_pk}")
    max_pk = int(cur.fetchone()[0])

    ranges = []
    for i, lo in enumerate(boundaries):
        hi = boundaries[i + 1] if i + 1 < len(boundaries) else max_pk + 1
        ranges.append((lo, hi))

    return ranges, total


# ── Batch UPDATE builder ──────────────────────────────────────────────

def build_batch_update(pk_lo, pk_hi):
    """
    Eligible PKs were staged once upfront — no LIKE per batch.
    Joins on integer ndid from STAGING_PK then applies the static value.
    """
    return f"""
UPDATE {TARGET_TABLE} p
JOIN {STAGING_PK} pkc ON p.{BATCH_KEY} = pkc.{BATCH_KEY}
SET p.ckd_comorbidity = 'Chronic kidney disease (CKD)'
WHERE p.{BATCH_KEY} >= {pk_lo}
  AND p.{BATCH_KEY} <  {pk_hi}
"""


# ── Checkpoint ─────────────────────────────────────────────────────────

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


# ── DDL: ensure ckd_comorbidity column exists ─────────────────────────

def ensure_std_columns():
    print(f"  Checking ckd_comorbidity column on {TARGET_TABLE}...", flush=True)
    conn = get_connection()
    cur  = conn.cursor()
    try:
        if not _col_exists(cur, TARGET_TABLE, "ckd_comorbidity"):
            print("    adding: ckd_comorbidity VARCHAR(255) ...", flush=True)
            cur.execute(
                f"ALTER TABLE {TARGET_TABLE} ADD COLUMN ckd_comorbidity VARCHAR(255) DEFAULT NULL"
            )
            conn.commit()
            print("    added: ckd_comorbidity")
        else:
            print("    exists: ckd_comorbidity")
    except Exception as exc:
        print(f"\n  ERROR: Could not alter {TARGET_TABLE}: {exc}")
        print(f"  Check for metadata locks:")
        print(f"    SELECT id, user, state FROM information_schema.processlist")
        print(f"    WHERE state LIKE '%lock%' ORDER BY time DESC;")
        sys.exit(1)
    finally:
        cur.close()
        conn.close()


# ── Setup ──────────────────────────────────────────────────────────────

def setup_tables():
    ensure_std_columns()

    conn = get_connection()
    cur  = conn.cursor()
    tgt_schema, tgt_table = TARGET_TABLE.split(".", 1)

    # ── 1. Indexes on filter/batch columns ────────────────────────────
    print("  Checking indexes on filter columns...", flush=True)
    for col in [BATCH_KEY, "icd_code", "mapped_icd_code"]:
        print(f"    {TARGET_TABLE} ({col})...", end=" ", flush=True)
        if not _index_exists(cur, tgt_schema, tgt_table, col):
            print("missing — creating...", flush=True)
            try:
                cur.execute(f"CREATE INDEX idx_{col} ON `{tgt_schema}`.`{tgt_table}` ({col})")
                conn.commit()
                print("      done")
            except Exception as exc:
                print(f"      warning: {exc}")
        else:
            print("exists")

    # TEXT columns need a prefix length
    for col in ["problem_desc", "problem_desc_std"]:
        print(f"    {TARGET_TABLE} ({col})...", end=" ", flush=True)
        if not _index_exists(cur, tgt_schema, tgt_table, col):
            print("missing — creating (prefix 100)...", flush=True)
            try:
                cur.execute(
                    f"CREATE INDEX idx_{col} ON `{tgt_schema}`.`{tgt_table}` ({col}(100))"
                )
                conn.commit()
                print("      done")
            except Exception as exc:
                print(f"      warning: {exc}")
        else:
            print("exists")

    # ── 2. PK staging — eligible ndids (OR/LIKE filters applied once) ─
    print("  Creating PK staging (OR/LIKE filters applied once)...", flush=True)
    if not _table_exists(cur, STAGING_PK):
        cur.execute(f"""
            CREATE TABLE {STAGING_PK} AS
            SELECT {BATCH_KEY}
            FROM {TARGET_TABLE}
            WHERE (
                LOWER(problem_desc)     LIKE '%chronic kidney%'
             OR LOWER(problem_desc_std) LIKE '%chronic kidney%'
             OR icd_code               LIKE 'N18%'
             OR mapped_icd_code        LIKE 'N18%'
            )
              AND {BATCH_KEY} IS NOT NULL
            ORDER BY {BATCH_KEY}
        """)
        cur.execute(f"ALTER TABLE {STAGING_PK} ADD INDEX idx_pk ({BATCH_KEY})")
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")

    ranges, total = _build_ranges(cur, STAGING_PK)
    print(f"    {total:,} rows → {len(ranges)} batches of {BATCH_SIZE:,}")

    if total == 0:
        print("  No matching rows — nothing to update.")
        cur.close()
        conn.close()
        return []

    # ── 3. Checkpoint table ───────────────────────────────────────────
    print("  Creating checkpoint table...", flush=True)
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
    print("    ready")

    cur.close()
    conn.close()
    return ranges


# ── Runner ─────────────────────────────────────────────────────────────

_LOCK_TIMEOUT_ERRNO = 1205
_LOCK_RETRIES       = 5
_LOCK_RETRY_SLEEP   = 15


def _execute_with_retry(conn, cur, sql):
    for attempt in range(_LOCK_RETRIES):
        try:
            cur.execute(sql)
            return
        except pymysql.err.OperationalError as exc:
            if exc.args[0] == _LOCK_TIMEOUT_ERRNO and attempt < _LOCK_RETRIES - 1:
                print(
                    f"\n  Lock timeout — retry {attempt + 1}/{_LOCK_RETRIES - 1}"
                    f" in {_LOCK_RETRY_SLEEP}s...", flush=True
                )
                time.sleep(_LOCK_RETRY_SLEEP)
                conn.ping(reconnect=True)
                cur = conn.cursor()
                cur.execute("SET unique_checks = 0")
                cur.execute("SET foreign_key_checks = 0")
            else:
                raise


def run_pass(ranges, pbar):
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
            _execute_with_retry(conn, cur, build_batch_update(pk_lo, pk_hi))
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
    print(f"  CKD Comorbidity UPDATE — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  target      : {TARGET_TABLE}")
    print(f"  batch_key   : {BATCH_KEY}")
    print(f"  batch_size  : {BATCH_SIZE:,}")
    print(f"  pk_staging  : {STAGING_PK}")
    print(f"  checkpoint  : {CHECKPOINT_TABLE}")
    print(f"{'='*70}\n", flush=True)

    ranges = setup_tables()

    if not ranges:
        print("Nothing to process. Exiting.")
        return

    print(f"\n  Starting CKD UPDATE ({len(ranges)} batches)...", flush=True)
    with tqdm(total=len(ranges), desc="CKD comorbidity", unit="batch") as pbar:
        result = run_pass(ranges, pbar)

    print()
    status = result["status"]
    tag    = "DONE" if status == "done" else "SKIP" if status == "skipped" else "FAIL"
    print(f"  [{tag}]  {result['rows']:>10,} rows updated  ({result['secs']}s)")

    print(f"\n{'='*70}")
    print(f"  Status : {status}")
    print(f"  Rows   : {result['rows']:,}")
    print(f"{'='*70}")

    print(f"\n  Cleanup SQL (run after verifying data):")
    print(f"    DROP TABLE IF EXISTS {STAGING_PK};")
    print(f"    DROP TABLE IF EXISTS {CHECKPOINT_TABLE};")

    if status.startswith("FAILED"):
        print(f"\n  Error: {status}")
        sys.exit(1)


if __name__ == "__main__":
    main()
