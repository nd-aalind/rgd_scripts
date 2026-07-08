#!/usr/bin/env python3
"""
Optimized batched standardisation UPDATE for: rgd_udm_silver.patient_demographics

Change TARGET_TABLE at the top to run against any patient_demographics table.

Single pass — with checkpoint/resume:

  Pass 1 — All rows:
    SET pat_marital_status_std
    Pure CASE WHEN on pat_marital_status column — no JOIN needed

Std columns added to target table if not present (with metadata lock guard).

Optimizations applied:
- PK staging table (all rows)
- Server-side boundary sampling (avoids loading all PKs into memory)
- Commit after every batch (frees undo/log space)
- Checkpoint/resume — re-run skips completed pass
- Disabled InnoDB checks per-session for bulk update speed
- Progress bar via tqdm

Usage:
    python opt_marital_st.py
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
    "database":        "rgd_udm_silver",
    "charset":         "utf8mb4",
    "connect_timeout": 30,
    "read_timeout":    21600,
    "write_timeout":   21600,
}

BATCH_SIZE = 50_000

# ── Change this to run against a different patient_demographics table ─
TARGET_TABLE = "rgd_udm_silver.patients"

# ─────────────────────────────────────────────────────────────────────
_TABLE_SUFFIX = TARGET_TABLE.replace(".", "_").replace("-", "_")

STAGING_PK       = f"staging.pat_marital_std_pk_{_TABLE_SUFFIX}"
CHECKPOINT_TABLE = f"staging.etl_checkpoint_pat_marital_std_{_TABLE_SUFFIX}"
CHECKPOINT_PASS1 = f"patients.marital.std.pass1.{_TABLE_SUFFIX}"

BATCH_KEY = "udm_inc_id"


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


def _col_exists(cur, full_table_name, col_name):
    schema, table = full_table_name.split(".")
    cur.execute(
        "SELECT COUNT(*) FROM information_schema.columns "
        "WHERE table_schema = %s AND table_name = %s AND column_name = %s",
        (schema, table, col_name),
    )
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

def build_pass1(pk_lo, pk_hi):
    """Pass 1: set pat_marital_status_std via CASE WHEN on pat_marital_status."""
    return f"""
UPDATE {TARGET_TABLE}
SET
    pat_marital_status_std = CASE
        WHEN UPPER(TRIM(pat_marital_status)) = 'SEPARATED'                       THEN 'Separated'
        WHEN UPPER(TRIM(pat_marital_status)) = 'DIVORCED'                        THEN 'Divorced'
        WHEN UPPER(TRIM(pat_marital_status)) = 'MARRIED'                         THEN 'Married'
        WHEN UPPER(TRIM(pat_marital_status)) = 'SINGLE'                          THEN 'Single'
        WHEN UPPER(TRIM(pat_marital_status)) = 'WIDOWED'                         THEN 'Widowed'
        WHEN UPPER(TRIM(pat_marital_status)) = 'COMMON LAW'                      THEN 'Common law'
        WHEN UPPER(TRIM(pat_marital_status)) = 'LIVING TOGETHER'                 THEN 'Living together'
        WHEN UPPER(TRIM(pat_marital_status)) IN ('DOMESTIC PARTNER', 'PARTNER')  THEN 'Domestic partner'
        WHEN UPPER(TRIM(pat_marital_status)) = 'REGISTERED DOMESTIC PARTNER'     THEN 'Registered domestic partner'
        WHEN UPPER(TRIM(pat_marital_status)) IN ('LEGALLY SEPARATED', 'LEGALLY SEPERATED') THEN 'Legally Separated'
        WHEN UPPER(TRIM(pat_marital_status)) = 'ANNULLED'                        THEN 'Annulled'
        WHEN UPPER(TRIM(pat_marital_status)) = 'INTERLOCUTORY'                   THEN 'Interlocutory'
        WHEN UPPER(TRIM(pat_marital_status)) = 'UNMARRIED'                       THEN 'Unmarried'
        WHEN UPPER(TRIM(pat_marital_status)) = 'UNKNOWN'                         THEN 'Unknown'
        WHEN UPPER(TRIM(pat_marital_status)) = 'OTHER'                           THEN 'Other'
        WHEN UPPER(TRIM(pat_marital_status)) = 'UNREPORTED'                      THEN 'Unreported'
        WHEN TRIM(pat_marital_status) = '' OR pat_marital_status IS NULL         THEN 'Unknown'
        ELSE 'NS'
    END
WHERE {BATCH_KEY} >= {pk_lo}
  AND {BATCH_KEY} <  {pk_hi}
"""


# ── Checkpoint ─────────────────────────────────────────────────────────

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


# ── DDL: ensure std column exists ─────────────────────────────────────

def ensure_std_columns():
    std_cols = [
        ("pat_marital_status_std", "VARCHAR(50)"),
    ]
    print(f"  Checking std columns on {TARGET_TABLE}...")
    ddl_conn = get_connection()
    ddl_cur  = ddl_conn.cursor()
    ddl_cur.execute("SET lock_wait_timeout = 15")
    ddl_error = None
    added = []
    try:
        for col_name, col_type in std_cols:
            if not _col_exists(ddl_cur, TARGET_TABLE, col_name):
                print(f"    adding: {col_name} {col_type} ...")
                ddl_cur.execute(
                    f"ALTER TABLE {TARGET_TABLE} ADD COLUMN {col_name} {col_type} DEFAULT NULL"
                )
                ddl_conn.commit()
                added.append(col_name)
                print(f"    added: {col_name}")
            else:
                print(f"    exists: {col_name}")
    except Exception as exc:
        ddl_error = exc
        try:
            ddl_conn.rollback()
        except Exception:
            pass
    finally:
        try:
            ddl_cur.close()
        except Exception:
            pass
        try:
            ddl_conn.close()
        except Exception:
            pass

    if ddl_error:
        print(f"\n  ERROR: Could not add column — metadata lock on {TARGET_TABLE}.")
        print(f"  Find the blocker:")
        print(f"    SELECT id, user, state, info FROM information_schema.processlist")
        print(f"    WHERE state LIKE '%lock%' OR state LIKE '%wait%' ORDER BY time DESC;")
        print(f"  Then: KILL <id>;")
        print(f"\n  Original error: {ddl_error}")
        sys.exit(1)

    if added:
        print(f"    Columns added: {', '.join(added)}")
    else:
        print(f"    All std columns already present.")


# ── Setup ──────────────────────────────────────────────────────────────

def setup_tables():
    ensure_std_columns()

    conn = get_connection()
    cur  = conn.cursor()

    # ── 1. PK staging — all rows ──────────────────────────────────────
    print("  Creating PK staging (all rows)...")
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
    ranges, total = _build_ranges(cur, STAGING_PK)
    print(f"    {total:,} rows → {len(ranges)} batches")

    # ── 2. Checkpoint table ────────────────────────────────────────────
    print("  Creating checkpoint table...")
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

def run_pass(checkpoint_key, build_fn, ranges, pbar):
    conn = get_connection()

    if is_done(conn, checkpoint_key):
        conn.close()
        pbar.update(len(ranges))
        return {"status": "skipped", "rows": 0, "secs": 0}

    mark(conn, checkpoint_key, "running")
    t0 = time.time()
    total_rows = 0

    try:
        cur = conn.cursor()
        cur.execute("SET unique_checks = 0")
        cur.execute("SET foreign_key_checks = 0")

        for pk_lo, pk_hi in ranges:
            sql = build_fn(pk_lo, pk_hi)
            cur.execute(sql)
            conn.commit()
            total_rows += cur.rowcount
            pbar.update(1)

        cur.execute("SET unique_checks = 1")
        cur.execute("SET foreign_key_checks = 1")
        cur.close()

        elapsed = round(time.time() - t0, 1)
        mark(conn, checkpoint_key, "done", total_rows)
        conn.close()
        return {"status": "done", "rows": total_rows, "secs": elapsed}

    except Exception as exc:
        elapsed = round(time.time() - t0, 1)
        mark(conn, checkpoint_key, "failed", total_rows, str(exc))
        try:
            conn.close()
        except Exception:
            pass
        return {"status": f"FAILED: {exc}", "rows": total_rows, "secs": elapsed}


# ── Main ───────────────────────────────────────────────────────────────

def main():
    print(f"\n{'='*70}", flush=True)
    print(f"  Marital Status Standardisation UPDATE — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  target     : {TARGET_TABLE}")
    print(f"  batch_key  : {BATCH_KEY}")
    print(f"  batch_size : {BATCH_SIZE:,}")
    print(f"  passes     : 1  (pat_marital_status → pat_marital_status_std)")
    print(f"{'='*70}\n", flush=True)

    print("  Connecting to database...")
    sys.stdout.flush()
    ranges = setup_tables()

    if not ranges:
        print(f"\nNo eligible rows found in {TARGET_TABLE}. Exiting.")
        return

    any_failed = False
    with tqdm(total=len(ranges), desc="Pass 1", unit="batch") as pbar:
        print(f"\n  Starting Pass 1 — marital status standardisation ({len(ranges)} batches)...")
        result = run_pass(CHECKPOINT_PASS1, build_pass1, ranges, pbar)

    status = result["status"]
    rows   = result["rows"]
    secs   = result["secs"]

    if status == "done":
        tag = " DONE"
    elif status == "skipped":
        tag = " SKIP"
    else:
        tag = " FAIL"
        any_failed = True

    print(f"\n{'='*70}")
    print(f"  Per-pass summary:")
    print(f"  [{tag}] Pass 1 — marital status standardisation (all rows)  {rows:>10,} rows  ({secs}s)")
    if status.startswith("FAILED"):
        print(f"         {status}")

    print(f"\n  Total rows updated: {rows:,}")
    print(f"{'='*70}")

    print(f"\n  Cleanup SQL (run after verifying data):")
    print(f"    DROP TABLE IF EXISTS {STAGING_PK};")
    print(f"    DROP TABLE IF EXISTS {CHECKPOINT_TABLE};")

    if any_failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
