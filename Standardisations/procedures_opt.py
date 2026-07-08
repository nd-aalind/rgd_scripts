#!/usr/bin/env python3
"""
Optimized procedures standardisation UPDATE for: rgd_udm_silver.procedures

Joins to two lookup tables:
  - semantics.hcpcs             (h) — matched by proc_code = HCPC
  - tncpa.PROCEDURECODEREFERENCE (t) — matched by proc_code = PROCEDURECODE

Updates four columns:
  - proc_code_std          (standardised procedure code)
  - proc_coding_system_std (HCPCS or CPT, inferred via REGEXP)
  - proc_name_std          (short description)
  - proc_description_std   (long description)

Filters: proc_code IS NOT NULL AND proc_code <> ''
Staging pre-applies the same filter — no wasted batches on NULL/empty codes.

Optimizations applied:
- Staging pre-filters to rows with non-null, non-empty proc_code
- Batch by actual primary key values (not arithmetic ranges — IDs can be sparse)
- Server-side boundary sampling (avoids loading millions of PKs into memory)
- Commit after every batch (frees undo/log space)
- Checkpoint/resume — re-run skips if already completed
- Disabled InnoDB checks per-session for bulk update speed
- Progress bar via tqdm

Usage:
    python procedures_opt.py
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
    "database":        "rgd_udm_silver",
    "charset":         "utf8mb4",
    "connect_timeout": 30,
    "read_timeout":    21600,
    "write_timeout":   21600,
}

BATCH_SIZE = 50_000   # rows per batch

TARGET_TABLE     = "kinsula_leq.procedures"
STAGING_TABLE    = "staging.tmp_procedures_std_staging_new_2"
CHECKPOINT_TABLE = "staging.etl_checkpoint_procedures_std_new_2"

# ── Primary key column used for batching ─────────────────────────────
BATCH_KEY = "udm_inc_id"

# Checkpoint key — single entry since this is one UPDATE operation
CHECKPOINT_KEY = "procedures.proc_code_std_update"


# ── Helpers ──────────────────────────────────────────────────────────

def get_connection():
    """One connection per call."""
    return pymysql.connect(**DB_CONFIG)


def build_batch_update(pk_lo, pk_hi):
    """
    Faithfully reproduces the original UPDATE ... JOIN logic in batched form.
    Two LEFT JOINs to different lookup tables:
      h — semantics.hcpcs             (matched by proc_code = HCPC)
      t — tncpa.PROCEDURECODEREFERENCE (matched by proc_code = PROCEDURECODE)
    h takes priority over t in all CASE expressions.
    Original WHERE filter (proc_code IS NOT NULL AND proc_code <> '')
    is combined with the batch range filter.
    """
    return f"""
UPDATE {TARGET_TABLE} p
LEFT JOIN semantics.hcpcs h ON p.proc_code = h.HCPC
LEFT JOIN tncpa.PROCEDURECODEREFERENCE t ON p.proc_code = t.PROCEDURECODE
SET
    p.proc_code_std = CASE
        WHEN h.HCPC IS NOT NULL THEN h.HCPC
        WHEN t.PROCEDURECODE IS NOT NULL THEN t.PROCEDURECODE
        ELSE 'NS'
    END,
    p.proc_coding_system_std = CASE
        WHEN h.HCPC IS NOT NULL THEN 'HCPCS'
        WHEN t.PROCEDURECODE IS NOT NULL
             AND t.PROCEDURECODE REGEXP '^[0-9]{{5}}$' THEN 'CPT'
        WHEN t.PROCEDURECODE IS NOT NULL
             AND t.PROCEDURECODE REGEXP '^[A-Z][0-9]{{4}}$' THEN 'HCPCS'
        ELSE 'NS'
    END,
    p.proc_name_std = CASE
        WHEN h.HCPC IS NOT NULL THEN h.`SHORT DESCRIPTION`
        WHEN t.PROCEDURECODE IS NOT NULL THEN t.COMMONDESCRIPTION
        ELSE 'NS'
    END,
    p.proc_description_std = CASE
        WHEN h.HCPC IS NOT NULL THEN h.`LONG DESCRIPTION`
        WHEN t.PROCEDURECODE IS NOT NULL THEN t.DESCRIPTION
        ELSE 'NS'
    END
WHERE p.proc_code IS NOT NULL
  AND p.proc_code <> ''
  AND p.{BATCH_KEY} >= {pk_lo} AND p.{BATCH_KEY} < {pk_hi}
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
    """Create staging and checkpoint tables. Return pk ranges."""
    conn = get_connection()
    cur = conn.cursor()

    # ── 1. Staging table: pre-filter to rows with valid proc_code ────
    print("  Creating staging table (non-null, non-empty proc_code rows)...")
    cur.execute(
        "SELECT COUNT(*) FROM information_schema.tables "
        "WHERE table_schema = %s AND table_name = %s",
        (STAGING_TABLE.split(".")[0], STAGING_TABLE.split(".")[1]),
    )
    if cur.fetchone()[0] == 0:
        cur.execute(f"""
            CREATE TABLE {STAGING_TABLE} AS
            SELECT {BATCH_KEY}
            FROM {TARGET_TABLE}
            WHERE {BATCH_KEY} IS NOT NULL
              AND proc_code IS NOT NULL
              AND proc_code <> ''
        """)
        cur.execute(f"ALTER TABLE {STAGING_TABLE} ADD INDEX idx_pk ({BATCH_KEY})")
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")

    cur.execute(f"SELECT COUNT(*) FROM {STAGING_TABLE}")
    row_count = cur.fetchone()[0]
    print(f"    {row_count:,} rows to update")

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

    # ── 3. Compute batch ranges via server-side boundary sampling ────
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

    print(f"    {len(ranges)} batches of ~{BATCH_SIZE:,} rows each")
    return ranges


# ── Runner ───────────────────────────────────────────────────────────

def run_update(ranges, pbar):
    """Execute the procedures standardisation UPDATE across all batches."""
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

        # Disable InnoDB checks for bulk update speed (session-scoped only)
        cur.execute("SET unique_checks = 0")
        cur.execute("SET foreign_key_checks = 0")

        for pk_lo, pk_hi in ranges:
            sql = build_batch_update(pk_lo, pk_hi)
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
    print(f"  Procedures Standardisation UPDATE — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  target     : {TARGET_TABLE}")
    print(f"  lookup 1   : semantics.hcpcs              (matched by proc_code = HCPC)")
    print(f"  lookup 2   : tncpa.PROCEDURECODEREFERENCE (matched by proc_code = PROCEDURECODE)")
    print(f"  filter     : proc_code IS NOT NULL AND proc_code <> ''")
    print(f"  batch_key  : {BATCH_KEY}")
    print(f"  staging    : {STAGING_TABLE}")
    print(f"  batch_size : {BATCH_SIZE:,}")
    print(f"{'='*70}\n", flush=True)

    print("  Connecting to database...")
    sys.stdout.flush()
    ranges = setup_tables()
    if not ranges:
        print(f"\nNo eligible rows found in {TARGET_TABLE}. Exiting.")
        return

    result = None
    with tqdm(total=len(ranges), desc="Overall", unit="batch") as pbar:
        result = run_update(ranges, pbar)

    print()
    if result["status"] == "done":
        tag = " DONE"
    elif result["status"] == "skipped":
        tag = " SKIP"
    else:
        tag = " FAIL"

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
