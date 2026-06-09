#!/usr/bin/env python3
"""
Optimized batched UPDATE for: rgd_udm_silver.patients

Operation: UPDATE rgd_udm_silver.patients rp
           JOIN (SELECT FLOOR(TRIM(pa.PatientID)) AS ndid, PatDeathFlag
                 FROM {SOURCE_SCHEMA}.Person p
                 JOIN {SOURCE_SCHEMA}.Patient pa ON p.PersonID = pa.PersonID
                 JOIN rgd_udm_silver.patients rp ON rp.ndid = FLOOR(TRIM(pa.PatientID))
                 WHERE rp.psid = {PSID}) b ON rp.ndid = b.ndid
           SET rp.pat_deceased_status = b.PatDeathFlag
           WHERE rp.psid = {PSID}

Optimizations applied:
- Pre-materialize source subquery (Person JOIN Patient) into staging lookup ONCE
- Batch JOIN UPDATE by actual ndid values from the staging lookup
- Server-side boundary sampling (avoids loading all PKs into memory)
- Commit after every batch (frees undo/log space)
- Checkpoint/resume — re-run skips if already completed
- Disabled InnoDB checks per-session for bulk update speed
- Progress bar via tqdm

Usage:
    python deseased_date_update_opt.py
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

# ── Change these two variables to run for a different schema/psid ─────
SOURCE_SCHEMA = "jwm"   # e.g. "mind", "savannah", "jwm", ...
PSID          = 11

# ─────────────────────────────────────────────────────────────────────
TARGET_TABLE     = "rgd_udm_silver.patients"
STAGING_LOOKUP   = f"staging.tmp_deceased_lookup_{SOURCE_SCHEMA}_psid{PSID}"
STAGING_PK       = f"staging.tmp_deceased_pk_{SOURCE_SCHEMA}_psid{PSID}"
CHECKPOINT_TABLE = f"staging.etl_checkpoint_deceased_{SOURCE_SCHEMA}_psid{PSID}"
CHECKPOINT_KEY   = f"patients.deceased_update.{SOURCE_SCHEMA}.psid{PSID}"

BATCH_KEY = "ndid"


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


# ── Batch UPDATE builder ──────────────────────────────────────────────

def build_batch_update(pk_lo, pk_hi):
    return f"""
UPDATE {TARGET_TABLE} rp
JOIN {STAGING_LOOKUP} b ON rp.ndid = b.ndid
SET rp.pat_deceased_status = b.PatDeathFlag
WHERE rp.psid = {PSID}
  AND b.ndid >= {pk_lo}
  AND b.ndid < {pk_hi}
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


# ── Setup ─────────────────────────────────────────────────────────────

def setup_tables():
    """
    1. Pre-materialize Person + Patient subquery into STAGING_LOOKUP (indexed on ndid).
    2. Create PK staging from STAGING_LOOKUP for batch boundary sampling.
    3. Create checkpoint table.
    4. Compute batch ranges from actual ndid values.
    Returns list of (lo, hi) ranges.
    """
    conn = get_connection()
    cur  = conn.cursor()

    # ── 1. Source lookup: Person JOIN Patient (computed once) ─────────
    print(f"  Materializing source lookup ({SOURCE_SCHEMA}.Person + Patient)...")
    if not _table_exists(cur, STAGING_LOOKUP):
        cur.execute(f"""
            CREATE TABLE {STAGING_LOOKUP} AS
            SELECT
                FLOOR(TRIM(pa.PatientID)) AS ndid,
                pa.PatDeathFlag
            FROM {SOURCE_SCHEMA}.Person p
            JOIN {SOURCE_SCHEMA}.Patient pa
                ON p.PersonID = pa.PersonID
            JOIN {TARGET_TABLE} rp
                ON rp.ndid = FLOOR(TRIM(pa.PatientID))
            WHERE rp.psid = {PSID}
        """)
        cur.execute(f"ALTER TABLE {STAGING_LOOKUP} ADD INDEX idx_ndid (ndid)")
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")
    cur.execute(f"SELECT COUNT(*) FROM {STAGING_LOOKUP}")
    print(f"    {cur.fetchone()[0]:,} rows in lookup")

    # ── 2. PK staging for boundary sampling ───────────────────────────
    print("  Creating PK staging table...")
    if not _table_exists(cur, STAGING_PK):
        cur.execute(f"""
            CREATE TABLE {STAGING_PK} AS
            SELECT ndid
            FROM {STAGING_LOOKUP}
            WHERE ndid IS NOT NULL
        """)
        cur.execute(f"ALTER TABLE {STAGING_PK} ADD INDEX idx_pk (ndid)")
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")

    cur.execute(f"SELECT COUNT(*) FROM {STAGING_PK}")
    total = cur.fetchone()[0]
    print(f"    {total:,} rows to update")

    # ── 3. Checkpoint table ───────────────────────────────────────────
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

    # ── 4. Batch boundary sampling ────────────────────────────────────
    print("  Computing batch boundaries...")
    sys.stdout.flush()

    if total == 0:
        cur.close()
        conn.close()
        return []

    cur.execute(f"""
        SELECT ndid
        FROM (
            SELECT ndid,
                   ROW_NUMBER() OVER (ORDER BY ndid) AS rn
            FROM {STAGING_PK}
        ) t
        WHERE (rn - 1) % {BATCH_SIZE} = 0
        ORDER BY ndid
    """)
    boundaries = [row[0] for row in cur.fetchall() if row[0] is not None]

    cur.execute(f"SELECT MAX(ndid) FROM {STAGING_PK}")
    max_pk = int(cur.fetchone()[0])

    cur.close()
    conn.close()

    ranges = []
    for i, lo in enumerate(boundaries):
        hi = boundaries[i + 1] if i + 1 < len(boundaries) else max_pk + 1
        ranges.append((lo, hi))

    print(f"    {len(ranges)} batches of ~{BATCH_SIZE:,} rows each")
    return ranges


# ── Runner ────────────────────────────────────────────────────────────

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

        for pk_lo, pk_hi in ranges:
            sql = build_batch_update(pk_lo, pk_hi)
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


# ── Main ──────────────────────────────────────────────────────────────

def main():
    print(f"\n{'='*70}", flush=True)
    print(f"  deceased_date UPDATE — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  source     : {SOURCE_SCHEMA}.Person + {SOURCE_SCHEMA}.Patient  (psid={PSID})")
    print(f"  target     : {TARGET_TABLE}")
    print(f"  set        : pat_deceased_status = PatDeathFlag")
    print(f"  lookup     : {STAGING_LOOKUP}")
    print(f"  staging_pk : {STAGING_PK}")
    print(f"  checkpoint : {CHECKPOINT_TABLE}")
    print(f"  batch_size : {BATCH_SIZE:,}")
    print(f"{'='*70}\n", flush=True)

    print("  Connecting to database...")
    sys.stdout.flush()
    ranges = setup_tables()

    if not ranges:
        print(f"\nNo eligible rows found for {SOURCE_SCHEMA} psid={PSID}. Exiting.")
        return

    result = None
    with tqdm(total=len(ranges), desc="UPDATE", unit="batch") as pbar:
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

    print(f"\n  Cleanup SQL (run after verifying data):")
    print(f"    DROP TABLE IF EXISTS {STAGING_LOOKUP};")
    print(f"    DROP TABLE IF EXISTS {STAGING_PK};")
    print(f"    DROP TABLE IF EXISTS {CHECKPOINT_TABLE};")

    if result["status"].startswith("FAILED"):
        sys.exit(1)


if __name__ == "__main__":
    main()
