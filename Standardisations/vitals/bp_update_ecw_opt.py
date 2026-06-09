#!/usr/bin/env python3
"""
bp_update_ecw_opt.py — Batched UPDATE to set vital_result_std/vital_unit_std = 'NS'
                        for blood pressure vital names on specific psids.

SQL equivalent:
    UPDATE rgd_udm_silver.vitals_dedup
    SET vital_result_std = 'NS',
        vital_unit_std   = 'NS'
    WHERE vital_name IN ('Blood pressure (BP)', 'BP', ...)
      AND psid IN (1,3,4,8,13,14);

Single pass with checkpoint/resume:
  - Eligible rows (vital_name IN / psid IN) pre-filtered into PK staging once
  - Batches by udm_inc_id using actual key values (sparse-ID safe)
  - Commits after every batch (frees undo/log space)
  - Re-running skips already-completed work via checkpoint

Usage:
    python bp_update_ecw_opt.py
"""

import sys
import time
from datetime import datetime
import pymysql
from tqdm import tqdm

# ── Configuration ─────────────────────────────────────────────────────
DB_CONFIG = {
    "host":            "ndai-dev-rds-instance.cwp60ymu4ko0.us-east-1.rds.amazonaws.com",
    "port":            3306,
    "user":            "Aalind",
    "password":        "A@L1nd@123",
    "database":        "rgd_udm_silver",
    "charset":         "utf8mb4",
    "connect_timeout": 30,
    "read_timeout":    21600,
    "write_timeout":   21600,
}

BATCH_SIZE = 500_000
BATCH_KEY  = "udm_inc_id"

# ── Change these to adjust scope ──────────────────────────────────────
TARGET_TABLE = "rgd_udm_silver.vitals_dedup"

BP_VITAL_NAMES = (
    "Blood pressure (BP)",
    "Blood pressure, sitting",
    "Blood pressure, standing",
    "Blood pressure, supine",
    "BP",
    "BP - Lying",
    "BP - Sitting",
    "BP - Standing",
    "BP sitting",
    "BP standing",
    "BP supine",
    "BP-Treatment",
    "BP:",
    "Diastolic BP:",
    "Repeat blood pressure",
    "Repeat BP",
    "Systolic BP:",
)

PSID_LIST = (1, 3, 4, 8, 13, 14)

# ─────────────────────────────────────────────────────────────────────
_TABLE_SUFFIX = TARGET_TABLE.replace(".", "_").replace("-", "_")

STAGING_PK       = f"staging.bp_upd_ecw_pk_{_TABLE_SUFFIX}"
CHECKPOINT_TABLE = f"staging.etl_checkpoint_bp_upd_ecw_{_TABLE_SUFFIX}"
CHECKPOINT_KEY   = f"bp_update_ecw.{_TABLE_SUFFIX}"

# Build SQL-safe literals once at module level
_NAMES_SQL = ", ".join(f"'{n}'" for n in BP_VITAL_NAMES)
_PSIDS_SQL = ", ".join(str(p) for p in PSID_LIST)


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

def build_update(pk_lo, pk_hi):
    # JOIN through the pre-filtered PK staging table instead of range-scanning
    # the full main table. This does exactly BATCH_SIZE PK index lookups on
    # vitals_dedup rather than scanning every row in [pk_lo, pk_hi) and
    # re-checking vital_name/psid — the eligible rows are only ~3.7% of the
    # table, so range scanning is ~27x more work than needed.
    return f"""
UPDATE {TARGET_TABLE} v
INNER JOIN (
    SELECT {BATCH_KEY}
    FROM {STAGING_PK}
    WHERE {BATCH_KEY} >= {pk_lo}
      AND {BATCH_KEY} <  {pk_hi}
) pk USING ({BATCH_KEY})
SET
    v.vital_result_std = 'NS',
    v.vital_unit_std   = 'NS'
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
    conn = get_connection()
    cur  = conn.cursor()

    try:
        # ── 1. Checkpoint table ──────────────────────────────────────
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

        # ── 2. PK staging — eligible rows only ──────────────────────
        # Pre-filters on vital_name IN / psid IN so batch boundaries
        # cover only rows that will actually be updated.
        print("  Creating PK staging (vital_name IN bp_list AND psid IN scope)...")
        if not _table_exists(cur, STAGING_PK):
            cur.execute(f"""
                CREATE TABLE {STAGING_PK} AS
                SELECT {BATCH_KEY}
                FROM {TARGET_TABLE}
                WHERE vital_name IN ({_NAMES_SQL})
                  AND psid       IN ({_PSIDS_SQL})
                  AND {BATCH_KEY} IS NOT NULL
            """)
            cur.execute(f"ALTER TABLE {STAGING_PK} ADD INDEX idx_pk ({BATCH_KEY})")
            conn.commit()
            cur.execute(f"SELECT COUNT(*) FROM {STAGING_PK}")
            n = cur.fetchone()[0]
            print(f"    created  ({n:,} eligible rows)")
        else:
            cur.execute(f"SELECT COUNT(*) FROM {STAGING_PK}")
            n = cur.fetchone()[0]
            print(f"    already exists, reusing  ({n:,} rows)")

        ranges, total = _build_ranges(cur, STAGING_PK)
        print(f"    {total:,} rows → {len(ranges)} batches of ~{BATCH_SIZE:,}")
        return ranges, total

    finally:
        cur.close()
        conn.close()


# ── Runner ────────────────────────────────────────────────────────────

def run(ranges, pbar):
    conn       = get_connection()
    t0         = time.time()
    total_rows = 0

    try:
        if is_done(conn):
            conn.close()
            pbar.update(len(ranges))
            return {"status": "skipped", "rows": 0, "secs": 0.0}

        mark(conn, "running")
        cur = conn.cursor()

        cur.execute("SET unique_checks = 0")
        cur.execute("SET foreign_key_checks = 0")

        for lo, hi in ranges:
            sql = build_update(lo, hi)
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
        err_msg = str(exc)
        print(f"\n  [ERROR] {err_msg}")
        try:
            mark(conn, "failed", total_rows, err_msg)
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass
        return {"status": f"FAILED: {exc}", "rows": total_rows, "secs": elapsed}


# ── Main ──────────────────────────────────────────────────────────────

def main():
    print(f"\n{'='*70}", flush=True)
    print(f"  BP vital_result_std/unit_std → 'NS' UPDATE — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  target     : {TARGET_TABLE}")
    print(f"  psids      : {list(PSID_LIST)}")
    print(f"  bp names   : {len(BP_VITAL_NAMES)} entries")
    print(f"  batch_key  : {BATCH_KEY}")
    print(f"  batch_size : {BATCH_SIZE:,}")
    print(f"{'='*70}\n", flush=True)

    print("Setup:")
    sys.stdout.flush()
    ranges, _ = setup_tables()
    print()

    if not ranges:
        print("  No eligible rows — nothing to do.")
        sys.exit(0)

    with tqdm(total=len(ranges), desc="Overall", unit="batch") as pbar:
        result = run(ranges, pbar)

    status = result["status"]
    rows   = result["rows"]
    secs   = result["secs"]

    print(f"\n{'='*70}")
    if status == "done":
        print(f"  DONE   {rows:,} rows updated  ({secs}s)")
    elif status == "skipped":
        print(f"  SKIPPED — already marked done in checkpoint")
    else:
        print(f"  FAILED — {status}")
    print(f"{'='*70}")

    print(f"\n  Cleanup SQL (run after verifying data):")
    print(f"    DROP TABLE IF EXISTS {STAGING_PK};")
    print(f"    DROP TABLE IF EXISTS {CHECKPOINT_TABLE};")
    print()

    if status.startswith("FAILED"):
        sys.exit(1)


if __name__ == "__main__":
    main()
