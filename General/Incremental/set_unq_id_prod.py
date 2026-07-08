#!/usr/bin/env python3
"""
set_unq_id_prod.py — Batched UPDATE to deactivate prod rows matched by udm_unq_id

SQL equivalent:
    UPDATE {PROD_TABLE} p
    INNER JOIN {DELTA_TABLE} d
        ON  p.udm_unq_id     = d.udm_unq_id
        AND p.udm_active_flag = 'Y'
        AND p.psid            = d.psid
        AND p.psid            = {PSID}
    SET
        p.udm_active_flag  = 'N',
        p.updated_datetime = NOW();

Change PROD_TABLE, DELTA_TABLE, and PSID at the top to run for any table/psid.

Single pass with checkpoint/resume:
  - Filters eligible prod rows (udm_active_flag='Y' AND psid=PSID) into a PK staging table once
  - Batches the JOIN UPDATE by udm_inc_id using actual key values (sparse-ID safe)
  - Commits after every batch (frees undo/log space)
  - Re-running skips already-completed work via checkpoint

Usage:
    python set_unq_id_prod.py
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
    "database":        "rgd_udm_staging",
    "charset":         "utf8mb4",
    "connect_timeout": 30,
    "read_timeout":    21600,
    "write_timeout":   21600,
}

BATCH_SIZE = 50_000
BATCH_KEY  = "udm_inc_id"

# ── Change these to run against a different table / psid ──────────────
PROD_TABLE  = "rgd_udm_staging.diagnosis"   # table being deactivated
DELTA_TABLE = "udm_staging.diagnosis"       # new/incoming data to match against
PSID        = 11

# ─────────────────────────────────────────────────────────────────────
_TABLE_SUFFIX = f"{PROD_TABLE.replace('.', '_').replace('-', '_')}_psid{PSID}"

STAGING_PK       = f"staging.set_unq_prod_pk_n3_{_TABLE_SUFFIX}"
CHECKPOINT_TABLE = f"staging.etl_checkpoint_set_unq_prod_n3_{_TABLE_SUFFIX}"
CHECKPOINT_KEY   = f"set_unq_id_prod.{_TABLE_SUFFIX}"


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


def _ensure_index(cur, conn, full_table_name, index_name, columns, prefix_len=None):
    """
    Creates an index on full_table_name(columns) if it does not already exist.
    columns : list of column names, e.g. ['udm_unq_id'] or ['psid', 'udm_active_flag']
    prefix_len : optional int — applied to ALL columns in the index (useful for TEXT/VARCHAR(>191))
    """
    schema, table = full_table_name.split(".", 1)
    cur.execute(
        "SELECT COUNT(*) FROM information_schema.statistics "
        "WHERE table_schema = %s AND table_name = %s AND index_name = %s",
        (schema, table, index_name),
    )
    if cur.fetchone()[0] > 0:
        print(f"    index {index_name} on {full_table_name} already exists — skipping")
        return

    if prefix_len:
        col_list = ", ".join(f"{c}({prefix_len})" for c in columns)
    else:
        col_list = ", ".join(columns)

    print(f"    creating index {index_name} on {full_table_name}({col_list}) ...")
    cur.execute(
        f"ALTER TABLE {full_table_name} ADD INDEX {index_name} ({col_list})"
    )
    conn.commit()
    print(f"    done")


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
    return f"""
UPDATE {PROD_TABLE} p
INNER JOIN {DELTA_TABLE} d
    ON  p.udm_unq_id     = d.udm_unq_id
    AND p.psid            = d.psid
SET
    p.udm_active_flag  = 'N',
    p.updated_datetime = NOW()
WHERE p.udm_active_flag = 'Y'
  AND p.psid            = {PSID}
  AND p.{BATCH_KEY}    >= {pk_lo}
  AND p.{BATCH_KEY}    <  {pk_hi}
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

        # ── 2. Ensure indexes on JOIN/filter columns ─────────────────
        # JOIN key: udm_unq_id on both tables
        # Filter:   (psid, udm_active_flag) on PROD_TABLE — avoids full scan
        print("  Ensuring indexes on prod and delta tables...")
        _ensure_index(cur, conn, PROD_TABLE,  "idx_unq_id_prod",  ["udm_unq_id"],               prefix_len=100)
        _ensure_index(cur, conn, PROD_TABLE,  "idx_psid_active",  ["psid", "udm_active_flag"])
        _ensure_index(cur, conn, DELTA_TABLE, "idx_unq_id_delta", ["udm_unq_id"],               prefix_len=100)

        # ── 3. PK staging — eligible prod rows only ──────────────────
        # Filters to udm_active_flag='Y' AND psid=PSID so batches only
        # touch rows that could actually be deactivated.
        print(f"  Creating PK staging (udm_active_flag='Y' AND psid={PSID})...")
        if not _table_exists(cur, STAGING_PK):
            cur.execute(f"""
                CREATE TABLE {STAGING_PK} AS
                SELECT {BATCH_KEY}
                FROM {PROD_TABLE}
                WHERE udm_active_flag = 'Y'
                  AND psid            = {PSID}
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
    print(f"  Set udm_unq_id Prod Deactivation — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  prod table   : {PROD_TABLE}")
    print(f"  delta table  : {DELTA_TABLE}")
    print(f"  psid         : {PSID}")
    print(f"  batch_key    : {BATCH_KEY}")
    print(f"  batch_size   : {BATCH_SIZE:,}")
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
        print(f"  DONE   {rows:,} rows deactivated  ({secs}s)")
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
