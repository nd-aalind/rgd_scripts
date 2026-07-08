#!/usr/bin/env python3
"""
notes_dent_nw_inc.py — Note incremental ETL (Dent NW)

Creates biogen_april.note_inc_may_dent_nw as a patient-cohort + date-filtered
subset of rgd_udm_silver.notes_part2 where psid IN (1, 4).

Patient cohort : biogen_april.patients_demo_inc_combined (pre-existing, JOIN on ndid)
Date range     : enc_start_date 2026-02-16 → 2026-03-31
psid filter    : IN (1, 4)
Batching       : udm_inc_id (falls back to ndid if column absent)
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
    "user":            os.environ.get("DB_ADMIN_USER"),
    "password":        os.environ.get("DB_ADMIN_PASSWORD"),
    "database":        "rgd_udm_silver",
    "charset":         "utf8mb4",
    "connect_timeout": 30,
    "read_timeout":    21600,
    "write_timeout":   21600,
}

SOURCE_TABLE = "rgd_udm_silver.notes_part2"
COHORT_TABLE = "biogen_april.patients_demo_inc_combined"
TARGET_TABLE = "biogen_april.note_additional_dent_nw"
STAGING_PK   = "staging.notes_dent_nw_inc_pk_n"
CKPT_TABLE   = "staging.etl_checkpoint_notes_dent_nw_inc_n"
CKPT_KEY     = "notes_dent_nw_inc"

BATCH_SIZE = 50_000
BATCH_KEY  = "ndid"   # falls back to ndid if not present in source

DATE_COL = "enc_start_date"
DATE_LO  = "2026-02-16"
DATE_HI  = "2026-03-31"


# ── Helpers ───────────────────────────────────────────────────────────

def get_connection():
    return pymysql.connect(**DB_CONFIG)


def _table_exists(cur, full_table_name: str) -> bool:
    schema, table = full_table_name.split(".", 1)
    cur.execute(
        "SELECT COUNT(*) FROM information_schema.tables "
        "WHERE table_schema = %s AND table_name = %s",
        (schema, table),
    )
    return cur.fetchone()[0] > 0


def _col_exists(cur, schema: str, table: str, column: str) -> bool:
    cur.execute(
        "SELECT COUNT(*) FROM information_schema.columns "
        "WHERE table_schema = %s AND table_name = %s AND column_name = %s",
        (schema, table, column),
    )
    return cur.fetchone()[0] > 0


def _index_exists(cur, schema: str, table: str, column: str) -> bool:
    cur.execute(
        "SELECT COUNT(*) FROM information_schema.statistics "
        "WHERE table_schema = %s AND table_name = %s AND column_name = %s",
        (schema, table, column),
    )
    return cur.fetchone()[0] > 0


def _ensure_index(cur, conn, schema: str, table: str, column: str) -> None:
    if not _col_exists(cur, schema, table, column):
        return
    if not _index_exists(cur, schema, table, column):
        print(f"    Creating index on {schema}.{table} ({column})...")
        cur.execute(f"CREATE INDEX idx_{column} ON {schema}.{table} ({column})")
        conn.commit()
        print(f"      done")
    else:
        print(f"    Index on {schema}.{table} ({column}) already exists")


def ensure_indexes() -> None:
    """Ensure only the small cohort table has an ndid index.
    We intentionally skip indexing the silver source table — it is massive and
    any ALTER TABLE on it would lock the production table for hours."""
    coh_schema, coh_table = COHORT_TABLE.split(".", 1)

    conn = get_connection()
    cur  = conn.cursor()
    print("  Ensuring indexes...")
    try:
        _ensure_index(cur, conn, coh_schema, coh_table, "ndid")
    finally:
        cur.close()
        conn.close()


def _build_ranges(cur, staging_pk: str, key: str):
    cur.execute(f"SELECT COUNT(*) FROM {staging_pk}")
    total = cur.fetchone()[0]
    if total == 0:
        return [], 0

    cur.execute(f"""
        SELECT {key}
        FROM (
            SELECT {key},
                   ROW_NUMBER() OVER (ORDER BY {key}) AS rn
            FROM {staging_pk}
        ) t
        WHERE (rn - 1) % {BATCH_SIZE} = 0
        ORDER BY {key}
    """)
    boundaries = [row[0] for row in cur.fetchall() if row[0] is not None]

    cur.execute(f"SELECT MAX({key}) FROM {staging_pk}")
    max_pk = int(cur.fetchone()[0])

    ranges = []
    for i, lo in enumerate(boundaries):
        hi = boundaries[i + 1] if i + 1 < len(boundaries) else max_pk + 1
        ranges.append((lo, hi))

    return ranges, total


# ── Checkpoint ────────────────────────────────────────────────────────

def is_done(conn) -> bool:
    cur = conn.cursor()
    cur.execute(
        f"SELECT status FROM {CKPT_TABLE} WHERE source_key = %s",
        (CKPT_KEY,),
    )
    row = cur.fetchone()
    cur.close()
    return row is not None and row[0] == "done"


def mark(conn, status: str, rows: int = 0, error: str = None) -> None:
    cur = conn.cursor()
    cur.execute(f"""
        INSERT INTO {CKPT_TABLE}
            (source_key, status, rows_inserted, started_at, completed_at, error_msg)
        VALUES (%s, %s, %s, NOW(), IF(%s = 'done', NOW(), NULL), %s)
        ON DUPLICATE KEY UPDATE
            status        = VALUES(status),
            rows_inserted = VALUES(rows_inserted),
            completed_at  = IF(VALUES(status) = 'done', NOW(), NULL),
            error_msg     = VALUES(error_msg)
    """, (CKPT_KEY, status, rows, status, error))
    conn.commit()
    cur.close()


# ── Setup ─────────────────────────────────────────────────────────────

def setup_tables():
    conn = get_connection()
    cur  = conn.cursor()

    print("  Ensuring staging schema exists...")
    cur.execute("CREATE DATABASE IF NOT EXISTS staging")
    conn.commit()

    print(f"  Verifying cohort table {COHORT_TABLE}...")
    if not _table_exists(cur, COHORT_TABLE):
        print(f"  [ERROR] {COHORT_TABLE} does not exist — cannot proceed.")
        sys.exit(1)
    cur.execute(f"SELECT COUNT(*) FROM {COHORT_TABLE}")
    print(f"    {cur.fetchone()[0]:,} patients in cohort")

    print(f"  Creating target table {TARGET_TABLE} if needed...")
    if not _table_exists(cur, TARGET_TABLE):
        cur.execute(f"""
            CREATE TABLE {TARGET_TABLE}
            SELECT
                a.ndid,
                a.eid                AS encounter_id,
                a.enc_start_date     AS encounter_date,
                a.note_type,
                a.note_source,
                a.note,
                CAST(NULL AS SIGNED) AS incremental_id,
                NULL                 AS udm_active_flag,
                NULL                 AS udm_unq_id
            FROM {SOURCE_TABLE} a
            INNER JOIN {COHORT_TABLE} c ON a.ndid = c.ndid
            WHERE 1 = 0
        """)
        conn.commit()
        print("    created (empty)")
    else:
        print("    already exists — will append")

    # Determine effective batch key
    src_schema, src_table = SOURCE_TABLE.split(".", 1)
    if _col_exists(cur, src_schema, src_table, BATCH_KEY):
        eff_key = BATCH_KEY
    else:
        eff_key = "ndid"
        print(f"  [INFO] '{BATCH_KEY}' not found in {SOURCE_TABLE} — batching by 'ndid'")

    print("  Creating checkpoint table...")
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {CKPT_TABLE} (
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

    print(f"  Creating staging PK table {STAGING_PK}...")
    if not _table_exists(cur, STAGING_PK):
        # STRAIGHT_JOIN forces cohort (24K rows) as the driving table so MySQL
        # performs 24K ndid lookups into notes_part2 instead of a full table scan.
        cur.execute(f"""
            CREATE TABLE {STAGING_PK} AS
            SELECT STRAIGHT_JOIN a.{eff_key}
            FROM {COHORT_TABLE} c
            INNER JOIN {SOURCE_TABLE} a ON a.ndid = c.ndid
            WHERE a.{eff_key} IS NOT NULL
              AND a.{DATE_COL} >= '{DATE_LO}'
              AND a.{DATE_COL} <= '{DATE_HI}'
              AND a.psid IN (1)
        """)
        cur.execute(f"ALTER TABLE {STAGING_PK} ADD INDEX idx_pk ({eff_key})")
        conn.commit()
        cur.execute(f"SELECT COUNT(*) FROM {STAGING_PK}")
        n = cur.fetchone()[0]
        print(f"    {n:,} eligible rows")
    else:
        cur.execute(f"SELECT COUNT(*) FROM {STAGING_PK}")
        n = cur.fetchone()[0]
        print(f"    already exists, reusing  ({n:,} rows)")

    ranges, total = _build_ranges(cur, STAGING_PK, eff_key)
    print(f"    {len(ranges)} batches of ~{BATCH_SIZE:,} rows  (total: {total:,})")

    cur.close()
    conn.close()
    return ranges, eff_key


# ── Runner ────────────────────────────────────────────────────────────

def run_insert(ranges, eff_key, pbar):
    conn = get_connection()
    t0   = time.time()
    total_rows = 0

    if is_done(conn):
        conn.close()
        pbar.update(len(ranges))
        return {"status": "skipped", "rows": 0, "secs": 0.0}

    mark(conn, "running")

    try:
        cur = conn.cursor()
        cur.execute("SET unique_checks = 0")
        cur.execute("SET foreign_key_checks = 0")

        for lo, hi in ranges:
            # Drive from staging_pk (small batch slice) → look up into notes_part2,
            # avoiding a full scan of the silver table on each batch.
            sql = f"""
INSERT INTO {TARGET_TABLE}
SELECT STRAIGHT_JOIN
    a.ndid,
    a.eid                AS encounter_id,
    a.enc_start_date     AS encounter_date,
    a.note_type,
    a.note_source,
    a.note,
    CAST(NULL AS SIGNED) AS incremental_id,
    NULL                 AS udm_active_flag,
    NULL                 AS udm_unq_id
FROM {STAGING_PK} pk
INNER JOIN {SOURCE_TABLE} a ON a.{eff_key} = pk.{eff_key}
WHERE pk.{eff_key} >= {lo}
  AND pk.{eff_key} <  {hi}
"""
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
    print(f"  Note Incremental ETL (Dent NW) — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  source      : {SOURCE_TABLE}")
    print(f"  cohort      : {COHORT_TABLE}")
    print(f"  target      : {TARGET_TABLE}")
    print(f"  date range  : {DATE_LO}  to  {DATE_HI}")
    print(f"  psid filter : IN (1)")
    print(f"  checkpoint  : {CKPT_TABLE}")
    print(f"  batch size  : {BATCH_SIZE:,}")
    print(f"{'='*70}\n", flush=True)

    ensure_indexes()
    print()
    ranges, eff_key = setup_tables()

    if not ranges:
        print("  No eligible rows found. Exiting.")
        return

    result = None
    with tqdm(total=len(ranges), desc="notes_dent_nw", unit="batch") as pbar:
        result = run_insert(ranges, eff_key, pbar)

    print()
    tag = "DONE" if result["status"] == "done" else \
          "SKIP" if result["status"] == "skipped" else "FAIL"

    print(f"\n{'='*70}")
    print(f"  [{tag}]  {result['rows']:>12,} rows inserted  ({result['secs']}s)")
    if result["status"].startswith("FAILED"):
        print(f"  ERROR: {result['status']}")
    print(f"{'='*70}")

    conn = get_connection()
    cur  = conn.cursor()
    try:
        if _table_exists(cur, TARGET_TABLE):
            cur.execute(f"SELECT COUNT(*), COUNT(DISTINCT ndid) FROM {TARGET_TABLE}")
            row = cur.fetchone()
            print(f"\n  {TARGET_TABLE}")
            print(f"    rows          : {row[0]:,}")
            print(f"    distinct ndid : {row[1]:,}")
    finally:
        cur.close()
        conn.close()

    print(f"\n  Cleanup SQL (run after verifying data):")
    print(f"    DROP TABLE IF EXISTS {STAGING_PK};")
    print(f"    DROP TABLE IF EXISTS {CKPT_TABLE};")
    print()

    if result["status"].startswith("FAILED"):
        sys.exit(1)


if __name__ == "__main__":
    main()
