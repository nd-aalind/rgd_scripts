#!/usr/bin/env python3
"""
Optimized batched CREATE TABLE ETL: rgd_udm_silver.* → rgd_udm_staging.*

Copies 18 tables from the silver layer to staging, adding:
  - udm_active_flag = 'Y'  (all tables)
  - udm_unq_id = CONCAT_WS(...)  (tables that have a meaningful composite key)

Each table runs in parallel (ThreadPoolExecutor).
Each table has its own PK staging table and checkpoint entry — re-run skips
completed tables automatically.

Optimizations applied:
- Destination tables created with LIKE source (no full-table scan at creation)
- Per-table PK staging for boundary sampling
- Server-side boundary sampling (ROW_NUMBER — avoids loading all PKs into memory)
- Commit after every batch (frees undo/log space)
- Checkpoint/resume per table
- Disabled InnoDB checks per-session for bulk insert speed
- Parallel table copies via ThreadPoolExecutor
- Progress bar via tqdm

Usage:
    python create_table_opt.py
"""

import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
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

BATCH_SIZE  = 50_000
MAX_WORKERS = 4           # parallel table copies; reduce if DB is under heavy load
BATCH_KEY   = "udm_inc_id"   # integer PK present on all rgd_udm_silver tables

CHECKPOINT_TABLE = "staging.etl_checkpoint_create_staging"

# ── Table definitions ─────────────────────────────────────────────────
# Each entry:
#   source   : full source table name
#   dest     : full destination table name
#   unq_id   : CONCAT_WS expression for udm_unq_id, or None if not needed
#
# All tables also get: udm_active_flag = 'Y'

_UNQ = {
    "diagnosis":
        "CONCAT_WS(':', COALESCE(psid,''), COALESCE(ndid,''), COALESCE(eid,''),"
        " COALESCE(enc_date,''), COALESCE(diag_date,''),"
        " COALESCE(diag_code,''), COALESCE(diag_desc,''))",

    "procedures":
        "CONCAT_WS(':', COALESCE(psid,''), COALESCE(ndid,''), COALESCE(eid,''),"
        " COALESCE(encounter_date,''), COALESCE(proc_start_date,''), COALESCE(proc_last_date,''),"
        " COALESCE(proc_code,''), COALESCE(proc_name,''))",

    "medications_part1":
        "CONCAT_WS(':', COALESCE(psid,''), COALESCE(ndid,''), COALESCE(eid,''),"
        " COALESCE(enc_date,''), COALESCE(med_start_date,''), COALESCE(med_end_date,''),"
        " COALESCE(med_code,''), COALESCE(med_name,''))",

    "medications_part2":
        "CONCAT_WS(':', COALESCE(psid,''), COALESCE(ndid,''), COALESCE(eid,''),"
        " COALESCE(enc_date,''), COALESCE(med_start_date,''), COALESCE(med_end_date,''),"
        " COALESCE(med_code,''), COALESCE(med_name,''))",

    "allergies":
        "CONCAT_WS(':', COALESCE(psid,''), COALESCE(ndid,''), COALESCE(enc_id,''),"
        " COALESCE(enc_date,''), COALESCE(allergy_start_date,''),"
        " COALESCE(allergen_code,''), COALESCE(allergen_name,''), COALESCE(allergy_type,''))",

    "labs":
        "CONCAT_WS(':', COALESCE(psid,''), COALESCE(ndid,''), COALESCE(eid,''),"
        " COALESCE(enc_start_date,''), COALESCE(result_date,''),"
        " COALESCE(result_code,''), COALESCE(result_name,''))",

    "encounters":
        "CONCAT_WS(':', COALESCE(psid,''), COALESCE(ndid,''), COALESCE(eid,''))",

    "examination":
        "CONCAT_WS(':', COALESCE(psid,''), COALESCE(ndid,''), COALESCE(eid,''))",
}

TABLES = [
    # Tables with udm_unq_id
    {"source": "rgd_udm_silver.diagnosis",          "dest": "rgd_udm_staging.diagnosis",          "unq_id": _UNQ["diagnosis"]},
    {"source": "rgd_udm_silver.procedures",          "dest": "rgd_udm_staging.procedures",          "unq_id": _UNQ["procedures"]},
    {"source": "rgd_udm_silver.medications_part1",   "dest": "rgd_udm_staging.medications_part1",   "unq_id": _UNQ["medications_part1"]},
    {"source": "rgd_udm_silver.medications_part2",   "dest": "rgd_udm_staging.medications_part2",   "unq_id": _UNQ["medications_part2"]},
    {"source": "rgd_udm_silver.allergies",           "dest": "rgd_udm_staging.allergies",           "unq_id": _UNQ["allergies"]},
    {"source": "rgd_udm_silver.labs",                "dest": "rgd_udm_staging.labs",                "unq_id": _UNQ["labs"]},
    {"source": "rgd_udm_silver.encounters",          "dest": "rgd_udm_staging.encounters",          "unq_id": _UNQ["encounters"]},
    {"source": "rgd_udm_silver.examination",         "dest": "rgd_udm_staging.examination",         "unq_id": _UNQ["examination"]},
    # Tables without udm_unq_id (only udm_active_flag added)
    {"source": "rgd_udm_silver.insurance",           "dest": "rgd_udm_staging.insurance",           "unq_id": None},
    {"source": "rgd_udm_silver.notes_part1",         "dest": "rgd_udm_staging.notes_part1",         "unq_id": None},
    {"source": "rgd_udm_silver.notes_part2",         "dest": "rgd_udm_staging.notes_part2",         "unq_id": None},
    {"source": "rgd_udm_silver.notes_part3",         "dest": "rgd_udm_staging.notes_part3",         "unq_id": None},
    {"source": "rgd_udm_silver.patients",            "dest": "rgd_udm_staging.patients",            "unq_id": None},
    {"source": "rgd_udm_silver.progressnotes_part1", "dest": "rgd_udm_staging.progressnotes_part1", "unq_id": None},
    {"source": "rgd_udm_silver.progressnotes_part2", "dest": "rgd_udm_staging.progressnotes_part2", "unq_id": None},
    {"source": "rgd_udm_silver.radiology",           "dest": "rgd_udm_staging.radiology",           "unq_id": None},
    {"source": "rgd_udm_silver.ros",                 "dest": "rgd_udm_staging.ros",                 "unq_id": None},
    {"source": "rgd_udm_silver.vitals",              "dest": "rgd_udm_staging.vitals",              "unq_id": None},
]


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


def _staging_name(dest_table):
    """Derive a short staging table name from the dest table name."""
    suffix = dest_table.split(".")[-1]   # e.g. 'diagnosis'
    return f"staging.tmp_crt_stg_{suffix}"


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


# ── Checkpoint ─────────────────────────────────────────────────────────

def is_done(conn, source_key):
    cur = conn.cursor()
    cur.execute(
        f"SELECT status FROM {CHECKPOINT_TABLE} WHERE source_key = %s",
        (source_key,),
    )
    row = cur.fetchone()
    cur.close()
    return row is not None and row[0] == "done"


def mark(conn, source_key, status, rows=0, error=None):
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
    """, (source_key, status, rows, status, error))
    conn.commit()
    cur.close()


# ── Per-table setup ────────────────────────────────────────────────────

def setup_table(tbl):
    """
    For one table definition: create dest table, PK staging, return batch ranges.
    Returns (ranges, total_rows).
    """
    source    = tbl["source"]
    dest      = tbl["dest"]
    unq_id    = tbl["unq_id"]
    stg_pk    = _staging_name(dest)

    conn = get_connection()
    cur  = conn.cursor()

    # 1. Create destination table
    if not _table_exists(cur, dest):
        cur.execute(f"CREATE TABLE {dest} LIKE {source}")
        cur.execute(f"ALTER TABLE {dest} ADD COLUMN udm_active_flag CHAR(1) DEFAULT NULL")
        if unq_id is not None:
            cur.execute(f"ALTER TABLE {dest} ADD COLUMN udm_unq_id TEXT DEFAULT NULL")
        conn.commit()

    # 2. Create PK staging
    if not _table_exists(cur, stg_pk):
        cur.execute(f"""
            CREATE TABLE {stg_pk} AS
            SELECT {BATCH_KEY}
            FROM {source}
            WHERE {BATCH_KEY} IS NOT NULL
        """)
        cur.execute(f"ALTER TABLE {stg_pk} ADD INDEX idx_pk ({BATCH_KEY})")
        conn.commit()

    ranges, total = _build_ranges(cur, stg_pk)
    cur.close()
    conn.close()
    return ranges, total


# ── Batch INSERT builder ───────────────────────────────────────────────

def build_batch_insert(source, dest, unq_id, pk_lo, pk_hi):
    if unq_id is not None:
        extra_cols = ", 'Y' AS udm_active_flag, {unq_id} AS udm_unq_id".format(unq_id=unq_id)
    else:
        extra_cols = ", 'Y' AS udm_active_flag"

    return f"""
INSERT INTO {dest}
SELECT d.*{extra_cols}
FROM {source} d
WHERE d.{BATCH_KEY} >= {pk_lo}
  AND d.{BATCH_KEY} <  {pk_hi}
"""


# ── Worker ─────────────────────────────────────────────────────────────

def run_table(tbl, ranges, pbar):
    source    = tbl["source"]
    dest      = tbl["dest"]
    unq_id    = tbl["unq_id"]
    ck_key    = f"create_staging.{dest}"

    conn = get_connection()

    if is_done(conn, ck_key):
        conn.close()
        pbar.update(len(ranges))
        return dest, {"status": "skipped", "rows": 0, "secs": 0}

    mark(conn, ck_key, "running")
    t0 = time.time()
    total_rows = 0

    try:
        cur = conn.cursor()
        cur.execute("SET unique_checks = 0")
        cur.execute("SET foreign_key_checks = 0")

        for pk_lo, pk_hi in ranges:
            sql = build_batch_insert(source, dest, unq_id, pk_lo, pk_hi)
            cur.execute(sql)
            conn.commit()
            total_rows += cur.rowcount
            pbar.update(1)

        cur.execute("SET unique_checks = 1")
        cur.execute("SET foreign_key_checks = 1")
        cur.close()

        elapsed = round(time.time() - t0, 1)
        mark(conn, ck_key, "done", total_rows)
        conn.close()
        return dest, {"status": "done", "rows": total_rows, "secs": elapsed}

    except Exception as exc:
        elapsed = round(time.time() - t0, 1)
        mark(conn, ck_key, "failed", total_rows, str(exc))
        try:
            conn.close()
        except Exception:
            pass
        return dest, {"status": f"FAILED: {exc}", "rows": total_rows, "secs": elapsed}


# ── Main ───────────────────────────────────────────────────────────────

def main():
    print(f"\n{'='*70}", flush=True)
    print(f"  Create Staging Tables ETL — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  tables     : {len(TABLES)}")
    print(f"  batch_key  : {BATCH_KEY}")
    print(f"  batch_size : {BATCH_SIZE:,}")
    print(f"  workers    : {MAX_WORKERS}")
    print(f"{'='*70}\n", flush=True)

    # ── Global checkpoint table ────────────────────────────────────────
    conn = get_connection()
    cur  = conn.cursor()
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
    cur.close()
    conn.close()

    # ── Per-table setup (sequential — DDL) ────────────────────────────
    print("  Setting up destination + PK staging tables...")
    table_ranges = {}
    total_batches = 0
    for tbl in TABLES:
        dest = tbl["dest"]
        print(f"    {dest} ...", end=" ", flush=True)
        ranges, total = setup_table(tbl)
        table_ranges[dest] = ranges
        total_batches += len(ranges)
        print(f"{total:,} rows → {len(ranges)} batches")

    print(f"\n  Total batches across all tables: {total_batches:,}")
    print(f"\n  Starting parallel copy ({MAX_WORKERS} workers)...\n")

    # ── Parallel INSERTs ───────────────────────────────────────────────
    results   = {}
    any_failed = False

    with tqdm(total=total_batches, desc="Overall", unit="batch") as pbar:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {
                executor.submit(run_table, tbl, table_ranges[tbl["dest"]], pbar): tbl["dest"]
                for tbl in TABLES
                if table_ranges[tbl["dest"]]   # skip tables with 0 rows
            }
            for future in as_completed(futures):
                dest, result = future.result()
                results[dest] = result
                if result["status"].startswith("FAILED"):
                    any_failed = True

    # ── Summary ───────────────────────────────────────────────────────
    print(f"\n{'='*70}")
    print(f"  Per-table summary:")
    total_rows = 0
    for tbl in TABLES:
        dest   = tbl["dest"]
        res    = results.get(dest, {"status": "not run", "rows": 0, "secs": 0})
        status = res["status"]
        rows   = res["rows"]
        secs   = res["secs"]
        if status == "done":
            tag = " DONE"; total_rows += rows
        elif status == "skipped":
            tag = " SKIP"
        elif status == "not run":
            tag = "  ---"
        else:
            tag = " FAIL"; any_failed = True
        label = dest.split(".")[-1]
        print(f"  [{tag}] {label:<35}  {rows:>12,} rows  ({secs}s)")
        if status.startswith("FAILED"):
            print(f"         {status}")

    print(f"\n  Total rows inserted: {total_rows:,}")
    print(f"{'='*70}")

    print(f"\n  Cleanup SQL (run after verifying data):")
    for tbl in TABLES:
        stg = _staging_name(tbl["dest"])
        print(f"    DROP TABLE IF EXISTS {stg};")
    print(f"    DROP TABLE IF EXISTS {CHECKPOINT_TABLE};")

    if any_failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
