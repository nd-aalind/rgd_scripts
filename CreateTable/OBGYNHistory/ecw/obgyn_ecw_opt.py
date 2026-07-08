#!/usr/bin/env python3
"""
Optimized ETL loader for: udm_staging.obgynhistory_rgd
Source: eCW — obf_pregnancy

Three UNION ALL branches, all from the same table (obf_pregnancy),
each selecting a different value column with a different question label:
  Branch 1 — Pregnancy status  (OBFStat,        Upd_Date as date)
  Branch 2 — Number of Babies  (NumberOfBabies,  Upd_Date as date)
  Branch 3 — Discharge Date    (Discharge_date,  Upd_Date as date)

All branches also select:
  obgyn_hist_code            = AsmtValue
  obgyn_hist_coding_system   = Pregid
  obgyn_hist_notes           = CONCAT_WS(' | ', notes, sticky_notes)

Single shared PK staging (all rows from obf_pregnancy).
Each branch runs in parallel and applies its own value_col IS NOT NULL filter.

Optimizations applied:
- Single PK staging (same source table across all branches)
- Server-side boundary sampling via ROW_NUMBER()
- Each branch runs in parallel via ThreadPoolExecutor (3 workers)
- Each worker has its own DB connection (pymysql is not thread-safe)
- Commit after every batch (frees undo/log space)
- Checkpoint/resume per branch
- Disabled InnoDB checks per-session for bulk insert speed
- Progress bar via tqdm

Usage:
    python obgyn_ecw_opt.py
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
    "database":        "fcn_latest",
    "charset":         "utf8mb4",
    "connect_timeout": 30,
    "read_timeout":    21600,
    "write_timeout":   21600,
}

BATCH_SIZE  = 50_000
MAX_WORKERS = 3   # one per UNION ALL branch

# ── Change these three variables to run for a different schema/psid ──
SOURCE_SCHEMA   = "arizona_staging"
PSID            = 14
EHR_SOURCE_NAME = "ecw"    # e.g. "ecw", "greenway"

SOURCE_TABLE     = "obf_pregnancy"
PK               = "Pregid"
DEST_TABLE       = "udm_staging.obgynhistory_rgd"
STAGING_PK       = f"staging.obgyn_ecw_pk_{SOURCE_SCHEMA}"
CHECKPOINT_TABLE = f"staging.etl_checkpoint_obgyn_ecw_{SOURCE_SCHEMA}"

# Set to True during setup if nd_extracted_date exists on the source table
_HAS_ND_EXTRACTED_DATE = True

# ── Source branch definitions ──────────────────────────────────────────
SOURCES = [
    {
        "key":       "obgyn_ecw_1",
        "question":  "Pregnancy status",
        "value_col": "OBFStat",
    },
    {
        "key":       "obgyn_ecw_2",
        "question":  "Number of Babies",
        "value_col": "NumberOfBabies",
    },
    {
        "key":       "obgyn_ecw_3",
        "question":  "Discharge Date",
        "value_col": "Discharge_date",
    },
]


# ── Date CASE helper ──────────────────────────────────────────────────

def date_case(col):
    """
    Converts a DATE/DATETIME column to DATE safely.
    Wraps col in CAST(... AS CHAR) to avoid MySQL strict mode error 1292.
    """
    c = f"CAST({col} AS CHAR)"
    return (
        f"CASE\n"
        f"        WHEN {c} IS NULL OR {c} IN ('', 'None') THEN NULL\n"
        f"        WHEN {c} REGEXP '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}} [0-9]{{2}}:[0-9]{{2}}:[0-9]{{2}}$'\n"
        f"            THEN DATE({c})\n"
        f"        WHEN {c} REGEXP '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}$'\n"
        f"            THEN STR_TO_DATE({c}, '%Y-%m-%d')\n"
        f"        WHEN {c} REGEXP '^[0-9]{{2}}-[0-9]{{2}}-[0-9]{{4}}$'\n"
        f"            THEN STR_TO_DATE({c}, '%m-%d-%Y')\n"
        f"        ELSE NULL\n"
        f"    END"
    )


# ── Batch INSERT builder ───────────────────────────────────────────────

def build_batch_insert(src, pk_lo, pk_hi):
    question  = src["question"]
    value_col = src["value_col"]

    return f"""
INSERT INTO {DEST_TABLE}
    (obgyn_hist_id, ndid, eid, encounter_date, obgyn_episode_date, obgyn_hist_date,
     obgyn_hist_category, obgyn_hist_subcategory, obgyn_hist_question,
     obgyn_hist_value, obgyn_hist_code, obgyn_hist_coding_system, obgyn_hist_notes,
     data_source, created_datetime, created_by, ehr_source_name,
     source_path, data_type, psid, nd_extracted_date)
SELECT
    p.{PK},
    p.Patientid,
    NULL,
    NULL,
    NULL,
    {date_case('p.Upd_Date')},
    'OB/GYN History',
    'Pregnancy',
    '{question}',
    p.{value_col},
    p.AsmtValue,
    p.{PK},
    CONCAT_WS(' | ', p.notes, p.sticky_notes),
    'eCW 1',
    CURRENT_TIMESTAMP(),
    'ND',
    '{EHR_SOURCE_NAME}',
    'bronze_layer',
    'Structured',
    {PSID},
    {"p.nd_extracted_date" if _HAS_ND_EXTRACTED_DATE else "NULL"}
FROM {SOURCE_SCHEMA}.{SOURCE_TABLE} p
WHERE p.{value_col} IS NOT NULL
  AND p.{PK} >= {pk_lo}
  AND p.{PK} <  {pk_hi}
"""


# ── Helpers ────────────────────────────────────────────────────────────

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


def _col_exists(cur, schema, table, col_name):
    cur.execute(
        "SELECT COUNT(*) FROM information_schema.columns "
        "WHERE table_schema = %s AND table_name = %s AND column_name = %s",
        (schema, table, col_name),
    )
    return cur.fetchone()[0] > 0


def _build_ranges(cur, stg_pk):
    cur.execute(f"SELECT COUNT(*) FROM {stg_pk}")
    total = cur.fetchone()[0]
    if total == 0:
        return [], 0

    cur.execute(f"""
        SELECT {PK}
        FROM (
            SELECT {PK},
                   ROW_NUMBER() OVER (ORDER BY {PK}) AS rn
            FROM {stg_pk}
        ) t
        WHERE (rn - 1) % {BATCH_SIZE} = 0
        ORDER BY {PK}
    """)
    boundaries = [row[0] for row in cur.fetchall() if row[0] is not None]

    cur.execute(f"SELECT MAX({PK}) FROM {stg_pk}")
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


# ── Setup ──────────��───────────────────────────────────────────────────

def setup_tables():
    global _HAS_ND_EXTRACTED_DATE

    conn = get_connection()
    cur  = conn.cursor()

    # ── 0. Check optional columns on source table ─────────────────────
    _HAS_ND_EXTRACTED_DATE = _col_exists(cur, SOURCE_SCHEMA, SOURCE_TABLE, "nd_extracted_date")
    print(f"  nd_extracted_date on source: {'yes' if _HAS_ND_EXTRACTED_DATE else 'no — will use NULL'}")

    # ── 1. Shared PK staging (all rows from obf_pregnancy) ────────────
    print(f"  Creating shared PK staging for {SOURCE_SCHEMA}.{SOURCE_TABLE}...")
    if not _table_exists(cur, STAGING_PK):
        cur.execute(f"""
            CREATE TABLE {STAGING_PK} AS
            SELECT {PK}
            FROM {SOURCE_SCHEMA}.{SOURCE_TABLE}
            WHERE {PK} IS NOT NULL
        """)
        # Use prefix length 100 in case PK column is TEXT/BLOB in some schemas
        try:
            cur.execute(f"ALTER TABLE {STAGING_PK} ADD INDEX idx_pk ({PK})")
        except Exception:
            cur.execute(f"ALTER TABLE {STAGING_PK} ADD INDEX idx_pk ({PK}(100))")
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")

    ranges, total = _build_ranges(cur, STAGING_PK)
    print(f"    {total:,} rows → {len(ranges)} batches")

    # ── 2. Destination table ───────────────────────────────────────────
    print("  Creating destination table...")
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {DEST_TABLE} (
            obgyn_hist_id             BIGINT        DEFAULT NULL,
            ndid                      BIGINT        DEFAULT NULL,
            eid                       BIGINT        DEFAULT NULL,
            encounter_date            DATE          DEFAULT NULL,
            obgyn_episode_date        DATE          DEFAULT NULL,
            obgyn_hist_date           DATE          DEFAULT NULL,
            obgyn_hist_category       VARCHAR(100)  DEFAULT NULL,
            obgyn_hist_subcategory    VARCHAR(100)  DEFAULT NULL,
            obgyn_hist_question       TEXT,
            obgyn_hist_value          TEXT,
            obgyn_hist_code           TEXT,
            obgyn_hist_coding_system  VARCHAR(50)   DEFAULT NULL,
            obgyn_hist_notes          TEXT,
            data_source               VARCHAR(50)   DEFAULT NULL,
            created_datetime          DATETIME      DEFAULT NULL,
            created_by                VARCHAR(50)   DEFAULT NULL,
            ehr_source_name           VARCHAR(100)  DEFAULT NULL,
            source_path               VARCHAR(100)  DEFAULT NULL,
            data_type                 VARCHAR(50)   DEFAULT NULL,
            psid                      INT           DEFAULT NULL,
            nd_extracted_date         DATE          DEFAULT NULL
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
    """)
    conn.commit()
    print("    ready")

    # ── 3. Checkpoint table ────────────────────────────────────────────
    print("  Creating checkpoint table...")
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {CHECKPOINT_TABLE} (
            source_key    VARCHAR(150) NOT NULL PRIMARY KEY,
            status        ENUM('running','done','failed') NOT NULL DEFAULT 'running',
            rows_inserted BIGINT      DEFAULT 0,
            started_at    DATETIME    DEFAULT NULL,
            completed_at  DATETIME    DEFAULT NULL,
            error_msg     TEXT        DEFAULT NULL
        )
    """)
    conn.commit()
    print("    ready")

    cur.close()
    conn.close()
    return ranges


# ── Worker ─────────────────────────────────────────────────────────────

def run_source(src, ranges, pbar):
    source_key = src["key"]
    conn = get_connection()

    if is_done(conn, source_key):
        conn.close()
        pbar.update(len(ranges))
        return source_key, {"status": "skipped", "rows": 0, "secs": 0}

    mark(conn, source_key, "running")
    t0 = time.time()
    total_rows = 0

    try:
        cur = conn.cursor()
        cur.execute("SET unique_checks = 0")
        cur.execute("SET foreign_key_checks = 0")

        for pk_lo, pk_hi in ranges:
            sql = build_batch_insert(src, pk_lo, pk_hi)
            cur.execute(sql)
            conn.commit()
            total_rows += cur.rowcount
            pbar.update(1)

        cur.execute("SET unique_checks = 1")
        cur.execute("SET foreign_key_checks = 1")
        cur.close()

        elapsed = round(time.time() - t0, 1)
        mark(conn, source_key, "done", total_rows)
        conn.close()
        return source_key, {"status": "done", "rows": total_rows, "secs": elapsed}

    except Exception as exc:
        elapsed = round(time.time() - t0, 1)
        mark(conn, source_key, "failed", total_rows, str(exc))
        try:
            conn.close()
        except Exception:
            pass
        return source_key, {"status": f"FAILED: {exc}", "rows": total_rows, "secs": elapsed}


# ── Main ───────────────────────────────────────────────────────────────

def main():
    print(f"\n{'='*70}", flush=True)
    print(f"  eCW OB/GYN History ETL — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  source     : {SOURCE_SCHEMA}.{SOURCE_TABLE}")
    print(f"  dest       : {DEST_TABLE}")
    print(f"  staging_pk : {STAGING_PK}")
    print(f"  checkpoint : {CHECKPOINT_TABLE}")
    print(f"  branches   : {len(SOURCES)}")
    print(f"  batch_size : {BATCH_SIZE:,}  |  workers: {MAX_WORKERS}")
    print(f"{'='*70}\n", flush=True)

    print("  Connecting to database...")
    sys.stdout.flush()
    ranges = setup_tables()

    if not ranges:
        print(f"\nNo eligible rows found in {SOURCE_SCHEMA}.{SOURCE_TABLE}. Exiting.")
        return

    total_batches = len(ranges) * len(SOURCES)
    print(f"\n  Starting parallel insert ({MAX_WORKERS} workers, "
          f"{len(ranges)} batches × {len(SOURCES)} branches = {total_batches} total)...\n")

    results    = {}
    any_failed = False

    with tqdm(total=total_batches, desc="Overall", unit="batch") as pbar:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
            futures = {
                pool.submit(run_source, src, ranges, pbar): src["key"]
                for src in SOURCES
            }
            for fut in as_completed(futures):
                source_key, result = fut.result()
                results[source_key] = result
                if result["status"].startswith("FAILED"):
                    any_failed = True

    print(f"\n{'='*70}")
    print(f"  Per-branch summary:")
    total_rows = 0
    for src in SOURCES:
        key    = src["key"]
        res    = results.get(key, {"status": "no rows", "rows": 0, "secs": 0})
        status = res["status"]
        rows   = res["rows"]
        secs   = res["secs"]
        if status == "done":
            tag = " DONE"; total_rows += rows
        elif status == "skipped":
            tag = " SKIP"
        elif status == "no rows":
            tag = "  ---"
        else:
            tag = " FAIL"; any_failed = True
        print(f"  [{tag}] {src['question']:<25}  ({src['value_col']:<16})  {rows:>10,} rows  ({secs}s)")
        if status.startswith("FAILED"):
            print(f"         {status}")

    print(f"\n  Total rows inserted: {total_rows:,}")
    print(f"{'='*70}")

    print(f"\n  Cleanup SQL (run after verifying data):")
    print(f"    DROP TABLE IF EXISTS {STAGING_PK};")
    print(f"    DROP TABLE IF EXISTS {CHECKPOINT_TABLE};")

    if any_failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
