#!/usr/bin/env python3
"""
Optimized ETL loader for: udm_staging.obgynhistory_rgd
Source: AthenaOne (AO) — PATIENTGPALHISTORY

Seven UNION ALL branches, all from the same table (PATIENTGPALHISTORY),
each selecting a different value column with a different question label:
  Branch 1 — Total Pregnancies      (TOTAL,               CREATEDDATETIME as date)
  Branch 2 — Full Term Births        (FULLTERM,             NULL date)
  Branch 3 — Premature Births        (PREMATURE,            NULL date)
  Branch 4 — Ectopic Pregnancies     (ECTOPICS,             NULL date)
  Branch 5 — Multiple Births         (MULTIPLEBIRTHS,       NULL date)
  Branch 6 — Spontaneous Abortions   (SPONTANEOUSABORTION,  NULL date)
  Branch 7 — Induced Abortions       (INDUCEDABORTION,      NULL date)

Single shared PK staging (all rows WHERE nd_active_flag = 'Y').
Each branch runs in parallel and applies its own column IS NOT NULL filter.

Optimizations applied:
- Single PK staging (same source table + same base filter across all branches)
- Server-side boundary sampling via ROW_NUMBER()
- Each branch runs in parallel via ThreadPoolExecutor (7 workers)
- Each worker has its own DB connection (pymysql is not thread-safe)
- Commit after every batch (frees undo/log space)
- Checkpoint/resume per branch
- Disabled InnoDB checks per-session for bulk insert speed
- Progress bar via tqdm

Usage:
    python obgyn_ao_opt.py
"""

import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import pymysql
from tqdm import tqdm

# ── Configuration ─────────────────────────────────────────────────────
DB_CONFIG = {
    "host":            "172.16.2.42",
    "port":            3306,
    "user":            "nd-root-mysql",
    "password":        "kmsamd89undsd4",
    "database":        "tncpa",
    "charset":         "utf8mb4",
    "connect_timeout": 30,
    "read_timeout":    21600,
    "write_timeout":   21600,
}

BATCH_SIZE  = 50_000
MAX_WORKERS = 7   # one per UNION ALL branch

# ── Change these three variables to run for a different schema/psid ──
SOURCE_SCHEMA    = "dcnd"
PSID             = 10
EHR_SOURCE_NAME  = "ao"    # e.g. "ao", "ecw", "greenway"

SOURCE_TABLE     = "PATIENTGPALHISTORY"
PK               = "PATIENTGPALHISTORYID"
DEST_TABLE       = "udm_staging.obgynhistory_rgd"
STAGING_PK       = f"staging.obgyn_ao_pk1_{SOURCE_SCHEMA}"
CHECKPOINT_TABLE = f"staging.etl_checkpoint_obgyn_ao1_{SOURCE_SCHEMA}"

# ── Source branch definitions ──────────────────────────────────────────
SOURCES = [
    {
        "key":        "obgyn_ao_1",
        "question":   "Total Pregnancies",
        "value_col":  "TOTAL",
        "date_col":   "CREATEDDATETIME",   # branch 1 has a date; others are NULL
    },
    {
        "key":        "obgyn_ao_2",
        "question":   "Full Term Births",
        "value_col":  "FULLTERM",
        "date_col":   None,
    },
    {
        "key":        "obgyn_ao_3",
        "question":   "Premature Births",
        "value_col":  "PREMATURE",
        "date_col":   None,
    },
    {
        "key":        "obgyn_ao_4",
        "question":   "Ectopic Pregnancies",
        "value_col":  "ECTOPICS",
        "date_col":   None,
    },
    {
        "key":        "obgyn_ao_5",
        "question":   "Multiple Births",
        "value_col":  "MULTIPLEBIRTHS",
        "date_col":   None,
    },
    {
        "key":        "obgyn_ao_6",
        "question":   "Spontaneous Abortions",
        "value_col":  "SPONTANEOUSABORTION",
        "date_col":   None,
    },
    {
        "key":        "obgyn_ao_7",
        "question":   "Induced Abortions",
        "value_col":  "INDUCEDABORTION",
        "date_col":   None,
    },
]


# ── Date CASE helper (AO style with CAST AS CHAR) ─────────────────────

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
    date_col  = src["date_col"]

    hist_date_expr = date_case(f"g.{date_col}") if date_col else "NULL"

    return f"""
INSERT INTO {DEST_TABLE}
    (obgyn_hist_id, ndid, eid, encounter_date, obgyn_episode_date, obgyn_hist_date,
     obgyn_hist_category, obgyn_hist_subcategory, obgyn_hist_question,
     obgyn_hist_value, obgyn_hist_code, obgyn_hist_coding_system, obgyn_hist_notes,
     data_source, created_datetime, created_by, ehr_source_name,
     source_path, data_type, psid, nd_extracted_date)
SELECT
    g.{PK},
    g.CHARTID,
    NULL,
    NULL,
    NULL,
    {hist_date_expr},
    'OB/GYN History',
    'Pregnancy',
    '{question}',
    g.{value_col},
    NULL,
    NULL,
    NULL,
    'AthenaOne',
    CURRENT_TIMESTAMP(),
    'ND',
    '{EHR_SOURCE_NAME}',
    'bronze_layer',
    'Structured',
    {PSID},
    g.nd_extracted_date
FROM {SOURCE_SCHEMA}.{SOURCE_TABLE} g
WHERE g.nd_active_flag = 'Y'
  AND g.{value_col} IS NOT NULL
  AND g.{PK} >= {pk_lo}
  AND g.{PK} <  {pk_hi}
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


# ── Checkpoint ──────────────────────────────────────────��──────────────

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


# ── Setup ──────────────────────────────────────────────────────────────

def setup_tables():
    conn = get_connection()
    cur  = conn.cursor()

    # ── 1. Shared PK staging (all rows WHERE nd_active_flag = 'Y') ────
    print(f"  Creating shared PK staging for {SOURCE_SCHEMA}.{SOURCE_TABLE}...")
    if not _table_exists(cur, STAGING_PK):
        cur.execute(f"""
            CREATE TABLE {STAGING_PK} AS
            SELECT {PK}
            FROM {SOURCE_SCHEMA}.{SOURCE_TABLE}
            WHERE nd_active_flag = 'Y'
              AND {PK} IS NOT NULL
        """)
        cur.execute(f"ALTER TABLE {STAGING_PK} ADD INDEX idx_pk ({PK})")
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
    print(f"  AO OB/GYN History ETL — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
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

    results = {}
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
        print(f"  [{tag}] {src['question']:<30}  ({src['value_col']:<22})  {rows:>10,} rows  ({secs}s)")
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
