#!/usr/bin/env python3
"""
vitals_new_ecw_opt.py — eClinicalWorks vitals ETL (optimized)

Loads vitals from two ECW source tables (vitals + vitalshistory) into the
destination table in parallel batches.

Sources (2 independent INSERT workers):
  1. vitals        — current vital readings
  2. vitalshistory — historical vital readings

Optimizations:
- Two parallel workers (one per source table)
- Batching by actual vitalID values (sparse-ID safe)
- Checkpoint/resume — re-run skips completed sources
- Commit after every batch
- InnoDB checks disabled per-session for bulk speed
- tqdm progress bar
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
SOURCE_SCHEMA = "dent"   # ← change per run (e.g. "kinsula_leq", "dent")
PSID          = 1   # ← change per run (integer psid for this source)

DB_CONFIG = {
    "host":            os.environ.get("DB_HOST"),
    "port":            3306,
    "user":            os.environ.get("DB_USER"),
    "password":        os.environ.get("DB_PASSWORD"),
    "database":        SOURCE_SCHEMA,
    "charset":         "utf8mb4",
    "connect_timeout": 30,
    "read_timeout":    21600,
    "write_timeout":   21600,
}

DEST_TABLE       = "rgd_udm_staging.vitals_new"
CHECKPOINT_TABLE = f"staging.etl_checkpoint_vitals_ecw_{SOURCE_SCHEMA}"

BATCH_SIZE  = 50_000
MAX_WORKERS = 2   # one per source table
BATCH_KEY   = "vitalID"

SOURCES = [
    {
        "key":        f"vitals_{SOURCE_SCHEMA}",
        "table":      "vitals",
        "pk_staging": f"staging.vitals_ecw_pk_vitals_{SOURCE_SCHEMA}",
    },
    {
        "key":        f"vitalshistory_{SOURCE_SCHEMA}",
        "table":      "vitalshistory",
        "pk_staging": f"staging.vitals_ecw_pk_vitalshistory_{SOURCE_SCHEMA}",
    },
]


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


def _index_exists(cur, schema: str, table: str, column: str) -> bool:
    cur.execute(
        "SELECT COUNT(*) FROM information_schema.statistics "
        "WHERE table_schema = %s AND table_name = %s AND column_name = %s",
        (schema, table, column),
    )
    return cur.fetchone()[0] > 0


def _build_ranges(cur, staging_pk: str):
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


# ── Checkpoint ────────────────────────────────────────────────────────

def is_done(conn, source_key: str) -> bool:
    cur = conn.cursor()
    cur.execute(
        f"SELECT status FROM {CHECKPOINT_TABLE} WHERE source_key = %s",
        (source_key,),
    )
    row = cur.fetchone()
    cur.close()
    return row is not None and row[0] == "done"


def mark(conn, source_key: str, status: str, rows: int = 0, error: str = None) -> None:
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


# ── Batch INSERT builder ───────────────────────────────────────────────

def build_batch_insert(src: dict, pk_lo, pk_hi) -> str:
    tbl = src["table"]
    return f"""
INSERT INTO {DEST_TABLE}
    (vital_id, ndid, eid,
     enc_date, enc_last_date,
     vital_name, vital_code, vital_coding_system,
     vital_date, vital_time, vital_unit, vital_range, vital_result,
     created_datetime, created_by, updated_datetime, updated_by,
     ehr_source_name, source_path, data_type, psid, nd_extracted_date)
SELECT
    a.vitalID,
    en.patientID,
    en.encounterID,
    DATE(en.date),
    NULL,
    b.itemName,
    a.propID,
    CASE WHEN COALESCE(a.Value, a.value2) IN ('', 'null') THEN NULL ELSE 'LOINC' END,
    DATE(en.date),
    DATE_FORMAT(en.starttime, '%H:%i:%s'),
    '',
    '',
    COALESCE(a.Value, a.value2),
    CURRENT_TIMESTAMP(),
    'ND',
    CURRENT_TIMESTAMP(),
    'ND',
    'athenaone',
    'bronze_layer',
    'Structured',
    {PSID},
    a.nd_extracted_date
FROM {SOURCE_SCHEMA}.{tbl} a
LEFT JOIN {SOURCE_SCHEMA}.items b  ON  a.vitalid     = b.itemid
                                   AND a.nd_ActiveFlag = 'Y'
                                   AND b.nd_ActiveFlag = 'Y'
LEFT JOIN {SOURCE_SCHEMA}.enc   en ON  a.encounterid  = en.encounterid
                                   AND en.nd_ActiveFlag = 'Y'
WHERE a.{BATCH_KEY} >= {pk_lo}
  AND a.{BATCH_KEY} <  {pk_hi}
"""


# ── Setup ─────────────────────────────────────────────────────────────

def setup_tables():
    conn = get_connection()
    cur  = conn.cursor()

    # Ensure indexes on shared join tables
    for tbl, col in [
        ("vitals",        "vitalID"),
        ("vitals",        "encounterid"),
        ("vitals",        "nd_ActiveFlag"),
        ("vitalshistory", "vitalID"),
        ("vitalshistory", "encounterid"),
        ("vitalshistory", "nd_ActiveFlag"),
        ("items",         "itemid"),
        ("items",         "nd_ActiveFlag"),
        ("enc",           "encounterid"),
        ("enc",           "nd_ActiveFlag"),
    ]:
        if not _index_exists(cur, SOURCE_SCHEMA, tbl, col):
            print(f"    Creating index on {SOURCE_SCHEMA}.{tbl} ({col})...")
            cur.execute(f"CREATE INDEX idx_{col} ON {SOURCE_SCHEMA}.{tbl} ({col})")
            conn.commit()
            print(f"      done")

    # Create destination table
    print(f"  Creating destination table {DEST_TABLE} if needed...")
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {DEST_TABLE} (
            vital_id            BIGINT        DEFAULT NULL,
            ndid                BIGINT        DEFAULT NULL,
            eid                 BIGINT        DEFAULT NULL,
            enc_date            DATE          DEFAULT NULL,
            enc_last_date       DATE          DEFAULT NULL,
            vital_date          DATE          DEFAULT NULL,
            vital_time          TIME          DEFAULT NULL,
            vital_code          VARCHAR(255)  DEFAULT NULL,
            vital_coding_system VARCHAR(50)   DEFAULT NULL,
            vital_name          TEXT          DEFAULT NULL,
            vital_unit          VARCHAR(255)  DEFAULT NULL,
            vital_range         VARCHAR(255)  DEFAULT NULL,
            vital_result        TEXT          DEFAULT NULL,
            created_datetime    DATETIME      DEFAULT NULL,
            created_by          VARCHAR(10)   DEFAULT NULL,
            updated_datetime    DATETIME      DEFAULT NULL,
            updated_by          VARCHAR(10)   DEFAULT NULL,
            ehr_source_name     VARCHAR(50)   DEFAULT NULL,
            source_path         VARCHAR(50)   DEFAULT NULL,
            data_type           VARCHAR(50)   DEFAULT NULL,
            psid                INT           DEFAULT NULL,
            nd_extracted_date   DATE          DEFAULT NULL
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
    """)
    conn.commit()
    print("    ready")

    # Checkpoint table
    print("  Creating checkpoint table...")
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
    print("    ready")

    # Build PK staging tables per source
    source_ranges = {}
    for src in SOURCES:
        staging_pk = src["pk_staging"]
        tbl        = src["table"]
        key        = src["key"]

        print(f"  Creating staging PK table {staging_pk}...")
        if not _table_exists(cur, staging_pk):
            cur.execute(f"""
                CREATE TABLE {staging_pk} AS
                SELECT DISTINCT {BATCH_KEY}
                FROM {SOURCE_SCHEMA}.{tbl}
                WHERE {BATCH_KEY} IS NOT NULL
                  AND nd_ActiveFlag = 'Y'
            """)
            cur.execute(f"ALTER TABLE {staging_pk} ADD INDEX idx_pk ({BATCH_KEY})")
            conn.commit()
            cur.execute(f"SELECT COUNT(*) FROM {staging_pk}")
            n = cur.fetchone()[0]
            print(f"    {n:,} distinct vitalIDs")
        else:
            cur.execute(f"SELECT COUNT(*) FROM {staging_pk}")
            n = cur.fetchone()[0]
            print(f"    already exists, reusing  ({n:,} rows)")

        ranges, total = _build_ranges(cur, staging_pk)
        print(f"    {len(ranges)} batches of ~{BATCH_SIZE:,}  (total: {total:,})")
        source_ranges[key] = ranges

    cur.close()
    conn.close()
    return source_ranges


# ── Worker ────────────────────────────────────────────────────────────

def run_source(src: dict, ranges: list, pbar) -> dict:
    key  = src["key"]
    conn = get_connection()
    t0   = time.time()
    total_rows = 0

    if is_done(conn, key):
        conn.close()
        pbar.update(len(ranges))
        return {"source": key, "status": "skipped", "rows": 0, "secs": 0.0}

    mark(conn, key, "running")

    try:
        cur = conn.cursor()
        cur.execute("SET unique_checks = 0")
        cur.execute("SET foreign_key_checks = 0")

        for lo, hi in ranges:
            cur.execute(build_batch_insert(src, lo, hi))
            conn.commit()
            total_rows += cur.rowcount
            pbar.update(1)

        cur.execute("SET unique_checks = 1")
        cur.execute("SET foreign_key_checks = 1")
        cur.close()

        elapsed = round(time.time() - t0, 1)
        mark(conn, key, "done", total_rows)
        conn.close()
        return {"source": key, "status": "done", "rows": total_rows, "secs": elapsed}

    except Exception as exc:
        elapsed = round(time.time() - t0, 1)
        err_msg = str(exc)
        print(f"\n  [ERROR] {key}: {err_msg}")
        try:
            mark(conn, key, "failed", total_rows, err_msg)
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass
        return {"source": key, "status": f"FAILED: {exc}", "rows": total_rows, "secs": elapsed}


# ── Main ──────────────────────────────────────────────────────────────

def main():
    print(f"\n{'='*70}", flush=True)
    print(f"  ECW Vitals ETL — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  source      : {SOURCE_SCHEMA}  (psid={PSID})")
    print(f"  dest        : {DEST_TABLE}")
    print(f"  checkpoint  : {CHECKPOINT_TABLE}")
    print(f"  workers     : {MAX_WORKERS}  |  batch_size : {BATCH_SIZE:,}")
    print(f"{'='*70}\n", flush=True)

    source_ranges = setup_tables()

    total_batches = sum(len(r) for r in source_ranges.values())
    if total_batches == 0:
        print("  No eligible rows found. Exiting.")
        return

    results = []
    with tqdm(total=total_batches, desc="vitals_ecw", unit="batch") as pbar:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
            futures = {
                pool.submit(run_source, src, source_ranges[src["key"]], pbar): src
                for src in SOURCES
                if source_ranges.get(src["key"])
            }
            for future in as_completed(futures):
                results.append(future.result())

    print()
    for r in sorted(results, key=lambda x: x["source"]):
        tag = "DONE" if r["status"] == "done" \
              else "SKIP" if r["status"] == "skipped" \
              else "FAIL"
        print(f"  [{tag}] {r['source']:<40} {r['rows']:>12,} rows  ({r['secs']}s)")

    done    = sum(1 for r in results if r["status"] == "done")
    skipped = sum(1 for r in results if r["status"] == "skipped")
    failed  = [r for r in results if "FAILED" in str(r["status"])]
    total   = sum(r["rows"] for r in results)

    print(f"\n{'='*70}")
    print(f"  Done: {done}  Skipped: {skipped}  Failed: {len(failed)}  |  Total rows: {total:,}")
    print(f"{'='*70}")

    if failed:
        print("\n  Failed sources:")
        for r in failed:
            print(f"    {r['source']}: {r['status']}")

    print(f"\n  Cleanup SQL (run after verifying data):")
    print(f"    DROP TABLE IF EXISTS {CHECKPOINT_TABLE};")
    for src in SOURCES:
        print(f"    DROP TABLE IF EXISTS {src['pk_staging']};")
    print()

    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
