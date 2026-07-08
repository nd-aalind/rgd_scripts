#!/usr/bin/env python3
"""
vitals_new_ao_opt.py — AthenaOne vitals ETL (optimized)

Loads vitals from AthenaOne (VITALSIGN + VITALATTRIBUTEREADING + CLINICALENCOUNTER)
into rgd_udm_silver.vitals in batches.

Optimizations:
- CLINICALENCOUNTER (active-only) pre-materialized once into staging
- Batching by actual ENCOUNTERDATAID values from VITALSIGN (sparse-ID safe)
- Checkpoint/resume — re-run skips if already completed
- Commit after every batch
- InnoDB checks disabled per-session for bulk speed
- tqdm progress bar
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
SOURCE_SCHEMA = "raleigh"   # ← change per run (e.g. "tncpa", "raleigh")
PSID          = 5         # ← change per run (integer psid for this source)

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
STAGING_ENC      = f"staging.vitals_ao_clin_enc_{SOURCE_SCHEMA}"
STAGING_PK       = f"staging.vitals_ao_pk_{SOURCE_SCHEMA}"
CHECKPOINT_TABLE = f"staging.etl_checkpoint_vitals_ao_{SOURCE_SCHEMA}"
CHECKPOINT_KEY   = f"vitals_ao_{SOURCE_SCHEMA}"

BATCH_SIZE = 50_000
BATCH_KEY  = "ENCOUNTERDATAID"   # on VITALSIGN, join key to VITALATTRIBUTEREADING


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


def _build_ranges(cur):
    cur.execute(f"SELECT COUNT(*) FROM {STAGING_PK}")
    total = cur.fetchone()[0]
    if total == 0:
        return [], 0

    cur.execute(f"""
        SELECT {BATCH_KEY}
        FROM (
            SELECT {BATCH_KEY},
                   ROW_NUMBER() OVER (ORDER BY {BATCH_KEY}) AS rn
            FROM {STAGING_PK}
        ) t
        WHERE (rn - 1) % {BATCH_SIZE} = 0
        ORDER BY {BATCH_KEY}
    """)
    boundaries = [row[0] for row in cur.fetchall() if row[0] is not None]

    cur.execute(f"SELECT MAX({BATCH_KEY}) FROM {STAGING_PK}")
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
        f"SELECT status FROM {CHECKPOINT_TABLE} WHERE source_key = %s",
        (CHECKPOINT_KEY,),
    )
    row = cur.fetchone()
    cur.close()
    return row is not None and row[0] == "done"


def mark(conn, status: str, rows: int = 0, error: str = None) -> None:
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
    """, (CHECKPOINT_KEY, status, rows, status, error))
    conn.commit()
    cur.close()


# ── Batch INSERT builder ───────────────────────────────────────────────

def build_batch_insert(pk_lo, pk_hi):
    return f"""
INSERT INTO {DEST_TABLE}
    (vital_id, ndid, eid,
     enc_date, enc_last_date, vital_date, vital_time,
     vital_code, vital_coding_system,
     vital_name, vital_unit, vital_range, vital_result,
     created_datetime, created_by, updated_datetime, updated_by,
     ehr_source_name, source_path, data_type, psid, nd_extracted_date)
SELECT
    var.VITALATTRIBUTEREADINGID,
    enc.chartid,
    CASE WHEN lower(trim(vt.clinicalencounterid)) IN ('null', '', 'none')
              OR vt.clinicalencounterid IS NULL
         THEN NULL ELSE trim(vt.clinicalencounterid) END,
    COALESCE(DATE(enc.ENCOUNTERDATE),
             STR_TO_DATE(LEFT(var.READINGDATEDATETIME, 10), '%Y-%m-%d')),
    COALESCE(DATE(enc.ENCOUNTERDATE),
             STR_TO_DATE(LEFT(var.READINGDATEDATETIME, 10), '%Y-%m-%d')),
    COALESCE(DATE(enc.ENCOUNTERDATE),
             STR_TO_DATE(LEFT(var.READINGDATEDATETIME, 10), '%Y-%m-%d')),
    COALESCE(
        STR_TO_DATE(SUBSTRING_INDEX(SUBSTRING_INDEX(var.READINGDATEDATETIME, ' ', -1), '+', 1), '%H:%i:%s'),
        STR_TO_DATE(SUBSTRING_INDEX(SUBSTRING_INDEX(var.CREATEDDATETIME,    ' ', -1), '+', 1), '%H:%i:%s')
    ),
    CASE WHEN COALESCE(vt.keyid, 0) <> 0 THEN vt.keyid ELSE var.VITALATTRIBUTEID END,
    CASE WHEN vt.value IS NULL OR vt.key IN ('null', '', 'none', 'Null', 'None')
         THEN NULL ELSE 'LOINC' END,
    vt.key,
    vt.DBUNIT,
    NULL,
    vt.value,
    CURRENT_TIMESTAMP(),
    'ND',
    CURRENT_TIMESTAMP(),
    'ND',
    'athenaone',
    'bronze_layer',
    'Structured',
    {PSID},
    vt.nd_extracted_date
FROM {SOURCE_SCHEMA}.VITALSIGN vt
LEFT JOIN {STAGING_ENC} enc
    ON  vt.clinicalencounterid = enc.clinicalencounterid
    AND vt.nd_active_flag = 'Y'
LEFT JOIN {SOURCE_SCHEMA}.VITALATTRIBUTEREADING var
    ON  var.CLINICALENCOUNTERDATAID = vt.ENCOUNTERDATAID
    AND var.nd_active_flag = 'Y'
WHERE vt.{BATCH_KEY} >= {pk_lo}
  AND vt.{BATCH_KEY} <  {pk_hi}
"""


# ── Setup ─────────────────────────────────────────────────────────────

def setup_tables():
    conn = get_connection()
    cur  = conn.cursor()

    # Materialize active CLINICALENCOUNTER — used in every batch
    print(f"  Materializing {STAGING_ENC}...")
    if not _table_exists(cur, STAGING_ENC):
        cur.execute(f"""
            CREATE TABLE {STAGING_ENC} AS
            SELECT * FROM {SOURCE_SCHEMA}.CLINICALENCOUNTER
            WHERE nd_active_flag = 'Y'
        """)
        cur.execute(
            f"ALTER TABLE {STAGING_ENC} ADD INDEX idx_clinicalencounterid (clinicalencounterid)"
        )
        conn.commit()
        cur.execute(f"SELECT COUNT(*) FROM {STAGING_ENC}")
        n = cur.fetchone()[0]
        print(f"    {n:,} active encounters")
    else:
        cur.execute(f"SELECT COUNT(*) FROM {STAGING_ENC}")
        n = cur.fetchone()[0]
        print(f"    already exists, reusing  ({n:,} rows)")

    # Ensure indexes on source join columns
    for tbl, col in [
        ("VITALSIGN",             "ENCOUNTERDATAID"),
        ("VITALSIGN",             "clinicalencounterid"),
        ("VITALSIGN",             "nd_active_flag"),
        ("VITALATTRIBUTEREADING", "CLINICALENCOUNTERDATAID"),
        ("VITALATTRIBUTEREADING", "nd_active_flag"),
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
            eid                 VARCHAR(255)  DEFAULT NULL,
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

    # Build staging PK table — distinct ENCOUNTERDATAID values (active only)
    print(f"  Creating staging PK table {STAGING_PK}...")
    if not _table_exists(cur, STAGING_PK):
        cur.execute(f"""
            CREATE TABLE {STAGING_PK} AS
            SELECT DISTINCT {BATCH_KEY}
            FROM {SOURCE_SCHEMA}.VITALSIGN
            WHERE {BATCH_KEY} IS NOT NULL
              AND nd_active_flag = 'Y'
        """)
        cur.execute(f"ALTER TABLE {STAGING_PK} ADD INDEX idx_pk ({BATCH_KEY})")
        conn.commit()
        cur.execute(f"SELECT COUNT(*) FROM {STAGING_PK}")
        n = cur.fetchone()[0]
        print(f"    {n:,} distinct ENCOUNTERDATAID values")
    else:
        cur.execute(f"SELECT COUNT(*) FROM {STAGING_PK}")
        n = cur.fetchone()[0]
        print(f"    already exists, reusing  ({n:,} rows)")

    ranges, total = _build_ranges(cur)
    print(f"    {len(ranges)} batches of ~{BATCH_SIZE:,} rows  (total distinct PKs: {total:,})")

    cur.close()
    conn.close()
    return ranges


# ── Runner ────────────────────────────────────────────────────────────

def run_insert(ranges, pbar):
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
            cur.execute(build_batch_insert(lo, hi))
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
    print(f"  AthenaOne Vitals ETL — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  source      : {SOURCE_SCHEMA}  (psid={PSID})")
    print(f"  dest        : {DEST_TABLE}")
    print(f"  staging enc : {STAGING_ENC}")
    print(f"  checkpoint  : {CHECKPOINT_TABLE}")
    print(f"  batch size  : {BATCH_SIZE:,}")
    print(f"{'='*70}\n", flush=True)

    ranges = setup_tables()

    if not ranges:
        print("  No eligible rows found. Exiting.")
        return

    result = None
    with tqdm(total=len(ranges), desc="vitals_ao", unit="batch") as pbar:
        result = run_insert(ranges, pbar)

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
        if _table_exists(cur, DEST_TABLE):
            cur.execute(f"SELECT COUNT(*), COUNT(DISTINCT ndid) FROM {DEST_TABLE}")
            row = cur.fetchone()
            print(f"\n  {DEST_TABLE}")
            print(f"    rows          : {row[0]:,}")
            print(f"    distinct ndid : {row[1]:,}")
    finally:
        cur.close()
        conn.close()

    print(f"\n  Cleanup SQL (run after verifying data):")
    print(f"    DROP TABLE IF EXISTS {STAGING_ENC};")
    print(f"    DROP TABLE IF EXISTS {STAGING_PK};")
    print(f"    DROP TABLE IF EXISTS {CHECKPOINT_TABLE};")
    print()

    if result["status"].startswith("FAILED"):
        sys.exit(1)


if __name__ == "__main__":
    main()
