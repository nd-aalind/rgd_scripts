#!/usr/bin/env python3
"""
Optimized ETL for: AthenaOne vitals incremental → staging insert

Sources (1 independent INSERT job):
  1. VITALSIGN (incremental bronze) — filtered nd_active_flag='Y' + nd_date_filter,
     INNER JOINed to pre-materialized CLINICALENCOUNTER staging for ndid/chartid

Optimizations:
- CLINICALENCOUNTER materialized once into staging (not re-scanned per batch)
- Batching by actual ENCOUNTERDATAID values (sparse ID safe)
- ThreadPoolExecutor with N workers
- Checkpoint/resume — re-run skips completed sources
- Commit after every batch
- InnoDB checks disabled per-session for bulk speed
- tqdm progress bar

Airflow usage (PythonOperator):
    from airflow_vitals_ao import run_etl
    run_etl(params={
        'main_schema':        'tng_athena_one',
        'incremental_schema': 'tng_inc',
        'staging_schema':     'udm_staging',
        'staging_table':      'vitals',
        'psid':               10,
        'ehr_source_name':    'athenaone',
        'nd_date_filter':     "= '2026-05-12'",
    })

CLI:
    python airflow_vitals_ao.py \\
        --main-schema tng_athena_one --inc-schema tng_inc \\
        --staging-schema udm_staging --staging-table vitals \\
        --psid 10 --ehr athenaone --date-filter "= '2026-05-12'"
"""

import sys
import time
import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import pymysql
from tqdm import tqdm

# ── Configuration ──────────────────────────────────────────────────────────────
DB_CONFIG = {
    "host":            "172.16.2.42",
    "port":            3306,
    "user":            "nd-root-mysql",
    "password":        "kmsamd89undsd4",
    "charset":         "utf8mb4",
    "connect_timeout": 30,
    "read_timeout":    21600,
    "write_timeout":   21600,
}

BATCH_SIZE  = 50_000
MAX_WORKERS = 4

# ── Helpers ────────────────────────────────────────────────────────────────────
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


def _index_exists(cur, schema, table, index_name):
    cur.execute(
        "SELECT COUNT(*) FROM information_schema.statistics "
        "WHERE table_schema = %s AND table_name = %s AND index_name = %s",
        (schema, table, index_name),
    )
    return cur.fetchone()[0] > 0


# ── Checkpoint ─────────────────────────────────────────────────────────────────
def is_done(conn, checkpoint_table, source_key):
    cur = conn.cursor()
    cur.execute(
        f"SELECT status FROM {checkpoint_table} WHERE source_key = %s",
        (source_key,),
    )
    row = cur.fetchone()
    cur.close()
    return row is not None and row[0] == "done"


def mark(conn, checkpoint_table, source_key, status, rows=0, error=None):
    cur = conn.cursor()
    cur.execute(f"""
        INSERT INTO {checkpoint_table}
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


# ── Batch INSERT builder ────────────────────────────────────────────────────────
def build_batch_insert(params, staging_ce, pk_lo, pk_hi):
    """
    Returns the INSERT...SELECT SQL for one ENCOUNTERDATAID batch range.
    Mirrors vitals_ao.sql exactly — column order, CASE expressions, MD5 key.
    KEY and VALUE are MySQL reserved words and are backtick-quoted.
    """
    inc_schema     = params["incremental_schema"]
    staging_schema = params["staging_schema"]
    staging_table  = params["staging_table"]
    ehr            = params["ehr_source_name"]
    psid           = int(params["psid"])
    nd_filter      = params["nd_date_filter"]

    hi_clause = f"AND vt.ENCOUNTERDATAID < {pk_hi}" if pk_hi is not None else ""

    return f"""
INSERT INTO `{staging_schema}`.`{staging_table}` (
    vital_id, ndid, eid, vital_code, vital_name, vital_coding_system,
    vital_date, vital_time, vital_unit, vital_range, vital_result,
    nd_extracted_date, created_datetime, created_by, updated_datetime, updated_by,
    ehr_source_name, source_path, data_type, psid,
    enc_date_proxy, udm_unq_id, udm_unq_id_raw
)
SELECT DISTINCT
    vt.ENCOUNTERDATAID                                                  AS vital_id,
    enc.CHARTID                                                         AS ndid,
    CASE
        WHEN LOWER(TRIM(vt.CLINICALENCOUNTERID)) IN ('null', '', 'none')
          OR vt.CLINICALENCOUNTERID IS NULL THEN NULL
        ELSE TRIM(vt.CLINICALENCOUNTERID)
    END                                                                 AS eid,
    vt.KEYID                                                            AS vital_code,
    vt.`KEY`                                                            AS vital_name,
    CASE
        WHEN vt.`KEY` IS NULL
          OR vt.`KEY` IN ('null', '', 'none', 'Null', 'None') THEN NULL
        ELSE 'LOINC'
    END                                                                 AS vital_coding_system,
    DATE(vt.CREATEDDATETIME)                                            AS vital_date,
    DATE_FORMAT(vt.CREATEDDATETIME, '%H:%i:%s')                        AS vital_time,
    vt.DBUNIT                                                           AS vital_unit,
    NULL                                                                AS vital_range,
    vt.`VALUE`                                                          AS vital_result,
    vt.nd_extracted_date                                                AS nd_extracted_date,
    CURRENT_TIMESTAMP()                                                 AS created_datetime,
    'ND'                                                                AS created_by,
    CURRENT_TIMESTAMP()                                                 AS updated_datetime,
    'ND'                                                                AS updated_by,
    '{ehr}'                                                             AS ehr_source_name,
    'bronze_layer'                                                      AS source_path,
    'Structured'                                                        AS data_type,
    {psid}                                                              AS psid,
    DATE(vt.CREATEDDATETIME)                                            AS enc_date_proxy,
    MD5(CONCAT_WS(':',
        COALESCE({psid},                                           ''),
        COALESCE(enc.CHARTID,                                      ''),
        COALESCE(TRIM(vt.CLINICALENCOUNTERID),                     ''),
        COALESCE(DATE(vt.CREATEDDATETIME),                         ''),
        COALESCE(DATE_FORMAT(vt.CREATEDDATETIME, '%H:%i:%s'),      ''),
        COALESCE(vt.KEYID,                                         ''),
        COALESCE(vt.`KEY`,                                         '')
    ))                                                                  AS udm_unq_id,
    CONCAT_WS(':',
        COALESCE({psid},                                           ''),
        COALESCE(enc.CHARTID,                                      ''),
        COALESCE(TRIM(vt.CLINICALENCOUNTERID),                     ''),
        COALESCE(DATE(vt.CREATEDDATETIME),                         ''),
        COALESCE(DATE_FORMAT(vt.CREATEDDATETIME, '%H:%i:%s'),      ''),
        COALESCE(vt.KEYID,                                         ''),
        COALESCE(vt.`KEY`,                                         '')
    )                                                                   AS udm_unq_id_raw
FROM `{inc_schema}`.VITALSIGN vt
INNER JOIN {staging_ce} enc
    ON vt.CLINICALENCOUNTERID = enc.CLINICALENCOUNTERID
WHERE vt.nd_active_flag    = 'Y'
  AND vt.nd_extracted_date {nd_filter}
  AND vt.ENCOUNTERDATAID  >= {pk_lo}
  {hi_clause}
"""


# ── Setup ──────────────────────────────────────────────────────────────────────
def setup_tables(params, staging_ce, staging_pks, checkpoint_table):
    main_schema    = params["main_schema"]
    inc_schema     = params["incremental_schema"]
    staging_schema = params["staging_schema"]
    staging_table  = params["staging_table"]
    nd_filter      = params["nd_date_filter"]

    conn = get_connection()
    cur  = conn.cursor()

    # 1. Materialize CLINICALENCOUNTER — only two columns needed for JOIN
    if not _table_exists(cur, staging_ce):
        print(f"  Creating {staging_ce} from {main_schema}.CLINICALENCOUNTER (nd_active_flag='Y')...")
        cur.execute(f"""
            CREATE TABLE {staging_ce} AS
            SELECT CLINICALENCOUNTERID, CHARTID
            FROM `{main_schema}`.CLINICALENCOUNTER
            WHERE nd_active_flag = 'Y'
        """)
        cur.execute(f"ALTER TABLE {staging_ce} ADD INDEX idx_clinicalencounterid (CLINICALENCOUNTERID)")
        conn.commit()
        print(f"  {staging_ce} ready.")
    else:
        print(f"  {staging_ce} exists — reusing.")

    # 2. Ensure VITALSIGN has indexes on join/filter columns
    for col, idx in [
        ("CLINICALENCOUNTERID", "idx_vt_ceid"),
        ("nd_extracted_date",   "idx_vt_ext_date"),
        ("nd_active_flag",      "idx_vt_active_flag"),
        ("ENCOUNTERDATAID",     "idx_vt_encounterdataid"),
    ]:
        if not _index_exists(cur, inc_schema, "VITALSIGN", idx):
            print(f"  Creating index {idx} on {inc_schema}.VITALSIGN({col})...")
            try:
                cur.execute(f"CREATE INDEX {idx} ON `{inc_schema}`.VITALSIGN ({col})")
                conn.commit()
            except Exception as exc:
                print(f"  Warning: {idx} not created — {exc}")

    # 3. Create destination table IF NOT EXISTS
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS `{staging_schema}`.`{staging_table}` (
            vital_id            BIGINT,
            ndid                BIGINT,
            eid                 BIGINT,
            vital_code          TEXT,
            vital_name          TEXT,
            vital_coding_system VARCHAR(10),
            vital_date          DATE,
            vital_time          VARCHAR(8),
            vital_unit          TEXT,
            vital_range         TEXT,
            vital_result        TEXT,
            nd_extracted_date   DATE,
            created_datetime    DATETIME,
            created_by          VARCHAR(10),
            updated_datetime    DATETIME,
            updated_by          VARCHAR(10),
            ehr_source_name     VARCHAR(50),
            source_path         VARCHAR(50),
            data_type           VARCHAR(20),
            psid                INT,
            enc_date_proxy      DATE,
            udm_unq_id          VARCHAR(32),
            udm_unq_id_raw      TEXT
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
    """)
    conn.commit()

    # 4. Create checkpoint table
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {checkpoint_table} (
            source_key    VARCHAR(150) NOT NULL PRIMARY KEY,
            status        ENUM('running','done','failed') NOT NULL DEFAULT 'running',
            rows_inserted BIGINT   DEFAULT 0,
            started_at    DATETIME DEFAULT NULL,
            completed_at  DATETIME DEFAULT NULL,
            error_msg     TEXT     DEFAULT NULL
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
    """)
    conn.commit()

    # 5. Build batch ranges from actual ENCOUNTERDATAID values (sparse ID safe)
    # Always drop + recreate — staging_pks is date-filter specific so a daily
    # re-trigger must not reuse stale PKs from the previous day's run.
    if _table_exists(cur, staging_pks):
        cur.execute(f"DROP TABLE {staging_pks}")
        conn.commit()

    print(f"  Creating {staging_pks} (filtered VITALSIGN PKs for: {nd_filter})...")
    cur.execute(f"""
        CREATE TABLE {staging_pks} AS
        SELECT ENCOUNTERDATAID
        FROM `{inc_schema}`.VITALSIGN
        WHERE nd_active_flag    = 'Y'
          AND nd_extracted_date {nd_filter}
          AND ENCOUNTERDATAID IS NOT NULL
        ORDER BY ENCOUNTERDATAID
    """)
    cur.execute(f"ALTER TABLE {staging_pks} ADD INDEX idx_pk (ENCOUNTERDATAID)")
    conn.commit()

    cur.execute(f"SELECT COUNT(*) FROM {staging_pks}")
    count = cur.fetchone()[0]

    if count == 0:
        print(f"  No rows match nd_date_filter: {nd_filter}")
        cur.close()
        conn.close()
        return {}

    print(f"  {count:,} VITALSIGN rows match filter — computing batch boundaries...")

    cur.execute(f"""
        SELECT ENCOUNTERDATAID
        FROM (
            SELECT ENCOUNTERDATAID,
                   ROW_NUMBER() OVER (ORDER BY ENCOUNTERDATAID) AS rn
            FROM {staging_pks}
        ) t
        WHERE (rn - 1) % {BATCH_SIZE} = 0
        ORDER BY ENCOUNTERDATAID
    """)
    boundaries = [row[0] for row in cur.fetchall()]

    ranges = []
    for i, lo in enumerate(boundaries):
        hi = boundaries[i + 1] if i + 1 < len(boundaries) else None
        ranges.append((lo, hi))

    print(f"  vitals_ao: {count:,} rows → {len(ranges)} batch(es) of {BATCH_SIZE:,}")

    cur.close()
    conn.close()
    return {"vitals_ao": ranges}


# ── Worker ─────────────────────────────────────────────────────────────────────
def run_source(params, staging_ce, checkpoint_table, source_key, ranges, pbar):
    conn = get_connection()

    if is_done(conn, checkpoint_table, source_key):
        conn.close()
        pbar.update(len(ranges))
        return {"source": source_key, "status": "skipped", "rows": 0, "secs": 0}

    mark(conn, checkpoint_table, source_key, "running")
    t0         = time.time()
    total_rows = 0

    try:
        cur = conn.cursor()
        cur.execute("SET unique_checks = 0")
        cur.execute("SET foreign_key_checks = 0")

        for pk_lo, pk_hi in ranges:
            sql = build_batch_insert(params, staging_ce, pk_lo, pk_hi)
            cur.execute(sql)
            conn.commit()
            total_rows += cur.rowcount
            pbar.update(1)

        cur.execute("SET unique_checks = 1")
        cur.execute("SET foreign_key_checks = 1")
        cur.close()

        mark(conn, checkpoint_table, source_key, "done", total_rows)
        conn.close()
        return {"source": source_key, "status": "done",
                "rows": total_rows, "secs": round(time.time() - t0, 1)}

    except Exception as exc:
        mark(conn, checkpoint_table, source_key, "failed", total_rows, str(exc))
        try:
            conn.close()
        except Exception:
            pass
        return {"source": source_key, "status": f"FAILED: {exc}",
                "rows": total_rows, "secs": round(time.time() - t0, 1)}


# ── Main ETL entrypoint ────────────────────────────────────────────────────────
def run_etl(params):
    """
    Main entrypoint — callable from Airflow PythonOperator or CLI.

    Required params keys:
        main_schema, incremental_schema, staging_schema, staging_table,
        psid, ehr_source_name, nd_date_filter
    """
    psid = int(params["psid"])

    # Per-psid staging names — safe for concurrent DAG runs across different psids
    staging_ce       = f"staging.vt_ao_ce_{psid}_v1"
    staging_pks      = f"staging.tmp_vt_ao_{psid}_pks_v1"
    checkpoint_table = f"staging.etl_checkpoint_vitals_ao_{psid}_v1"

    print(f"\n{'='*70}")
    print(f"  Vitals AO ETL — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  source     : {params['incremental_schema']}.VITALSIGN")
    print(f"  ce staging : {staging_ce}")
    print(f"  dest       : {params['staging_schema']}.{params['staging_table']}")
    print(f"  filter     : nd_extracted_date {params['nd_date_filter']}")
    print(f"  psid       : {psid}  |  ehr : {params['ehr_source_name']}")
    print(f"  checkpoint : {checkpoint_table}")
    print(f"  workers    : {MAX_WORKERS}  |  batch_size : {BATCH_SIZE:,}")
    print(f"{'='*70}\n")

    source_ranges = setup_tables(params, staging_ce, staging_pks, checkpoint_table)

    total_batches = sum(len(r) for r in source_ranges.values())
    if total_batches == 0:
        print("No rows to process. Exiting.")
        return

    # Scope source_key to the date filter so each daily run gets its own
    # checkpoint entry — re-triggering the same day resumes, next day starts fresh.
    import re as _re
    date_slug = _re.sub(r"[^a-zA-Z0-9_-]", "_", params["nd_date_filter"]).strip("_")
    scoped_key = f"vitals_ao_{date_slug}"

    ranges = source_ranges["vitals_ao"]
    results = []
    with tqdm(total=total_batches, desc="Overall", unit="batch") as pbar:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
            future = pool.submit(
                run_source, params, staging_ce, checkpoint_table,
                scoped_key, ranges, pbar
            )
            results.append(future.result())

    print()
    for r in sorted(results, key=lambda x: x["source"]):
        tag = "DONE" if r["status"] == "done" \
              else "SKIP" if r["status"] == "skipped" \
              else "FAIL"
        print(f"  [{tag}] {r['source']:<42} {r['rows']:>10,} rows  ({r['secs']}s)")

    done    = sum(1 for r in results if r["status"] == "done")
    skipped = sum(1 for r in results if r["status"] == "skipped")
    failed  = [r for r in results if "FAILED" in str(r["status"])]
    total   = sum(r["rows"] for r in results)

    print(f"\n{'='*70}")
    print(f"  Done: {done}  Skipped: {skipped}  Failed: {len(failed)}  |  Total rows: {total:,}")
    print(f"{'='*70}")

    print(f"\n  Cleanup SQL (run after validating data):")
    print(f"    DROP TABLE IF EXISTS {staging_pks};")
    print(f"    DROP TABLE IF EXISTS {checkpoint_table};")
    print(f"    -- Keep {staging_ce} for reuse in next daily run")

    if failed:
        print("\n  Failed sources:")
        for r in failed:
            print(f"    {r['source']}: {r['status']}")
        sys.exit(1)


# ── CLI ────────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Vitals AO incremental ETL")
    parser.add_argument("--main-schema",    required=True, help="Primary bronze schema  (e.g. tng_athena_one)")
    parser.add_argument("--inc-schema",     required=True, help="Incremental bronze schema (e.g. tng_inc)")
    parser.add_argument("--staging-schema", required=True, help="Target staging schema (e.g. udm_staging)")
    parser.add_argument("--staging-table",  required=True, help="Target table name (e.g. vitals)")
    parser.add_argument("--psid",           required=True, type=int, help="Provider-site integer ID")
    parser.add_argument("--ehr",            required=True, help="EHR source name (e.g. athenaone)")
    parser.add_argument("--date-filter",    required=True,
                        help="nd_extracted_date SQL filter fragment (e.g. \"= '2026-05-12'\")")
    parser.add_argument("--batch-size",     type=int, default=BATCH_SIZE,
                        help=f"Rows per batch (default: {BATCH_SIZE:,})")
    parser.add_argument("--workers",        type=int, default=MAX_WORKERS,
                        help=f"Parallel workers (default: {MAX_WORKERS})")
    args = parser.parse_args()

    global BATCH_SIZE, MAX_WORKERS
    BATCH_SIZE  = args.batch_size
    MAX_WORKERS = args.workers

    run_etl({
        "main_schema":        args.main_schema,
        "incremental_schema": args.inc_schema,
        "staging_schema":     args.staging_schema,
        "staging_table":      args.staging_table,
        "psid":               args.psid,
        "ehr_source_name":    args.ehr,
        "nd_date_filter":     args.date_filter,
    })


if __name__ == "__main__":
    main()
