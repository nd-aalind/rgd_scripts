#!/usr/bin/env python3
"""
Optimized ETL for: udm_staging.surgicalhistory_final
Source: AthenaOne

Sources (2 independent INSERT jobs via UNION ALL):
  1. PATIENTSURGERY          — nd_active_flag='Y', type<>'REVIEWED.PATIENTSURGICALHISTORY',
                               deleteddatetime IS NULL
  2. PATIENTSURGICALHISTORY  — nd_active_flag='Y', deleteddatetime IS NULL
                               LEFT JOIN SNOMED + SURGICALHISTORYPROCEDURE

Pre-materialized lookup tables (computed ONCE, reused across all batches):
  - staging.sh_ao_snomed_v1_{schema}  (SNOMED, keyed on SNOMEDCODE)
  - staging.sh_ao_shp_v1_{schema}     (SURGICALHISTORYPROCEDURE active, keyed on SURGICALHISTORYPROCEDUREID)

Optimizations:
- Lookup tables pre-materialized once (not re-scanned per batch)
- Batching by actual PK values (sparse ID safe)
- ThreadPoolExecutor with 2 workers (one per branch)
- Checkpoint/resume — re-run skips completed sources
- Commit after every batch
- InnoDB checks disabled per-session for bulk speed
- tqdm progress bar

Usage:
    python opt_surg_hist_ao.py
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

# ── Configuration ─────────────────────────────────────────────────────────────
DB_CONFIG = {
    "host":            os.environ.get("DB_HOST"),
    "port":            3306,
    "user":            os.environ.get("DB_USER"),
    "password":        os.environ.get("DB_PASSWORD"),
    "database":        "udm_staging",
    "charset":         "utf8mb4",
    "connect_timeout": 30,
    "read_timeout":    21600,
    "write_timeout":   21600,
}

BATCH_SIZE  = 50_000
MAX_WORKERS = 2

# ── Change these two variables to run for a different schema/psid ──────────────
SOURCE_SCHEMA = "raleigh"
PSID          = 5

DEST_TABLE       = "udm_staging.surgicalhistory_final"
STAGING_SNOMED   = f"staging.sh_ao_snomed_v1_{SOURCE_SCHEMA}"
STAGING_SHP      = f"staging.sh_ao_shp_v1_{SOURCE_SCHEMA}"
CHECKPOINT_TABLE = f"staging.etl_checkpoint_sh_ao_v1_{SOURCE_SCHEMA}"

SOURCES = [
    {
        "key":        "patientsurgery",
        "table":      "PATIENTSURGERY",
        "pk":         "PATIENTSURGERYID",
        "pk_staging": f"staging.tmp_sh_ao_ps_v1_{SOURCE_SCHEMA}",
    },
    {
        "key":        "patientsurgicalhistory",
        "table":      "PATIENTSURGICALHISTORY",
        "pk":         "PATIENTSURGICALHISTORYID",
        "pk_staging": f"staging.tmp_sh_ao_psh_v1_{SOURCE_SCHEMA}",
    },
]


# ── Date CASE helper ──────────────────────────────────────────────────────────

def date_case(col):
    """Parse VARCHAR date columns (SURGERYDATETIME, CREATEDDATETIME, etc.).
    {{N}} in f-string produces {N} in the returned string — correct MySQL REGEXP quantifier.
    """
    left = f"LEFT({col}, 10)"
    return (
        f"CASE\n"
        f"        WHEN {col} IS NULL OR {col} IN ('None', '') THEN NULL\n"
        f"        WHEN {left} REGEXP '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}$'\n"
        f"            THEN STR_TO_DATE({left}, '%Y-%m-%d')\n"
        f"        WHEN {left} REGEXP '^[0-9]{{2}}-[0-9]{{2}}-[0-9]{{4}}$'\n"
        f"            THEN STR_TO_DATE({left}, '%m-%d-%Y')\n"
        f"        ELSE NULL\n"
        f"    END"
    )


# ── Index helper ──────────────────────────────────────────────────────────────

def _ensure_index(cur, conn, full_table_name, index_name, columns, prefix_len=None):
    schema, table = full_table_name.split(".", 1)
    cur.execute(
        "SELECT COUNT(*) FROM information_schema.statistics "
        "WHERE table_schema = %s AND table_name = %s AND index_name = %s",
        (schema, table, index_name),
    )
    if cur.fetchone()[0] > 0:
        print(f"    index {index_name} on {full_table_name} already exists — skipping")
        return
    col_list = ", ".join(f"{c}({prefix_len})" if prefix_len else c for c in columns)
    print(f"    creating index {index_name} on {full_table_name}({col_list}) ...")
    cur.execute(f"ALTER TABLE {full_table_name} ADD INDEX {index_name} ({col_list})")
    conn.commit()
    print(f"    done")


# ── Batch INSERT builders ──────────────────────────────────────────────────────

def _build_ps_insert(pk_lo, pk_hi):
    p = "ps"
    surgery_date = (
        f"COALESCE(\n"
        f"        {date_case(f'{p}.SURGERYDATETIME')},\n"
        f"        {date_case(f'{p}.CREATEDDATETIME')}\n"
        f"    )"
    )
    return f"""
INSERT INTO {DEST_TABLE}
    (surgicalhistoryid, ndid, eid, enc_date,
     surgery_date, surg_hist_type, surgery_name,
     surgery_code, surgery_coding_system, surgery_reason,
     created_datetime, created_by, updated_datetime, updated_by,
     ehr_source_name, source_path, data_type,
     psid, nd_extracted_date)
SELECT
    {p}.PATIENTSURGERYID,
    {p}.CHARTID,
    NULL,
    NULL,
    {surgery_date},
    {p}.type,
    {p}.procedure,
    COALESCE({p}.snomedcode, {p}.procedurecode),
    CASE
        WHEN {p}.snomedcode IS NOT NULL                        THEN 'SNOMED'
        WHEN {p}.procedurecode REGEXP '^[0-9]{{5}}$'           THEN 'CPT'
        WHEN {p}.procedurecode REGEXP '^[A-Za-z][0-9]{{4}}$'   THEN 'HCPCS'
    END,
    NULL,
    CURRENT_TIMESTAMP(),
    'ND',
    CURRENT_TIMESTAMP(),
    'ND',
    'AthenaOne',
    'bronze_layer',
    'Structured',
    {PSID},
    {p}.nd_extracted_date
FROM {SOURCE_SCHEMA}.PATIENTSURGERY {p}
WHERE {p}.nd_active_flag = 'Y'
  AND {p}.type <> 'REVIEWED.PATIENTSURGICALHISTORY'
  AND {p}.deleteddatetime IS NULL
  AND {p}.PATIENTSURGERYID >= {pk_lo}
  AND {p}.PATIENTSURGERYID <  {pk_hi}
"""


def _build_psh_insert(pk_lo, pk_hi):
    p = "psh"
    surgery_date = (
        f"COALESCE(\n"
        f"        {date_case(f'{p}.SURGERYDATEDATETIME')},\n"
        f"        {date_case(f'{p}.CREATEDDATETIME')}\n"
        f"    )"
    )
    return f"""
INSERT INTO {DEST_TABLE}
    (surgicalhistoryid, ndid, eid, enc_date,
     surgery_date, surg_hist_type, surgery_name,
     surgery_code, surgery_coding_system, surgery_reason,
     created_datetime, created_by, updated_datetime, updated_by,
     ehr_source_name, source_path, data_type,
     psid, nd_extracted_date)
SELECT
    {p}.PATIENTSURGICALHISTORYID,
    {p}.CHARTID,
    NULL,
    NULL,
    {surgery_date},
    'PATIENTSURGICALHISTORY',
    COALESCE(shp.NAME, s.DESCRIPTION),
    COALESCE({p}.snomedcode, {p}.procedurecode),
    CASE
        WHEN {p}.snomedcode IS NOT NULL                        THEN 'SNOMED'
        WHEN {p}.procedurecode REGEXP '^[0-9]{{5}}$'           THEN 'CPT'
        WHEN {p}.procedurecode REGEXP '^[A-Za-z][0-9]{{4}}$'   THEN 'HCPCS'
    END,
    {p}.note,
    CURRENT_TIMESTAMP(),
    'ND',
    CURRENT_TIMESTAMP(),
    'ND',
    'AthenaOne',
    'bronze_layer',
    'Structured',
    {PSID},
    {p}.nd_extracted_date
FROM {SOURCE_SCHEMA}.PATIENTSURGICALHISTORY {p}
LEFT JOIN {STAGING_SNOMED} s   ON s.SNOMEDCODE                   = {p}.snomedcode
LEFT JOIN {STAGING_SHP}    shp ON shp.SURGICALHISTORYPROCEDUREID = {p}.SURGICALHISTORYPROCEDUREID
WHERE {p}.nd_active_flag = 'Y'
  AND {p}.deleteddatetime IS NULL
  AND {p}.PATIENTSURGICALHISTORYID >= {pk_lo}
  AND {p}.PATIENTSURGICALHISTORYID <  {pk_hi}
"""


def build_batch_insert(source, pk_lo, pk_hi):
    if source["key"] == "patientsurgery":
        return _build_ps_insert(pk_lo, pk_hi)
    return _build_psh_insert(pk_lo, pk_hi)


# ── Helpers ───────────────────────────────────────────────────────────────────

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


# ── Checkpoint ────────────────────────────────────────────────────────────────

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


# ── Setup ─────────────────────────────────────────────────────────────────────

def setup_tables():
    conn = get_connection()
    cur  = conn.cursor()

    # ── 1. Ensure indexes on source filter columns ───────────────────
    print("  Ensuring indexes on source tables...")
    _ensure_index(cur, conn, f"{SOURCE_SCHEMA}.PATIENTSURGERY",
                  "idx_active_flag", ["nd_active_flag"])
    _ensure_index(cur, conn, f"{SOURCE_SCHEMA}.PATIENTSURGERY",
                  "idx_deleted",     ["deleteddatetime"])
    _ensure_index(cur, conn, f"{SOURCE_SCHEMA}.PATIENTSURGICALHISTORY",
                  "idx_active_flag", ["nd_active_flag"])
    _ensure_index(cur, conn, f"{SOURCE_SCHEMA}.PATIENTSURGICALHISTORY",
                  "idx_deleted",     ["deleteddatetime"])
    _ensure_index(cur, conn, f"{SOURCE_SCHEMA}.PATIENTSURGICALHISTORY",
                  "idx_snomedcode",  ["snomedcode"])
    _ensure_index(cur, conn, f"{SOURCE_SCHEMA}.PATIENTSURGICALHISTORY",
                  "idx_shp_id",      ["SURGICALHISTORYPROCEDUREID"])

    # ── 2. Pre-materialize SNOMED lookup ─────────────────────────────
    print("  Materializing SNOMED lookup...")
    if not _table_exists(cur, STAGING_SNOMED):
        cur.execute(f"""
            CREATE TABLE {STAGING_SNOMED} AS
            SELECT SNOMEDCODE, DESCRIPTION
            FROM {SOURCE_SCHEMA}.SNOMED
        """)
        cur.execute(f"ALTER TABLE {STAGING_SNOMED} ADD INDEX idx_snomed (SNOMEDCODE)")
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")
    cur.execute(f"SELECT COUNT(*) FROM {STAGING_SNOMED}")
    print(f"    {cur.fetchone()[0]:,} SNOMED rows")

    # ── 3. Pre-materialize SURGICALHISTORYPROCEDURE lookup ────────────
    print("  Materializing SURGICALHISTORYPROCEDURE lookup (nd_active_flag='Y')...")
    if not _table_exists(cur, STAGING_SHP):
        cur.execute(f"""
            CREATE TABLE {STAGING_SHP} AS
            SELECT SURGICALHISTORYPROCEDUREID, NAME
            FROM {SOURCE_SCHEMA}.SURGICALHISTORYPROCEDURE
            WHERE nd_active_flag = 'Y'
        """)
        cur.execute(f"ALTER TABLE {STAGING_SHP} ADD INDEX idx_shp (SURGICALHISTORYPROCEDUREID)")
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")
    cur.execute(f"SELECT COUNT(*) FROM {STAGING_SHP}")
    print(f"    {cur.fetchone()[0]:,} SHP rows")

    # ── 4. Destination table ──────────────────────────────────────────
    print("  Creating destination table...")
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {DEST_TABLE} (
            surgicalhistoryid     BIGINT        DEFAULT NULL,
            ndid                  BIGINT        DEFAULT NULL,
            eid                   BIGINT        DEFAULT NULL,
            enc_date              DATE          DEFAULT NULL,
            surgery_date          DATE          DEFAULT NULL,
            surg_hist_type        VARCHAR(200)  DEFAULT NULL,
            surgery_name          TEXT,
            surgery_code          VARCHAR(50)   DEFAULT NULL,
            surgery_coding_system VARCHAR(20)   DEFAULT NULL,
            surgery_reason        TEXT,
            created_datetime      DATETIME      DEFAULT NULL,
            created_by            VARCHAR(50)   DEFAULT NULL,
            updated_datetime      DATETIME      DEFAULT NULL,
            updated_by            VARCHAR(50)   DEFAULT NULL,
            ehr_source_name       VARCHAR(100)  DEFAULT NULL,
            source_path           VARCHAR(100)  DEFAULT NULL,
            data_type             VARCHAR(50)   DEFAULT NULL,
            psid                  INT           DEFAULT NULL,
            nd_extracted_date     DATE          DEFAULT NULL
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
    """)
    conn.commit()
    print("    ready")

    # ── 5. Checkpoint table ───────────────────────────────────────────
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

    # ── 6. PK staging per source ──────────────────────────────────────
    source_ranges = {}
    for src in SOURCES:
        pk      = src["pk"]
        table   = src["table"]
        staging = src["pk_staging"]
        print(f"  Building PK staging for {table}...")

        if not _table_exists(cur, staging):
            if src["key"] == "patientsurgery":
                extra_where = (
                    "  AND nd_active_flag = 'Y'\n"
                    "  AND type <> 'REVIEWED.PATIENTSURGICALHISTORY'\n"
                    "  AND deleteddatetime IS NULL"
                )
            else:
                extra_where = (
                    "  AND nd_active_flag = 'Y'\n"
                    "  AND deleteddatetime IS NULL"
                )
            cur.execute(f"""
                CREATE TABLE {staging} AS
                SELECT {pk}
                FROM {SOURCE_SCHEMA}.{table}
                WHERE {pk} IS NOT NULL
                {extra_where}
            """)
            cur.execute(f"ALTER TABLE {staging} ADD INDEX idx_pk ({pk})")
            conn.commit()
            print("    created")
        else:
            print("    already exists, reusing")

        cur.execute(f"SELECT COUNT(*) FROM {staging}")
        count = cur.fetchone()[0]

        if count == 0:
            source_ranges[src["key"]] = []
            print(f"    0 rows — skipping")
            continue

        cur.execute(f"""
            SELECT {pk}
            FROM (
                SELECT {pk},
                       ROW_NUMBER() OVER (ORDER BY {pk}) AS rn
                FROM {staging}
            ) t
            WHERE (rn - 1) % {BATCH_SIZE} = 0
            ORDER BY {pk}
        """)
        boundaries = [row[0] for row in cur.fetchall() if row[0] is not None]
        cur.execute(f"SELECT MAX({pk}) FROM {staging}")
        max_pk = int(cur.fetchone()[0])

        ranges = []
        for i, lo in enumerate(boundaries):
            hi = boundaries[i + 1] if i + 1 < len(boundaries) else max_pk + 1
            ranges.append((lo, hi))

        source_ranges[src["key"]] = ranges
        print(f"    {count:,} rows → {len(ranges)} batches of ~{BATCH_SIZE:,}")

    cur.close()
    conn.close()
    return source_ranges


# ── Worker ────────────────────────────────────────────────────────────────────

def run_source(source, ranges, pbar):
    key  = source["key"]
    conn = get_connection()

    if is_done(conn, key):
        conn.close()
        pbar.update(len(ranges))
        return {"source": key, "status": "skipped", "rows": 0, "secs": 0}

    mark(conn, key, "running")
    t0         = time.time()
    total_rows = 0

    try:
        cur = conn.cursor()
        cur.execute("SET unique_checks = 0")
        cur.execute("SET foreign_key_checks = 0")

        for pk_lo, pk_hi in ranges:
            sql = build_batch_insert(source, pk_lo, pk_hi)
            cur.execute(sql)
            conn.commit()
            total_rows += cur.rowcount
            pbar.update(1)

        cur.execute("SET unique_checks = 1")
        cur.execute("SET foreign_key_checks = 1")
        cur.close()

        mark(conn, key, "done", total_rows)
        conn.close()
        return {"source": key, "status": "done",
                "rows": total_rows, "secs": round(time.time() - t0, 1)}

    except Exception as exc:
        elapsed = round(time.time() - t0, 1)
        mark(conn, key, "failed", total_rows, str(exc))
        try:
            conn.close()
        except Exception:
            pass
        return {"source": key, "status": f"FAILED: {exc}",
                "rows": total_rows, "secs": elapsed}


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print(f"\n{'='*70}", flush=True)
    print(f"  AthenaOne Surgical History ETL — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  source     : {SOURCE_SCHEMA}.PATIENTSURGERY + PATIENTSURGICALHISTORY  (psid={PSID})")
    print(f"  dest       : {DEST_TABLE}")
    print(f"  checkpoint : {CHECKPOINT_TABLE}")
    print(f"  workers    : {MAX_WORKERS}  |  batch_size : {BATCH_SIZE:,}")
    print(f"{'='*70}\n", flush=True)

    print("Setup:")
    sys.stdout.flush()
    source_ranges = setup_tables()
    print()

    total_batches = sum(len(r) for r in source_ranges.values())
    if total_batches == 0:
        print("No rows to process. Exiting.")
        return

    results = []
    with tqdm(total=total_batches, desc="Overall", unit="batch") as pbar:
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
        print(f"  [{tag}] {r['source']:<42} {r['rows']:>10,} rows  ({r['secs']}s)")

    done    = sum(1 for r in results if r["status"] == "done")
    skipped = sum(1 for r in results if r["status"] == "skipped")
    failed  = [r for r in results if "FAILED" in str(r["status"])]
    total   = sum(r["rows"] for r in results)

    print(f"\n{'='*70}")
    print(f"  Done: {done}  Skipped: {skipped}  Failed: {len(failed)}  |  Total rows: {total:,}")
    print(f"{'='*70}")

    print(f"\n  Cleanup SQL:")
    print(f"    DROP TABLE IF EXISTS {STAGING_SNOMED};")
    print(f"    DROP TABLE IF EXISTS {STAGING_SHP};")
    print(f"    DROP TABLE IF EXISTS {CHECKPOINT_TABLE};")
    for src in SOURCES:
        print(f"    DROP TABLE IF EXISTS {src['pk_staging']};")

    if failed:
        print("\n  Failed sources:")
        for r in failed:
            print(f"    {r['source']}: {r['status']}")
        sys.exit(1)


if __name__ == "__main__":
    main()
