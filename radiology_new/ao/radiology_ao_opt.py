#!/usr/bin/env python3
"""
Optimized ETL for: AthenaOne Imaging Clinical Results → destination table

Sources (1 INSERT job):
  1. CLINICALRESULT — imaging results joined with CLINICALRESULTOBSERVATION,
     DOCUMENT, and CLINICALENCOUNTER; filtered to IMAGING order type group

Optimizations:
- Staging table pre-materialized once (not re-scanned per batch)
- Batching by actual PK values (sparse ID safe)
- ThreadPoolExecutor with 1 worker
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


SOURCE_SCHEMA = 'raleigh'
PSID = 5


# ── Configuration ─────────────────────────────────────────────────────────────
DB_CONFIG = {
    "host":            "ndai-dev-rds-instance.cwp60ymu4ko0.us-east-1.rds.amazonaws.com",
    "port":            3306,
    "user":            "Aalind",
    "password":        "A@L1nd@123",
    "database":        SOURCE_SCHEMA,
    "charset":         "utf8mb4",
    "connect_timeout": 30,
    "read_timeout":    21600,
    "write_timeout":   21600,
}

BATCH_SIZE  = 50_000
MAX_WORKERS = 1   # single source branch

DEST_TABLE       = "rgd_udm_staging.radiology_new"
STAGING_TABLE    = f"staging.tmp_imaging_cr_staging_v1_{SOURCE_SCHEMA}"
CHECKPOINT_TABLE = f"staging.etl_checkpoint_imaging_cr_v1_{SOURCE_SCHEMA}"

# ── Source definitions ────────────────────────────────────────────────────────
SOURCES = [
    {
        "key":        "CLINICALRESULT_IMAGING",
        "table":      "CLINICALRESULT",
        "alias":      "cr",
        "pk":         "CLINICALRESULTID",
        "pk_staging": "staging.tmp_imaging_cr_pk_staging",
    },
]



# ── Helpers ───────────────────────────────────────────────────────────────────
def get_connection():
    """One connection per call — each thread gets its own."""
    return pymysql.connect(**DB_CONFIG)


def _table_exists(cur, full_table_name):
    schema, table = full_table_name.split(".")
    cur.execute(
        "SELECT COUNT(*) FROM information_schema.tables "
        "WHERE table_schema = %s AND table_name = %s",
        (schema, table),
    )
    return cur.fetchone()[0] > 0


def _index_exists(cur, schema, table, column):
    cur.execute("""
        SELECT COUNT(*) FROM information_schema.statistics
        WHERE table_schema = %s AND table_name = %s AND column_name = %s
    """, (schema, table, column))
    return cur.fetchone()[0] > 0


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


def date_case(col):
    """EHR-safe VARCHAR → DATE conversion covering common AthenaOne formats."""
    return (
        f"CASE\n"
        f"  WHEN {col} IS NULL OR {col} IN ('', 'None') THEN NULL\n"
        f"  WHEN {col} REGEXP '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}} [0-9]{{2}}:[0-9]{{2}}:[0-9]{{2}}$'\n"
        f"      THEN DATE(STR_TO_DATE({col}, '%Y-%m-%d %H:%i:%s'))\n"
        f"  WHEN {col} REGEXP '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}$'\n"
        f"      THEN STR_TO_DATE({col}, '%Y-%m-%d')\n"
        f"  WHEN {col} REGEXP '^[0-9]{{2}}-[0-9]{{2}}-[0-9]{{4}} [0-9]{{2}}:[0-9]{{2}}:[0-9]{{2}}$'\n"
        f"      THEN DATE(STR_TO_DATE({col}, '%m-%d-%Y %H:%i:%s'))\n"
        f"  WHEN {col} REGEXP '^[0-9]{{2}}-[0-9]{{2}}-[0-9]{{4}}$'\n"
        f"      THEN STR_TO_DATE({col}, '%m-%d-%Y')\n"
        f"  ELSE NULL\n"
        f"END"
    )


# ── Batch INSERT builder ──────────────────────────────────────────────────────
def build_batch_insert(source, pk_lo, pk_hi):
    """Returns the INSERT … SELECT SQL for one batch range."""
    S = SOURCE_SCHEMA

    enc_date_expr  = date_case("ce.ENCOUNTERDATE")
    obs_date_expr  = date_case("cr.OBSERVATIONDATETIME")
    order_date_expr = (
        f"CASE WHEN (d.ORDERDATETIME) IN ('', 'None', 'null') THEN NULL\n"
        f"     ELSE DATE(STR_TO_DATE(d.ORDERDATETIME, '%m-%d-%Y %H:%i:%s')) END"
    )

    return f"""
INSERT INTO {DEST_TABLE} (
    result_id, ndid, eid, enc_date, img_date,
    study_name, modality, img_status,
    img_report_text, img_finding,
    report_id, report_date, report_status,
    img_reason, order_date, order_status,
    order_prescription, provider_id,
    provider_name, provider_npi,
    internal_notes, note_to_patient,
    facility_id, interpretation,
    source, result, report_text,
    created_datetime, created_by,
    updated_datetime, updated_by,
    ehr_source_name, source_path, data_type, psid
)
SELECT
    cr.CLINICALRESULTID                                          AS result_id,
    COALESCE(d.CHARTID, ce.chartid)                             AS ndid,
    ce.CLINICALENCOUNTERID                                      AS eid,
    {enc_date_expr}                                             AS enc_date,
    {obs_date_expr}                                             AS img_date,
    cr.CLINICALORDERTYPE                                        AS study_name,
    cr.CLINICALORDERGENUS                                       AS modality,
    cro.RESULTSTATUS                                            AS img_status,
    NULL                                                        AS img_report_text,
    GROUP_CONCAT(DISTINCT COALESCE(cro.result, d.documenttextdata)
                 SEPARATOR '\n')                                AS img_finding,
    d.DOCUMENTID                                                AS report_id,
    {obs_date_expr}                                             AS report_date,
    d.STATUS                                                    AS report_status,
    NULL                                                        AS img_reason,
    {order_date_expr}                                           AS order_date,
    cr.REPORTSTATUS                                             AS order_status,
    cr.ORDERDOCUMENTID                                          AS order_prescription,
    cr.CLINICALPROVIDERID                                       AS provider_id,
    NULL                                                        AS provider_name,
    NULL                                                        AS provider_npi,
    CONCAT(COALESCE(cro.observationnote, ''), ' - ',
           COALESCE(d.providernote, ''))                        AS internal_notes,
    cr.EXTERNALNOTE                                             AS note_to_patient,
    d.DEPARTMENTID                                              AS facility_id,
    NULL                                                        AS interpretation,
    d.SOURCE                                                    AS source,
    NULL                                                        AS result,
    NULL                                                        AS report_text,
    CURRENT_TIMESTAMP()                                         AS created_datetime,
    'ND'                                                        AS created_by,
    CURRENT_TIMESTAMP()                                         AS updated_datetime,
    'ND'                                                        AS updated_by,
    'athenaone'                                                 AS ehr_source_name,
    'bronze_layer'                                              AS source_path,
    'Structured'                                                AS data_type,
    {PSID}                                                      AS psid
FROM {S}.CLINICALRESULT cr
LEFT JOIN (
    SELECT * FROM {S}.CLINICALRESULTOBSERVATION WHERE nd_active_flag = 'Y'
) cro ON cr.CLINICALRESULTID = cro.CLINICALRESULTID
JOIN (
    SELECT * FROM {S}.DOCUMENT WHERE nd_active_flag = 'Y'
) d ON cr.DOCUMENTID = d.DOCUMENTID
LEFT JOIN (
    SELECT * FROM {S}.CLINICALENCOUNTER WHERE nd_active_flag = 'Y'
) ce ON d.chartid = ce.chartid
   AND ce.clinicalencounterid = d.clinicalencounterid
WHERE cr.clinicalordertypegroup = 'IMAGING'
  AND cr.nd_active_flag = 'Y'
  AND cr.CLINICALRESULTID >= {pk_lo}
  AND cr.CLINICALRESULTID <  {pk_hi}
GROUP BY
    cr.CLINICALRESULTID,
    COALESCE(d.CHARTID, ce.chartid),
    ce.CLINICALENCOUNTERID,
    {enc_date_expr},
    cr.CLINICALORDERTYPE,
    cr.CLINICALORDERGENUS,
    {obs_date_expr},
    cro.RESULTSTATUS,
    d.DOCUMENTID,
    d.STATUS,
    {order_date_expr},
    cr.REPORTSTATUS,
    cr.ORDERDOCUMENTID,
    cr.CLINICALPROVIDERID,
    CONCAT(COALESCE(cro.observationnote, ''), ' - ', COALESCE(d.providernote, '')),
    cr.EXTERNALNOTE,
    d.SOURCE
"""


# ── Setup ─────────────────────────────────────────────────────────────────────
def setup_tables():
    """Create staging, dest, checkpoint tables. Return batch ranges per source."""
    conn = get_connection()
    cur  = conn.cursor()

    # ── 1. Source table indexes — must exist BEFORE staging table creation ────────
    # prefix: TEXT/BLOB columns require a key length; None = use column as-is
    index_defs = [
        ("CLINICALRESULT",            "CLINICALRESULTID",       None),
        ("CLINICALRESULT",            "DOCUMENTID",             None),
        ("CLINICALRESULT",            "clinicalordertypegroup", 100),
        ("CLINICALRESULT",            "nd_active_flag",         None),
        ("CLINICALRESULTOBSERVATION", "CLINICALRESULTID",       None),
        ("CLINICALRESULTOBSERVATION", "nd_active_flag",         None),
        ("DOCUMENT",                  "DOCUMENTID",             None),
        ("DOCUMENT",                  "nd_active_flag",         None),
        ("CLINICALENCOUNTER",         "clinicalencounterid",    None),
        ("CLINICALENCOUNTER",         "nd_active_flag",         None),
    ]
    print("  Checking/creating source table indexes...", flush=True)
    for tbl, col, prefix in index_defs:
        print(f"    {SOURCE_SCHEMA}.{tbl} ({col})...", end=" ", flush=True)
        if not _index_exists(cur, SOURCE_SCHEMA, tbl, col):
            col_spec = f"{col}({prefix})" if prefix else col
            print("missing — creating...", flush=True)
            cur.execute(f"CREATE INDEX idx_{col} ON {SOURCE_SCHEMA}.{tbl} ({col_spec})")
            conn.commit()
            print("      done")
        else:
            print("exists")

    # ── 2. Staging table: pre-filter CLINICALRESULTID for IMAGING rows ─────────
    print("  Creating staging table...", flush=True)
    if not _table_exists(cur, STAGING_TABLE):
        print("    creating...", flush=True)
        cur.execute(f"""
            CREATE TABLE {STAGING_TABLE} AS
            SELECT DISTINCT cr.CLINICALRESULTID
            FROM {SOURCE_SCHEMA}.CLINICALRESULT cr
            JOIN {SOURCE_SCHEMA}.DOCUMENT d
              ON cr.DOCUMENTID = d.DOCUMENTID
             AND d.nd_active_flag = 'Y'
            WHERE cr.clinicalordertypegroup = 'IMAGING'
              AND cr.nd_active_flag = 'Y'
            ORDER BY cr.CLINICALRESULTID
        """)
        cur.execute(f"ALTER TABLE {STAGING_TABLE} ADD INDEX idx_pk (CLINICALRESULTID)")
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")

    # ── 3. Destination table ───────────────────────────────────────────────────
    print("  Creating destination table if needed...")
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {DEST_TABLE} (
            result_id           BIGINT,
            ndid                BIGINT,
            eid                 BIGINT,
            enc_date            DATE,
            img_date            DATE,
            study_name          VARCHAR(500),
            modality            VARCHAR(255),
            img_status          VARCHAR(255),
            img_report_text     TEXT,
            img_finding         LONGTEXT,
            report_id           BIGINT,
            report_date         DATE,
            report_status       VARCHAR(255),
            img_reason          VARCHAR(500),
            order_date          DATE,
            order_status        VARCHAR(255),
            order_prescription  VARCHAR(500),
            provider_id         BIGINT,
            provider_name       VARCHAR(500),
            provider_npi        VARCHAR(20),
            internal_notes      TEXT,
            note_to_patient     TEXT,
            facility_id         BIGINT,
            interpretation      TEXT,
            source              VARCHAR(255),
            result              TEXT,
            report_text         TEXT,
            created_datetime    DATETIME,
            created_by          VARCHAR(50),
            updated_datetime    DATETIME,
            updated_by          VARCHAR(50),
            ehr_source_name     VARCHAR(100),
            source_path         VARCHAR(100),
            data_type           VARCHAR(50),
            psid                INT
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
    """)
    conn.commit()

    # ── 4. Checkpoint table ────────────────────────────────────────────────────
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

    # ── 5. Batch boundaries (sparse-ID safe) ───────────────────────────────────
    print("  Computing batch boundaries...")
    cur.execute(f"SELECT COUNT(*) FROM {STAGING_TABLE}")
    total = cur.fetchone()[0]

    if total == 0:
        cur.close()
        conn.close()
        print("  No IMAGING rows found in source. Exiting.")
        return {}

    source_ranges = {}
    for src in SOURCES:
        pk  = src["pk"]
        stg = src["pk_staging"]

        if not _table_exists(cur, stg):
            cur.execute(f"""
                CREATE TABLE {stg} AS
                SELECT {pk} FROM {STAGING_TABLE}
                WHERE {pk} IS NOT NULL ORDER BY {pk}
            """)
            cur.execute(f"ALTER TABLE {stg} ADD INDEX idx_pk ({pk})")
            conn.commit()

        cur.execute(f"SELECT COUNT(*) FROM {stg}")
        count = cur.fetchone()[0]

        if count == 0:
            source_ranges[src["key"]] = []
            continue

        cur.execute(f"""
            SELECT {pk}
            FROM (
                SELECT {pk}, ROW_NUMBER() OVER (ORDER BY {pk}) AS rn
                FROM {stg}
            ) t
            WHERE (rn - 1) % {BATCH_SIZE} = 0
            ORDER BY {pk}
        """)
        boundaries = [row[0] for row in cur.fetchall()]

        cur.execute(f"SELECT MAX({pk}) FROM {stg}")
        max_pk = int(cur.fetchone()[0])

        ranges = []
        for i, lo in enumerate(boundaries):
            hi = boundaries[i + 1] if i + 1 < len(boundaries) else max_pk + 1
            ranges.append((lo, hi))

        source_ranges[src["key"]] = ranges
        print(f"  {src['key']}: {count:,} rows → {len(ranges)} batches")

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
    t0, total_rows = time.time(), 0

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
        return {
            "source": key,
            "status": "done",
            "rows":   total_rows,
            "secs":   round(time.time() - t0, 1),
        }

    except Exception as exc:
        mark(conn, key, "failed", total_rows, str(exc))
        try:
            conn.close()
        except Exception:
            pass
        return {
            "source": key,
            "status": f"FAILED: {exc}",
            "rows":   total_rows,
            "secs":   round(time.time() - t0, 1),
        }


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    print(f"\n{'='*70}")
    print(f"  ETL — Imaging Clinical Results  [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]")
    print(f"  source     : {SOURCE_SCHEMA}")
    print(f"  dest       : {DEST_TABLE}")
    print(f"  staging    : {STAGING_TABLE}")
    print(f"  checkpoint : {CHECKPOINT_TABLE}")
    print(f"  workers    : {MAX_WORKERS}  |  batch_size : {BATCH_SIZE:,}")
    print(f"{'='*70}\n")

    source_ranges = setup_tables()

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
        tag = ("DONE" if r["status"] == "done"
               else "SKIP" if r["status"] == "skipped"
               else "FAIL")
        print(f"  [{tag}] {r['source']:<50} {r['rows']:>10,} rows  ({r['secs']}s)")

    done    = sum(1 for r in results if r["status"] == "done")
    skipped = sum(1 for r in results if r["status"] == "skipped")
    failed  = [r for r in results if "FAILED" in str(r["status"])]
    total   = sum(r["rows"] for r in results)

    print(f"\n{'='*70}")
    print(f"  Done: {done}  Skipped: {skipped}  Failed: {len(failed)}  |  "
          f"Total rows: {total:,}")
    print(f"{'='*70}")

    print(f"\n  Cleanup SQL (run after verifying load):")
    print(f"    DROP TABLE IF EXISTS {STAGING_TABLE};")
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