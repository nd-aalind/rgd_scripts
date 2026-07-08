#!/usr/bin/env python3
"""
enc_opt.py — AthenaOne Dashboard: Optimized batched INSERT for patient_encounters

Sources (1 INSERT job, batched by APPOINTMENT_ID):
  STAGING_APPT (pre-materialized latest_appt CTE) -> staging.patient_encounters

Optimizations:
- latest_appt CTE (ROW_NUMBER OVER PARTITION BY APPOINTMENT_ID + APPOINTMENTCANCELREASON JOIN)
  pre-materialized once into staging.patient_enc_appt_<schema> with indexes.
  Avoids re-computing ROW_NUMBER on every batch.
- Batching by actual APPOINTMENT_ID values from staging (sparse-ID safe).
- visittype_mapping JOIN preserved per original SQL.
- Checkpoint/resume -- re-run skips if already completed.
- Commit after every batch.
- InnoDB checks disabled per-session for bulk speed.
- tqdm progress bar + file logging.

Usage:
    python enc_opt.py
"""

import logging
import os
import sys
import time
from datetime import datetime
import pymysql
from tqdm import tqdm
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())

# -- Configuration ---------------------------------------------------------
SOURCE_SCHEMA = "raleigh"   # <- change per run (AthenaOne schema: "tncpa", "raleigh", ...)

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

DEST_TABLE       = "reporting_raleigh.patient_encounters"
STAGING_APPT     = f"staging.patient_enc_appt_{SOURCE_SCHEMA}"
STAGING_PK       = f"staging.patient_enc_pk_{SOURCE_SCHEMA}"
CHECKPOINT_TABLE = f"staging.etl_checkpoint_patient_enc_{SOURCE_SCHEMA}"
CHECKPOINT_KEY   = f"patient_enc_{SOURCE_SCHEMA}"

BATCH_SIZE = 50_000
BATCH_KEY  = "APPOINTMENT_ID"

# -- Logging ---------------------------------------------------------------
_log_dir  = os.path.dirname(os.path.abspath(__file__))
_log_file = os.path.join(
    _log_dir,
    f"enc_opt_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log",
)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(_log_file, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)
log    = logger.info


# -- Helpers ---------------------------------------------------------------

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


def _index_exists(cur, schema, table, column):
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


# -- Checkpoint ------------------------------------------------------------

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


# -- Batch INSERT builder --------------------------------------------------

def build_batch_insert(pk_lo, pk_hi):
    """
    INSERT from pre-indexed STAGING_APPT (ROW_NUMBER already computed).
    Batches on APPOINTMENT_ID -- no ROW_NUMBER re-computation per batch.

    AND la.rn = 1 in the CLINICALENCOUNTER JOIN ON clause is preserved from
    original SQL: rows with rn != 1 are included but encounter columns are NULL.
    The second PROVIDER join (p, for SCHEDULINGPROVIDERID) is unused in SELECT
    but preserved from the original SQL.
    """
    return f"""
INSERT INTO {DEST_TABLE}
    (encounter_id, appointment_id, appointment_date, appointment_start_time,
     visit_type, appointment_status, cancellation_flag, cancellation_reason,
     patient_id, encounter_date, chartid, rendering_provider_name,
     scheduling_provider, visit_type_grouped)
SELECT
    a.CLINICALENCOUNTERID                   AS encounter_id,
    la.APPOINTMENT_ID                       AS appointment_id,
    la.APPOINTMENT_DATE                     AS appointment_date,
    la.APPOINTMENT_START_TIME               AS appointment_start_time,
    la.BOOKED_APPOINTMENT_NAME              AS visit_type,
    la.appointment_status                   AS appointment_status,
    la.cancellation_flag                    AS cancellation_flag,
    la.cancellation_reason                  AS cancellation_reason,
    la.PATIENT_ID                           AS patient_id,
    a.ENCOUNTERDATE                         AS encounter_date,
    b.CHARTID                               AS chartid,
    c.PATIENTFACINGNAME                     AS rendering_provider_name,
    ap2.SCHEDULINGPROVIDER                  AS scheduling_provider,
    vm.visit_type_grouped
FROM {STAGING_APPT} la
LEFT JOIN {SOURCE_SCHEMA}.CLINICALENCOUNTER a
    ON  a.APPOINTMENTID  = la.APPOINTMENT_ID
    AND a.nd_active_flag = 'Y'
    AND la.rn = 1
LEFT JOIN {SOURCE_SCHEMA}.CHART b
    ON  b.CHARTID        = a.CHARTID
    AND b.nd_active_flag = 'Y'
LEFT JOIN {SOURCE_SCHEMA}.PROVIDER c
    ON  c.PROVIDERID     = a.PROVIDERID
    AND c.nd_active_flag = 'Y'
LEFT JOIN {SOURCE_SCHEMA}.visittype_mapping vm
    ON  LOWER(TRIM(vm.visit_type)) = LOWER(TRIM(la.BOOKED_APPOINTMENT_NAME))
LEFT JOIN {SOURCE_SCHEMA}.appointment_2 ap2
    ON  ap2.APPOINTMENTID  = la.APPOINTMENT_ID
    AND ap2.nd_active_flag = 'Y'
LEFT JOIN {SOURCE_SCHEMA}.PROVIDER p
    ON  p.PROVIDERID       = ap2.SCHEDULINGPROVIDERID
    AND p.nd_active_flag   = 'Y'
WHERE la.{BATCH_KEY} >= {pk_lo}
  AND la.{BATCH_KEY} <  {pk_hi}
"""


# -- Setup -----------------------------------------------------------------

def setup_tables():
    conn = get_connection()
    cur  = conn.cursor()

    # 1. Materialize latest_appt CTE
    log(f"  Materializing latest_appt CTE -> {STAGING_APPT}...")
    if not _table_exists(cur, STAGING_APPT):
        cur.execute(f"""
            CREATE TABLE {STAGING_APPT} AS
            SELECT
                d.PATIENT_ID,
                d.APPOINTMENT_ID,
                d.BOOKED_APPOINTMENT_NAME,
                d.APPOINTMENT_DATE,
                d.APPOINTMENT_START_TIME,
                CASE
                    WHEN d.STATUS_TYPE = 'f'          THEN 'Scheduled'
                    WHEN d.STATUS_TYPE = '2'           THEN 'Checked In'
                    WHEN d.STATUS_TYPE IN ('3', '4')   THEN 'Completed'
                    WHEN d.STATUS_TYPE = 'x'           THEN 'Cancelled'
                    WHEN d.STATUS_TYPE = 'o'           THEN 'Open'
                    ELSE NULL
                END AS appointment_status,
                CASE WHEN d.STATUS_TYPE = 'x' THEN 1 ELSE 0 END AS cancellation_flag,
                COALESCE(d.CANCELLED_REASON_LOCAL_NAME, acr.NAME) AS cancellation_reason,
                d.lastupdated,
                ROW_NUMBER() OVER (
                    PARTITION BY d.APPOINTMENT_ID
                    ORDER BY d.lastupdated DESC
                ) AS rn
            FROM {SOURCE_SCHEMA}.APPOINTMENT d
            LEFT JOIN {SOURCE_SCHEMA}.APPOINTMENTCANCELREASON acr
                ON  acr.NAME           = d.CANCELLED_REASON_LOCAL_NAME
                AND acr.nd_active_flag = 'Y'
            WHERE d.nd_active_flag = 'Y'
        """)
        conn.commit()
        cur.execute(f"SELECT COUNT(*) FROM {STAGING_APPT}")
        n = cur.fetchone()[0]
        log(f"    {n:,} rows materialized")

        stg_schema, stg_tbl = STAGING_APPT.split(".", 1)
        for col in (BATCH_KEY, "PATIENT_ID", "rn"):
            if not _index_exists(cur, stg_schema, stg_tbl, col):
                cur.execute(f"ALTER TABLE {STAGING_APPT} ADD INDEX idx_{col} ({col})")
                conn.commit()
                log(f"    index added: {col}")
    else:
        cur.execute(f"SELECT COUNT(*) FROM {STAGING_APPT}")
        n = cur.fetchone()[0]
        log(f"    already exists, reusing  ({n:,} rows)")

    # 2. Ensure indexes on source join columns
    log(f"  Checking indexes on source join columns...")
    for tbl, col in [
        ("CLINICALENCOUNTER", "APPOINTMENTID"),
        ("CLINICALENCOUNTER", "nd_active_flag"),
        ("CHART",             "CHARTID"),
        ("PROVIDER",          "PROVIDERID"),
        ("appointment_2",     "APPOINTMENTID"),
    ]:
        if not _index_exists(cur, SOURCE_SCHEMA, tbl, col):
            log(f"    creating: {SOURCE_SCHEMA}.{tbl} ({col})...")
            try:
                cur.execute(
                    f"CREATE INDEX idx_{col} ON `{SOURCE_SCHEMA}`.`{tbl}` ({col})"
                )
                conn.commit()
                log(f"      done")
            except Exception as exc:
                try:
                    conn.rollback()
                except Exception:
                    pass
                log(f"      warning: {exc}")
        else:
            log(f"    exists: {SOURCE_SCHEMA}.{tbl} ({col})")

    # 3. Destination table
    log(f"  Creating destination table {DEST_TABLE} if needed...")
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {DEST_TABLE} (
            encounter_id            BIGINT        DEFAULT NULL,
            appointment_id          BIGINT        DEFAULT NULL,
            appointment_date        DATE          DEFAULT NULL,
            appointment_start_time  TIME          DEFAULT NULL,
            visit_type              VARCHAR(500)  DEFAULT NULL,
            appointment_status      VARCHAR(50)   DEFAULT NULL,
            cancellation_flag       TINYINT(1)    DEFAULT NULL,
            cancellation_reason     VARCHAR(500)  DEFAULT NULL,
            patient_id              BIGINT        DEFAULT NULL,
            encounter_date          DATE          DEFAULT NULL,
            chartid                 BIGINT        DEFAULT NULL,
            rendering_provider_name VARCHAR(500)  DEFAULT NULL,
            scheduling_provider     VARCHAR(500)  DEFAULT NULL,
            visit_type_grouped      VARCHAR(255)  DEFAULT NULL
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
    """)
    conn.commit()
    log("    ready")

    # 4. Checkpoint table
    log(f"  Creating checkpoint table...")
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
    log("    ready")

    # 5. PK staging -- distinct APPOINTMENT_IDs for batch range generation
    log(f"  Creating PK staging table {STAGING_PK}...")
    if not _table_exists(cur, STAGING_PK):
        cur.execute(f"""
            CREATE TABLE {STAGING_PK} AS
            SELECT DISTINCT {BATCH_KEY}
            FROM {STAGING_APPT}
            WHERE {BATCH_KEY} IS NOT NULL
        """)
        cur.execute(f"ALTER TABLE {STAGING_PK} ADD INDEX idx_pk ({BATCH_KEY})")
        conn.commit()
        cur.execute(f"SELECT COUNT(*) FROM {STAGING_PK}")
        n = cur.fetchone()[0]
        log(f"    {n:,} distinct {BATCH_KEY} values")
    else:
        cur.execute(f"SELECT COUNT(*) FROM {STAGING_PK}")
        n = cur.fetchone()[0]
        log(f"    already exists, reusing  ({n:,} rows)")

    ranges, total = _build_ranges(cur)
    log(f"    {len(ranges)} batches of ~{BATCH_SIZE:,}  (total distinct PKs: {total:,})")

    cur.close()
    conn.close()
    return ranges


# -- Runner ----------------------------------------------------------------

def run_insert(ranges, pbar):
    conn       = get_connection()
    t0         = time.time()
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
        log(f"\n  [ERROR] {err_msg}")
        try:
            mark(conn, "failed", total_rows, err_msg)
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass
        return {"status": f"FAILED: {exc}", "rows": total_rows, "secs": elapsed}


# -- Main ------------------------------------------------------------------

def main():
    log(f"\n{'='*70}")
    log(f"  AthenaOne Dashboard: patient_encounters ETL -- {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log(f"  source schema : {SOURCE_SCHEMA}")
    log(f"  dest          : {DEST_TABLE}")
    log(f"  staging appt  : {STAGING_APPT}")
    log(f"  staging pk    : {STAGING_PK}")
    log(f"  checkpoint    : {CHECKPOINT_TABLE}")
    log(f"  batch size    : {BATCH_SIZE:,}")
    log(f"  log file      : {_log_file}")
    log(f"{'='*70}\n")

    ranges = setup_tables()

    if not ranges:
        log("  No eligible rows found. Exiting.")
        return

    result = None
    with tqdm(total=len(ranges), desc="patient_encounters", unit="batch") as pbar:
        result = run_insert(ranges, pbar)

    log("")
    tag = ("DONE" if result["status"] == "done" else
           "SKIP" if result["status"] == "skipped" else "FAIL")

    log(f"\n{'='*70}")
    log(f"  [{tag}]  {result['rows']:>12,} rows inserted  ({result['secs']}s)")
    if result["status"].startswith("FAILED"):
        log(f"  ERROR: {result['status']}")
    log(f"{'='*70}")

    conn = get_connection()
    cur  = conn.cursor()
    try:
        if _table_exists(cur, DEST_TABLE):
            cur.execute(
                f"SELECT COUNT(*), COUNT(DISTINCT appointment_id) FROM {DEST_TABLE}"
            )
            row = cur.fetchone()
            log(f"\n  {DEST_TABLE}")
            log(f"    rows                    : {row[0]:,}")
            log(f"    distinct appointment_id : {row[1]:,}")
    finally:
        cur.close()
        conn.close()

    log(f"\n  Cleanup SQL (run after verifying data):")
    log(f"    DROP TABLE IF EXISTS {STAGING_APPT};")
    log(f"    DROP TABLE IF EXISTS {STAGING_PK};")
    log(f"    DROP TABLE IF EXISTS {CHECKPOINT_TABLE};")

    if result["status"].startswith("FAILED"):
        sys.exit(1)


if __name__ == "__main__":
    main()
