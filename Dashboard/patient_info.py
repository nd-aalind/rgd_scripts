#!/usr/bin/env python3
"""
patient_info.py — AthenaOne Dashboard: Optimized batched INSERT for patient_information

Sources (1 INSERT job, batched by patientid / ENTERPRISEID):
  STAGING_PATIENT (pre-materialized inner SELECT + ROW_NUMBER) -> patient_information

Optimizations:
- PATIENTINSURANCE subquery (ins_rn=1) pre-materialized once into STAGING_INS.
- patient_encounters last-enc aggregation pre-materialized once into STAGING_LAST_ENC.
- Full inner SELECT + ROW_NUMBER OVER (PARTITION BY ENTERPRISEID) pre-materialized
  into STAGING_PATIENT — avoids per-batch window function re-computation.
- Batch INSERT from STAGING_PATIENT WHERE rn=1 batched on patientid.
- Checkpoint/resume -- re-run skips if already completed.
- Commit after every batch.
- InnoDB checks disabled per-session for bulk speed.
- tqdm progress bar + file logging.

Usage:
    python patient_info.py
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
SOURCE_SCHEMA    = "raleigh"             # <- AthenaOne schema (PATIENT, CHART, etc.)
DEST_SCHEMA      = "reporting_raleigh"   # <- schema for patient_information + patient_encounters

# Activity cutoff date used in active_flag_* CASE WHEN expressions
ACTIVITY_CUTOFF  = "2025-07-01"         # <- update per reporting cycle

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

DEST_TABLE       = f"{DEST_SCHEMA}.patient_information"

# Pre-materialized staging tables (reused across re-runs unless dropped)
STAGING_INS      = f"staging.pat_info_ins_{SOURCE_SCHEMA}"       # PATIENTINSURANCE ins_rn=1
STAGING_LAST_ENC = f"staging.pat_info_last_enc_{SOURCE_SCHEMA}"  # last encounter per patient
STAGING_PATIENT  = f"staging.pat_info_base_{SOURCE_SCHEMA}"      # full inner SELECT + rn

# Per-run tables
STAGING_PK       = f"staging.pat_info_pk_{SOURCE_SCHEMA}"
CHECKPOINT_TABLE = f"staging.etl_checkpoint_pat_info_{SOURCE_SCHEMA}"
CHECKPOINT_KEY   = f"patient_info_{SOURCE_SCHEMA}"

BATCH_SIZE = 50_000
BATCH_KEY  = "patientid"    # ENTERPRISEID aliased in staging

# -- Logging ---------------------------------------------------------------
_log_dir  = os.path.dirname(os.path.abspath(__file__))
_log_file = os.path.join(
    _log_dir,
    f"patient_info_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log",
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
    INSERT from pre-indexed STAGING_PATIENT.
    ROW_NUMBER already computed -- no window function per batch.
    Only rn=1 rows (de-duplicated by ENTERPRISEID) are inserted.
    """
    return f"""
INSERT INTO {DEST_TABLE}
    (registrationdate, patientid, ndid, gender, ufname, ulname, dob,
     race, race_grouped, uemail, mobile, upaddress, upaddress2, upcity,
     upstate, upPhone, zipcode, country_name, patientstatus, primaryprovider,
     deceased_status, deceasedDate,
     active_flag_6m, active_flag_12m, active_flag_18m, active_flag_24m)
SELECT
    registrationdate,
    patientid,
    chartid         AS ndid,
    gender,
    ufname,
    ulname,
    dob,
    race,
    race_grouped,
    uemail,
    mobile,
    upaddress,
    upaddress2,
    upcity,
    upstate,
    upPhone,
    zipcode,
    country_name,
    patientstatus,
    primaryprovider,
    deceased_status,
    deceasedDate,
    active_flag_6m,
    active_flag_12m,
    active_flag_18m,
    active_flag_24m
FROM {STAGING_PATIENT}
WHERE rn = 1
  AND {BATCH_KEY} >= {pk_lo}
  AND {BATCH_KEY} <  {pk_hi}
"""


# -- Staging materializations ----------------------------------------------

def _materialize_staging_ins(cur, conn):
    """Pre-materialize PATIENTINSURANCE ins_rn=1 (first insurance per patient)."""
    log(f"  Materializing PATIENTINSURANCE (ins_rn=1) -> {STAGING_INS}...")
    if not _table_exists(cur, STAGING_INS):
        cur.execute(f"""
            CREATE TABLE {STAGING_INS} AS
            SELECT PATIENTID, INSUREDCOUNTRYID, COUNTRY
            FROM (
                SELECT
                    PATIENTID,
                    INSUREDCOUNTRYID,
                    COUNTRY,
                    ROW_NUMBER() OVER (
                        PARTITION BY PATIENTID
                        ORDER BY SEQUENCENUMBER ASC
                    ) AS ins_rn
                FROM {SOURCE_SCHEMA}.PATIENTINSURANCE
                WHERE DELETEDDATETIME IS NULL
            ) t
            WHERE ins_rn = 1
        """)
        conn.commit()
        ins_schema, ins_tbl = STAGING_INS.split(".", 1)
        if not _index_exists(cur, ins_schema, ins_tbl, "PATIENTID"):
            cur.execute(f"ALTER TABLE {STAGING_INS} ADD INDEX idx_patientid (PATIENTID)")
            conn.commit()
        cur.execute(f"SELECT COUNT(*) FROM {STAGING_INS}")
        log(f"    {cur.fetchone()[0]:,} rows")
    else:
        cur.execute(f"SELECT COUNT(*) FROM {STAGING_INS}")
        log(f"    already exists, reusing  ({cur.fetchone()[0]:,} rows)")


def _materialize_staging_last_enc(cur, conn):
    """Pre-materialize last encounter date per patient from patient_encounters."""
    log(f"  Materializing last_enc per patient -> {STAGING_LAST_ENC}...")
    if not _table_exists(cur, STAGING_LAST_ENC):
        cur.execute(f"""
            CREATE TABLE {STAGING_LAST_ENC} AS
            SELECT patient_id, MAX(encounter_date) AS last_enc
            FROM {DEST_SCHEMA}.patient_encounters
            GROUP BY patient_id
        """)
        conn.commit()
        le_schema, le_tbl = STAGING_LAST_ENC.split(".", 1)
        if not _index_exists(cur, le_schema, le_tbl, "patient_id"):
            cur.execute(f"ALTER TABLE {STAGING_LAST_ENC} ADD INDEX idx_patient_id (patient_id)")
            conn.commit()
        cur.execute(f"SELECT COUNT(*) FROM {STAGING_LAST_ENC}")
        log(f"    {cur.fetchone()[0]:,} rows")
    else:
        cur.execute(f"SELECT COUNT(*) FROM {STAGING_LAST_ENC}")
        log(f"    already exists, reusing  ({cur.fetchone()[0]:,} rows)")


def _materialize_staging_patient(cur, conn):
    """
    Pre-materialize full inner SELECT + ROW_NUMBER OVER (PARTITION BY ENTERPRISEID).
    Joins STAGING_INS and STAGING_LAST_ENC instead of raw subqueries.
    """
    log(f"  Materializing full patient base + ROW_NUMBER -> {STAGING_PATIENT}...")
    if not _table_exists(cur, STAGING_PATIENT):
        cur.execute(f"""
            CREATE TABLE {STAGING_PATIENT} AS
            SELECT
                p.registrationdate,
                p.ENTERPRISEID                  AS patientid,
                ch.chartid,
                CASE
                    WHEN LOWER(p.sex) = 'm' THEN 'Male'
                    WHEN LOWER(p.sex) = 'f' THEN 'Female'
                    ELSE 'Unknown'
                END                             AS gender,
                p.FIRSTNAME                     AS ufname,
                p.LASTNAME                      AS ulname,
                p.DOB                           AS dob,
                p.RACE                          AS race,
                rc.race_grouped,
                p.EMAIL                         AS uemail,
                p.MOBILEPHONE                   AS mobile,
                p.ADDRESS                       AS upaddress,
                p.ADDRESS2                      AS upaddress2,
                p.CITY                          AS upcity,
                p.STATE                         AS upstate,
                p.PATIENTHOMEPHONE              AS upPhone,
                p.ZIP                           AS zipcode,
                COALESCE(cntry.NAME, pi.COUNTRY) AS country_name,
                CASE p.PATIENTSTATUS
                    WHEN 'i' THEN 'Inactive'
                    WHEN 'd' THEN 'Deleted'
                    WHEN 'a' THEN 'Active'
                    ELSE 'Pending'
                END                             AS patientstatus,
                pro.BILLEDNAME                  AS primaryprovider,
                CASE WHEN p.DECEASEDDATE IS NOT NULL THEN 1 ELSE 0
                END                             AS deceased_status,
                p.DECEASEDDATE                  AS deceasedDate,
                CASE
                    WHEN p.DECEASEDDATE IS NOT NULL THEN 0
                    WHEN le.last_enc > DATE_SUB('{ACTIVITY_CUTOFF}', INTERVAL 6 MONTH) THEN 1
                    ELSE 0
                END                             AS active_flag_6m,
                CASE
                    WHEN p.DECEASEDDATE IS NOT NULL THEN 0
                    WHEN le.last_enc > DATE_SUB('{ACTIVITY_CUTOFF}', INTERVAL 12 MONTH) THEN 1
                    ELSE 0
                END                             AS active_flag_12m,
                CASE
                    WHEN p.DECEASEDDATE IS NOT NULL THEN 0
                    WHEN le.last_enc > DATE_SUB('{ACTIVITY_CUTOFF}', INTERVAL 18 MONTH) THEN 1
                    ELSE 0
                END                             AS active_flag_18m,
                CASE
                    WHEN p.DECEASEDDATE IS NOT NULL THEN 0
                    WHEN le.last_enc > DATE_SUB('{ACTIVITY_CUTOFF}', INTERVAL 24 MONTH) THEN 1
                    ELSE 0
                END                             AS active_flag_24m,
                ROW_NUMBER() OVER (
                    PARTITION BY p.ENTERPRISEID
                    ORDER BY le.last_enc DESC
                )                               AS rn
            FROM {SOURCE_SCHEMA}.PATIENT p
            LEFT JOIN {SOURCE_SCHEMA}.ref_race_grouping rc
                ON  LOWER(TRIM(rc.race)) = LOWER(TRIM(p.RACE))
            LEFT JOIN {STAGING_LAST_ENC} le
                ON  le.patient_id = p.ENTERPRISEID
            LEFT JOIN {SOURCE_SCHEMA}.provider pro
                ON  pro.PROVIDERID = p.PRIMARYPROVIDERID
            LEFT JOIN {SOURCE_SCHEMA}.chart ch
                ON  ch.enterpriseid = p.patientid
            LEFT JOIN {STAGING_INS} pi
                ON  pi.PATIENTID = p.PATIENTID
            LEFT JOIN {SOURCE_SCHEMA}.COUNTRY cntry
                ON  cntry.COUNTRYID = pi.INSUREDCOUNTRYID
            WHERE p.nd_active_flag = 'Y'
        """)
        conn.commit()
        base_schema, base_tbl = STAGING_PATIENT.split(".", 1)
        for col in ("patientid", "rn"):
            if not _index_exists(cur, base_schema, base_tbl, col):
                cur.execute(f"ALTER TABLE {STAGING_PATIENT} ADD INDEX idx_{col} ({col})")
                conn.commit()
                log(f"    index added: {col}")
        cur.execute(f"SELECT COUNT(*) FROM {STAGING_PATIENT}")
        log(f"    {cur.fetchone()[0]:,} rows (all rn values)")
    else:
        cur.execute(f"SELECT COUNT(*) FROM {STAGING_PATIENT}")
        log(f"    already exists, reusing  ({cur.fetchone()[0]:,} rows)")


# -- Setup -----------------------------------------------------------------

def setup_tables():
    conn = get_connection()
    cur  = conn.cursor()

    # 1. Pre-materialize subqueries
    _materialize_staging_ins(cur, conn)
    _materialize_staging_last_enc(cur, conn)
    _materialize_staging_patient(cur, conn)

    # 2. Ensure indexes on source join columns
    log(f"  Checking indexes on source join columns...")
    for tbl, col in [
        ("PATIENT",   "ENTERPRISEID"),
        ("PATIENT",   "nd_active_flag"),
        ("PATIENT",   "PRIMARYPROVIDERID"),
        ("PATIENT",   "patientid"),
        ("provider",  "PROVIDERID"),
        ("chart",     "enterpriseid"),
        ("COUNTRY",   "COUNTRYID"),
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
            registrationdate  DATE          DEFAULT NULL,
            patientid         BIGINT        DEFAULT NULL,
            ndid              BIGINT        DEFAULT NULL,
            gender            VARCHAR(20)   DEFAULT NULL,
            ufname            VARCHAR(200)  DEFAULT NULL,
            ulname            VARCHAR(200)  DEFAULT NULL,
            dob               DATE          DEFAULT NULL,
            race              VARCHAR(200)  DEFAULT NULL,
            race_grouped      VARCHAR(200)  DEFAULT NULL,
            uemail            VARCHAR(500)  DEFAULT NULL,
            mobile            VARCHAR(50)   DEFAULT NULL,
            upaddress         VARCHAR(500)  DEFAULT NULL,
            upaddress2        VARCHAR(500)  DEFAULT NULL,
            upcity            VARCHAR(200)  DEFAULT NULL,
            upstate           VARCHAR(100)  DEFAULT NULL,
            upPhone           VARCHAR(50)   DEFAULT NULL,
            zipcode           VARCHAR(20)   DEFAULT NULL,
            country_name      VARCHAR(200)  DEFAULT NULL,
            patientstatus     VARCHAR(20)   DEFAULT NULL,
            primaryprovider   VARCHAR(500)  DEFAULT NULL,
            deceased_status   TINYINT(1)    DEFAULT NULL,
            deceasedDate      DATE          DEFAULT NULL,
            active_flag_6m    TINYINT(1)    DEFAULT NULL,
            active_flag_12m   TINYINT(1)    DEFAULT NULL,
            active_flag_18m   TINYINT(1)    DEFAULT NULL,
            active_flag_24m   TINYINT(1)    DEFAULT NULL
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

    # 5. PK staging -- distinct patientid where rn=1
    log(f"  Creating PK staging table {STAGING_PK}...")
    if not _table_exists(cur, STAGING_PK):
        cur.execute(f"""
            CREATE TABLE {STAGING_PK} AS
            SELECT DISTINCT {BATCH_KEY}
            FROM {STAGING_PATIENT}
            WHERE rn = 1
              AND {BATCH_KEY} IS NOT NULL
        """)
        cur.execute(f"ALTER TABLE {STAGING_PK} ADD INDEX idx_pk ({BATCH_KEY})")
        conn.commit()
        cur.execute(f"SELECT COUNT(*) FROM {STAGING_PK}")
        n = cur.fetchone()[0]
        log(f"    {n:,} distinct {BATCH_KEY} values (rn=1 patients)")
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
    log(f"  AthenaOne Dashboard: patient_information ETL -- {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log(f"  source schema    : {SOURCE_SCHEMA}")
    log(f"  dest schema      : {DEST_SCHEMA}")
    log(f"  dest table       : {DEST_TABLE}")
    log(f"  activity cutoff  : {ACTIVITY_CUTOFF}")
    log(f"  staging ins      : {STAGING_INS}")
    log(f"  staging last_enc : {STAGING_LAST_ENC}")
    log(f"  staging patient  : {STAGING_PATIENT}")
    log(f"  staging pk       : {STAGING_PK}")
    log(f"  checkpoint       : {CHECKPOINT_TABLE}")
    log(f"  batch size       : {BATCH_SIZE:,}")
    log(f"  log file         : {_log_file}")
    log(f"{'='*70}\n")

    ranges = setup_tables()

    if not ranges:
        log("  No eligible rows found. Exiting.")
        return

    result = None
    with tqdm(total=len(ranges), desc="patient_information", unit="batch") as pbar:
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
                f"SELECT COUNT(*), COUNT(DISTINCT patientid) FROM {DEST_TABLE}"
            )
            row = cur.fetchone()
            log(f"\n  {DEST_TABLE}")
            log(f"    rows                 : {row[0]:,}")
            log(f"    distinct patientid   : {row[1]:,}")
    finally:
        cur.close()
        conn.close()

    log(f"\n  Cleanup SQL (run after verifying data):")
    log(f"    DROP TABLE IF EXISTS {STAGING_INS};")
    log(f"    DROP TABLE IF EXISTS {STAGING_LAST_ENC};")
    log(f"    DROP TABLE IF EXISTS {STAGING_PATIENT};")
    log(f"    DROP TABLE IF EXISTS {STAGING_PK};")
    log(f"    DROP TABLE IF EXISTS {CHECKPOINT_TABLE};")

    if result["status"].startswith("FAILED"):
        sys.exit(1)


if __name__ == "__main__":
    main()
