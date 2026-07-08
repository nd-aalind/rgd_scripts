#!/usr/bin/env python3
"""
Optimized ETL loader for: udm_staging.appointment (AthenaOne)

Source: {SOURCE_SCHEMA}.APPOINTMENT  (single table, batched by APPOINTMENT_ID)

Pre-materialized lookup tables (computed ONCE, reused across all batches):
  - staging.ao_apt_patient_v1_{SOURCE_SCHEMA}  (active patients — INNER JOIN filter)
  - staging.ao_apt_ce_v1_{SOURCE_SCHEMA}       (CLINICALENCOUNTER, active)
  - staging.ao_apt_type_v1_{SOURCE_SCHEMA}     (APPOINTMENTTYPE, active)
  - staging.ao_apt_acr_v1_{SOURCE_SCHEMA}      (APPOINTMENTCANCELREASON, active)
  - staging.ao_apt_aei_v1_{SOURCE_SCHEMA}      (APPOINTMENTELIGIBILITYINFO, active)
  - staging.ao_apt_ral_v1_{SOURCE_SCHEMA}      (REFERRALAPPOINTMENTLINK, active, not deleted)
  - staging.ao_apt_ra_v1_{SOURCE_SCHEMA}       (REFERRALAUTHORIZATION, not deleted)

Optimizations applied:
- Lookup tables pre-materialized once (not re-scanned per batch)
- PK staging table pre-filters eligible active appointments
- Batch by actual primary key values (not arithmetic ranges — IDs can be sparse)
- Server-side boundary sampling (avoids loading millions of PKs into memory)
- Commit after every batch (frees undo/log space)
- Checkpoint/resume — re-run skips if already completed
- Disabled InnoDB checks per-session for bulk insert speed
- Progress bar via tqdm

Usage:
    python apt_ao_opt.py
"""

import sys
import time
from datetime import datetime
import pymysql
from tqdm import tqdm
import os
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())

# ── Configuration ────────────────────────────────────────────────────
DB_CONFIG = {
    "host":            os.environ.get("DB_HOST"),
    "port":            3306,
    "user":            os.environ.get("DB_USER"),
    "password":        os.environ.get("DB_PASSWORD"),
    "database":        'udm_staging',
    "charset":         "utf8mb4",
    "connect_timeout": 30,
    "read_timeout":    21600,
    "write_timeout":   21600,
}

BATCH_SIZE = 50_000

# ── Change these two variables to run for a different schema/psid ────
SOURCE_SCHEMA = "raleigh"   # e.g. "tncpa", "raleigh", ...
PSID          = 5

DEST_TABLE       = "udm_staging.appointment_fn_test"
STAGING_TABLE    = f"staging.tmp_ao_apt_staging_v3_{SOURCE_SCHEMA}"
CHECKPOINT_TABLE = f"staging.etl_checkpoint_ao_apt_v3_{SOURCE_SCHEMA}"
CHECKPOINT_KEY   = f"appointment.insert.{SOURCE_SCHEMA}"

BATCH_KEY = "APPOINTMENT_ID"

# ── Pre-materialized lookup staging tables ───────────────────────────
STAGING_PATIENT = f"staging.ao_apt_patient_v3_{SOURCE_SCHEMA}"
STAGING_CE      = f"staging.ao_apt_ce_v3_{SOURCE_SCHEMA}"
STAGING_APT     = f"staging.ao_apt_type_v3_{SOURCE_SCHEMA}"
STAGING_ACR     = f"staging.ao_apt_acr_v3_{SOURCE_SCHEMA}"
STAGING_AEI     = f"staging.ao_apt_aei_v3_{SOURCE_SCHEMA}"
STAGING_RAL     = f"staging.ao_apt_ral_v3_{SOURCE_SCHEMA}"
STAGING_RA      = f"staging.ao_apt_ra_v3_{SOURCE_SCHEMA}"


# ── Date helper — handles VARCHAR date columns stored in mixed formats ─

def date_case(col):
    return (
        f"CASE"
        f"  WHEN {col} IS NULL OR {col} IN ('', 'None') THEN NULL"
        f"  WHEN {col} REGEXP '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}} [0-9]{{2}}:[0-9]{{2}}:[0-9]{{2}}$'"
        f"      THEN DATE({col})"
        f"  WHEN {col} REGEXP '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}$'"
        f"      THEN STR_TO_DATE({col}, '%Y-%m-%d')"
        f"  WHEN {col} REGEXP '^[0-9]{{2}}/[0-9]{{2}}/[0-9]{{4}}$'"
        f"      THEN STR_TO_DATE({col}, '%m/%d/%Y')"
        f"  WHEN {col} REGEXP '^[0-9]{{2}}-[0-9]{{2}}-[0-9]{{4}}$'"
        f"      THEN STR_TO_DATE({col}, '%m-%d-%Y')"
        f"  ELSE NULL"
        f" END"
    )


# ── Batch INSERT builder ──────────────────────────────────────────────

def build_batch_insert(pk_lo, pk_hi):
    a   = "a"
    p   = "p"
    ce  = "ce"
    apt = "apt"
    acr = "acr"
    aei = "aei"
    ral = "ral"
    ra  = "ra"
    return f"""
INSERT INTO {DEST_TABLE}
    (appointment_id, ndid, encounter_id, encounter_date,
     appointment_created_date, appointment_date, appointment_start_time,
     appointment_duration, appointment_status, appointment_type,
     appointment_name, provider_id, provider_name, doc_speciality,
     department_id, appointment_notes, cancellation_flag, cancellation_reason,
     no_show_flag, reschedule_flag, rescheduled_appt_id,
     pat_insurance, pat_insurance_type,
     referral_flag, referral_id, appointment_prior_auth_id, insurance_id,
     copay_amount, copay_collected, telehealth_flag,
     created_datetime, created_by, updated_time, updated_by,
     ehr_source_name, source_path, data_type, psid)
SELECT
    CAST({a}.APPOINTMENT_ID AS SIGNED),
    CAST({a}.PATIENT_ID AS SIGNED),
    CAST({ce}.CLINICALENCOUNTERID AS SIGNED),
    {date_case(f'{ce}.ENCOUNTERDATE')},
    {a}.SCHEDULED_TIMESTAMP,
    {date_case(f'{a}.APPOINTMENT_DATE')},
    {a}.APPOINTMENT_START_TIME,
    {a}.BOOKED_APPOINTMENT_DURATION,
    CASE
        WHEN {a}.STATUS_TYPE = 'f' THEN 'Scheduled'
        WHEN {a}.STATUS_TYPE = '2' THEN 'Checked In'
        WHEN {a}.STATUS_TYPE IN ('3', '4') THEN 'Completed'
        WHEN {a}.STATUS_TYPE = 'x' THEN 'Cancelled'
        WHEN {a}.STATUS_TYPE = 'o' THEN 'Open'
        ELSE NULL
    END,
    COALESCE({apt}.APPOINTMENTTYPECLASS, {ce}.CLINICALENCOUNTERTYPE),
    COALESCE({a}.BOOKED_APPOINTMENT_NAME, {apt}.APPOINTMENTTYPENAME),
    CAST({a}.RENDERING_PROVIDER_ID AS SIGNED),
    NULL,
    {a}.RENDERING_PROVIDER_SPECIALTY_TYPE,
    CAST({a}.DEPARTMENT_ID AS SIGNED),
    {a}.APPOINTMENT_NOTE,
    CASE WHEN {a}.STATUS_TYPE = 'x' THEN 1 ELSE 0 END,
    COALESCE({a}.CANCELLED_REASON_LOCAL_NAME, {acr}.NAME),
    CASE
        WHEN {a}.FULL_NO_SHOW_COUNT_INDICATOR = 1
          OR {a}.NON_RESCHED_NO_SHOW_COUNT_INDICATOR = 1 THEN 1
        WHEN {acr}.NOSHOWYN = 'Y' THEN 1
        ELSE 0
    END,
    CASE
        WHEN {a}.RESCHEDULED_COUNT_INDICATOR = 1 THEN 1
        WHEN {acr}.PATIENTRESCHEDULEDYN = 'Y' THEN 1
        ELSE 0
    END,
    CAST({a}.RESCHEDULED_APPOINTMENT_ID AS SIGNED),
    {a}.patient_insurance_category_type,
    {a}.patient_insurance_grouping_type,
    CASE WHEN {ral}.REFERRALID IS NOT NULL THEN 1 ELSE 0 END,
    CAST({ral}.REFERRALID AS SIGNED),
    {ra}.REFERRALAUTHNUMBER,
    CAST({ra}.PATIENTINSURANCEID AS SIGNED),
    {aei}.COPAYAMOUNT,
    {aei}.COPAYAMOUNTCOLLECTED,
    {a}.TELEHEALTH_APPOINTMENT_INDICATOR,
    CURRENT_TIMESTAMP(),
    'ND',
    CURRENT_TIMESTAMP(),
    'ND',
    'AthenaOne',
    'bronze_layer',
    'Structured',
    {PSID}
FROM {SOURCE_SCHEMA}.APPOINTMENT {a}
INNER JOIN {STAGING_PATIENT} {p}   ON {p}.ENTERPRISEID        = {a}.PATIENT_ID
LEFT  JOIN {STAGING_CE}      {ce}  ON {ce}.APPOINTMENTID      = {a}.APPOINTMENT_ID
LEFT  JOIN {STAGING_APT}     {apt} ON {apt}.APPOINTMENTTYPEID = {a}.BOOKED_APPOINTMENT_TYPE_ID
LEFT  JOIN {STAGING_ACR}     {acr} ON {acr}.NAME              = {a}.CANCELLED_REASON_LOCAL_NAME
LEFT  JOIN {STAGING_AEI}     {aei} ON {aei}.APPOINTMENTID     = {a}.APPOINTMENT_ID
LEFT  JOIN {STAGING_RAL}     {ral} ON {ral}.APPOINTMENTID     = {a}.APPOINTMENT_ID
LEFT  JOIN {STAGING_RA}      {ra}  ON {ra}.REFERRALAUTHID     = {ral}.REFERRALID
WHERE {a}.nd_active_flag = 'Y'
  AND {a}.{BATCH_KEY} >= {pk_lo} AND {a}.{BATCH_KEY} < {pk_hi}
"""


# ── Helpers ──────────────────────────────────────────────────────────

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


# ── Checkpoint ───────────────────────────────────────────────────────

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


# ── Setup ────────────────────────────────────────────────────────────

def setup_tables():
    """Create lookup + PK staging + dest + checkpoint tables. Return batch ranges."""
    conn = get_connection()
    cur  = conn.cursor()

    # ── 1a. Active patient IDs via ENTERPRISEID (INNER JOIN filter) ──
    print("  Materializing patient lookup (nd_active_flag='Y')...")
    if not _table_exists(cur, STAGING_PATIENT):
        cur.execute(f"""
            CREATE TABLE {STAGING_PATIENT} AS
            SELECT ENTERPRISEID
            FROM {SOURCE_SCHEMA}.PATIENT
            WHERE nd_active_flag = 'Y'
        """)
        cur.execute(f"ALTER TABLE {STAGING_PATIENT} ADD INDEX idx_pt (ENTERPRISEID)")
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")
    cur.execute(f"SELECT COUNT(*) FROM {STAGING_PATIENT}")
    print(f"    {cur.fetchone()[0]:,} patient rows")

    # ── 1b. CLINICALENCOUNTER (active) ───────────────────────────────
    print("  Materializing CLINICALENCOUNTER lookup (nd_active_flag='Y')...")
    if not _table_exists(cur, STAGING_CE):
        cur.execute(f"""
            CREATE TABLE {STAGING_CE} AS
            SELECT APPOINTMENTID, CLINICALENCOUNTERID, ENCOUNTERDATE, CLINICALENCOUNTERTYPE
            FROM {SOURCE_SCHEMA}.CLINICALENCOUNTER
            WHERE nd_active_flag = 'Y'
        """)
        cur.execute(f"ALTER TABLE {STAGING_CE} ADD INDEX idx_ce (APPOINTMENTID)")
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")
    cur.execute(f"SELECT COUNT(*) FROM {STAGING_CE}")
    print(f"    {cur.fetchone()[0]:,} CLINICALENCOUNTER rows")

    # ── 1c. APPOINTMENTTYPE ──────────────────────────────────────────
    print("  Materializing APPOINTMENTTYPE lookup...")
    if not _table_exists(cur, STAGING_APT):
        cur.execute(f"""
            CREATE TABLE {STAGING_APT} AS
            SELECT APPOINTMENTTYPEID, APPOINTMENTTYPECLASS, APPOINTMENTTYPENAME
            FROM {SOURCE_SCHEMA}.APPOINTMENTTYPE
        """)
        cur.execute(f"ALTER TABLE {STAGING_APT} ADD INDEX idx_apt (APPOINTMENTTYPEID)")
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")
    cur.execute(f"SELECT COUNT(*) FROM {STAGING_APT}")
    print(f"    {cur.fetchone()[0]:,} APPOINTMENTTYPE rows")

    # ── 1d. APPOINTMENTCANCELREASON ──────────────────────────────────
    print("  Materializing APPOINTMENTCANCELREASON lookup...")
    if not _table_exists(cur, STAGING_ACR):
        cur.execute(f"""
            CREATE TABLE {STAGING_ACR} AS
            SELECT NAME, NOSHOWYN, PATIENTRESCHEDULEDYN
            FROM {SOURCE_SCHEMA}.APPOINTMENTCANCELREASON
        """)
        cur.execute(f"ALTER TABLE {STAGING_ACR} ADD INDEX idx_acr (NAME(100))")
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")
    cur.execute(f"SELECT COUNT(*) FROM {STAGING_ACR}")
    print(f"    {cur.fetchone()[0]:,} APPOINTMENTCANCELREASON rows")

    # ── 1e. APPOINTMENTELIGIBILITYINFO (active) ──────────────────────
    print("  Materializing APPOINTMENTELIGIBILITYINFO lookup (nd_active_flag='Y')...")
    if not _table_exists(cur, STAGING_AEI):
        cur.execute(f"""
            CREATE TABLE {STAGING_AEI} AS
            SELECT APPOINTMENTID, COPAYAMOUNT, COPAYAMOUNTCOLLECTED
            FROM {SOURCE_SCHEMA}.APPOINTMENTELIGIBILITYINFO
            WHERE nd_active_flag = 'Y'
        """)
        cur.execute(f"ALTER TABLE {STAGING_AEI} ADD INDEX idx_aei (APPOINTMENTID)")
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")
    cur.execute(f"SELECT COUNT(*) FROM {STAGING_AEI}")
    print(f"    {cur.fetchone()[0]:,} APPOINTMENTELIGIBILITYINFO rows")

    # ── 1f. REFERRALAPPOINTMENTLINK (not deleted) ────────────────────
    print("  Materializing REFERRALAPPOINTMENTLINK lookup...")
    if not _table_exists(cur, STAGING_RAL):
        cur.execute(f"""
            CREATE TABLE {STAGING_RAL} AS
            SELECT APPOINTMENTID, REFERRALID
            FROM {SOURCE_SCHEMA}.REFERRALAPPOINTMENTLINK
            WHERE DELETEDDATETIME IS NULL
        """)
        cur.execute(f"ALTER TABLE {STAGING_RAL} ADD INDEX idx_ral (APPOINTMENTID)")
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")
    cur.execute(f"SELECT COUNT(*) FROM {STAGING_RAL}")
    print(f"    {cur.fetchone()[0]:,} REFERRALAPPOINTMENTLINK rows")

    # ── 1g. REFERRALAUTHORIZATION (not deleted) ──────────────────────
    print("  Materializing REFERRALAUTHORIZATION lookup...")
    if not _table_exists(cur, STAGING_RA):
        cur.execute(f"""
            CREATE TABLE {STAGING_RA} AS
            SELECT REFERRALAUTHID, REFERRALAUTHNUMBER, PATIENTINSURANCEID
            FROM {SOURCE_SCHEMA}.REFERRALAUTHORIZATION
            WHERE DELETEDDATETIME IS NULL
        """)
        cur.execute(f"ALTER TABLE {STAGING_RA} ADD INDEX idx_ra (REFERRALAUTHID)")
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")
    cur.execute(f"SELECT COUNT(*) FROM {STAGING_RA}")
    print(f"    {cur.fetchone()[0]:,} REFERRALAUTHORIZATION rows")

    # ── 2. PK staging table ──────────────────────────────────────────
    print("  Creating PK staging table...")
    if not _table_exists(cur, STAGING_TABLE):
        cur.execute(f"""
            CREATE TABLE {STAGING_TABLE} AS
            SELECT CAST({BATCH_KEY} AS SIGNED) AS {BATCH_KEY}
            FROM {SOURCE_SCHEMA}.APPOINTMENT
            WHERE {BATCH_KEY} IS NOT NULL
              AND nd_active_flag = 'Y'
        """)
        cur.execute(f"ALTER TABLE {STAGING_TABLE} ADD INDEX idx_pk ({BATCH_KEY})")
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")

    cur.execute(f"SELECT COUNT(*) FROM {STAGING_TABLE}")
    total = cur.fetchone()[0]
    print(f"    {total:,} rows to insert")

    # ── 3. Destination table ─────────────────────────────────────────
    print("  Creating destination table...")
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {DEST_TABLE} (
            appointment_id           BIGINT         DEFAULT NULL,
            ndid                     BIGINT         DEFAULT NULL,
            encounter_id             BIGINT         DEFAULT NULL,
            encounter_date           DATE           DEFAULT NULL,
            appointment_created_date DATETIME       DEFAULT NULL,
            appointment_date         DATE           DEFAULT NULL,
            appointment_start_time   VARCHAR(20)    DEFAULT NULL,
            appointment_duration     INT            DEFAULT NULL,
            appointment_status       VARCHAR(50)    DEFAULT NULL,
            appointment_type         VARCHAR(200)   DEFAULT NULL,
            appointment_name         VARCHAR(200)   DEFAULT NULL,
            provider_id              BIGINT         DEFAULT NULL,
            provider_name            VARCHAR(200)   DEFAULT NULL,
            doc_speciality           VARCHAR(200)   DEFAULT NULL,
            department_id            BIGINT         DEFAULT NULL,
            appointment_notes        TEXT,
            cancellation_flag        TINYINT        DEFAULT NULL,
            cancellation_reason      VARCHAR(500)   DEFAULT NULL,
            no_show_flag             TINYINT        DEFAULT NULL,
            reschedule_flag          TINYINT        DEFAULT NULL,
            rescheduled_appt_id      BIGINT         DEFAULT NULL,
            pat_insurance            VARCHAR(200)   DEFAULT NULL,
            pat_insurance_type       VARCHAR(200)   DEFAULT NULL,
            referral_flag            TINYINT        DEFAULT NULL,
            referral_id              BIGINT         DEFAULT NULL,
            appointment_prior_auth_id VARCHAR(100)  DEFAULT NULL,
            insurance_id             BIGINT         DEFAULT NULL,
            copay_amount             DECIMAL(10,2)  DEFAULT NULL,
            copay_collected          DECIMAL(10,2)  DEFAULT NULL,
            telehealth_flag          TINYINT        DEFAULT NULL,
            created_datetime         DATETIME       DEFAULT NULL,
            created_by               VARCHAR(20)    DEFAULT NULL,
            updated_time             DATETIME       DEFAULT NULL,
            updated_by               VARCHAR(20)    DEFAULT NULL,
            ehr_source_name          VARCHAR(50)    DEFAULT NULL,
            source_path              VARCHAR(50)    DEFAULT NULL,
            data_type                VARCHAR(50)    DEFAULT NULL,
            psid                     INT            DEFAULT NULL
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
    """)
    conn.commit()
    print("    ready")

    # ── 4. Checkpoint table ──────────────────────────────────────────
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

    # ── 5. Batch boundary sampling ───────────────────────────────────
    print("  Computing batch boundaries...")
    sys.stdout.flush()

    if total == 0:
        cur.close()
        conn.close()
        return []

    cur.execute(f"""
        SELECT {BATCH_KEY}
        FROM (
            SELECT {BATCH_KEY},
                   ROW_NUMBER() OVER (ORDER BY {BATCH_KEY}) AS rn
            FROM {STAGING_TABLE}
        ) t
        WHERE (rn - 1) % {BATCH_SIZE} = 0
        ORDER BY {BATCH_KEY}
    """)
    boundaries = [row[0] for row in cur.fetchall() if row[0] is not None]

    cur.execute(f"SELECT MAX({BATCH_KEY}) FROM {STAGING_TABLE}")
    max_pk = int(cur.fetchone()[0])

    cur.close()
    conn.close()

    ranges = []
    for i, lo in enumerate(boundaries):
        hi = boundaries[i + 1] if i + 1 < len(boundaries) else max_pk + 1
        ranges.append((lo, hi))

    print(f"    {len(ranges)} batches of ~{BATCH_SIZE:,} rows each")
    return ranges


# ── Runner ───────────────────────────────────────────────────────────

def run_insert(ranges, pbar):
    conn = get_connection()

    if is_done(conn):
        conn.close()
        pbar.update(len(ranges))
        return {"status": "skipped", "rows": 0, "secs": 0}

    mark(conn, "running")
    t0 = time.time()
    total_rows = 0

    try:
        cur = conn.cursor()
        cur.execute("SET SESSION sql_mode = ''")
        cur.execute("SET unique_checks = 0")
        cur.execute("SET foreign_key_checks = 0")

        for pk_lo, pk_hi in ranges:
            sql = build_batch_insert(pk_lo, pk_hi)
            cur.execute(sql)
            conn.commit()
            total_rows += cur.rowcount
            pbar.update(1)

        cur.execute("SET SESSION sql_mode = DEFAULT")
        cur.execute("SET unique_checks = 1")
        cur.execute("SET foreign_key_checks = 1")
        cur.close()

        elapsed = round(time.time() - t0, 1)
        mark(conn, "done", total_rows)
        conn.close()
        return {"status": "done", "rows": total_rows, "secs": elapsed}

    except Exception as exc:
        elapsed = round(time.time() - t0, 1)
        mark(conn, "failed", total_rows, str(exc))
        try:
            conn.close()
        except Exception:
            pass
        return {"status": f"FAILED: {exc}", "rows": total_rows, "secs": elapsed}


# ── Main ─────────────────────────────────────────────────────────────

def main():
    print(f"\n{'='*70}", flush=True)
    print(f"  AthenaOne Appointment ETL — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  source     : {SOURCE_SCHEMA}.APPOINTMENT  (psid={PSID})")
    print(f"  dest       : {DEST_TABLE}")
    print(f"  staging    : {STAGING_TABLE}")
    print(f"  checkpoint : {CHECKPOINT_TABLE}")
    print(f"  batch_size : {BATCH_SIZE:,}")
    print(f"{'='*70}\n", flush=True)

    print("  Connecting to database...")
    sys.stdout.flush()
    ranges = setup_tables()

    if not ranges:
        print(f"\nNo eligible rows in {SOURCE_SCHEMA}.APPOINTMENT. Exiting.")
        return

    result = None
    with tqdm(total=len(ranges), desc="Overall", unit="batch") as pbar:
        result = run_insert(ranges, pbar)

    print()
    if result["status"] == "done":
        tag = " DONE"
    elif result["status"] == "skipped":
        tag = " SKIP"
    else:
        tag = " FAIL"
    print(f"  [{tag}] {SOURCE_SCHEMA}.APPOINTMENT  "
          f"{result['rows']:>10,} rows inserted  ({result['secs']}s)")

    print(f"\n{'='*70}")
    if result["status"].startswith("FAILED"):
        print(f"  FAILED: {result['status']}")
    else:
        print(f"  Status: {result['status']}  |  Total rows inserted: {result['rows']:,}")
    print(f"{'='*70}")

    print(f"\n  Cleanup SQL:")
    print(f"    DROP TABLE IF EXISTS {STAGING_PATIENT};")
    print(f"    DROP TABLE IF EXISTS {STAGING_CE};")
    print(f"    DROP TABLE IF EXISTS {STAGING_APT};")
    print(f"    DROP TABLE IF EXISTS {STAGING_ACR};")
    print(f"    DROP TABLE IF EXISTS {STAGING_AEI};")
    print(f"    DROP TABLE IF EXISTS {STAGING_RAL};")
    print(f"    DROP TABLE IF EXISTS {STAGING_RA};")
    print(f"    DROP TABLE IF EXISTS {STAGING_TABLE};")
    print(f"    DROP TABLE IF EXISTS {CHECKPOINT_TABLE};")

    if result["status"].startswith("FAILED"):
        sys.exit(1)


if __name__ == "__main__":
    main()
