#!/usr/bin/env python3
"""
Optimized ETL loader for: udm_staging.appointment (eCW)

Source: {SOURCE_SCHEMA}.enc  (single table, batched by encounterID)

Pre-materialized lookup tables (computed ONCE, reused across all batches):
  - staging.ecw_apt_ed_v1_{SOURCE_SCHEMA}   (encounterdata,   nd_activeflag='Y')
  - staging.ecw_apt_vc_v1_{SOURCE_SCHEMA}   (visitcodes,      nd_activeflag='Y')
  - staging.ecw_apt_rd_v1_{SOURCE_SCHEMA}   (referraldetail,  nd_activeflag='Y')
  - staging.ecw_apt_r_v1_{SOURCE_SCHEMA}    (referral,        deleteFlag=0, nd_activeflag='Y')
  - staging.ecw_apt_d_v1_{SOURCE_SCHEMA}    (doctors,         nd_activeflag='Y')

New columns vs previous version: doc_speciality, pat_insurance, pat_insurance_type,
  updated_time, updated_by.

NOTE: If destination table already exists, add the missing columns manually:
  ALTER TABLE udm_staging.appointment_ecw_fn
    ADD COLUMN doc_speciality    VARCHAR(200) DEFAULT NULL AFTER provider_name,
    ADD COLUMN pat_insurance      VARCHAR(200) DEFAULT NULL AFTER confirmation_status,
    ADD COLUMN pat_insurance_type VARCHAR(200) DEFAULT NULL AFTER pat_insurance,
    ADD COLUMN updated_time       DATETIME     DEFAULT NULL AFTER created_by,
    ADD COLUMN updated_by         VARCHAR(20)  DEFAULT NULL AFTER updated_time;

Optimizations applied:
- Lookup tables pre-materialized once (not re-scanned per batch)
- PK staging table pre-filters eligible rows
- Batch by actual primary key values (not arithmetic ranges — IDs can be sparse)
- Server-side boundary sampling (avoids loading millions of PKs into memory)
- Commit after every batch (frees undo/log space)
- Checkpoint/resume — re-run skips if already completed
- Disabled InnoDB checks per-session for bulk insert speed
- Progress bar via tqdm

Usage:
    python apt_ecw_opt.py
"""

import sys
import time
from datetime import datetime
import pymysql
from tqdm import tqdm

# ── Configuration ────────────────────────────────────────────────────
DB_CONFIG = {
    "host":            "ndai-dev-rds-instance.cwp60ymu4ko0.us-east-1.rds.amazonaws.com",
    "port":            3306,
    "user":            "Aalind",
    "password":        "A@L1nd@123",
    "database":        'udm_staging',
    "charset":         "utf8mb4",
    "connect_timeout": 30,
    "read_timeout":    21600,
    "write_timeout":   21600,
}

BATCH_SIZE = 50_000

# ── Change these two variables to run for a different schema/psid ────
SOURCE_SCHEMA = "fcn_latest"   # e.g. "kinsula_leq", "arizona", ...
PSID          = 8

DEST_TABLE       = "udm_staging.appointment_fn"
STAGING_TABLE    = f"staging.tmp_ecw_apt_staging_v2_{SOURCE_SCHEMA}"
CHECKPOINT_TABLE = f"staging.etl_checkpoint_ecw_apt_v13_{SOURCE_SCHEMA}"
CHECKPOINT_KEY   = f"appointment.insert.{SOURCE_SCHEMA}"

BATCH_KEY = "encounterID"

# ── Pre-materialized lookup staging tables ───────────────────────────
STAGING_ED = f"staging.ecw_apt_ed_v2_{SOURCE_SCHEMA}"
STAGING_VC = f"staging.ecw_apt_vc_v2_{SOURCE_SCHEMA}"
STAGING_RD = f"staging.ecw_apt_rd_v2_{SOURCE_SCHEMA}"
STAGING_R  = f"staging.ecw_apt_r_v2_{SOURCE_SCHEMA}"
STAGING_D  = f"staging.ecw_apt_d_v2_{SOURCE_SCHEMA}"


# ── Batch INSERT builder ──────────────────────────────────────────────

def build_batch_insert(pk_lo, pk_hi):
    e  = "e"
    ed = "ed"
    vc = "vc"
    d  = "d"
    rd = "rd"
    r  = "r"
    return f"""
INSERT INTO {DEST_TABLE}
    (appointment_id, ndid, encounter_id, encounter_date,
     appointment_created_date, appointment_date, appointment_start_time,
     appointment_end_time, appointment_duration, appointment_status,
     appointment_type, appointment_name, provider_id, provider_name, doc_speciality,
     department_id, check_in_time, check_out_time, appointment_reason,
     appointment_notes, cancellation_flag, cancellation_reason,
     no_show_flag, reschedule_flag, rescheduled_appt_id, confirmation_status,
     pat_insurance, pat_insurance_type,
     referral_flag, referral_id, appointment_prior_auth_id, insurance_id,
     copay_amount, copay_collected, telehealth_flag,
     created_datetime, created_by, updated_time, updated_by,
     ehr_source_name, source_path, data_type, psid)
SELECT
    CAST({e}.encounterID AS SIGNED),
    CAST({e}.patientID AS SIGNED),
    CAST({e}.encounterID AS SIGNED),
    {e}.date,
    NULL,
    DATE({e}.date),
    TIME({e}.startTime),
    TIME({e}.endTime),
    TIMESTAMPDIFF(MINUTE, {e}.startTime, {e}.endTime),
    {e}.STATUS,
    CASE
        WHEN {e}.encType = 1 THEN 'Office Visit'
        WHEN {e}.encType = 2 THEN 'Tele/Virtual Visit'
        WHEN {e}.encType = 3 THEN 'Out of Office'
        WHEN {e}.encType = 4 THEN 'Claim'
        WHEN {e}.encType = 5 THEN 'Lab'
        WHEN {e}.encType = 6 THEN 'Web Encounter'
        WHEN {e}.encType = 7 THEN 'ePrescription Refills'
        WHEN {e}.encType = 8 THEN 'PTDASH'
        WHEN {e}.encType = 9 THEN 'Orderset'
        ELSE NULL
    END,
    COALESCE({vc}.Description, {e}.visitType),
    CAST({e}.doctorID AS SIGNED),
    NULL,
    {d}.speciality,
    CAST({e}.deptid AS SIGNED),
    {e}.timeIn,
    {e}.timeOut,
    COALESCE({e}.reason, {ed}.chiefComplaint),
    {e}.generalNotes,
    CASE
        WHEN {e}.deleteFlag = 1
          OR UPPER({e}.STATUS) IN ('CANCELLED', 'CANCELED', 'CX', 'CAN')
        THEN 1 ELSE 0
    END,
    {ed}.cancellationReason,
    CASE
        WHEN UPPER({e}.STATUS) IN ('N/S', 'NOS', 'NOSHOW', 'NO SHOW', 'N/S FEE', 'NO-SHOW')
        THEN 1 ELSE 0
    END,
    CASE
        WHEN UPPER({e}.STATUS) IN ('R/S', 'RES', 'RESCHEDULED', 'R/S NWN', 'RESCH')
        THEN 1 ELSE 0
    END,
    NULL,
    NULL,
    NULL,
    NULL,
    CASE WHEN {r}.referralid IS NOT NULL THEN 1 ELSE 0 END,
    CAST({r}.referralid AS SIGNED),
    {r}.authNo,
    CAST({r}.insid AS SIGNED),
    {e}.VisitCopay,
    {ed}.copay,
    CASE WHEN {e}.encType = 2 THEN 1 ELSE 0 END,
    CURRENT_TIMESTAMP(),
    'ND',
    CURRENT_TIMESTAMP(),
    'ND',
    'eCW',
    'bronze_layer',
    'Structured',
    {PSID}
FROM {SOURCE_SCHEMA}.enc {e}
LEFT JOIN {STAGING_ED} {ed} ON {ed}.encounterID = {e}.encounterID
LEFT JOIN {STAGING_VC} {vc} ON {vc}.Name        = {e}.visitType
LEFT JOIN {STAGING_D}  {d}  ON {d}.doctorID     = {e}.doctorID
                             AND {e}.nd_activeflag = 'Y'
LEFT JOIN {STAGING_RD} {rd} ON {rd}.encounterid = {e}.encounterID
LEFT JOIN {STAGING_R}  {r}  ON {r}.referralid   = {rd}.referralid
WHERE {e}.{BATCH_KEY} >= {pk_lo} AND {e}.{BATCH_KEY} < {pk_hi}
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


def _index_exists(cur, schema, table, column):
    cur.execute(
        "SELECT COUNT(*) FROM information_schema.statistics "
        "WHERE table_schema = %s AND table_name = %s AND column_name = %s",
        (schema, table, column),
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

    cur.execute("SET SESSION lock_wait_timeout = 3600")
    cur.execute("SET SESSION innodb_lock_wait_timeout = 3600")

    # ── 1a. encounterdata ────────────────────────────────────────────
    print("  Materializing encounterdata lookup...")
    if not _table_exists(cur, STAGING_ED):
        cur.execute(f"""
            CREATE TABLE {STAGING_ED} AS
            SELECT encounterID, chiefComplaint, cancellationReason, copay
            FROM {SOURCE_SCHEMA}.encounterdata
            WHERE nd_activeflag = 'Y'
        """)
        cur.execute(f"ALTER TABLE {STAGING_ED} ADD INDEX idx_ed (encounterID)")
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")
    cur.execute(f"SELECT COUNT(*) FROM {STAGING_ED}")
    print(f"    {cur.fetchone()[0]:,} encounterdata rows")

    # ── 1b. visitcodes ───────────────────────────────────────────────
    print("  Materializing visitcodes lookup...")
    if not _table_exists(cur, STAGING_VC):
        cur.execute(f"""
            CREATE TABLE {STAGING_VC} AS
            SELECT Name, Description
            FROM {SOURCE_SCHEMA}.visitcodes
            WHERE nd_activeflag = 'Y'
        """)
        cur.execute(f"ALTER TABLE {STAGING_VC} ADD INDEX idx_vc (Name)")
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")
    cur.execute(f"SELECT COUNT(*) FROM {STAGING_VC}")
    print(f"    {cur.fetchone()[0]:,} visitcodes rows")

    # ── 1c. referraldetail ───────────────────────────────────────────
    print("  Materializing referraldetail lookup...")
    if not _table_exists(cur, STAGING_RD):
        cur.execute(f"""
            CREATE TABLE {STAGING_RD} AS
            SELECT encounterid, referralid
            FROM {SOURCE_SCHEMA}.referraldetail
            WHERE nd_activeflag = 'Y'
        """)
        cur.execute(f"ALTER TABLE {STAGING_RD} ADD INDEX idx_rd (encounterid)")
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")
    cur.execute(f"SELECT COUNT(*) FROM {STAGING_RD}")
    print(f"    {cur.fetchone()[0]:,} referraldetail rows")

    # ── 1d. referral (deleteFlag=0) ──────────────────────────────────
    print("  Materializing referral lookup (deleteFlag=0)...")
    if not _table_exists(cur, STAGING_R):
        cur.execute(f"""
            CREATE TABLE {STAGING_R} AS
            SELECT referralid, authNo, insid
            FROM {SOURCE_SCHEMA}.referral
            WHERE deleteFlag = 0
              AND nd_activeflag = 'Y'
        """)
        cur.execute(f"ALTER TABLE {STAGING_R} ADD INDEX idx_r (referralid)")
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")
    cur.execute(f"SELECT COUNT(*) FROM {STAGING_R}")
    print(f"    {cur.fetchone()[0]:,} referral rows")

    # ── 1e. doctors ──────────────────────────────────────────────────
    print("  Materializing doctors lookup...")
    if not _table_exists(cur, STAGING_D):
        cur.execute(f"""
            CREATE TABLE {STAGING_D} AS
            SELECT doctorID, speciality
            FROM {SOURCE_SCHEMA}.doctors
            WHERE nd_activeflag = 'Y'
        """)
        cur.execute(f"ALTER TABLE {STAGING_D} ADD INDEX idx_d (doctorID)")
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")
    cur.execute(f"SELECT COUNT(*) FROM {STAGING_D}")
    print(f"    {cur.fetchone()[0]:,} doctor rows")

    # ── 2. PK staging table ──────────────────────────────────────────
    print("  Creating PK staging table...")
    if not _table_exists(cur, STAGING_TABLE):
        cur.execute(f"""
            CREATE TABLE {STAGING_TABLE} AS
            SELECT CAST({BATCH_KEY} AS SIGNED) AS {BATCH_KEY}
            FROM {SOURCE_SCHEMA}.enc
            WHERE {BATCH_KEY} IS NOT NULL
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
            appointment_id            BIGINT        DEFAULT NULL,
            ndid                      BIGINT        DEFAULT NULL,
            encounter_id              BIGINT        DEFAULT NULL,
            encounter_date            DATE          DEFAULT NULL,
            appointment_created_date  DATETIME      DEFAULT NULL,
            appointment_date          DATE          DEFAULT NULL,
            appointment_start_time    TIME          DEFAULT NULL,
            appointment_end_time      TIME          DEFAULT NULL,
            appointment_duration      INT           DEFAULT NULL,
            appointment_status        VARCHAR(100)  DEFAULT NULL,
            appointment_type          VARCHAR(200)  DEFAULT NULL,
            appointment_name          VARCHAR(200)  DEFAULT NULL,
            provider_id               BIGINT        DEFAULT NULL,
            provider_name             VARCHAR(200)  DEFAULT NULL,
            doc_speciality            VARCHAR(200)  DEFAULT NULL,
            department_id             BIGINT        DEFAULT NULL,
            check_in_time             TIME          DEFAULT NULL,
            check_out_time            TIME          DEFAULT NULL,
            appointment_reason        TEXT,
            appointment_notes         TEXT,
            cancellation_flag         TINYINT       DEFAULT NULL,
            cancellation_reason       VARCHAR(500)  DEFAULT NULL,
            no_show_flag              TINYINT       DEFAULT NULL,
            reschedule_flag           TINYINT       DEFAULT NULL,
            rescheduled_appt_id       BIGINT        DEFAULT NULL,
            confirmation_status       VARCHAR(100)  DEFAULT NULL,
            pat_insurance             VARCHAR(200)  DEFAULT NULL,
            pat_insurance_type        VARCHAR(200)  DEFAULT NULL,
            referral_flag             TINYINT       DEFAULT NULL,
            referral_id               BIGINT        DEFAULT NULL,
            appointment_prior_auth_id VARCHAR(100)  DEFAULT NULL,
            insurance_id              BIGINT        DEFAULT NULL,
            copay_amount              DECIMAL(10,2) DEFAULT NULL,
            copay_collected           DECIMAL(10,2) DEFAULT NULL,
            telehealth_flag           TINYINT       DEFAULT NULL,
            created_datetime          DATETIME      DEFAULT NULL,
            created_by                VARCHAR(20)   DEFAULT NULL,
            updated_time              DATETIME      DEFAULT NULL,
            updated_by                VARCHAR(20)   DEFAULT NULL,
            ehr_source_name           VARCHAR(50)   DEFAULT NULL,
            source_path               VARCHAR(50)   DEFAULT NULL,
            data_type                 VARCHAR(50)   DEFAULT NULL,
            psid                      INT           DEFAULT NULL
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

    # ── 5. Ensure source enc table has required indexes ──────────────
    # encounterID — batch WHERE clause (critical: without this every batch full-scans enc)
    for col in [BATCH_KEY]:
        if not _index_exists(cur, SOURCE_SCHEMA, "enc", col):
            print(f"  Adding index idx_{col} on {SOURCE_SCHEMA}.enc({col}) — may take a few minutes...")
            cur.execute(f"ALTER TABLE {SOURCE_SCHEMA}.enc ADD INDEX idx_{col} ({col})")
            conn.commit()
            print("    done")
        else:
            print(f"  Index on {SOURCE_SCHEMA}.enc({col}) already exists.")

    # ── 6. Batch boundary sampling ───────────────────────────────────
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
    print(f"  eCW Appointment ETL — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  source     : {SOURCE_SCHEMA}.enc  (psid={PSID})")
    print(f"  dest       : {DEST_TABLE}")
    print(f"  staging    : {STAGING_TABLE}")
    print(f"  checkpoint : {CHECKPOINT_TABLE}")
    print(f"  batch_size : {BATCH_SIZE:,}")
    print(f"{'='*70}\n", flush=True)

    print("  Connecting to database...")
    sys.stdout.flush()
    ranges = setup_tables()

    if not ranges:
        print(f"\nNo rows found in {SOURCE_SCHEMA}.enc. Exiting.")
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
    print(f"  [{tag}] {SOURCE_SCHEMA}.enc  "
          f"{result['rows']:>10,} rows inserted  ({result['secs']}s)")

    print(f"\n{'='*70}")
    if result["status"].startswith("FAILED"):
        print(f"  FAILED: {result['status']}")
    else:
        print(f"  Status: {result['status']}  |  Total rows inserted: {result['rows']:,}")
    print(f"{'='*70}")

    print(f"\n  Cleanup SQL:")
    print(f"    DROP TABLE IF EXISTS {STAGING_ED};")
    print(f"    DROP TABLE IF EXISTS {STAGING_VC};")
    print(f"    DROP TABLE IF EXISTS {STAGING_RD};")
    print(f"    DROP TABLE IF EXISTS {STAGING_R};")
    print(f"    DROP TABLE IF EXISTS {STAGING_D};")
    print(f"    DROP TABLE IF EXISTS {STAGING_TABLE};")
    print(f"    DROP TABLE IF EXISTS {CHECKPOINT_TABLE};")

    if result["status"].startswith("FAILED"):
        sys.exit(1)


if __name__ == "__main__":
    main()
