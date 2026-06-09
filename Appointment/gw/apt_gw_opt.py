#!/usr/bin/env python3
"""
Optimized ETL loader for: udm_staging.appointment_fn (Greenway → ECW table structure)

Source: {SOURCE_SCHEMA}.ScheduleAppointment  (single table, batched by ApptID)

Pre-materialized lookup tables (computed ONCE, reused across all batches):
  - staging.gw_apt_pl_v_v1_{SOURCE_SCHEMA}    (PatientList + Visit, active)
  - staging.gw_apt_vt_ecl_v1_{SOURCE_SCHEMA}  (VisitTypes + EncounterClassLookUp, active)
  - staging.gw_apt_plrp_v1_{SOURCE_SCHEMA}    (PatientListResourceProviders, active)
  - staging.gw_apt_sar_v1_{SOURCE_SCHEMA}     (ScheduleAppointmentResources, active)
  - staging.gw_apt_satl_v1_{SOURCE_SCHEMA}    (ScheduleApptTypeList, active)
  - staging.gw_apt_sacl_v1_{SOURCE_SCHEMA}    (ScheduleApptChangeLookup, active)
  - staging.gw_apt_ref_v1_{SOURCE_SCHEMA}     (ScheduleReferralLink + InsurancePreCert, active)

Optimizations applied:
- 10 source JOINs collapsed into 7 pre-materialized staging tables (chains merged)
- Lookup tables pre-materialized once (not re-scanned per batch)
- PK staging table pre-filters eligible rows
- Batch by actual primary key values (not arithmetic ranges — IDs can be sparse)
- Server-side boundary sampling (avoids loading millions of PKs into memory)
- Commit after every batch (frees undo/log space)
- Checkpoint/resume — re-run skips if already completed
- Disabled InnoDB checks per-session for bulk insert speed
- Progress bar via tqdm

Usage:
    python apt_gw_opt.py
"""

import signal
import sys
import threading
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
    "database":        "rgd_udm_silver",
    "charset":         "utf8mb4",
    "connect_timeout": 30,
    "read_timeout":    21600,
    "write_timeout":   21600,
}

BATCH_SIZE = 50_000

# ── Change these two variables to run for a different schema/psid ────
SOURCE_SCHEMA = "mind"   # e.g. "jwm", "greenway", ...
PSID          = 12

DEST_TABLE       = "udm_staging.appointment_fn"
STAGING_TABLE    = f"staging.tmp_gw_apt_staging_v6_{SOURCE_SCHEMA}"
CHECKPOINT_TABLE = f"staging.etl_checkpoint_gw_apt_v6_{SOURCE_SCHEMA}"
CHECKPOINT_KEY   = f"appointment.insert.{SOURCE_SCHEMA}"

BATCH_KEY = "ApptID"

# ── Pre-materialized lookup staging tables ───────────────────────────
STAGING_PL_V   = f"staging.gw_apt_pl_v_v1_{SOURCE_SCHEMA}"    # PatientList + Visit
STAGING_VT_ECL = f"staging.gw_apt_vt_ecl_v1_{SOURCE_SCHEMA}"  # VisitTypes + EncounterClassLookUp
STAGING_PLRP   = f"staging.gw_apt_plrp_v1_{SOURCE_SCHEMA}"    # PatientListResourceProviders
STAGING_SAR    = f"staging.gw_apt_sar_v1_{SOURCE_SCHEMA}"     # ScheduleAppointmentResources
STAGING_SATL   = f"staging.gw_apt_satl_v1_{SOURCE_SCHEMA}"    # ScheduleApptTypeList
STAGING_SACL   = f"staging.gw_apt_sacl_v1_{SOURCE_SCHEMA}"    # ScheduleApptChangeLookup
STAGING_REF    = f"staging.gw_apt_ref_v3_{SOURCE_SCHEMA}"     # ScheduleReferralLink + InsurancePreCert


# ── Batch INSERT builder ──────────────────────────────────────────────

def build_batch_insert(pk_lo, pk_hi):
    sa    = "sa"
    plv   = "plv"
    vtecl = "vtecl"
    plrp  = "plrp"
    sar   = "sar"
    satl  = "satl"
    sacl  = "sacl"
    ref   = "ref"
    return f"""
INSERT INTO {DEST_TABLE}
    (appointment_id, ndid, encounter_id, encounter_date,
     appointment_created_date, appointment_date, appointment_start_time,
     appointment_end_time, appointment_duration, appointment_status,
     appointment_type, appointment_name, provider_id, provider_name,
     doc_speciality, department_id,
     check_in_time, check_out_time, appointment_reason, appointment_notes,
     cancellation_flag, cancellation_reason, no_show_flag, reschedule_flag,
     rescheduled_appt_id, confirmation_status,
     pat_insurance, pat_insurance_type,
     referral_flag, referral_id, appointment_prior_auth_id, insurance_id,
     copay_amount, copay_collected, telehealth_flag,
     created_datetime, created_by, updated_time, updated_by,
     ehr_source_name, source_path, data_type, psid)
SELECT
    CAST({sa}.ApptID AS SIGNED),
    CAST({sa}.PatientID AS SIGNED),
    CAST({plv}.VisitID AS SIGNED),
    DATE({plv}.FromDateTime),
    {sa}.DateMade,
    DATE({sa}.StartDate),
    TIME({sa}.StartDate),
    TIME(COALESCE({sar}.EndTime, {plv}.AppointmentEndTime)),
    COALESCE({sar}.DurationMinutes, TIMESTAMPDIFF(MINUTE, {sa}.StartDate, {sar}.EndTime)),
    CASE
        WHEN {sa}.ChangeId = 2 THEN 'No Show'
        WHEN {sa}.ChangeId IN (1, 3, 8) OR {sa}.Disable = 1 THEN 'Cancelled'
        WHEN {sa}.ChangeId IN (4, 5) THEN 'Rescheduled'
        ELSE NULL
    END,
    COALESCE({vtecl}.EncounterValueName, CAST({vtecl}.EncounterClassID AS CHAR)),
    {satl}.ApptTypeName,
    CAST(COALESCE({plrp}.ProviderID, {plv}.CareProviderID) AS SIGNED),
    NULL,
    NULL,
    NULL,
    TIME({plv}.TimeIn),
    TIME({plv}.TimeOut),
    COALESCE({sa}.PatientComplaint, {plv}.ChiefComplaint, {plv}.PrimaryComplaint),
    {sa}.ApptDescription,
    CASE WHEN {sa}.ChangeId IN (1, 3, 8) OR {sa}.Disable = 1 THEN 1 ELSE 0 END,
    {sacl}.ChangeReason,
    CASE WHEN {sa}.ChangeId = 2 THEN 1 ELSE 0 END,
    CASE WHEN {sa}.ChangeId IN (4, 5) THEN 1 ELSE 0 END,
    NULL,
    NULL,
    NULL,
    NULL,
    CASE WHEN {ref}.InsPreCertId IS NOT NULL AND {ref}.Active = 1 THEN 1 ELSE 0 END,
    NULL,
    {ref}.InsPreCertId,
    CAST({ref}.InsID AS SIGNED),
    {ref}.CoPayAmount,
    NULL,
    COALESCE({sa}.IsTelehealthAppt, {plv}.IsTelehealthAppt),
    CURRENT_TIMESTAMP(),
    'ND',
    CURRENT_TIMESTAMP(),
    'ND',
    'Greenway',
    'bronze_layer',
    'Structured',
    {PSID}
FROM {SOURCE_SCHEMA}.ScheduleAppointment {sa}
LEFT JOIN {STAGING_PL_V}   {plv}   ON {plv}.ApptId         = {sa}.ApptID
LEFT JOIN {STAGING_VT_ECL} {vtecl} ON {vtecl}.VisitTypeID  = {plv}.VisitTypeID
LEFT JOIN {STAGING_PLRP}   {plrp}  ON {plrp}.PatientListID = {plv}.PatientListID
LEFT JOIN {STAGING_SAR}    {sar}   ON {sar}.ApptID          = {sa}.ApptID
LEFT JOIN {STAGING_SATL}   {satl}  ON {satl}.ApptTypeID     = {sa}.ApptTypeID
LEFT JOIN {STAGING_SACL}   {sacl}  ON {sacl}.ChangeID       = {sa}.ChangeId
LEFT JOIN {STAGING_REF}    {ref}   ON {ref}.ApptId          = {sa}.ApptID
WHERE {sa}.{BATCH_KEY} >= {pk_lo} AND {sa}.{BATCH_KEY} < {pk_hi}
"""


# ── Helpers ──────────────────────────────────────────────────────────

# Tracks MySQL connection IDs so Ctrl+C can KILL QUERY on the server side
_open_conn_ids: list[int] = []
_conn_ids_lock = threading.Lock()


def get_connection():
    conn = pymysql.connect(**DB_CONFIG)
    try:
        cur = conn.cursor()
        cur.execute("SELECT CONNECTION_ID()")
        conn_id = cur.fetchone()[0]
        cur.close()
        with _conn_ids_lock:
            _open_conn_ids.append(conn_id)
        conn._nd_conn_id = conn_id
    except Exception:
        pass
    return conn


def _release_conn(conn):
    """Close connection and remove it from the kill registry."""
    try:
        nd_id = getattr(conn, "_nd_conn_id", None)
        if nd_id:
            with _conn_ids_lock:
                try:
                    _open_conn_ids.remove(nd_id)
                except ValueError:
                    pass
        conn.close()
    except Exception:
        pass


def _sigint_handler(sig, frame):
    with _conn_ids_lock:
        ids = list(_open_conn_ids)
    if ids:
        print(f"\n  Interrupted — sending KILL QUERY for {len(ids)} connection(s): {ids}")
        try:
            killer = pymysql.connect(**DB_CONFIG)
            kc = killer.cursor()
            for mid in ids:
                try:
                    kc.execute(f"KILL QUERY {mid}")
                    print(f"    killed {mid}")
                except Exception:
                    pass
            kc.close()
            killer.close()
        except Exception:
            pass
    else:
        print("\n  Interrupted.")
    sys.exit(130)


signal.signal(signal.SIGINT, _sigint_handler)


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

    # ── 1a. PatientList + Visit (active) ─────────────────────────────
    print("  Materializing PatientList + Visit lookup (nd_ActiveFlag='Y')...")
    if not _table_exists(cur, STAGING_PL_V):
        cur.execute(f"""
            CREATE TABLE {STAGING_PL_V} AS
            SELECT
                pl.ApptId,
                pl.PatientListID,
                pl.TimeIn,
                pl.TimeOut,
                pl.ChiefComplaint,
                pl.AppointmentEndTime,
                pl.IsTelehealthAppt,
                v.VisitID,
                v.FromDateTime,
                v.CareProviderID,
                v.PrimaryComplaint,
                v.VisitTypeID
            FROM {SOURCE_SCHEMA}.PatientList pl
            LEFT JOIN {SOURCE_SCHEMA}.Visit v
                ON pl.VisitId = v.VisitID AND v.nd_ActiveFlag = 'Y'
            WHERE pl.nd_ActiveFlag = 'Y'
        """)
        cur.execute(f"ALTER TABLE {STAGING_PL_V} ADD INDEX idx_plv (ApptId)")
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")
    cur.execute(f"SELECT COUNT(*) FROM {STAGING_PL_V}")
    print(f"    {cur.fetchone()[0]:,} PatientList rows")

    # ── 1b. VisitTypes + EncounterClassLookUp (active) ───────────────
    print("  Materializing VisitTypes + EncounterClassLookUp lookup (nd_ActiveFlag='Y')...")
    if not _table_exists(cur, STAGING_VT_ECL):
        cur.execute(f"""
            CREATE TABLE {STAGING_VT_ECL} AS
            SELECT
                vt.VisitTypeID,
                vt.EncounterClassID,
                ecl.EncounterValueName
            FROM {SOURCE_SCHEMA}.VisitTypes vt
            LEFT JOIN {SOURCE_SCHEMA}.EncounterClassLookUp ecl
                ON vt.EncounterClassID = ecl.EncounterClassID AND ecl.nd_ActiveFlag = 'Y'
            WHERE vt.nd_ActiveFlag = 'Y'
        """)
        cur.execute(f"ALTER TABLE {STAGING_VT_ECL} ADD INDEX idx_vtecl (VisitTypeID)")
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")
    cur.execute(f"SELECT COUNT(*) FROM {STAGING_VT_ECL}")
    print(f"    {cur.fetchone()[0]:,} VisitTypes rows")

    # ── 1c. PatientListResourceProviders (active) ────────────────────
    print("  Materializing PatientListResourceProviders lookup (nd_ActiveFlag='Y')...")
    if not _table_exists(cur, STAGING_PLRP):
        cur.execute(f"""
            CREATE TABLE {STAGING_PLRP} AS
            SELECT PatientListID, ProviderID
            FROM {SOURCE_SCHEMA}.PatientListResourceProviders
            WHERE nd_ActiveFlag = 'Y'
        """)
        cur.execute(f"ALTER TABLE {STAGING_PLRP} ADD INDEX idx_plrp (PatientListID)")
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")
    cur.execute(f"SELECT COUNT(*) FROM {STAGING_PLRP}")
    print(f"    {cur.fetchone()[0]:,} PatientListResourceProviders rows")

    # ── 1d. ScheduleAppointmentResources (active) ────────────────────
    print("  Materializing ScheduleAppointmentResources lookup (nd_ActiveFlag='Y')...")
    if not _table_exists(cur, STAGING_SAR):
        cur.execute(f"""
            CREATE TABLE {STAGING_SAR} AS
            SELECT ApptID, EndTime, DurationMinutes
            FROM {SOURCE_SCHEMA}.ScheduleAppointmentResources
            WHERE nd_ActiveFlag = 'Y'
        """)
        cur.execute(f"ALTER TABLE {STAGING_SAR} ADD INDEX idx_sar (ApptID)")
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")
    cur.execute(f"SELECT COUNT(*) FROM {STAGING_SAR}")
    print(f"    {cur.fetchone()[0]:,} ScheduleAppointmentResources rows")

    # ── 1e. ScheduleApptTypeList (active) ────────────────────────────
    print("  Materializing ScheduleApptTypeList lookup (nd_ActiveFlag='Y')...")
    if not _table_exists(cur, STAGING_SATL):
        cur.execute(f"""
            CREATE TABLE {STAGING_SATL} AS
            SELECT ApptTypeID, ApptTypeName
            FROM {SOURCE_SCHEMA}.ScheduleApptTypeList
            WHERE nd_ActiveFlag = 'Y'
        """)
        cur.execute(f"ALTER TABLE {STAGING_SATL} ADD INDEX idx_satl (ApptTypeID)")
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")
    cur.execute(f"SELECT COUNT(*) FROM {STAGING_SATL}")
    print(f"    {cur.fetchone()[0]:,} ScheduleApptTypeList rows")

    # ── 1f. ScheduleApptChangeLookup (active) ────────────────────────
    print("  Materializing ScheduleApptChangeLookup lookup (nd_ActiveFlag='Y')...")
    if not _table_exists(cur, STAGING_SACL):
        cur.execute(f"""
            CREATE TABLE {STAGING_SACL} AS
            SELECT ChangeID, ChangeReason
            FROM {SOURCE_SCHEMA}.ScheduleApptChangeLookup
            WHERE nd_ActiveFlag = 'Y'
        """)
        cur.execute(f"ALTER TABLE {STAGING_SACL} ADD INDEX idx_sacl (ChangeID)")
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")
    cur.execute(f"SELECT COUNT(*) FROM {STAGING_SACL}")
    print(f"    {cur.fetchone()[0]:,} ScheduleApptChangeLookup rows")

    # ── 1g. ScheduleReferralLink + InsurancePreCert (active) ─────────
    # Ensure source table indexes exist before full-table scans
    for tbl, col in [
        ("ScheduleReferralLink", "nd_ActiveFlag"),
        ("ScheduleReferralLink", "InsPreCertId"),
        ("InsurancePreCert",     "nd_ActiveFlag"),
        ("InsurancePreCert",     "InsPreCertID"),
    ]:
        if not _index_exists(cur, SOURCE_SCHEMA, tbl, col):
            print(f"    Adding index on {SOURCE_SCHEMA}.{tbl}({col})...")
            cur.execute(f"CREATE INDEX idx_{col.lower()} ON {SOURCE_SCHEMA}.{tbl} ({col})")
            conn.commit()
            print(f"    done")

    # Split into two steps to avoid a long-running JOIN dropping the connection:
    # Step 1: copy ScheduleReferralLink alone (fast single-table scan)
    # Step 2: UPDATE join with InsurancePreCert using an index (fast)
    print("  Materializing ScheduleReferralLink + InsurancePreCert lookup (nd_ActiveFlag='Y')...")
    if not _table_exists(cur, STAGING_REF):
        cur.execute(f"""
            CREATE TABLE {STAGING_REF} AS
            SELECT
                srl.ApptId,
                srl.InsPreCertId,
                NULL AS AuthorizationStatus,
                NULL AS Active,
                NULL AS InsID,
                NULL AS CoPayAmount
            FROM {SOURCE_SCHEMA}.ScheduleReferralLink srl
            WHERE srl.nd_ActiveFlag = 'Y'
        """)
        cur.execute(f"ALTER TABLE {STAGING_REF} ADD INDEX idx_ref (ApptId)")
        cur.execute(f"ALTER TABLE {STAGING_REF} ADD INDEX idx_ref_cert (InsPreCertId)")
        conn.commit()
        cur.execute(f"""
            UPDATE {STAGING_REF} ref
            JOIN {SOURCE_SCHEMA}.InsurancePreCert ipc
                ON ref.InsPreCertId = ipc.InsPreCertID
               AND ipc.nd_ActiveFlag = 'Y'
            SET ref.AuthorizationStatus = ipc.AuthorizationStatus,
                ref.Active              = ipc.Active,
                ref.InsID               = ipc.InsID,
                ref.CoPayAmount         = ipc.CoPayAmount
        """)
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")
    cur.execute(f"SELECT COUNT(*) FROM {STAGING_REF}")
    print(f"    {cur.fetchone()[0]:,} ScheduleReferralLink rows")

    # ── 2. PK staging table ──────────────────────────────────────────
    print("  Creating PK staging table...")
    if not _table_exists(cur, STAGING_TABLE):
        cur.execute(f"""
            CREATE TABLE {STAGING_TABLE} AS
            SELECT CAST({BATCH_KEY} AS SIGNED) AS {BATCH_KEY}
            FROM {SOURCE_SCHEMA}.ScheduleAppointment
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

    # ── 5. Batch boundary sampling ───────────────────────────────────
    print("  Computing batch boundaries...")
    sys.stdout.flush()

    if total == 0:
        cur.close()
        _release_conn(conn)
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
    _release_conn(conn)

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
        _release_conn(conn)
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
        _release_conn(conn)
        return {"status": "done", "rows": total_rows, "secs": elapsed}

    except Exception as exc:
        elapsed = round(time.time() - t0, 1)
        mark(conn, "failed", total_rows, str(exc))
        _release_conn(conn)
        return {"status": f"FAILED: {exc}", "rows": total_rows, "secs": elapsed}


# ── Main ─────────────────────────────────────────────────────────────

def main():
    print(f"\n{'='*70}", flush=True)
    print(f"  Greenway Appointment ETL → {DEST_TABLE} — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  source     : {SOURCE_SCHEMA}.ScheduleAppointment  (psid={PSID})")
    print(f"  dest       : {DEST_TABLE}")
    print(f"  staging    : {STAGING_TABLE}")
    print(f"  checkpoint : {CHECKPOINT_TABLE}")
    print(f"  batch_size : {BATCH_SIZE:,}")
    print(f"{'='*70}\n", flush=True)

    print("  Connecting to database...")
    sys.stdout.flush()
    ranges = setup_tables()

    if not ranges:
        print(f"\nNo rows found in {SOURCE_SCHEMA}.ScheduleAppointment. Exiting.")
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
    print(f"  [{tag}] {SOURCE_SCHEMA}.ScheduleAppointment  "
          f"{result['rows']:>10,} rows inserted  ({result['secs']}s)")

    print(f"\n{'='*70}")
    if result["status"].startswith("FAILED"):
        print(f"  FAILED: {result['status']}")
    else:
        print(f"  Status: {result['status']}  |  Total rows inserted: {result['rows']:,}")
    print(f"{'='*70}")

    print(f"\n  Cleanup SQL:")
    print(f"    DROP TABLE IF EXISTS {STAGING_PL_V};")
    print(f"    DROP TABLE IF EXISTS {STAGING_VT_ECL};")
    print(f"    DROP TABLE IF EXISTS {STAGING_PLRP};")
    print(f"    DROP TABLE IF EXISTS {STAGING_SAR};")
    print(f"    DROP TABLE IF EXISTS {STAGING_SATL};")
    print(f"    DROP TABLE IF EXISTS {STAGING_SACL};")
    print(f"    DROP TABLE IF EXISTS {STAGING_REF};")
    print(f"    DROP TABLE IF EXISTS {STAGING_TABLE};")
    print(f"    DROP TABLE IF EXISTS {CHECKPOINT_TABLE};")

    if result["status"].startswith("FAILED"):
        sys.exit(1)


if __name__ == "__main__":
    main()
