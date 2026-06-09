#!/usr/bin/env python3
"""
inc_gw_proc_staging.py — Incremental INSERT for udm_staging.procedures (Greenway / savannah)

SQL equivalent (flattened):
    INSERT INTO udm_staging.procedures (...)
    SELECT
        FLOOR(TRIM(a.ServiceDetailID))       AS proc_id,
        TRIM(a.PatientID)                    AS ndid,
        TRIM(b.VisitID)                      AS eid,
        DATE_FORMAT(STR_TO_DATE(...fromdatetime...), '%Y-%m-%d') AS encounter_date,
        DATE_FORMAT(STR_TO_DATE(...FromDate...), '%Y-%m-%d')     AS proc_start_date,
        DATE_FORMAT(STR_TO_DATE(...ToDate...), '%Y-%m-%d')       AS proc_last_date,
        NULL                                 AS proc_category,
        TRIM(a.ProcedureCode)                AS proc_code,
        TRIM(c.StandardDescription)          AS proc_name / proc_description,
        ...
        CONCAT_WS(':',psid,ndid,eid,encounter_date,proc_start_date,proc_last_date,
                  proc_code,proc_name)       AS udm_unq_id,
        COALESCE(encounter_date)             AS enc_date_proxy
    FROM savannah.br_ServiceDetail a
    INNER JOIN savannah.Visit b               ON a.VisitID=b.VisitID  AND a/b.nd_ActiveFlag='Y'
    INNER JOIN savannah.ProcedureMasterInfo c ON a.ProcedureMasterID=c.ProcedureMasterID AND c.nd_ActiveFlag='Y'
    LEFT  JOIN savannah.CareProvider d        ON a.CareProviderID=d.CareProviderID AND d.nd_ActiveFlag='Y'
    LEFT  JOIN savannah.PlaceOfService ps     ON a.PlaceOfServiceID=ps.POSCode AND ps.nd_ActiveFlag='Y'
    WHERE DATE(a.nd_extracted_date) > INCREMENTAL_DATE;

Pre-materialized lookup tables (computed ONCE, reused across all batches):
  - staging.inc_gw_proc_visit_{SOURCE_SCHEMA}        (Visit — nd_ActiveFlag='Y', enc_date pre-computed)
  - staging.inc_gw_proc_master_{SOURCE_SCHEMA}       (ProcedureMasterInfo — nd_ActiveFlag='Y')
  - staging.inc_gw_proc_provider_{SOURCE_SCHEMA}     (CareProvider — nd_ActiveFlag='Y')
  - staging.inc_gw_proc_pos_{SOURCE_SCHEMA}          (PlaceOfService — nd_ActiveFlag='Y')

Batching by ServiceDetailID.
Filter: a.nd_ActiveFlag='Y' AND DATE(a.nd_extracted_date) > INCREMENTAL_DATE.

Usage:
    python inc_gw_proc_staging.py
"""

import sys
import time
from datetime import datetime
import pymysql
from tqdm import tqdm

# ── Configuration ─────────────────────────────────────────────────────
DB_CONFIG = {
    "host":            "172.16.2.42",
    "port":            3306,
    "user":            "nd-root-mysql",
    "password":        "kmsamd89undsd4",
    "database":        "udm_staging",
    "charset":         "utf8mb4",
    "connect_timeout": 30,
    "read_timeout":    21600,
    "write_timeout":   21600,
}

BATCH_SIZE = 50_000

# ── Change these to run for a different schema / date cutoff ──────────
SOURCE_SCHEMA    = "savannah"
PSID             = 9
INCREMENTAL_DATE = "2026-01-26"

DEST_TABLE = "udm_staging.procedures"

# ─────────────────────────────────────────────────────────────────────
STAGING_VISIT    = f"staging.inc_gw_proc_visit1_{SOURCE_SCHEMA}"     # Visit — enc_date pre-computed
STAGING_MASTER   = f"staging.inc_gw_proc_master1_{SOURCE_SCHEMA}"    # ProcedureMasterInfo
STAGING_PROVIDER = f"staging.inc_gw_proc_provider1_{SOURCE_SCHEMA}"  # CareProvider
STAGING_POS      = f"staging.inc_gw_proc_pos1_{SOURCE_SCHEMA}"       # PlaceOfService
STAGING_PK       = f"staging.inc_gw_proc_pk_{SOURCE_SCHEMA}"        # eligible ServiceDetailIDs
CHECKPOINT_TABLE = f"staging.etl_checkpoint_inc_gw_proc2_{SOURCE_SCHEMA}"
CHECKPOINT_KEY   = f"inc_gw_procedures.{SOURCE_SCHEMA}"

BATCH_KEY = "ServiceDetailID"


# ── Helpers ───────────────────────────────────────────────────────────

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


def _build_ranges(cur, staging_pk):
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


# ── Batch INSERT builder ──────────────────────────────────────────────

def build_batch_insert(pk_lo, pk_hi):
    """
    Joins br_ServiceDetail against pre-materialized staging tables.
    encounter_date is pre-computed in STAGING_VISIT (avoids datetime cast issues on raw source).
    proc_start_date / proc_last_date use the same STR_TO_DATE formatting as the original SQL
    (applied inline to br_ServiceDetail columns FromDate / ToDate).
    udm_unq_id and enc_date_proxy computed inline — no outer wrapper needed.
    """
    return f"""
INSERT INTO {DEST_TABLE}
    (proc_id, ndid, eid, encounter_date, proc_start_date, proc_last_date,
     proc_category, proc_code, proc_name, proc_coding_system, proc_units,
     proc_description, proc_notes, anesthesia_flag, anesthesia_detail_id,
     ordering_provider_id, ordering_provider_name, ordering_provider_npi,
     rendering_provider_id, rendering_provider_name, rendering_provider_npi,
     referring_provider_id, referring_provider_name, referring_provider_npi,
     place_of_service_Id, place_of_service_desc, order_date, Diagnosis_Indication,
     nd_extracted_date, created_datetime, created_by, updated_datetime, updated_by,
     ehr_source_name, source_path, data_type, psid, udm_inc_id,
     udm_unq_id, enc_date_proxy)
SELECT
    FLOOR(TRIM(a.{BATCH_KEY}))                                     AS proc_id,
    TRIM(a.PatientID)                                              AS ndid,
    TRIM(b.VisitID)                                                AS eid,
    b.encounter_date                                               AS encounter_date,
    DATE_FORMAT(
        STR_TO_DATE(SUBSTRING_INDEX(a.FromDate, '.', 1),
                    '%Y-%m-%d %H:%i:%s'),
        '%Y-%m-%d')                                                AS proc_start_date,
    DATE_FORMAT(
        STR_TO_DATE(SUBSTRING_INDEX(a.ToDate, '.', 1),
                    '%Y-%m-%d %H:%i:%s'),
        '%Y-%m-%d')                                                AS proc_last_date,
    NULL                                                           AS proc_category,
    TRIM(a.ProcedureCode)                                          AS proc_code,
    TRIM(c.StandardDescription)                                    AS proc_name,
    NULL                                                           AS proc_coding_system,
    a.NumberOfDaysOrUnits                                          AS proc_units,
    TRIM(c.StandardDescription)                                    AS proc_description,
    NULL                                                           AS proc_notes,
    NULL                                                           AS anesthesia_flag,
    NULL                                                           AS anesthesia_detail_id,
    TRIM(a.CareProviderID)                                         AS ordering_provider_id,
    NULL                                                           AS ordering_provider_name,
    TRIM(d.NationalProviderID)                                     AS ordering_provider_npi,
    FLOOR(TRIM(a.RenderingProviderID))                             AS rendering_provider_id,
    NULL                                                           AS rendering_provider_name,
    NULL                                                           AS rendering_provider_npi,
    FLOOR(TRIM(a.ReferringProvID))                                 AS referring_provider_id,
    NULL                                                           AS referring_provider_name,
    CASE WHEN a.CareProviderID = a.ReferringProvID
         THEN d.NationalProviderID END                             AS referring_provider_npi,
    a.PlaceOfServiceID                                             AS place_of_service_Id,
    ps.POSDesc                                                     AS place_of_service_desc,
    a.OrderDate                                                    AS order_date,
    NULL                                                           AS Diagnosis_Indication,
    a.nd_extracted_date                                            AS nd_extracted_date,
    CURRENT_DATE()                                                 AS created_datetime,
    'ND'                                                           AS created_by,
    CURRENT_DATE()                                                 AS updated_datetime,
    'ND'                                                           AS updated_by,
    'Greenway'                                                     AS ehr_source_name,
    'bronze_table'                                                 AS source_path,
    'Structured'                                                   AS data_type,
    '{PSID}'                                                       AS psid,
    NULL                                                           AS udm_inc_id,
    CONCAT_WS(':',
        COALESCE('{PSID}',                                         ''),
        COALESCE(TRIM(a.PatientID),                                ''),
        COALESCE(TRIM(b.VisitID),                                  ''),
        COALESCE(b.encounter_date,                                 ''),
        COALESCE(DATE_FORMAT(
            STR_TO_DATE(SUBSTRING_INDEX(a.FromDate, '.', 1),
                        '%Y-%m-%d %H:%i:%s'), '%Y-%m-%d'),         ''),
        COALESCE(DATE_FORMAT(
            STR_TO_DATE(SUBSTRING_INDEX(a.ToDate, '.', 1),
                        '%Y-%m-%d %H:%i:%s'), '%Y-%m-%d'),         ''),
        COALESCE(TRIM(a.ProcedureCode),                            ''),
        COALESCE(TRIM(c.StandardDescription),                      '')
    )                                                              AS udm_unq_id,
    b.encounter_date                                               AS enc_date_proxy
FROM {SOURCE_SCHEMA}.br_ServiceDetail a
INNER JOIN {STAGING_VISIT} b
    ON b.VisitID = a.VisitID
INNER JOIN {STAGING_MASTER} c
    ON c.ProcedureMasterID = a.ProcedureMasterID
LEFT JOIN {STAGING_PROVIDER} d
    ON d.CareProviderID = a.CareProviderID
LEFT JOIN {STAGING_POS} ps
    ON ps.POSCode = a.PlaceOfServiceID
WHERE a.nd_ActiveFlag = 'Y'
  AND DATE(a.nd_extracted_date) > '{INCREMENTAL_DATE}'
  AND a.{BATCH_KEY} >= {pk_lo}
  AND a.{BATCH_KEY} <  {pk_hi}
"""


# ── Checkpoint ────────────────────────────────────────────────────────

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


# ── Setup ─────────────────────────────────────────────────────────────

def setup_tables():
    conn = get_connection()
    cur  = conn.cursor()

    try:
        # ── 1. Checkpoint table ──────────────────────────────────────
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

        # ── 2. Visit staging ──────────────────────────────────────────
        # encounter_date is pre-computed here on the raw source (fromdatetime is
        # VARCHAR-like in the source; storing it as-is would let MySQL cast it to
        # DATETIME, breaking the STR_TO_DATE formatting in later expressions).
        print("  Materializing Visit lookup (nd_ActiveFlag='Y')...")
        if not _table_exists(cur, STAGING_VISIT):
            cur.execute(f"""
                CREATE TABLE {STAGING_VISIT} AS
                SELECT
                    VisitID,
                    DATE_FORMAT(
                        STR_TO_DATE(SUBSTRING_INDEX(fromdatetime, '.', 1),
                                    '%Y-%m-%d %H:%i:%s'),
                        '%Y-%m-%d') AS encounter_date
                FROM {SOURCE_SCHEMA}.Visit
                WHERE nd_ActiveFlag = 'Y'
            """)
            cur.execute(f"ALTER TABLE {STAGING_VISIT} ADD INDEX idx_visit (VisitID)")
            conn.commit()
            print("    created")
        else:
            print("    already exists, reusing")
        cur.execute(f"SELECT COUNT(*) FROM {STAGING_VISIT}")
        print(f"    {cur.fetchone()[0]:,} rows")

        # ── 3. ProcedureMasterInfo staging ────────────────────────────
        print("  Materializing ProcedureMasterInfo lookup (nd_ActiveFlag='Y')...")
        if not _table_exists(cur, STAGING_MASTER):
            cur.execute(f"""
                CREATE TABLE {STAGING_MASTER} AS
                SELECT
                    ProcedureMasterID,
                    StandardDescription
                FROM {SOURCE_SCHEMA}.ProcedureMasterInfo
                WHERE nd_ActiveFlag = 'Y'
            """)
            cur.execute(f"ALTER TABLE {STAGING_MASTER} ADD INDEX idx_master (ProcedureMasterID)")
            conn.commit()
            print("    created")
        else:
            print("    already exists, reusing")
        cur.execute(f"SELECT COUNT(*) FROM {STAGING_MASTER}")
        print(f"    {cur.fetchone()[0]:,} rows")

        # ── 4. CareProvider staging ───────────────────────────────────
        print("  Materializing CareProvider lookup (nd_ActiveFlag='Y')...")
        if not _table_exists(cur, STAGING_PROVIDER):
            cur.execute(f"""
                CREATE TABLE {STAGING_PROVIDER} AS
                SELECT
                    CareProviderID,
                    NationalProviderID
                FROM {SOURCE_SCHEMA}.CareProvider
                WHERE nd_ActiveFlag = 'Y'
            """)
            cur.execute(f"ALTER TABLE {STAGING_PROVIDER} ADD INDEX idx_provider (CareProviderID)")
            conn.commit()
            print("    created")
        else:
            print("    already exists, reusing")
        cur.execute(f"SELECT COUNT(*) FROM {STAGING_PROVIDER}")
        print(f"    {cur.fetchone()[0]:,} rows")

        # ── 5. PlaceOfService staging ─────────────────────────────────
        print("  Materializing PlaceOfService lookup (nd_ActiveFlag='Y')...")
        if not _table_exists(cur, STAGING_POS):
            cur.execute(f"""
                CREATE TABLE {STAGING_POS} AS
                SELECT
                    POSCode,
                    POSDesc
                FROM {SOURCE_SCHEMA}.PlaceOfService
                WHERE nd_ActiveFlag = 'Y'
            """)
            cur.execute(f"ALTER TABLE {STAGING_POS} ADD INDEX idx_pos (POSCode(50))")
            conn.commit()
            print("    created")
        else:
            print("    already exists, reusing")
        cur.execute(f"SELECT COUNT(*) FROM {STAGING_POS}")
        print(f"    {cur.fetchone()[0]:,} rows")

        # ── 6. PK staging — eligible ServiceDetailIDs ─────────────────
        print(f"  Creating PK staging (nd_ActiveFlag='Y' AND nd_extracted_date > '{INCREMENTAL_DATE}')...")
        if not _table_exists(cur, STAGING_PK):
            cur.execute(f"""
                CREATE TABLE {STAGING_PK} AS
                SELECT {BATCH_KEY}
                FROM {SOURCE_SCHEMA}.br_ServiceDetail
                WHERE nd_ActiveFlag = 'Y'
                  AND DATE(nd_extracted_date) > '{INCREMENTAL_DATE}'
                  AND {BATCH_KEY} IS NOT NULL
            """)
            cur.execute(f"ALTER TABLE {STAGING_PK} ADD INDEX idx_pk ({BATCH_KEY})")
            conn.commit()
            cur.execute(f"SELECT COUNT(*) FROM {STAGING_PK}")
            n = cur.fetchone()[0]
            print(f"    created  ({n:,} eligible rows)")
        else:
            cur.execute(f"SELECT COUNT(*) FROM {STAGING_PK}")
            n = cur.fetchone()[0]
            print(f"    already exists, reusing  ({n:,} rows)")

        ranges, total = _build_ranges(cur, STAGING_PK)
        print(f"    {total:,} rows → {len(ranges)} batches of ~{BATCH_SIZE:,}")
        return ranges, total

    finally:
        cur.close()
        conn.close()


# ── Runner ────────────────────────────────────────────────────────────

def run(ranges, pbar):
    conn       = get_connection()
    t0         = time.time()
    total_rows = 0

    try:
        if is_done(conn):
            conn.close()
            pbar.update(len(ranges))
            return {"status": "skipped", "rows": 0, "secs": 0.0}

        mark(conn, "running")
        cur = conn.cursor()

        cur.execute("SET unique_checks = 0")
        cur.execute("SET foreign_key_checks = 0")

        for lo, hi in ranges:
            sql = build_batch_insert(lo, hi)
            cur.execute(sql)
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
    print(f"  Incremental GW Procedures Staging ETL — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  source schema    : {SOURCE_SCHEMA}  (psid={PSID})")
    print(f"  incremental date : > {INCREMENTAL_DATE}")
    print(f"  dest             : {DEST_TABLE}")
    print(f"  visit staging    : {STAGING_VISIT}")
    print(f"  proc master stg  : {STAGING_MASTER}")
    print(f"  provider staging : {STAGING_PROVIDER}")
    print(f"  pos staging      : {STAGING_POS}")
    print(f"  checkpoint       : {CHECKPOINT_TABLE}")
    print(f"  batch_key        : {BATCH_KEY}")
    print(f"  batch_size       : {BATCH_SIZE:,}")
    print(f"{'='*70}\n", flush=True)

    print("Setup:")
    sys.stdout.flush()
    ranges, _ = setup_tables()
    print()

    if not ranges:
        print("  No eligible rows — nothing to do.")
        sys.exit(0)

    with tqdm(total=len(ranges), desc="Overall", unit="batch") as pbar:
        result = run(ranges, pbar)

    status = result["status"]
    rows   = result["rows"]
    secs   = result["secs"]

    print(f"\n{'='*70}")
    if status == "done":
        print(f"  DONE   {rows:,} rows inserted  ({secs}s)")
    elif status == "skipped":
        print(f"  SKIPPED — already marked done in checkpoint")
    else:
        print(f"  FAILED — {status}")
    print(f"{'='*70}")

    print(f"\n  Cleanup SQL (run after verifying data):")
    print(f"    DROP TABLE IF EXISTS {STAGING_VISIT};")
    print(f"    DROP TABLE IF EXISTS {STAGING_MASTER};")
    print(f"    DROP TABLE IF EXISTS {STAGING_PROVIDER};")
    print(f"    DROP TABLE IF EXISTS {STAGING_POS};")
    print(f"    DROP TABLE IF EXISTS {STAGING_PK};")
    print(f"    DROP TABLE IF EXISTS {CHECKPOINT_TABLE};")
    print()

    if status.startswith("FAILED"):
        sys.exit(1)


if __name__ == "__main__":
    main()
