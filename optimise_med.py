#!/usr/bin/env python3
"""
Optimized ETL loader: rgd_udm_silver.medication
Sources: CLINICALPRESCRIPTION (clinicalprescription) and PATIENTMEDICATION (patientmedication)
Two independent UNION ALL branches batched in parallel using actual med_id ranges.
"""

import sys
import time
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import pymysql
from tqdm import tqdm

# ── Configuration ─────────────────────────────────────────────────────────────
DB_CONFIG = {
    "host":     "YOUR_HOST",
    "port":     3306,
    "user":     "YOUR_USER",
    "password": "YOUR_PASSWORD",
    "database": "YOUR_SOURCE_DATABASE",   # source/staging DB
    "charset":  "utf8mb4",
    "connect_timeout": 30,
    "autocommit": False,
}

BATCH_SIZE  = 50_000
MAX_WORKERS = 2   # Only 2 independent sources; raise if you add more branches

DEST_TABLE       = "rgd_udm_silver.medication"
STAGING_TABLE    = "etl_staging_medication_ids"   # materializes med_id lists per source
CHECKPOINT_TABLE = "etl_checkpoint_medication"

# ── Source definitions ────────────────────────────────────────────────────────
# Each dict describes one UNION ALL branch.
# 'key_col' is the PRIMARY key fetched from the source to drive batching.
# 'source_label' matches the literal 'source' column value in the SQL.
SOURCES = [
    {
        "key":          "clinicalprescription",
        "source_label": "clinicalprescription",
        "key_table":    "CLINICALPRESCRIPTION",
        "key_col":      "CLINICALPRESCRIPTIONID",
        "active_flag":  "cp.nd_active_flag = 'Y'",
    },
    {
        "key":          "patientmedication",
        "source_label": "patientmedication",
        "key_table":    "PATIENTMEDICATION",
        "key_col":      "PATIENTMEDICATIONID",
        "active_flag":  "MED.nd_active_flag = 'Y'",
    },
]

# Source tables that need indexes on their join keys (table → join column).
# WARNING: Creating indexes on large, active production tables may acquire locks
#          and take significant time. Set SKIP_INDEX_CREATION = True to skip.
SKIP_INDEX_CREATION = False

SOURCE_TABLES_JOIN_KEY = {
    "CLINICALPRESCRIPTION": "CLINICALPRESCRIPTIONID",
    "DOCUMENT":             "documentid",
    "CLINICALENCOUNTER":    "clinicalencounterid",
    "FDB_RNDC14":           "NDC",
    "PATIENTMEDICATION":    "PATIENTMEDICATIONID",
    "MEDICATION":           "medicationid",
}

# ── Helpers ───────────────────────────────────────────────────────────────────
def get_connection():
    return pymysql.connect(**DB_CONFIG)


def chunk(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


# ── Checkpoint ────────────────────────────────────────────────────────────────
def is_done(conn, source_key):
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT status FROM {CHECKPOINT_TABLE} WHERE source_key = %s",
            (source_key,),
        )
        row = cur.fetchone()
        return row is not None and row[0] == "done"


def mark(conn, source_key, status, rows=0, error=None):
    with conn.cursor() as cur:
        cur.execute(
            f"""
            INSERT INTO {CHECKPOINT_TABLE} (source_key, status, rows_inserted, error_msg, updated_at)
            VALUES (%s, %s, %s, %s, NOW())
            ON DUPLICATE KEY UPDATE
                status        = VALUES(status),
                rows_inserted = rows_inserted + VALUES(rows_inserted),
                error_msg     = VALUES(error_msg),
                updated_at    = NOW()
            """,
            (source_key, status, rows, (str(error)[:2000] if error else None)),
        )
    conn.commit()


# ── INSERT builders ───────────────────────────────────────────────────────────
def build_insert_clinicalprescription(id_lo, id_hi):
    return f"""
        INSERT INTO {DEST_TABLE} (
            source, med_id, ndid, eid, enc_date,
            written_date, med_administered_datetime, doc_orderdatetime,
            med_start_date, med_end_date,
            med_createddatetime, doc_createddatetime,
            last_dispensed_date, sample_expiration_date,
            administer_expiration_date, earliest_fill_date,
            med_code, med_name, med_coding_system, med_status, med_status_flag,
            med_indication, med_formulation, med_route,
            med_strength, med_strength_unit, med_frequency,
            med_pb_qty, med_days_supply, med_refills, med_directions,
            fill_date, med_fill_type, discont_date, discont_reason,
            created_datetime, created_by, ehr_source_name, source_path,
            data_type, psid, updated_datetime, updated_by, nd_extracted_date
        )
        SELECT
            'clinicalprescription'                                         AS source,
            cp.CLINICALPRESCRIPTIONID                                      AS med_id,
            d.CHARTID                                                      AS ndid,
            ce.CLINICALENCOUNTERID                                         AS eid,
            ce.ENCOUNTERDATE                                               AS enc_date,
            STR_TO_DATE(cp.WRITTENDATEDATETIME,          '%Y-%m-%d %H:%i:%s') AS written_date,
            STR_TO_DATE(cp.MEDICATIONADMINISTEREDDATETIME,'%Y-%m-%d %H:%i:%s') AS med_administered_datetime,
            STR_TO_DATE(d.ORDERDATETIME,                 '%Y-%m-%d %H:%i:%s') AS doc_orderdatetime,
            STR_TO_DATE(cp.STARTDATEDATETIME,            '%Y-%m-%d %H:%i:%s') AS med_start_date,
            STR_TO_DATE(cp.STOPDATEDATETIME,             '%Y-%m-%d %H:%i:%s') AS med_end_date,
            cp.CREATEDDATETIME                                             AS med_createddatetime,
            d.CREATEDDATETIME                                              AS doc_createddatetime,
            STR_TO_DATE(cp.LASTDISPENSEDDATEDATETIME,    '%Y-%m-%d %H:%i:%s') AS last_dispensed_date,
            STR_TO_DATE(cp.SAMPLEEXPIRATIONDATEDATETIME, '%Y-%m-%d %H:%i:%s') AS sample_expiration_date,
            STR_TO_DATE(cp.ADMINISTEREXPIRATIONDATEDATETIME,'%Y-%m-%d %H:%i:%s') AS administer_expiration_date,
            STR_TO_DATE(cp.EARLIESTFILLDATEDATETIME,     '%Y-%m-%d %H:%i:%s') AS earliest_fill_date,
            cp.NDC                                                         AS med_code,
            COALESCE(fndc.LN60, cp.labelname, fdb.MED_MEDID_DESC,
                     d.CLINICALORDERTYPE)                                  AS med_name,
            CASE WHEN cp.NDC IS NOT NULL THEN 'NDC' ELSE NULL END          AS med_coding_system,
            NULL                                                           AS med_status,
            ''                                                             AS med_status_flag,
            ''                                                             AS med_indication,
            cp.DOSAGEFORM                                                  AS med_formulation,
            NULL                                                           AS med_route,
            cp.AVGDAILYDOSEQUANTITY                                        AS med_strength,
            cp.AVGDAILYDOSEUNIT                                            AS med_strength_unit,
            cp.FREQUENCY                                                   AS med_frequency,
            cp.DOSAGEQUANTITY                                              AS med_pb_qty,
            cp.DURATION                                                    AS med_days_supply,
            cp.NUMBERREFILLSALLOWED                                        AS med_refills,
            cp.SIG                                                         AS med_directions,
            STR_TO_DATE(cp.LASTFILLDATEDATETIME,         '%Y-%m-%d %H:%i:%s') AS fill_date,
            NULL                                                           AS med_fill_type,
            NULL                                                           AS discont_date,
            ''                                                             AS discont_reason,
            CURRENT_TIMESTAMP()                                            AS created_datetime,
            'ND'                                                           AS created_by,
            'athenaone'                                                    AS ehr_source_name,
            'bronze_layer'                                                 AS source_path,
            'Structured'                                                   AS data_type,
            5                                                              AS psid,
            CURRENT_TIMESTAMP()                                            AS updated_datetime,
            'ND'                                                           AS updated_by,
            cp.nd_extracted_date
        FROM CLINICALPRESCRIPTION cp
        LEFT JOIN DOCUMENT d
            ON cp.documentid = d.documentid AND d.nd_active_flag = 'Y'
        LEFT JOIN CLINICALENCOUNTER ce
            ON d.clinicalencounterid = ce.clinicalencounterid AND ce.nd_active_flag = 'Y'
        LEFT JOIN FDB_RNDC14 fndc
            ON cp.NDC = fndc.NDC AND fndc.nd_active_flag = 'Y'
        LEFT JOIN (
            SELECT *
            FROM (
                SELECT *,
                       ROW_NUMBER() OVER (PARTITION BY FDB_RMIID1ID ORDER BY LASTUPDATED DESC) AS rn
                FROM raleigh.FDB_RMIID1
            ) x
            WHERE rn = 1
        ) fdb ON d.fbdmedid = fdb.medid
        WHERE cp.nd_active_flag = 'Y'
          AND cp.CLINICALPRESCRIPTIONID >= {id_lo}
          AND cp.CLINICALPRESCRIPTIONID <  {id_hi}
    """


def build_insert_patientmedication(id_lo, id_hi):
    return f"""
        INSERT INTO {DEST_TABLE} (
            source, med_id, ndid, eid, enc_date,
            written_date, med_administered_datetime, doc_orderdatetime,
            med_start_date, med_end_date,
            med_createddatetime, doc_createddatetime,
            last_dispensed_date, sample_expiration_date,
            administer_expiration_date, earliest_fill_date,
            med_code, med_name, med_coding_system, med_status, med_status_flag,
            med_indication, med_formulation, med_route,
            med_strength, med_strength_unit, med_frequency,
            med_pb_qty, med_days_supply, med_refills, med_directions,
            fill_date, med_fill_type, discont_date, discont_reason,
            created_datetime, created_by, ehr_source_name, source_path,
            data_type, psid, updated_datetime, updated_by, nd_extracted_date
        )
        SELECT DISTINCT
            'patientmedication'                                             AS source,
            MED.PATIENTMEDICATIONID                                         AS med_id,
            MED.CHARTID                                                     AS ndid,
            CE.CLINICALENCOUNTERID                                          AS eid,
            CE.ENCOUNTERDATE                                                AS enc_date,
            NULL                                                            AS written_date,
            STR_TO_DATE(MED.MEDADMINISTEREDDATETIME,    '%Y-%m-%d %H:%i:%s') AS med_administered_datetime,
            STR_TO_DATE(DOC.ORDERDATETIME,              '%Y-%m-%d %H:%i:%s') AS doc_orderdatetime,
            STR_TO_DATE(MED.startdate,                  '%Y-%m-%d %H:%i:%s') AS med_start_date,
            STR_TO_DATE(MED.stopdate,                   '%Y-%m-%d %H:%i:%s') AS med_end_date,
            MED.CREATEDDATETIME                                             AS med_createddatetime,
            DOC.CREATEDDATETIME                                             AS doc_createddatetime,
            STR_TO_DATE(MED.DISPENSEDEXPIRATIONDATE,    '%Y-%m-%d %H:%i:%s') AS last_dispensed_date,
            NULL                                                            AS sample_expiration_date,
            STR_TO_DATE(MED.ADMINISTEREDEXPIRATIONDATE, '%Y-%m-%d %H:%i:%s') AS administer_expiration_date,
            NULL                                                            AS earliest_fill_date,
            CASE WHEN LOWER(TRIM(MED1.NDC)) = 'none' THEN NULL
                 ELSE TRIM(MED1.NDC) END                                    AS med_code,
            COALESCE(
                NULLIF(TRIM(MED.MEDICATIONNAME), 'none'),
                NULLIF(TRIM(MED1.MEDICATIONNAME), 'none'),
                DOC.CLINICALORDERTYPE
            )                                                               AS med_name,
            CASE WHEN MED1.NDC IS NOT NULL THEN 'NDC' ELSE NULL END         AS med_coding_system,
            CASE WHEN MED.DEACTIVATIONDATETIME IS NULL
                 THEN 'Active' ELSE 'Inactive' END                          AS med_status,
            ''                                                              AS med_status_flag,
            ''                                                              AS med_indication,
            TRIM(MED.DOSAGEFORM)                                            AS med_formulation,
            TRIM(MED.DOSAGEROUTE)                                           AS med_route,
            TRIM(MED.DOSAGESTRENGTH)                                        AS med_strength,
            TRIM(MED.DOSAGESTRENGTHUNITS)                                   AS med_strength_unit,
            TRIM(MED.FREQUENCY)                                             AS med_frequency,
            MED.PRESCRIPTIONFILLQUANTITY                                    AS med_pb_qty,
            MED.LENGTHOFCOURSE                                              AS med_days_supply,
            REPLACE(MED.NUMBEROFREFILLSPRESCRIBED, '.0', '')                AS med_refills,
            MED.sig                                                         AS med_directions,
            STR_TO_DATE(MED.FILLDATE,                   '%Y-%m-%d %H:%i:%s') AS fill_date,
            NULL                                                            AS med_fill_type,
            NULL                                                            AS discont_date,
            ''                                                              AS discont_reason,
            CURRENT_TIMESTAMP()                                             AS created_datetime,
            'ND'                                                            AS created_by,
            'athenaone'                                                     AS ehr_source_name,
            'bronze_layer'                                                  AS source_path,
            'Structured'                                                    AS data_type,
            5                                                               AS psid,
            CURRENT_TIMESTAMP()                                             AS updated_datetime,
            'ND'                                                            AS updated_by,
            MED.nd_extracted_date
        FROM PATIENTMEDICATION MED
        LEFT JOIN MEDICATION MED1
            ON REPLACE(MED.medicationid, '.0', '') = REPLACE(MED1.medicationid, '.0', '')
           AND MED1.nd_active_flag = 'Y'
        LEFT JOIN DOCUMENT DOC
            ON MED.DOCUMENTID = DOC.DOCUMENTID AND DOC.nd_active_flag = 'Y'
        LEFT JOIN CLINICALENCOUNTER CE
            ON DOC.CLINICALENCOUNTERID = CE.CLINICALENCOUNTERID AND CE.nd_active_flag = 'Y'
        WHERE MED.nd_active_flag = 'Y'
          AND MED.PATIENTMEDICATIONID >= {id_lo}
          AND MED.PATIENTMEDICATIONID <  {id_hi}
    """


INSERT_BUILDERS = {
    "clinicalprescription": build_insert_clinicalprescription,
    "patientmedication":    build_insert_patientmedication,
}


# ── Setup ─────────────────────────────────────────────────────────────────────
def setup_tables():
    conn = get_connection()
    try:
        cur = conn.cursor()
        db_name = DB_CONFIG["database"]

        # 1. Destination table (IF NOT EXISTS — never drop/truncate)
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {DEST_TABLE} (
                id                         BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
                source                     VARCHAR(100),
                med_id                     VARCHAR(100),
                ndid                       VARCHAR(100),
                eid                        VARCHAR(100),
                enc_date                   DATE,
                written_date               DATETIME,
                med_administered_datetime  DATETIME,
                doc_orderdatetime          DATETIME,
                med_start_date             DATETIME,
                med_end_date               DATETIME,
                med_createddatetime        DATETIME,
                doc_createddatetime        DATETIME,
                last_dispensed_date        DATETIME,
                sample_expiration_date     DATETIME,
                administer_expiration_date DATETIME,
                earliest_fill_date         DATETIME,
                med_code                   VARCHAR(255),
                med_name                   TEXT,
                med_coding_system          VARCHAR(50),
                med_status                 VARCHAR(50),
                med_status_flag            VARCHAR(50),
                med_indication             VARCHAR(255),
                med_formulation            VARCHAR(255),
                med_route                  VARCHAR(255),
                med_strength               VARCHAR(255),
                med_strength_unit          VARCHAR(100),
                med_frequency              VARCHAR(255),
                med_pb_qty                 VARCHAR(100),
                med_days_supply            VARCHAR(100),
                med_refills                VARCHAR(100),
                med_directions             TEXT,
                fill_date                  DATETIME,
                med_fill_type              VARCHAR(100),
                discont_date               DATETIME,
                discont_reason             VARCHAR(255),
                created_datetime           DATETIME,
                created_by                 VARCHAR(50),
                ehr_source_name            VARCHAR(100),
                source_path                VARCHAR(100),
                data_type                  VARCHAR(50),
                psid                       INT,
                updated_datetime           DATETIME,
                updated_by                 VARCHAR(50),
                nd_extracted_date          DATE,
                INDEX idx_source (source),
                INDEX idx_med_id (med_id(100))
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """)
        conn.commit()
        print(f"[setup] Destination table ready: {DEST_TABLE}")

        # 2. Checkpoint table (IF NOT EXISTS)
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {CHECKPOINT_TABLE} (
                source_key    VARCHAR(100) PRIMARY KEY,
                status        VARCHAR(20) NOT NULL DEFAULT 'pending',
                rows_inserted BIGINT       NOT NULL DEFAULT 0,
                error_msg     TEXT,
                updated_at    DATETIME
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """)
        conn.commit()
        print(f"[setup] Checkpoint table ready: {CHECKPOINT_TABLE}")

        # 3. Ensure source-table indexes on join keys
        if not SKIP_INDEX_CREATION:
            for tbl, col in SOURCE_TABLES_JOIN_KEY.items():
                cur.execute("""
                    SELECT COUNT(*) FROM information_schema.statistics
                    WHERE table_schema = %s AND table_name = %s AND column_name = %s
                """, (db_name, tbl, col))
                if cur.fetchone()[0] == 0:
                    idx_name = f"idx_etl_{col.lower()}"
                    print(f"[setup] Creating index {idx_name} on {tbl}({col}) …")
                    cur.execute(f"CREATE INDEX {idx_name} ON {tbl} ({col})")
                    conn.commit()
                    print(f"[setup]   Done.")
                else:
                    print(f"[setup] Index already exists on {tbl}({col}) — skipping.")
        else:
            print("[setup] SKIP_INDEX_CREATION=True — skipping source-table index checks.")

        # 4. Load actual key ranges per source (sparse-key-safe batching)
        ranges = {}
        for src in SOURCES:
            skey  = src["key"]
            tbl   = src["key_table"]
            col   = src["key_col"]
            aflag = src["active_flag"]
            alias = tbl.split(".")[0] if "." in tbl else tbl[:3].upper()
            if tbl == "CLINICALPRESCRIPTION":
                alias = "cp"
            elif tbl == "PATIENTMEDICATION":
                alias = "MED"

            print(f"[setup] Fetching key list for {skey} …")
            cur.execute(f"SELECT {col} FROM {tbl} {alias} WHERE {aflag} ORDER BY {col}")
            ids = [r[0] for r in cur.fetchall()]
            batches = []
            for batch in chunk(ids, BATCH_SIZE):
                lo = batch[0]
                hi = batch[-1] + 1   # exclusive upper bound
                batches.append((lo, hi))
            ranges[skey] = batches
            print(f"[setup]   {len(ids):,} rows → {len(batches):,} batches")

        return ranges

    finally:
        conn.close()


# ── Worker ────────────────────────────────────────────────────────────────────
def run_source(src, ranges, pbar):
    skey    = src["key"]
    batches = ranges[skey]
    builder = INSERT_BUILDERS[skey]
    total_rows = 0

    conn = get_connection()
    try:
        if is_done(conn, skey):
            print(f"\n[{skey}] Already done — skipping.")
            pbar.update(len(batches))
            return skey, 0, "skipped"

        mark(conn, skey, "running")

        with conn.cursor() as cur:
            cur.execute("SET SESSION unique_checks      = 0")
            cur.execute("SET SESSION foreign_key_checks = 0")

        for lo, hi in batches:
            sql = builder(lo, hi)
            with conn.cursor() as cur:
                cur.execute(sql)
                n = cur.rowcount
            conn.commit()
            total_rows += n
            pbar.update(1)

        with conn.cursor() as cur:
            cur.execute("SET SESSION unique_checks      = 1")
            cur.execute("SET SESSION foreign_key_checks = 1")

        mark(conn, skey, "done", rows=total_rows)
        return skey, total_rows, "done"

    except Exception as e:
        try:
            conn.rollback()
            with conn.cursor() as cur:
                cur.execute("SET SESSION unique_checks      = 1")
                cur.execute("SET SESSION foreign_key_checks = 1")
        except Exception:
            pass
        mark(conn, skey, "failed", error=e)
        return skey, total_rows, f"FAILED: {e}"

    finally:
        conn.close()


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    print("=" * 70)
    print(f"  Medication ETL — {datetime.now():%Y-%m-%d %H:%M:%S}")
    print(f"  Destination : {DEST_TABLE}")
    print(f"  Checkpoint  : {CHECKPOINT_TABLE}")
    print(f"  Batch size  : {BATCH_SIZE:,}")
    print(f"  Workers     : {MAX_WORKERS}")
    print("=" * 70)

    t0 = time.time()

    print("\n[phase 1] Setting up tables and computing batch ranges …")
    ranges = setup_tables()

    total_batches = sum(len(v) for v in ranges.values())
    print(f"\n[phase 2] Launching {len(SOURCES)} worker(s) | {total_batches} total batches\n")

    results = {}
    with tqdm(total=total_batches, unit="batch", ncols=80) as pbar:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            futures = {
                ex.submit(run_source, src, ranges, pbar): src["key"]
                for src in SOURCES
            }
            for fut in as_completed(futures):
                skey, rows, status = fut.result()
                results[skey] = (rows, status)

    elapsed = time.time() - t0
    print(f"\n{'─'*70}")
    print(f"  {'Source':<30}  {'Rows':>10}  Status")
    print(f"{'─'*70}")
    for src in SOURCES:
        skey = src["key"]
        rows, status = results.get(skey, (0, "unknown"))
        print(f"  {skey:<30}  {rows:>10,}  {status}")
    print(f"{'─'*70}")
    print(f"  Total elapsed: {elapsed:.1f}s\n")

    print("── Cleanup (run manually when ready) ──────────────────────────────")
    print(f"  DROP TABLE IF EXISTS {CHECKPOINT_TABLE};")
    print(f"  -- DROP TABLE IF EXISTS {STAGING_TABLE};  (not used by this script)")


if __name__ == "__main__":
    main()