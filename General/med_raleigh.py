#!/usr/bin/env python3
"""
Batched INSERT for raleigh medications → incremental_staging.medications

Two independent sources inserted in parallel:
  1. clinicalprescription — CLINICALPRESCRIPTION + DOCUMENT + FDB joins
  2. patientmedication    — PATIENTMEDICATION + MEDICATION + DOCUMENT joins

Both sources filter on nd_active_flag='Y' AND nd_extracted_date > EXTRACTED_AFTER.
Batched by primary key of each source table (CLINICALPRESCRIPTIONID / PATIENTMEDICATIONID).

Usage:
    python medications_raleigh.py
"""

import csv
import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import pymysql
from tqdm import tqdm
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())

CSV_PATH   = os.path.join(os.path.dirname(os.path.abspath(__file__)), "incremental_medications_raleigh_results.csv")
CSV_HEADER = ["run_timestamp", "source", "status", "rows_inserted", "elapsed_secs"]
_csv_lock  = threading.Lock()


def append_csv(row: dict):
    file_exists = os.path.isfile(CSV_PATH)
    with _csv_lock:
        with open(CSV_PATH, "a", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_HEADER)
            if not file_exists:
                writer.writeheader()
            writer.writerow(row)


# ── Configuration ────────────────────────────────────────────────────
SCHEMA         = "dcnd"
EHR_SOURCE     = "athenaone"
PSID           = 10
EXTRACTED_AFTER = "2026-01-30"

DB_CONFIG = {
    "host":            os.environ.get("DB_INTERNAL_HOST"),
    "port":            3306,
    "user":            os.environ.get("DB_INTERNAL_USER"),
    "password":        os.environ.get("DB_INTERNAL_PASSWORD"),
    "database":        SCHEMA,
    "charset":         "utf8mb4",
    "connect_timeout": 30,
    "read_timeout":    21600,
    "write_timeout":   21600,
}

BATCH_SIZE       = 50_000
BATCH_WORKERS    = 4
TARGET_TABLE     = "incremental_staging.medications"
CHECKPOINT_TABLE = "incremental_staging.etl_checkpoint_medications_dcnd_n3"
BATCH_CKPT_TABLE = "incremental_staging.etl_batch_ckpt_medications_dcnd_n3"


# ── Target DDL ───────────────────────────────────────────────────────

TARGET_DDL = f"""
CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
    source                      VARCHAR(50),
    med_id                      BIGINT,
    ndid                        BIGINT,
    eid                         BIGINT,
    enc_date                    DATETIME,
    written_date                DATETIME,
    med_administered_datetime   DATETIME,
    doc_orderdatetime           DATETIME,
    med_start_date              DATETIME,
    med_end_date                DATETIME,
    med_createddatetime         DATETIME,
    doc_createddatetime         DATETIME,
    last_dispensed_date         DATETIME,
    sample_expiration_date      DATETIME,
    administer_expiration_date  DATETIME,
    earliest_fill_date          DATETIME,
    med_code                    VARCHAR(255),
    med_name                    VARCHAR(500),
    med_coding_system           VARCHAR(50),
    med_status                  VARCHAR(100),
    med_status_flag             VARCHAR(50),
    med_indication              VARCHAR(100),
    med_formulation             TEXT,
    med_route                   TEXT,
    med_strength                TEXT,
    med_strength_unit           VARCHAR(50),
    med_frequency               TEXT,
    med_pb_qty                  TEXT,
    med_days_supply             TEXT,
    med_refills                 TEXT,
    med_directions              TEXT,
    fill_date                   DATETIME,
    med_fill_type               VARCHAR(50),
    discont_date                DATE,
    discont_reason              VARCHAR(100),
    created_datetime            DATETIME,
    created_by                  VARCHAR(10),
    updated_datetime            DATETIME,
    updated_by                  VARCHAR(10),
    ehr_source_name             VARCHAR(50),
    source_path                 VARCHAR(50),
    data_type                   VARCHAR(20),
    psid                        INT,
    nd_extracted_date           DATE,
    enc_date_proxy              DATE,
    udm_unq_id                  VARCHAR(32),
    udm_unq_id_raw              TEXT,
    INDEX idx_ndid           (ndid),
    INDEX idx_eid            (eid),
    INDEX idx_enc_date_proxy (enc_date_proxy),
    INDEX idx_psid           (psid)
)
"""


# ── INSERT SQL: clinicalprescription ──────────────────────────────────

_CP_COLS = """(
    source, med_id, ndid, eid, enc_date,
    written_date, med_administered_datetime, doc_orderdatetime,
    med_start_date, med_end_date, med_createddatetime, doc_createddatetime,
    last_dispensed_date, sample_expiration_date, administer_expiration_date, earliest_fill_date,
    med_code, med_name, med_coding_system, med_status,
    med_formulation, med_route, med_strength, med_strength_unit, med_frequency,
    med_pb_qty, med_days_supply, med_refills, med_directions,
    fill_date, med_fill_type,
    created_datetime, created_by, updated_datetime, updated_by,
    ehr_source_name, source_path, data_type, psid, nd_extracted_date,
    enc_date_proxy, udm_unq_id, udm_unq_id_raw
)"""

CP_INSERT_SQL = f"""
INSERT INTO {TARGET_TABLE} {_CP_COLS}
SELECT
    'clinicalprescription'                                      AS source,
    cp.CLINICALPRESCRIPTIONID                                   AS med_id,
    d.CHARTID                                                   AS ndid,
    ce.CLINICALENCOUNTERID                                      AS eid,
    ce.ENCOUNTERDATE                                            AS enc_date,
    cp.WRITTENDATEDATETIME                                      AS written_date,
    cp.MEDICATIONADMINISTEREDDATETIME                           AS med_administered_datetime,
    d.ORDERDATETIME                                             AS doc_orderdatetime,
    cp.STARTDATEDATETIME                                        AS med_start_date,
    cp.STOPDATEDATETIME                                         AS med_end_date,
    cp.CREATEDDATETIME                                          AS med_createddatetime,
    d.CREATEDDATETIME                                           AS doc_createddatetime,
    cp.LASTDISPENSEDDATEDATETIME                                AS last_dispensed_date,
    cp.SAMPLEEXPIRATIONDATEDATETIME                             AS sample_expiration_date,
    cp.ADMINISTEREXPIRATIONDATEDATETIME                         AS administer_expiration_date,
    cp.EARLIESTFILLDATEDATETIME                                 AS earliest_fill_date,
    cp.NDC                                                      AS med_code,
    COALESCE(fndc.LN60, cp.LABELNAME,
             fdb.MED_MEDID_DESC, d.CLINICALORDERTYPE)           AS med_name,
    CASE WHEN cp.NDC IS NOT NULL THEN 'NDC' END                 AS med_coding_system,
    NULL                                                        AS med_status,
    cp.DOSAGEFORM                                               AS med_formulation,
    NULL                                                        AS med_route,
    cp.AVGDAILYDOSEQUANTITY                                     AS med_strength,
    cp.AVGDAILYDOSEUNIT                                         AS med_strength_unit,
    cp.FREQUENCY                                                AS med_frequency,
    cp.DOSAGEQUANTITY                                           AS med_pb_qty,
    cp.DURATION                                                 AS med_days_supply,
    cp.NUMBERREFILLSALLOWED                                     AS med_refills,
    cp.SIG                                                      AS med_directions,
    cp.LASTFILLDATEDATETIME                                     AS fill_date,
    NULL                                                        AS med_fill_type,
    CURRENT_TIMESTAMP()                                         AS created_datetime,
    'ND'                                                        AS created_by,
    CURRENT_TIMESTAMP()                                         AS updated_datetime,
    'ND'                                                        AS updated_by,
    '{EHR_SOURCE}'                                              AS ehr_source_name,
    'bronze_layer'                                              AS source_path,
    'Structured'                                                AS data_type,
    {PSID}                                                      AS psid,
    cp.nd_extracted_date                                        AS nd_extracted_date,

    /* enc_date_proxy — inline, clinicalprescription priority */
    COALESCE(
        DATE(ce.ENCOUNTERDATE),
        DATE(cp.MEDICATIONADMINISTEREDDATETIME),
        DATE(cp.LASTFILLDATEDATETIME),
        DATE(cp.STARTDATEDATETIME),
        DATE(cp.WRITTENDATEDATETIME),
        DATE(cp.CREATEDDATETIME),
        DATE(d.CREATEDDATETIME)
    )                                                           AS enc_date_proxy,

    /* udm_unq_id — MD5 hash */
    MD5(CONCAT_WS(':',
        COALESCE({PSID}, ''),
        COALESCE(d.CHARTID, ''),
        COALESCE(ce.CLINICALENCOUNTERID, ''),
        COALESCE(ce.ENCOUNTERDATE, ''),
        COALESCE(cp.STARTDATEDATETIME, ''),
        COALESCE(cp.STOPDATEDATETIME, ''),
        COALESCE(cp.NDC, ''),
        COALESCE(fndc.LN60, cp.LABELNAME, fdb.MED_MEDID_DESC, d.CLINICALORDERTYPE, '')
    ))                                                          AS udm_unq_id,

    /* udm_unq_id_raw — plain CONCAT_WS for debugging */
    CONCAT_WS(':',
        COALESCE({PSID}, ''),
        COALESCE(d.CHARTID, ''),
        COALESCE(ce.CLINICALENCOUNTERID, ''),
        COALESCE(ce.ENCOUNTERDATE, ''),
        COALESCE(cp.STARTDATEDATETIME, ''),
        COALESCE(cp.STOPDATEDATETIME, ''),
        COALESCE(cp.NDC, ''),
        COALESCE(fndc.LN60, cp.LABELNAME, fdb.MED_MEDID_DESC, d.CLINICALORDERTYPE, '')
    )                                                           AS udm_unq_id_raw

FROM {SCHEMA}.CLINICALPRESCRIPTION cp FORCE INDEX (idx_cp_active_date)
JOIN {SCHEMA}.DOCUMENT d
    ON  d.DOCUMENTID = cp.DOCUMENTID
    AND d.nd_active_flag = 'Y'
LEFT JOIN {SCHEMA}.CLINICALENCOUNTER ce
    ON  ce.CLINICALENCOUNTERID = d.CLINICALENCOUNTERID
    AND ce.nd_active_flag = 'Y'
LEFT JOIN {SCHEMA}.FDB_RNDC14 fndc
    ON  fndc.NDC = cp.NDC
    AND fndc.nd_active_flag = 'Y'
LEFT JOIN (
    SELECT MEDID, MED_MEDID_DESC
    FROM (
        SELECT MEDID, MED_MEDID_DESC,
               ROW_NUMBER() OVER (PARTITION BY MEDID ORDER BY LASTUPDATED DESC) AS rn
        FROM {SCHEMA}.FDB_RMIID1
        WHERE nd_active_flag = 'Y'
    ) ranked
    WHERE rn = 1
) fdb ON fdb.MEDID = d.FBDMEDID
WHERE cp.nd_active_flag = 'Y'
  AND cp.nd_extracted_date > '{EXTRACTED_AFTER}'
  AND cp.CLINICALPRESCRIPTIONID >= {{pk_lo}} AND cp.CLINICALPRESCRIPTIONID < {{pk_hi}}
"""


# ── INSERT SQL: patientmedication ─────────────────────────────────────

_PM_COLS = """(
    source, med_id, ndid, eid, enc_date,
    written_date, med_administered_datetime, doc_orderdatetime,
    med_start_date, med_end_date, med_createddatetime, doc_createddatetime,
    last_dispensed_date, sample_expiration_date, administer_expiration_date, earliest_fill_date,
    med_code, med_name, med_coding_system,
    med_status, med_status_flag, med_indication,
    med_formulation, med_route, med_strength, med_strength_unit, med_frequency,
    med_pb_qty, med_days_supply, med_refills, med_directions,
    fill_date, med_fill_type, discont_date, discont_reason,
    created_datetime, created_by,
    ehr_source_name, source_path, data_type, psid, nd_extracted_date,
    enc_date_proxy, udm_unq_id, udm_unq_id_raw
)"""

PM_INSERT_SQL = f"""
INSERT INTO {TARGET_TABLE} {_PM_COLS}
SELECT
    'patientmedication'                                         AS source,
    med.PATIENTMEDICATIONID                                     AS med_id,
    med.CHARTID                                                 AS ndid,
    ce.CLINICALENCOUNTERID                                      AS eid,
    ce.ENCOUNTERDATE                                            AS enc_date,
    NULL                                                        AS written_date,
    STR_TO_DATE(med.MEDADMINISTEREDDATETIME, '%Y-%m-%d %H:%i:%s') AS med_administered_datetime,
    STR_TO_DATE(doc.ORDERDATETIME,           '%Y-%m-%d %H:%i:%s') AS doc_orderdatetime,
    STR_TO_DATE(med.STARTDATE,               '%Y-%m-%d %H:%i:%s') AS med_start_date,
    STR_TO_DATE(med.STOPDATE,                '%Y-%m-%d %H:%i:%s') AS med_end_date,
    med.CREATEDDATETIME                                         AS med_createddatetime,
    doc.CREATEDDATETIME                                         AS doc_createddatetime,
    STR_TO_DATE(med.DISPENSEDEXPIRATIONDATE, '%Y-%m-%d %H:%i:%s') AS last_dispensed_date,
    NULL                                                        AS sample_expiration_date,
    STR_TO_DATE(med.ADMINISTEREDEXPIRATIONDATE, '%Y-%m-%d %H:%i:%s') AS administer_expiration_date,
    NULL                                                        AS earliest_fill_date,
    CASE WHEN LOWER(TRIM(med1.NDC)) = 'none' THEN NULL
         ELSE TRIM(med1.NDC)
    END                                                         AS med_code,
    COALESCE(
        NULLIF(TRIM(med.MEDICATIONNAME),  'none'),
        NULLIF(TRIM(med1.MEDICATIONNAME), 'none'),
        doc.CLINICALORDERTYPE
    )                                                           AS med_name,
    CASE WHEN med1.NDC IS NOT NULL THEN 'NDC' END               AS med_coding_system,
    CASE WHEN med.DEACTIVATIONDATETIME IS NULL
         THEN 'Active' ELSE 'Inactive'
    END                                                         AS med_status,
    ''                                                          AS med_status_flag,
    ''                                                          AS med_indication,
    TRIM(med.DOSAGEFORM)                                        AS med_formulation,
    TRIM(med.DOSAGEROUTE)                                       AS med_route,
    TRIM(med.DOSAGESTRENGTH)                                    AS med_strength,
    TRIM(med.DOSAGESTRENGTHUNITS)                               AS med_strength_unit,
    TRIM(med.FREQUENCY)                                         AS med_frequency,
    med.PRESCRIPTIONFILLQUANTITY                                AS med_pb_qty,
    med.LENGTHOFCOURSE                                          AS med_days_supply,
    REPLACE(med.NUMBEROFREFILLSPRESCRIBED, '.0', '')            AS med_refills,
    med.SIG                                                     AS med_directions,
    STR_TO_DATE(med.FILLDATE, '%Y-%m-%d %H:%i:%s')              AS fill_date,
    NULL                                                        AS med_fill_type,
    NULL                                                        AS discont_date,
    ''                                                          AS discont_reason,
    CURRENT_TIMESTAMP()                                         AS created_datetime,
    'ND'                                                        AS created_by,
    '{EHR_SOURCE}'                                              AS ehr_source_name,
    'bronze_layer'                                              AS source_path,
    'Structured'                                                AS data_type,
    {PSID}                                                      AS psid,
    med.nd_extracted_date                                       AS nd_extracted_date,

    /* enc_date_proxy — inline, patientmedication priority */
    COALESCE(
        DATE(ce.ENCOUNTERDATE),
        DATE(STR_TO_DATE(med.STARTDATE,                '%Y-%m-%d %H:%i:%s')),
        DATE(STR_TO_DATE(med.MEDADMINISTEREDDATETIME,  '%Y-%m-%d %H:%i:%s')),
        DATE(STR_TO_DATE(med.FILLDATE,                 '%Y-%m-%d %H:%i:%s')),
        DATE(med.CREATEDDATETIME),
        DATE(doc.CREATEDDATETIME)
    )                                                           AS enc_date_proxy,

    /* udm_unq_id — MD5 hash */
    MD5(CONCAT_WS(':',
        COALESCE({PSID}, ''),
        COALESCE(med.CHARTID, ''),
        COALESCE(ce.CLINICALENCOUNTERID, ''),
        COALESCE(ce.ENCOUNTERDATE, ''),
        COALESCE(STR_TO_DATE(med.STARTDATE, '%Y-%m-%d %H:%i:%s'), ''),
        COALESCE(STR_TO_DATE(med.STOPDATE,  '%Y-%m-%d %H:%i:%s'), ''),
        COALESCE(CASE WHEN LOWER(TRIM(med1.NDC)) = 'none' THEN NULL
                      ELSE TRIM(med1.NDC) END, ''),
        COALESCE(NULLIF(TRIM(med.MEDICATIONNAME),  'none'),
                 NULLIF(TRIM(med1.MEDICATIONNAME), 'none'),
                 doc.CLINICALORDERTYPE, '')
    ))                                                          AS udm_unq_id,

    /* udm_unq_id_raw — plain CONCAT_WS for debugging */
    CONCAT_WS(':',
        COALESCE({PSID}, ''),
        COALESCE(med.CHARTID, ''),
        COALESCE(ce.CLINICALENCOUNTERID, ''),
        COALESCE(ce.ENCOUNTERDATE, ''),
        COALESCE(STR_TO_DATE(med.STARTDATE, '%Y-%m-%d %H:%i:%s'), ''),
        COALESCE(STR_TO_DATE(med.STOPDATE,  '%Y-%m-%d %H:%i:%s'), ''),
        COALESCE(CASE WHEN LOWER(TRIM(med1.NDC)) = 'none' THEN NULL
                      ELSE TRIM(med1.NDC) END, ''),
        COALESCE(NULLIF(TRIM(med.MEDICATIONNAME),  'none'),
                 NULLIF(TRIM(med1.MEDICATIONNAME), 'none'),
                 doc.CLINICALORDERTYPE, '')
    )                                                           AS udm_unq_id_raw

FROM {SCHEMA}.PATIENTMEDICATION med
LEFT JOIN {SCHEMA}.MEDICATION med1
    ON  med1.MEDICATIONID = med.MEDICATIONID
    AND med1.nd_active_flag = 'Y'
LEFT JOIN {SCHEMA}.DOCUMENT doc
    ON  doc.DOCUMENTID = med.DOCUMENTID
    AND doc.nd_active_flag = 'Y'
LEFT JOIN {SCHEMA}.CLINICALENCOUNTER ce
    ON  ce.CLINICALENCOUNTERID = doc.CLINICALENCOUNTERID
    AND ce.nd_active_flag = 'Y'
WHERE med.nd_active_flag = 'Y'
  AND med.nd_extracted_date > '{EXTRACTED_AFTER}'
  AND med.PATIENTMEDICATIONID >= {{pk_lo}} AND med.PATIENTMEDICATIONID < {{pk_hi}}
"""


# ── Source definitions ────────────────────────────────────────────────
SOURCES = [
    {
        "name":      "clinicalprescription",
        "ckpt":      "raleigh.inc.medications.clinicalprescription",
        "pk_table":  f"{SCHEMA}.CLINICALPRESCRIPTION",
        "pk_col":    "CLINICALPRESCRIPTIONID",
        "pk_filter": f"nd_active_flag = 'Y' AND nd_extracted_date > '{EXTRACTED_AFTER}'",
        "insert":    CP_INSERT_SQL,
    },
    {
        "name":      "patientmedication",
        "ckpt":      "raleigh.inc.medications.patientmedication",
        "pk_table":  f"{SCHEMA}.PATIENTMEDICATION",
        "pk_col":    "PATIENTMEDICATIONID",
        "pk_filter": f"nd_active_flag = 'Y' AND nd_extracted_date > '{EXTRACTED_AFTER}'",
        "insert":    PM_INSERT_SQL,
    },
]


# ── Join-column indexes to ensure before running ─────────────────────
# Format: (table, column_for_display, index_name, col_spec_for_ddl)
# col_spec_for_ddl differs from column only for BLOB/TEXT (needs prefix length)
REQUIRED_INDEXES = [
    ("DOCUMENT",             "DOCUMENTID",         "idx_doc_documentid",    "DOCUMENTID"),
    ("DOCUMENT",             "CLINICALENCOUNTERID", "idx_doc_ceid",          "CLINICALENCOUNTERID"),
    ("DOCUMENT",             "FBDMEDID",            "idx_doc_fbdmedid",      "FBDMEDID(255)"),
    ("CLINICALENCOUNTER",    "CLINICALENCOUNTERID", "idx_ce_ceid",           "CLINICALENCOUNTERID"),
    ("FDB_RNDC14",           "NDC",                "idx_fdb_rndc14_ndc",    "NDC(255)"),
    ("FDB_RMIID1",           "MEDID",              "idx_fdb_rmiid1_medid",  "MEDID"),
    ("MEDICATION",           "MEDICATIONID",       "idx_med_medicationid",  "MEDICATIONID"),
    ("PATIENTMEDICATION",    "nd_extracted_date",  "idx_pm_extracted_date", "nd_extracted_date"),
    ("CLINICALPRESCRIPTION", "nd_extracted_date",  "idx_cp_extracted_date", "nd_extracted_date"),
]


# ── Helpers ──────────────────────────────────────────────────────────

def get_connection():
    return pymysql.connect(**DB_CONFIG)


def ensure_indexes(conn):
    """Check and create missing indexes on all join/filter columns.
    Auto-retries with (255) prefix if MySQL raises 1170 (BLOB/TEXT without key length)."""
    cur = conn.cursor()
    print("  Checking/creating join-column indexes...")
    for table, column, idx_name, col_spec in REQUIRED_INDEXES:
        cur.execute("""
            SELECT COUNT(*) FROM information_schema.statistics
            WHERE table_schema = %s AND table_name = %s AND index_name = %s
        """, (SCHEMA, table, idx_name))
        if cur.fetchone()[0] == 0:
            print(f"    Creating {idx_name} on {table}({column})...")
            try:
                cur.execute(f"CREATE INDEX {idx_name} ON {SCHEMA}.{table} ({col_spec})")
            except pymysql.err.OperationalError as e:
                if e.args[0] == 1170:  # BLOB/TEXT needs prefix length
                    fallback = col_spec if "(" in col_spec else f"{col_spec}(255)"
                    cur.execute(f"CREATE INDEX {idx_name} ON {SCHEMA}.{table} ({fallback})")
                elif e.args[0] == 1089:  # numeric column; prefix length not allowed
                    bare = col_spec.split("(")[0]
                    cur.execute(f"CREATE INDEX {idx_name} ON {SCHEMA}.{table} ({bare})")
                else:
                    raise
            conn.commit()
            print(f"      created")
        else:
            print(f"    {idx_name} on {table}.{column} — exists")
    cur.close()


# ── Checkpoint ───────────────────────────────────────────────────────

def setup_checkpoint(conn):
    cur = conn.cursor()
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {CHECKPOINT_TABLE} (
            source_key   VARCHAR(150) NOT NULL PRIMARY KEY,
            status       ENUM('running','done','failed') NOT NULL DEFAULT 'running',
            started_at   DATETIME DEFAULT NULL,
            completed_at DATETIME DEFAULT NULL,
            error_msg    TEXT     DEFAULT NULL
        )
    """)
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {BATCH_CKPT_TABLE} (
            ckpt_key  VARCHAR(150) NOT NULL,
            pk_lo     BIGINT       NOT NULL,
            pk_hi     BIGINT       NOT NULL,
            PRIMARY KEY (ckpt_key, pk_lo)
        )
    """)
    conn.commit()
    cur.close()


def _get_completed_batches(conn, ckpt_key):
    cur = conn.cursor()
    cur.execute(
        f"SELECT pk_lo FROM {BATCH_CKPT_TABLE} WHERE ckpt_key = %s", (ckpt_key,)
    )
    completed = {row[0] for row in cur.fetchall()}
    cur.close()
    return completed


def is_done(conn, ckpt):
    cur = conn.cursor()
    cur.execute(
        f"SELECT status FROM {CHECKPOINT_TABLE} WHERE source_key = %s", (ckpt,)
    )
    row = cur.fetchone()
    cur.close()
    return row is not None and row[0] == "done"


def mark(conn, ckpt, status, error=None):
    cur = conn.cursor()
    cur.execute(f"""
        INSERT INTO {CHECKPOINT_TABLE}
            (source_key, status, started_at, completed_at, error_msg)
        VALUES (%s, %s, NOW(), IF(%s = 'done', NOW(), NULL), %s)
        ON DUPLICATE KEY UPDATE
            status        = VALUES(status),
            completed_at  = IF(VALUES(status) = 'done', NOW(), NULL),
            error_msg     = VALUES(error_msg)
    """, (ckpt, status, status, error))
    conn.commit()
    cur.close()


# ── Setup ────────────────────────────────────────────────────────────

def create_target_table(conn):
    cur = conn.cursor()
    cur.execute("SET SESSION lock_wait_timeout = 15")
    cur.execute(TARGET_DDL)
    cur.execute("SET SESSION lock_wait_timeout = 600")
    conn.commit()
    cur.close()


# ── Batch ranges ─────────────────────────────────────────────────────

def get_batch_ranges(conn, src):
    pk_table  = src["pk_table"]
    pk_col    = src["pk_col"]
    pk_filter = src["pk_filter"]

    cur = conn.cursor()
    cur.execute(f"SELECT COUNT(*) FROM {pk_table} WHERE {pk_filter}")
    total = cur.fetchone()[0]
    print(f"    {src['name']}: {total:,} rows")

    if total == 0:
        cur.close()
        return []

    cur.execute(f"""
        SELECT {pk_col}
        FROM (
            SELECT {pk_col},
                   ROW_NUMBER() OVER (ORDER BY {pk_col}) AS rn
            FROM {pk_table}
            WHERE {pk_filter}
        ) t
        WHERE (rn - 1) % {BATCH_SIZE} = 0
        ORDER BY {pk_col}
    """)
    boundaries = [row[0] for row in cur.fetchall() if row[0] is not None]

    cur.execute(f"SELECT MAX({pk_col}) FROM {pk_table} WHERE {pk_filter}")
    max_pk = int(cur.fetchone()[0])
    cur.close()

    ranges = []
    for i, lo in enumerate(boundaries):
        hi = boundaries[i + 1] if i + 1 < len(boundaries) else max_pk + 1
        ranges.append((lo, hi))

    print(f"    → {len(ranges)} batches of ~{BATCH_SIZE:,} each")
    return ranges


# ── Batch worker ─────────────────────────────────────────────────────

def _batch_worker(insert_template, pk_lo, pk_hi, ckpt_key):
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("SET unique_checks = 0")
        cur.execute("SET foreign_key_checks = 0")
        cur.execute("SET innodb_lock_wait_timeout = 600")
        sql = insert_template.format(pk_lo=pk_lo, pk_hi=pk_hi)
        cur.execute(sql)
        rows = cur.rowcount
        cur.execute(
            f"INSERT IGNORE INTO {BATCH_CKPT_TABLE} (ckpt_key, pk_lo, pk_hi) VALUES (%s, %s, %s)",
            (ckpt_key, pk_lo, pk_hi),
        )
        conn.commit()
        cur.close()
        return rows
    finally:
        conn.close()


# ── Runner ───────────────────────────────────────────────────────────

def run_source(src, ranges, pbar_pos=0):
    name = src["name"]
    ckpt = src["ckpt"]

    conn = get_connection()

    if is_done(conn, ckpt):
        conn.close()
        conn2 = get_connection()
        cur = conn2.cursor()
        cur.execute(f"SELECT COUNT(*) FROM {TARGET_TABLE} WHERE source = %s AND psid = {PSID}", (name,))
        rows = cur.fetchone()[0]
        cur.close()
        conn2.close()
        print(f"    {name} — already done, skipping ({rows:,} rows)")
        return {"source": name, "status": "skipped", "rows": rows, "secs": 0}

    completed = _get_completed_batches(conn, ckpt)
    pending_ranges = [(lo, hi) for lo, hi in ranges if lo not in completed]

    if completed:
        conn_tmp = get_connection()
        cur = conn_tmp.cursor()
        cur.execute(f"SELECT COUNT(*) FROM {TARGET_TABLE} WHERE source = %s AND psid = {PSID}", (name,))
        already_rows = cur.fetchone()[0]
        cur.close()
        conn_tmp.close()
        print(
            f"    [{name}] resuming — {len(completed)} batches done "
            f"({already_rows:,} rows), {len(pending_ranges)} remaining"
        )
    else:
        already_rows = 0

    mark(conn, ckpt, "running")
    conn.close()

    t0 = time.time()
    total_rows = already_rows
    run_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    first_error = None

    try:
        with tqdm(
            total=len(ranges),
            desc=f"  {name}",
            unit="batch",
            position=pbar_pos,
            leave=True,
            initial=len(completed),
        ) as pbar:
            pbar.set_postfix(rows=f"{total_rows:,}")
            with ThreadPoolExecutor(max_workers=BATCH_WORKERS) as pool:
                futures = {
                    pool.submit(_batch_worker, src["insert"], lo, hi, ckpt): (lo, hi)
                    for lo, hi in pending_ranges
                }
                for future in as_completed(futures):
                    rows = future.result()
                    total_rows += rows
                    pbar.set_postfix(rows=f"{total_rows:,}")
                    pbar.update(1)

    except Exception as exc:
        first_error = exc

    elapsed = round(time.time() - t0, 1)
    conn = get_connection()

    if first_error is None:
        mark(conn, ckpt, "done")
        conn.close()
        append_csv({"run_timestamp": run_ts, "source": name,
                    "status": "done", "rows_inserted": total_rows, "elapsed_secs": elapsed})
        return {"source": name, "status": "done", "rows": total_rows, "secs": elapsed}
    else:
        print(f"\n  ERROR [{name}]: {first_error}", flush=True)
        mark(conn, ckpt, "failed", str(first_error))
        conn.close()
        append_csv({"run_timestamp": run_ts, "source": name,
                    "status": f"FAILED: {first_error}", "rows_inserted": total_rows,
                    "elapsed_secs": elapsed})
        return {"source": name, "status": f"FAILED: {first_error}",
                "rows": total_rows, "secs": elapsed}


# ── Main ─────────────────────────────────────────────────────────────

def main():
    print(f"\n{'='*70}", flush=True)
    print(f"  raleigh incremental medications — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  target         : {TARGET_TABLE}")
    print(f"  schema         : {SCHEMA}  |  psid: {PSID}")
    print(f"  sources        : {', '.join(s['name'] for s in SOURCES)}")
    print(f"  extracted_after: {EXTRACTED_AFTER}")
    print(f"  batch_size     : {BATCH_SIZE:,}  |  batch_workers: {BATCH_WORKERS} per source")
    print(f"  results csv    : {CSV_PATH}")
    print(f"{'='*70}\n", flush=True)

    conn = get_connection()
    setup_checkpoint(conn)

    print("\n  Creating target table (if not exist)...")
    create_target_table(conn)
    print(f"    {TARGET_TABLE} — ready")

    print()
    ensure_indexes(conn)
    conn.close()

    print("\n  Computing batch boundaries...")
    ranges_map = {}
    conn = get_connection()
    for src in SOURCES:
        ranges_map[src["name"]] = get_batch_ranges(conn, src)
    conn.close()

    print("\n  Inserting data (both sources in parallel)...\n")
    results = []
    with ThreadPoolExecutor(max_workers=len(SOURCES)) as pool:
        futures = {
            pool.submit(run_source, src, ranges_map[src["name"]], i): src
            for i, src in enumerate(SOURCES)
            if ranges_map[src["name"]]
        }
        for src in SOURCES:
            if not ranges_map[src["name"]]:
                print(f"    No rows found for {src['name']}. Skipping.")
                results.append({"source": src["name"], "status": "skipped", "rows": 0, "secs": 0})

        for future in as_completed(futures):
            results.append(future.result())

    print(f"\n\n{'='*70}")
    print(f"  Summary")
    print(f"{'='*70}")
    any_failed = False
    for r in sorted(results, key=lambda x: x["source"]):
        if r["status"] == "done":
            tag = " DONE"
        elif r["status"] == "skipped":
            tag = " SKIP"
        else:
            tag = " FAIL"
            any_failed = True
        print(f"  [{tag}] {r['source']:<42} {r['rows']:>10,} rows  ({r['secs']}s)")

    total = sum(r["rows"] for r in results)
    print(f"{'─'*70}")
    print(f"  {'TOTAL':<48} {total:>10,} rows")
    print(f"{'='*70}")
    print(f"\n  Results appended to: {CSV_PATH}")
    print(f"\n  Cleanup SQL:")
    print(f"    DROP TABLE IF EXISTS {CHECKPOINT_TABLE};")
    print(f"    DROP TABLE IF EXISTS {BATCH_CKPT_TABLE};")
    print(f"    -- DROP TABLE IF EXISTS {TARGET_TABLE};")

    if any_failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
