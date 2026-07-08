#!/usr/bin/env python3
"""
Optimized ETL for: udm_staging.medication_final
Source: AthenaOne

Sources (2 independent INSERT jobs via UNION ALL inside CTE):
  1. CLINICALPRESCRIPTION — nd_active_flag='Y'
                            INNER JOIN DOCUMENT + CHART
                            LEFT JOIN  CLINICALENCOUNTER + FDB_RNDC14 + FDB_RMIID1
  2. PATIENTMEDICATION    — nd_active_flag='Y'
                            INNER JOIN CHART
                            LEFT JOIN  MEDICATION + DOCUMENT + CLINICALENCOUNTER

CTE final SELECT inlined per branch — STR_TO_DATE conversions + enc_date COALESCE applied
directly in each batch INSERT (CTE not materialized separately).

Pre-materialized lookup tables (computed ONCE, reused across all batches):
  - staging.med_ao_doc_v1_{schema}     (DOCUMENT active, keyed on documentid)
  - staging.med_ao_chart_v1_{schema}   (CHART active, keyed on CHARTID)
  - staging.med_ao_ce_v1_{schema}      (CLINICALENCOUNTER active, keyed on CLINICALENCOUNTERID)
  - staging.med_ao_fdb_ndc_v1_{schema} (FDB_RNDC14 active, keyed on NDC)
  - staging.med_ao_fdb_med_v1          (FDB_RMIID1 latest-per-FDB_RMIID1ID, from athenaone schema)
  - staging.med_ao_med1_v1_{schema}    (MEDICATION active, REPLACE(medicationid,'.0','') pre-computed)

Optimizations:
- All repeated JOINs pre-materialized once
- FDB_RMIID1 ROW_NUMBER deduplication materialized once (not re-run per batch)
- Batching by actual PK values (sparse ID safe)
- ThreadPoolExecutor with 2 workers (one per branch)
- Checkpoint/resume per source
- Commit after every batch
- InnoDB checks disabled per-session for bulk speed
- tqdm progress bar

Usage:
    python opt_med_new_ao.py
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
FDB_SCHEMA    = SOURCE_SCHEMA   # schema that holds FDB_RMIID1 (same as SOURCE_SCHEMA on most instances)

DEST_TABLE       = "udm_staging.medication_final_raleigh_v3"
STAGING_DOC      = f"staging.med_ao_doc_v4_{SOURCE_SCHEMA}"
STAGING_CHART    = f"staging.med_ao_chart_v4_{SOURCE_SCHEMA}"
STAGING_CE       = f"staging.med_ao_ce_v4_{SOURCE_SCHEMA}"
STAGING_FDB_NDC  = f"staging.med_ao_fdb_ndc_v4_{SOURCE_SCHEMA}"
STAGING_FDB_MED  = f"staging.med_ao_fdb_med_v4_{SOURCE_SCHEMA}"
STAGING_MED1     = f"staging.med_ao_med1_v4_{SOURCE_SCHEMA}"
CHECKPOINT_TABLE = f"staging.etl_checkpoint_med_ao_v7_{SOURCE_SCHEMA}"

SOURCES = [
    {
        "key":        "clinicalprescription",
        "table":      "CLINICALPRESCRIPTION",
        "pk":         "CLINICALPRESCRIPTIONID",
        "pk_staging": f"staging.tmp_med_ao_cp_v1_{SOURCE_SCHEMA}",
    },
    {
        "key":        "patientmedication",
        "table":      "PATIENTMEDICATION",
        "pk":         "PATIENTMEDICATIONID",
        "pk_staging": f"staging.tmp_med_ao_pm_v1_{SOURCE_SCHEMA}",
    },
]


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


# ── Date helper ───────────────────────────────────────────────────────────────

def date_case(col):
    """Return a SQL CASE expression that safely converts a VARCHAR date column
    to DATE regardless of whether it's stored as 'YYYY-MM-DD HH:MM:SS',
    'YYYY-MM-DD', 'MM/DD/YYYY', or 'MM-DD-YYYY'."""
    return (
        f"CASE"
        f" WHEN {col} IS NULL OR {col} IN ('', 'None') THEN NULL"
        f" WHEN {col} REGEXP '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}} [0-9]{{2}}:[0-9]{{2}}:[0-9]{{2}}$'"
        f"   THEN DATE({col})"
        f" WHEN {col} REGEXP '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}$'"
        f"   THEN STR_TO_DATE({col}, '%Y-%m-%d')"
        f" WHEN {col} REGEXP '^[0-9]{{2}}/[0-9]{{2}}/[0-9]{{4}}$'"
        f"   THEN STR_TO_DATE({col}, '%m/%d/%Y')"
        f" WHEN {col} REGEXP '^[0-9]{{2}}-[0-9]{{2}}-[0-9]{{4}}$'"
        f"   THEN STR_TO_DATE({col}, '%m-%d-%Y')"
        f" ELSE NULL END"
    )


# ── Batch INSERT builders ──────────────────────────────────────────────────────

def _build_cp_insert(pk_lo, pk_hi):
    dc_written       = date_case('cp.WRITTENDATEDATETIME')
    dc_med_admin     = date_case('cp.MEDICATIONADMINISTEREDDATETIME')
    dc_order         = date_case('d.ORDERDATETIME')
    dc_start         = date_case('cp.STARTDATEDATETIME')
    dc_stop          = date_case('cp.STOPDATEDATETIME')
    dc_last_disp     = date_case('cp.LASTDISPENSEDDATEDATETIME')
    dc_sample_exp    = date_case('cp.SAMPLEEXPIRATIONDATEDATETIME')
    dc_adm_exp       = date_case('cp.ADMINISTEREXPIRATIONDATEDATETIME')
    dc_earliest_fill = date_case('cp.EARLIESTFILLDATEDATETIME')
    dc_fill          = date_case('cp.LASTFILLDATEDATETIME')
    # enc_date: psid IN (2,5,6,10), source='clinicalprescription'
    enc_date = (
        f"COALESCE(\n"
        f"        ce.ENCOUNTERDATE,\n"
        f"        {dc_med_admin},\n"
        f"        {dc_fill},\n"
        f"        {dc_start},\n"
        f"        {dc_written},\n"
        f"        cp.CREATEDDATETIME,\n"
        f"        d.CREATEDDATETIME\n"
        f"    )"
    )
    enc_date_proxy = enc_date  # same expression as enc_date in updated SQL
    return f"""
INSERT INTO {DEST_TABLE}
    (source, med_id, ndid, enc_date, eid,
     written_date, med_administered_datetime, doc_orderdatetime,
     med_start_date, med_end_date, med_createddatetime, doc_createddatetime,
     last_dispensed_date, sample_expiration_date, administer_expiration_date,
     earliest_fill_date, med_code, med_name, med_coding_system,
     med_status, med_status_flag, med_indication,
     med_formulation, med_route, med_strength, med_strength_unit,
     med_frequency, med_presc_quantity, med_days_supply, med_refills,
     med_directions, med_fill_date, med_fill_type,
     discont_date, discont_reason,
     created_datetime, created_by, updated_datetime, updated_by,
     ehr_source_name, source_path, data_type, psid, nd_extracted_date,
     udm_unq_id, enc_date_proxy)
SELECT DISTINCT
    'clinicalprescription',
    cp.CLINICALPRESCRIPTIONID,
    d.CHARTID,
    {enc_date},
    ce.CLINICALENCOUNTERID,
    {dc_written},
    {dc_med_admin},
    {dc_order},
    {dc_start},
    {dc_stop},
    cp.CREATEDDATETIME,
    d.CREATEDDATETIME,
    {dc_last_disp},
    {dc_sample_exp},
    {dc_adm_exp},
    {dc_earliest_fill},
    cp.NDC,
    COALESCE(fndc.LN60, cp.labelname, fdb.MED_MEDID_DESC, d.CLINICALORDERTYPE),
    CASE WHEN cp.NDC IS NOT NULL THEN 'NDC' ELSE NULL END,
    NULL,
    '',
    '',
    cp.DOSAGEFORM,
    NULL,
    cp.AVGDAILYDOSEQUANTITY,
    cp.AVGDAILYDOSEUNIT,
    cp.FREQUENCY,
    cp.DOSAGEQUANTITY,
    cp.DURATION,
    cp.NUMBERREFILLSALLOWED,
    cp.SIG,
    {dc_fill},
    NULL,
    NULL,
    '',
    CURRENT_TIMESTAMP(),
    'ND',
    CURRENT_TIMESTAMP(),
    'ND',
    'athenaone',
    'bronze_layer',
    'Structured',
    {PSID},
    cp.nd_extracted_date,
    MD5(CONCAT_WS(':',
        COALESCE({PSID},                  ''),
        COALESCE(d.CHARTID,               ''),
        COALESCE(ce.CLINICALENCOUNTERID,  ''),
        COALESCE(ce.ENCOUNTERDATE,        ''),
        COALESCE(cp.STARTDATEDATETIME,    ''),
        COALESCE(cp.STOPDATEDATETIME,     ''),
        COALESCE(cp.NDC,                  ''),
        COALESCE(COALESCE(fndc.LN60, cp.labelname, fdb.MED_MEDID_DESC, d.CLINICALORDERTYPE), ''),
        COALESCE(cp.CLINICALPRESCRIPTIONID, '')
    )),
    {enc_date_proxy}
FROM {SOURCE_SCHEMA}.CLINICALPRESCRIPTION cp
INNER JOIN {STAGING_DOC}     d    ON d.documentid           = cp.documentid
INNER JOIN {STAGING_CHART}   ch   ON ch.CHARTID             = d.CHARTID
LEFT  JOIN {STAGING_CE}      ce   ON ce.CLINICALENCOUNTERID  = d.clinicalencounterid
LEFT  JOIN {STAGING_FDB_NDC} fndc ON fndc.NDC               = cp.NDC
LEFT  JOIN {STAGING_FDB_MED} fdb  ON fdb.medid              = d.fbdmedid
WHERE cp.nd_active_flag = 'Y'
  AND cp.CLINICALPRESCRIPTIONID >= {pk_lo}
  AND cp.CLINICALPRESCRIPTIONID <  {pk_hi}
"""


def _build_pm_insert(pk_lo, pk_hi):
    dc_med_admin = date_case('MED.MEDADMINISTEREDDATETIME')
    dc_order     = date_case('doc.ORDERDATETIME')
    dc_start     = date_case('MED.startdate')
    dc_stop      = date_case('MED.stopdate')
    dc_disp_exp  = date_case('MED.DISPENSEDEXPIRATIONDATE')
    dc_adm_exp   = date_case('MED.ADMINISTEREDEXPIRATIONDATE')
    dc_fill      = date_case('MED.FILLDATE')
    # enc_date: psid IN (2,5,6,10), source='patientmedication'
    enc_date = (
        f"COALESCE(\n"
        f"        ce.ENCOUNTERDATE,\n"
        f"        {dc_start},\n"
        f"        {dc_med_admin},\n"
        f"        {dc_fill},\n"
        f"        MED.CREATEDDATETIME,\n"
        f"        doc.CREATEDDATETIME\n"
        f"    )"
    )
    enc_date_proxy = enc_date  # same expression as enc_date in updated SQL
    return f"""
INSERT INTO {DEST_TABLE}
    (source, med_id, ndid, enc_date, eid,
     written_date, med_administered_datetime, doc_orderdatetime,
     med_start_date, med_end_date, med_createddatetime, doc_createddatetime,
     last_dispensed_date, sample_expiration_date, administer_expiration_date,
     earliest_fill_date, med_code, med_name, med_coding_system,
     med_status, med_status_flag, med_indication,
     med_formulation, med_route, med_strength, med_strength_unit,
     med_frequency, med_presc_quantity, med_days_supply, med_refills,
     med_directions, med_fill_date, med_fill_type,
     discont_date, discont_reason,
     created_datetime, created_by, updated_datetime, updated_by,
     ehr_source_name, source_path, data_type, psid, nd_extracted_date,
     udm_unq_id, enc_date_proxy)
SELECT DISTINCT
    'patientmedication',
    MED.PATIENTMEDICATIONID,
    MED.CHARTID,
    {enc_date},
    ce.CLINICALENCOUNTERID,
    NULL,
    {dc_med_admin},
    {dc_order},
    {dc_start},
    {dc_stop},
    MED.CREATEDDATETIME,
    doc.CREATEDDATETIME,
    {dc_disp_exp},
    NULL,
    {dc_adm_exp},
    NULL,
    CASE
        WHEN LOWER(TRIM(med1.NDC)) = 'none' THEN NULL
        ELSE TRIM(med1.NDC)
    END,
    COALESCE(
        NULLIF(TRIM(MED.MEDICATIONNAME),  'none'),
        NULLIF(TRIM(med1.MEDICATIONNAME), 'none'),
        doc.CLINICALORDERTYPE
    ),
    CASE WHEN med1.NDC IS NOT NULL THEN 'NDC' ELSE NULL END,
    CASE
        WHEN MED.DEACTIVATIONDATETIME IS NULL THEN 'Active'
        ELSE 'Inactive'
    END,
    '',
    '',
    TRIM(MED.DOSAGEFORM),
    TRIM(MED.DOSAGEROUTE),
    TRIM(MED.DOSAGESTRENGTH),
    TRIM(MED.DOSAGESTRENGTHUNITS),
    TRIM(MED.FREQUENCY),
    MED.PRESCRIPTIONFILLQUANTITY,
    MED.LENGTHOFCOURSE,
    REPLACE(MED.NUMBEROFREFILLSPRESCRIBED, '.0', ''),
    MED.sig,
    {dc_fill},
    NULL,
    NULL,
    '',
    CURRENT_TIMESTAMP(),
    'ND',
    CURRENT_TIMESTAMP(),
    'ND',
    'athenaone',
    'bronze_layer',
    'Structured',
    {PSID},
    MED.nd_extracted_date,
    MD5(CONCAT_WS(':',
        COALESCE({PSID},                  ''),
        COALESCE(MED.CHARTID,             ''),
        COALESCE(ce.CLINICALENCOUNTERID,  ''),
        COALESCE(ce.ENCOUNTERDATE,        ''),
        COALESCE(MED.startdate,           ''),
        COALESCE(MED.stopdate,            ''),
        COALESCE(CASE WHEN LOWER(TRIM(med1.NDC)) = 'none' THEN NULL ELSE TRIM(med1.NDC) END, ''),
        COALESCE(COALESCE(NULLIF(TRIM(MED.MEDICATIONNAME), 'none'), NULLIF(TRIM(med1.MEDICATIONNAME), 'none'), doc.CLINICALORDERTYPE), ''),
        COALESCE(MED.PATIENTMEDICATIONID, '')
    )),
    {enc_date_proxy}
FROM {SOURCE_SCHEMA}.PATIENTMEDICATION MED
LEFT  JOIN {STAGING_MED1}  med1 ON med1.med_id_clean        = REPLACE(MED.medicationid, '.0', '')
LEFT  JOIN {STAGING_DOC}   doc  ON doc.documentid           = MED.DOCUMENTID
INNER JOIN {STAGING_CHART} ch   ON ch.CHARTID               = MED.CHARTID
LEFT  JOIN {STAGING_CE}    ce   ON ce.CLINICALENCOUNTERID    = doc.CLINICALENCOUNTERID
WHERE MED.nd_active_flag = 'Y'
  AND MED.PATIENTMEDICATIONID >= {pk_lo}
  AND MED.PATIENTMEDICATIONID <  {pk_hi}
"""


def build_batch_insert(source, pk_lo, pk_hi):
    if source["key"] == "clinicalprescription":
        return _build_cp_insert(pk_lo, pk_hi)
    return _build_pm_insert(pk_lo, pk_hi)


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

    # ── 1. Indexes on source join/filter columns ──────────────────────
    print("  Ensuring indexes on source tables...")
    _ensure_index(cur, conn, f"{SOURCE_SCHEMA}.CLINICALPRESCRIPTION",
                  "idx_documentid",  ["documentid"])
    _ensure_index(cur, conn, f"{SOURCE_SCHEMA}.CLINICALPRESCRIPTION",
                  "idx_active_flag", ["nd_active_flag"])
    _ensure_index(cur, conn, f"{SOURCE_SCHEMA}.PATIENTMEDICATION",
                  "idx_documentid",  ["DOCUMENTID"])
    _ensure_index(cur, conn, f"{SOURCE_SCHEMA}.PATIENTMEDICATION",
                  "idx_chartid",     ["CHARTID"])
    _ensure_index(cur, conn, f"{SOURCE_SCHEMA}.PATIENTMEDICATION",
                  "idx_active_flag", ["nd_active_flag"])

    # ── 2. STAGING_DOC: DOCUMENT filtered active ──────────────────────
    print("  Materializing DOCUMENT lookup (nd_active_flag='Y')...")
    if not _table_exists(cur, STAGING_DOC):
        cur.execute(f"""
            CREATE TABLE {STAGING_DOC} AS
            SELECT documentid, CHARTID, ORDERDATETIME, CREATEDDATETIME,
                   CLINICALORDERTYPE, clinicalencounterid, fbdmedid
            FROM {SOURCE_SCHEMA}.DOCUMENT
            WHERE nd_active_flag = 'Y'
        """)
        cur.execute(f"ALTER TABLE {STAGING_DOC} ADD INDEX idx_docid (documentid)")
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")
    cur.execute(f"SELECT COUNT(*) FROM {STAGING_DOC}")
    print(f"    {cur.fetchone()[0]:,} document rows")

    # ── 3. STAGING_CHART: CHART filtered active ───────────────────────
    print("  Materializing CHART lookup (nd_active_flag='Y')...")
    if not _table_exists(cur, STAGING_CHART):
        cur.execute(f"""
            CREATE TABLE {STAGING_CHART} AS
            SELECT CHARTID, ENTERPRISEID
            FROM {SOURCE_SCHEMA}.CHART
            WHERE nd_active_flag = 'Y'
        """)
        cur.execute(f"ALTER TABLE {STAGING_CHART} ADD INDEX idx_chartid (CHARTID)")
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")
    cur.execute(f"SELECT COUNT(*) FROM {STAGING_CHART}")
    print(f"    {cur.fetchone()[0]:,} chart rows")

    # ── 4. STAGING_CE: CLINICALENCOUNTER filtered active ─────────────
    # ENCOUNTERDATE is converted here (not in batch INSERT) so VARCHAR formats
    # like 'MM-DD-YYYY' are normalised once and never land as 0000-00-00.
    print("  Materializing CLINICALENCOUNTER lookup (nd_active_flag='Y')...")
    if not _table_exists(cur, STAGING_CE):
        dc_enc = date_case('ENCOUNTERDATE')
        cur.execute(f"""
            CREATE TABLE {STAGING_CE} AS
            SELECT CLINICALENCOUNTERID,
                   {dc_enc} AS ENCOUNTERDATE
            FROM {SOURCE_SCHEMA}.CLINICALENCOUNTER
            WHERE nd_active_flag = 'Y'
        """)
        cur.execute(f"ALTER TABLE {STAGING_CE} ADD INDEX idx_ceid (CLINICALENCOUNTERID)")
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")
    cur.execute(f"SELECT COUNT(*) FROM {STAGING_CE}")
    print(f"    {cur.fetchone()[0]:,} clinical encounter rows")

    # ── 5. STAGING_FDB_NDC: FDB_RNDC14 filtered active ───────────────
    print("  Materializing FDB_RNDC14 lookup (nd_active_flag='Y')...")
    if not _table_exists(cur, STAGING_FDB_NDC):
        cur.execute(f"""
            CREATE TABLE {STAGING_FDB_NDC} AS
            SELECT NDC, LN60
            FROM {SOURCE_SCHEMA}.FDB_RNDC14
            WHERE nd_active_flag = 'Y'
        """)
        cur.execute(f"ALTER TABLE {STAGING_FDB_NDC} ADD INDEX idx_ndc (NDC(50))")
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")
    cur.execute(f"SELECT COUNT(*) FROM {STAGING_FDB_NDC}")
    print(f"    {cur.fetchone()[0]:,} FDB_RNDC14 rows")

    # ── 6. STAGING_FDB_MED: FDB_RMIID1 deduped (latest per FDB_RMIID1ID) ─
    # Source is athenaone.FDB_RMIID1 (global reference, not per SOURCE_SCHEMA).
    # ROW_NUMBER dedup materialized once — avoids re-running the window function per batch.
    print(f"  Materializing FDB_RMIID1 lookup (latest per FDB_RMIID1ID, {FDB_SCHEMA} schema)...")
    if not _table_exists(cur, STAGING_FDB_MED):
        cur.execute(f"""
            CREATE TABLE {STAGING_FDB_MED} AS
            SELECT medid, MED_MEDID_DESC
            FROM (
                SELECT medid, MED_MEDID_DESC,
                       ROW_NUMBER() OVER (
                           PARTITION BY FDB_RMIID1ID
                           ORDER BY LASTUPDATED DESC
                       ) AS rn
                FROM {FDB_SCHEMA}.FDB_RMIID1
            ) x
            WHERE rn = 1
        """)
        cur.execute(f"ALTER TABLE {STAGING_FDB_MED} ADD INDEX idx_medid (medid)")
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")
    cur.execute(f"SELECT COUNT(*) FROM {STAGING_FDB_MED}")
    print(f"    {cur.fetchone()[0]:,} FDB_RMIID1 rows (deduped)")

    # ── 7. STAGING_MED1: MEDICATION active with cleaned medicationid ──
    # REPLACE(medicationid, '.0', '') pre-computed so the JOIN avoids per-row REPLACE on staging.
    print("  Materializing MEDICATION lookup (nd_active_flag='Y', cleaned medicationid)...")
    if not _table_exists(cur, STAGING_MED1):
        cur.execute(f"""
            CREATE TABLE {STAGING_MED1} AS
            SELECT CAST(REPLACE(medicationid, '.0', '') AS CHAR(100)) AS med_id_clean,
                   MEDICATIONNAME, NDC
            FROM {SOURCE_SCHEMA}.MEDICATION
            WHERE nd_active_flag = 'Y'
        """)
        cur.execute(f"ALTER TABLE {STAGING_MED1} ADD INDEX idx_medid (med_id_clean)")
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")
    cur.execute(f"SELECT COUNT(*) FROM {STAGING_MED1}")
    print(f"    {cur.fetchone()[0]:,} MEDICATION rows")

    # ── 8. Destination table ──────────────────────────────────────────
    print("  Creating destination table...")
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {DEST_TABLE} (
            source                     VARCHAR(50)   DEFAULT NULL,
            med_id                     BIGINT        DEFAULT NULL,
            ndid                       BIGINT        DEFAULT NULL,
            enc_date                   DATE          DEFAULT NULL,
            eid                        BIGINT        DEFAULT NULL,
            written_date               DATE          DEFAULT NULL,
            med_administered_datetime  DATE          DEFAULT NULL,
            doc_orderdatetime          DATE          DEFAULT NULL,
            med_start_date             DATE          DEFAULT NULL,
            med_end_date               DATE          DEFAULT NULL,
            med_createddatetime        DATETIME      DEFAULT NULL,
            doc_createddatetime        DATETIME      DEFAULT NULL,
            last_dispensed_date        DATE          DEFAULT NULL,
            sample_expiration_date     DATE          DEFAULT NULL,
            administer_expiration_date DATE          DEFAULT NULL,
            earliest_fill_date         DATE          DEFAULT NULL,
            med_code                   VARCHAR(100)  DEFAULT NULL,
            med_name                   TEXT,
            med_coding_system          VARCHAR(20)   DEFAULT NULL,
            med_status                 VARCHAR(50)   DEFAULT NULL,
            med_status_flag            VARCHAR(50)   DEFAULT NULL,
            med_indication             VARCHAR(200)  DEFAULT NULL,
            med_formulation            VARCHAR(200)  DEFAULT NULL,
            med_route                  VARCHAR(200)  DEFAULT NULL,
            med_strength               VARCHAR(200)  DEFAULT NULL,
            med_strength_unit          VARCHAR(200)  DEFAULT NULL,
            med_frequency              VARCHAR(200)  DEFAULT NULL,
            med_presc_quantity         VARCHAR(100)  DEFAULT NULL,
            med_days_supply            VARCHAR(100)  DEFAULT NULL,
            med_refills                VARCHAR(50)   DEFAULT NULL,
            med_directions             TEXT,
            med_fill_date              DATE          DEFAULT NULL,
            med_fill_type              VARCHAR(100)  DEFAULT NULL,
            discont_date               DATE          DEFAULT NULL,
            discont_reason             VARCHAR(200)  DEFAULT NULL,
            created_datetime           DATETIME      DEFAULT NULL,
            created_by                 VARCHAR(50)   DEFAULT NULL,
            updated_datetime           DATETIME      DEFAULT NULL,
            updated_by                 VARCHAR(50)   DEFAULT NULL,
            ehr_source_name            VARCHAR(100)  DEFAULT NULL,
            source_path                VARCHAR(100)  DEFAULT NULL,
            data_type                  VARCHAR(50)   DEFAULT NULL,
            psid                       INT           DEFAULT NULL,
            nd_extracted_date          DATE          DEFAULT NULL,
            udm_unq_id                 VARCHAR(32)   DEFAULT NULL,
            enc_date_proxy             DATE          DEFAULT NULL
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
    """)
    conn.commit()
    print("    ready")

    # ── 9. Checkpoint table ───────────────────────────────────────────
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

    # ── 10. PK staging per source ─────────────────────────────────────
    source_ranges = {}
    for src in SOURCES:
        pk      = src["pk"]
        table   = src["table"]
        staging = src["pk_staging"]
        print(f"  Building PK staging for {table}...")

        if not _table_exists(cur, staging):
            cur.execute(f"""
                CREATE TABLE {staging} AS
                SELECT {pk}
                FROM {SOURCE_SCHEMA}.{table}
                WHERE {pk} IS NOT NULL
                  AND nd_active_flag = 'Y'
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
    print(f"  AthenaOne Medication ETL — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  source     : {SOURCE_SCHEMA}.CLINICALPRESCRIPTION + PATIENTMEDICATION  (psid={PSID})")
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
    print(f"    DROP TABLE IF EXISTS {STAGING_DOC};")
    print(f"    DROP TABLE IF EXISTS {STAGING_CHART};")
    print(f"    DROP TABLE IF EXISTS {STAGING_CE};")
    print(f"    DROP TABLE IF EXISTS {STAGING_FDB_NDC};")
    print(f"    DROP TABLE IF EXISTS {STAGING_FDB_MED};")
    print(f"    DROP TABLE IF EXISTS {STAGING_MED1};")
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
