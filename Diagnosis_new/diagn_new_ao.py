#!/usr/bin/env python3
"""
diag.py — Optimized ETL: INSERT into staging.diagnoses_athenaone
          from 4 AthenaOne sources, 7-branch UNION ALL (diagnoses)

SQL bugs fixed from diag_temp.sql:
  1. Semicolon inside branch-1 WHERE clause removed
  2. Branch 2 missing snomed_desc column — NULL added
  3. Branch 4 missing comma before nd_extracted_date — fixed
  4. Branch 7 missing comma before rad.CREATEDDATETIME — fixed
  5. Outer query referenced 'enc_date' but inner alias was 'encounter_date' — fixed

v1 design (two-phase):
  Phase 1 — Pre-materialize 7-branch UNION ALL once per source into
             staging.tmp_diag_ao_{key}_v1  (slow, runs once, checkpointed)
  Phase 2 — Batch INSERT from pre-mat via pk range scan (fast, no JOINs)

Sources:
  dcnd           psid=10   schema: dcnd
  tng_athena_one psid=2    schema: tng_athena_one
  raleigh        psid=5    schema: raleigh
  tncpa          psid=6    schema: tncpa

Reset commands (full re-run):
  TRUNCATE TABLE staging.diagnoses_athenaone;
  DROP TABLE IF EXISTS staging.etl_checkpoint_diag_ao_v1;
  DROP TABLE IF EXISTS staging.tmp_diag_ao_dcnd_v1;
  DROP TABLE IF EXISTS staging.tmp_diag_ao_tng_athena_one_v1;
  DROP TABLE IF EXISTS staging.tmp_diag_ao_raleigh_v1;
  DROP TABLE IF EXISTS staging.tmp_diag_ao_tncpa_v1;
"""

import sys
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import pymysql
from tqdm import tqdm

sys.path.insert(0, __import__("pathlib").Path(__file__).resolve().parent.parent.__str__())
from config_aws import DB_CONFIG  # noqa: E402

# ── Configuration ─────────────────────────────────────────────────────────────
BATCH_SIZE  = 100_000
MAX_WORKERS = 4

DEST_TABLE       = "staging.diagnoses_athenaone"
CHECKPOINT_TABLE = "staging.etl_checkpoint_diag_ao_v1"
ND_DATE_FILTER   = ">= '2000-01-01'"

SOURCES = [
    {"key": "dcnd",           "schema": "dcnd",           "psid": 10},
    {"key": "tng_athena_one", "schema": "tng_athena_one",  "psid": 2},
    {"key": "raleigh",        "schema": "raleigh",         "psid": 5},
    {"key": "tncpa",          "schema": "tncpa",           "psid": 6},
]


def _premat(key): return f"staging.tmp_diag_ao_{key}_v1"


# ── Date-cast helper (VARCHAR → DATE, 2 AthenaOne formats) ───────────────────
def _dc(col: str) -> str:
    return (
        f"CASE\n"
        f"             WHEN {col} IS NULL OR {col} IN ('None', '') THEN NULL\n"
        f"             WHEN {col} REGEXP '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}$'"
        f" THEN STR_TO_DATE({col}, '%Y-%m-%d')\n"
        f"             WHEN {col} REGEXP '^[0-9]{{2}}-[0-9]{{2}}-[0-9]{{4}}$'"
        f" THEN STR_TO_DATE({col}, '%m-%d-%Y')\n"
        f"             ELSE NULL END"
    )


# ── INSERT column list ────────────────────────────────────────────────────────
_INSERT_COLS = (
    "diag_id, ndid, eid, enc_date, diag_date, diag_code, diag_desc, "
    "diag_coding_system, diag_code_stripped, primary_diagnosis_flag, "
    "parent_diag_code, parent_diag_desc, icd_codeset, icd_codeset_desc, "
    "icd_codeset_group, icd_codeset_system, snomed_code, snomed_desc, "
    "diag_severity, diag_status, diag_end_date, provisional_diag_flag, "
    "differential_diag_flag, comments_notes, diag_category, diag_risk, "
    "work_up_status, created_datetime, created_by, updated_datetime, "
    "updated_by, ehr_source_name, source_path, data_type, psid, "
    "nd_extracted_date, enc_date_proxy, udm_unq_id"
)


# ── Pre-mat SQL: full 7-branch UNION ALL, all transforms applied once ─────────
def _build_premat_sql(src: dict) -> str:
    schema = src["schema"]
    psid   = src["psid"]
    premat = _premat(src["key"])
    return f"""
CREATE TABLE {premat} AS
SELECT
    ROW_NUMBER() OVER ()    AS pk,
    t.diag_id,
    t.ndid,
    t.eid,
    t.enc_date,
    t.diag_date,
    t.diag_code,
    t.diag_desc,
    t.diag_coding_system,
    t.diag_code_stripped,
    t.primary_diagnosis_flag,
    t.parent_diag_code,
    t.parent_diag_desc,
    t.icd_codeset,
    t.icd_codeset_desc,
    t.icd_codeset_group,
    t.icd_codeset_system,
    t.snomed_code,
    t.snomed_desc,
    t.diag_severity,
    t.diag_status,
    t.diag_end_date,
    t.provisional_diag_flag,
    t.differential_diag_flag,
    t.comments_notes,
    t.diag_category,
    t.diag_risk,
    t.work_up_status,
    t.created_datetime,
    t.created_by,
    t.updated_datetime,
    t.updated_by,
    t.ehr_source_name,
    t.source_path,
    t.data_type,
    t.psid,
    t.nd_extracted_date,
    COALESCE(t.enc_date, t.diag_date) AS enc_date_proxy,
    MD5(CONCAT_WS(':',
        COALESCE(t.psid,                   ''),
        COALESCE(t.ndid,                   ''),
        COALESCE(t.eid,                    ''),
        COALESCE(t.enc_date,               ''),
        COALESCE(t.diag_date,              ''),
        COALESCE(t.diag_id,                ''),
        COALESCE(t.diag_code,              ''),
        COALESCE(t.diag_desc,              ''),
        COALESCE(t.snomed_code,            ''),
        COALESCE(t.snomed_desc,            ''),
        COALESCE(t.primary_diagnosis_flag, '')
    )) AS udm_unq_id
FROM (
    SELECT
        dx.source_dx_id                         AS diag_id,
        dx.patient_id                           AS ndid,
        dx.clinical_encounter_id                AS eid,
        {_dc('dx.encounter_date')}              AS enc_date,
        {_dc('dx.dx_created_date')}             AS diag_date,
        dx.icd_code                             AS diag_code,
        dx.icd_description                      AS diag_desc,
        dx.icd_codeset                          AS diag_coding_system,
        dx.icd_code                             AS diag_code_stripped,
        dx.dx_ordering                          AS primary_diagnosis_flag,
        NULL                                    AS parent_diag_code,
        NULL                                    AS parent_diag_desc,
        NULL                                    AS icd_codeset,
        NULL                                    AS icd_codeset_desc,
        NULL                                    AS icd_codeset_group,
        NULL                                    AS icd_codeset_system,
        dx.snomed_code                          AS snomed_code,
        dx.snomed_desc                          AS snomed_desc,
        ''                                      AS diag_severity,
        dx.dx_status                            AS diag_status,
        NULL                                    AS diag_end_date,
        NULL                                    AS provisional_diag_flag,
        NULL                                    AS differential_diag_flag,
        dx.dx_note                              AS comments_notes,
        NULL                                    AS diag_category,
        NULL                                    AS diag_risk,
        NULL                                    AS work_up_status,
        CURRENT_TIMESTAMP()                     AS created_datetime,
        'ND'                                    AS created_by,
        CURRENT_TIMESTAMP()                     AS updated_datetime,
        'ND'                                    AS updated_by,
        'athenaone'                             AS ehr_source_name,
        'bronze_layer'                          AS source_path,
        'Structured'                            AS data_type,
        {psid}                                  AS psid,
        dx.nd_extracted_date                    AS nd_extracted_date
    FROM (
        -- ── 1. CLINICAL_ENCOUNTER_DX_DIRECT ─────────────────────────
        SELECT
            'CLINICAL_ENCOUNTER_DX_DIRECT'      AS dx_source,
            p.ENTERPRISEID                      AS patient_id,
            ce.CLINICALENCOUNTERID              AS clinical_encounter_id,
            ce.ENCOUNTERDATE                    AS encounter_date,
            ced.CLINICALENCOUNTERDXID           AS source_dx_id,
            ced.DIAGNOSISCODE                   AS icd_code,
            'DIRECT'                            AS icd_codeset,
            NULL                                AS icd_description,
            ced.ORDERING                        AS dx_ordering,
            ced.STATUS                          AS dx_status,
            ced.NOTE                            AS dx_note,
            CAST(ced.SNOMEDCODE AS CHAR)        AS snomed_code,
            CAST(dc.DESCRIPTION AS CHAR)        AS snomed_desc,
            ced.CREATEDDATETIME                 AS dx_created_date,
            ced.nd_extracted_date               AS nd_extracted_date
        FROM {schema}.CLINICALENCOUNTERDIAGNOSIS ced
        JOIN {schema}.CLINICALENCOUNTER ce
            ON  ced.CLINICALENCOUNTERID = ce.CLINICALENCOUNTERID
            AND ced.CONTEXTID           = ce.CONTEXTID
            AND ce.nd_active_flag       = 'Y'
        JOIN {schema}.PATIENT p
            ON  ce.CHARTID              = p.ENTERPRISEID
            AND ce.CONTEXTID            = p.CONTEXTID
            AND p.nd_active_flag        = 'Y'
        LEFT JOIN {schema}.SNOMED dc
            ON  ced.SNOMEDCODE          = dc.SNOMEDCODE
            AND ced.CONTEXTID           = dc.CONTEXTID
            AND dc.nd_active_flag       = 'Y'
        WHERE ced.nd_active_flag   = 'Y'
          AND ced.nd_extracted_date {ND_DATE_FILTER}

        UNION ALL

        -- ── 2. CLINICAL_ENCOUNTER_DX_ICD10 ──────────────────────────
        SELECT
            'CLINICAL_ENCOUNTER_DX_ICD10',
            p.CHARTID,
            ce.CLINICALENCOUNTERID,
            ce.ENCOUNTERDATE,
            dxicd10.CLINICALENCOUNTERDXICD10ID,
            icd.DIAGNOSISCODE,
            'ICD10',
            icd.DIAGNOSISCODEDESCRIPTION,
            dxicd10.ORDERING,
            ced.STATUS,
            ced.NOTE,
            CAST(ced.SNOMEDCODE AS CHAR),
            NULL,
            dxicd10.CREATEDDATETIME,
            ced.nd_extracted_date
        FROM {schema}.CLINICALENCOUNTERDIAGNOSIS ced
        JOIN {schema}.CLINICALENCOUNTERDXICD10 dxicd10
            ON  dxicd10.CLINICALENCOUNTERDXID = ced.CLINICALENCOUNTERDXID
            AND dxicd10.CONTEXTID              = ced.CONTEXTID
            AND dxicd10.DELETEDDATETIME       IS NULL
            AND dxicd10.nd_active_flag         = 'Y'
        JOIN {schema}.CLINICALENCOUNTER ce
            ON  ced.CLINICALENCOUNTERID = ce.CLINICALENCOUNTERID
            AND ced.CONTEXTID           = ce.CONTEXTID
            AND ce.nd_active_flag       = 'Y'
        JOIN {schema}.PATIENT p
            ON  ce.CHARTID              = p.CHARTID
            AND ce.CONTEXTID            = p.CONTEXTID
            AND p.nd_active_flag        = 'Y'
        LEFT JOIN {schema}.ICDCODEALL icd
            ON  dxicd10.ICDCODEID  = icd.ICDCODEID
            AND dxicd10.CONTEXTID  = icd.CONTEXTID
            AND icd.nd_active_flag = 'Y'
        WHERE ced.DELETEDDATETIME  IS NULL
          AND ced.nd_active_flag    = 'Y'
          AND ced.nd_extracted_date {ND_DATE_FILTER}

        UNION ALL

        -- ── 3. CLINICAL_ENCOUNTER_DX_ICD9 ───────────────────────────
        SELECT
            'CLINICAL_ENCOUNTER_DX_ICD9',
            p.ENTERPRISEID,
            ce.CLINICALENCOUNTERID,
            ce.ENCOUNTERDATE,
            dxicd10.CLINICALENCOUNTERDXICD10ID,
            icd.DIAGNOSISCODE,
            'ICD10',
            icd.DIAGNOSISCODEDESCRIPTION,
            dxicd10.ORDERING,
            ced.STATUS,
            ced.NOTE,
            CAST(ced.SNOMEDCODE AS CHAR),
            CAST(dc.DESCRIPTION AS CHAR),
            dxicd10.CREATEDDATETIME,
            ced.nd_extracted_date
        FROM {schema}.CLINICALENCOUNTERDIAGNOSIS ced
        JOIN {schema}.CLINICALENCOUNTERDXICD10 dxicd10
            ON  dxicd10.CLINICALENCOUNTERDXID = ced.CLINICALENCOUNTERDXID
            AND dxicd10.CONTEXTID              = ced.CONTEXTID
            AND dxicd10.DELETEDDATETIME       IS NULL
            AND dxicd10.nd_active_flag         = 'Y'
        JOIN {schema}.CLINICALENCOUNTER ce
            ON  ced.CLINICALENCOUNTERID = ce.CLINICALENCOUNTERID
            AND ced.CONTEXTID           = ce.CONTEXTID
            AND ce.nd_active_flag       = 'Y'
        JOIN {schema}.PATIENT p
            ON  ce.CHARTID              = p.ENTERPRISEID
            AND ce.CONTEXTID            = p.CONTEXTID
            AND p.nd_active_flag        = 'Y'
        LEFT JOIN {schema}.ICDCODEALL icd
            ON  dxicd10.ICDCODEID  = icd.ICDCODEID
            AND dxicd10.CONTEXTID  = icd.CONTEXTID
            AND icd.nd_active_flag = 'Y'
        LEFT JOIN {schema}.SNOMED dc
            ON  ced.SNOMEDCODE    = dc.SNOMEDCODE
            AND ced.CONTEXTID     = dc.CONTEXTID
            AND dc.nd_active_flag = 'Y'
        WHERE ced.DELETEDDATETIME  IS NULL
          AND ced.nd_active_flag    = 'Y'
          AND ced.nd_extracted_date {ND_DATE_FILTER}

        UNION ALL

        -- ── 4. DOCUMENT_DX_ICD10 ────────────────────────────────────
        SELECT
            'DOCUMENT_DX_ICD10',
            p.ENTERPRISEID,
            doc.CLINICALENCOUNTERID,
            ce.ENCOUNTERDATE,
            ddi10.DOCUMENTDIAGNOSISICD10ID,
            icd.DIAGNOSISCODE,
            'ICD10',
            icd.DIAGNOSISCODEDESCRIPTION,
            ddi10.ORDERING,
            NULL,
            NULL,
            CAST(dd.SNOMEDCODE AS CHAR),
            CAST(dc1.DESCRIPTION AS CHAR),
            ddi10.CREATEDDATETIME,
            ddi10.nd_extracted_date
        FROM {schema}.DOCUMENTDIAGNOSISICD10 ddi10
        JOIN {schema}.DOCUMENTDIAGNOSIS dd
            ON  ddi10.DOCUMENTDIAGNOSISID = dd.DOCUMENTDIAGNOSISID
            AND ddi10.CONTEXTID            = dd.CONTEXTID
            AND dd.DELETEDDATETIME        IS NULL
            AND dd.nd_active_flag          = 'Y'
        JOIN {schema}.DOCUMENT doc
            ON  dd.DOCUMENTID      = doc.DOCUMENTID
            AND dd.CONTEXTID       = doc.CONTEXTID
            AND doc.nd_active_flag = 'Y'
        JOIN {schema}.PATIENT p
            ON  doc.CHARTID        = p.ENTERPRISEID
            AND doc.CONTEXTID      = p.CONTEXTID
            AND p.nd_active_flag   = 'Y'
        LEFT JOIN {schema}.CLINICALENCOUNTER ce
            ON  doc.CLINICALENCOUNTERID = ce.CLINICALENCOUNTERID
            AND doc.CONTEXTID            = ce.CONTEXTID
            AND ce.nd_active_flag        = 'Y'
        LEFT JOIN {schema}.ICDCODEALL icd
            ON  ddi10.ICDCODEID    = icd.ICDCODEID
            AND ddi10.CONTEXTID    = icd.CONTEXTID
            AND icd.nd_active_flag = 'Y'
        LEFT JOIN {schema}.SNOMED dc1
            ON  dd.SNOMEDCODE      = dc1.SNOMEDCODE
            AND dd.CONTEXTID       = dc1.CONTEXTID
            AND dc1.nd_active_flag = 'Y'
        WHERE ddi10.DELETEDDATETIME IS NULL
          AND ddi10.nd_active_flag   = 'Y'
          AND ddi10.nd_extracted_date {ND_DATE_FILTER}

        UNION ALL

        -- ── 5. DOCUMENT_DX_ICD9 ─────────────────────────────────────
        SELECT
            'DOCUMENT_DX_ICD9',
            p.ENTERPRISEID,
            doc.CLINICALENCOUNTERID,
            ce.ENCOUNTERDATE,
            ddi9.DOCUMENTDIAGNOSISICD9ID,
            ddi9.DIAGNOSISCODE,
            'ICD9',
            dc.DIAGNOSISCODEDESCRIPTION,
            ddi9.ORDERING,
            NULL,
            NULL,
            CAST(dd.SNOMEDCODE AS CHAR),
            CAST(dc1.DESCRIPTION AS CHAR),
            ddi9.CREATEDDATETIME,
            ddi9.nd_extracted_date
        FROM {schema}.DOCUMENTDIAGNOSISICD9 ddi9
        JOIN {schema}.DOCUMENTDIAGNOSIS dd
            ON  ddi9.DOCUMENTDIAGNOSISID = dd.DOCUMENTDIAGNOSISID
            AND ddi9.CONTEXTID            = dd.CONTEXTID
            AND dd.DELETEDDATETIME       IS NULL
            AND dd.nd_active_flag         = 'Y'
        JOIN {schema}.DOCUMENT doc
            ON  dd.DOCUMENTID      = doc.DOCUMENTID
            AND dd.CONTEXTID       = doc.CONTEXTID
            AND doc.nd_active_flag = 'Y'
        JOIN {schema}.PATIENT p
            ON  doc.CHARTID        = p.ENTERPRISEID
            AND doc.CONTEXTID      = p.CONTEXTID
            AND p.nd_active_flag   = 'Y'
        LEFT JOIN {schema}.CLINICALENCOUNTER ce
            ON  doc.CLINICALENCOUNTERID = ce.CLINICALENCOUNTERID
            AND doc.CONTEXTID            = ce.CONTEXTID
            AND ce.nd_active_flag        = 'Y'
        LEFT JOIN {schema}.DIAGNOSISCODE dc
            ON  ddi9.DIAGNOSISCODE = dc.DIAGNOSISCODE
            AND ddi9.CONTEXTID     = dc.CONTEXTID
            AND dc.nd_active_flag  = 'Y'
        LEFT JOIN {schema}.SNOMED dc1
            ON  dd.SNOMEDCODE      = dc1.SNOMEDCODE
            AND dd.CONTEXTID       = dc1.CONTEXTID
            AND dc1.nd_active_flag = 'Y'
        WHERE ddi9.DELETEDDATETIME IS NULL
          AND ddi9.nd_active_flag   = 'Y'
          AND ddi9.nd_extracted_date {ND_DATE_FILTER}

        UNION ALL

        -- ── 6. CLINICAL_SERVICE_DX ──────────────────────────────────
        SELECT
            'CLINICAL_SERVICE_DX',
            p.ENTERPRISEID,
            ce.CLINICALENCOUNTERID,
            ce.ENCOUNTERDATE,
            csd.CLINICALSERVICEDIAGNOSISID,
            csd.DIAGNOSISCODE,
            csd.DIAGNOSISCODESET,
            dc.DIAGNOSISCODEDESCRIPTION,
            csd.ORDERING,
            NULL,
            NULL,
            NULL,
            NULL,
            csd.CREATEDDATETIME,
            cs.nd_extracted_date
        FROM {schema}.CLINICALSERVICE cs
        JOIN {schema}.CLINICALENCOUNTER ce
            ON  cs.CLINICALENCOUNTERID = ce.CLINICALENCOUNTERID
            AND cs.CONTEXTID           = ce.CONTEXTID
            AND ce.nd_active_flag      = 'Y'
        JOIN {schema}.PATIENT p
            ON  ce.CHARTID             = p.ENTERPRISEID
            AND ce.CONTEXTID           = p.CONTEXTID
            AND p.nd_active_flag       = 'Y'
        JOIN {schema}.CLINICALSERVICEPROCEDURECODE cspc
            ON  cspc.CLINICALSERVICEID = cs.CLINICALSERVICEID
            AND cspc.CONTEXTID         = cs.CONTEXTID
            AND cspc.nd_active_flag    = 'Y'
        JOIN {schema}.CLINICALSERVICEDIAGNOSIS csd
            ON  csd.CLINICALSERVICEPROCCODEID = cspc.CLINICALSERVICEPROCCODEID
            AND csd.CONTEXTID                  = cs.CONTEXTID
            AND csd.DELETEDDATETIME           IS NULL
            AND csd.nd_active_flag             = 'Y'
        LEFT JOIN {schema}.ICDCODEALL dc
            ON  TRIM(csd.DIAGNOSISCODE) = TRIM(dc.DIAGNOSISCODE)
            AND csd.CONTEXTID           = dc.CONTEXTID
            AND dc.nd_active_flag       = 'Y'
        WHERE cs.DELETEDDATETIME IS NULL
          AND cs.nd_active_flag   = 'Y'
          AND cs.nd_extracted_date {ND_DATE_FILTER}

        UNION ALL

        -- ── 7. REFERRAL_AUTH_DX ─────────────────────────────────────
        SELECT
            'REFERRAL_AUTH_DX',
            p.ENTERPRISEID,
            NULL,
            NULL,
            rad.REFERRALAUTHDIAGNOSISID,
            rad.DIAGNOSISCODE,
            rad.DIAGNOSISCODESETNAME,
            icd.DIAGNOSISCODEDESCRIPTION,
            rad.ORDERING,
            NULL,
            NULL,
            NULL,
            NULL,
            rad.CREATEDDATETIME,
            rad.nd_extracted_date
        FROM {schema}.REFERRALAUTHDIAGNOSISCODE rad
        JOIN {schema}.REFERRALAUTHORIZATION ra
            ON  rad.REFERRALAUTHID = ra.REFERRALAUTHID
            AND rad.CONTEXTID      = ra.CONTEXTID
            AND ra.nd_active_flag  = 'Y'
        JOIN {schema}.PATIENTINSURANCE pi
            ON  ra.PATIENTINSURANCEID = pi.PATIENTINSURANCEID
            AND ra.CONTEXTID          = pi.CONTEXTID
            AND pi.nd_active_flag     = 'Y'
        JOIN {schema}.PATIENT p
            ON  pi.PATIENTID     = p.ENTERPRISEID
            AND pi.CONTEXTID     = p.CONTEXTID
            AND p.nd_active_flag = 'Y'
        LEFT JOIN {schema}.ICDCODEALL icd
            ON  rad.ICDCODEID      = icd.ICDCODEID
            AND rad.CONTEXTID      = icd.CONTEXTID
            AND icd.nd_active_flag = 'Y'
        WHERE rad.DELETEDDATETIME IS NULL
          AND ra.DELETEDDATETIME  IS NULL
          AND rad.nd_active_flag   = 'Y'
          AND rad.nd_extracted_date {ND_DATE_FILTER}
    ) dx
) t
"""


# ── Connection & utilities ────────────────────────────────────────────────────
def get_connection():
    return pymysql.connect(**DB_CONFIG)


def _table_exists(cur, schema: str, table: str) -> bool:
    cur.execute(
        "SELECT COUNT(*) FROM information_schema.tables "
        "WHERE table_schema = %s AND table_name = %s",
        (schema, table),
    )
    return cur.fetchone()[0] > 0


def is_done(cur, job_key: str) -> bool:
    try:
        cur.execute(
            f"SELECT status FROM {CHECKPOINT_TABLE} WHERE job_key = %s",
            (job_key,),
        )
        row = cur.fetchone()
        return row is not None and row[0] == "done"
    except pymysql.err.ProgrammingError:
        return False


def mark(cur, conn, job_key: str, status: str = "done"):
    cur.execute(
        f"""
        INSERT INTO {CHECKPOINT_TABLE} (job_key, status, updated_at)
        VALUES (%s, %s, NOW())
        ON DUPLICATE KEY UPDATE status = VALUES(status), updated_at = NOW()
        """,
        (job_key, status),
    )
    conn.commit()


# ── Setup ─────────────────────────────────────────────────────────────────────
def setup_checkpoint():
    conn = get_connection()
    cur  = conn.cursor()
    try:
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {CHECKPOINT_TABLE} (
                job_key    VARCHAR(255) NOT NULL PRIMARY KEY,
                status     VARCHAR(50)  NOT NULL,
                updated_at DATETIME     NOT NULL
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """
        )
        conn.commit()
        print(f"[setup] Checkpoint table ready: {CHECKPOINT_TABLE}")
    finally:
        cur.close()
        conn.close()


def setup_dest_table():
    conn = get_connection()
    cur  = conn.cursor()
    try:
        sch, tbl = DEST_TABLE.split(".")
        if _table_exists(cur, sch, tbl):
            cur.execute(f"SELECT COUNT(*) FROM {DEST_TABLE}")
            print(f"[setup] Dest table exists — {cur.fetchone()[0]:,} rows.")
            return
        cur.execute(
            f"""
            CREATE TABLE {DEST_TABLE} (
                udm_inc_id              BIGINT    NOT NULL AUTO_INCREMENT PRIMARY KEY,
                diag_id                 VARCHAR(255),
                ndid                    VARCHAR(255),
                eid                     VARCHAR(255),
                enc_date                DATE,
                diag_date               DATE,
                diag_code               VARCHAR(255),
                diag_desc               VARCHAR(1000),
                diag_coding_system      VARCHAR(100),
                diag_code_stripped      VARCHAR(255),
                primary_diagnosis_flag  VARCHAR(100),
                parent_diag_code        VARCHAR(255),
                parent_diag_desc        VARCHAR(500),
                icd_codeset             VARCHAR(100),
                icd_codeset_desc        VARCHAR(500),
                icd_codeset_group       VARCHAR(255),
                icd_codeset_system      VARCHAR(255),
                snomed_code             VARCHAR(255),
                snomed_desc             VARCHAR(1000),
                diag_severity           VARCHAR(100),
                diag_status             VARCHAR(100),
                diag_end_date           DATE,
                provisional_diag_flag   VARCHAR(10),
                differential_diag_flag  VARCHAR(10),
                comments_notes          TEXT,
                diag_category           VARCHAR(255),
                diag_risk               VARCHAR(255),
                work_up_status          VARCHAR(255),
                created_datetime        DATETIME,
                created_by              VARCHAR(100),
                updated_datetime        DATETIME,
                updated_by              VARCHAR(100),
                ehr_source_name         VARCHAR(100),
                source_path             VARCHAR(100),
                data_type               VARCHAR(100),
                psid                    INT,
                nd_extracted_date       DATE,
                enc_date_proxy          DATE,
                udm_unq_id              VARCHAR(64)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """
        )
        conn.commit()
        print(f"[setup] Created dest table: {DEST_TABLE}")
    finally:
        cur.close()
        conn.close()


def setup_indexes(src: dict):
    """Create indexes on source tables used across all 7 branches."""
    conn   = get_connection()
    cur    = conn.cursor()
    schema = src["schema"]
    key    = src["key"]
    indexes = [
        ("CLINICALENCOUNTERDIAGNOSIS", "CLINICALENCOUNTERID",      "idx_clinicalencounterid"),
        ("CLINICALENCOUNTERDIAGNOSIS", "nd_extracted_date",        "idx_nd_extracted_date"),
        ("CLINICALENCOUNTERDIAGNOSIS", "nd_active_flag",           "idx_nd_active_flag"),
        ("CLINICALENCOUNTERDXICD10",   "CLINICALENCOUNTERDXID",    "idx_clinicalencounterdxid"),
        ("CLINICALENCOUNTER",          "CLINICALENCOUNTERID",      "idx_clinicalencounterid"),
        ("DOCUMENTDIAGNOSISICD10",     "DOCUMENTDIAGNOSISID",      "idx_documentdiagnosisid"),
        ("DOCUMENTDIAGNOSISICD10",     "nd_extracted_date",        "idx_nd_extracted_date"),
        ("DOCUMENTDIAGNOSIS",          "DOCUMENTID",               "idx_documentid"),
        ("DOCUMENT",                   "CLINICALENCOUNTERID",      "idx_clinicalencounterid"),
        ("DOCUMENTDIAGNOSISICD9",      "DOCUMENTDIAGNOSISID",      "idx_documentdiagnosisid"),
        ("DOCUMENTDIAGNOSISICD9",      "nd_extracted_date",        "idx_nd_extracted_date"),
        ("CLINICALSERVICE",            "CLINICALENCOUNTERID",      "idx_clinicalencounterid"),
        ("CLINICALSERVICE",            "nd_extracted_date",        "idx_nd_extracted_date"),
        ("CLINICALSERVICEPROCEDURECODE","CLINICALSERVICEID",       "idx_clinicalserviceid"),
        ("CLINICALSERVICEDIAGNOSIS",   "CLINICALSERVICEPROCCODEID","idx_clinicalserviceproccodeid"),
        ("REFERRALAUTHDIAGNOSISCODE",  "REFERRALAUTHID",           "idx_referralauthid"),
        ("REFERRALAUTHDIAGNOSISCODE",  "nd_extracted_date",        "idx_nd_extracted_date"),
        ("REFERRALAUTHORIZATION",      "PATIENTINSURANCEID",       "idx_patientinsuranceid"),
    ]
    try:
        for table, col, idx_name in indexes:
            if not _table_exists(cur, schema, table):
                continue
            cur.execute(
                "SELECT COUNT(*) FROM information_schema.statistics "
                "WHERE table_schema = %s AND table_name = %s AND index_name = %s",
                (schema, table, idx_name),
            )
            if cur.fetchone()[0] > 0:
                continue
            try:
                cur.execute(f"CREATE INDEX {idx_name} ON {schema}.{table} ({col})")
                conn.commit()
                print(f"[{key}] Created index {schema}.{table}({col})")
            except Exception as e:
                conn.rollback()
                print(f"[{key}] Index {schema}.{table}({col}) skipped: {e}")
    finally:
        cur.close()
        conn.close()


# ── Phase 1: Pre-materialization ──────────────────────────────────────────────
def run_premat(src: dict):
    conn = get_connection()
    cur  = conn.cursor()
    premat    = _premat(src["key"])
    sch, tbl  = premat.split(".")
    job_key   = f"v1:premat:{src['key']}"
    try:
        if _table_exists(cur, sch, tbl):
            cur.execute(f"SELECT COUNT(*) FROM {premat}")
            print(f"[{src['key']}] Pre-mat exists — {cur.fetchone()[0]:,} rows.")
            return

        print(f"[{src['key']}] Pre-materializing (7 branches, slow, runs once) ...")
        t0 = time.time()

        cur.execute("SET SESSION net_read_timeout    = 86400")
        cur.execute("SET SESSION net_write_timeout   = 86400")

        sql = _build_premat_sql(src)
        cur.execute(sql)
        conn.commit()

        cur.execute(f"CREATE INDEX idx_pk ON {premat} (pk)")
        conn.commit()

        cur.execute(f"SELECT COUNT(*) FROM {premat}")
        cnt     = cur.fetchone()[0]
        elapsed = time.time() - t0
        print(f"[{src['key']}] Pre-mat done — {cnt:,} rows in {elapsed:.0f}s")
        mark(cur, conn, job_key)
    except Exception as e:
        conn.rollback()
        print(f"[{src['key']}] Pre-mat FAILED: {e}")
        raise
    finally:
        cur.close()
        conn.close()


# ── Batch ranges ──────────────────────────────────────────────────────────────
def get_batch_ranges(premat_table: str) -> list:
    conn = get_connection()
    cur  = conn.cursor()
    try:
        cur.execute(f"SELECT MIN(pk), MAX(pk), COUNT(*) FROM {premat_table}")
        mn, mx, total = cur.fetchone()
        if not total:
            return []
        cur.execute(
            f"""
            SELECT pk FROM (
                SELECT pk, ROW_NUMBER() OVER (ORDER BY pk) AS rn
                FROM {premat_table}
            ) t
            WHERE rn % {BATCH_SIZE} = 1
            ORDER BY pk
            """
        )
        boundaries = [r[0] for r in cur.fetchall()]
        ranges = []
        for i, lo in enumerate(boundaries):
            hi = boundaries[i + 1] if i + 1 < len(boundaries) else None
            ranges.append((lo, hi))
        return ranges
    finally:
        cur.close()
        conn.close()


# ── Batch INSERT (no JOINs — trivial range scan) ──────────────────────────────
def build_batch_insert(premat_table: str, lo: int, hi) -> str:
    pk_filter = f"WHERE pk >= {lo}" + (f" AND pk < {hi}" if hi is not None else "")
    return f"""
INSERT INTO {DEST_TABLE} ({_INSERT_COLS})
SELECT {_INSERT_COLS}
FROM {premat_table}
{pk_filter}
"""


# ── Phase 2: Batch inserts ────────────────────────────────────────────────────
def run_source(src: dict, batch_ranges: list, pbar: tqdm) -> tuple:
    key     = src["key"]
    premat  = _premat(key)
    total   = 0
    t_start = time.time()
    conn    = get_connection()
    cur     = conn.cursor()
    try:
        cur.execute("SET unique_checks    = 0")
        cur.execute("SET foreign_key_checks = 0")

        for lo, hi in batch_ranges:
            job_key = f"v1:insert:{key}:{lo}"
            if is_done(cur, job_key):
                pbar.update(1)
                continue
            sql  = build_batch_insert(premat, lo, hi)
            cur.execute(sql)
            rows = cur.rowcount
            conn.commit()
            total += rows
            mark(cur, conn, job_key)
            pbar.update(1)

        cur.execute("SET unique_checks    = 1")
        cur.execute("SET foreign_key_checks = 1")
    finally:
        cur.close()
        conn.close()

    elapsed = time.time() - t_start
    print(
        f"[{key}] Insert done — {total:,} rows  "
        f"{len(batch_ranges)} batches  {elapsed:.1f}s"
    )
    return key, total, elapsed


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    t0 = time.time()
    sources_str = ', '.join(f"{s['key']} (psid={s['psid']})" for s in SOURCES)
    print(f"\n{'='*68}")
    print(f"  AthenaOne Diagnoses ETL v1 — {datetime.now():%Y-%m-%d %H:%M:%S}")
    print(f"  Dest     : {DEST_TABLE}")
    print(f"  Sources  : {sources_str}")
    print(f"  Batch    : {BATCH_SIZE:,}   Workers: {MAX_WORKERS}")
    print(f"  Filter   : nd_extracted_date {ND_DATE_FILTER}")
    print(f"{'='*68}\n")

    setup_checkpoint()
    setup_dest_table()

    results:    dict = {}
    all_ranges: dict = {}
    pbar_lock = threading.Lock()

    def _pipeline(src):
        key = src["key"]
        setup_indexes(src)
        run_premat(src)

        ranges = get_batch_ranges(_premat(key))
        all_ranges[key] = ranges

        with pbar_lock:
            pbar.total = (pbar.total or 0) + len(ranges)
            pbar.refresh()

        return run_source(src, ranges, pbar)

    with tqdm(total=0, unit="batch", desc="  Overall") as pbar:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
            futures = {pool.submit(_pipeline, s): s["key"] for s in SOURCES}
            for future in as_completed(futures):
                key, rows, elapsed = future.result()
                results[key] = (rows, elapsed)

    grand_total   = sum(r for r, _ in results.values())
    elapsed_total = time.time() - t0

    print(f"\n{'='*68}")
    print(f"  Summary:")
    for src in SOURCES:
        rows, elapsed = results.get(src["key"], (0, 0.0))
        batches = len(all_ranges.get(src["key"], []))
        print(
            f"    {src['key']:<20} psid={src['psid']}  "
            f"{rows:>10,} rows  {batches} batches  {elapsed:.1f}s"
        )
    print(f"\n  Grand total : {grand_total:,} rows inserted")
    print(f"  Elapsed     : {elapsed_total:.1f}s  ({elapsed_total/60:.1f} min)")
    print(f"{'='*68}")

    print(f"\n  Cleanup SQL (run after verifying results):")
    for src in SOURCES:
        print(f"    DROP TABLE IF EXISTS {_premat(src['key'])};")
    print(f"    DROP TABLE IF EXISTS {CHECKPOINT_TABLE};")


if __name__ == "__main__":
    main()
