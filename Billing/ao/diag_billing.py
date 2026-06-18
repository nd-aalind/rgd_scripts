#!/usr/bin/env python3
"""
Optimized ETL for: billing_chatbot.diagnosis_claims

Pre-materialized staging tables (ROW_NUMBER dedup / MAX-agg computed ONCE):
  - staging.diag_billing_claim_{schema}    (CLAIM         latest per CLAIMAPPOINTMENTID)
  - staging.diag_billing_enc_{schema}      (CLINICALENCOUNTER latest per APPOINTMENTID)
  - staging.diag_billing_claimdiag_{schema}(CLAIMDIAGNOSIS latest per CLAIMID+SEQUENCENUMBER)
  - staging.diag_billing_encdiag_{schema}  (CLINICALENCOUNTERDIAGNOSIS latest per ENCID+DIAGCODE)
  - staging.diag_billing_icd_{schema}      (ICDCODEALL MAX-agg per DIAGNOSISCODE+CODESET)
  - staging.diag_billing_dc_{schema}       (DIAGNOSISCODE MAX-agg per DIAGNOSISCODE+contextid)

Optimizations:
- All 6 CTE dedup/agg operations materialized once (not re-run per batch)
- Batching by actual CLAIMID values (sparse-ID safe)
  Outer ROW_NUMBER partitions by (CLAIMID, SEQUENCENUMBER) — all rows for a
  given CLAIMID fall in the same batch, so per-batch dedup == full-table dedup
- Indexes ensured on all join-key columns before processing
- Checkpoint/resume — re-run skips if already completed
- Commit after every batch
- InnoDB checks disabled per-session for bulk speed
- tqdm progress bar

Usage:
    python diag_billing.py
"""

import sys
import time
from datetime import datetime
import pymysql
from tqdm import tqdm

# ── Configuration ──────────────────────────────────────────────────────────────
DB_CONFIG = {
    "host":            "ndai-dev-rds-instance.cwp60ymu4ko0.us-east-1.rds.amazonaws.com",
    "port":            3306,
    "user":            "Aalind",
    "password":        "A@L1nd@123",
    "database":        "tng_athena_one",
    "charset":         "utf8mb4",
    "connect_timeout": 30,
    "read_timeout":    21600,
    "write_timeout":   21600,
}

BATCH_SIZE = 25_000   # billing rows are wide; smaller batches reduce lock time

# ── Change these to run for a different schema / context ──────────────────────
SOURCE_SCHEMA = "tng_athena_one"
CONTEXT_ID    = "25810"           # contextid filter applied in all CTEs

DEST_TABLE       = "billing_chatbot.diagnosis_claims"
STAGING_CLAIM    = f"staging.diag_billing_claim_{SOURCE_SCHEMA}"
STAGING_ENC      = f"staging.diag_billing_enc_{SOURCE_SCHEMA}"
STAGING_CD       = f"staging.diag_billing_claimdiag_{SOURCE_SCHEMA}"
STAGING_CED      = f"staging.diag_billing_encdiag_{SOURCE_SCHEMA}"
STAGING_ICD      = f"staging.diag_billing_icd_{SOURCE_SCHEMA}"
STAGING_DC       = f"staging.diag_billing_dc_{SOURCE_SCHEMA}"
STAGING_PK       = f"staging.diag_billing_pk_{SOURCE_SCHEMA}"
CHECKPOINT_TABLE = f"staging.etl_checkpoint_diag_billing_{SOURCE_SCHEMA}"
CHECKPOINT_KEY   = f"diag_billing_{SOURCE_SCHEMA}"
BATCH_KEY        = "CLAIMID"


# ── Helpers ────────────────────────────────────────────────────────────────────
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


def _ensure_index(cur, conn, schema, table, index_name, columns):
    cur.execute(
        "SELECT COUNT(*) FROM information_schema.statistics "
        "WHERE table_schema = %s AND table_name = %s AND index_name = %s",
        (schema, table, index_name),
    )
    if cur.fetchone()[0] > 0:
        print(f"    {table}.{columns[0]} — index exists")
        return
    col_list = ", ".join(columns)
    print(f"    Creating index {index_name} on {schema}.{table}({col_list})...")
    cur.execute(f"CREATE INDEX {index_name} ON `{schema}`.`{table}` ({col_list})")
    conn.commit()
    print("    done")


# ── Checkpoint ─────────────────────────────────────────────────────────────────
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


# ── Batch INSERT builder ────────────────────────────────────────────────────────
def build_batch_insert(pk_lo, pk_hi):
    return f"""
INSERT INTO {DEST_TABLE}
    (contextid, claim_id, patient_id, service_date, claim_created_datetime,
     appointment_id, primary_claim_status, secondary_claim_status, patient_claim_status,
     rendering_provider_id, rendering_provider_name, rendering_provider_specialty,
     supervising_provider_id, supervising_provider_name, supervising_provider_specialty,
     service_department_name, service_department_city,
     encounter_id, encounter_date,
     claim_diagnosis_id, diagnosis_sequence, claim_diagnosis_code, diagnosis_codeset,
     claim_diagnosis_description, diagnosis_priority,
     encounter_diagnosis_id, encounter_diagnosis_code, encounter_diagnosis_description,
     encounter_diagnosis_status, diagnosis_laterality, diagnosis_note,
     diagnosis_recorded_by, diagnosis_recorded_datetime,
     diagnosis_status_label, diagnosis_source_match,
     claim_diagnosis_created_datetime, claim_diagnosis_created_by)
SELECT
    contextid, claim_id, patient_id, service_date, claim_created_datetime,
    appointment_id, primary_claim_status, secondary_claim_status, patient_claim_status,
    rendering_provider_id, rendering_provider_name, rendering_provider_specialty,
    supervising_provider_id, supervising_provider_name, supervising_provider_specialty,
    service_department_name, service_department_city,
    encounter_id, encounter_date,
    claim_diagnosis_id, diagnosis_sequence, claim_diagnosis_code, diagnosis_codeset,
    claim_diagnosis_description, diagnosis_priority,
    encounter_diagnosis_id, encounter_diagnosis_code, encounter_diagnosis_description,
    encounter_diagnosis_status, diagnosis_laterality, diagnosis_note,
    diagnosis_recorded_by, diagnosis_recorded_datetime,
    diagnosis_status_label, diagnosis_source_match,
    claim_diagnosis_created_datetime, claim_diagnosis_created_by
FROM (
    SELECT
        c.contextid,
        c.CLAIMID                                                                AS claim_id,
        c.PATIENTID                                                              AS patient_id,
        c.CLAIMSERVICEDATE                                                       AS service_date,
        c.CLAIMCREATEDDATETIME                                                   AS claim_created_datetime,
        c.CLAIMAPPOINTMENTID                                                     AS appointment_id,
        c.PRIMARYCLAIMSTATUS                                                     AS primary_claim_status,
        c.SECONDARYCLAIMSTATUS                                                   AS secondary_claim_status,
        c.PATIENTCLAIMSTATUS                                                     AS patient_claim_status,
        rend_prov.PROVIDERID                                                     AS rendering_provider_id,
        CONCAT(COALESCE(rend_prov.PROVIDERFIRSTNAME,''), ' ',
               COALESCE(rend_prov.PROVIDERLASTNAME,''))                          AS rendering_provider_name,
        rend_prov.SPECIALTY                                                      AS rendering_provider_specialty,
        sup_prov.PROVIDERID                                                      AS supervising_provider_id,
        CONCAT(COALESCE(sup_prov.PROVIDERFIRSTNAME,''), ' ',
               COALESCE(sup_prov.PROVIDERLASTNAME,''))                           AS supervising_provider_name,
        sup_prov.SPECIALTY                                                       AS supervising_provider_specialty,
        dep.DEPARTMENTNAME                                                       AS service_department_name,
        dep.DEPARTMENTCITY                                                       AS service_department_city,
        ce.CLINICALENCOUNTERID                                                   AS encounter_id,
        ce.ENCOUNTERDATE                                                         AS encounter_date,
        cd.CLAIMDIAGNOSISID                                                      AS claim_diagnosis_id,
        cd.SEQUENCENUMBER                                                        AS diagnosis_sequence,
        cd.DIAGNOSISCODE                                                         AS claim_diagnosis_code,
        cd.DIAGNOSISCODESETNAME                                                  AS diagnosis_codeset,
        icd.DIAGNOSISCODEDESCRIPTION                                             AS claim_diagnosis_description,
        CASE cd.SEQUENCENUMBER
            WHEN 1 THEN 'Primary'
            WHEN 2 THEN 'Secondary'
            WHEN 3 THEN 'Tertiary'
            ELSE CONCAT('Diagnosis ', cd.SEQUENCENUMBER)
        END                                                                      AS diagnosis_priority,
        ced.CLINICALENCOUNTERDXID                                                AS encounter_diagnosis_id,
        ced.DIAGNOSISCODE                                                        AS encounter_diagnosis_code,
        dc.diagnosiscodedescription                                              AS encounter_diagnosis_description,
        ced.STATUS                                                               AS encounter_diagnosis_status,
        ced.LATERALITY                                                           AS diagnosis_laterality,
        ced.NOTE                                                                 AS diagnosis_note,
        ced.CREATEDBY                                                            AS diagnosis_recorded_by,
        ced.CREATEDDATETIME                                                      AS diagnosis_recorded_datetime,
        CASE ced.STATUS
            WHEN 'NEWPROBLEMWORKUP'     THEN 'New Problem - Workup'
            WHEN 'ESTABLISHEDSTABLE'    THEN 'Established - Stable'
            WHEN 'ESTABLISHEDIMPROVING' THEN 'Established - Improving'
            WHEN 'ESTABLISHEDWORSENING' THEN 'Established - Worsening'
            WHEN 'ESTABLISHEDANDSTABLE' THEN 'Established and Stable'
            WHEN 'DIFFERENTIALDX'       THEN 'Differential Diagnosis'
            WHEN 'NEXTVISITWORKUP'      THEN 'Next Visit Workup'
            WHEN 'NEWPROVLMENOWORKUP'   THEN 'New Problem - No Workup'
            WHEN 'UNCONTROLLED'         THEN 'Uncontrolled'
            WHEN 'MINOR'                THEN 'Minor'
            ELSE COALESCE(ced.STATUS, 'Not Specified')
        END                                                                      AS diagnosis_status_label,
        CASE
            WHEN cd.DIAGNOSISCODE = ced.DIAGNOSISCODE                           THEN 'Matched'
            WHEN cd.DIAGNOSISCODE IS NOT NULL AND ced.DIAGNOSISCODE IS NULL     THEN 'Claim Only'
            WHEN cd.DIAGNOSISCODE IS NULL     AND ced.DIAGNOSISCODE IS NOT NULL THEN 'Encounter Only'
            ELSE 'Unmatched'
        END                                                                      AS diagnosis_source_match,
        cd.CREATEDDATETIME                                                       AS claim_diagnosis_created_datetime,
        cd.CREATEDBY                                                             AS claim_diagnosis_created_by,
        ROW_NUMBER() OVER (
            PARTITION BY c.CLAIMID, cd.SEQUENCENUMBER
            ORDER BY ce.CLINICALENCOUNTERID DESC, ced.CREATEDDATETIME DESC
        )                                                                        AS rn
    FROM {STAGING_CLAIM} c
    INNER JOIN `{SOURCE_SCHEMA}`.PATIENT p
        ON  p.PATIENTID      = c.PATIENTID
        AND p.contextid      = c.contextid
        AND p.nd_active_flag = 'Y'
    LEFT JOIN `{SOURCE_SCHEMA}`.PROVIDER rend_prov
        ON  rend_prov.PROVIDERID     = c.RENDERINGPROVIDERID
        AND rend_prov.contextid      = c.contextid
        AND rend_prov.nd_active_flag = 'Y'
    LEFT JOIN `{SOURCE_SCHEMA}`.PROVIDER sup_prov
        ON  sup_prov.PROVIDERID     = c.SUPERVISINGPROVIDERID
        AND sup_prov.contextid      = c.contextid
        AND sup_prov.nd_active_flag = 'Y'
    LEFT JOIN `{SOURCE_SCHEMA}`.DEPARTMENT dep
        ON  dep.DEPARTMENTID   = c.SERVICEDEPARTMENTID
        AND dep.contextid      = c.contextid
        AND dep.nd_active_flag = 'Y'
    LEFT JOIN {STAGING_ENC} ce
        ON  ce.APPOINTMENTID = c.CLAIMAPPOINTMENTID
        AND ce.contextid     = c.contextid
    LEFT JOIN {STAGING_CD} cd
        ON  cd.CLAIMID   = c.CLAIMID
        AND cd.contextid = c.contextid
    LEFT JOIN {STAGING_ICD} icd
        ON  icd.DIAGNOSISCODE    = cd.DIAGNOSISCODE
        AND icd.DIAGNOSISCODESET = cd.DIAGNOSISCODESETNAME
    LEFT JOIN {STAGING_CED} ced
        ON  ced.CLINICALENCOUNTERID = ce.CLINICALENCOUNTERID
        AND ced.DIAGNOSISCODE       = cd.DIAGNOSISCODE
    LEFT JOIN {STAGING_DC} dc
        ON  dc.DIAGNOSISCODE = ced.DIAGNOSISCODE
        AND dc.contextid     = ced.contextid
    WHERE c.CLAIMID >= {pk_lo} AND c.CLAIMID < {pk_hi}
) final
WHERE rn = 1
"""


# ── Setup ──────────────────────────────────────────────────────────────────────
def setup_tables():
    conn = get_connection()
    cur  = conn.cursor()

    # ── 1. Source table indexes (needed before staging creation) ──────────────
    print("  Ensuring indexes on source tables...")
    for tbl, idx, cols in [
        # CLAIM — contextid filter + dedup partition key
        ("CLAIM",                     "idx_claimid",         ["CLAIMID"]),
        ("CLAIM",                     "idx_contextid",       ["contextid"]),
        ("CLAIM",                     "idx_appointmentid",   ["CLAIMAPPOINTMENTID"]),
        ("CLAIM",                     "idx_active_flag",     ["nd_active_flag"]),
        # CLINICALENCOUNTER — dedup partition key + contextid
        ("CLINICALENCOUNTER",         "idx_appointmentid",   ["APPOINTMENTID"]),
        ("CLINICALENCOUNTER",         "idx_contextid",       ["contextid"]),
        # CLAIMDIAGNOSIS — dedup partition key + contextid
        ("CLAIMDIAGNOSIS",            "idx_claimid",         ["CLAIMID"]),
        ("CLAIMDIAGNOSIS",            "idx_contextid",       ["contextid"]),
        # CLINICALENCOUNTERDIAGNOSIS — dedup partition key + contextid
        ("CLINICALENCOUNTERDIAGNOSIS","idx_encounterid",     ["CLINICALENCOUNTERID"]),
        ("CLINICALENCOUNTERDIAGNOSIS","idx_diagnosiscode",   ["DIAGNOSISCODE(100)"]),  # TEXT col needs prefix
        ("CLINICALENCOUNTERDIAGNOSIS","idx_contextid",       ["contextid"]),
        # ICDCODEALL — GROUP BY join key (DIAGNOSISCODE/DIAGNOSISCODESET may be TEXT)
        ("ICDCODEALL",                "idx_diagcode_codeset",["DIAGNOSISCODE(100)", "DIAGNOSISCODESET(50)"]),
        # DIAGNOSISCODE — GROUP BY join key
        ("DIAGNOSISCODE",             "idx_diagcode_ctx",    ["DIAGNOSISCODE(100)", "contextid"]),
        # Lookup tables used directly in batch INSERT
        ("PATIENT",                   "idx_patientid",       ["PATIENTID"]),
        ("PATIENT",                   "idx_contextid",       ["contextid"]),
        ("PROVIDER",                  "idx_providerid",      ["PROVIDERID"]),
        ("PROVIDER",                  "idx_contextid",       ["contextid"]),
        ("DEPARTMENT",                "idx_departmentid",    ["DEPARTMENTID"]),
        ("DEPARTMENT",                "idx_contextid",       ["contextid"]),
    ]:
        _ensure_index(cur, conn, SOURCE_SCHEMA, tbl, idx, cols)

    # ── 2. claim_dedup — latest CLAIM per CLAIMAPPOINTMENTID ─────────────────
    print(f"  Materializing claim dedup ({STAGING_CLAIM})...")
    if not _table_exists(cur, STAGING_CLAIM):
        cur.execute(f"""
            CREATE TABLE {STAGING_CLAIM} AS
            SELECT
                CLAIMID, PATIENTID, CLAIMSERVICEDATE, CLAIMCREATEDDATETIME,
                CLAIMAPPOINTMENTID, PRIMARYCLAIMSTATUS, SECONDARYCLAIMSTATUS,
                PATIENTCLAIMSTATUS, RENDERINGPROVIDERID, PRIMARYBILLINGPROVIDERID,
                SUPERVISINGPROVIDERID, SERVICEDEPARTMENTID, contextid
            FROM (
                SELECT c.*,
                       ROW_NUMBER() OVER (
                           PARTITION BY c.CLAIMAPPOINTMENTID
                           ORDER BY c.CLAIMCREATEDDATETIME DESC
                       ) AS rn
                FROM `{SOURCE_SCHEMA}`.CLAIM c
                WHERE c.nd_active_flag = 'Y'
                  AND c.contextid      = '{CONTEXT_ID}'
            ) x WHERE rn = 1
        """)
        cur.execute(f"""
            ALTER TABLE {STAGING_CLAIM}
                ADD INDEX idx_claimid          (CLAIMID),
                ADD INDEX idx_appointmentid    (CLAIMAPPOINTMENTID),
                ADD INDEX idx_patientid        (PATIENTID),
                ADD INDEX idx_renderingprovid  (RENDERINGPROVIDERID),
                ADD INDEX idx_supervisingprovid(SUPERVISINGPROVIDERID),
                ADD INDEX idx_servicedeptid    (SERVICEDEPARTMENTID)
        """)
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")
    cur.execute(f"SELECT COUNT(*) FROM {STAGING_CLAIM}")
    print(f"    {cur.fetchone()[0]:,} claims (contextid={CONTEXT_ID})")

    # ── 3. encounter_dedup — latest CLINICALENCOUNTER per APPOINTMENTID ───────
    print(f"  Materializing encounter dedup ({STAGING_ENC})...")
    if not _table_exists(cur, STAGING_ENC):
        cur.execute(f"""
            CREATE TABLE {STAGING_ENC} AS
            SELECT CLINICALENCOUNTERID, APPOINTMENTID, ENCOUNTERDATE, contextid
            FROM (
                SELECT ce.*,
                       ROW_NUMBER() OVER (
                           PARTITION BY ce.APPOINTMENTID
                           ORDER BY ce.CLINICALENCOUNTERID DESC
                       ) AS rn
                FROM `{SOURCE_SCHEMA}`.CLINICALENCOUNTER ce
                WHERE ce.DELETEDDATETIME IS NULL
                  AND ce.contextid       = '{CONTEXT_ID}'
                  AND ce.ENCOUNTERSTATUS NOT IN ('DELETED','TEMP')
            ) x WHERE rn = 1
        """)
        cur.execute(f"""
            ALTER TABLE {STAGING_ENC}
                ADD INDEX idx_appointmentid    (APPOINTMENTID),
                ADD INDEX idx_encounterid      (CLINICALENCOUNTERID)
        """)
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")
    cur.execute(f"SELECT COUNT(*) FROM {STAGING_ENC}")
    print(f"    {cur.fetchone()[0]:,} encounters")

    # ── 4. claim_diagnosis_dedup — latest CLAIMDIAGNOSIS per CLAIMID+SEQNUM ──
    print(f"  Materializing claim diagnosis dedup ({STAGING_CD})...")
    if not _table_exists(cur, STAGING_CD):
        cur.execute(f"""
            CREATE TABLE {STAGING_CD} AS
            SELECT CLAIMDIAGNOSISID, CLAIMID, SEQUENCENUMBER, DIAGNOSISCODE,
                   DIAGNOSISCODESETNAME, CREATEDDATETIME, CREATEDBY, contextid
            FROM (
                SELECT cd.*,
                       ROW_NUMBER() OVER (
                           PARTITION BY cd.CLAIMID, cd.SEQUENCENUMBER
                           ORDER BY cd.CREATEDDATETIME DESC
                       ) AS rn
                FROM `{SOURCE_SCHEMA}`.CLAIMDIAGNOSIS cd
                WHERE cd.DELETEDDATETIME IS NULL
                  AND cd.contextid       = '{CONTEXT_ID}'
            ) x WHERE rn = 1
        """)
        cur.execute(f"""
            ALTER TABLE {STAGING_CD}
                ADD INDEX idx_claimid      (CLAIMID),
                ADD INDEX idx_diagnosiscode(DIAGNOSISCODE(100))
        """)
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")
    cur.execute(f"SELECT COUNT(*) FROM {STAGING_CD}")
    print(f"    {cur.fetchone()[0]:,} claim diagnoses")

    # ── 5. encounter_diagnosis_dedup — latest per CLINICALENCOUNTERID+DIAGCODE
    print(f"  Materializing encounter diagnosis dedup ({STAGING_CED})...")
    if not _table_exists(cur, STAGING_CED):
        cur.execute(f"""
            CREATE TABLE {STAGING_CED} AS
            SELECT CLINICALENCOUNTERDXID, CLINICALENCOUNTERID, DIAGNOSISCODE,
                   STATUS, LATERALITY, NOTE, CREATEDBY, CREATEDDATETIME, contextid
            FROM (
                SELECT ced.*,
                       ROW_NUMBER() OVER (
                           PARTITION BY ced.CLINICALENCOUNTERID, ced.DIAGNOSISCODE
                           ORDER BY ced.CREATEDDATETIME DESC
                       ) AS rn
                FROM `{SOURCE_SCHEMA}`.CLINICALENCOUNTERDIAGNOSIS ced
                WHERE ced.DELETEDDATETIME IS NULL
                  AND ced.contextid       = '{CONTEXT_ID}'
            ) x WHERE rn = 1
        """)
        cur.execute(f"""
            ALTER TABLE {STAGING_CED}
                ADD INDEX idx_encounterid  (CLINICALENCOUNTERID),
                ADD INDEX idx_diagnosiscode(DIAGNOSISCODE(100))
        """)
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")
    cur.execute(f"SELECT COUNT(*) FROM {STAGING_CED}")
    print(f"    {cur.fetchone()[0]:,} encounter diagnoses")

    # ── 6. icd_dedup — MAX-agg ICDCODEALL per DIAGNOSISCODE+CODESET ──────────
    print(f"  Materializing ICD code lookup ({STAGING_ICD})...")
    if not _table_exists(cur, STAGING_ICD):
        cur.execute(f"""
            CREATE TABLE {STAGING_ICD} AS
            SELECT
                DIAGNOSISCODE,
                DIAGNOSISCODESET,
                MAX(DIAGNOSISCODEDESCRIPTION) AS DIAGNOSISCODEDESCRIPTION,
                MAX(EFFECTIVEDATE)            AS EFFECTIVEDATE,
                MAX(EXPIRATIONDATE)           AS EXPIRATIONDATE
            FROM `{SOURCE_SCHEMA}`.ICDCODEALL
            WHERE (ISDELETED = 0 OR ISDELETED IS NULL)
              AND EXPIRATIONDATE IS NULL
            GROUP BY DIAGNOSISCODE, DIAGNOSISCODESET
        """)
        cur.execute(f"""
            ALTER TABLE {STAGING_ICD}
                ADD INDEX idx_diagcode_codeset(DIAGNOSISCODE(100), DIAGNOSISCODESET(50))
        """)
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")
    cur.execute(f"SELECT COUNT(*) FROM {STAGING_ICD}")
    print(f"    {cur.fetchone()[0]:,} ICD codes")

    # ── 7. diagnosiscode_dedup — MAX-agg DIAGNOSISCODE per code+contextid ─────
    print(f"  Materializing diagnosis code lookup ({STAGING_DC})...")
    if not _table_exists(cur, STAGING_DC):
        cur.execute(f"""
            CREATE TABLE {STAGING_DC} AS
            SELECT
                DIAGNOSISCODE,
                contextid,
                MAX(DIAGNOSISCODEDESCRIPTION) AS diagnosiscodedescription
            FROM `{SOURCE_SCHEMA}`.DIAGNOSISCODE
            WHERE ISDELETED = 0 OR ISDELETED IS NULL
            GROUP BY DIAGNOSISCODE, contextid
        """)
        cur.execute(f"""
            ALTER TABLE {STAGING_DC}
                ADD INDEX idx_diagcode_ctx(DIAGNOSISCODE(100), contextid)
        """)
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")
    cur.execute(f"SELECT COUNT(*) FROM {STAGING_DC}")
    print(f"    {cur.fetchone()[0]:,} diagnosis code descriptions")

    # ── 8. Destination table ──────────────────────────────────────────────────
    print("  Creating destination table...")
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {DEST_TABLE} (
            contextid                        VARCHAR(50)   DEFAULT NULL,
            claim_id                         BIGINT        DEFAULT NULL,
            patient_id                       BIGINT        DEFAULT NULL,
            service_date                     DATE          DEFAULT NULL,
            claim_created_datetime           DATETIME      DEFAULT NULL,
            appointment_id                   BIGINT        DEFAULT NULL,
            primary_claim_status             VARCHAR(100)  DEFAULT NULL,
            secondary_claim_status           VARCHAR(100)  DEFAULT NULL,
            patient_claim_status             VARCHAR(100)  DEFAULT NULL,
            rendering_provider_id            BIGINT        DEFAULT NULL,
            rendering_provider_name          VARCHAR(255)  DEFAULT NULL,
            rendering_provider_specialty     VARCHAR(255)  DEFAULT NULL,
            supervising_provider_id          BIGINT        DEFAULT NULL,
            supervising_provider_name        VARCHAR(255)  DEFAULT NULL,
            supervising_provider_specialty   VARCHAR(255)  DEFAULT NULL,
            service_department_name          VARCHAR(255)  DEFAULT NULL,
            service_department_city          VARCHAR(255)  DEFAULT NULL,
            encounter_id                     BIGINT        DEFAULT NULL,
            encounter_date                   VARCHAR(100)  DEFAULT NULL,
            claim_diagnosis_id               BIGINT        DEFAULT NULL,
            diagnosis_sequence               INT           DEFAULT NULL,
            claim_diagnosis_code             VARCHAR(50)   DEFAULT NULL,
            diagnosis_codeset                VARCHAR(50)   DEFAULT NULL,
            claim_diagnosis_description      VARCHAR(500)  DEFAULT NULL,
            diagnosis_priority               VARCHAR(50)   DEFAULT NULL,
            encounter_diagnosis_id           BIGINT        DEFAULT NULL,
            encounter_diagnosis_code         VARCHAR(50)   DEFAULT NULL,
            encounter_diagnosis_description  VARCHAR(500)  DEFAULT NULL,
            encounter_diagnosis_status       VARCHAR(100)  DEFAULT NULL,
            diagnosis_laterality             VARCHAR(100)  DEFAULT NULL,
            diagnosis_note                   TEXT,
            diagnosis_recorded_by            VARCHAR(100)  DEFAULT NULL,
            diagnosis_recorded_datetime      DATETIME      DEFAULT NULL,
            diagnosis_status_label           VARCHAR(100)  DEFAULT NULL,
            diagnosis_source_match           VARCHAR(50)   DEFAULT NULL,
            claim_diagnosis_created_datetime DATETIME      DEFAULT NULL,
            claim_diagnosis_created_by       VARCHAR(100)  DEFAULT NULL
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
    """)
    conn.commit()
    print("    ready")

    # ── 9. Checkpoint table ───────────────────────────────────────────────────
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

    # ── 10. PK staging + batch boundaries ────────────────────────────────────
    print(f"  Building PK staging ({STAGING_PK})...")
    if not _table_exists(cur, STAGING_PK):
        cur.execute(f"""
            CREATE TABLE {STAGING_PK} AS
            SELECT {BATCH_KEY}
            FROM {STAGING_CLAIM}
            WHERE {BATCH_KEY} IS NOT NULL
            ORDER BY {BATCH_KEY}
        """)
        cur.execute(f"ALTER TABLE {STAGING_PK} ADD INDEX idx_pk ({BATCH_KEY})")
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")

    cur.execute(f"SELECT COUNT(*) FROM {STAGING_PK}")
    total = cur.fetchone()[0]
    print(f"    {total:,} eligible claims")

    if total == 0:
        cur.close()
        conn.close()
        return []

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

    cur.close()
    conn.close()

    ranges = []
    for i, lo in enumerate(boundaries):
        hi = boundaries[i + 1] if i + 1 < len(boundaries) else max_pk + 1
        ranges.append((lo, hi))

    print(f"    → {len(ranges)} batches of ~{BATCH_SIZE:,} claims each")
    return ranges


# ── Runner ─────────────────────────────────────────────────────────────────────
def run_insert(ranges, pbar):
    conn = get_connection()

    if is_done(conn):
        conn.close()
        pbar.update(len(ranges))
        return {"status": "skipped", "rows": 0, "secs": 0}

    mark(conn, "running")
    t0         = time.time()
    total_rows = 0

    try:
        cur = conn.cursor()
        cur.execute("SET SESSION innodb_lock_wait_timeout = 3600")
        cur.execute("SET unique_checks = 0")
        cur.execute("SET foreign_key_checks = 0")

        for pk_lo, pk_hi in ranges:
            cur.execute(build_batch_insert(pk_lo, pk_hi))
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
        mark(conn, "failed", total_rows, str(exc))
        try:
            conn.close()
        except Exception:
            pass
        return {"status": f"FAILED: {exc}", "rows": total_rows, "secs": elapsed}


# ── Main ───────────────────────────────────────────────────────────────────────
def main():
    print(f"\n{'='*70}", flush=True)
    print(f"  AthenaOne Diagnosis Claims ETL — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  source     : {SOURCE_SCHEMA}.CLAIM  (contextid={CONTEXT_ID})")
    print(f"  dest       : {DEST_TABLE}")
    print(f"  checkpoint : {CHECKPOINT_TABLE}")
    print(f"  batch_size : {BATCH_SIZE:,}")
    print(f"{'='*70}\n", flush=True)

    print("Setup:")
    sys.stdout.flush()
    ranges = setup_tables()
    print()

    if not ranges:
        print("No rows to process. Exiting.")
        return

    result = None
    with tqdm(total=len(ranges), desc="diag_billing", unit="batch") as pbar:
        result = run_insert(ranges, pbar)

    print()
    tag = "DONE" if result["status"] == "done" \
          else "SKIP" if result["status"] == "skipped" \
          else "FAIL"
    print(f"\n{'='*70}")
    print(f"  [{tag}]  {result['rows']:>12,} rows  ({result['secs']}s)")
    if result["status"].startswith("FAILED"):
        print(f"  ERROR: {result['status']}")
    print(f"{'='*70}")

    print(f"\n  Cleanup SQL (run after verifying data):")
    print(f"    DROP TABLE IF EXISTS {STAGING_CLAIM};")
    print(f"    DROP TABLE IF EXISTS {STAGING_ENC};")
    print(f"    DROP TABLE IF EXISTS {STAGING_CD};")
    print(f"    DROP TABLE IF EXISTS {STAGING_CED};")
    print(f"    DROP TABLE IF EXISTS {STAGING_ICD};")
    print(f"    DROP TABLE IF EXISTS {STAGING_DC};")
    print(f"    DROP TABLE IF EXISTS {STAGING_PK};")
    print(f"    DROP TABLE IF EXISTS {CHECKPOINT_TABLE};")

    if result["status"].startswith("FAILED"):
        sys.exit(1)


if __name__ == "__main__":
    main()
