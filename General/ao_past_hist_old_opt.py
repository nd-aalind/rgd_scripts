#!/usr/bin/env python3
"""
Optimized ETL: Recreate 4 history tables in TARGET_SCHEMA from SOURCE_SCHEMA.

Tables dropped and recreated each run:
  - {TARGET_SCHEMA}.social_history
  - {TARGET_SCHEMA}.family_history
  - {TARGET_SCHEMA}.surgical_history
  - {TARGET_SCHEMA}.medical_history

Configure TARGET_SCHEMA, SOURCE_SCHEMA, PSID at the top of this file before running.
The script is designed to be reused across different EHR source schemas.

Optimizations applied:
- Per-table PK staging tables (distinct CHARTIDs only)
- Server-side boundary sampling for batch ranges
- Commit after every batch (frees undo/log space)
- Checkpoint/resume per table — re-run skips completed tables
- Disabled InnoDB checks per-session for bulk insert speed
- Progress bar via tqdm

Usage:
    python ao_past_hist_old_opt.py
"""

import sys
import time
from datetime import datetime
import pymysql
from tqdm import tqdm

# ── Configuration ─────────────────────────────────────────────────────
DB_CONFIG = {
    "host":            "ndai-dev-rds-instance.cwp60ymu4ko0.us-east-1.rds.amazonaws.com",
    "port":            3306,
    "user":            "admin",
    "password":        "ClAx5UNkjnM8JgLG",
    "charset":         "utf8mb4",
    "connect_timeout": 30,
    "read_timeout":    21600,
    "write_timeout":   21600,
}

TARGET_SCHEMA = "udm_raleigh"   # ← change per run (e.g. udm_noran, udm_kinsula_leq)
SOURCE_SCHEMA = "raleigh"       # ← change per run (e.g. noran, kinsula_leq)
PSID          = 5             # ← change per run

BATCH_SIZE = 50_000

# ── Derived staging / checkpoint names (namespaced by source schema) ──
_sfx = SOURCE_SCHEMA

STAGING_PK_SOCIAL   = f"staging.tmp_ao_social_pk_n_{_sfx}"
STAGING_PK_FAMILY   = f"staging.tmp_ao_family_pk_n_{_sfx}"
STAGING_PK_SURGICAL = f"staging.tmp_ao_surgical_pk_n_{_sfx}"
STAGING_PK_MEDICAL  = f"staging.tmp_ao_medical_pk_n_{_sfx}"

CHECKPOINT_TABLE = f"staging.etl_checkpoint_ao_hist_n_{_sfx}"
CK_SOCIAL        = "ao_hist.social_history_n"
CK_FAMILY        = "ao_hist.family_history_n"
CK_SURGICAL      = "ao_hist.surgical_history_n"
CK_MEDICAL       = "ao_hist.medical_history_n"

BATCH_KEY = "CHARTID"


# ── Date-parse CASE helper ─────────────────────────────────────────────

def _date_case(col):
    """Returns a SQL CASE expression that safely parses YYYY-MM-DD or MM-DD-YYYY strings."""
    return (
        f"CASE "
        f"WHEN {col} IN ('None', '') THEN NULL "
        f"WHEN LEFT({col}, 10) REGEXP '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}$' "
        f"    THEN STR_TO_DATE(LEFT({col}, 10), '%Y-%m-%d') "
        f"WHEN LEFT({col}, 10) REGEXP '^[0-9]{{2}}-[0-9]{{2}}-[0-9]{{4}}$' "
        f"    THEN STR_TO_DATE(LEFT({col}, 10), '%m-%d-%Y') "
        f"ELSE NULL END"
    )


# ── Batch INSERT builders ──────────────────────────────────────────────

def build_insert_social_history(pk_lo, pk_hi):
    enc_date  = _date_case("ce.ENCOUNTERDATE")
    soc_date2 = _date_case("ps.CREATEDDATETIME")
    return f"""
INSERT INTO {TARGET_SCHEMA}.social_history
SELECT
    shr.socialhxformresponseid  AS socialhistoryid,
    shr.CHARTID                 AS ndid,
    shr.CLINICALENCOUNTERID     AS eid,
    {enc_date}                  AS encounter_date,
    {enc_date}                  AS social_hist_date,
    'SocialHistory'             AS hist_category,
    shra.QUESTIONKEY            AS social_category,
    shra.VALUE                  AS social_option,
    shra.NOTE                   AS social_notes,
    CURRENT_DATE()              AS created_datetime,
    'ND'                        AS created_by,
    CURRENT_DATE()              AS updated_datetime,
    'ND'                        AS updated_by,
    'athenaone'                 AS ehr_source_name,
    'bronze_layer'              AS source_path,
    'Structured'                AS data_type,
    {PSID}                      AS psid
FROM (SELECT * FROM {SOURCE_SCHEMA}.SOCIALHXFORMRESPONSEANSWER WHERE nd_active_flag = 'Y') shra
INNER JOIN (SELECT * FROM {SOURCE_SCHEMA}.SOCIALHXFORMRESPONSE   WHERE nd_active_flag = 'Y') shr
    ON shra.socialhxformresponseid = shr.socialhxformresponseid
INNER JOIN (SELECT * FROM {SOURCE_SCHEMA}.CLINICALENCOUNTER       WHERE nd_active_flag = 'Y') ce
    ON shr.CHARTID = ce.CHARTID AND shr.CLINICALENCOUNTERID = ce.CLINICALENCOUNTERID
WHERE shr.{BATCH_KEY} >= {pk_lo}
  AND shr.{BATCH_KEY} <  {pk_hi}

UNION ALL

SELECT
    ps.socialhistoryid          AS socialhistoryid,
    ps.CHARTID                  AS ndid,
    NULL                        AS eid,
    NULL                        AS encounter_date,
    {soc_date2}                 AS social_hist_date,
    ps.socialhistorykey         AS hist_category,
    ps.socialhistoryname        AS social_category,
    ps.socialhistoryanswer      AS social_option,
    NULL                        AS social_notes,
    CURRENT_DATE()              AS created_datetime,
    'ND'                        AS created_by,
    CURRENT_DATE()              AS updated_datetime,
    'ND'                        AS updated_by,
    'athenaone'                 AS ehr_source_name,
    'bronze_layer'              AS source_path,
    'Structured'                AS data_type,
    {PSID}                      AS psid
FROM (SELECT * FROM {SOURCE_SCHEMA}.PATIENTSOCIALHISTORY WHERE nd_active_flag = 'Y') ps
WHERE ps.socialhistorykey <> 'REVIEWED.SOCIALHISTORY'
  AND ps.{BATCH_KEY} >= {pk_lo}
  AND ps.{BATCH_KEY} <  {pk_hi}
"""


def build_insert_family_history(pk_lo, pk_hi):
    fam_date = _date_case("pf.CREATEDDATETIME")
    return f"""
INSERT INTO {TARGET_SCHEMA}.family_history
SELECT
    pf.familyhistoryid          AS familyhistoryid,
    pf.CHARTID                  AS ndid,
    NULL                        AS eid,
    NULL                        AS encounter_date,
    {fam_date}                  AS fam_hist_date,
    'FamilyHistory'             AS hist_category,
    pf.relation                 AS fam_hist_relation,
    pf.familyhistoryproblem     AS fam_hist_detail,
    CURRENT_DATE()              AS created_datetime,
    'ND'                        AS created_by,
    CURRENT_DATE()              AS updated_datetime,
    'ND'                        AS updated_by,
    'athenaone'                 AS ehr_source_name,
    'bronze_layer'              AS source_path,
    'Structured'                AS data_type,
    {PSID}                      AS psid
FROM (SELECT * FROM {SOURCE_SCHEMA}.PATIENTFAMILYHISTORY WHERE nd_active_flag = 'Y') pf
WHERE pf.{BATCH_KEY} >= {pk_lo}
  AND pf.{BATCH_KEY} <  {pk_hi}
"""


def build_insert_surgical_history(pk_lo, pk_hi):
    sdate1a = _date_case("ps.SURGERYDATETIME")
    sdate1b = _date_case("ps.CREATEDDATETIME")
    sdate2a = _date_case("psh.SURGERYDATEDATETIME")
    sdate2b = _date_case("psh.CREATEDDATETIME")
    return f"""
INSERT INTO {TARGET_SCHEMA}.surgical_history
SELECT
    ps.PATIENTSURGERYID         AS surgicalhistoryid,
    ps.CHARTID                  AS ndid,
    NULL                        AS eid,
    NULL                        AS encounter_date,
    COALESCE({sdate1a}, {sdate1b}) AS surgery_date,
    ps.type                     AS surg_hist_type,
    ps.procedure                AS surgery_name,
    COALESCE(ps.snomedcode, ps.procedurecode) AS surgery_code,
    CASE WHEN ps.snomedcode    IS NOT NULL THEN 'SNOMED'
         WHEN ps.procedurecode IS NOT NULL THEN 'CPT/HCPCS' END AS surgery_coding_system,
    NULL                        AS surgery_reason,
    CURRENT_DATE()              AS created_datetime,
    'ND'                        AS created_by,
    CURRENT_DATE()              AS updated_datetime,
    'ND'                        AS updated_by,
    'athenaone'                 AS ehr_source_name,
    'bronze_layer'              AS source_path,
    'Structured'                AS data_type,
    {PSID}                      AS psid
FROM (SELECT * FROM {SOURCE_SCHEMA}.PATIENTSURGERY WHERE nd_active_flag = 'Y') ps
WHERE ps.type <> 'REVIEWED.PATIENTSURGICALHISTORY'
  AND ps.{BATCH_KEY} >= {pk_lo}
  AND ps.{BATCH_KEY} <  {pk_hi}

UNION ALL

SELECT
    psh.PATIENTSURGICALHISTORYid   AS surgicalhistoryid,
    psh.CHARTID                     AS ndid,
    NULL                            AS eid,
    NULL                            AS encounter_date,
    COALESCE({sdate2a}, {sdate2b})  AS surgery_date,
    'PATIENTSURGICALHISTORY'        AS surg_hist_type,
    COALESCE(shp.NAME, s.DESCRIPTION) AS surgery_name,
    COALESCE(psh.snomedcode, psh.procedurecode) AS surgery_code,
    CASE WHEN psh.snomedcode    IS NOT NULL THEN 'SNOMED'
         WHEN psh.procedurecode IS NOT NULL THEN 'CPT/HCPCS' END AS surgery_coding_system,
    psh.note                        AS surgery_reason,
    CURRENT_DATE()                  AS created_datetime,
    'ND'                            AS created_by,
    CURRENT_DATE()                  AS updated_datetime,
    'ND'                            AS updated_by,
    'athenaone'                     AS ehr_source_name,
    'bronze_layer'                  AS source_path,
    'Structured'                    AS data_type,
    {PSID}                          AS psid
FROM (SELECT * FROM {SOURCE_SCHEMA}.PATIENTSURGICALHISTORY WHERE nd_active_flag = 'Y') psh
LEFT JOIN (SELECT * FROM {SOURCE_SCHEMA}.SNOMED WHERE nd_active_flag = 'Y') s
    ON psh.snomedcode = s.SNOMEDCODE
LEFT JOIN (SELECT * FROM {SOURCE_SCHEMA}.SURGICALHISTORYPROCEDURE WHERE nd_active_flag = 'Y') shp
    ON psh.SURGICALHISTORYPROCEDUREID = shp.SURGICALHISTORYPROCEDUREID
WHERE psh.{BATCH_KEY} >= {pk_lo}
  AND psh.{BATCH_KEY} <  {pk_hi}
"""


def build_insert_medical_history(pk_lo, pk_hi):
    med_date = _date_case("pm.CREATEDDATETIME")
    return f"""
INSERT INTO {TARGET_SCHEMA}.medical_history
SELECT
    pm.PASTMEDICALHISTORYID         AS PASTMEDICALHISTORYID,
    pm.CHARTID                      AS ndid,
    NULL                            AS eid,
    NULL                            AS encounter_date,
    {med_date}                      AS med_hist_date,
    'MedicalHistory'                AS hist_category,
    pm.pastmedicalhistorykey        AS med_hist_category,
    pm.pastmedicalhistoryquestion   AS med_hist_question,
    pm.pastmedicalhistoryanswer     AS med_hist_answer,
    CURRENT_DATE()                  AS created_datetime,
    'ND'                            AS created_by,
    CURRENT_DATE()                  AS updated_datetime,
    'ND'                            AS updated_by,
    'athenaone'                     AS ehr_source_name,
    'bronze_layer'                  AS source_path,
    'Structured'                    AS data_type,
    {PSID}                          AS psid
FROM (SELECT * FROM {SOURCE_SCHEMA}.PATIENTPASTMEDICALHISTORY WHERE nd_active_flag = 'Y') pm
WHERE pm.pastmedicalhistorykey <> 'REVIEWED.PASTMEDICALHISTORY'
  AND pm.{BATCH_KEY} >= {pk_lo}
  AND pm.{BATCH_KEY} <  {pk_hi}
"""


# ── Helpers ───────────────────────────────────────────────────────────

def get_connection():
    return pymysql.connect(**DB_CONFIG)


def _index_exists(cur, schema, table, column):
    """Return True if any index already covers column as its first key part."""
    cur.execute(
        "SELECT COUNT(*) FROM information_schema.statistics "
        "WHERE table_schema = %s AND table_name = %s "
        "  AND column_name = %s AND seq_in_index = 1",
        (schema, table, column),
    )
    return cur.fetchone()[0] > 0


def ensure_indexes(cur, conn):
    """
    Check and create missing indexes on source tables used in JOIN/WHERE/batch conditions.
    Skips silently if the index already exists.
    """
    # (schema, table, column)
    needed = [
        # ── social_history source tables ─────────────────────────────
        (SOURCE_SCHEMA, "SOCIALHXFORMRESPONSEANSWER", "nd_active_flag"),
        (SOURCE_SCHEMA, "SOCIALHXFORMRESPONSEANSWER", "socialhxformresponseid"),
        (SOURCE_SCHEMA, "SOCIALHXFORMRESPONSE",       "nd_active_flag"),
        (SOURCE_SCHEMA, "SOCIALHXFORMRESPONSE",       "socialhxformresponseid"),
        (SOURCE_SCHEMA, "SOCIALHXFORMRESPONSE",       "CHARTID"),
        (SOURCE_SCHEMA, "CLINICALENCOUNTER",           "nd_active_flag"),
        (SOURCE_SCHEMA, "CLINICALENCOUNTER",           "CHARTID"),
        (SOURCE_SCHEMA, "CLINICALENCOUNTER",           "CLINICALENCOUNTERID"),
        (SOURCE_SCHEMA, "PATIENTSOCIALHISTORY",        "nd_active_flag"),
        (SOURCE_SCHEMA, "PATIENTSOCIALHISTORY",        "CHARTID"),
        # ── family_history source tables ─────────────────────────────
        (SOURCE_SCHEMA, "PATIENTFAMILYHISTORY",        "nd_active_flag"),
        (SOURCE_SCHEMA, "PATIENTFAMILYHISTORY",        "CHARTID"),
        # ── surgical_history source tables ────────────────────────────
        (SOURCE_SCHEMA, "PATIENTSURGERY",              "nd_active_flag"),
        (SOURCE_SCHEMA, "PATIENTSURGERY",              "CHARTID"),
        (SOURCE_SCHEMA, "PATIENTSURGICALHISTORY",      "nd_active_flag"),
        (SOURCE_SCHEMA, "PATIENTSURGICALHISTORY",      "CHARTID"),
        (SOURCE_SCHEMA, "PATIENTSURGICALHISTORY",      "SURGICALHISTORYPROCEDUREID"),
        (SOURCE_SCHEMA, "SNOMED",                      "nd_active_flag"),
        (SOURCE_SCHEMA, "SNOMED",                      "SNOMEDCODE"),
        (SOURCE_SCHEMA, "SURGICALHISTORYPROCEDURE",    "nd_active_flag"),
        (SOURCE_SCHEMA, "SURGICALHISTORYPROCEDURE",    "SURGICALHISTORYPROCEDUREID"),
        # ── medical_history source tables ─────────────────────────────
        (SOURCE_SCHEMA, "PATIENTPASTMEDICALHISTORY",   "nd_active_flag"),
        (SOURCE_SCHEMA, "PATIENTPASTMEDICALHISTORY",   "CHARTID"),
    ]

    print("  Checking source table indexes...")
    created = []
    for schema, table, col in needed:
        if not _index_exists(cur, schema, table, col):
            idx_name = f"idx_etl_{col.lower()}"
            print(f"    creating index on {schema}.{table}({col})...")
            cur.execute(f"ALTER TABLE `{schema}`.`{table}` ADD INDEX `{idx_name}` (`{col}`)")
            conn.commit()
            created.append(f"{table}.{col}")
        # else: already exists, skip silently

    if created:
        print(f"    created {len(created)} index(es): {', '.join(created)}")
    else:
        print("    all indexes already present")


def _table_exists(cur, full_table_name):
    schema, table = full_table_name.split(".")
    cur.execute(
        "SELECT COUNT(*) FROM information_schema.tables "
        "WHERE table_schema = %s AND table_name = %s",
        (schema, table),
    )
    return cur.fetchone()[0] > 0


def _build_ranges(cur, staging_pk):
    """Compute batch boundary ranges from a PK staging table."""
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


# ── Checkpoint ────────────────────────────────────────────────────────

def is_done(conn, ck_key):
    cur = conn.cursor()
    cur.execute(
        f"SELECT status FROM {CHECKPOINT_TABLE} WHERE source_key = %s",
        (ck_key,),
    )
    row = cur.fetchone()
    cur.close()
    return row is not None and row[0] == "done"


def mark(conn, ck_key, status, rows=0, error=None):
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
    """, (ck_key, status, rows, status, error))
    conn.commit()
    cur.close()


# ── Setup ──────────────────────────────────────────────────────────────

def setup_tables():
    """
    1. Drop and recreate all 4 destination tables (empty).
    2. Create PK staging tables (distinct CHARTIDs per source table/filter).
    3. Create checkpoint table.
    4. Compute and return batch ranges per table.
    """
    conn = get_connection()
    cur  = conn.cursor()

    # ── 0. Ensure source table indexes exist ──────────────────────────
    ensure_indexes(cur, conn)

    # ── 1. Destination tables — explicit DDL (avoids schema inference issues) ──
    dest_ddls = [
        (
            f"{TARGET_SCHEMA}.social_history",
            f"""CREATE TABLE {TARGET_SCHEMA}.social_history (
                    socialhistoryid  BIGINT,
                    ndid             BIGINT,
                    eid              BIGINT,
                    encounter_date   DATE,
                    social_hist_date DATE,
                    hist_category    VARCHAR(500),
                    social_category  TEXT,
                    social_option    TEXT,
                    social_notes     TEXT,
                    created_datetime DATE,
                    created_by       VARCHAR(10),
                    updated_datetime DATE,
                    updated_by       VARCHAR(10),
                    ehr_source_name  VARCHAR(50),
                    source_path      VARCHAR(50),
                    data_type        VARCHAR(50),
                    psid             INT
                ) ENGINE=InnoDB ROW_FORMAT=DYNAMIC""",
        ),
        (
            f"{TARGET_SCHEMA}.family_history",
            f"""CREATE TABLE {TARGET_SCHEMA}.family_history (
                    familyhistoryid  BIGINT,
                    ndid             BIGINT,
                    eid              BIGINT,
                    encounter_date   DATE,
                    fam_hist_date    DATE,
                    hist_category    VARCHAR(50),
                    fam_hist_relation VARCHAR(500),
                    fam_hist_detail  TEXT,
                    created_datetime DATE,
                    created_by       VARCHAR(10),
                    updated_datetime DATE,
                    updated_by       VARCHAR(10),
                    ehr_source_name  VARCHAR(50),
                    source_path      VARCHAR(50),
                    data_type        VARCHAR(50),
                    psid             INT
                ) ENGINE=InnoDB ROW_FORMAT=DYNAMIC""",
        ),
        (
            f"{TARGET_SCHEMA}.surgical_history",
            f"""CREATE TABLE {TARGET_SCHEMA}.surgical_history (
                    surgicalhistoryid    BIGINT,
                    ndid                 BIGINT,
                    eid                  BIGINT,
                    encounter_date       DATE,
                    surgery_date         DATE,
                    surg_hist_type       VARCHAR(500),
                    surgery_name         TEXT,
                    surgery_code         VARCHAR(100),
                    surgery_coding_system VARCHAR(20),
                    surgery_reason       TEXT,
                    created_datetime     DATE,
                    created_by           VARCHAR(10),
                    updated_datetime     DATE,
                    updated_by           VARCHAR(10),
                    ehr_source_name      VARCHAR(50),
                    source_path          VARCHAR(50),
                    data_type            VARCHAR(50),
                    psid                 INT
                ) ENGINE=InnoDB ROW_FORMAT=DYNAMIC""",
        ),
        (
            f"{TARGET_SCHEMA}.medical_history",
            f"""CREATE TABLE {TARGET_SCHEMA}.medical_history (
                    PASTMEDICALHISTORYID   BIGINT,
                    ndid                   BIGINT,
                    eid                    BIGINT,
                    encounter_date         DATE,
                    med_hist_date          DATE,
                    hist_category          VARCHAR(50),
                    med_hist_category      VARCHAR(500),
                    med_hist_question      TEXT,
                    med_hist_answer        TEXT,
                    created_datetime       DATE,
                    created_by             VARCHAR(10),
                    updated_datetime       DATE,
                    updated_by             VARCHAR(10),
                    ehr_source_name        VARCHAR(50),
                    source_path            VARCHAR(50),
                    data_type              VARCHAR(50),
                    psid                   INT
                ) ENGINE=InnoDB ROW_FORMAT=DYNAMIC""",
        ),
    ]

    for dest_full, ddl in dest_ddls:
        print(f"  Recreating {dest_full}...")
        cur.execute(f"DROP TABLE IF EXISTS {dest_full}")
        cur.execute(ddl)
        conn.commit()
        print(f"    created (empty)")

    # ── 2. PK staging tables ──────────────────────────────────────────
    pk_stagings = [
        {
            "name":  STAGING_PK_SOCIAL,
            "label": "social_history CHARTIDs",
            "sql":   f"""SELECT DISTINCT {BATCH_KEY} FROM (
                            SELECT CHARTID FROM {SOURCE_SCHEMA}.SOCIALHXFORMRESPONSE
                            WHERE nd_active_flag = 'Y' AND CHARTID IS NOT NULL
                            UNION
                            SELECT CHARTID FROM {SOURCE_SCHEMA}.PATIENTSOCIALHISTORY
                            WHERE nd_active_flag = 'Y'
                              AND socialhistorykey <> 'REVIEWED.SOCIALHISTORY'
                              AND CHARTID IS NOT NULL
                        ) t""",
        },
        {
            "name":  STAGING_PK_FAMILY,
            "label": "family_history CHARTIDs",
            "sql":   f"""SELECT DISTINCT {BATCH_KEY} FROM {SOURCE_SCHEMA}.PATIENTFAMILYHISTORY
                         WHERE nd_active_flag = 'Y' AND {BATCH_KEY} IS NOT NULL""",
        },
        {
            "name":  STAGING_PK_SURGICAL,
            "label": "surgical_history CHARTIDs",
            "sql":   f"""SELECT DISTINCT {BATCH_KEY} FROM (
                            SELECT CHARTID FROM {SOURCE_SCHEMA}.PATIENTSURGERY
                            WHERE nd_active_flag = 'Y'
                              AND type <> 'REVIEWED.PATIENTSURGICALHISTORY'
                              AND CHARTID IS NOT NULL
                            UNION
                            SELECT CHARTID FROM {SOURCE_SCHEMA}.PATIENTSURGICALHISTORY
                            WHERE nd_active_flag = 'Y' AND CHARTID IS NOT NULL
                        ) t""",
        },
        {
            "name":  STAGING_PK_MEDICAL,
            "label": "medical_history CHARTIDs",
            "sql":   f"""SELECT DISTINCT {BATCH_KEY} FROM {SOURCE_SCHEMA}.PATIENTPASTMEDICALHISTORY
                         WHERE nd_active_flag = 'Y'
                           AND pastmedicalhistorykey <> 'REVIEWED.PASTMEDICALHISTORY'
                           AND {BATCH_KEY} IS NOT NULL""",
        },
    ]

    for ps in pk_stagings:
        print(f"  Creating PK staging for {ps['label']}...")
        if _table_exists(cur, ps["name"]):
            cur.execute(f"DROP TABLE {ps['name']}")
        cur.execute(f"CREATE TABLE {ps['name']} AS {ps['sql']}")
        cur.execute(f"ALTER TABLE {ps['name']} ADD INDEX idx_pk ({BATCH_KEY})")
        conn.commit()
        cur.execute(f"SELECT COUNT(*) FROM {ps['name']}")
        print(f"    {cur.fetchone()[0]:,} distinct CHARTIDs")

    # ── 3. Checkpoint table ───────────────────────────────────────────
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

    # ── 4. Compute batch ranges ───────────────────────────────────────
    all_ranges = {
        CK_SOCIAL:   _build_ranges(cur, STAGING_PK_SOCIAL)[0],
        CK_FAMILY:   _build_ranges(cur, STAGING_PK_FAMILY)[0],
        CK_SURGICAL: _build_ranges(cur, STAGING_PK_SURGICAL)[0],
        CK_MEDICAL:  _build_ranges(cur, STAGING_PK_MEDICAL)[0],
    }

    for ck, ranges in all_ranges.items():
        print(f"    {ck.split('.')[-1]}: {len(ranges)} batches")

    cur.close()
    conn.close()
    return all_ranges


# ── Runner ─────────────────────────────────────────────────────────────

def run_table(ck_key, build_fn, ranges, pbar):
    conn = get_connection()

    if is_done(conn, ck_key):
        conn.close()
        pbar.update(len(ranges))
        return {"status": "skipped", "rows": 0, "secs": 0}

    mark(conn, ck_key, "running")
    t0 = time.time()
    total_rows = 0

    try:
        cur = conn.cursor()
        cur.execute("SET unique_checks = 0")
        cur.execute("SET foreign_key_checks = 0")

        for pk_lo, pk_hi in ranges:
            sql = build_fn(pk_lo, pk_hi)
            cur.execute(sql)
            conn.commit()
            total_rows += cur.rowcount
            pbar.update(1)

        cur.execute("SET unique_checks = 1")
        cur.execute("SET foreign_key_checks = 1")
        cur.close()

        elapsed = round(time.time() - t0, 1)
        mark(conn, ck_key, "done", total_rows)
        conn.close()
        return {"status": "done", "rows": total_rows, "secs": elapsed}

    except Exception as exc:
        elapsed = round(time.time() - t0, 1)
        mark(conn, ck_key, "failed", total_rows, str(exc))
        try:
            conn.close()
        except Exception:
            pass
        return {"status": f"FAILED: {exc}", "rows": total_rows, "secs": elapsed}


# ── Main ───────────────────────────────────────────────────────────────

def main():
    print(f"\n{'='*70}", flush=True)
    print(f"  AO Past History ETL — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  source_schema : {SOURCE_SCHEMA}")
    print(f"  target_schema : {TARGET_SCHEMA}")
    print(f"  psid          : {PSID}")
    print(f"  batch_size    : {BATCH_SIZE:,}")
    print(f"  tables        : social_history | family_history | surgical_history | medical_history")
    print(f"{'='*70}\n", flush=True)

    print("  Setting up tables...", flush=True)
    all_ranges = setup_tables()

    tables = [
        (CK_SOCIAL,   "social_history",   build_insert_social_history),
        (CK_FAMILY,   "family_history",   build_insert_family_history),
        (CK_SURGICAL, "surgical_history", build_insert_surgical_history),
        (CK_MEDICAL,  "medical_history",  build_insert_medical_history),
    ]

    results = {}
    any_failed = False

    total_batches = sum(len(all_ranges.get(ck, [])) for ck, _, _ in tables)
    with tqdm(total=total_batches, desc="Overall", unit="batch") as pbar:
        for ck_key, label, build_fn in tables:
            ranges = all_ranges.get(ck_key, [])

            if not ranges:
                print(f"\n  [SKIP] {label} — no eligible rows")
                continue

            print(f"\n  Starting {label} ({len(ranges)} batches)...")
            result = run_table(ck_key, build_fn, ranges, pbar)
            results[ck_key] = result

            if result["status"].startswith("FAILED"):
                print(f"\n  FAILED at {label}: {result['status']}")
                print("  Aborting remaining tables.")
                any_failed = True
                break

    print(f"\n{'='*70}")
    print(f"  Per-table summary:")
    total_rows = 0
    for ck_key, label, _ in tables:
        res = results.get(ck_key, {"status": "not run", "rows": 0, "secs": 0})
        status = res["status"]
        rows   = res["rows"]
        secs   = res["secs"]
        if status == "done":
            tag = " DONE"; total_rows += rows
        elif status == "skipped":
            tag = " SKIP"
        elif status == "not run":
            tag = "  ---"
        else:
            tag = " FAIL"; any_failed = True
        print(f"  [{tag}] {label:<22}  {rows:>10,} rows  ({secs}s)")
        if status.startswith("FAILED"):
            print(f"         {status}")

    print(f"\n  Total rows inserted : {total_rows:,}")
    print(f"{'='*70}")

    print(f"\n  Cleanup SQL (run after verifying data):")
    for t in [STAGING_PK_SOCIAL, STAGING_PK_FAMILY, STAGING_PK_SURGICAL,
              STAGING_PK_MEDICAL, CHECKPOINT_TABLE]:
        print(f"    DROP TABLE IF EXISTS {t};")

    if any_failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
