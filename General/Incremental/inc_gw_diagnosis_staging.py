#!/usr/bin/env python3
"""
inc_gw_diagnosis_staging.py — Incremental INSERT for udm_staging.diagnosis (Greenway / savannah)

SQL equivalent (flattened):
    INSERT INTO udm_staging.diagnosis (...)
    SELECT DISTINCT
        brvd.VisitDiagnosisID                          AS diag_id,
        v.PatientID                                    AS ndid,
        v.VisitID                                      AS eid,
        DATE(v.FromDateTime)                           AS enc_date,
        ...encounter_end_date CASE WHEN...,
        DATE(v.FromDateTime)                           AS diag_date,
        TRIM(brvd.DiagnosisCode)                       AS diag_code,
        CASE CodingSystemID → icd9/icd10 LONG_DESCRIPTION AS diag_desc,
        ...
        COALESCE(enc_date, diag_date)                  AS enc_date_proxy,
        CONCAT_WS(':',psid,ndid,eid,enc_date,diag_date,diag_id) AS udm_unq_id
    FROM savannah.br_VisitDiagnosis brvd
    INNER JOIN savannah.Visit v ON brvd.VisitID=v.VisitID AND brvd/v.nd_ActiveFlag='Y'
    LEFT JOIN semantics.icd9_fixed  icd9  ON diag_code_stripped = icd9.DIAGNOSIS_CODE
    LEFT JOIN semantics.icd10_fixed icd10 ON diag_code_stripped = icd10.CODE
    WHERE brvd.nd_ActiveFlag='Y' AND DATE(brvd.nd_extracted_date) > INCREMENTAL_DATE;

Pre-materialized lookup tables (computed ONCE, reused across all batches):
  - staging.inc_gw_diag_visit_{SOURCE_SCHEMA}  (Visit — nd_ActiveFlag='Y')

ICD9/ICD10 from semantics kept inline (small reference tables, simple indexed lookups).

Batching by VisitDiagnosisID.
Filter: brvd.nd_ActiveFlag='Y' AND DATE(brvd.nd_extracted_date) > INCREMENTAL_DATE.

Usage:
    python inc_gw_diagnosis_staging.py
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
SOURCE_SCHEMA    = "jwm"
PSID             = 11
INCREMENTAL_DATE = "2026-01-26"

DEST_TABLE = "udm_staging.diagnosis"

# ─────────────────────────────────────────────────────────────────────
STAGING_VISIT    = f"staging.inc_gw_diag_visit5_{SOURCE_SCHEMA}"   # Visit — nd_ActiveFlag='Y'
STAGING_CODES    = f"staging.inc_gw_diag_codes6_{SOURCE_SCHEMA}"    # pre-computed stripped codes (eligible rows)
STAGING_ICD9     = "staging.inc_gw_diag_icd9_n"                      # local copy of semantics.icd9_fixed (shared)
STAGING_ICD10    = "staging.inc_gw_diag_icd10_n"                      # local copy of semantics.icd10_fixed (shared)
CHECKPOINT_TABLE = f"staging.etl_checkpoint_inc_gw_diag7_{SOURCE_SCHEMA}"
CHECKPOINT_KEY   = f"inc_gw_diagnosis.{SOURCE_SCHEMA}"

BATCH_KEY = "VisitDiagnosisID"


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
    Joins through pre-materialized staging tables only — no cross-database JOINs,
    no COLLATE casts, no TRIM/REPLACE per row at batch time.
      dc  = STAGING_CODES  (pre-filtered eligible rows + pre-computed diag_code_stripped)
      v   = STAGING_VISIT  (active visits with enc_date/encounter_end_date)
      icd9/icd10 = local copies of semantics reference tables
    """
    return f"""
INSERT INTO {DEST_TABLE}
    (diag_id, ndid, eid, enc_date, encounter_end_date, diag_date,
     diag_code, diag_desc, diag_coding_system, diag_code_stripped,
     primary_diagnosis_flag, parent_diagnosis_code, parent_diagnosis_desc,
     icd_codeset, icd_codeset_desc, icd_codeset_group, icd_codeset_system,
     snomed_code, diag_severity, diag_status, diag_end_date,
     provisional_diag_flag, differential_diag_flag, comments_notes,
     diag_risk, specify, nd_extracted_date,
     created_datetime, created_by, updated_datetime, updated_by,
     ehr_source_name, source_path, data_type, psid, udm_inc_id,
     enc_date_proxy, udm_unq_id)
SELECT DISTINCT
    dc.{BATCH_KEY}                                                 AS diag_id,
    v.PatientID                                                    AS ndid,
    v.VisitID                                                      AS eid,
    v.enc_date                                                     AS enc_date,
    v.encounter_end_date                                           AS encounter_end_date,
    v.enc_date                                                     AS diag_date,
    dc.diag_code                                                   AS diag_code,
    CASE
        WHEN dc.CodingSystemID = 1020 THEN icd9.LONG_DESCRIPTION
        WHEN dc.CodingSystemID = 1016 THEN icd10.LONG_DESCRIPTION
    END                                                            AS diag_desc,
    CASE
        WHEN dc.CodingSystemID = 1020 THEN 'ICD-9'
        WHEN dc.CodingSystemID = 1016 THEN 'ICD-10'
        ELSE NULL
    END                                                            AS diag_coding_system,
    dc.diag_code_stripped                                          AS diag_code_stripped,
    CASE WHEN dc.Priority = 1 THEN 'Y' ELSE 'N' END               AS primary_diagnosis_flag,
    ''                                                             AS parent_diagnosis_code,
    ''                                                             AS parent_diagnosis_desc,
    ''                                                             AS icd_codeset,
    ''                                                             AS icd_codeset_desc,
    ''                                                             AS icd_codeset_group,
    ''                                                             AS icd_codeset_system,
    ''                                                             AS snomed_code,
    NULL                                                           AS diag_severity,
    NULL                                                           AS diag_status,
    NULL                                                           AS diag_end_date,
    NULL                                                           AS provisional_diag_flag,
    NULL                                                           AS differential_diag_flag,
    NULL                                                           AS comments_notes,
    NULL                                                           AS diag_risk,
    NULL                                                           AS specify,
    dc.nd_extracted_date                                           AS nd_extracted_date,
    CURRENT_DATE()                                                 AS created_datetime,
    'ND'                                                           AS created_by,
    CURRENT_DATE()                                                 AS updated_datetime,
    'ND'                                                           AS updated_by,
    'Greenway'                                                     AS ehr_source_name,
    'bronze_table'                                                 AS source_path,
    'Structured'                                                   AS data_type,
    '{PSID}'                                                       AS psid,
    NULL                                                           AS udm_inc_id,
    v.enc_date                                                     AS enc_date_proxy,
    CONCAT_WS(':',
        COALESCE('{PSID}',                                          ''),
        COALESCE(CAST(v.PatientID                         AS CHAR), ''),
        COALESCE(CAST(v.VisitID                           AS CHAR), ''),
        COALESCE(CAST(v.enc_date                          AS CHAR), ''),
        COALESCE(CAST(v.enc_date                          AS CHAR), ''),
        COALESCE(dc.diag_code_stripped,                             ''),
        COALESCE(CASE
            WHEN dc.CodingSystemID = 1020 THEN icd9.LONG_DESCRIPTION
            WHEN dc.CodingSystemID = 1016 THEN icd10.LONG_DESCRIPTION
        END,                                                        '')
    )                                                              AS udm_unq_id
FROM {STAGING_CODES} dc
INNER JOIN {STAGING_VISIT} v
    ON dc.VisitID = v.VisitID
LEFT JOIN {STAGING_ICD9} icd9
    ON dc.diag_code_stripped = icd9.DIAGNOSIS_CODE
LEFT JOIN {STAGING_ICD10} icd10
    ON dc.diag_code_stripped = icd10.CODE
WHERE dc.{BATCH_KEY} >= {pk_lo}
  AND dc.{BATCH_KEY} <  {pk_hi}
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

        # ── 2. Visit staging (active rows only) ──────────────────────
        # Materializes savannah.Visit WHERE nd_ActiveFlag='Y' once.
        # encounter_end_date is computed here on the raw source — ThroughDateTime
        # is VARCHAR in the source so empty-string guards work correctly. If we
        # stored ThroughDateTime as-is MySQL would cast it to DATETIME, turning ''
        # into 0000-00-00 which defeats the '' guard in later CASE expressions.
        print("  Materializing Visit lookup (nd_ActiveFlag='Y')...")
        if not _table_exists(cur, STAGING_VISIT):
            cur.execute(f"""
                CREATE TABLE {STAGING_VISIT} AS
                SELECT
                    VisitID,
                    PatientID,
                    DATE(FromDateTime) AS enc_date,
                    CASE
                        WHEN ThroughDateTime IS NULL
                          OR ThroughDateTime IN ('', 'None',
                             '0000-00-00', '0000-00-00 00:00:00')      THEN NULL
                        WHEN ThroughDateTime REGEXP
                             '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}'      THEN DATE(ThroughDateTime)
                        ELSE NULL
                    END AS encounter_end_date
                FROM {SOURCE_SCHEMA}.Visit
                WHERE nd_ActiveFlag = 'Y'
            """)
            cur.execute(
                f"ALTER TABLE {STAGING_VISIT} "
                f"ADD INDEX idx_visit (VisitID)"
            )
            conn.commit()
            print("    created")
        else:
            print("    already exists, reusing")
        cur.execute(f"SELECT COUNT(*) FROM {STAGING_VISIT}")
        print(f"    {cur.fetchone()[0]:,} rows")

        # ── 3. ICD9 local copy (shared across schemas) ───────────────
        print("  Materializing ICD9 lookup (local copy)...")
        if not _table_exists(cur, STAGING_ICD9):
            cur.execute(f"""
                CREATE TABLE {STAGING_ICD9} AS
                SELECT DIAGNOSIS_CODE, LONG_DESCRIPTION
                FROM semantics.icd9_fixed
            """)
            cur.execute(f"ALTER TABLE {STAGING_ICD9} ADD INDEX idx_code (DIAGNOSIS_CODE(20))")
            conn.commit()
            print("    created")
        else:
            print("    already exists, reusing")
        cur.execute(f"SELECT COUNT(*) FROM {STAGING_ICD9}")
        print(f"    {cur.fetchone()[0]:,} rows")

        # ── 4. ICD10 local copy (shared across schemas) ──────────────
        print("  Materializing ICD10 lookup (local copy)...")
        if not _table_exists(cur, STAGING_ICD10):
            cur.execute(f"""
                CREATE TABLE {STAGING_ICD10} AS
                SELECT CODE, LONG_DESCRIPTION
                FROM semantics.icd10_fixed
            """)
            cur.execute(f"ALTER TABLE {STAGING_ICD10} ADD INDEX idx_code (CODE(20))")
            conn.commit()
            print("    created")
        else:
            print("    already exists, reusing")
        cur.execute(f"SELECT COUNT(*) FROM {STAGING_ICD10}")
        print(f"    {cur.fetchone()[0]:,} rows")

        # ── 5. Pre-computed codes staging (eligible rows + stripped codes) ──
        # Computes TRIM/REPLACE once; batch INSERT reads pre-computed values.
        # Also carries VisitID so the INNER JOIN to STAGING_VISIT stays local.
        print(f"  Materializing diagnosis codes (nd_ActiveFlag='Y' AND nd_extracted_date > '{INCREMENTAL_DATE}')...")
        if not _table_exists(cur, STAGING_CODES):
            cur.execute(f"""
                CREATE TABLE {STAGING_CODES} AS
                SELECT
                    {BATCH_KEY},
                    VisitID,
                    CodingSystemID,
                    Priority,
                    TRIM(DiagnosisCode)                   AS diag_code,
                    TRIM(REPLACE(DiagnosisCode, '.', '')) AS diag_code_stripped,
                    DATE(nd_extracted_date)               AS nd_extracted_date
                FROM {SOURCE_SCHEMA}.br_VisitDiagnosis
                WHERE nd_ActiveFlag = 'Y'
                  AND DATE(nd_extracted_date) > '{INCREMENTAL_DATE}'
                  AND {BATCH_KEY} IS NOT NULL
            """)
            cur.execute(f"ALTER TABLE {STAGING_CODES} ADD INDEX idx_pk ({BATCH_KEY})")
            cur.execute(f"ALTER TABLE {STAGING_CODES} ADD INDEX idx_code (diag_code_stripped)")
            conn.commit()
            cur.execute(f"SELECT COUNT(*) FROM {STAGING_CODES}")
            n = cur.fetchone()[0]
            print(f"    created  ({n:,} eligible rows)")
        else:
            cur.execute(f"SELECT COUNT(*) FROM {STAGING_CODES}")
            n = cur.fetchone()[0]
            print(f"    already exists, reusing  ({n:,} rows)")

        ranges, total = _build_ranges(cur, STAGING_CODES)
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
    print(f"  Incremental GW Diagnosis Staging ETL — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  source schema    : {SOURCE_SCHEMA}  (psid={PSID})")
    print(f"  incremental date : > {INCREMENTAL_DATE}")
    print(f"  dest             : {DEST_TABLE}")
    print(f"  visit staging    : {STAGING_VISIT}")
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
    print(f"    DROP TABLE IF EXISTS {STAGING_CODES};")
    print(f"    DROP TABLE IF EXISTS {STAGING_ICD9};")
    print(f"    DROP TABLE IF EXISTS {STAGING_ICD10};")
    print(f"    DROP TABLE IF EXISTS {CHECKPOINT_TABLE};")
    print()

    if status.startswith("FAILED"):
        sys.exit(1)


if __name__ == "__main__":
    main()
