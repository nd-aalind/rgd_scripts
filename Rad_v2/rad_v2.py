#!/usr/bin/env python3
"""
Optimized ETL for: reporting_raleigh.patient_radiology_v2
Source: athenaone — CLINICALRESULT (IMAGING orders)

Single INSERT job driven by CLINICALRESULT.CLINICALRESULTID.

Pre-materialized lookup tables (computed ONCE, reused across all batches):
  - staging.rad_v2_cro_v1_{schema}   (CLINICALRESULTOBSERVATION — img_status, img_finding,
                                       internal_notes; replaces 3 correlated subqueries)
  - staging.rad_v2_prov_v1_{schema}  (PROVIDER — BILLEDNAME by latest CREATEDDATETIME)
  - staging.rad_v2_diag_v1_{schema}  (DOCUMENTDIAGNOSIS codes aggregated per DOCUMENTID)

Optimizations:
- All correlated subqueries replaced by pre-materialized LEFT JOINs
- Batching by actual CLINICALRESULTID values (sparse-ID safe)
- Commit after every batch
- Checkpoint/resume — re-run skips if already completed
- InnoDB checks disabled per-session for bulk speed
- tqdm progress bar

Usage:
    python rad_v2.py
"""

import sys
import time
from datetime import datetime
import pymysql
from tqdm import tqdm
import os
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())

# ── Configuration ─────────────────────────────────────────────────────────────
DB_CONFIG = {
    "host":            os.environ.get("DB_INTERNAL_HOST"),
    "port":            3306,
    "user":            os.environ.get("DB_INTERNAL_USER"),
    "password":        os.environ.get("DB_INTERNAL_PASSWORD"),
    "database":        "reporting_raleigh",
    "charset":         "utf8mb4",
    "connect_timeout": 30,
    "read_timeout":    21600,
    "write_timeout":   21600,
}

BATCH_SIZE = 50_000

SOURCE_SCHEMA = "athenaone"

DEST_TABLE       = "reporting_raleigh.patient_radiology_v2"
STAGING_CRO      = f"staging.rad_v2_cro_v1_{SOURCE_SCHEMA}"
STAGING_PROV     = f"staging.rad_v2_prov_v1_{SOURCE_SCHEMA}"
STAGING_DIAG     = f"staging.rad_v2_diag_v1_{SOURCE_SCHEMA}"
STAGING_TABLE    = f"staging.tmp_rad_v2_pk_{SOURCE_SCHEMA}"
CHECKPOINT_TABLE = f"staging.etl_checkpoint_rad_v2_{SOURCE_SCHEMA}"
CHECKPOINT_KEY   = f"rad_v2.insert.{SOURCE_SCHEMA}"

BATCH_KEY = "CLINICALRESULTID"


# ── Date CASE helper ──────────────────────────────────────────────────────────

def obs_date_case(col):
    """Parse OBSERVATIONDATETIME — handles date-only and datetime string variants."""
    return (
        f"CASE\n"
        f"        WHEN {col} IS NULL THEN NULL\n"
        f"        WHEN {col} REGEXP '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}$'\n"
        f"            THEN DATE(STR_TO_DATE({col}, '%Y-%m-%d'))\n"
        f"        WHEN {col} REGEXP '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}} [0-9]{{2}}:[0-9]{{2}}:[0-9]{{2}}$'\n"
        f"            THEN DATE(STR_TO_DATE({col}, '%Y-%m-%d %H:%i:%s'))\n"
        f"        WHEN {col} REGEXP '^[0-9]{{2}}-[0-9]{{2}}-[0-9]{{4}}$'\n"
        f"            THEN DATE(STR_TO_DATE({col}, '%m-%d-%Y'))\n"
        f"        WHEN {col} REGEXP '^[0-9]{{2}}-[0-9]{{2}}-[0-9]{{4}} [0-9]{{2}}:[0-9]{{2}}:[0-9]{{2}}$'\n"
        f"            THEN DATE(STR_TO_DATE({col}, '%m-%d-%Y %H:%i:%s'))\n"
        f"        ELSE NULL\n"
        f"    END"
    )


# ── Batch INSERT builder ──────────────────────────────────────────────────────

def build_batch_insert(pk_lo, pk_hi):
    cr       = "cr"
    d        = "d"
    ce       = "ce"
    cp       = "cp"
    obs_date = obs_date_case(f"{cr}.OBSERVATIONDATETIME")
    return f"""
INSERT INTO {DEST_TABLE}
    (patientid, result_id, ndid, eid, enc_date, enc_id,
     created_datetime, order_date, perform_date, img_date,
     study_name, modality, result_status, img_status, order_status,
     img_finding, img_report_text, report_id, order_id, report_date,
     report_status, order_prescription, provider_id, facility_name,
     provider_npi, internal_notes, note_to_patient, facility,
     interpretation, source, diagnosis_codes, report_text)
SELECT
    {d}.patientid,
    {cr}.CLINICALRESULTID,
    COALESCE({d}.CHARTID, {ce}.CHARTID),
    {d}.CLINICALENCOUNTERID,
    DATE({ce}.ENCOUNTERDATE),
    {ce}.CLINICALENCOUNTERID,
    {cr}.CREATEDDATETIME,
    {d}.ORDERDATETIME,
    {d}.OBSERVATIONDATETIME,
    {obs_date},
    {cr}.CLINICALORDERTYPE,
    {cr}.CLINICALORDERGENUS,
    {cr}.RESULTSTATUS,
    cro.img_status,
    {cr}.REPORTSTATUS,
    COALESCE({d}.DOCUMENTTEXTDATA, {d}.RESULTNOTES, cro.img_finding),
    COALESCE({d}.DOCUMENTTEXTDATA, {d}.RESULTNOTES),
    {d}.DOCUMENTID,
    {cr}.ORDERDOCUMENTID,
    {obs_date},
    {d}.STATUS,
    {cr}.ORDERDOCUMENTID,
    {cr}.CLINICALPROVIDERID,
    COALESCE(prov.BILLEDNAME, {cp}.NAME),
    {cp}.NPI,
    CONCAT(COALESCE(cro.internal_notes, ''), ' - ', COALESCE({d}.PROVIDERNOTE, '')),
    {cr}.EXTERNALNOTE,
    {d}.DEPARTMENTID,
    {d}.INTERNALINTERPRETATION,
    {d}.SOURCE,
    diag.diagnosis_codes,
    COALESCE({d}.DOCUMENTTEXTDATA, {d}.RESULTNOTES)
FROM {SOURCE_SCHEMA}.CLINICALRESULT {cr}
JOIN {SOURCE_SCHEMA}.DOCUMENT {d}
    ON {cr}.DOCUMENTID = {d}.DOCUMENTID
   AND {d}.nd_active_flag = 'Y'
LEFT JOIN {SOURCE_SCHEMA}.CLINICALENCOUNTER {ce}
    ON {d}.CHARTID = {ce}.CHARTID
   AND {d}.CLINICALENCOUNTERID = {ce}.CLINICALENCOUNTERID
   AND {ce}.nd_active_flag = 'Y'
LEFT JOIN {SOURCE_SCHEMA}.CLINICALPROVIDER {cp}
    ON {cr}.CLINICALPROVIDERID = {cp}.CLINICALPROVIDERID
   AND {cp}.nd_active_flag = 'Y'
LEFT JOIN {STAGING_CRO} cro
    ON cro.CLINICALRESULTID = {cr}.CLINICALRESULTID
LEFT JOIN {STAGING_PROV} prov
    ON prov.PROVIDERID = {cp}.PROVIDERID
LEFT JOIN {STAGING_DIAG} diag
    ON diag.DOCUMENTID = {d}.DOCUMENTID
WHERE {cr}.CLINICALORDERTYPEGROUP = 'IMAGING'
  AND {cr}.nd_active_flag = 'Y'
  AND {cr}.{BATCH_KEY} >= {pk_lo}
  AND {cr}.{BATCH_KEY} <  {pk_hi}
"""


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


def _ensure_index(cur, conn, full_table_name, index_name, columns):
    schema, table = full_table_name.split(".", 1)
    cur.execute(
        "SELECT COUNT(*) FROM information_schema.statistics "
        "WHERE table_schema = %s AND table_name = %s AND index_name = %s",
        (schema, table, index_name),
    )
    if cur.fetchone()[0] > 0:
        print(f"    index {index_name} on {full_table_name} already exists — skipping")
        return
    col_list = ", ".join(columns)
    print(f"    creating index {index_name} on {full_table_name}({col_list}) ...")
    cur.execute(f"ALTER TABLE {full_table_name} ADD INDEX {index_name} ({col_list})")
    conn.commit()
    print(f"    done")


# ── Checkpoint ────────────────────────────────────────────────────────────────

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


# ── Setup ─────────────────────────────────────────────────────────────────────

def setup_tables():
    conn = get_connection()
    cur  = conn.cursor()

    # ── 1. Indexes on source tables ───────────────────────────────────────────
    print("  Ensuring indexes on source tables...")
    _ensure_index(cur, conn, f"{SOURCE_SCHEMA}.CLINICALRESULT",
                  "idx_ordertypegroup", ["CLINICALORDERTYPEGROUP"])
    _ensure_index(cur, conn, f"{SOURCE_SCHEMA}.CLINICALRESULT",
                  "idx_nd_active",      ["nd_active_flag"])
    _ensure_index(cur, conn, f"{SOURCE_SCHEMA}.CLINICALRESULT",
                  "idx_documentid",     ["DOCUMENTID"])
    _ensure_index(cur, conn, f"{SOURCE_SCHEMA}.DOCUMENT",
                  "idx_nd_active",      ["nd_active_flag"])

    # ── 2. Pre-materialize CLINICALRESULTOBSERVATION ──────────────────────────
    # Collapses 3 correlated subqueries (img_status, img_finding, internal_notes)
    # into one pass over the table, keyed on CLINICALRESULTID.
    print("  Materializing CLINICALRESULTOBSERVATION (img_status / img_finding / internal_notes)...")
    if not _table_exists(cur, STAGING_CRO):
        cur.execute(f"""
            CREATE TABLE {STAGING_CRO} AS
            SELECT
                CLINICALRESULTID,
                MAX(CASE WHEN rn = 1 THEN RESULTSTATUS END) AS img_status,
                GROUP_CONCAT(
                    CASE WHEN RESULT IS NOT NULL THEN RESULT END
                    ORDER BY ORDERING SEPARATOR '\\n'
                )                                           AS img_finding,
                GROUP_CONCAT(
                    CASE WHEN OBSERVATIONNOTE IS NOT NULL THEN OBSERVATIONNOTE END
                    ORDER BY ORDERING SEPARATOR ' | '
                )                                           AS internal_notes
            FROM (
                SELECT CLINICALRESULTID, RESULTSTATUS, RESULT, OBSERVATIONNOTE, ORDERING,
                       ROW_NUMBER() OVER (PARTITION BY CLINICALRESULTID ORDER BY ORDERING ASC) AS rn
                FROM {SOURCE_SCHEMA}.CLINICALRESULTOBSERVATION
                WHERE nd_active_flag = 'Y'
            ) ranked
            GROUP BY CLINICALRESULTID
        """)
        cur.execute(f"ALTER TABLE {STAGING_CRO} ADD INDEX idx_crid (CLINICALRESULTID)")
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")
    cur.execute(f"SELECT COUNT(*) FROM {STAGING_CRO}")
    print(f"    {cur.fetchone()[0]:,} CRO rows")

    # ── 3. Pre-materialize PROVIDER lookup ────────────────────────────────────
    print("  Materializing PROVIDER lookup (BILLEDNAME by latest CREATEDDATETIME)...")
    if not _table_exists(cur, STAGING_PROV):
        cur.execute(f"""
            CREATE TABLE {STAGING_PROV} AS
            SELECT PROVIDERID, BILLEDNAME
            FROM (
                SELECT PROVIDERID, BILLEDNAME,
                       ROW_NUMBER() OVER (PARTITION BY PROVIDERID ORDER BY CREATEDDATETIME DESC) AS rn
                FROM {SOURCE_SCHEMA}.PROVIDER
                WHERE nd_active_flag = 'Y'
            ) t
            WHERE rn = 1
        """)
        cur.execute(f"ALTER TABLE {STAGING_PROV} ADD INDEX idx_prov (PROVIDERID)")
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")
    cur.execute(f"SELECT COUNT(*) FROM {STAGING_PROV}")
    print(f"    {cur.fetchone()[0]:,} PROVIDER rows")

    # ── 4. Pre-materialize DOCUMENTDIAGNOSIS codes ────────────────────────────
    print("  Materializing DOCUMENTDIAGNOSIS codes per DOCUMENTID...")
    if not _table_exists(cur, STAGING_DIAG):
        cur.execute(f"""
            CREATE TABLE {STAGING_DIAG} AS
            SELECT
                dd.DOCUMENTID,
                GROUP_CONCAT(
                    DISTINCT CONCAT(
                        COALESCE(ica.DIAGNOSISCODE, ''),
                        ' - ',
                        COALESCE(ica.DIAGNOSISCODEDESCRIPTION, '')
                    )
                    ORDER BY ddicd.ORDERING
                    SEPARATOR ' | '
                ) AS diagnosis_codes
            FROM {SOURCE_SCHEMA}.DOCUMENTDIAGNOSIS dd
            JOIN {SOURCE_SCHEMA}.DOCUMENTDIAGNOSISICD10 ddicd
                ON dd.DOCUMENTDIAGNOSISID = ddicd.DOCUMENTDIAGNOSISID
               AND ddicd.nd_active_flag = 'Y'
            JOIN {SOURCE_SCHEMA}.ICDCODEALL ica
                ON ddicd.ICDCODEID = ica.ICDCODEID
               AND ica.ISDELETED = FALSE
            WHERE dd.nd_active_flag = 'Y'
            GROUP BY dd.DOCUMENTID
        """)
        cur.execute(f"ALTER TABLE {STAGING_DIAG} ADD INDEX idx_docid (DOCUMENTID)")
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")
    cur.execute(f"SELECT COUNT(*) FROM {STAGING_DIAG}")
    print(f"    {cur.fetchone()[0]:,} DOCUMENT diagnosis rows")

    # ── 5. Destination table ──────────────────────────────────────────────────
    print("  Creating destination table...")
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {DEST_TABLE} (
            patientid          BIGINT        DEFAULT NULL,
            result_id          BIGINT        DEFAULT NULL,
            ndid               BIGINT        DEFAULT NULL,
            eid                BIGINT        DEFAULT NULL,
            enc_date           DATE          DEFAULT NULL,
            enc_id             BIGINT        DEFAULT NULL,
            created_datetime   DATETIME      DEFAULT NULL,
            order_date         DATETIME      DEFAULT NULL,
            perform_date       DATETIME      DEFAULT NULL,
            img_date           DATE          DEFAULT NULL,
            study_name         VARCHAR(500)  DEFAULT NULL,
            modality           VARCHAR(200)  DEFAULT NULL,
            result_status      VARCHAR(100)  DEFAULT NULL,
            img_status         VARCHAR(100)  DEFAULT NULL,
            order_status       VARCHAR(100)  DEFAULT NULL,
            img_finding        LONGTEXT,
            img_report_text    LONGTEXT,
            report_id          BIGINT        DEFAULT NULL,
            order_id           BIGINT        DEFAULT NULL,
            report_date        DATE          DEFAULT NULL,
            report_status      VARCHAR(100)  DEFAULT NULL,
            order_prescription BIGINT        DEFAULT NULL,
            provider_id        BIGINT        DEFAULT NULL,
            facility_name      VARCHAR(500)  DEFAULT NULL,
            provider_npi       VARCHAR(50)   DEFAULT NULL,
            internal_notes     LONGTEXT,
            note_to_patient    TEXT,
            facility           VARCHAR(200)  DEFAULT NULL,
            interpretation     TEXT,
            source             VARCHAR(100)  DEFAULT NULL,
            diagnosis_codes    TEXT,
            report_text        LONGTEXT
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
    """)
    conn.commit()
    print("    ready")

    # ── 6. Checkpoint table ───────────────────────────────────────────────────
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

    # ── 7. PK staging — IMAGING rows with a matching DOCUMENT ────────────────
    print(f"  Creating PK staging (IMAGING, nd_active_flag='Y')...")
    if not _table_exists(cur, STAGING_TABLE):
        cur.execute(f"""
            CREATE TABLE {STAGING_TABLE} AS
            SELECT cr.{BATCH_KEY}
            FROM {SOURCE_SCHEMA}.CLINICALRESULT cr
            JOIN {SOURCE_SCHEMA}.DOCUMENT d
                ON cr.DOCUMENTID = d.DOCUMENTID
               AND d.nd_active_flag = 'Y'
            WHERE cr.CLINICALORDERTYPEGROUP = 'IMAGING'
              AND cr.nd_active_flag = 'Y'
              AND cr.{BATCH_KEY} IS NOT NULL
        """)
        cur.execute(f"ALTER TABLE {STAGING_TABLE} ADD INDEX idx_pk ({BATCH_KEY})")
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")

    cur.execute(f"SELECT COUNT(*) FROM {STAGING_TABLE}")
    total = cur.fetchone()[0]
    print(f"    {total:,} rows to insert")

    if total == 0:
        cur.close()
        conn.close()
        return []

    # ── 8. Batch boundary sampling ────────────────────────────────────────────
    print("  Computing batch boundaries...")
    sys.stdout.flush()

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


# ── Runner ────────────────────────────────────────────────────────────────────

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
        cur.execute("SET unique_checks = 0")
        cur.execute("SET foreign_key_checks = 0")

        for pk_lo, pk_hi in ranges:
            sql = build_batch_insert(pk_lo, pk_hi)
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
        mark(conn, "failed", total_rows, str(exc))
        try:
            conn.close()
        except Exception:
            pass
        return {"status": f"FAILED: {exc}", "rows": total_rows, "secs": elapsed}


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print(f"\n{'='*70}", flush=True)
    print(f"  Radiology v2 ETL — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  source     : {SOURCE_SCHEMA}.CLINICALRESULT (IMAGING)")
    print(f"  dest       : {DEST_TABLE}")
    print(f"  staging    : {STAGING_TABLE}")
    print(f"  checkpoint : {CHECKPOINT_TABLE}")
    print(f"  batch_size : {BATCH_SIZE:,}")
    print(f"{'='*70}\n", flush=True)

    print("Setup:")
    sys.stdout.flush()
    ranges = setup_tables()
    print()

    if not ranges:
        print(f"\nNo eligible IMAGING rows in {SOURCE_SCHEMA}. Exiting.")
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
    print(f"  [{tag}] {DEST_TABLE}  "
          f"{result['rows']:>10,} rows inserted  ({result['secs']}s)")

    print(f"\n{'='*70}")
    if result["status"].startswith("FAILED"):
        print(f"  FAILED: {result['status']}")
    else:
        print(f"  Status: {result['status']}  |  Total rows inserted: {result['rows']:,}")
    print(f"{'='*70}")

    print(f"\n  Cleanup SQL:")
    print(f"    DROP TABLE IF EXISTS {STAGING_CRO};")
    print(f"    DROP TABLE IF EXISTS {STAGING_PROV};")
    print(f"    DROP TABLE IF EXISTS {STAGING_DIAG};")
    print(f"    DROP TABLE IF EXISTS {STAGING_TABLE};")
    print(f"    DROP TABLE IF EXISTS {CHECKPOINT_TABLE};")

    if result["status"].startswith("FAILED"):
        sys.exit(1)


if __name__ == "__main__":
    main()
