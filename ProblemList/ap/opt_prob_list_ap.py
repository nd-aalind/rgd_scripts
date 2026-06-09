#!/usr/bin/env python3
"""
Optimized Problem List ETL for: udm_staging.problemlist
Source: Athena Practice (noran.PROBLEM)

Source (single INSERT job):
  1. PROBLEM  LEFT JOIN MasterDiagnosis (ICD10)
              LEFT JOIN MasterDiagnosis AS MasterDiagnosisICD9 (ICD9)

Pre-materialized lookup tables (computed ONCE, reused across all batches):
  - staging.pl_ap_masterdiag_{SOURCE_SCHEMA}   (noran.MasterDiagnosis)

Optimizations applied:
- Lookup table pre-materialized once (not re-scanned per batch)
- Source batched by primary key SPRID (CAST AS SIGNED to handle TEXT PK)
- Server-side boundary sampling (avoids loading all PKs into memory)
- Commit after every batch (frees undo/log space)
- Checkpoint/resume — re-run skips completed source
- Disabled InnoDB checks per-session for bulk insert speed
- REGEXP {n} quantifiers escaped as {{n}} inside f-strings
- Progress bar via tqdm

Usage:
    python opt_prob_list_ap.py
"""

import sys
import time
from datetime import datetime
import pymysql
from tqdm import tqdm

# ── Configuration ────────────────────────────────────────────────────
DB_CONFIG = {
    "host":            "172.16.2.42",
    "port":            3306,
    "user":            "nd-root-mysql",
    "password":        "kmsamd89undsd4",
    "database":        "noran",
    "charset":         "utf8mb4",
    "connect_timeout": 30,
    "read_timeout":    21600,
    "write_timeout":   21600,
}

BATCH_SIZE = 50_000   # PKs per batch

# ── Change these two variables to run for a different schema/psid ────
SOURCE_SCHEMA = "noran"   # e.g. "noran", ...
PSID          = 7

DEST_TABLE       = "udm_staging.problemlist_updated_ao"
CHECKPOINT_TABLE = f"staging.etl_checkpoint_pl_ap_v_5{SOURCE_SCHEMA}"

# ── Pre-materialized lookup staging table ────────────────────────────
STAGING_MASTERDIAG = f"staging.pl_ap_masterdiag_v_2{SOURCE_SCHEMA}"

# ── PK staging table ─────────────────────────────────────────────────
STAGING_PK = f"staging.tmp_pl_ap_staging_v_{SOURCE_SCHEMA}"

# ── Source definition ─────────────────────────────────────────────────
SOURCE_TABLE = "PROBLEM"
BATCH_KEY    = "SPRID"


# ── Date CASE helper ──────────────────────────────────────────────────

def date_case(col):
    """
    Returns a CASE expression that converts a VARCHAR datetime column to DATE.
    Handles: 'YYYY-MM-DD HH:MM:SS', 'YYYY-MM-DD', 'MM-DD-YYYY'.
    {{4}} / {{2}} in this f-string produce literal {4} / {2} in the returned
    string. The caller embeds the result via {date_case(...)}, which does NOT
    re-evaluate the returned string as an f-string, so single-escape is enough.
    """
    return (
        f"CASE\n"
        f"            WHEN {col} IS NULL OR {col} IN ('', 'None') THEN NULL\n"
        f"            WHEN {col} REGEXP '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}} [0-9]{{2}}:[0-9]{{2}}:[0-9]{{2}}$'\n"
        f"                THEN DATE({col})\n"
        f"            WHEN {col} REGEXP '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}$'\n"
        f"                THEN STR_TO_DATE({col}, '%Y-%m-%d')\n"
        f"            WHEN {col} REGEXP '^[0-9]{{2}}-[0-9]{{2}}-[0-9]{{4}}$'\n"
        f"                THEN STR_TO_DATE({col}, '%m-%d-%Y')\n"
        f"            ELSE NULL\n"
        f"        END"
    )


# ── Batch INSERT builder ──────────────────────────────────────────────

def build_batch_insert(pk_lo, pk_hi):
    return f"""
INSERT INTO {DEST_TABLE}
    (diag_id, ndid, eid, encounter_date, problem_date, problem_onset_date,
     problem_end_date, resolved,
     problem_id, problem_code, problem_description, coding_system,
     problem_type, status, severity, laterality, problem_notes,
     created_datetime, created_by, updated_datetime, updated_by,
     ehr_source_name, source_path, data_type, psid, nd_extracted_date)
SELECT
    CAST(p.SPRID AS SIGNED),
    CAST(p.PID AS SIGNED),
    NULL,
    NULL,
    NULL,
    {date_case('p.ONSETDATE')},
    {date_case('p.STOPDATE')},
    NULL,
    COALESCE(p.ICD10MasterDiagnosisId, p.ICD9MasterDiagnosisId, p.SNOMEDMasterDiagnosisId),
    COALESCE(md.Code, md9.Code, mdsnomed.Code,
        CASE WHEN p.CODE LIKE '%-%' THEN SUBSTRING_INDEX(p.CODE, '-', -1) ELSE p.CODE END),
    COALESCE(md.LongDescription, md9.LongDescription, mdsnomed.LongDescription,
             md.ShortDescription, md9.ShortDescription, mdsnomed.ShortDescription,
             p.DESCRIPTION),
    CASE
        WHEN p.ICD10MasterDiagnosisId IS NOT NULL THEN 'ICD-10'
        WHEN p.ICD9MasterDiagnosisId  IS NOT NULL THEN 'ICD-9'
        WHEN p.SNOMEDMasterDiagnosisId IS NOT NULL THEN 'SNOMED'
        WHEN TRIM(UPPER(p.CODE)) LIKE 'CPT-%'   THEN 'CPT'
        WHEN TRIM(UPPER(p.CODE)) LIKE 'SNO%'    THEN 'SNOMED'
        WHEN TRIM(UPPER(p.CODE)) LIKE 'ICD9-%'  THEN 'ICD-9'
        WHEN TRIM(UPPER(p.CODE)) LIKE 'ICD10-%' THEN 'ICD-10'
        WHEN TRIM(UPPER(p.CODE)) LIKE 'ICD-%'   THEN 'ICD'
        WHEN p.CODE REGEXP '^[0-9]{{6,18}}$'      THEN 'SNOMED'
        WHEN p.CODE REGEXP '^[0-9]{{5}}$'          THEN 'CPT'
        WHEN p.CODE REGEXP '^[A-Z][0-9]{{4}}$'     THEN 'HCPCS'
        WHEN p.CODE REGEXP '^[A-Z][0-9A-Z\\\\.]{{2,}}$' THEN 'ICD-10'
        WHEN p.CODE REGEXP '^[0-9]{{3}}(\\\\.[0-9]{{1,2}})?$' THEN 'ICD-9'
        ELSE NULL
    END,
    p.QUALIFIER,
    NULL,
    NULL,
    NULL,
    NULL,
    CURRENT_DATE(),
    'ND',
    CURRENT_DATE(),
    'ND',
    'Athenaone',
    'bronze_layer',
    'Structured',
    {PSID},
    DATE(p.nd_extracted_date)
FROM {SOURCE_SCHEMA}.PROBLEM p
LEFT JOIN {STAGING_MASTERDIAG} md      ON md.MasterDiagnosisID      = p.ICD10MasterDiagnosisId
LEFT JOIN {STAGING_MASTERDIAG} md9     ON md9.MasterDiagnosisID     = p.ICD9MasterDiagnosisId
LEFT JOIN {STAGING_MASTERDIAG} mdsnomed ON mdsnomed.MasterDiagnosisID = p.SNOMEDMasterDiagnosisId
WHERE p.{BATCH_KEY} >= {pk_lo} AND p.{BATCH_KEY} < {pk_hi}
"""


# ── Helpers ───────────────────────────────────────────────────────────

def get_connection():
    return pymysql.connect(**DB_CONFIG)


def _table_exists(cur, full_table_name):
    schema, table = full_table_name.split(".")
    cur.execute(
        "SELECT COUNT(*) FROM information_schema.tables "
        "WHERE table_schema = %s AND table_name = %s",
        (schema, table),
    )
    return cur.fetchone()[0] > 0


# ── Checkpoint ────────────────────────────────────────────────────────

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


# ── Setup ─────────────────────────────────────────────────────────────

def setup_tables():
    """Setup lookup + PK staging tables. Return batch ranges."""
    conn = get_connection()
    cur  = conn.cursor()

    # ── 1. Pre-materialized MasterDiagnosis lookup ────────────────────
    print("  Materializing MasterDiagnosis lookup...")
    if not _table_exists(cur, STAGING_MASTERDIAG):
        cur.execute(f"""
            CREATE TABLE {STAGING_MASTERDIAG} AS
            SELECT * FROM {SOURCE_SCHEMA}.MasterDiagnosis
        """)
        cur.execute(f"ALTER TABLE {STAGING_MASTERDIAG} ADD INDEX idx_md (MasterDiagnosisID)")
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")
    cur.execute(f"SELECT COUNT(*) FROM {STAGING_MASTERDIAG}")
    print(f"    {cur.fetchone()[0]:,} MasterDiagnosis rows")

    # ── 2. Destination table ────────────────────��─────────────────────
    print("  Creating destination table...")
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {DEST_TABLE} (
            diag_id              BIGINT       DEFAULT NULL,
            ndid                 BIGINT       DEFAULT NULL,
            eid                  BIGINT       DEFAULT NULL,
            encounter_date       DATE         DEFAULT NULL,
            problem_date         DATE         DEFAULT NULL,
            problem_onset_date   DATE         DEFAULT NULL,
            problem_end_date     DATE         DEFAULT NULL,
            resolved             BIGINT       DEFAULT NULL,
            problem_id           VARCHAR(50)  DEFAULT NULL,
            problem_code         VARCHAR(100) DEFAULT NULL,
            problem_description  TEXT,
            coding_system        VARCHAR(20)  DEFAULT NULL,
            problem_type         VARCHAR(100) DEFAULT NULL,
            status               VARCHAR(100) DEFAULT NULL,
            severity             VARCHAR(100) DEFAULT NULL,
            laterality           VARCHAR(100) DEFAULT NULL,
            problem_notes        TEXT,
            created_datetime     DATE         DEFAULT NULL,
            created_by           VARCHAR(20)  DEFAULT NULL,
            updated_datetime     DATE         DEFAULT NULL,
            updated_by           VARCHAR(20)  DEFAULT NULL,
            ehr_source_name      VARCHAR(100) DEFAULT NULL,
            source_path          VARCHAR(100) DEFAULT NULL,
            data_type            VARCHAR(50)  DEFAULT NULL,
            psid                 INT          DEFAULT NULL,
            nd_extracted_date    DATE         DEFAULT NULL
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
    """)
    conn.commit()
    print("    ready")

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

    # ── 4. PK staging table ───────────────────────────────────────────
    print(f"  Creating PK staging for {SOURCE_TABLE}...")
    if not _table_exists(cur, STAGING_PK):
        cur.execute(f"""
            CREATE TABLE {STAGING_PK} AS
            SELECT CAST({BATCH_KEY} AS SIGNED) AS {BATCH_KEY}
            FROM {SOURCE_SCHEMA}.{SOURCE_TABLE}
            WHERE {BATCH_KEY} IS NOT NULL
        """)
        cur.execute(f"ALTER TABLE {STAGING_PK} ADD INDEX idx_pk ({BATCH_KEY})")
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")

    cur.execute(f"SELECT COUNT(*) FROM {STAGING_PK}")
    count = cur.fetchone()[0]
    print(f"    {count:,} rows to process")

    if count == 0:
        cur.close()
        conn.close()
        return []

    # ── 5. Server-side boundary sampling ─────────────────────────────
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

    ranges = []
    for i, lo in enumerate(boundaries):
        hi = boundaries[i + 1] if i + 1 < len(boundaries) else max_pk + 1
        ranges.append((lo, hi))

    print(f"    {len(ranges)} batches of ~{BATCH_SIZE:,} rows each")

    cur.close()
    conn.close()
    return ranges


# ── Worker ────────────────────────────────────────────────────────────

def run_source(ranges, pbar):
    """Process PROBLEM table across all batch ranges."""
    source_key = SOURCE_TABLE

    conn = get_connection()

    if is_done(conn, source_key):
        conn.close()
        pbar.update(len(ranges))
        return {"source": source_key, "status": "skipped", "rows": 0, "secs": 0}

    mark(conn, source_key, "running")
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
        mark(conn, source_key, "done", total_rows)
        conn.close()
        return {"source": source_key, "status": "done", "rows": total_rows, "secs": elapsed}

    except Exception as exc:
        elapsed = round(time.time() - t0, 1)
        mark(conn, source_key, "failed", total_rows, str(exc))
        try:
            conn.close()
        except Exception:
            pass
        return {"source": source_key, "status": f"FAILED: {exc}", "rows": total_rows, "secs": elapsed}


# ── Main ──────────────────────────────────────────────────────────────

def main():
    print(f"\n{'='*70}", flush=True)
    print(f"  Athena Practice Problem List ETL — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  source     : {SOURCE_SCHEMA}.{SOURCE_TABLE}  (psid={PSID})")
    print(f"  dest       : {DEST_TABLE}")
    print(f"  checkpoint : {CHECKPOINT_TABLE}")
    print(f"  batch_size : {BATCH_SIZE:,}")
    print(f"{'='*70}\n", flush=True)

    print("  Connecting to database...")
    sys.stdout.flush()
    ranges = setup_tables()

    if not ranges:
        print(f"\nNo rows found in {SOURCE_SCHEMA}.{SOURCE_TABLE}. Exiting.")
        return

    result = None
    with tqdm(total=len(ranges), desc="PROBLEM", unit="batch") as pbar:
        result = run_source(ranges, pbar)

    print()
    if result["status"] == "done":
        tag = " DONE"
    elif result["status"] == "skipped":
        tag = " SKIP"
    else:
        tag = " FAIL"
    print(f"  [{tag}] {result['source']:<42} {result['rows']:>10,} rows  ({result['secs']}s)")

    print(f"\n{'='*70}")
    print(f"  Total rows inserted: {result['rows']:,}")
    print(f"{'='*70}")

    if result["status"].startswith("FAILED"):
        print(f"\n  Error: {result['status']}")

    print(f"\n  Cleanup SQL:")
    print(f"    DROP TABLE IF EXISTS {DEST_TABLE};")
    print(f"    DROP TABLE IF EXISTS {STAGING_MASTERDIAG};")
    print(f"    DROP TABLE IF EXISTS {STAGING_PK};")
    print(f"    DROP TABLE IF EXISTS {CHECKPOINT_TABLE};")

    if result["status"].startswith("FAILED"):
        sys.exit(1)


if __name__ == "__main__":
    main()
