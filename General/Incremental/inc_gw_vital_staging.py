#!/usr/bin/env python3
"""
inc_gw_vital_staging.py — Incremental INSERT for udm_staging.vitals (Greenway / savannah)

SQL equivalent (flattened):
    INSERT INTO udm_staging.vitals (...)
    SELECT
        CAST(a.ClinicalVitalID AS DECIMAL(18,0))   AS vital_id,
        CAST(b.PatientID AS SIGNED)                 AS ndid,
        CAST(b.VisitID   AS SIGNED)                 AS eid,
        CAST(c.vital_code AS SIGNED),
        CAST(c.vital_name AS CHAR(100)),
        CAST(c.vital_coding_system AS CHAR(10)),
        CAST(c.vital_date AS CHAR(10)),
        CAST(c.vital_time AS CHAR(13)),
        CAST(c.vital_unit AS CHAR(60)),
        CAST(NULL AS BINARY(0)),                    AS vital_range
        CAST(c.vital_result AS CHAR(500)),
        CURRENT_TIMESTAMP(),                        AS created_datetime
        'ND',                                       AS created_by
        CURRENT_TIMESTAMP(),                        AS updated_datetime
        'ND',                                       AS updated_by
        'Greenway',                                 AS ehr_source_name
        'bronze_table',                             AS source_path
        'Structured',                               AS data_type
        '9',                                       AS psid
        CAST(a.nd_extracted_date AS DATE),
        CONCAT_WS(':',
            COALESCE(psid,''), COALESCE(ndid,''), COALESCE(eid,''),
            COALESCE(vital_id,''), COALESCE(vital_date,''), COALESCE(vital_time,''),
            COALESCE(vital_code,''), COALESCE(vital_name,''))   AS udm_unq_id,
        CASE WHEN psid IN (1..14) THEN vital_date END           AS enc_date_proxy
    FROM savannah.ClinicalVital a
    JOIN  savannah.ClinicalVitalGroup b   ON ... AND a/b.nd_ActiveFlag='Y'
    LEFT JOIN (OBXManual ⨝ OBXManualClinicalVital) c ON c.ClinicalVitalID = a.ClinicalVitalID
    WHERE DATE(a.nd_extracted_date) > INCREMENTAL_DATE;

Pre-materialized lookup tables (computed ONCE, reused across all batches):
  - staging.inc_gw_vit_grp_{SOURCE_SCHEMA}   (ClinicalVitalGroup — nd_ActiveFlag='Y')
  - staging.inc_gw_vit_obx_{SOURCE_SCHEMA}   (OBXManual ⨝ OBXManualClinicalVital — nd_ActiveFlag='Y' on both)

Batching by ClinicalVitalID.
Filter: a.nd_ActiveFlag='Y' AND DATE(a.nd_extracted_date) > INCREMENTAL_DATE.

Usage:
    python inc_gw_vital_staging.py
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
PSID             = 11                    # Mind / savannah
INCREMENTAL_DATE = "2026-01-26"          # rows WHERE DATE(nd_extracted_date) > this

DEST_TABLE = "udm_staging.vitals"

# ─────────────────────────────────────────────────────────────────────
STAGING_VIT_GRP  = f"staging.inc_gw_vit_grp_{SOURCE_SCHEMA}"   # ClinicalVitalGroup
STAGING_OBX      = f"staging.inc_gw_vit_obx_{SOURCE_SCHEMA}"   # OBXManual ⨝ OBXManualClinicalVital
STAGING_PK       = f"staging.inc_gw_vit_pk_{SOURCE_SCHEMA}"    # eligible ClinicalVitalIDs
CHECKPOINT_TABLE = f"staging.etl_checkpoint_inc_gw_vit_{SOURCE_SCHEMA}"
CHECKPOINT_KEY   = f"inc_gw_vitals.{SOURCE_SCHEMA}"

BATCH_KEY = "ClinicalVitalID"


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
    Flattens the original nested SELECT+subquery into a single INSERT
    joining through pre-materialized staging tables.
    udm_unq_id and enc_date_proxy are computed inline (no outer wrapper needed).
    """
    return f"""
INSERT INTO {DEST_TABLE}
    (vital_id, ndid, eid, vital_code, vital_name,
     vital_coding_system, vital_date, vital_time, vital_unit, vital_range,
     vital_result, created_datetime, created_by, updated_datetime, updated_by,
     ehr_source_name, source_path, data_type, psid, nd_extracted_date,
     udm_unq_id, enc_date_proxy)
SELECT
    CAST(a.{BATCH_KEY} AS DECIMAL(18,0))                  AS vital_id,
    CAST(b.PatientID   AS SIGNED)                          AS ndid,
    CAST(b.VisitID     AS SIGNED)                          AS eid,
    CAST(c.vital_code  AS SIGNED)                          AS vital_code,
    CAST(c.vital_name  AS CHAR(100))                       AS vital_name,
    CAST(c.vital_coding_system AS CHAR(10))                AS vital_coding_system,
    CAST(c.vital_date  AS CHAR(10))                        AS vital_date,
    CAST(c.vital_time  AS CHAR(13))                        AS vital_time,
    CAST(c.vital_unit  AS CHAR(60))                        AS vital_unit,
    CAST(NULL          AS BINARY(0))                       AS vital_range,
    CAST(c.vital_result AS CHAR(500))                      AS vital_result,
    CAST(CURRENT_TIMESTAMP() AS DATETIME)                  AS created_datetime,
    CAST('ND'          AS CHAR(2))                         AS created_by,
    CAST(CURRENT_TIMESTAMP() AS DATETIME)                  AS updated_datetime,
    CAST('ND'          AS CHAR(2))                         AS updated_by,
    CAST('Greenway'    AS CHAR(255))                       AS ehr_source_name,
    CAST('bronze_table' AS CHAR(12))                       AS source_path,
    CAST('Structured'  AS CHAR(10))                        AS data_type,
    CAST('{PSID}'      AS CHAR(4))                         AS psid,
    CAST(a.nd_extracted_date AS DATE)                      AS nd_extracted_date,
    CONCAT_WS(':',
        COALESCE(CAST('{PSID}' AS CHAR),              ''),
        COALESCE(CAST(b.PatientID AS CHAR),            ''),
        COALESCE(CAST(b.VisitID   AS CHAR),            ''),
        COALESCE(CAST(a.{BATCH_KEY} AS CHAR),          ''),
        COALESCE(c.vital_date,                         ''),
        COALESCE(c.vital_time,                         ''),
        COALESCE(CAST(c.vital_code AS CHAR),           ''),
        COALESCE(c.vital_name,                         '')
    )                                                      AS udm_unq_id,
    CASE
        WHEN {PSID} IN (1,2,3,4,5,6,7,8,9,10,11,12,13,14)
        THEN COALESCE(c.vital_date)
    END                                                    AS enc_date_proxy
FROM {SOURCE_SCHEMA}.ClinicalVital a
JOIN  {STAGING_VIT_GRP} b
    ON b.ClinicalVitalGroupID = a.ClinicalVitalGroupID
LEFT JOIN {STAGING_OBX} c
    ON c.{BATCH_KEY} = a.{BATCH_KEY}
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
        # ── 1. ClinicalVitalGroup staging (active rows only) ──────────
        print("  Materializing ClinicalVitalGroup lookup (nd_ActiveFlag='Y')...")
        if not _table_exists(cur, STAGING_VIT_GRP):
            cur.execute(f"""
                CREATE TABLE {STAGING_VIT_GRP} AS
                SELECT
                    ClinicalVitalGroupID,
                    PatientID,
                    VisitID
                FROM {SOURCE_SCHEMA}.ClinicalVitalGroup
                WHERE nd_ActiveFlag = 'Y'
            """)
            cur.execute(
                f"ALTER TABLE {STAGING_VIT_GRP} "
                f"ADD INDEX idx_grp (ClinicalVitalGroupID)"
            )
            conn.commit()
            print("    created")
        else:
            print("    already exists, reusing")
        cur.execute(f"SELECT COUNT(*) FROM {STAGING_VIT_GRP}")
        print(f"    {cur.fetchone()[0]:,} rows")

        # ── 2. OBXManual ⨝ OBXManualClinicalVital staging ───────────
        # Materializes the LEFT JOIN subquery c from the original SQL.
        # Both nd_ActiveFlag guards included.
        print("  Materializing OBXManual lookup (nd_ActiveFlag='Y' on both tables)...")
        if not _table_exists(cur, STAGING_OBX):
            cur.execute(f"""
                CREATE TABLE {STAGING_OBX} AS
                SELECT
                    omc.{BATCH_KEY},
                    CAST(om.OBXConceptID   AS SIGNED)      AS vital_code,
                    CAST(om.TestDescription AS CHAR(100))  AS vital_name,
                    CAST(NULL               AS CHAR(10))   AS vital_coding_system,
                    DATE_FORMAT(om.CollectionDate, '%Y-%m-%d') AS vital_date,
                    DATE_FORMAT(om.CollectionDate, '%H:%i:%s') AS vital_time,
                    CAST(om.ResultUnits     AS CHAR(60))   AS vital_unit,
                    CAST(om.ReferenceRange  AS CHAR(255))  AS vital_range,
                    CAST(om.ResultValue     AS CHAR(500))  AS vital_result
                FROM {SOURCE_SCHEMA}.OBXManual om
                JOIN {SOURCE_SCHEMA}.OBXManualClinicalVital omc
                    ON om.OBXManualId = omc.OBXManualId
                   AND om.nd_ActiveFlag  = 'Y'
                   AND omc.nd_ActiveFlag = 'Y'
            """)
            cur.execute(
                f"ALTER TABLE {STAGING_OBX} "
                f"ADD INDEX idx_cvid ({BATCH_KEY})"
            )
            conn.commit()
            print("    created")
        else:
            print("    already exists, reusing")
        cur.execute(f"SELECT COUNT(*) FROM {STAGING_OBX}")
        print(f"    {cur.fetchone()[0]:,} rows")

        # ── 3. PK staging — eligible ClinicalVitalIDs ─────────────────
        print(f"  Creating PK staging (nd_ActiveFlag='Y' AND nd_extracted_date > '{INCREMENTAL_DATE}')...")
        if not _table_exists(cur, STAGING_PK):
            cur.execute(f"""
                CREATE TABLE {STAGING_PK} AS
                SELECT {BATCH_KEY}
                FROM {SOURCE_SCHEMA}.ClinicalVital
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

        # ── 4. Checkpoint table ────────────────────────────────────────
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
    print(f"  Incremental GW Vitals Staging ETL — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  source schema    : {SOURCE_SCHEMA}  (psid={PSID})")
    print(f"  incremental date : > {INCREMENTAL_DATE}")
    print(f"  dest             : {DEST_TABLE}")
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
    print(f"    DROP TABLE IF EXISTS {STAGING_VIT_GRP};")
    print(f"    DROP TABLE IF EXISTS {STAGING_OBX};")
    print(f"    DROP TABLE IF EXISTS {STAGING_PK};")
    print(f"    DROP TABLE IF EXISTS {CHECKPOINT_TABLE};")
    print()

    if status.startswith("FAILED"):
        sys.exit(1)


if __name__ == "__main__":
    main()
