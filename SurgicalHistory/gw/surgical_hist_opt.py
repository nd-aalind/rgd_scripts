#!/usr/bin/env python3
"""
Optimized ETL for: udm_staging.surgicalhistory_final
Source: Greenway

Source (single INSERT job):
  PatHistSurgical a
    JOIN PatHistCatPatHistItem b ON a.PatHistCatPatHistItemID = b.PatHistCatPatHistItemID (both active)
    JOIN PatHistItem c           ON b.PatHistItemID = c.PatHistItemID (active)

Pre-materialized lookup table (computed ONCE, reused across all batches):
  - staging.sh_gw_phi_v1_{schema}  — PatHistCatPatHistItem INNER JOIN PatHistItem (both active),
                                     stores PatHistCatPatHistItemID, PatHistItemDescription,
                                     AltSystemCode, AltSystem
                                     Keyed on PatHistCatPatHistItemID — both JOINs collapsed to one.

Batch key: PatHistSurgicalID (integer PK)

Column mapping to shared dest table:
  Surg_Category (PatHistItemDescription) → surgery_name
  Surg_Code     (AltSystemCode)          → surgery_code
  Surg_Coding_sys (AltSystem)            → surgery_coding_system
  Surg_Notes    (PSHNote)                → surgery_reason
  surg_hist_type                         → NULL (not provided by Greenway)

Optimizations:
- Both JOINs pre-collapsed into one staging table (not re-scanned per batch)
- Batching by actual PatHistSurgicalID values (sparse ID safe)
- Commit after every batch (frees undo/log space)
- Checkpoint/resume — re-run skips if already completed
- InnoDB checks disabled per-session for bulk speed
- tqdm progress bar

Usage:
    python surgical_hist_opt.py
"""

import sys
import time
from datetime import datetime
import pymysql
from tqdm import tqdm

# ── Configuration ─────────────────────────────────────────────────────────────
DB_CONFIG = {
    "host":            "ndai-dev-rds-instance.cwp60ymu4ko0.us-east-1.rds.amazonaws.com",
    "port":            3306,
    "user":            "Aalind",
    "password":        "A@L1nd@123",
    "database":        "udm_staging",
    "charset":         "utf8mb4",
    "connect_timeout": 30,
    "read_timeout":    21600,
    "write_timeout":   21600,
}

BATCH_SIZE = 50_000

# ── Change these two variables to run for a different schema/psid ──────────────
SOURCE_SCHEMA = "savannah"
PSID          = 9

DEST_TABLE       = "udm_staging.surgicalhistory_final"
STAGING_PHI      = f"staging.sh_gw_phi_v1_{SOURCE_SCHEMA}"
STAGING_PK       = f"staging.tmp_sh_gw_v1_{SOURCE_SCHEMA}"
CHECKPOINT_TABLE = f"staging.etl_checkpoint_sh_gw_v1_{SOURCE_SCHEMA}"
CHECKPOINT_KEY   = f"pathhistsurgical.gw.insert.{SOURCE_SCHEMA}"

BATCH_KEY = "PatHistSurgicalID"


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


# ── Batch INSERT builder ──────────────────────────────────────────────────────

def build_batch_insert(pk_lo, pk_hi):
    return f"""
INSERT INTO {DEST_TABLE}
    (surgicalhistoryid, ndid, eid, enc_date,
     surgery_date, surg_hist_type, surgery_name,
     surgery_code, surgery_coding_system, surgery_reason,
     created_datetime, created_by, updated_datetime, updated_by,
     ehr_source_name, source_path, data_type,
     psid, nd_extracted_date)
SELECT
    a.PatHistSurgicalID,
    a.PatientID,
    CAST(NULL AS UNSIGNED),
    CAST(NULL AS DATE),
    CASE
        WHEN a.DateOfProcedure IS NULL
             OR TRIM(CAST(a.DateOfProcedure AS CHAR)) = ''
             OR STR_TO_DATE(a.DateOfProcedure, '%Y-%m-%d') IS NULL
        THEN DATE(a.CreateDate)
        ELSE DATE(a.DateOfProcedure)
    END,
    NULL,
    phi.PatHistItemDescription,
    phi.AltSystemCode,
    phi.AltSystem,
    a.PSHNote,
    CURRENT_TIMESTAMP(),
    'ND',
    CURRENT_TIMESTAMP(),
    'ND',
    'Greenway',
    'bronze_layer',
    'Structured',
    {PSID},
    a.nd_extracted_date
FROM {SOURCE_SCHEMA}.PatHistSurgical a
INNER JOIN {STAGING_PHI} phi ON phi.PatHistCatPatHistItemID = a.PatHistCatPatHistItemID
WHERE a.nd_ActiveFlag = 'Y'
  AND a.{BATCH_KEY} >= {pk_lo}
  AND a.{BATCH_KEY} <  {pk_hi}
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

    # ── 1. Ensure indexes on source join/filter columns ──────────────
    print("  Ensuring indexes on source tables...")
    _ensure_index(cur, conn, f"{SOURCE_SCHEMA}.PatHistSurgical",
                  "idx_pathcatid",     ["PatHistCatPatHistItemID"])
    _ensure_index(cur, conn, f"{SOURCE_SCHEMA}.PatHistSurgical",
                  "idx_nd_activeflag", ["nd_ActiveFlag"])
    _ensure_index(cur, conn, f"{SOURCE_SCHEMA}.PatHistCatPatHistItem",
                  "idx_pathcatid",     ["PatHistCatPatHistItemID"])
    _ensure_index(cur, conn, f"{SOURCE_SCHEMA}.PatHistCatPatHistItem",
                  "idx_pathitemid",    ["PatHistItemID"])
    _ensure_index(cur, conn, f"{SOURCE_SCHEMA}.PatHistItem",
                  "idx_pathitemid",    ["PatHistItemID"])

    # ── 2. Pre-materialize PHI lookup ─────────────────────────────────
    # Collapses PatHistCatPatHistItem JOIN PatHistItem (both nd_ActiveFlag='Y')
    # into one staging table keyed on PatHistCatPatHistItemID. Each batch JOIN
    # becomes a single lookup instead of two sequential JOINs on live tables.
    print("  Materializing PHI lookup (PatHistCatPatHistItem INNER JOIN PatHistItem, both active)...")
    if not _table_exists(cur, STAGING_PHI):
        cur.execute(f"""
            CREATE TABLE {STAGING_PHI} AS
            SELECT
                b.PatHistCatPatHistItemID,
                c.PatHistItemDescription,
                c.AltSystemCode,
                c.AltSystem
            FROM {SOURCE_SCHEMA}.PatHistCatPatHistItem b
            INNER JOIN {SOURCE_SCHEMA}.PatHistItem c
                ON b.PatHistItemID = c.PatHistItemID
               AND c.nd_ActiveFlag = 'Y'
            WHERE b.nd_ActiveFlag = 'Y'
        """)
        cur.execute(f"ALTER TABLE {STAGING_PHI} ADD INDEX idx_pathcatid (PatHistCatPatHistItemID)")
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")
    cur.execute(f"SELECT COUNT(*) FROM {STAGING_PHI}")
    print(f"    {cur.fetchone()[0]:,} PHI rows")

    # ── 3. Destination table ──────────────────────────────────────────
    print("  Creating destination table...")
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {DEST_TABLE} (
            surgicalhistoryid     BIGINT        DEFAULT NULL,
            ndid                  BIGINT        DEFAULT NULL,
            eid                   BIGINT        DEFAULT NULL,
            enc_date              DATE          DEFAULT NULL,
            surgery_date          DATE          DEFAULT NULL,
            surg_hist_type        VARCHAR(200)  DEFAULT NULL,
            surgery_name          TEXT,
            surgery_code          VARCHAR(50)   DEFAULT NULL,
            surgery_coding_system VARCHAR(20)   DEFAULT NULL,
            surgery_reason        TEXT,
            created_datetime      DATETIME      DEFAULT NULL,
            created_by            VARCHAR(50)   DEFAULT NULL,
            updated_datetime      DATETIME      DEFAULT NULL,
            updated_by            VARCHAR(50)   DEFAULT NULL,
            ehr_source_name       VARCHAR(100)  DEFAULT NULL,
            source_path           VARCHAR(100)  DEFAULT NULL,
            data_type             VARCHAR(50)   DEFAULT NULL,
            psid                  INT           DEFAULT NULL,
            nd_extracted_date     DATE          DEFAULT NULL
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
    """)
    conn.commit()
    print("    ready")

    # ── 4. Checkpoint table ───────────────────────────────────────────
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

    # ── 5. PK staging ─────────────────────────────────────────────────
    print(f"  Creating PK staging for PatHistSurgical...")
    if not _table_exists(cur, STAGING_PK):
        cur.execute(f"""
            CREATE TABLE {STAGING_PK} AS
            SELECT {BATCH_KEY}
            FROM {SOURCE_SCHEMA}.PatHistSurgical
            WHERE {BATCH_KEY} IS NOT NULL
              AND nd_ActiveFlag = 'Y'
        """)
        cur.execute(f"ALTER TABLE {STAGING_PK} ADD INDEX idx_pk ({BATCH_KEY})")
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")

    cur.execute(f"SELECT COUNT(*) FROM {STAGING_PK}")
    total = cur.fetchone()[0]
    print(f"    {total:,} rows to insert")

    # ── 6. Batch boundary sampling ────────────────────────────────────
    print("  Computing batch boundaries...")
    sys.stdout.flush()

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
    t0         = time.time()
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
    print(f"  Greenway Surgical History ETL — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  source     : {SOURCE_SCHEMA}.PatHistSurgical  (psid={PSID})")
    print(f"  dest       : {DEST_TABLE}")
    print(f"  staging    : {STAGING_PK}")
    print(f"  checkpoint : {CHECKPOINT_TABLE}")
    print(f"  batch_size : {BATCH_SIZE:,}")
    print(f"{'='*70}\n", flush=True)

    print("Setup:")
    sys.stdout.flush()
    ranges = setup_tables()

    if not ranges:
        print(f"\nNo eligible rows in {SOURCE_SCHEMA}.PatHistSurgical. Exiting.")
        return

    result = None
    with tqdm(total=len(ranges), desc="PatHistSurgical", unit="batch") as pbar:
        result = run_insert(ranges, pbar)

    print()
    if result["status"] == "done":
        tag = " DONE"
    elif result["status"] == "skipped":
        tag = " SKIP"
    else:
        tag = " FAIL"
    print(f"  [{tag}] {SOURCE_SCHEMA}.PatHistSurgical  "
          f"{result['rows']:>10,} rows inserted  ({result['secs']}s)")

    print(f"\n{'='*70}")
    if result["status"].startswith("FAILED"):
        print(f"  FAILED: {result['status']}")
    else:
        print(f"  Status: {result['status']}  |  Total rows inserted: {result['rows']:,}")
    print(f"{'='*70}")

    print(f"\n  Cleanup SQL:")
    print(f"    DROP TABLE IF EXISTS {STAGING_PHI};")
    print(f"    DROP TABLE IF EXISTS {STAGING_PK};")
    print(f"    DROP TABLE IF EXISTS {CHECKPOINT_TABLE};")

    if result["status"].startswith("FAILED"):
        sys.exit(1)


if __name__ == "__main__":
    main()
