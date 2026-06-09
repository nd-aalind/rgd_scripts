#!/usr/bin/env python3
"""
Optimized ETL loader for: udm_staging.athenaone_medicalhistory
Source: Greenway (GW)

Source (single INSERT job):
  PatHistMedical a
    JOIN PatHistCatPatHistItem b ON a.PatHistCatPatHistItemID = b.PatHistCatPatHistItemID (both active)
    JOIN PatHistCat d            ON d.PatHistCatID = b.PatHistCatID (active)

Pre-materialized lookup table (computed ONCE, reused across all batches):
  - staging.mh_gw_phcd_v1_{schema}  — PatHistCatPatHistItem INNER JOIN PatHistCat (both active),
                                      stores PatHistCatPatHistItemID, PatHistCatDescription
                                      Keyed on PatHistCatPatHistItemID — both JOINs collapsed to one.

Batch key: PatHistMedicalID (integer PK)

Column mapping (same dest table structure as optimise_medical_history.py / AO):
  hist_category   ('MedicalHistory')          → hist_category
  PatHistCatDescription                        → med_hist_category
  med_hist_question                            → NULL
  med_hist_value                               → NULL

Optimizations:
- Both JOINs pre-collapsed into one staging table (not re-scanned per batch)
- Batching by actual PatHistMedicalID values (sparse ID safe)
- Commit after every batch (frees undo/log space)
- Checkpoint/resume — re-run skips if already completed
- InnoDB checks disabled per-session for bulk speed
- LEFT(col, 10) date prefix — handles YYYY-MM-DD HH:MM:SS and YYYY-MM-DD uniformly
- tqdm progress bar

Usage:
    python opt_med_hist_gw.py
"""

import sys
import time
from datetime import datetime
import pymysql
from tqdm import tqdm

# ── Configuration ─────────────────────────────────────────────────────────────
DB_CONFIG = {
    "host":            "172.16.2.42",
    "port":            3306,
    "user":            "nd-root-mysql",
    "password":        "kmsamd89undsd4",
    "database":        "savannah",
    "charset":         "utf8mb4",
    "connect_timeout": 30,
    "read_timeout":    21600,
    "write_timeout":   21600,
}

BATCH_SIZE = 50_000

# ── Change these two variables to run for a different schema/psid ──────────────
SOURCE_SCHEMA = "savannah"
PSID          = 9

DEST_TABLE       = "udm_staging.athenaone_medicalhistory"
STAGING_PHCD     = f"staging.mh_gw_phcd_v1_{SOURCE_SCHEMA}"
STAGING_PK       = f"staging.tmp_mh_gw_v1_{SOURCE_SCHEMA}"
CHECKPOINT_TABLE = f"staging.etl_checkpoint_mh_gw_v1_{SOURCE_SCHEMA}"
CHECKPOINT_KEY   = f"pathistmedical.gw.insert.{SOURCE_SCHEMA}"

BATCH_KEY = "PatHistMedicalID"


# ── Date CASE helper (LEFT approach — matches AO medical history) ─────────────

def date_case(col):
    """
    Uses LEFT(col, 10) to extract the date prefix, then pattern-matches.
    LEFT handles YYYY-MM-DD HH:MM:SS by truncating to YYYY-MM-DD automatically.
    Handles: NULL, 'None', '', YYYY-MM-DD, YYYY-MM-DD HH:MM:SS, MM-DD-YYYY.
    {{4}}/{{2}} in f-strings produce {4}/{2} — correct MySQL REGEXP quantifiers.
    """
    return (
        f"CASE\n"
        f"        WHEN {col} IS NULL OR {col} IN ('None', '') THEN NULL\n"
        f"        WHEN LEFT({col}, 10) REGEXP '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}$'\n"
        f"            THEN STR_TO_DATE(LEFT({col}, 10), '%Y-%m-%d')\n"
        f"        WHEN LEFT({col}, 10) REGEXP '^[0-9]{{2}}-[0-9]{{2}}-[0-9]{{4}}$'\n"
        f"            THEN STR_TO_DATE(LEFT({col}, 10), '%m-%d-%Y')\n"
        f"        ELSE NULL\n"
        f"    END"
    )


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
    (med_hist_id, ndid, eid, encounter_date, med_hist_date,
     hist_category, med_hist_category, med_hist_question, med_hist_value,
     created_datetime, created_by, updated_datetime, updated_by,
     ehr_source_name, source_path, data_type,
     psid, nd_extracted_date)
SELECT
    a.{BATCH_KEY},
    a.PatientID,
    NULL,
    NULL,
    {date_case('a.CreateDate')},
    'MedicalHistory',
    phcd.PatHistCatDescription,
    NULL,
    NULL,
    CURRENT_DATE(),
    'ND',
    CURRENT_DATE(),
    'ND',
    'Greenway',
    'bronze_layer',
    'Structured',
    {PSID},
    a.nd_extracted_date
FROM {SOURCE_SCHEMA}.PatHistMedical a
INNER JOIN {STAGING_PHCD} phcd ON phcd.PatHistCatPatHistItemID = a.PatHistCatPatHistItemID
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
    _ensure_index(cur, conn, f"{SOURCE_SCHEMA}.PatHistMedical",
                  "idx_pathcatid",     ["PatHistCatPatHistItemID"])
    _ensure_index(cur, conn, f"{SOURCE_SCHEMA}.PatHistMedical",
                  "idx_nd_activeflag", ["nd_ActiveFlag"])
    _ensure_index(cur, conn, f"{SOURCE_SCHEMA}.PatHistCatPatHistItem",
                  "idx_pathcatid",     ["PatHistCatPatHistItemID"])
    _ensure_index(cur, conn, f"{SOURCE_SCHEMA}.PatHistCatPatHistItem",
                  "idx_pathcatid2",    ["PatHistCatID"])
    _ensure_index(cur, conn, f"{SOURCE_SCHEMA}.PatHistCat",
                  "idx_pathcatid",     ["PatHistCatID"])

    # ── 2. Pre-materialize PHCD lookup ────────────────────────────────
    # Collapses PatHistCatPatHistItem JOIN PatHistCat (both nd_ActiveFlag='Y')
    # into one staging table keyed on PatHistCatPatHistItemID.
    print("  Materializing PHCD lookup (PatHistCatPatHistItem INNER JOIN PatHistCat, both active)...")
    if not _table_exists(cur, STAGING_PHCD):
        cur.execute(f"""
            CREATE TABLE {STAGING_PHCD} AS
            SELECT
                b.PatHistCatPatHistItemID,
                d.PatHistCatDescription
            FROM {SOURCE_SCHEMA}.PatHistCatPatHistItem b
            INNER JOIN {SOURCE_SCHEMA}.PatHistCat d
                ON d.PatHistCatID = b.PatHistCatID
               AND d.nd_ActiveFlag = 'Y'
            WHERE b.nd_ActiveFlag = 'Y'
        """)
        cur.execute(f"ALTER TABLE {STAGING_PHCD} ADD INDEX idx_pathcatid (PatHistCatPatHistItemID)")
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")
    cur.execute(f"SELECT COUNT(*) FROM {STAGING_PHCD}")
    print(f"    {cur.fetchone()[0]:,} PHCD rows")

    # ── 3. Destination table (same structure as optimise_medical_history.py / AO) ──
    print("  Creating destination table...")
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {DEST_TABLE} (
            med_hist_id             BIGINT        DEFAULT NULL,
            ndid                    BIGINT        DEFAULT NULL,
            eid                     BIGINT        DEFAULT NULL,
            encounter_date          DATE          DEFAULT NULL,
            med_hist_date           DATE          DEFAULT NULL,
            hist_category           VARCHAR(100)  DEFAULT NULL,
            med_hist_category       VARCHAR(500)  DEFAULT NULL,
            med_hist_question       TEXT,
            med_hist_value          TEXT,
            med_hist_code           VARCHAR(50)   DEFAULT NULL,
            med_hist_coding_system  VARCHAR(50)   DEFAULT NULL,
            med_hist_notes          TEXT,
            data_source             VARCHAR(50)   DEFAULT NULL,
            created_datetime        DATETIME      DEFAULT NULL,
            created_by              VARCHAR(50)   DEFAULT NULL,
            updated_datetime        DATETIME      DEFAULT NULL,
            updated_by              VARCHAR(50)   DEFAULT NULL,
            ehr_source_name         VARCHAR(100)  DEFAULT NULL,
            source_path             VARCHAR(100)  DEFAULT NULL,
            data_type               VARCHAR(50)   DEFAULT NULL,
            psid                    INT           DEFAULT NULL,
            nd_extracted_date       DATE          DEFAULT NULL
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
    print("  Creating PK staging for PatHistMedical...")
    if not _table_exists(cur, STAGING_PK):
        cur.execute(f"""
            CREATE TABLE {STAGING_PK} AS
            SELECT {BATCH_KEY}
            FROM {SOURCE_SCHEMA}.PatHistMedical
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
    print(f"  Greenway Medical History ETL — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  source     : {SOURCE_SCHEMA}.PatHistMedical  (psid={PSID})")
    print(f"  dest       : {DEST_TABLE}")
    print(f"  staging    : {STAGING_PK}")
    print(f"  checkpoint : {CHECKPOINT_TABLE}")
    print(f"  batch_size : {BATCH_SIZE:,}")
    print(f"{'='*70}\n", flush=True)

    print("Setup:")
    sys.stdout.flush()
    ranges = setup_tables()

    if not ranges:
        print(f"\nNo eligible rows in {SOURCE_SCHEMA}.PatHistMedical. Exiting.")
        return

    result = None
    with tqdm(total=len(ranges), desc="PatHistMedical", unit="batch") as pbar:
        result = run_insert(ranges, pbar)

    print()
    if result["status"] == "done":
        tag = " DONE"
    elif result["status"] == "skipped":
        tag = " SKIP"
    else:
        tag = " FAIL"
    print(f"  [{tag}] {SOURCE_SCHEMA}.PatHistMedical  "
          f"{result['rows']:>10,} rows inserted  ({result['secs']}s)")

    print(f"\n{'='*70}")
    if result["status"].startswith("FAILED"):
        print(f"  FAILED: {result['status']}")
    else:
        print(f"  Status: {result['status']}  |  Total rows inserted: {result['rows']:,}")
    print(f"{'='*70}")

    print(f"\n  Cleanup SQL:")
    print(f"    DROP TABLE IF EXISTS {STAGING_PHCD};")
    print(f"    DROP TABLE IF EXISTS {STAGING_PK};")
    print(f"    DROP TABLE IF EXISTS {CHECKPOINT_TABLE};")

    if result["status"].startswith("FAILED"):
        sys.exit(1)


if __name__ == "__main__":
    main()
