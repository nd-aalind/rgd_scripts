#!/usr/bin/env python3
"""
Optimized ETL loader for: udm_staging.socialhistory_ap
Source: AthenaPlus (AP) — Noran

Single source branch:
  {SOURCE_SCHEMA}.OBS
    LEFT JOIN OBSHEAD   ON OBS.HDID = OBSHEAD.HDID
    LEFT JOIN HIERGRPS  ON OBSHEAD.GROUPID = HIERGRPS.GROUPID
    WHERE HIERGRPS.GROUPNAME IN ('SH', 'Lifestyle/habits', 'tobacco use', 'Counseling')

Pre-materialized lookup (computed ONCE, reused across all batches):
  - staging.sh_ap_obshead_{SOURCE_SCHEMA}
      OBSHEAD JOIN HIERGRPS filtered by GROUPNAME — keyed on HDID

Optimizations applied:
- OBSHEAD + HIERGRPS lookup pre-materialized once (not re-scanned per batch)
- Batch by actual OBSID values via server-side PK boundary sampling
- Commit after every batch (frees undo/log space)
- Checkpoint/resume — re-run skips if already completed
- Disabled InnoDB checks per-session for bulk insert speed
- CAST(col AS CHAR) on DATE/DATETIME columns to avoid MySQL strict mode error 1292
- Progress bar via tqdm

Usage:
    python social_hist_ap.py
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
    "database":        "noran",
    "charset":         "utf8mb4",
    "connect_timeout": 30,
    "read_timeout":    21600,
    "write_timeout":   21600,
}

BATCH_SIZE = 50_000

# ── Change these two variables to run for a different schema/psid ─────
SOURCE_SCHEMA = "noran"   # e.g. "noran", "ap_staging", ...
PSID          = 7

DEST_TABLE       = "udm_staging.socialhistory_new"
STAGING_OBSHEAD  = f"staging.sh_ap_obshead1_{SOURCE_SCHEMA}"   # OBSHEAD + HIERGRPS lookup
STAGING_PK       = f"staging.sh_ap_pk1_{SOURCE_SCHEMA}"        # OBS OBSID staging
CHECKPOINT_TABLE = f"staging.etl_checkpoint_sh_ap1_{SOURCE_SCHEMA}"
CHECKPOINT_KEY   = f"socialhistory1.ap.insert.{SOURCE_SCHEMA}"

PK = "OBSID"

GROUPNAMES = ("'SH'", "'Lifestyle/habits'", "'tobacco use'", "'Counseling'")


# ── Date CASE helper (with CAST AS CHAR) ──────────────────────────────

def date_case(col):
    """
    Converts a DATE/DATETIME column to DATE safely.
    Wraps col in CAST(... AS CHAR) to avoid MySQL strict mode error 1292.
    Handles: NULL, empty, YYYY-MM-DD, YYYY-MM-DD HH:MM:SS, MM-DD-YYYY.
    {{4}}/{{2}} produce literal {4}/{2} — correct MySQL REGEXP quantifiers.
    """
    c = f"CAST({col} AS CHAR)"
    return (
        f"CASE\n"
        f"        WHEN {c} IS NULL OR {c} IN ('', 'None') THEN NULL\n"
        f"        WHEN {c} REGEXP '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}} [0-9]{{2}}:[0-9]{{2}}:[0-9]{{2}}$'\n"
        f"            THEN DATE({c})\n"
        f"        WHEN {c} REGEXP '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}$'\n"
        f"            THEN STR_TO_DATE({c}, '%Y-%m-%d')\n"
        f"        WHEN {c} REGEXP '^[0-9]{{2}}-[0-9]{{2}}-[0-9]{{4}} [0-9]{{2}}:[0-9]{{2}}:[0-9]{{2}}$'\n"
        f"            THEN DATE(STR_TO_DATE({c}, '%m-%d-%Y %H:%i:%s'))\n"
        f"        WHEN {c} REGEXP '^[0-9]{{2}}-[0-9]{{2}}-[0-9]{{4}}$'\n"
        f"            THEN STR_TO_DATE({c}, '%m-%d-%Y')\n"
        f"        ELSE NULL\n"
        f"    END"
    )


# ── Batch INSERT builder ───────────────────────────────────────────────

def build_batch_insert(pk_lo, pk_hi):
    return f"""
INSERT INTO {DEST_TABLE}
    (social_hist_id, ndid, eid, encounter_date, social_hist_date,
     social_hist_category, social_hist_subcategory, social_hist_question,
     social_hist_value, social_hist_code, social_hist_coding_system,
     social_hist_notes,
     created_datetime, created_by, updated_datetime, updated_by,
     ehr_source_name, source_path, data_type,
     psid, nd_extracted_date)
SELECT
    o.{PK},
    o.PID,
    NULL,
    NULL,
    {date_case('o.OBSDATE')},
    'Social History',
    h.NAME,
    h.DESCRIPTION,
    o.OBSVALUE,
    COALESCE(h.SNOMEDCODE, h.LOINCCODE, h.ICDCODE, h.CPTCODE, h.OTHERCODE, h.MLCODE),
    CASE
        WHEN h.SNOMEDCODE IS NOT NULL THEN 'SNOMED'
        WHEN h.LOINCCODE  IS NOT NULL THEN 'LOINC'
        WHEN h.ICDCODE    IS NOT NULL THEN 'ICD'
        WHEN h.CPTCODE    IS NOT NULL THEN 'CPT'
        WHEN h.OTHERCODE  IS NOT NULL THEN 'CVX'
        ELSE NULL
    END,
    o.DESCRIPTION,
    CURRENT_DATE(),
    'ND',
    CURRENT_DATE(),
    'ND',
    'Athenaone',
    'bronze_layer',
    'Structured',
    {PSID},
    o.nd_extracte_date
FROM {SOURCE_SCHEMA}.OBS o
INNER JOIN {STAGING_OBSHEAD} h ON h.HDID = o.HDID
WHERE o.nd_Activeflag = 'Y'
  AND o.{PK} >= {pk_lo}
  AND o.{PK} < {pk_hi}
"""


# ── Helpers ────────────────────────────────────────────────────────────

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


# ── Checkpoint ─────────────────────────────────────────────────────────

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


# ── Setup ──────────────────────────────────────────────────────────────

def setup_tables():
    """
    1. Pre-materialize OBSHEAD + HIERGRPS lookup (filtered by GROUPNAME).
    2. Create PK staging for OBS (INNER JOIN lookup to apply filter).
    3. Create destination and checkpoint tables.
    4. Compute batch ranges from actual OBSID values.
    Returns list of (lo, hi) ranges.
    """
    conn = get_connection()
    cur  = conn.cursor()

    groupnames_csv = ", ".join(GROUPNAMES)

    # ── 1. OBSHEAD + HIERGRPS lookup ──────────────────────────────────
    print("  Materializing OBSHEAD + HIERGRPS lookup...")
    if not _table_exists(cur, STAGING_OBSHEAD):
        cur.execute(f"""
            CREATE TABLE {STAGING_OBSHEAD} AS
            SELECT
                oh.HDID,
                oh.NAME,
                oh.DESCRIPTION,
                oh.SNOMEDCODE,
                oh.LOINCCODE,
                oh.ICDCODE,
                oh.CPTCODE,
                oh.OTHERCODE,
                oh.MLCODE
            FROM {SOURCE_SCHEMA}.OBSHEAD oh
            INNER JOIN {SOURCE_SCHEMA}.HIERGRPS hg
                ON hg.GROUPID = oh.GROUPID
               AND hg.nd_Activeflag = 'Y'
            WHERE oh.nd_Activeflag = 'Y'
              AND hg.GROUPNAME IN ({groupnames_csv})
        """)
        cur.execute(f"ALTER TABLE {STAGING_OBSHEAD} ADD INDEX idx_hdid (HDID)")
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")
    cur.execute(f"SELECT COUNT(*) FROM {STAGING_OBSHEAD}")
    print(f"    {cur.fetchone()[0]:,} rows")

    # ── 2. PK staging (OBS INNER JOIN lookup to apply HIERGRPS filter) ─
    print("  Creating PK staging for OBS...")
    if not _table_exists(cur, STAGING_PK):
        cur.execute(f"""
            CREATE TABLE {STAGING_PK} AS
            SELECT o.{PK}
            FROM {SOURCE_SCHEMA}.OBS o
            INNER JOIN {STAGING_OBSHEAD} h ON h.HDID = o.HDID
            WHERE o.{PK} IS NOT NULL
              AND o.nd_Activeflag = 'Y'
        """)
        cur.execute(f"ALTER TABLE {STAGING_PK} ADD INDEX idx_pk ({PK})")
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")

    cur.execute(f"SELECT COUNT(*) FROM {STAGING_PK}")
    total = cur.fetchone()[0]
    print(f"    {total:,} rows to insert")

    # ── 3. Destination table ───────────────────────────────────────────
    print("  Creating destination table...")
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {DEST_TABLE} (
            social_hist_id            BIGINT        DEFAULT NULL,
            ndid                      BIGINT        DEFAULT NULL,
            eid                       BIGINT        DEFAULT NULL,
            encounter_date            DATE          DEFAULT NULL,
            social_hist_date          DATE          DEFAULT NULL,
            social_hist_category      VARCHAR(100)  DEFAULT NULL,
            social_hist_subcategory   TEXT,
            social_hist_question      TEXT,
            social_hist_value         TEXT,
            social_hist_code          TEXT,
            social_hist_coding_system VARCHAR(50)   DEFAULT NULL,
            social_hist_notes         TEXT,
            data_source               VARCHAR(50)   DEFAULT NULL,
            created_datetime          DATETIME      DEFAULT NULL,
            created_by                VARCHAR(50)   DEFAULT NULL,
            updated_datetime          DATETIME      DEFAULT NULL,
            updated_by                VARCHAR(50)   DEFAULT NULL,
            ehr_source_name           VARCHAR(100)  DEFAULT NULL,
            source_path               VARCHAR(100)  DEFAULT NULL,
            data_type                 VARCHAR(50)   DEFAULT NULL,
            psid                      INT           DEFAULT NULL,
            nd_extracted_date         DATE          DEFAULT NULL
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
    """)
    conn.commit()
    print("    ready")

    # ── 4. Checkpoint table ────────────────────────────────────────────
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

    # ── 5. Batch boundary sampling ─────────────────────────────────────
    print("  Computing batch boundaries...")
    sys.stdout.flush()

    if total == 0:
        cur.close()
        conn.close()
        return []

    cur.execute(f"""
        SELECT {PK}
        FROM (
            SELECT {PK},
                   ROW_NUMBER() OVER (ORDER BY {PK}) AS rn
            FROM {STAGING_PK}
        ) t
        WHERE (rn - 1) % {BATCH_SIZE} = 0
        ORDER BY {PK}
    """)
    boundaries = [row[0] for row in cur.fetchall() if row[0] is not None]

    cur.execute(f"SELECT MAX({PK}) FROM {STAGING_PK}")
    max_pk = int(cur.fetchone()[0])

    cur.close()
    conn.close()

    ranges = []
    for i, lo in enumerate(boundaries):
        hi = boundaries[i + 1] if i + 1 < len(boundaries) else max_pk + 1
        ranges.append((lo, hi))

    print(f"    {len(ranges)} batches of ~{BATCH_SIZE:,} rows each")
    return ranges


# ── Runner ─────────────────────────────────────────────────────────────

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


# ── Main ───────────────────────────────────────────────────────────────

def main():
    print(f"\n{'='*70}", flush=True)
    print(f"  AP Social History ETL — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  source     : {SOURCE_SCHEMA}.OBS  (psid={PSID})")
    print(f"  dest       : {DEST_TABLE}")
    print(f"  obshead    : {STAGING_OBSHEAD}")
    print(f"  staging_pk : {STAGING_PK}")
    print(f"  checkpoint : {CHECKPOINT_TABLE}")
    print(f"  batch_size : {BATCH_SIZE:,}")
    print(f"{'='*70}\n", flush=True)

    print("  Connecting to database...")
    sys.stdout.flush()
    ranges = setup_tables()

    if not ranges:
        print(f"\nNo eligible rows found in {SOURCE_SCHEMA}.OBS. Exiting.")
        return

    result = None
    with tqdm(total=len(ranges), desc="OBS", unit="batch") as pbar:
        result = run_insert(ranges, pbar)

    print()
    if result["status"] == "done":
        tag = " DONE"
    elif result["status"] == "skipped":
        tag = " SKIP"
    else:
        tag = " FAIL"
    print(f"  [{tag}] {SOURCE_SCHEMA}.OBS  "
          f"{result['rows']:>10,} rows inserted  ({result['secs']}s)")

    print(f"\n{'='*70}")
    if result["status"].startswith("FAILED"):
        print(f"  FAILED: {result['status']}")
    else:
        print(f"  Status: {result['status']}  |  Total rows inserted: {result['rows']:,}")
    print(f"{'='*70}")

    print(f"\n  Cleanup SQL (run after verifying data):")
    print(f"    DROP TABLE IF EXISTS {STAGING_OBSHEAD};")
    print(f"    DROP TABLE IF EXISTS {STAGING_PK};")
    print(f"    DROP TABLE IF EXISTS {CHECKPOINT_TABLE};")

    if result["status"].startswith("FAILED"):
        sys.exit(1)


if __name__ == "__main__":
    main()
