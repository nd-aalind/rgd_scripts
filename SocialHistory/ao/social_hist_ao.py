#!/usr/bin/env python3
"""
Optimized ETL loader for: udm_staging.socialhistory_ao
Source: AthenaOne (AO)

Two UNION ALL branches:
  Branch 1: {SOURCE_SCHEMA}.SOCIALHXFORMRESPONSE  (data_source = 'AthenaOne')
            LEFT JOIN CLINICALENCOUNTER (nd_active_flag = 'Y') → pre-materialized
            LEFT JOIN SOCIALHXFORMRESPONSEANSWER (nd_active_flag = 'Y') → pre-materialized
  Branch 2: {SOURCE_SCHEMA}.PATIENTSOCIALHISTORY  (data_source = 'AthenaOne')
            Filter: socialhistorykey <> 'REVIEWED.SOCIALHISTORY' AND nd_active_flag = 'Y'
            No JOINs

Pre-materialized lookup tables (computed ONCE, reused across all batches):
  - staging.sh_ao_ce_{SOURCE_SCHEMA}    (CLINICALENCOUNTER filtered nd_active_flag='Y')
  - staging.sh_ao_ans_{SOURCE_SCHEMA}   (SOCIALHXFORMRESPONSEANSWER filtered nd_active_flag='Y')

Optimizations applied:
- All lookups pre-materialized once (not re-scanned per batch)
- Per-source PK staging for batch boundary sampling
- Checkpoint/resume per source
- Parallel execution via ThreadPoolExecutor (2 workers)
- Commit after every batch (frees undo/log space)
- Disabled InnoDB checks per-session for bulk insert speed
- REGEXP {{n}} quantifiers escaped as {{n}} inside f-strings
- LEFT(col, 10) to extract date prefix — handles YYYY-MM-DD HH:MM:SS and YYYY-MM-DD uniformly
- Progress bar via tqdm

Usage:
    python social_hist_ao.py
"""

import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import pymysql
from tqdm import tqdm

# ── Configuration ─────────────────────────────────────────────────────
DB_CONFIG = {
    "host":            "172.16.2.42",
    "port":            3306,
    "user":            "nd-root-mysql",
    "password":        "kmsamd89undsd4",
    "database":        "dcnd",
    "charset":         "utf8mb4",
    "connect_timeout": 30,
    "read_timeout":    21600,
    "write_timeout":   21600,
}

BATCH_SIZE  = 50_000
MAX_WORKERS = 2   # one per UNION ALL branch

# ── Change these two variables to run for a different schema/psid ─────
SOURCE_SCHEMA = "tncpa"   # e.g. "tng_athena_one", "dcnd", "raleigh", ...
PSID          = 6

DEST_TABLE       = "udm_staging.socialhistory_new"
STAGING_CE       = f"staging.sh_ao_ce12_{SOURCE_SCHEMA}"    # CLINICALENCOUNTER lookup
STAGING_ANS      = f"staging.sh_ao_ans12_{SOURCE_SCHEMA}"   # SOCIALHXFORMRESPONSEANSWER lookup
CHECKPOINT_TABLE = f"staging.etl_checkpoint_sh_ao13_{SOURCE_SCHEMA}"

# ── Source definitions ─────────────────────────────────────────────────
SOURCES = [
    {
        "key":        "ao_social1",
        "table":      "SOCIALHXFORMRESPONSE",
        "pk":         "SOCIALHXFORMRESPONSEID",
        "staging_pk": f"staging.sh_ao_stg2_{SOURCE_SCHEMA}",
    },
    {
        "key":        "ao_social2",
        "table":      "PATIENTSOCIALHISTORY",
        "pk":         "socialhistoryid",
        "staging_pk": f"staging.sh_ao_stg3_{SOURCE_SCHEMA}",
    },
]


# ── Date CASE helper (AO style with CAST AS CHAR) ─────────────────────

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


# ── Batch INSERT builders ──────────────────────────────────────────────

def build_batch_insert_branch1(pk_lo, pk_hi):
    """SOCIALHXFORMRESPONSEANSWER (driving table) — INNER JOIN SOCIALHXFORMRESPONSE, LEFT JOIN CE."""
    enc_date = date_case('ce.ENCOUNTERDATE')
    return f"""
INSERT INTO {DEST_TABLE}
    (social_hist_id, ndid, eid, encounter_date, social_hist_date,
     social_hist_category, social_hist_subcategory, social_hist_question,
     social_hist_value, social_hist_code, social_hist_coding_system,
     social_hist_notes, data_source,
     created_datetime, created_by, updated_datetime, updated_by,
     ehr_source_name, source_path, data_type,
     psid, nd_extracted_date)
SELECT
    shr.SOCIALHXFORMRESPONSEID,
    shr.CHARTID,
    shr.CLINICALENCOUNTERID,
    {enc_date},
    {enc_date},
    'SocialHistory',
    NULL,
    ans.QUESTIONKEY,
    ans.VALUE,
    NULL,
    NULL,
    ans.NOTE,
    'athenaone',
    CURRENT_DATE(),
    'ND',
    CURRENT_DATE(),
    'ND',
    'athenaone',
    'bronze_layer',
    'Structured',
    {PSID},
    ans.nd_extracted_date
FROM {STAGING_ANS} ans
INNER JOIN {SOURCE_SCHEMA}.SOCIALHXFORMRESPONSE shr
    ON ans.SOCIALHXFORMRESPONSEID = shr.SOCIALHXFORMRESPONSEID
    AND shr.nd_active_flag = 'Y'
LEFT JOIN {STAGING_CE} ce
    ON shr.CHARTID = ce.chartid
    AND shr.CLINICALENCOUNTERID = ce.clinicalencounterid
WHERE ans.SOCIALHXFORMRESPONSEID >= {pk_lo}
  AND ans.SOCIALHXFORMRESPONSEID < {pk_hi}
"""


def build_batch_insert_branch2(pk_lo, pk_hi):
    """PATIENTSOCIALHISTORY — no JOINs, filter already applied in PK staging."""
    return f"""
INSERT INTO {DEST_TABLE}
    (social_hist_id, ndid, eid, encounter_date, social_hist_date,
     social_hist_category, social_hist_subcategory, social_hist_question,
     social_hist_value, social_hist_code, social_hist_coding_system,
     social_hist_notes, data_source,
     created_datetime, created_by, updated_datetime, updated_by,
     ehr_source_name, source_path, data_type,
     psid, nd_extracted_date)
SELECT
    p.socialhistoryid,
    p.CHARTID,
    NULL,
    NULL,
    {date_case('p.CREATEDDATETIME')},
    p.socialhistorykey,
    NULL,
    p.socialhistoryname,
    p.socialhistoryanswer,
    NULL,
    NULL,
    NULL,
    'athenaone',
    CURRENT_DATE(),
    'ND',
    CURRENT_DATE(),
    'ND',
    'athenaone',
    'bronze_layer',
    'Structured',
    {PSID},
    p.nd_extracted_date
FROM {SOURCE_SCHEMA}.PATIENTSOCIALHISTORY p
WHERE p.socialhistorykey <> 'REVIEWED.SOCIALHISTORY'
  AND p.nd_active_flag = 'Y'
  AND p.deleteddatetime IS NULL
  AND p.socialhistoryid >= {pk_lo}
  AND p.socialhistoryid < {pk_hi}
"""


BUILD_FN = {
    "ao_social1": build_batch_insert_branch1,
    "ao_social2": build_batch_insert_branch2,
}


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


# ── Setup ──────────────────────────────────────────────────────────────

def setup_tables():
    """
    1. Pre-materialize CLINICALENCOUNTER and SOCIALHXFORMRESPONSEANSWER lookups.
    2. For each source: create PK staging and compute batch ranges.
    3. Create destination and checkpoint tables.
    Returns dict: source_key → list of (lo, hi) ranges.
    """
    conn = get_connection()
    cur  = conn.cursor()

    # ── 1. CLINICALENCOUNTER lookup (branch 1) ─────────────────────────
    print("  Materializing CLINICALENCOUNTER lookup (nd_active_flag = 'Y')...")
    if not _table_exists(cur, STAGING_CE):
        cur.execute(f"""
            CREATE TABLE {STAGING_CE} AS
            SELECT chartid, clinicalencounterid, ENCOUNTERDATE
            FROM {SOURCE_SCHEMA}.CLINICALENCOUNTER
            WHERE nd_active_flag = 'Y'
        """)
        cur.execute(f"ALTER TABLE {STAGING_CE} ADD INDEX idx_ce (chartid, clinicalencounterid)")
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")
    cur.execute(f"SELECT COUNT(*) FROM {STAGING_CE}")
    print(f"    {cur.fetchone()[0]:,} rows")

    # ── 2. SOCIALHXFORMRESPONSEANSWER lookup (branch 1) ────────────────
    print("  Materializing SOCIALHXFORMRESPONSEANSWER lookup (nd_active_flag = 'Y')...")
    if not _table_exists(cur, STAGING_ANS):
        cur.execute(f"""
            CREATE TABLE {STAGING_ANS} AS
            SELECT SOCIALHXFORMRESPONSEID, QUESTIONKEY, VALUE, NOTE, nd_extracted_date
            FROM {SOURCE_SCHEMA}.SOCIALHXFORMRESPONSEANSWER
            WHERE nd_active_flag = 'Y'
              AND DELETEDDATETIME IS NULL
        """)
        cur.execute(f"ALTER TABLE {STAGING_ANS} ADD INDEX idx_ans (SOCIALHXFORMRESPONSEID)")
        conn.commit()
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")
    cur.execute(f"SELECT COUNT(*) FROM {STAGING_ANS}")
    print(f"    {cur.fetchone()[0]:,} rows")

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

    # ── 5. Per-source PK staging + boundary sampling ───────────────────
    all_ranges = {}
    for src in SOURCES:
        pk         = src["pk"]
        table      = src["table"]
        stg        = src["staging_pk"]
        source_key = src["key"]

        print(f"  Creating PK staging for {SOURCE_SCHEMA}.{table}...")
        if not _table_exists(cur, stg):
            if source_key == "ao_social1":
                # Drive from SOCIALHXFORMRESPONSEANSWER (filtered); batch by SOCIALHXFORMRESPONSEID
                cur.execute(f"""
                    CREATE TABLE {stg} AS
                    SELECT DISTINCT {pk}
                    FROM {STAGING_ANS}
                    WHERE {pk} IS NOT NULL
                """)
            else:
                # Branch 2: apply filter in PK staging too
                cur.execute(f"""
                    CREATE TABLE {stg} AS
                    SELECT {pk}
                    FROM {SOURCE_SCHEMA}.{table}
                    WHERE {pk} IS NOT NULL
                      AND socialhistorykey <> 'REVIEWED.SOCIALHISTORY'
                      AND nd_active_flag = 'Y'
                """)
            cur.execute(f"ALTER TABLE {stg} ADD INDEX idx_pk ({pk})")
            conn.commit()
            print("    created")
        else:
            print("    already exists, reusing")

        cur.execute(f"SELECT COUNT(*) FROM {stg}")
        total = cur.fetchone()[0]
        print(f"    {total:,} rows")

        if total == 0:
            all_ranges[source_key] = []
            continue

        print(f"  Computing batch boundaries for {table}...")
        cur.execute(f"""
            SELECT {pk}
            FROM (
                SELECT {pk},
                       ROW_NUMBER() OVER (ORDER BY {pk}) AS rn
                FROM {stg}
            ) t
            WHERE (rn - 1) % {BATCH_SIZE} = 0
            ORDER BY {pk}
        """)
        boundaries = [row[0] for row in cur.fetchall() if row[0] is not None]

        cur.execute(f"SELECT MAX({pk}) FROM {stg}")
        max_pk = int(cur.fetchone()[0])

        ranges = []
        for i, lo in enumerate(boundaries):
            hi = boundaries[i + 1] if i + 1 < len(boundaries) else max_pk + 1
            ranges.append((lo, hi))

        print(f"    {len(ranges)} batches of ~{BATCH_SIZE:,} rows each")
        all_ranges[source_key] = ranges

    cur.close()
    conn.close()
    return all_ranges


# ── Worker ─────────────────────────────────────────────────────────────

def run_source(src, ranges, pbar):
    source_key = src["key"]
    build_fn   = BUILD_FN[source_key]
    conn = get_connection()

    if is_done(conn, source_key):
        conn.close()
        pbar.update(len(ranges))
        return source_key, {"status": "skipped", "rows": 0, "secs": 0}

    mark(conn, source_key, "running")
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
        mark(conn, source_key, "done", total_rows)
        conn.close()
        return source_key, {"status": "done", "rows": total_rows, "secs": elapsed}

    except Exception as exc:
        elapsed = round(time.time() - t0, 1)
        mark(conn, source_key, "failed", total_rows, str(exc))
        try:
            conn.close()
        except Exception:
            pass
        return source_key, {"status": f"FAILED: {exc}", "rows": total_rows, "secs": elapsed}


# ── Main ───────────────────────────────────────────────────────────────

def main():
    print(f"\n{'='*70}", flush=True)
    print(f"  AO Social History ETL — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  source     : {SOURCE_SCHEMA} (SOCIALHXFORMRESPONSE + PATIENTSOCIALHISTORY)")
    print(f"  dest       : {DEST_TABLE}")
    print(f"  ce_lookup  : {STAGING_CE}")
    print(f"  ans_lookup : {STAGING_ANS}")
    print(f"  checkpoint : {CHECKPOINT_TABLE}")
    print(f"  batch_size : {BATCH_SIZE:,}  |  workers: {MAX_WORKERS}")
    print(f"{'='*70}\n", flush=True)

    print("  Connecting to database...")
    sys.stdout.flush()
    all_ranges = setup_tables()

    total_batches = sum(len(r) for r in all_ranges.values())
    if total_batches == 0:
        print(f"\nNo eligible rows found in any source table. Exiting.")
        return

    results = {}
    with tqdm(total=total_batches, desc="Overall", unit="batch") as pbar:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
            futures = {
                pool.submit(run_source, src, all_ranges[src["key"]], pbar): src["key"]
                for src in SOURCES
                if all_ranges.get(src["key"])
            }
            for fut in as_completed(futures):
                source_key, result = fut.result()
                results[source_key] = result

    print(f"\n{'='*70}")
    print(f"  Per-source summary:")
    total_rows = 0
    any_failed = False
    for src in SOURCES:
        key = src["key"]
        res = results.get(key, {"status": "no rows", "rows": 0, "secs": 0})
        status = res["status"]
        rows   = res["rows"]
        secs   = res["secs"]
        if status == "done":
            tag = " DONE"
            total_rows += rows
        elif status == "skipped":
            tag = " SKIP"
        elif status == "no rows":
            tag = "  ---"
        else:
            tag = " FAIL"
            any_failed = True
        print(f"  [{tag}] {src['table']:<40}  {rows:>10,} rows  ({secs}s)")
        if status.startswith("FAILED"):
            print(f"         {status}")

    print(f"\n  Total rows inserted: {total_rows:,}")
    print(f"{'='*70}")

    print(f"\n  Cleanup SQL (run after verifying data):")
    print(f"    DROP TABLE IF EXISTS {STAGING_CE};")
    print(f"    DROP TABLE IF EXISTS {STAGING_ANS};")
    for src in SOURCES:
        print(f"    DROP TABLE IF EXISTS {src['staging_pk']};")
    print(f"    DROP TABLE IF EXISTS {CHECKPOINT_TABLE};")

    if any_failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
