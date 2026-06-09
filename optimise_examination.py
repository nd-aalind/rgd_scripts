#!/usr/bin/env python3
"""
Optimized ETL loader for: rgd_udm_silver.examination
Source: raleigh.CLINICALTEMPLATE INNER JOIN raleigh.CLINICALENCOUNTER on CLINICALENCOUNTERID

Optimizations applied:
- Materialized staging table of CLINICALENCOUNTER IDs (batching anchor)
- Batch by actual CLINICALENCOUNTERID values (not arithmetic ranges — IDs can be sparse)
- Index on join keys for both source tables
- Checkpoint/resume — re-run skips completed sources
- Disabled InnoDB checks per-session for bulk insert speed
- Commit after every batch (frees undo/log space)
- Progress bar via tqdm

Usage:
    python optimise_examination.py
"""

import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import pymysql
from tqdm import tqdm

# ── Configuration ────────────────────────────────────────────────────
DB_CONFIG = {
    "host":            "172.16.2.42",
    "port":            3306,
    "user":            "nd-root-mysql",
    "password":        "kmsamd89undsd4",
    "database":        "raleigh",
    "charset":         "utf8mb4",
    "connect_timeout": 30,
    "read_timeout":    21600,
    "write_timeout":   21600,
}

BATCH_SIZE  = 50_000   # CLINICALENCOUNTER IDs per batch
MAX_WORKERS = 1        # single source; wired for future expansion

# ── Change this one variable to run for a different schema ───────────
SOURCE_SCHEMA = "raleigh"   # e.g. "raleigh", "tng_athena_one", ...

DEST_TABLE       = "rgd_udm_silver.examination"
STAGING_TABLE    = f"staging.tmp_exam_enc_staging_{SOURCE_SCHEMA}"
CHECKPOINT_TABLE = f"staging.etl_checkpoint_exam_{SOURCE_SCHEMA}"

# ── Source definitions: (source_table, alias, column) ────────────────
# One entry per logical unit of work.
SOURCES = [
    ("CLINICALTEMPLATE", "ct", "CLINICALENCOUNTERID"),
]

# ── Source tables that need an index on their join key ───────────────
# WARNING: Creating indexes on large production tables takes time and
# acquires metadata locks. Set CREATE_INDEXES = False to skip if tables
# are already indexed or are actively under heavy load.
CREATE_INDEXES = True

SOURCE_TABLES_JOIN_KEY = {
    "CLINICALTEMPLATE":   "CLINICALENCOUNTERID",
    "CLINICALENCOUNTER":  "CLINICALENCOUNTERID",
}


# ── Helpers ──────────────────────────────────────────────────────────

def get_connection():
    """One connection per call — each thread gets its own."""
    return pymysql.connect(**DB_CONFIG)


def build_batch_insert(eid_lo, eid_hi):
    """
    Faithfully reproduces the original INNER JOIN logic in batched form.
    Batching is applied on CLINICALTEMPLATE.CLINICALENCOUNTERID (driving table).
    The subquery on CLINICALENCOUNTER is flattened into a direct JOIN condition.
    """
    return f"""
INSERT INTO {DEST_TABLE}
(
    examid,
    ndid,
    eid,
    enc_start_date,
    exam_date,
    exam_category,
    exam_name,
    exam_findings,
    finding_type,
    exam_parameters,
    created_datetime,
    created_by,
    ehr_source_name,
    source_path,
    data_type,
    psid,
    nd_extracted_date
)
SELECT
    CAST(ct.PATIENTTEMPLATEDATAID AS CHAR(30))       AS examid,
    CAST(ce.CHARTID AS SIGNED)                        AS ndid,
    CAST(ce.CLINICALENCOUNTERID AS SIGNED)            AS eid,
    CASE
        WHEN ce.ENCOUNTERDATE IN ('None', '')         THEN NULL
        WHEN LEFT(ce.ENCOUNTERDATE,10) REGEXP '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}$'
            THEN STR_TO_DATE(LEFT(ce.ENCOUNTERDATE,10), '%Y-%m-%d')
        WHEN LEFT(ce.ENCOUNTERDATE,10) REGEXP '^[0-9]{{2}}-[0-9]{{2}}-[0-9]{{4}}$'
            THEN STR_TO_DATE(LEFT(ce.ENCOUNTERDATE,10), '%m-%d-%Y')
        ELSE NULL
    END AS enc_start_date,
    CASE
        WHEN ce.ENCOUNTERDATE IN ('None', '')         THEN NULL
        WHEN LEFT(ce.ENCOUNTERDATE,10) REGEXP '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}$'
            THEN STR_TO_DATE(LEFT(ce.ENCOUNTERDATE,10), '%Y-%m-%d')
        WHEN LEFT(ce.ENCOUNTERDATE,10) REGEXP '^[0-9]{{2}}-[0-9]{{2}}-[0-9]{{4}}$'
            THEN STR_TO_DATE(LEFT(ce.ENCOUNTERDATE,10), '%m-%d-%Y')
        ELSE NULL
    END AS exam_date,
    CAST(ct.CLINICALTEMPLATENAME AS CHAR(255))        AS exam_category,
    CAST(ct.CLINICALTEMPLATEPARAGRAPH AS CHAR(255))   AS exam_name,
    CONCAT(
        COALESCE(ct.CLINICALTEMPLATESENTENCE, ''),
        COALESCE(ct.CLINICALFINDING, '')
    )                                                  AS exam_findings,
    CAST(ct.FINDINGTYPE AS CHAR(5000))                AS finding_type,
    CAST(ct.CLINICALTEMPLATESENTENCE AS CHAR(500))    AS exam_parameters,
    CURRENT_TIMESTAMP()                                AS created_datetime,
    'ND'                                               AS created_by,
    'athenaone'                                        AS ehr_source_name,
    'bronze_layer'                                     AS source_path,
    'Structured'                                       AS data_type,
    5                                                  AS psid,
    ct.nd_extracted_date
FROM {SOURCE_SCHEMA}.CLINICALTEMPLATE ct
INNER JOIN {SOURCE_SCHEMA}.CLINICALENCOUNTER ce
    ON ct.CLINICALENCOUNTERID = ce.CLINICALENCOUNTERID
WHERE
    ct.ENCOUNTERSECTION = 'PhysicalExam'
    AND ct.nd_active_flag = 'Y'
    AND ce.nd_active_flag = 'Y'
    AND ct.CLINICALENCOUNTERID >= {eid_lo} AND ct.CLINICALENCOUNTERID < {eid_hi}
"""


# ── Checkpoint ───────────────────────────────────────────────────────

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


# ── Setup ────────────────────────────────────────────────────────────

def setup_tables():
    """Create staging, destination, and checkpoint tables. Return eid ranges."""
    conn = get_connection()
    cur = conn.cursor()
    db_name = DB_CONFIG["database"]

    # ── 1. Staging table: materialize filtered CLINICALENCOUNTER IDs ──
    print("  Creating staging table...")
    cur.execute(
        "SELECT COUNT(*) FROM information_schema.tables "
        "WHERE table_schema = %s AND table_name = %s",
        (STAGING_TABLE.split(".")[0], STAGING_TABLE.split(".")[1]),
    )
    if cur.fetchone()[0] == 0:
        cur.execute(f"""
            CREATE TABLE {STAGING_TABLE} AS
            SELECT DISTINCT CLINICALENCOUNTERID
            FROM {SOURCE_SCHEMA}.CLINICALTEMPLATE
            WHERE ENCOUNTERSECTION = 'PhysicalExam'
              AND nd_active_flag = 'Y'
              AND CLINICALENCOUNTERID IS NOT NULL
        """)
        cur.execute(f"ALTER TABLE {STAGING_TABLE} ADD INDEX idx_eid (CLINICALENCOUNTERID)")
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")

    cur.execute(f"SELECT COUNT(*) FROM {STAGING_TABLE}")
    enc_count = cur.fetchone()[0]
    print(f"    {enc_count:,} distinct CLINICALENCOUNTERIDs")

    # ── 2. Ensure source tables have indexes on CLINICALENCOUNTERID ──
    if CREATE_INDEXES:
        print("  Checking/creating source table indexes...")
        for table, key in SOURCE_TABLES_JOIN_KEY.items():
            cur.execute("""
                SELECT COUNT(*) FROM information_schema.statistics
                WHERE table_schema = %s AND table_name = %s AND column_name = %s
            """, (db_name, table, key))
            if cur.fetchone()[0] == 0:
                print(f"    Creating index on {table}.{key} ...")
                cur.execute(f"CREATE INDEX idx_{key} ON {SOURCE_SCHEMA}.{table} ({key})")
                conn.commit()
                print(f"    Done")
            else:
                print(f"    {table}.{key} — index exists")
    else:
        print("  Skipping index creation (CREATE_INDEXES=False)")

    # ── 3. Destination table ─────────────────────────────────────────
    print("  Creating destination table...")
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {DEST_TABLE} (
            examid            VARCHAR(30)   DEFAULT NULL,
            ndid              BIGINT        DEFAULT NULL,
            eid               BIGINT        DEFAULT NULL,
            enc_start_date    DATE          DEFAULT NULL,
            exam_date         DATE          DEFAULT NULL,
            exam_category     VARCHAR(255)  DEFAULT NULL,
            exam_name         VARCHAR(255)  DEFAULT NULL,
            exam_findings     LONGTEXT,
            finding_type      VARCHAR(5000) DEFAULT NULL,
            exam_parameters   VARCHAR(500)  DEFAULT NULL,
            created_datetime  DATETIME      DEFAULT NULL,
            created_by        VARCHAR(10)   DEFAULT NULL,
            ehr_source_name   VARCHAR(50)   DEFAULT NULL,
            source_path       VARCHAR(50)   DEFAULT NULL,
            data_type         VARCHAR(50)   DEFAULT NULL,
            psid              INT           DEFAULT NULL,
            nd_extracted_date DATE          DEFAULT NULL
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
    """)
    conn.commit()
    print("    ready")

    # ── 4. Checkpoint table ──────────────────────────────────────────
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

    # ── 5. Compute batch ranges using server-side boundary sampling ───
    # Avoids loading millions of IDs into Python memory.
    # Fetches only the boundary rows: row 0, BATCH_SIZE, 2*BATCH_SIZE, ...
    print("  Computing batch boundaries...")
    sys.stdout.flush()

    cur.execute(f"SELECT COUNT(*) FROM {STAGING_TABLE}")
    total = cur.fetchone()[0]

    if total == 0:
        cur.close()
        conn.close()
        return []

    # Fetch only boundary CLINICALENCOUNTERIDs via ROW_NUMBER (MySQL 8+)
    cur.execute(f"""
        SELECT CLINICALENCOUNTERID
        FROM (
            SELECT CLINICALENCOUNTERID,
                   ROW_NUMBER() OVER (ORDER BY CLINICALENCOUNTERID) AS rn
            FROM {STAGING_TABLE}
        ) t
        WHERE (rn - 1) % {BATCH_SIZE} = 0
        ORDER BY CLINICALENCOUNTERID
    """)
    boundaries = [row[0] for row in cur.fetchall() if row[0] is not None]

    # Also grab the max for the final upper bound
    cur.execute(f"SELECT MAX(CLINICALENCOUNTERID) FROM {STAGING_TABLE}")
    max_eid = int(cur.fetchone()[0])

    cur.close()
    conn.close()

    ranges = []
    for i, lo in enumerate(boundaries):
        hi = boundaries[i + 1] if i + 1 < len(boundaries) else max_eid + 1
        ranges.append((lo, hi))

    print(f"    {len(ranges)} batches of ~{BATCH_SIZE:,} CLINICALENCOUNTERIDs each")
    return ranges


# ── Worker ───────────────────────────────────────────────────────────

def run_source(source_def, ranges, pbar):
    """Process one source across all eid-range batches."""
    table, column = source_def[0], source_def[2]
    source_key = f"{table}.{column}"

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

        # Disable InnoDB checks for bulk insert speed (session-scoped only)
        cur.execute("SET unique_checks = 0")
        cur.execute("SET foreign_key_checks = 0")

        for eid_lo, eid_hi in ranges:
            sql = build_batch_insert(eid_lo, eid_hi)
            cur.execute(sql)
            conn.commit()
            total_rows += cur.rowcount
            pbar.update(1)

        # Re-enable checks
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


# ── Main ─────────────────────────────────────────────────────────────

def main():
    print(f"\n{'='*70}", flush=True)
    print(f"  Examination ETL — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  source     : {SOURCE_SCHEMA}")
    print(f"  dest       : {DEST_TABLE}")
    print(f"  staging    : {STAGING_TABLE}")
    print(f"  workers    : {MAX_WORKERS}")
    print(f"  batch_size : {BATCH_SIZE:,}")
    print(f"{'='*70}\n", flush=True)

    print("  Connecting to database...")
    sys.stdout.flush()
    ranges = setup_tables()
    if not ranges:
        print("\nNo CLINICALENCOUNTERIDs found in filtered CLINICALTEMPLATE. Exiting.")
        return

    total_batches = len(SOURCES) * len(ranges)
    results = []

    with tqdm(total=total_batches, desc="Overall", unit="batch") as pbar:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
            futures = {
                pool.submit(run_source, src, ranges, pbar): src for src in SOURCES
            }
            for future in as_completed(futures):
                results.append(future.result())

    # Per-source summary
    print()
    for r in sorted(results, key=lambda x: x["source"]):
        if r["status"] == "done":
            tag = " DONE"
        elif r["status"] == "skipped":
            tag = " SKIP"
        else:
            tag = " FAIL"
        print(f"  [{tag}] {r['source']:<42} {r['rows']:>10,} rows  ({r['secs']}s)")

    done    = sum(1 for r in results if r["status"] == "done")
    skipped = sum(1 for r in results if r["status"] == "skipped")
    failed  = [r for r in results if r["status"].startswith("FAILED")]
    total   = sum(r["rows"] for r in results)

    print(f"\n{'='*70}")
    print(f"  Done: {done}  Skipped: {skipped}  Failed: {len(failed)}  |  Total rows: {total:,}")
    print(f"{'='*70}")

    if failed:
        print("\n  Failed sources:")
        for r in failed:
            print(f"    {r['source']}: {r['status']}")

    print(f"\n  Cleanup SQL:")
    print(f"    DROP TABLE IF EXISTS {DEST_TABLE};")
    print(f"    DROP TABLE IF EXISTS {STAGING_TABLE};")
    print(f"    DROP TABLE IF EXISTS {CHECKPOINT_TABLE};")

    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
