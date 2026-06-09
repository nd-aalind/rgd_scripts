#!/usr/bin/env python3
"""
Optimized ETL loader for: udm_staging.athenaone_progressnotes
Sources (UNION ALL):
  1. CLINICALENCOUNTERDATA   LEFT JOIN CLINICALENCOUNTER  — KEY LIKE '%FROZENSECTIONHTML_%', nd_active_flag='Y'
  2. CLINICALENCOUNTERDIAGNOSIS INNER JOIN CLINICALENCOUNTER — static KEY, nd_active_flag='Y'

Optimizations applied:
- Staging table materializes filtered CLINICALENCOUNTERID values (WHERE filters pre-applied)
- Batch by actual CLINICALENCOUNTERID values (not arithmetic ranges — IDs can be sparse)
- Index on join keys for both source tables
- Checkpoint/resume — re-run skips completed sources
- Disabled InnoDB checks per-session for bulk insert speed
- Commit after every batch (frees undo/log space)
- Progress bar via tqdm

Usage:
    python optimise_pgn_athenaone.py
"""

import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import pymysql
from tqdm import tqdm

# ── Configuration ────────────────────────────────────────────────────
DB_CONFIG = {
    "host":            "ndai-dev-rds-instance.cwp60ymu4ko0.us-east-1.rds.amazonaws.com",
    "port":            3306,
    "user":            "Aalind",
    "password":        "A@L1nd@123",
    "database":        'tng_athena_one',
    "charset":         "utf8mb4",
    "connect_timeout": 30,
    "read_timeout":    21600,
    "write_timeout":   21600,
}

BATCH_SIZE  = 50_000   # CLINICALENCOUNTERIDs per batch
MAX_WORKERS = 2        # 2 independent sources run in parallel

# ── Change this one variable to run for a different schema ───────────
SOURCE_SCHEMA = "tng_athena_one"   # e.g. "tncpa", "ncpa2", ...
PSID = 2

DEST_TABLE       = "suven.tng_progressnotes_06022026"
STAGING_TABLE    = f"staging.tmp_pgn_athenaone_staging_v9{SOURCE_SCHEMA}"
CHECKPOINT_TABLE = f"staging.etl_checkpoint_pgn_athenaone_v9{SOURCE_SCHEMA}"

# ── Source definitions: (source_table, alias, column) ────────────────
SOURCES = [
    ("CLINICALENCOUNTERDATA",     "a", "ENCOUNTERDATACLOB"),
    ("CLINICALENCOUNTERDIAGNOSIS", "a", "NOTE"),
]

# ── Source tables that need an index on their join key ───────────────
# WARNING: Creating indexes on large production tables takes time and
# acquires metadata locks. Set CREATE_INDEXES = False to skip if tables
# are already indexed or are actively under heavy load.
CREATE_INDEXES = True

SOURCE_TABLES_JOIN_KEY = {
    "CLINICALENCOUNTERDATA":     "CLINICALENCOUNTERID",
    "CLINICALENCOUNTERDIAGNOSIS": "CLINICALENCOUNTERID",
    "CLINICALENCOUNTER":          "CLINICALENCOUNTERID",
}


# ── Helpers ──────────────────────────────────────────────────────────

def get_connection():
    """One connection per call — each thread gets its own."""
    return pymysql.connect(**DB_CONFIG)


def build_batch_insert(source_def, eid_lo, eid_hi):
    """Builds the per-source INSERT SQL, preserving the original UNION ALL logic."""
    table = source_def[0]

    if table == "CLINICALENCOUNTERDATA":
        # Branch 1: LEFT JOIN, KEY LIKE filter, column = ENCOUNTERDATACLOB
        return f"""
INSERT INTO {DEST_TABLE}
    (chartid, clinicalencounterid,enc_date, `key`, encounterdataclob, psid, nd_extracted_date)
SELECT
    b.CHARTID,
    a.CLINICALENCOUNTERID,
    CASE
        WHEN b.ENCOUNTERDATE IS NULL
             OR b.ENCOUNTERDATE IN ('', 'None')
            THEN NULL
        WHEN b.ENCOUNTERDATE REGEXP '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}} [0-9]{{2}}:[0-9]{{2}}:[0-9]{{2}}$'
            THEN DATE(b.ENCOUNTERDATE)
        WHEN b.ENCOUNTERDATE REGEXP '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}$'
            THEN STR_TO_DATE(b.ENCOUNTERDATE, '%Y-%m-%d')
        WHEN b.ENCOUNTERDATE REGEXP '^[0-9]{{2}}-[0-9]{{2}}-[0-9]{{4}}$'
            THEN STR_TO_DATE(b.ENCOUNTERDATE, '%m-%d-%Y')
        ELSE NULL
    END AS enc_date,
    a.`KEY`,
    a.ENCOUNTERDATACLOB,
    {PSID},
    a.nd_extracted_date
FROM {SOURCE_SCHEMA}.CLINICALENCOUNTERDATA a
LEFT JOIN {SOURCE_SCHEMA}.CLINICALENCOUNTER b ON a.CLINICALENCOUNTERID = b.CLINICALENCOUNTERID and b.nd_active_flag = 'Y'
WHERE a.CLINICALENCOUNTERID >= {eid_lo} AND a.CLINICALENCOUNTERID < {eid_hi}
  AND a.`KEY` LIKE '%FROZENSECTIONHTML_%'
  AND a.nd_active_flag = 'Y'
"""

    elif table == "CLINICALENCOUNTERDIAGNOSIS":
        # Branch 2: INNER JOIN, static KEY, column = NOTE
        return f"""
INSERT INTO {DEST_TABLE}
    (chartid, clinicalencounterid,enc_date, `key`, encounterdataclob, psid, nd_extracted_date)
SELECT
    b.CHARTID,
    a.CLINICALENCOUNTERID,
    CASE
        WHEN b.ENCOUNTERDATE IS NULL
             OR b.ENCOUNTERDATE IN ('', 'None')
            THEN NULL
        WHEN b.ENCOUNTERDATE REGEXP '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}} [0-9]{{2}}:[0-9]{{2}}:[0-9]{{2}}$'
            THEN DATE(b.ENCOUNTERDATE)
        WHEN b.ENCOUNTERDATE REGEXP '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}$'
            THEN STR_TO_DATE(b.ENCOUNTERDATE, '%Y-%m-%d')
        WHEN b.ENCOUNTERDATE REGEXP '^[0-9]{{2}}-[0-9]{{2}}-[0-9]{{4}}$'
            THEN STR_TO_DATE(b.ENCOUNTERDATE, '%m-%d-%Y')
        ELSE NULL
    END AS enc_date,
    'FROZENSECTIONHTML_DiagnosisNote',
    a.NOTE,
    {PSID},
    a.nd_extracted_date
FROM {SOURCE_SCHEMA}.CLINICALENCOUNTERDIAGNOSIS a
JOIN {SOURCE_SCHEMA}.CLINICALENCOUNTER b ON a.CLINICALENCOUNTERID = b.CLINICALENCOUNTERID and b.nd_active_flag = 'Y'
WHERE a.CLINICALENCOUNTERID >= {eid_lo} AND a.CLINICALENCOUNTERID < {eid_hi}
  AND a.nd_active_flag = 'Y'
"""

    raise ValueError(f"Unknown source table: {table}")


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
    db_name = SOURCE_SCHEMA

    # ── 1. Staging table: materialize filtered CLINICALENCOUNTERIDs ──
    # WHERE filters are applied here so batching only covers rows that
    # will actually produce output (avoids empty batches).
    print("  Creating staging table...")
    cur.execute(
        "SELECT COUNT(*) FROM information_schema.tables "
        "WHERE table_schema = %s AND table_name = %s",
        (STAGING_TABLE.split(".")[0], STAGING_TABLE.split(".")[1]),
    )
    if cur.fetchone()[0] == 0:
        cur.execute(f"""
            CREATE TABLE {STAGING_TABLE} AS
            SELECT DISTINCT CLINICALENCOUNTERID FROM (
                SELECT CLINICALENCOUNTERID
                FROM {SOURCE_SCHEMA}.CLINICALENCOUNTERDATA
                WHERE CLINICALENCOUNTERID IS NOT NULL
                  AND `KEY` LIKE '%FROZENSECTIONHTML_%'
                  AND nd_active_flag = 'Y'
                UNION
                SELECT CLINICALENCOUNTERID
                FROM {SOURCE_SCHEMA}.CLINICALENCOUNTERDIAGNOSIS
                WHERE CLINICALENCOUNTERID IS NOT NULL
                  AND nd_active_flag = 'Y'
            ) combined
        """)
        cur.execute(f"ALTER TABLE {STAGING_TABLE} ADD INDEX idx_eid (CLINICALENCOUNTERID)")
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")

    cur.execute(f"SELECT COUNT(*) FROM {STAGING_TABLE}")
    enc_count = cur.fetchone()[0]
    print(f"    {enc_count:,} distinct CLINICALENCOUNTERIDs (filtered)")

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
            chartid             BIGINT      DEFAULT NULL,
            clinicalencounterid BIGINT      DEFAULT NULL,
            enc_date DATE DEFAULT NULL,
            `key`               VARCHAR(500) DEFAULT NULL,
            encounterdataclob   LONGTEXT,
            psid                INT         DEFAULT NULL,
            nd_extracted_date   DATE        DEFAULT NULL
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
    # Fetches only the boundary rows — avoids loading millions of IDs into memory.
    print("  Computing batch boundaries...")
    sys.stdout.flush()

    cur.execute(f"SELECT COUNT(*) FROM {STAGING_TABLE}")
    total = cur.fetchone()[0]

    if total == 0:
        cur.close()
        conn.close()
        return []

    # Fetch only boundary IDs via ROW_NUMBER (MySQL 8+)
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
            sql = build_batch_insert(source_def, eid_lo, eid_hi)
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
    print(f"  AthenaOne Progress Notes ETL — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
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
        print("\nNo CLINICALENCOUNTERIDs found matching filters. Exiting.")
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
