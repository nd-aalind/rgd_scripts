#!/usr/bin/env python3
"""
Optimized ETL loader for: udm_staging.notes_ao_fn
Sources (UNION ALL of 4 branches, all driven by CLINICALENCOUNTER):
  1. CLINICALENCOUNTERDATA       — LEFT JOIN on clinicalencounterid + contextid
  2. CHARTQUESTIONNAIRE+ANSWER   — LEFT JOIN on chartid + contextid (double join)
  3. CLINICALENCOUNTERDIAGNOSIS  — LEFT JOIN on clinicalencounterid + contextid
  4. CLINICALENCOUNTERPREPNOTE   — LEFT JOIN on clinicalencounterid + contextid

Pre-materialized lookup table (computed ONCE):
  - staging.n_ao_ce_v1_{SOURCE_SCHEMA}  (CLINICALENCOUNTER, nd_active_flag='Y')

Optimizations applied:
- CLINICALENCOUNTER pre-materialized once (not re-scanned per branch/batch)
- PK staging pre-filters eligible CLINICALENCOUNTERID values
- Batch by actual CLINICALENCOUNTERID values (not arithmetic ranges — IDs are sparse)
- Server-side boundary sampling (avoids loading all PKs into memory)
- 4 sources run in parallel (ThreadPoolExecutor)
- Commit after every batch (frees undo/log space)
- Checkpoint/resume — re-run skips completed sources
- Disabled InnoDB checks per-session for bulk insert speed
- Progress bar via tqdm

Usage:
    python opt_notes_ao.py
"""

import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import pymysql
from tqdm import tqdm
import os
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())

# ── Configuration ────────────────────────────────────────────────────
DB_CONFIG = {
    "host":            os.environ.get("DB_HOST"),
    "port":            3306,
    "user":            os.environ.get("DB_USER"),
    "password":        os.environ.get("DB_PASSWORD"),
    "database":        'tng_athena_one',
    "charset":         "utf8mb4",
    "connect_timeout": 30,
    "read_timeout":    21600,
    "write_timeout":   21600,
}

BATCH_SIZE  = 50_000
MAX_WORKERS = 4

SOURCE_SCHEMA = "tng_athena_one"
PSID          = 2

DEST_TABLE       = "suven.tng_notes_06022026"
STAGING_CE       = f"staging.n_ao_ce_v11_{SOURCE_SCHEMA}"
STAGING_CQA      = f"staging.n_ao_cqa_v11_{SOURCE_SCHEMA}"   # pre-joined CQ+CQA
STAGING_TABLE    = f"staging.tmp_n_ao_eid_v11_{SOURCE_SCHEMA}"
CHECKPOINT_TABLE = f"staging.etl_checkpoint_n_ao_v11_{SOURCE_SCHEMA}"

BATCH_KEY = "clinicalencounterid"

# ── Source definitions: (source_table, alias, note_column) ───────────
SOURCES = [
    ("CLINICALENCOUNTERDATA",      "ed", "encounterdataclob"),
    ("CHARTQUESTIONNAIRE",         "cq", "freetextanswer"),
    ("CLINICALENCOUNTERDIAGNOSIS",  "d",  "note"),
    ("CLINICALENCOUNTERPREPNOTE",   "p",  "prepnote"),
]

# ── Source tables that need an index on their join key ───────────────
SOURCE_TABLES_JOIN_KEY = {
    "CLINICALENCOUNTERDATA":      "CLINICALENCOUNTERID",
    "CHARTQUESTIONNAIRE":         "CHARTID",
    "CHARTQUESTIONNAIREANSWER":   "CHARTQUESTIONNAIREID",
    "CLINICALENCOUNTERDIAGNOSIS":  "CLINICALENCOUNTERID",
    "CLINICALENCOUNTERPREPNOTE":   "CLINICALENCOUNTERID",
}

# Plain string (not f-string) so regex {4}/{2} quantifiers stay literal
_ENC_DATE_CASE = """\
        CASE
            WHEN ce.encounterdate IS NULL
                 OR ce.encounterdate IN ('', 'None')
                THEN NULL
            WHEN ce.encounterdate REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}$'
                THEN DATE(ce.encounterdate)
            WHEN ce.encounterdate REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
                THEN STR_TO_DATE(ce.encounterdate, '%Y-%m-%d')
            WHEN ce.encounterdate REGEXP '^[0-9]{2}-[0-9]{2}-[0-9]{4}$'
                THEN STR_TO_DATE(ce.encounterdate, '%m-%d-%Y')
            ELSE NULL
        END"""

_INSERT_COLS = (
    "(ndid, eid, enc_start_date, note, note_type, note_source,"
    " created_datetime, created_by, ehr_source_name, source_path, data_type,"
    " psid, nd_extracted_date)"
)


# ── Batch INSERT builder ──────────────────────────────────────────────

def build_batch_insert(source_def, eid_lo, eid_hi):
    table = source_def[0]

    if table == "CLINICALENCOUNTERDATA":
        # No DISTINCT — LONGTEXT can't be hash-deduped; filesort on CLOB is catastrophic.
        # No staging copy either — copying CLOBs is as expensive as scanning them.
        # Composite index (CLINICALENCOUNTERID, CONTEXTID) on the raw table (created in
        # setup_tables) lets MySQL read only CLOB rows for this batch's CE range.
        return f"""
INSERT INTO {DEST_TABLE} {_INSERT_COLS}
SELECT
    CAST(ce.chartid AS SIGNED),
    CAST(ce.clinicalencounterid AS SIGNED),
{_ENC_DATE_CASE},
    ed.encounterdataclob,
    ed.`key`,
    'CLINICALENCOUNTERDATA',
    CURRENT_TIMESTAMP(),
    'ND',
    'athenaone',
    'bronze_layer',
    'Structured',
    {PSID},
    ce.nd_extracted_date
FROM {STAGING_CE} ce
INNER JOIN `{SOURCE_SCHEMA}`.CLINICALENCOUNTERDATA ed
    ON  ed.CLINICALENCOUNTERID = ce.clinicalencounterid
    AND ed.CONTEXTID           = ce.contextid
    AND ed.nd_active_flag      = 'Y'
    AND ed.encounterdataclob IS NOT NULL
    AND ed.encounterdataclob  != ''
WHERE ce.{BATCH_KEY} >= {eid_lo} AND ce.{BATCH_KEY} < {eid_hi}
"""

    elif table == "CHARTQUESTIONNAIRE":
        # Join to pre-materialized staging table instead of raw CQ+CQA double join.
        # The 3-way join on raw tables caused per-batch fan-out and was the bottleneck.
        return f"""
INSERT INTO {DEST_TABLE} {_INSERT_COLS}
SELECT DISTINCT
    CAST(ce.chartid AS SIGNED),
    CAST(ce.clinicalencounterid AS SIGNED),
{_ENC_DATE_CASE},
    cqa_s.freetextanswer,
    cqa_s.questionnairetemplatename,
    'CHARTQUESTIONNAIREANSWER',
    CURRENT_TIMESTAMP(),
    'ND',
    'athenaone',
    'bronze_layer',
    'Structured',
    {PSID},
    ce.nd_extracted_date
FROM {STAGING_CE} ce
LEFT JOIN {STAGING_CQA} cqa_s
    ON  cqa_s.CHARTID    = ce.chartid
    AND cqa_s.CONTEXTID  = ce.contextid
WHERE ce.{BATCH_KEY} >= {eid_lo} AND ce.{BATCH_KEY} < {eid_hi}
"""

    elif table == "CLINICALENCOUNTERDIAGNOSIS":
        return f"""
INSERT INTO {DEST_TABLE} {_INSERT_COLS}
SELECT DISTINCT
    CAST(ce.chartid AS SIGNED),
    CAST(ce.clinicalencounterid AS SIGNED),
{_ENC_DATE_CASE},
    d.note,
    d.status,
    'CLINICALENCOUNTERDIAGNOSIS',
    CURRENT_TIMESTAMP(),
    'ND',
    'athenaone',
    'bronze_layer',
    'Structured',
    {PSID},
    ce.nd_extracted_date
FROM {STAGING_CE} ce
LEFT JOIN {SOURCE_SCHEMA}.CLINICALENCOUNTERDIAGNOSIS d
    ON d.CLINICALENCOUNTERID = ce.clinicalencounterid
   AND d.CONTEXTID           = ce.contextid
   AND d.nd_active_flag      = 'Y'
WHERE ce.{BATCH_KEY} >= {eid_lo} AND ce.{BATCH_KEY} < {eid_hi}
"""

    elif table == "CLINICALENCOUNTERPREPNOTE":
        return f"""
INSERT INTO {DEST_TABLE} {_INSERT_COLS}
SELECT DISTINCT
    CAST(ce.chartid AS SIGNED),
    CAST(ce.clinicalencounterid AS SIGNED),
{_ENC_DATE_CASE},
    p.prepnote,
    NULL,
    'CLINICALENCOUNTERPREPNOTE',
    CURRENT_TIMESTAMP(),
    'ND',
    'athenaone',
    'bronze_layer',
    'Structured',
    {PSID},
    ce.nd_extracted_date
FROM {STAGING_CE} ce
LEFT JOIN {SOURCE_SCHEMA}.CLINICALENCOUNTERPREPNOTE p
    ON p.CLINICALENCOUNTERID = ce.clinicalencounterid
   AND p.CONTEXTID           = ce.contextid
   AND p.nd_active_flag      = 'Y'
WHERE ce.{BATCH_KEY} >= {eid_lo} AND ce.{BATCH_KEY} < {eid_hi}
"""

    raise ValueError(f"Unknown source table: {table}")


# ── Helpers ──────────────────────────────────────────────────────────

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
    """Pre-materialize CE lookup + PK staging + dest/checkpoint. Return batch ranges."""
    conn = get_connection()
    cur  = conn.cursor()

    # ── 1. Pre-materialize CLINICALENCOUNTER (used by all 4 branches) ─
    print("  Pre-materializing CLINICALENCOUNTER...")
    if not _table_exists(cur, STAGING_CE):
        cur.execute(f"""
            CREATE TABLE {STAGING_CE} AS
            SELECT clinicalencounterid, patientid, chartid,
                   encounterdate, contextid, nd_extracted_date
            FROM {SOURCE_SCHEMA}.CLINICALENCOUNTER
            WHERE nd_active_flag = 'Y'
              AND clinicalencounterid IS NOT NULL
        """)
        cur.execute(f"ALTER TABLE {STAGING_CE} ADD INDEX idx_eid     (clinicalencounterid)")
        cur.execute(f"ALTER TABLE {STAGING_CE} ADD INDEX idx_chartid (chartid)")
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")
    cur.execute(f"SELECT COUNT(*) FROM {STAGING_CE}")
    print(f"    {cur.fetchone()[0]:,} CLINICALENCOUNTER rows")

    # ── 2. Pre-materialize CHARTQUESTIONNAIRE+ANSWER (double join done once) ──
    # The per-batch 3-way join (ce→cq→cqa) was the bottleneck: one chartid can
    # fan out to thousands of cqa rows mid-query. Flattening it here means each
    # batch only does a single indexed lookup on staging_cqa.chartid.
    print("  Pre-materializing CHARTQUESTIONNAIRE+ANSWER...")
    if not _table_exists(cur, STAGING_CQA):
        cur.execute(f"""
            CREATE TABLE {STAGING_CQA} AS
            SELECT
                cq.CHARTID,
                cq.CONTEXTID,
                cqa.freetextanswer,
                cq.questionnairetemplatename
            FROM `{SOURCE_SCHEMA}`.CHARTQUESTIONNAIRE cq
            LEFT JOIN `{SOURCE_SCHEMA}`.CHARTQUESTIONNAIREANSWER cqa
                ON  cqa.CHARTQUESTIONNAIREID = cq.CHARTQUESTIONNAIREID
                AND cqa.CONTEXTID            = cq.CONTEXTID
                AND cqa.nd_active_flag       = 'Y'
            WHERE cq.nd_active_flag = 'Y'
        """)
        cur.execute(f"ALTER TABLE {STAGING_CQA} ADD INDEX idx_chartid_ctx (CHARTID, CONTEXTID)")
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")
    cur.execute(f"SELECT COUNT(*) FROM {STAGING_CQA}")
    print(f"    {cur.fetchone()[0]:,} CQ+CQA rows")

    # ── 3. Composite index on CLINICALENCOUNTERDATA (CLOB — do NOT copy to staging) ──
    # Copying CLOB data to a staging table takes as long as reading it — no gain.
    # A composite index (CLINICALENCOUNTERID, CONTEXTID) lets each batch do a fast
    # indexed lookup on the raw table and read only the rows for that CE range.
    print("  Ensuring composite index on CLINICALENCOUNTERDATA...")
    cur.execute("""
        SELECT COUNT(*) FROM information_schema.statistics
        WHERE table_schema = %s AND table_name = 'CLINICALENCOUNTERDATA'
          AND index_name = 'idx_ced_ceid_ctx'
    """, (SOURCE_SCHEMA,))
    if cur.fetchone()[0] == 0:
        print("    Creating idx_ced_ceid_ctx (CLINICALENCOUNTERID, CONTEXTID) — may take a moment...")
        cur.execute(f"""
            CREATE INDEX idx_ced_ceid_ctx
            ON `{SOURCE_SCHEMA}`.CLINICALENCOUNTERDATA (CLINICALENCOUNTERID, CONTEXTID)
        """)
        conn.commit()
        print("    done")
    else:
        print("    idx_ced_ceid_ctx exists")

    # ── 4. PK staging — distinct clinicalencounterid values ──────────
    print("  Creating PK staging...")
    if not _table_exists(cur, STAGING_TABLE):
        cur.execute(f"""
            CREATE TABLE {STAGING_TABLE} AS
            SELECT DISTINCT {BATCH_KEY}
            FROM {STAGING_CE}
        """)
        cur.execute(f"ALTER TABLE {STAGING_TABLE} ADD INDEX idx_eid ({BATCH_KEY})")
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")
    cur.execute(f"SELECT COUNT(*) FROM {STAGING_TABLE}")
    total = cur.fetchone()[0]
    print(f"    {total:,} distinct CLINICALENCOUNTERIDs")

    # ── 5. Ensure source table indexes ───────────────────────────────
    print("  Checking/creating source table indexes...")
    for table, key in SOURCE_TABLES_JOIN_KEY.items():
        cur.execute("""
            SELECT COUNT(*) FROM information_schema.statistics
            WHERE table_schema = %s AND table_name = %s AND column_name = %s
        """, (SOURCE_SCHEMA, table, key))
        if cur.fetchone()[0] == 0:
            print(f"    Creating index on {table}.{key} ...")
            cur.execute(f"CREATE INDEX idx_{key.lower()} ON {SOURCE_SCHEMA}.{table} ({key})")
            conn.commit()
            print("      done")
        else:
            print(f"    {table}.{key} — index exists")

    # ── 6. Destination table ─────────────────────────────────────────
    print("  Creating destination table...")
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {DEST_TABLE} (
            ndid              BIGINT       DEFAULT NULL,
            eid               BIGINT       DEFAULT NULL,
            enc_start_date    DATE         DEFAULT NULL,
            note              LONGTEXT,
            note_type         LONGTEXT,
            note_source       VARCHAR(50)  DEFAULT NULL,
            created_datetime  DATETIME     DEFAULT NULL,
            created_by        VARCHAR(10)  DEFAULT NULL,
            ehr_source_name   VARCHAR(50)  DEFAULT NULL,
            source_path       VARCHAR(50)  DEFAULT NULL,
            data_type         VARCHAR(50)  DEFAULT NULL,
            psid              INT          DEFAULT NULL,
            nd_extracted_date DATE         DEFAULT NULL
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
    """)
    conn.commit()
    print("    ready")

    # ── 7. Checkpoint table ──────────────────────────────────────────
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

    # ── 8. Compute batch boundaries ──────────────────────────────────
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
            FROM {STAGING_TABLE}
        ) t
        WHERE (rn - 1) % {BATCH_SIZE} = 0
        ORDER BY {BATCH_KEY}
    """)
    boundaries = [row[0] for row in cur.fetchall() if row[0] is not None]

    cur.execute(f"SELECT MAX({BATCH_KEY}) FROM {STAGING_TABLE}")
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
    table, _, col = source_def
    source_key = f"{table}.{col}"

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

        for eid_lo, eid_hi in ranges:
            sql = build_batch_insert(source_def, eid_lo, eid_hi)
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


# ── Main ─────────────────────────────────────────────────────────────

def main():
    print(f"\n{'='*70}", flush=True)
    print(f"  AthenaOne Notes ETL — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  source     : {SOURCE_SCHEMA}  (psid={PSID})")
    print(f"  dest       : {DEST_TABLE}")
    print(f"  ce_staging : {STAGING_CE}")
    print(f"  pk_staging : {STAGING_TABLE}")
    print(f"  checkpoint : {CHECKPOINT_TABLE}")
    print(f"  workers    : {MAX_WORKERS}")
    print(f"  batch_size : {BATCH_SIZE:,}")
    print(f"{'='*70}\n", flush=True)

    print("  Connecting to database...")
    sys.stdout.flush()
    ranges = setup_tables()

    if not ranges:
        print("\nNo CLINICALENCOUNTERIDs found. Exiting.")
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

    print()
    for r in sorted(results, key=lambda x: x["source"]):
        if r["status"] == "done":
            tag = " DONE"
        elif r["status"] == "skipped":
            tag = " SKIP"
        else:
            tag = " FAIL"
        print(f"  [{tag}] {r['source']:<45} {r['rows']:>10,} rows  ({r['secs']}s)")

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
    print(f"    DROP TABLE IF EXISTS {STAGING_CE};")
    print(f"    DROP TABLE IF EXISTS {STAGING_CQA};")
    print(f"    DROP TABLE IF EXISTS {STAGING_TABLE};")
    print(f"    DROP TABLE IF EXISTS {CHECKPOINT_TABLE};")

    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
