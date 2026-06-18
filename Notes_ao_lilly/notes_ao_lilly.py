#!/usr/bin/env python3
"""
Optimized ETL for: udm_staging.notes_lilly_final — AthenaOne clinical notes (Lilly cohort)

Sources (4 independent INSERT jobs):
  1. CLINICALENCOUNTERDATA       — encounter clob notes
  2. CHARTQUESTIONNAIREANSWER    — questionnaire free-text answers
  3. CLINICALENCOUNTERDIAGNOSIS  — diagnosis notes
  4. CLINICALENCOUNTERPREPNOTE   — prep notes

Optimizations:
- CLINICALENCOUNTER × patientlist_lilly_all pre-materialized as staging (not re-scanned per batch)
- Batching by actual clinicalencounterid values (sparse ID safe)
- ThreadPoolExecutor with 4 workers — one per source
- Checkpoint/resume — re-run skips completed sources
- Commit after every batch
- InnoDB checks disabled per-session for bulk speed
- tqdm progress bar
"""

import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import pymysql
from tqdm import tqdm

# ── Configuration ──────────────────────────────────────────────────────────────
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

BATCH_SIZE  = 50_000
MAX_WORKERS = 4

# ── Change these to run for a different schema/psid ────────────────────────────
SOURCE_SCHEMA = "tng_athena_one"
PSID          = 2

DEST_TABLE       = "udm_staging.notes_lilly_final_ral"
STAGING_TABLE    = f"staging.tmp_notes_lilly_ao_ce_{SOURCE_SCHEMA}"
PK_STAGING_TABLE = f"staging.tmp_notes_lilly_ao_pk_{SOURCE_SCHEMA}"
CHECKPOINT_TABLE = f"staging.etl_checkpoint_notes_lilly_ao_{SOURCE_SCHEMA}"

BATCH_KEY = "clinicalencounterid"

SOURCES = [
    {"key": f"notes_lilly.ced.{SOURCE_SCHEMA}", "label": "CLINICALENCOUNTERDATA"},
    {"key": f"notes_lilly.cqa.{SOURCE_SCHEMA}", "label": "CHARTQUESTIONNAIREANSWER"},
    {"key": f"notes_lilly.cdx.{SOURCE_SCHEMA}", "label": "CLINICALENCOUNTERDIAGNOSIS"},
    {"key": f"notes_lilly.cpn.{SOURCE_SCHEMA}", "label": "CLINICALENCOUNTERPREPNOTE"},
]


# ── Helpers ────────────────────────────────────────────────────────────────────
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


def _index_exists(cur, schema, table, column):
    cur.execute("""
        SELECT COUNT(*) FROM information_schema.statistics
        WHERE table_schema = %s AND table_name = %s AND column_name = %s
    """, (schema, table, column))
    return cur.fetchone()[0] > 0


# ── Checkpoint ─────────────────────────────────────────────────────────────────
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


# ── Batch INSERT builder ────────────────────────────────────────────────────────
def build_batch_insert(source_label, pk_lo, pk_hi):
    s = "s"

    # encounterdate is VARCHAR in AthenaOne — handle multiple storage formats.
    # {{4}} / {{2}} in this f-string produce literal {4}/{2} in the resulting
    # SQL string, which MySQL then evaluates as REGEXP quantifiers.
    enc_start_date = (
        f"CASE\n"
        f"          WHEN {s}.encounterdate IS NULL\n"
        f"            OR {s}.encounterdate IN ('', 'None') THEN NULL\n"
        f"          WHEN {s}.encounterdate REGEXP\n"
        f"              '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}} [0-9]{{2}}:[0-9]{{2}}:[0-9]{{2}}$'\n"
        f"              THEN DATE({s}.encounterdate)\n"
        f"          WHEN {s}.encounterdate REGEXP '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}$'\n"
        f"              THEN STR_TO_DATE({s}.encounterdate, '%Y-%m-%d')\n"
        f"          WHEN {s}.encounterdate REGEXP '^[0-9]{{2}}-[0-9]{{2}}-[0-9]{{4}}$'\n"
        f"              THEN STR_TO_DATE({s}.encounterdate, '%m-%d-%Y')\n"
        f"          ELSE NULL\n"
        f"      END"
    )

    insert_hdr = f"""INSERT INTO {DEST_TABLE}
    (ndid, eid, enc_start_date, note, note_type, note_source,
     created_datetime, created_by, ehr_source_name, source_path, data_type,
     psid, nd_extracted_date)"""

    select_prefix = f"""
    CAST({s}.chartid AS SIGNED),
    CAST({s}.clinicalencounterid AS SIGNED),
    {enc_start_date},"""

    select_suffix = f"""
    CURRENT_TIMESTAMP(),
    'ND',
    'athenaone',
    'bronze_layer',
    'Structured',
    {PSID},
    {s}.nd_extracted_date"""

    batch_where = (
        f"WHERE {s}.{BATCH_KEY} >= {pk_lo} AND {s}.{BATCH_KEY} < {pk_hi}"
    )

    if source_label == "CLINICALENCOUNTERDATA":
        return f"""
{insert_hdr}
SELECT DISTINCT{select_prefix}
    ed.encounterdataclob,
    ed.`key`,
    'CLINICALENCOUNTERDATA',{select_suffix}
FROM {STAGING_TABLE} {s}
INNER JOIN {SOURCE_SCHEMA}.CLINICALENCOUNTERDATA ed
    ON  {s}.clinicalencounterid = ed.clinicalencounterid
    AND {s}.contextid           = ed.contextid
    AND ed.nd_active_flag       = 'Y'
    AND ed.encounterdataclob IS NOT NULL
    AND ed.encounterdataclob    != ''
{batch_where}
"""

    if source_label == "CHARTQUESTIONNAIREANSWER":
        return f"""
{insert_hdr}
SELECT DISTINCT{select_prefix}
    cqa.freetextanswer,
    cq.questionnairetemplatename,
    'CHARTQUESTIONNAIREANSWER',{select_suffix}
FROM {STAGING_TABLE} {s}
LEFT JOIN {SOURCE_SCHEMA}.CHARTQUESTIONNAIRE cq
    ON  {s}.chartid       = cq.chartid
    AND {s}.contextid     = cq.contextid
    AND cq.nd_active_flag = 'Y'
LEFT JOIN {SOURCE_SCHEMA}.CHARTQUESTIONNAIREANSWER cqa
    ON  cq.chartquestionnaireid = cqa.chartquestionnaireid
    AND cq.contextid            = cqa.contextid
    AND cqa.nd_active_flag      = 'Y'
{batch_where}
"""

    if source_label == "CLINICALENCOUNTERDIAGNOSIS":
        return f"""
{insert_hdr}
SELECT DISTINCT{select_prefix}
    d.note,
    d.status,
    'CLINICALENCOUNTERDIAGNOSIS',{select_suffix}
FROM {STAGING_TABLE} {s}
LEFT JOIN {SOURCE_SCHEMA}.CLINICALENCOUNTERDIAGNOSIS d
    ON  {s}.clinicalencounterid = d.clinicalencounterid
    AND {s}.contextid           = d.contextid
    AND d.nd_active_flag        = 'Y'
{batch_where}
"""

    if source_label == "CLINICALENCOUNTERPREPNOTE":
        return f"""
{insert_hdr}
SELECT DISTINCT{select_prefix}
    p.prepnote,
    NULL,
    'CLINICALENCOUNTERPREPNOTE',{select_suffix}
FROM {STAGING_TABLE} {s}
LEFT JOIN {SOURCE_SCHEMA}.CLINICALENCOUNTERPREPNOTE p
    ON  {s}.clinicalencounterid = p.clinicalencounterid
    AND {s}.contextid           = p.contextid
    AND p.nd_active_flag        = 'Y'
{batch_where}
"""

    raise ValueError(f"Unknown source_label: {source_label}")


# ── Setup ──────────────────────────────────────────────────────────────────────
def setup_tables():
    """
    Create staging, PK-range, destination, and checkpoint tables.
    Returns list of (pk_lo, pk_hi) batch ranges shared across all 4 sources.
    """
    conn = get_connection()
    cur  = conn.cursor()

    # 1. Indexes on CLINICALENCOUNTER and patientlist_lilly_all — used during
    #    staging creation, so must exist before that CREATE TABLE AS SELECT.
    print("  Ensuring pre-staging indexes...")
    pre_indexes = [
        ("udm_staging", "patientlist_lilly_all", "ndid",    "idx_pll_ndid"),
        (SOURCE_SCHEMA, "CLINICALENCOUNTER",      "chartid", "idx_ce_chartid"),
    ]
    for schema, tbl, col, idx_name in pre_indexes:
        if not _index_exists(cur, schema, tbl, col):
            print(f"    creating {schema}.{tbl}({col})...")
            cur.execute(f"CREATE INDEX {idx_name} ON {schema}.{tbl} ({col})")
            conn.commit()
    print("    done")

    # 2. Materialize CLINICALENCOUNTER filtered to Lilly patients
    print("  Creating CE staging table...")
    if not _table_exists(cur, STAGING_TABLE):
        cur.execute(f"""
            CREATE TABLE {STAGING_TABLE} AS
            SELECT DISTINCT
                e.clinicalencounterid,
                e.chartid,
                e.encounterdate,
                e.contextid,
                e.nd_extracted_date
            FROM {SOURCE_SCHEMA}.CLINICALENCOUNTER e
            INNER JOIN udm_staging.patientlist_lilly_all p
                ON p.ndid = e.chartid
            WHERE e.nd_active_flag = 'Y'
        """)
        cur.execute(
            f"ALTER TABLE {STAGING_TABLE} "
            f"ADD INDEX idx_encid     ({BATCH_KEY}), "
            f"ADD INDEX idx_chartid_ctx (chartid, contextid), "
            f"ADD INDEX idx_encid_ctx  (clinicalencounterid, contextid)"
        )
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")

    cur.execute(f"SELECT COUNT(*) FROM {STAGING_TABLE}")
    total = cur.fetchone()[0]
    print(f"    {total:,} eligible encounters")

    # 3. Indexes on source tables joined during batch inserts
    print("  Ensuring source table indexes...")
    source_indexes = [
        (SOURCE_SCHEMA, "CLINICALENCOUNTERDATA",      "clinicalencounterid", "idx_ced_encid"),
        (SOURCE_SCHEMA, "CLINICALENCOUNTERDATA",      "contextid",           "idx_ced_ctx"),
        (SOURCE_SCHEMA, "CHARTQUESTIONNAIRE",         "chartid",             "idx_cq_chartid"),
        (SOURCE_SCHEMA, "CHARTQUESTIONNAIRE",         "contextid",           "idx_cq_ctx"),
        (SOURCE_SCHEMA, "CHARTQUESTIONNAIRE",         "chartquestionnaireid","idx_cq_pk"),
        (SOURCE_SCHEMA, "CHARTQUESTIONNAIREANSWER",   "chartquestionnaireid","idx_cqa_cqid"),
        (SOURCE_SCHEMA, "CHARTQUESTIONNAIREANSWER",   "contextid",           "idx_cqa_ctx"),
        (SOURCE_SCHEMA, "CLINICALENCOUNTERDIAGNOSIS", "clinicalencounterid", "idx_cdx_encid"),
        (SOURCE_SCHEMA, "CLINICALENCOUNTERDIAGNOSIS", "contextid",           "idx_cdx_ctx"),
        (SOURCE_SCHEMA, "CLINICALENCOUNTERPREPNOTE",  "clinicalencounterid", "idx_cpn_encid"),
        (SOURCE_SCHEMA, "CLINICALENCOUNTERPREPNOTE",  "contextid",           "idx_cpn_ctx"),
    ]
    for schema, tbl, col, idx_name in source_indexes:
        if not _index_exists(cur, schema, tbl, col):
            print(f"    creating {schema}.{tbl}({col})...")
            cur.execute(f"CREATE INDEX {idx_name} ON {schema}.{tbl} ({col})")
            conn.commit()
    print("    done")

    # 4. Destination table
    print("  Creating destination table...")
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {DEST_TABLE} (
            ndid              BIGINT        DEFAULT NULL,
            eid               BIGINT        DEFAULT NULL,
            enc_start_date    DATE          DEFAULT NULL,
            note              LONGTEXT,
            note_type         VARCHAR(500)  DEFAULT NULL,
            note_source       VARCHAR(100)  DEFAULT NULL,
            created_datetime  DATETIME      DEFAULT NULL,
            created_by        VARCHAR(50)   DEFAULT NULL,
            ehr_source_name   VARCHAR(100)  DEFAULT NULL,
            source_path       VARCHAR(100)  DEFAULT NULL,
            data_type         VARCHAR(50)   DEFAULT NULL,
            psid              INT           DEFAULT NULL,
            nd_extracted_date DATE          DEFAULT NULL
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
    """)
    conn.commit()
    print("    ready")

    # 5. Checkpoint table
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

    # 6. PK staging table + batch boundaries (shared across all 4 sources)
    print("  Computing batch boundaries...")
    if total == 0:
        cur.close()
        conn.close()
        return []

    if not _table_exists(cur, PK_STAGING_TABLE):
        cur.execute(f"""
            CREATE TABLE {PK_STAGING_TABLE} AS
            SELECT {BATCH_KEY}
            FROM {STAGING_TABLE}
            WHERE {BATCH_KEY} IS NOT NULL
            ORDER BY {BATCH_KEY}
        """)
        cur.execute(f"ALTER TABLE {PK_STAGING_TABLE} ADD INDEX idx_pk ({BATCH_KEY})")
        conn.commit()

    cur.execute(f"SELECT COUNT(*) FROM {PK_STAGING_TABLE}")
    pk_count = cur.fetchone()[0]

    cur.execute(f"""
        SELECT {BATCH_KEY}
        FROM (
            SELECT {BATCH_KEY},
                   ROW_NUMBER() OVER (ORDER BY {BATCH_KEY}) AS rn
            FROM {PK_STAGING_TABLE}
        ) t
        WHERE (rn - 1) % {BATCH_SIZE} = 0
        ORDER BY {BATCH_KEY}
    """)
    boundaries = [row[0] for row in cur.fetchall() if row[0] is not None]

    cur.execute(f"SELECT MAX({BATCH_KEY}) FROM {PK_STAGING_TABLE}")
    max_pk = int(cur.fetchone()[0])

    cur.close()
    conn.close()

    ranges = []
    for i, lo in enumerate(boundaries):
        hi = boundaries[i + 1] if i + 1 < len(boundaries) else max_pk + 1
        ranges.append((lo, hi))

    print(f"    {pk_count:,} PKs → {len(ranges)} batches of ~{BATCH_SIZE:,} rows each")
    return ranges


# ── Worker ─────────────────────────────────────────────────────────────────────
def run_source(source, ranges, pbar):
    key   = source["key"]
    label = source["label"]
    conn  = get_connection()

    if is_done(conn, key):
        conn.close()
        pbar.update(len(ranges))
        return {"source": label, "status": "skipped", "rows": 0, "secs": 0}

    mark(conn, key, "running")
    t0         = time.time()
    total_rows = 0

    try:
        cur = conn.cursor()
        cur.execute("SET unique_checks = 0")
        cur.execute("SET foreign_key_checks = 0")

        for pk_lo, pk_hi in ranges:
            sql = build_batch_insert(label, pk_lo, pk_hi)
            cur.execute(sql)
            conn.commit()
            total_rows += cur.rowcount
            pbar.update(1)

        cur.execute("SET unique_checks = 1")
        cur.execute("SET foreign_key_checks = 1")
        cur.close()

        mark(conn, key, "done", total_rows)
        conn.close()
        return {
            "source": label, "status": "done",
            "rows": total_rows, "secs": round(time.time() - t0, 1),
        }

    except Exception as exc:
        mark(conn, key, "failed", total_rows, str(exc))
        try:
            conn.close()
        except Exception:
            pass
        return {
            "source": label, "status": f"FAILED: {exc}",
            "rows": total_rows, "secs": round(time.time() - t0, 1),
        }


# ── Main ───────────────────────────────────────────────────────────────────────
def main():
    print(f"\n{'='*70}", flush=True)
    print(f"  AthenaOne Notes (Lilly) ETL — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  source     : {SOURCE_SCHEMA}.CLINICALENCOUNTER + 4 note tables  (psid={PSID})")
    print(f"  dest       : {DEST_TABLE}")
    print(f"  staging    : {STAGING_TABLE}")
    print(f"  checkpoint : {CHECKPOINT_TABLE}")
    print(f"  workers    : {MAX_WORKERS}  |  batch_size : {BATCH_SIZE:,}")
    print(f"{'='*70}\n", flush=True)

    ranges = setup_tables()

    if not ranges:
        print(f"\nNo eligible encounters in {SOURCE_SCHEMA}. Exiting.")
        return

    total_batches = len(ranges) * len(SOURCES)
    results = []
    with tqdm(total=total_batches, desc="Overall", unit="batch") as pbar:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
            futures = {
                pool.submit(run_source, src, ranges, pbar): src
                for src in SOURCES
            }
            for future in as_completed(futures):
                results.append(future.result())

    print()
    for r in sorted(results, key=lambda x: x["source"]):
        tag = "DONE" if r["status"] == "done" \
              else "SKIP" if r["status"] == "skipped" \
              else "FAIL"
        print(f"  [{tag}] {r['source']:<40} {r['rows']:>10,} rows  ({r['secs']}s)")

    done    = sum(1 for r in results if r["status"] == "done")
    skipped = sum(1 for r in results if r["status"] == "skipped")
    failed  = [r for r in results if "FAILED" in str(r["status"])]
    total   = sum(r["rows"] for r in results)

    print(f"\n{'='*70}")
    print(f"  Done: {done}  Skipped: {skipped}  Failed: {len(failed)}  |  Total rows: {total:,}")
    print(f"{'='*70}")

    print(f"\n  Cleanup SQL:")
    print(f"    DROP TABLE IF EXISTS {STAGING_TABLE};")
    print(f"    DROP TABLE IF EXISTS {PK_STAGING_TABLE};")
    print(f"    DROP TABLE IF EXISTS {CHECKPOINT_TABLE};")

    if failed:
        print("\n  Failed sources:")
        for r in failed:
            print(f"    {r['source']}: {r['status']}")
        sys.exit(1)


if __name__ == "__main__":
    main()
