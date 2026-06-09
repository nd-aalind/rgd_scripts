#!/usr/bin/env python3
"""
Optimized Problem List ETL (Greenway) for: udm_staging.problemlist

Sources (2 independent INSERT jobs run in parallel):
  1. PatHistProblemList        LEFT JOIN PatHistCatPatHistItem, PatHistItem, ProblemListStatus
  2. PatHistProblemListHistory LEFT JOIN PatHistCatPatHistItem, PatHistItem, ProblemListStatus

Pre-materialized lookup tables (computed ONCE, reused across all batches):
  - staging.gw_pl_pathist_cat_{SOURCE_SCHEMA}    (PatHistCatPatHistItem)
  - staging.gw_pl_pathist_item_{SOURCE_SCHEMA}   (PatHistItem)
  - staging.gw_pl_status_{SOURCE_SCHEMA}         (ProblemListStatus)

Optimizations applied:
- Lookup tables pre-materialized once (not re-scanned per batch)
- Each source batched independently by its primary key
- Server-side boundary sampling (avoids loading all PKs into memory)
- 2 sources run in parallel via ThreadPoolExecutor
- Commit after every batch (frees undo/log space)
- Checkpoint/resume — re-run skips completed sources
- Disabled InnoDB checks per-session for bulk insert speed
- Progress bar via tqdm

Usage:
    python opt_prob_list_2_union_gw.py
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
    "database":        "mind",
    "charset":         "utf8mb4",
    "connect_timeout": 30,
    "read_timeout":    21600,
    "write_timeout":   21600,
}

BATCH_SIZE  = 50_000   # PKs per batch
MAX_WORKERS = 2        # 2 independent sources run in parallel

# ── Change these two variables to run for a different schema/psid ────
SOURCE_SCHEMA = "jwm"   # e.g. "greenway", "gw_raleigh", ...
PSID          = 11

DEST_TABLE       = "udm_staging.problemlist_fn"
CHECKPOINT_TABLE = f"staging.etl_checkpoint_pl_gw1_{SOURCE_SCHEMA}"

# ── Pre-materialized lookup staging tables ───────────────────────────
STAGING_PATHIST_CAT_ITEM = f"staging.gw_pl_pathist_cat_v2_{SOURCE_SCHEMA}"
STAGING_PATHIST_ITEM     = f"staging.gw_pl_pathist_item_v2_{SOURCE_SCHEMA}"
STAGING_STATUS           = f"staging.gw_pl_status_v2_{SOURCE_SCHEMA}"

# ── Per-source definitions ────────────────────────────────────────────
SOURCES = [
    {
        "table":   "PatHistProblemList",
        "pk":      "PatHistProblemListID",
        "staging": f"staging.tmp_gw_pl_staging_v2_{SOURCE_SCHEMA}",
    },
    {
        "table":   "PatHistProblemListHistory",
        "pk":      "PatHistProblemListID",
        "staging": f"staging.tmp_gw_plh_staging_v2_{SOURCE_SCHEMA}",
    },
]


# ── Date CASE helper ─────────────────────────────────────────────────

def date_case(col):
    """
    Returns a CASE expression that converts a DATETIME/VARCHAR column to DATE.
    Wraps col in CAST(... AS CHAR) so that comparisons to '' and REGEXP work
    even when the source column is a DATETIME type (avoids MySQL 1292 in strict
    mode). {{4}} / {{2}} produce literal {4} / {2} in the returned string.
    """
    c = f"CAST({col} AS CHAR)"
    return (
        f"CASE\n"
        f"            WHEN {c} IS NULL OR {c} IN ('', 'None', '0000-00-00', '0000-00-00 00:00:00') THEN NULL\n"
        f"            WHEN {c} REGEXP '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}} [0-9]{{2}}:[0-9]{{2}}:[0-9]{{2}}$'\n"
        f"                THEN DATE({c})\n"
        f"            WHEN {c} REGEXP '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}$'\n"
        f"                THEN STR_TO_DATE({c}, '%Y-%m-%d')\n"
        f"            WHEN {c} REGEXP '^[0-9]{{2}}-[0-9]{{2}}-[0-9]{{4}}$'\n"
        f"                THEN STR_TO_DATE({c}, '%m-%d-%Y')\n"
        f"            ELSE NULL\n"
        f"        END"
    )


# ── Batch INSERT builders ─────────────────────────────────────────────

def build_batch_insert(source, pk_lo, pk_hi):
    table = source["table"]
    pk    = source["pk"]
    p     = "p"
    pci   = "pci"
    phi   = "phi"
    pls   = "pls"

    if table == "PatHistProblemList":
        return f"""
INSERT INTO {DEST_TABLE}
    (diag_id, ndid, eid, encounter_date, problem_date, problem_onset_date,
     problem_end_date, resolved, problem_desc, snomed_code, icd_code,
     problem_type, status, severity, laterality, problem_notes,
     data_source, psid, nd_extracted_date)
SELECT
    CAST({p}.{pk} AS SIGNED),
    CAST({p}.PatientID AS SIGNED),
    NULL,
    NULL,
    {date_case(f'{p}.LastChanged')},
    {date_case(f'{p}.ProblemStartDate')},
    {date_case(f'{p}.DateResolved')},
    CAST({p}.IsCurrent AS SIGNED),
    {phi}.PatHistItemDescription,
    CASE WHEN {phi}.AltSystem LIKE '%SNO%' THEN {phi}.AltSystemCode ELSE NULL END,
    CASE WHEN {phi}.AltSystem LIKE '%ICD%' THEN {phi}.AltSystemCode ELSE NULL END,
    NULL,
    {pls}.ProblemListStatusDesc,
    NULL,
    NULL,
    {p}.ProblemNote,
    'Greenway',
    {PSID},
    NULL
FROM {SOURCE_SCHEMA}.PatHistProblemList {p}
LEFT JOIN {STAGING_PATHIST_CAT_ITEM} {pci}
    ON {pci}.PatHistCatPatHistItemID = {p}.PatHistCatPatHistItemID
LEFT JOIN {STAGING_PATHIST_ITEM} {phi}
    ON {phi}.PatHistItemID = {pci}.PatHistItemID
LEFT JOIN {STAGING_STATUS} {pls}
    ON {pls}.ProblemListStatusID = {p}.ProblemListStatusID
WHERE {p}.{pk} >= {pk_lo} AND {p}.{pk} < {pk_hi}
"""

    if table == "PatHistProblemListHistory":
        return f"""
INSERT INTO {DEST_TABLE}
    (diag_id, ndid, eid, encounter_date, problem_date, problem_onset_date,
     problem_end_date, resolved, problem_desc, snomed_code, icd_code,
     problem_type, status, severity, laterality, problem_notes,
     data_source, psid, nd_extracted_date)
SELECT
    CAST({p}.{pk} AS SIGNED),
    CAST({p}.PatientID AS SIGNED),
    NULL,
    NULL,
    {date_case(f'{p}.LastChanged')},
    {date_case(f'{p}.ProblemStartDate')},
    {date_case(f'{p}.DateResolved')},
    NULL,
    {phi}.PatHistItemDescription,
    CASE WHEN {phi}.AltSystem LIKE '%SNO%' THEN {phi}.AltSystemCode ELSE NULL END,
    CASE WHEN {phi}.AltSystem LIKE '%ICD%' THEN {phi}.AltSystemCode ELSE NULL END,
    NULL,
    {pls}.ProblemListStatusDesc,
    NULL,
    NULL,
    {p}.ProblemNote,
    'Greenway',
    {PSID},
    NULL
FROM {SOURCE_SCHEMA}.PatHistProblemListHistory {p}
LEFT JOIN {STAGING_PATHIST_CAT_ITEM} {pci}
    ON {pci}.PatHistCatPatHistItemID = {p}.PatHistCatPatHistItemID
LEFT JOIN {STAGING_PATHIST_ITEM} {phi}
    ON {phi}.PatHistItemID = {pci}.PatHistItemID
LEFT JOIN {STAGING_STATUS} {pls}
    ON {pls}.ProblemListStatusID = {p}.ProblemListStatusID
WHERE {p}.{pk} >= {pk_lo} AND {p}.{pk} < {pk_hi}
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


# ── Setup ─────────────���──────────────────────────────────────────────

def setup_lookup_tables(conn, cur):
    """Pre-materialize lookup tables. Computed once, used by all batches."""

    print("  Materializing PatHistCatPatHistItem lookup...")
    if not _table_exists(cur, STAGING_PATHIST_CAT_ITEM):
        cur.execute(f"""
            CREATE TABLE {STAGING_PATHIST_CAT_ITEM} AS
            SELECT * FROM {SOURCE_SCHEMA}.PatHistCatPatHistItem
        """)
        cur.execute(f"ALTER TABLE {STAGING_PATHIST_CAT_ITEM} ADD INDEX idx_cat (PatHistCatPatHistItemID)")
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")
    cur.execute(f"SELECT COUNT(*) FROM {STAGING_PATHIST_CAT_ITEM}")
    print(f"    {cur.fetchone()[0]:,} rows")

    print("  Materializing PatHistItem lookup...")
    if not _table_exists(cur, STAGING_PATHIST_ITEM):
        cur.execute(f"""
            CREATE TABLE {STAGING_PATHIST_ITEM} AS
            SELECT * FROM {SOURCE_SCHEMA}.PatHistItem
        """)
        cur.execute(f"ALTER TABLE {STAGING_PATHIST_ITEM} ADD INDEX idx_item (PatHistItemID)")
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")
    cur.execute(f"SELECT COUNT(*) FROM {STAGING_PATHIST_ITEM}")
    print(f"    {cur.fetchone()[0]:,} rows")

    print("  Materializing ProblemListStatus lookup...")
    if not _table_exists(cur, STAGING_STATUS):
        cur.execute(f"""
            CREATE TABLE {STAGING_STATUS} AS
            SELECT * FROM {SOURCE_SCHEMA}.ProblemListStatus
        """)
        cur.execute(f"ALTER TABLE {STAGING_STATUS} ADD INDEX idx_status (ProblemListStatusID)")
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")
    cur.execute(f"SELECT COUNT(*) FROM {STAGING_STATUS}")
    print(f"    {cur.fetchone()[0]:,} rows")


def setup_source_staging(conn, cur, source):
    """Create PK staging table for one source. Return batch ranges."""
    table   = source["table"]
    pk      = source["pk"]
    staging = source["staging"]

    print(f"  Creating PK staging for {table}...")
    if not _table_exists(cur, staging):
        cur.execute(f"""
            CREATE TABLE {staging} AS
            SELECT {pk}
            FROM {SOURCE_SCHEMA}.{table}
            WHERE {pk} IS NOT NULL
        """)
        cur.execute(f"ALTER TABLE {staging} ADD INDEX idx_pk ({pk})")
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")

    cur.execute(f"SELECT COUNT(*) FROM {staging}")
    count = cur.fetchone()[0]
    print(f"    {count:,} rows to process")

    if count == 0:
        return []

    cur.execute(f"""
        SELECT {pk}
        FROM (
            SELECT {pk},
                   ROW_NUMBER() OVER (ORDER BY {pk}) AS rn
            FROM {staging}
        ) t
        WHERE (rn - 1) % {BATCH_SIZE} = 0
        ORDER BY {pk}
    """)
    boundaries = [row[0] for row in cur.fetchall() if row[0] is not None]

    cur.execute(f"SELECT MAX({pk}) FROM {staging}")
    max_pk = int(cur.fetchone()[0])

    ranges = []
    for i, lo in enumerate(boundaries):
        hi = boundaries[i + 1] if i + 1 < len(boundaries) else max_pk + 1
        ranges.append((lo, hi))

    print(f"    {len(ranges)} batches of ~{BATCH_SIZE:,} rows each")
    return ranges


def setup_tables():
    """Setup all lookup + PK staging tables. Return per-source ranges dict."""
    conn = get_connection()
    cur  = conn.cursor()

    # ── 1. Pre-materialized lookup tables ────────────────────────────
    setup_lookup_tables(conn, cur)

    # ── 2. Destination table ─────────────────────────────────────────
    print("  Creating destination table...")
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {DEST_TABLE} (
            diag_id            BIGINT       DEFAULT NULL,
            ndid               BIGINT       DEFAULT NULL,
            eid                BIGINT       DEFAULT NULL,
            encounter_date     DATE         DEFAULT NULL,
            problem_date       DATE         DEFAULT NULL,
            problem_onset_date DATE         DEFAULT NULL,
            problem_end_date   DATE         DEFAULT NULL,
            resolved           BIGINT       DEFAULT NULL,
            problem_desc       TEXT,
            snomed_code        VARCHAR(50)  DEFAULT NULL,
            icd_code           VARCHAR(50)  DEFAULT NULL,
            problem_type       VARCHAR(100) DEFAULT NULL,
            status             VARCHAR(100) DEFAULT NULL,
            severity           VARCHAR(100) DEFAULT NULL,
            laterality         VARCHAR(100) DEFAULT NULL,
            problem_notes      TEXT,
            data_source        VARCHAR(50)  DEFAULT NULL,
            psid               INT          DEFAULT NULL,
            nd_extracted_date  DATE         DEFAULT NULL
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
    """)
    conn.commit()
    print("    ready")

    # ── 3. Checkpoint table ──────────────────────────────────────────
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

    # ── 4. Per-source PK staging + batch ranges ──────────────────────
    source_ranges = {}
    for src in SOURCES:
        ranges = setup_source_staging(conn, cur, src)
        source_ranges[src["table"]] = ranges

    cur.close()
    conn.close()
    return source_ranges


# ── Worker ───────────────────────────────────────────────────────────

def run_source(source, ranges, pbar):
    """Process one source table across all batch ranges."""
    table      = source["table"]
    source_key = table

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
            sql = build_batch_insert(source, pk_lo, pk_hi)
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
    print(f"  Greenway Problem List ETL — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  source     : {SOURCE_SCHEMA}  (psid={PSID})")
    print(f"  dest       : {DEST_TABLE}")
    print(f"  checkpoint : {CHECKPOINT_TABLE}")
    print(f"  workers    : {MAX_WORKERS}")
    print(f"  batch_size : {BATCH_SIZE:,}")
    print(f"{'='*70}\n", flush=True)

    print("  Connecting to database...")
    sys.stdout.flush()
    source_ranges = setup_tables()

    total_batches = sum(len(r) for r in source_ranges.values())
    if total_batches == 0:
        print(f"\nNo rows found for any source in schema '{SOURCE_SCHEMA}'. Exiting.")
        return

    results = []
    with tqdm(total=total_batches, desc="Overall", unit="batch") as pbar:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
            futures = {
                pool.submit(run_source, src, source_ranges[src["table"]], pbar): src
                for src in SOURCES
                if source_ranges[src["table"]]  # skip empty sources
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
    print(f"    DROP TABLE IF EXISTS {STAGING_PATHIST_CAT_ITEM};")
    print(f"    DROP TABLE IF EXISTS {STAGING_PATHIST_ITEM};")
    print(f"    DROP TABLE IF EXISTS {STAGING_STATUS};")
    for src in SOURCES:
        print(f"    DROP TABLE IF EXISTS {src['staging']};")
    print(f"    DROP TABLE IF EXISTS {CHECKPOINT_TABLE};")

    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
