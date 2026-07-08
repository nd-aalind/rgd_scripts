#!/usr/bin/env python3
"""
Optimized ETL loader for: udm_staging.problemlist (eCW 2-table source)

Source: {SOURCE_SCHEMA}.problemlist  (single table)
  Batch  : SlNo (primary key)

Pre-materialized lookup tables (computed ONCE, reused across all batches):
  - staging.pl_ecw2_enc_{SOURCE_SCHEMA}   (enc table)

Note: icd_code is NULL in this variant (no icd_synonyms join).

Optimizations applied:
- Lookup tables pre-materialized once (not re-scanned per batch)
- PK staging table pre-filters eligible rows
- Batch by actual primary key values (not arithmetic ranges — IDs can be sparse)
- Server-side boundary sampling (avoids loading millions of PKs into memory)
- Commit after every batch (frees undo/log space)
- Checkpoint/resume — re-run skips if already completed
- Disabled InnoDB checks per-session for bulk insert speed
- Progress bar via tqdm

Usage:
    python opt_problem_list_ecw_2_tables.py
"""

import sys
import time
from datetime import datetime
import pymysql
from tqdm import tqdm
import os
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())

# ── Configuration ────────────────────────────────────────────────────
DB_CONFIG = {
    "host":            os.environ.get("DB_INTERNAL_HOST"),
    "port":            3306,
    "user":            os.environ.get("DB_INTERNAL_USER"),
    "password":        os.environ.get("DB_INTERNAL_PASSWORD"),
    "database":        "fcn_latest",
    "charset":         "utf8mb4",
    "connect_timeout": 30,
    "read_timeout":    21600,
    "write_timeout":   21600,
}

BATCH_SIZE = 50_000

# ── Change these two variables to run for a different schema/psid ────
SOURCE_SCHEMA = "arizona"   # e.g. "ecw", "ecw_raleigh", ...
PSID          = 14

DEST_TABLE       = "udm_staging.problemlist_fn"
STAGING_TABLE    = f"staging.tmp_pl_ecw2_staging__v4_{SOURCE_SCHEMA}"
CHECKPOINT_TABLE = f"staging.etl_checkpoint_pl_ecw2__v5_{SOURCE_SCHEMA}"
CHECKPOINT_KEY   = f"problemlist3.insert.{SOURCE_SCHEMA}"

BATCH_KEY = "SlNo"

# ── Pre-materialized lookup staging tables ───────────────────────────
STAGING_ENC   = f"staging.pl_ecw2_enc_n4_{SOURCE_SCHEMA}"
STAGING_ITEMS = f"staging.pl_ecw2_items_n4_{SOURCE_SCHEMA}"


# ── Batch INSERT builder ──────────────────────────────────────────────

def build_batch_insert(pk_lo, pk_hi):
    p   = "p"
    enc = "enc"
    it  = "it"
    return f"""
INSERT INTO {DEST_TABLE}
    (diag_id, ndid, eid, encounter_date, problem_date, problem_onset_date,
     problem_end_date, resolved, problem_desc, snomed_code, icd_code,
     problem_type, status, severity, laterality, problem_notes,
     data_source, psid, nd_extracted_date)
SELECT
    CAST({p}.SlNo AS SIGNED),
    CAST({p}.patientID AS SIGNED),
    CAST({p}.encounterID AS SIGNED),
    DATE({enc}.date),
    DATE({p}.AddedDate),
    DATE({p}.onsetdate),
    DATE({p}.resolvedon),
    CAST({p}.Resolved AS SIGNED),
    CASE WHEN {p}.SNOMEDDesc NOT IN ('', NULL) THEN {p}.SNOMEDDesc ELSE {it}.itemName END,
    CAST({p}.SNOMED AS CHAR(50)),
    NULL,
    CASE WHEN {p}.problemtype NOT IN ('', NULL) THEN {p}.problemtype
         WHEN {p}.SNOMEDDesc NOT IN ('', NULL) THEN 'SNOMED'
         ELSE {it}.itemName END,
    CAST({p}.WUStatus AS CHAR(100)),
    CAST({p}.Risk AS CHAR(100)),
    NULL,
    CAST({p}.notes AS CHAR),
    'eCW',
    {PSID},
    NULL
FROM {SOURCE_SCHEMA}.problemlist {p}
LEFT JOIN {STAGING_ENC}   {enc} ON {enc}.encounterid = CAST({p}.encounterID AS CHAR)
LEFT JOIN {STAGING_ITEMS} {it}  ON {it}.itemid       = {p}.asmtid
WHERE {p}.{BATCH_KEY} >= {pk_lo} AND {p}.{BATCH_KEY} < {pk_hi}
"""


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


# ── Setup ────────────────────────────────────────────────────────────

def setup_tables():
    """Create lookup + PK staging + dest + checkpoint tables. Return batch ranges."""
    conn = get_connection()
    cur  = conn.cursor()

    # ── 1a. Pre-materialized enc lookup (active only) ────────────────
    print("  Materializing enc lookup (nd_ActiveFlag='Y')...")
    if not _table_exists(cur, STAGING_ENC):
        cur.execute(f"""
            CREATE TABLE {STAGING_ENC} AS
            SELECT * FROM {SOURCE_SCHEMA}.enc
        """)
        cur.execute(f"ALTER TABLE {STAGING_ENC} ADD INDEX idx_enc (encounterid)")
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")
    cur.execute(f"SELECT COUNT(*) FROM {STAGING_ENC}")
    print(f"    {cur.fetchone()[0]:,} enc rows")

    # ── 1b. Pre-materialized items lookup ────────────────────────────
    print("  Materializing items lookup...")
    if not _table_exists(cur, STAGING_ITEMS):
        cur.execute(f"""
            CREATE TABLE {STAGING_ITEMS} AS
            SELECT * FROM {SOURCE_SCHEMA}.items
        """)
        cur.execute(f"ALTER TABLE {STAGING_ITEMS} ADD INDEX idx_items (itemid)")
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")
    cur.execute(f"SELECT COUNT(*) FROM {STAGING_ITEMS}")
    print(f"    {cur.fetchone()[0]:,} items rows")

    # ── 2. PK staging table ──────────────────────────────────────────
    print("  Creating PK staging table...")
    if not _table_exists(cur, STAGING_TABLE):
        cur.execute(f"""
            CREATE TABLE {STAGING_TABLE} AS
            SELECT CAST({BATCH_KEY} AS SIGNED) AS {BATCH_KEY}
            FROM {SOURCE_SCHEMA}.problemlist
            WHERE {BATCH_KEY} IS NOT NULL
        """)
        cur.execute(f"ALTER TABLE {STAGING_TABLE} ADD INDEX idx_pk ({BATCH_KEY})")
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")

    cur.execute(f"SELECT COUNT(*) FROM {STAGING_TABLE}")
    total = cur.fetchone()[0]
    print(f"    {total:,} rows to insert")

    # ── 3. Destination table ─────────────────────────────────────────
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
            problem_type       TEXT,
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

    # ── 5. Batch boundary sampling ───────────────────────────────────
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
    max_pk = int(cur.fetchone()[0])

    cur.close()
    conn.close()

    ranges = []
    for i, lo in enumerate(boundaries):
        hi = boundaries[i + 1] if i + 1 < len(boundaries) else max_pk + 1
        ranges.append((lo, hi))

    print(f"    {len(ranges)} batches of ~{BATCH_SIZE:,} rows each")
    return ranges


# ── Runner ───────────────────────────────────────────────────────────

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


# ── Main ─────────────────────────────────────────────────────────────

def main():
    print(f"\n{'='*70}", flush=True)
    print(f"  eCW Problem List (2-table) ETL — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  source     : {SOURCE_SCHEMA}.problemlist  (psid={PSID})")
    print(f"  dest       : {DEST_TABLE}")
    print(f"  staging    : {STAGING_TABLE}")
    print(f"  checkpoint : {CHECKPOINT_TABLE}")
    print(f"  batch_size : {BATCH_SIZE:,}")
    print(f"{'='*70}\n", flush=True)

    print("  Connecting to database...")
    sys.stdout.flush()
    ranges = setup_tables()

    if not ranges:
        print(f"\nNo eligible rows in {SOURCE_SCHEMA}.problemlist. Exiting.")
        return

    result = None
    with tqdm(total=len(ranges), desc="Overall", unit="batch") as pbar:
        result = run_insert(ranges, pbar)

    print()
    if result["status"] == "done":
        tag = " DONE"
    elif result["status"] == "skipped":
        tag = " SKIP"
    else:
        tag = " FAIL"
    print(f"  [{tag}] {SOURCE_SCHEMA}.problemlist  "
          f"{result['rows']:>10,} rows inserted  ({result['secs']}s)")

    print(f"\n{'='*70}")
    if result["status"].startswith("FAILED"):
        print(f"  FAILED: {result['status']}")
    else:
        print(f"  Status: {result['status']}  |  Total rows inserted: {result['rows']:,}")
    print(f"{'='*70}")

    print(f"\n  Cleanup SQL:")
    print(f"    DROP TABLE IF EXISTS {STAGING_ENC};")
    print(f"    DROP TABLE IF EXISTS {STAGING_ITEMS};")
    print(f"    DROP TABLE IF EXISTS {STAGING_TABLE};")
    print(f"    DROP TABLE IF EXISTS {CHECKPOINT_TABLE};")

    if result["status"].startswith("FAILED"):
        sys.exit(1)


if __name__ == "__main__":
    main()
