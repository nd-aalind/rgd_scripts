#!/usr/bin/env python3
"""
Optimized ETL loader for: rgd_udm_silver.progressnotes_part3
Source: Greenway — clinicalbin_decrypted_extracteddata
        JOIN  ClinicalDocuments  ON Documentid
        LEFT JOIN Visit          ON VisitId

Columns inserted:
  ndid, eid, enc_date, notes, psid, nd_extracted_date, Documentid, BINTYPEID

Optimizations applied:
- Materialized staging table of Documentids (batching anchor from driving table)
- Batch by actual Documentid values (not arithmetic ranges — IDs can be sparse)
- Index on join keys for all three source tables
- Checkpoint/resume — re-run skips completed work
- Disabled InnoDB checks per-session for bulk insert speed
- Commit after every batch (frees undo/log space)
- Progress bar via tqdm

Usage:
    python opt_pgn_gw.py
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
    "host":            os.environ.get("DB_INTERNAL_HOST"),
    "port":            3306,
    "user":            os.environ.get("DB_INTERNAL_USER"),
    "password":        os.environ.get("DB_INTERNAL_PASSWORD"),
    "database":        "mind",
    "charset":         "utf8mb4",
    "connect_timeout": 30,
    "read_timeout":    21600,
    "write_timeout":   21600,
}

BATCH_SIZE  = 50_000
MAX_WORKERS = 1

# ── Change this one variable to run for a different schema ───────────
SOURCE_SCHEMA = "mind"   # e.g. "mind", "another_gw_schema"
PSID          = 12

DEST_TABLE       = "staging.progressnotes_part3"
STAGING_TABLE    = f"staging.tmp_pgn_gw_staging_fn_{SOURCE_SCHEMA}"
CHECKPOINT_TABLE = f"staging.etl_checkpoint_pgn_gw5_fn_{SOURCE_SCHEMA}"

SOURCE_TABLE = "clinicalbin_decrypted_extracteddata"
PK           = "Documentid"

# ── Source tables that need an index on their join key ───────────────
# WARNING: Creating indexes on large production tables takes time and
# acquires metadata locks. Set CREATE_INDEXES = False to skip if tables
# are already indexed or are actively under heavy load.
CREATE_INDEXES = False

SOURCE_TABLES_JOIN_KEY = {
    "clinicalbin_decrypted_extracteddata": "Documentid",
    "ClinicalDocuments":                   "Documentid",
    "Visit":                               "VisitId",
}


# ── Helpers ──────────────────────────────────────────────────────────

def get_connection():
    return pymysql.connect(**DB_CONFIG)


def build_batch_insert(doc_lo, doc_hi):
    """
    Faithfully reproduces the original JOIN logic in batched form.
    Batching is applied on clinicalbin_decrypted_extracteddata.Documentid.
    """
    return f"""
INSERT INTO {DEST_TABLE}
    (ndid, eid, enc_date, notes, psid, nd_extracted_date, Documentid, BINTYPEID)
SELECT
    a.Patientid,
    b.VisitId,
    DATE(b.FromDateTime),
    a.DOCCONTENT,
    {PSID},
    NULL,
    a.Documentid,
    a.BINTYPEID
FROM {SOURCE_SCHEMA}.clinicalbin_decrypted_extracteddata a
JOIN  {SOURCE_SCHEMA}.ClinicalDocuments cd ON a.Documentid = cd.Documentid
LEFT JOIN {SOURCE_SCHEMA}.Visit          b  ON cd.VisitId   = b.VisitId
WHERE a.{PK} >= {doc_lo}
  AND a.{PK} <  {doc_hi}
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
    conn = get_connection()
    cur  = conn.cursor()
    db_name = SOURCE_SCHEMA

    # ── 1. Staging table: materialize Documentids from the driving table ──
    print("  Creating staging table...")
    cur.execute(
        "SELECT COUNT(*) FROM information_schema.tables "
        "WHERE table_schema = %s AND table_name = %s",
        (STAGING_TABLE.split(".")[0], STAGING_TABLE.split(".")[1]),
    )
    if cur.fetchone()[0] == 0:
        cur.execute(f"""
            CREATE TABLE {STAGING_TABLE} AS
            SELECT DISTINCT {PK}
            FROM {SOURCE_SCHEMA}.{SOURCE_TABLE}
            WHERE {PK} IS NOT NULL
        """)
        cur.execute(f"ALTER TABLE {STAGING_TABLE} ADD INDEX idx_pk ({PK})")
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")

    cur.execute(f"SELECT COUNT(*) FROM {STAGING_TABLE}")
    print(f"    {cur.fetchone()[0]:,} distinct Documentids")

    # ── 2. Ensure source tables have indexes on their join keys ──────────
    if CREATE_INDEXES:
        print("  Checking/creating source table indexes...")
        cur.execute("SET lock_wait_timeout = 15")
        for table, key in SOURCE_TABLES_JOIN_KEY.items():
            cur.execute("""
                SELECT COUNT(*) FROM information_schema.statistics
                WHERE table_schema = %s AND table_name = %s AND column_name = %s
            """, (db_name, table, key))
            if cur.fetchone()[0] == 0:
                print(f"    Creating index on {table}.{key} ...")
                try:
                    cur.execute(f"CREATE INDEX idx_{key} ON {SOURCE_SCHEMA}.{table} ({key})")
                    conn.commit()
                    print(f"    Done")
                except Exception as idx_exc:
                    print(f"    WARNING: Could not create index on {table}.{key}: {idx_exc}")
                    print(f"    Table may be locked. Find the blocker:")
                    print(f"      SELECT id, user, state, info FROM information_schema.processlist")
                    print(f"      WHERE state LIKE '%lock%' OR state LIKE '%wait%' ORDER BY time DESC;")
                    print(f"    Then: KILL <id>;  — or set CREATE_INDEXES=False to skip and run anyway.")
                    conn.rollback()
            else:
                print(f"    {table}.{key} — index exists")
        cur.execute("SET lock_wait_timeout = DEFAULT")
    else:
        print("  Skipping index creation (CREATE_INDEXES=False)")

    # ── 3. Destination table ─────────────────────────────────────────
    print("  Creating destination table...")
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {DEST_TABLE} (
            ndid              BIGINT       DEFAULT NULL,
            eid               BIGINT       DEFAULT NULL,
            enc_date          DATE         DEFAULT NULL,
            notes             LONGTEXT,
            psid              INT          DEFAULT NULL,
            nd_extracted_date DATE         DEFAULT NULL,
            Documentid        BIGINT       DEFAULT NULL,
            BINTYPEID         VARCHAR(50)  DEFAULT NULL,
            KEY idx_psid       (psid),
            KEY idx_eid        (eid),
            KEY idx_ndid       (ndid),
            KEY idx_enc_date   (enc_date),
            KEY idx_documentid (Documentid)
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

    # ── 5. Compute batch ranges via server-side boundary sampling ────
    print("  Computing batch boundaries...")
    sys.stdout.flush()

    cur.execute(f"SELECT COUNT(*) FROM {STAGING_TABLE}")
    total = cur.fetchone()[0]

    if total == 0:
        cur.close()
        conn.close()
        return []

    cur.execute(f"""
        SELECT {PK}
        FROM (
            SELECT {PK},
                   ROW_NUMBER() OVER (ORDER BY {PK}) AS rn
            FROM {STAGING_TABLE}
        ) t
        WHERE (rn - 1) % {BATCH_SIZE} = 0
        ORDER BY {PK}
    """)
    boundaries = [row[0] for row in cur.fetchall() if row[0] is not None]

    cur.execute(f"SELECT MAX({PK}) FROM {STAGING_TABLE}")
    max_pk = int(cur.fetchone()[0])

    cur.close()
    conn.close()

    ranges = []
    for i, lo in enumerate(boundaries):
        hi = boundaries[i + 1] if i + 1 < len(boundaries) else max_pk + 1
        ranges.append((lo, hi))

    print(f"    {len(ranges)} batches of ~{BATCH_SIZE:,} Documentids each")
    return ranges


# ── Worker ───────────────────────────────────────────────────────────

def run_source(ranges, pbar):
    source_key = f"pgn_gw.{SOURCE_SCHEMA}"
    conn = get_connection()

    if is_done(conn, source_key):
        conn.close()
        pbar.update(len(ranges))
        return {"status": "skipped", "rows": 0, "secs": 0}

    mark(conn, source_key, "running")
    t0 = time.time()
    total_rows = 0

    try:
        cur = conn.cursor()
        cur.execute("SET unique_checks = 0")
        cur.execute("SET foreign_key_checks = 0")

        for doc_lo, doc_hi in ranges:
            sql = build_batch_insert(doc_lo, doc_hi)
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
        return {"status": "done", "rows": total_rows, "secs": elapsed}

    except Exception as exc:
        elapsed = round(time.time() - t0, 1)
        mark(conn, source_key, "failed", total_rows, str(exc))
        try:
            conn.close()
        except Exception:
            pass
        return {"status": f"FAILED: {exc}", "rows": total_rows, "secs": elapsed}


# ── Main ─────────────────────────────────────────────────────────────

def main():
    print(f"\n{'='*70}", flush=True)
    print(f"  Greenway Progress Notes ETL — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  source     : {SOURCE_SCHEMA}.{SOURCE_TABLE}")
    print(f"  dest       : {DEST_TABLE}")
    print(f"  staging    : {STAGING_TABLE}")
    print(f"  checkpoint : {CHECKPOINT_TABLE}")
    print(f"  psid       : {PSID}")
    print(f"  batch_size : {BATCH_SIZE:,}")
    print(f"{'='*70}\n", flush=True)

    print("  Connecting to database...")
    sys.stdout.flush()
    ranges = setup_tables()

    if not ranges:
        print(f"\nNo Documentids found in {SOURCE_SCHEMA}.{SOURCE_TABLE}. Exiting.")
        return

    any_failed = False
    with tqdm(total=len(ranges), desc="Overall", unit="batch") as pbar:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
            future = pool.submit(run_source, ranges, pbar)
            result = future.result()

    status = result["status"]
    rows   = result["rows"]
    secs   = result["secs"]

    print(f"\n{'='*70}")
    if status == "done":
        tag = " DONE"
    elif status == "skipped":
        tag = " SKIP"
    else:
        tag = " FAIL"
        any_failed = True

    print(f"  [{tag}] {SOURCE_SCHEMA}.{SOURCE_TABLE:<40}  {rows:>10,} rows  ({secs}s)")
    if status.startswith("FAILED"):
        print(f"         {status}")

    print(f"\n  Total rows inserted: {rows:,}")
    print(f"{'='*70}")

    print(f"\n  Cleanup SQL (run after verifying data):")
    print(f"    DROP TABLE IF EXISTS {STAGING_TABLE};")
    print(f"    DROP TABLE IF EXISTS {CHECKPOINT_TABLE};")

    if any_failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
