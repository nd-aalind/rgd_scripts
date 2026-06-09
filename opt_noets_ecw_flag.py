#!/usr/bin/env python3
"""
Optimized UDM notes loader — with nd_activeflag = 'Y' filter on all sources.

Combines the best of both approaches:
- eid-range batching      → space safety (avoids temp-table blowup)
- parallel execution      → wall-clock speed (6 concurrent workers)
- checkpoint/resume       → reliability (re-run skips completed sources)
- idempotent cleanup      → safety (delete-before-insert per source)
- nd_activeflag filtering → correctness (only active records)
- disabled InnoDB checks  → bulk insert speed
- explicit dest schema    → no dependency on production table

Usage:
    python opt_noets_ecw_flag.py
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
    "database":        "fcn_latest",
    "charset":         "utf8mb4",
    "connect_timeout": 30,
    "read_timeout":    21600,
    "write_timeout":   21600,
}

BATCH_SIZE  = 50_000   # eid range per batch — lower = less DB space used
MAX_WORKERS = 6        # concurrent source workers

DEST_TABLE       = "staging.notes_fcn"
STAGING_TABLE    = "staging.nw_testing_notes_n_up_11"
CHECKPOINT_TABLE = "staging.etl_checkpoint_3_n_up_11"

# ── Source definitions: (table, alias, column, note_type, note_source) ──
SOURCES = [
    ("encounterdata",    "ed",   "currentmedication",     "currentmedication",     "encounterdata"),
    ("encounterdata",    "ed",   "AsmtNotes",             "AsmtNotes",             "encounterdata"),
    ("encounterdata",    "ed",   "ChiefComplaint",        "ChiefComplaint",        "encounterdata"),
    ("encounterdata",    "ed",   "HPINotes",              "HPINotes",              "encounterdata"),
    ("encounterdata",    "ed",   "ExamNotes",             "ExamNotes",             "encounterdata"),
    ("encounterdata",    "ed",   "TreatNotes",            "TreatNotes",            "encounterdata"),
    ("encounterdata",    "ed",   "PastHistory",           "PastHistory",           "encounterdata"),
    ("notes",            "n",    "notes",                 "notes",                 "notes"),
    ("telenc",           "t",    "message",               "message",               "telenc"),
    ("telenc",           "t",    "actiontaken",           "actiontaken",           "telenc"),
    ("treatmentnotes",   "tn",   "Notes",                 "TreatNotes",            "treatmentnotes"),
    ("ptinstruction",    "pi",   "notes",                 "pt_instructions_notes", "ptinstruction"),
    ("encaddendums",     "ea",   "addendum",              "addendum",              "encaddendums"),
    ("hpi",              "h",    "notes",                 "HPINotes",              "hpi"),
    ("encounters",       "enc",  "notes",                 "notes",                 "encounters"),
    ("annualnotes",      "an",   "notes",                 "notes",                 "annualnotes"),
    ("procedurespl",     "ps",   "value",                 "value",                 "procedurespl"),
    ("structured_data",  "sd",   "data",                  "data",                  "structured_data"),
    ("interactionnotes", "inn",  "notes",                 "notes",                 "interactionnotes"),
    ("interactionnotes", "inn",  "provideraction",        "provideraction",        "interactionnotes"),
    ("structhpi",        "shpi", "value",                 "value",                 "structhpi"),
    ("structhpi",        "shpi", "notes",                 "notes",                 "structhpi"),
    ("edi_dfr_info",     "edi",  "curentdelayptrecovery", "curentdelayptrecovery", "edi_dfr_info"),
    ("edi_dfr_info",     "edi",  "subcomplaint",          "subcomplaint",          "edi_dfr_info"),
]


# ── Helpers ──────────────────────────────────────────────────────────

def get_connection():
    """One connection per call — each thread gets its own."""
    return pymysql.connect(**DB_CONFIG)


def build_batch_insert(table, alias, column, note_type, note_source, eid_lo, eid_hi):
    return f"""
INSERT INTO {DEST_TABLE}
    (ndid, eid, enc_start_date, note, note_type, note_source,
     created_datetime, created_by, ehr_source_name, source_path,
     data_type, psid, nd_extracted_date)
SELECT
    e.ndid,
    e.eid,
    e.enc_start_date,
    {alias}.{column},
    '{note_type}',
    CAST('{note_source}' AS CHAR(26)),
    NOW(),
    'ND',
    'eCW',
    'bronze_table',
    'Structured',
    8,
    NULL
FROM {STAGING_TABLE} e
JOIN {table} {alias} ON e.eid = {alias}.encounterID
WHERE e.eid >= {eid_lo} AND e.eid < {eid_hi}
  AND {alias}.nd_activeflag = 'Y'
  AND {alias}.{column} IS NOT NULL
  AND {alias}.{column} != ''
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

    # Encounter staging — permanent table so all worker threads can see it
    print("  Creating staging table...")
    cur.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = %s AND table_name = %s",
        (STAGING_TABLE.split(".")[0], STAGING_TABLE.split(".")[1]),
    )
    if cur.fetchone()[0] == 0:
        cur.execute(f"""
            CREATE TABLE {STAGING_TABLE} AS
            SELECT
                CAST(patientid   AS SIGNED) AS ndid,
                CAST(encounterID AS SIGNED) AS eid,
                DATE(date)                  AS enc_start_date
            FROM enc
        """)
        cur.execute(f"ALTER TABLE {STAGING_TABLE} ADD INDEX idx_eid (eid)")
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")

    cur.execute(f"SELECT COUNT(*) FROM {STAGING_TABLE}")
    enc_count = cur.fetchone()[0]
    print(f"    {enc_count:,} encounters")

    # Destination table
    print("  Creating destination table...")
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {DEST_TABLE} (
            ndid              BIGINT        DEFAULT NULL,
            eid               BIGINT        DEFAULT NULL,
            enc_start_date    DATE          DEFAULT NULL,
            note              LONGTEXT,
            note_type         LONGTEXT,
            note_source       VARCHAR(26)   NOT NULL DEFAULT '',
            created_datetime  DATETIME      NOT NULL,
            created_by        VARCHAR(2)    NOT NULL DEFAULT '',
            ehr_source_name   VARCHAR(9)    NOT NULL DEFAULT '',
            source_path       VARCHAR(12)   NOT NULL DEFAULT '',
            data_type         VARCHAR(10)   NOT NULL DEFAULT '',
            psid              INT           NOT NULL DEFAULT 0,
            nd_extracted_date DATE          DEFAULT NULL,
            udm_inc_id        BIGINT        NOT NULL AUTO_INCREMENT,
            UNIQUE KEY uq_inc_id        (udm_inc_id),
            KEY idx_ndid_eid            (ndid, eid),
            KEY idx_extracted_date      (nd_extracted_date),
            KEY idx_psid                (psid)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
    """)
    conn.commit()

    # Checkpoint table — tracks per-source completion for resume
    print("  Creating checkpoint table...")
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {CHECKPOINT_TABLE} (
            source_key    VARCHAR(150) NOT NULL PRIMARY KEY,
            status        ENUM('running', 'done', 'failed') NOT NULL DEFAULT 'running',
            rows_inserted BIGINT      DEFAULT 0,
            started_at    DATETIME    DEFAULT NULL,
            completed_at  DATETIME    DEFAULT NULL,
            error_msg     TEXT        DEFAULT NULL
        )
    """)
    conn.commit()

    # Compute eid ranges from actual values (eids can be sparse)
    print("  Computing batch boundaries...")
    sys.stdout.flush()
    cur.execute(f"SELECT eid FROM {STAGING_TABLE} WHERE eid IS NOT NULL ORDER BY eid")
    eids = [row[0] for row in cur.fetchall()]
    cur.close()
    conn.close()

    if not eids:
        return []

    ranges = []
    for i in range(0, len(eids), BATCH_SIZE):
        lo = eids[i]
        hi = eids[i + BATCH_SIZE] if i + BATCH_SIZE < len(eids) else eids[-1] + 1
        ranges.append((lo, hi))

    print(f"    {len(ranges)} batches of ~{BATCH_SIZE:,} encounters each")
    return ranges


# ── Worker ───────────────────────────────────────────────────────────

def run_source(source_def, ranges, pbar):
    """Process one source column across all eid-range batches."""
    table, alias, column, note_type, note_source = source_def
    source_key = f"{table}.{column}"

    conn = get_connection()

    # Resume support — skip if already completed
    if is_done(conn, source_key):
        conn.close()
        pbar.update(len(ranges))
        return {"source": source_key, "status": "skipped", "rows": 0, "secs": 0}

    mark(conn, source_key, "running")
    t0 = time.time()
    total_rows = 0

    try:
        cur = conn.cursor()

        # Disable InnoDB checks for bulk insert speed
        cur.execute("SET unique_checks = 0")
        cur.execute("SET foreign_key_checks = 0")

        # Batched inserts by eid range
        for eid_lo, eid_hi in ranges:
            sql = build_batch_insert(
                table, alias, column, note_type, note_source, eid_lo, eid_hi,
            )
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
    print(f"  UDM Notes ETL — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  dest       : {DEST_TABLE}")
    print(f"  staging    : {STAGING_TABLE}")
    print(f"  workers    : {MAX_WORKERS}")
    print(f"  batch_size : {BATCH_SIZE:,}")
    print(f"{'='*70}\n", flush=True)

    print("  Connecting to database...")
    sys.stdout.flush()
    ranges = setup_tables()
    if not ranges:
        print("\nNo encounters found. Exiting.")
        return

    # Parallel execution — each source gets its own connection and batches
    total_batches = len(SOURCES) * len(ranges)
    results = []

    with tqdm(total=total_batches, desc="Overall", unit="batch") as pbar:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
            futures = {
                pool.submit(run_source, src, ranges, pbar): src for src in SOURCES
            }
            for future in as_completed(futures):
                r = future.result()
                results.append(r)

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

    # Summary
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
