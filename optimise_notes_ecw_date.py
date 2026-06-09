#!/usr/bin/env python3
"""
Optimized UDM notes loader — date-filtered variant.

Same as optimise.py but restricts encounters to a specific date range
(enc.date BETWEEN DATE_LO AND DATE_HI) so only notes from that window
are loaded into the destination table.

Date range : 2026-02-16 → 2026-03-31
Source     : northwest.enc  (filtered by date)
Dest       : rgd_udm_silver.notes_part2

Usage:
    python optimise_notes_ecw_date.py
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
    "user":            "admin",
    "password":        "ClAx5UNkjnM8JgLG",
    "charset":         "utf8mb4",
    "connect_timeout": 30,
    "read_timeout":    21600,
    "write_timeout":   21600,
}

SOURCE_SCHEMA = "dent"   # ← change per run (e.g. "dent", "northwest", "kinsula_leq")

DATE_LO = "2026-02-16"
DATE_HI = "2026-03-31"

BATCH_SIZE  = 50_000
MAX_WORKERS = 6

DEST_TABLE       = "biogen_april.dent_notes_inc"
STAGING_TABLE    = "udm_staging.nw_notes_date_enc_n2"
CHECKPOINT_TABLE = "udm_staging.etl_checkpoint_nw_notes_date_n2"

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
    1,
    NULL
FROM {STAGING_TABLE} e
JOIN {SOURCE_SCHEMA}.{table} {alias} ON e.eid = {alias}.encounterID
WHERE e.eid >= {eid_lo} AND e.eid < {eid_hi}
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
    conn = get_connection()
    cur  = conn.cursor()

    # Staging table — encounters restricted to the date window
    print(f"  Creating staging table (date: {DATE_LO} → {DATE_HI})...")
    cur.execute(
        "SELECT COUNT(*) FROM information_schema.tables "
        "WHERE table_schema = %s AND table_name = %s",
        (STAGING_TABLE.split(".")[0], STAGING_TABLE.split(".")[1]),
    )
    if cur.fetchone()[0] == 0:
        cur.execute(f"""
            CREATE TABLE {STAGING_TABLE} AS
            SELECT
                CAST(patientid   AS SIGNED) AS ndid,
                CAST(encounterID AS SIGNED) AS eid,
                DATE(date)                  AS enc_start_date
            FROM {SOURCE_SCHEMA}.enc
            WHERE DATE(date) BETWEEN '{DATE_LO}' AND '{DATE_HI}'
        """)
        cur.execute(f"ALTER TABLE {STAGING_TABLE} ADD INDEX idx_eid (eid)")
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")

    cur.execute(f"SELECT COUNT(*) FROM {STAGING_TABLE}")
    enc_count = cur.fetchone()[0]
    print(f"    {enc_count:,} encounters in date window")

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

    # Checkpoint table
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

    # Batch boundaries from actual eid values (sparse-safe)
    print("  Computing batch boundaries...")
    sys.stdout.flush()
    cur.execute(f"""
        SELECT eid
        FROM (
            SELECT eid, ROW_NUMBER() OVER (ORDER BY eid) AS rn
            FROM {STAGING_TABLE}
        ) t
        WHERE (rn - 1) % {BATCH_SIZE} = 0
        ORDER BY eid
    """)
    boundaries = [row[0] for row in cur.fetchall()]

    cur.execute(f"SELECT MAX(eid) FROM {STAGING_TABLE}")
    max_eid = cur.fetchone()[0]

    cur.close()
    conn.close()

    if not boundaries:
        return []

    ranges = []
    for i, lo in enumerate(boundaries):
        hi = boundaries[i + 1] if i + 1 < len(boundaries) else int(max_eid) + 1
        ranges.append((lo, hi))

    print(f"    {len(ranges)} batches of ~{BATCH_SIZE:,} encounters each")
    return ranges


# ── Worker ───────────────────────────────────────────────────────────

def run_source(source_def, ranges, pbar):
    table, alias, column, note_type, note_source = source_def
    source_key = f"{table}.{column}"

    conn = get_connection()

    if is_done(conn, source_key):
        conn.close()
        pbar.update(len(ranges))
        return {"source": source_key, "status": "skipped", "rows": 0, "secs": 0}

    mark(conn, source_key, "running")
    t0         = time.time()
    total_rows = 0

    try:
        cur = conn.cursor()
        cur.execute("SET unique_checks = 0")
        cur.execute("SET foreign_key_checks = 0")

        for eid_lo, eid_hi in ranges:
            sql = build_batch_insert(table, alias, column, note_type, note_source, eid_lo, eid_hi)
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
    print(f"  UDM Notes ETL (date-filtered) — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  date range : {DATE_LO}  to  {DATE_HI}")
    print(f"  dest       : {DEST_TABLE}")
    print(f"  staging    : {STAGING_TABLE}")
    print(f"  checkpoint : {CHECKPOINT_TABLE}")
    print(f"  workers    : {MAX_WORKERS}  |  batch_size : {BATCH_SIZE:,}")
    print(f"{'='*70}\n", flush=True)

    ranges = setup_tables()
    if not ranges:
        print("\n  No encounters found in date window. Exiting.")
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
        tag = " DONE" if r["status"] == "done" \
              else " SKIP" if r["status"] == "skipped" \
              else " FAIL"
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

    print(f"\n  Cleanup SQL (run after verifying data):")
    print(f"    DROP TABLE IF EXISTS {STAGING_TABLE};")
    print(f"    DROP TABLE IF EXISTS {CHECKPOINT_TABLE};")

    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
