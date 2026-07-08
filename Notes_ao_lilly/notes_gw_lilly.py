#!/usr/bin/env python3
"""
Optimized ETL for: udm_staging.notes_lilly_final_gw — Greenway clinical notes (Lilly cohort)

Sources (7 independent INSERT jobs, all from CLINICALBIN_CT_RWE_2_extracteddata × Visit):
  1. Chief_Complaint        — note_type: ChiefComplaint         (psid=12)
  2. ROS                    — note_type: ROS                    (psid=12)
  3. HPI                    — note_type: HPI Notes              (psid=12)
  4. Social_History         — note_type: Social_History         (psid=12)
  5. Instructions           — note_type: Instructions           (psid=12)
  6. Family_Medical_History — note_type: Family_Medical_History (psid=12)
  7. Assessment             — note_type: Assessment             (psid=9)

Optimizations:
- All 7 note columns pre-materialized once from SOURCE_TABLE × Visit × patientlist_lilly_all
  (source tables never re-scanned per batch — all reads go against staging)
- Batching by actual VisitId values (sparse ID safe)
- ThreadPoolExecutor with 7 workers — one per note column
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
import os
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())

# ── Configuration ──────────────────────────────────────────────────────────────
DB_CONFIG = {
    "host":            os.environ.get("DB_HOST"),
    "port":            3306,
    "user":            os.environ.get("DB_USER"),
    "password":        os.environ.get("DB_PASSWORD"),
    "database":        "udm_staging",
    "charset":         "utf8mb4",
    "connect_timeout": 30,
    "read_timeout":    21600,
    "write_timeout":   21600,
}

BATCH_SIZE  = 50_000
MAX_WORKERS = 7

# ── Change these to run for a different schema / base table ───────────────────
SOURCE_SCHEMA = "jwm"
SOURCE_TABLE  = "CLINICALBIN_CT_RWE_2_extracteddata"   # base clinical note table

DEST_TABLE       = "udm_staging.notes_lilly_final_gw"
STAGING_TABLE    = f"staging.tmp_notes_gw_lilly_{SOURCE_SCHEMA}"
PK_STAGING_TABLE = f"staging.tmp_notes_gw_lilly_pk_{SOURCE_SCHEMA}"
CHECKPOINT_TABLE = f"staging.etl_checkpoint_notes_gw_lilly_{SOURCE_SCHEMA}"

BATCH_KEY = "VisitId"

# One entry per UNION ALL branch.
# psid=9 for Assessment only; psid=12 for all others (preserved from original SQL).
SOURCES = [
    {"key": f"notes_gw.cc.{SOURCE_SCHEMA}",  "label": "Chief_Complaint",
     "note_col": "Chief_Complaint",        "note_type": "ChiefComplaint",         "psid": 12},
    {"key": f"notes_gw.ros.{SOURCE_SCHEMA}", "label": "ROS",
     "note_col": "ROS",                    "note_type": "ROS",                    "psid": 12},
    {"key": f"notes_gw.hpi.{SOURCE_SCHEMA}", "label": "HPI",
     "note_col": "HPI",                    "note_type": "HPI Notes",              "psid": 12},
    {"key": f"notes_gw.sh.{SOURCE_SCHEMA}",  "label": "Social_History",
     "note_col": "Social_History",         "note_type": "Social_History",         "psid": 12},
    {"key": f"notes_gw.ins.{SOURCE_SCHEMA}", "label": "Instructions",
     "note_col": "Instructions",           "note_type": "Instructions",           "psid": 12},
    {"key": f"notes_gw.fmh.{SOURCE_SCHEMA}", "label": "Family_Medical_History",
     "note_col": "Family_Medical_History", "note_type": "Family_Medical_History", "psid": 12},
    {"key": f"notes_gw.ass.{SOURCE_SCHEMA}", "label": "Assessment",
     "note_col": "Assessment",             "note_type": "Assessment",             "psid": 9},
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
def build_batch_insert(source, pk_lo, pk_hi):
    """
    All 7 sources read exclusively from the pre-materialized staging table.
    Each filters on its own note column IS NOT NULL within the batch VisitId range.
    """
    s         = "s"
    note_col  = source["note_col"]
    note_type = source["note_type"]
    psid      = source["psid"]

    return f"""
INSERT INTO {DEST_TABLE}
    (ndid, eid, enc_start_date, note, note_type, note_source,
     created_datetime, created_by, ehr_source_name, source_path, data_type,
     psid, nd_extracted_date)
SELECT DISTINCT
    {s}.Patientid,
    {s}.{BATCH_KEY},
    DATE({s}.FromDateTime),
    {s}.{note_col},
    '{note_type}',
    'Clinicalbin',
    CURRENT_DATE(),
    'ND',
    'Greenway',
    'bronze_table',
    'Structured',
    {psid},
    DATE({s}.nd_extracted_date)
FROM {STAGING_TABLE} {s}
WHERE {s}.{note_col} IS NOT NULL
  AND {s}.{BATCH_KEY} >= {pk_lo} AND {s}.{BATCH_KEY} < {pk_hi}
"""


# ── Setup ──────────────────────────────────────────────────────────────────────
def setup_tables():
    """
    Create staging, PK-range, destination, and checkpoint tables.
    Returns list of (pk_lo, pk_hi) batch ranges shared across all 7 sources.
    """
    conn = get_connection()
    cur  = conn.cursor()

    # 1. Indexes needed before staging table creation
    print("  Ensuring pre-staging indexes...")
    pre_indexes = [
        ("udm_staging", "patientlist_lilly_all", "ndid",      "idx_pll_ndid"),
        (SOURCE_SCHEMA, SOURCE_TABLE,             "Patientid", "idx_cb_patientid"),
        (SOURCE_SCHEMA, SOURCE_TABLE,             "VisitId",   "idx_cb_visitid"),
        (SOURCE_SCHEMA, "Visit",                  "VisitId",   "idx_vis_visitid"),
    ]
    for schema, tbl, col, idx_name in pre_indexes:
        if not _index_exists(cur, schema, tbl, col):
            print(f"    creating {schema}.{tbl}({col})...")
            cur.execute(f"CREATE INDEX {idx_name} ON {schema}.{tbl} ({col})")
            conn.commit()
    print("    done")

    # 2. Materialize all 7 note columns at once — batch inserts never touch
    #    the source tables again; every read comes from this staging table.
    print("  Creating note staging table...")
    if not _table_exists(cur, STAGING_TABLE):
        cur.execute(f"""
            CREATE TABLE {STAGING_TABLE} AS
            SELECT DISTINCT
                a.Patientid,
                b.VisitId,
                b.FromDateTime,
                b.nd_extracted_date,
                a.Chief_Complaint,
                a.ROS,
                a.HPI,
                a.Social_History,
                a.Instructions,
                a.Family_Medical_History,
                a.Assessment
            FROM {SOURCE_SCHEMA}.{SOURCE_TABLE} a
            JOIN {SOURCE_SCHEMA}.Visit b
                ON a.VisitId = b.VisitId
            INNER JOIN udm_staging.patientlist_lilly_all d
                ON d.ndid = a.Patientid
        """)
        cur.execute(
            f"ALTER TABLE {STAGING_TABLE} "
            f"ADD INDEX idx_visitid   ({BATCH_KEY}), "
            f"ADD INDEX idx_patientid (Patientid)"
        )
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")

    cur.execute(f"SELECT COUNT(*) FROM {STAGING_TABLE}")
    total = cur.fetchone()[0]
    print(f"    {total:,} eligible rows")

    # 3. Destination table
    print("  Creating destination table...")
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {DEST_TABLE} (
            ndid              BIGINT        DEFAULT NULL,
            eid               BIGINT        DEFAULT NULL,
            enc_start_date    DATE          DEFAULT NULL,
            note              LONGTEXT,
            note_type         VARCHAR(100)  DEFAULT NULL,
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

    # 4. Checkpoint table
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

    # 5. PK staging + batch boundaries (shared across all 7 sources)
    print("  Computing batch boundaries...")
    if total == 0:
        cur.close()
        conn.close()
        return []

    if not _table_exists(cur, PK_STAGING_TABLE):
        cur.execute(f"""
            CREATE TABLE {PK_STAGING_TABLE} AS
            SELECT DISTINCT {BATCH_KEY}
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
            sql = build_batch_insert(source, pk_lo, pk_hi)
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
    print(f"  Greenway Notes (Lilly) ETL — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  source     : {SOURCE_SCHEMA}.{SOURCE_TABLE} × Visit")
    print(f"  dest       : {DEST_TABLE}")
    print(f"  staging    : {STAGING_TABLE}")
    print(f"  checkpoint : {CHECKPOINT_TABLE}")
    print(f"  workers    : {MAX_WORKERS}  |  batch_size : {BATCH_SIZE:,}")
    print(f"{'='*70}\n", flush=True)

    ranges = setup_tables()

    if not ranges:
        print(f"\nNo eligible rows in {SOURCE_SCHEMA}.{SOURCE_TABLE}. Exiting.")
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
        print(f"  [{tag}] {r['source']:<30} {r['rows']:>10,} rows  ({r['secs']}s)")

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
