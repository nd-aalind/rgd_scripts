#!/usr/bin/env python3
"""
dedup_vitals_opt.py — Optimized dedup ETL for vitals, processed per psid

Original SQL:
    SET @row_num := 0;
    CREATE TABLE rgd_udm_silver.vitals_dedup AS
    SELECT a.*, CONCAT_WS(':',psid,ndid,eid,vital_id,vital_date,vital_time,vital_code,vital_name)
    FROM (
        SELECT DISTINCT ..., (@row_num := @row_num + 1) AS udm_inc_id
        FROM rgd_udm_staging.vitals_new ORDER BY created_datetime
    ) a;

Optimizations:
- 14 psid-scoped SELECT DISTINCT (~12M rows each) instead of one 167M-row scan
- DISTINCT rows pre-materialized per psid into staging.vitals_dedup_src_v1_ps{psid}
- Batching by actual ndid values (sparse ID safe via ROW_NUMBER keyset pagination)
- Parallel INSERT workers — up to MAX_WORKERS psids run concurrently
- Checkpoint/resume per psid — re-run skips completed psids
- Commit after every batch (frees InnoDB undo log)
- InnoDB checks disabled per-session for bulk speed
- udm_inc_id via AUTO_INCREMENT (replaces MySQL 5.x @row_num session variable)
- tqdm progress bar

Note on udm_inc_id ordering:
  udm_inc_id is AUTO_INCREMENT assigned in psid/ndid-batch order, not global
  created_datetime. If strict global ordering is required, run a post-insert
  UPDATE assigning ROW_NUMBER OVER (ORDER BY created_datetime).

Usage:
    python dedup_vitals_opt.py
"""

import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import pymysql
from tqdm import tqdm

# ── Configuration ─────────────────────────────────────────────────────
DB_CONFIG = {
    "host":            "ndai-dev-rds-instance.cwp60ymu4ko0.us-east-1.rds.amazonaws.com",
    "port":            3306,
    "user":            "Aalind",
    "password":        "A@L1nd@123",
    "database":        "rgd_udm_silver",
    "charset":         "utf8mb4",
    "connect_timeout": 30,
    "read_timeout":    21600,
    "write_timeout":   21600,
}

BATCH_SIZE  = 200_000
MAX_WORKERS = 3  # concurrent psids during INSERT phase; each uses its own DB connection

SOURCE_TABLE     = "rgd_udm_staging.vitals_new"
DEST_TABLE       = "rgd_udm_silver.vitals_dedup"
CHECKPOINT_TABLE = "staging.etl_checkpoint_dedup_vitals_v1"

BATCH_KEY = "ndid"

# Set to a list to restrict which psids to run, e.g. PSIDS_OVERRIDE = [1, 3, 5]
# Set to None to auto-discover all distinct psids from source table
PSIDS_OVERRIDE = None


# ── Per-psid staging table names ───────────────────────────────────────

def _staging_src(psid):
    return f"staging.vitals_dedup_src_v1_ps{psid}"

def _staging_pk(psid):
    return f"staging.tmp_vitals_dedup_pk_v1_ps{psid}"

def _checkpoint_key(psid):
    return f"dedup_vitals.vitals_new.psid_{psid}"


# ── Helpers ────────────────────────────────────────────────────────────

def get_connection():
    return pymysql.connect(**DB_CONFIG)


def _table_exists(cur, full_table_name):
    schema, table = full_table_name.split(".", 1)
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


# ── Checkpoint ─────────────────────────────────────────────────────────

def is_done(conn, ck_key):
    cur = conn.cursor()
    cur.execute(
        f"SELECT status FROM {CHECKPOINT_TABLE} WHERE source_key = %s",
        (ck_key,),
    )
    row = cur.fetchone()
    cur.close()
    return row is not None and row[0] == "done"


def mark(conn, ck_key, status, rows=0, error=None):
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
    """, (ck_key, status, rows, status, error))
    conn.commit()
    cur.close()


# ── Batch INSERT builder ───────────────────────────────────────────────

def build_batch_insert(psid, pk_lo, pk_hi):
    staging = _staging_src(psid)
    return f"""
INSERT INTO {DEST_TABLE} (
    vital_id, ndid, eid, enc_date, enc_last_date,
    vital_date, vital_time, vital_code, vital_coding_system,
    vital_name, vital_unit, vital_range, vital_result,
    created_datetime, created_by, updated_datetime, updated_by,
    ehr_source_name, source_path, data_type, psid,
    nd_extracted_date, udm_unq_id_raw
)
SELECT
    vital_id, ndid, eid, enc_date, enc_last_date,
    vital_date, vital_time, vital_code, vital_coding_system,
    vital_name, vital_unit, vital_range, vital_result,
    created_datetime, created_by, updated_datetime, updated_by,
    ehr_source_name, source_path, data_type, psid,
    nd_extracted_date,
    CONCAT_WS(':',
        COALESCE(psid,       ''),
        COALESCE(ndid,       ''),
        COALESCE(eid,        ''),
        COALESCE(vital_id,   ''),
        COALESCE(vital_date, ''),
        COALESCE(vital_time, ''),
        COALESCE(vital_code, ''),
        COALESCE(vital_name, '')
    ) AS udm_unq_id_raw
FROM {staging}
WHERE {BATCH_KEY} >= {pk_lo}
  AND {BATCH_KEY} <  {pk_hi}
ORDER BY created_datetime
"""


# ── Global setup (dest table, checkpoint, source indexes) ─────────────

def setup_global():
    conn = get_connection()
    cur  = conn.cursor()

    src_schema, src_table = SOURCE_TABLE.split(".", 1)

    # psid index — needed for per-psid SELECT DISTINCT to avoid full scans
    for col in ["psid", BATCH_KEY]:
        if not _index_exists(cur, src_schema, src_table, col):
            print(f"  Creating index idx_{col} on {SOURCE_TABLE}({col})...")
            cur.execute(f"CREATE INDEX idx_{col} ON {SOURCE_TABLE} ({col})")
            conn.commit()
            print("    done")
        else:
            print(f"  Index on {SOURCE_TABLE}({col}) exists")

    print(f"  Creating destination table {DEST_TABLE}...")
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {DEST_TABLE} (
            udm_inc_id          BIGINT       NOT NULL AUTO_INCREMENT PRIMARY KEY,
            vital_id            BIGINT       DEFAULT NULL,
            ndid                BIGINT       DEFAULT NULL,
            eid                 BIGINT       DEFAULT NULL,
            enc_date            DATE         DEFAULT NULL,
            enc_last_date       DATE         DEFAULT NULL,
            vital_date          DATE         DEFAULT NULL,
            vital_time          VARCHAR(8)   DEFAULT NULL,
            vital_code          TEXT,
            vital_coding_system VARCHAR(50)  DEFAULT NULL,
            vital_name          TEXT,
            vital_unit          TEXT,
            vital_range         TEXT,
            vital_result        TEXT,
            created_datetime    DATETIME     DEFAULT NULL,
            created_by          VARCHAR(10)  DEFAULT NULL,
            updated_datetime    DATETIME     DEFAULT NULL,
            updated_by          VARCHAR(10)  DEFAULT NULL,
            ehr_source_name     VARCHAR(50)  DEFAULT NULL,
            source_path         VARCHAR(50)  DEFAULT NULL,
            data_type           VARCHAR(20)  DEFAULT NULL,
            psid                INT          DEFAULT NULL,
            nd_extracted_date   DATE         DEFAULT NULL,
            udm_unq_id_raw      TEXT
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
    """)
    conn.commit()
    print("    ready")

    print("  Creating checkpoint table...")
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {CHECKPOINT_TABLE} (
            source_key    VARCHAR(150) NOT NULL PRIMARY KEY,
            status        ENUM('running','done','failed') NOT NULL DEFAULT 'running',
            rows_inserted BIGINT      DEFAULT 0,
            started_at    DATETIME    DEFAULT NULL,
            completed_at  DATETIME    DEFAULT NULL,
            error_msg     TEXT        DEFAULT NULL
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
    """)
    conn.commit()
    print("    ready")

    cur.close()
    conn.close()


# ── Discover psids ─────────────────────────────────────────────────────

def discover_psids():
    if PSIDS_OVERRIDE is not None:
        return sorted(PSIDS_OVERRIDE)
    conn = get_connection()
    cur  = conn.cursor()
    cur.execute(
        f"SELECT DISTINCT psid FROM {SOURCE_TABLE} WHERE psid IS NOT NULL ORDER BY psid"
    )
    psids = [row[0] for row in cur.fetchall()]
    cur.close()
    conn.close()
    return psids


# ── Per-psid setup (materialization + batch ranges) ────────────────────

def setup_psid(psid):
    staging_src = _staging_src(psid)
    staging_pk  = _staging_pk(psid)
    conn = get_connection()
    cur  = conn.cursor()

    if not _table_exists(cur, staging_src):
        print(f"  [psid={psid}] SELECT DISTINCT — may take several minutes...")
        cur.execute(f"""
            CREATE TABLE {staging_src} AS
            SELECT DISTINCT
                vital_id, ndid, eid, enc_date, enc_last_date,
                vital_date, vital_time, vital_code, vital_coding_system,
                vital_name, vital_unit, vital_range, vital_result,
                created_datetime, created_by, updated_datetime, updated_by,
                ehr_source_name, source_path, data_type, psid,
                nd_extracted_date
            FROM {SOURCE_TABLE}
            WHERE psid = {psid}
        """)
        cur.execute(f"ALTER TABLE {staging_src} ADD INDEX idx_ndid ({BATCH_KEY})")
        cur.execute(f"ALTER TABLE {staging_src} ADD INDEX idx_created (created_datetime)")
        conn.commit()
        print(f"  [psid={psid}] staging created")
    else:
        print(f"  [psid={psid}] staging already exists, reusing")

    cur.execute(f"SELECT COUNT(*) FROM {staging_src}")
    src_count = cur.fetchone()[0]
    print(f"  [psid={psid}] {src_count:,} distinct rows")

    if not _table_exists(cur, staging_pk):
        cur.execute(f"""
            CREATE TABLE {staging_pk} AS
            SELECT {BATCH_KEY}
            FROM {staging_src}
            WHERE {BATCH_KEY} IS NOT NULL
            ORDER BY {BATCH_KEY}
        """)
        cur.execute(f"ALTER TABLE {staging_pk} ADD INDEX idx_pk ({BATCH_KEY})")
        conn.commit()

    cur.execute(f"SELECT COUNT(*) FROM {staging_pk}")
    count = cur.fetchone()[0]

    if count == 0:
        cur.close()
        conn.close()
        return []

    cur.execute(f"""
        SELECT {BATCH_KEY}
        FROM (
            SELECT {BATCH_KEY},
                   ROW_NUMBER() OVER (ORDER BY {BATCH_KEY}) AS rn
            FROM {staging_pk}
        ) t
        WHERE (rn - 1) % {BATCH_SIZE} = 0
        ORDER BY {BATCH_KEY}
    """)
    boundaries = [row[0] for row in cur.fetchall() if row[0] is not None]

    cur.execute(f"SELECT MAX({BATCH_KEY}) FROM {staging_pk}")
    max_pk = int(cur.fetchone()[0])

    ranges = []
    for i, lo in enumerate(boundaries):
        hi = boundaries[i + 1] if i + 1 < len(boundaries) else max_pk + 1
        ranges.append((lo, hi))

    print(f"  [psid={psid}] {count:,} rows → {len(ranges)} batches of {BATCH_SIZE:,}")

    cur.close()
    conn.close()
    return ranges


# ── Worker ─────────────────────────────────────────────────────────────

def run_psid(psid, ranges, pbar):
    ck_key = _checkpoint_key(psid)
    conn   = get_connection()

    if is_done(conn, ck_key):
        conn.close()
        pbar.update(len(ranges))
        return {"psid": psid, "status": "skipped", "rows": 0, "secs": 0}

    mark(conn, ck_key, "running")
    t0         = time.time()
    total_rows = 0

    try:
        cur = conn.cursor()
        cur.execute("SET SESSION innodb_lock_wait_timeout = 3600")
        cur.execute("SET unique_checks = 0")
        cur.execute("SET foreign_key_checks = 0")

        for pk_lo, pk_hi in ranges:
            cur.execute(build_batch_insert(psid, pk_lo, pk_hi))
            conn.commit()
            total_rows += cur.rowcount
            pbar.update(1)

        cur.execute("SET unique_checks = 1")
        cur.execute("SET foreign_key_checks = 1")
        cur.close()

        elapsed = round(time.time() - t0, 1)
        mark(conn, ck_key, "done", total_rows)
        conn.close()
        return {"psid": psid, "status": "done", "rows": total_rows, "secs": elapsed}

    except Exception as exc:
        elapsed = round(time.time() - t0, 1)
        mark(conn, ck_key, "failed", total_rows, str(exc))
        try:
            conn.close()
        except Exception:
            pass
        return {"psid": psid, "status": f"FAILED: {exc}", "rows": total_rows, "secs": elapsed}


# ── Main ───────────────────────────────────────────────────────────────

def main():
    print(f"\n{'='*70}", flush=True)
    print(f"  Vitals Dedup ETL (per-psid) — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  source     : {SOURCE_TABLE}")
    print(f"  dest       : {DEST_TABLE}")
    print(f"  checkpoint : {CHECKPOINT_TABLE}")
    print(f"  batch_size : {BATCH_SIZE:,}  |  workers : {MAX_WORKERS}")
    print(f"{'='*70}\n", flush=True)

    setup_global()

    print("\n  Discovering psids...")
    psids = discover_psids()
    print(f"  Found {len(psids)} psids: {psids}\n")

    print("  Setting up per-psid staging tables (sequential)...")
    psid_ranges = {}
    for psid in psids:
        psid_ranges[psid] = setup_psid(psid)

    total_batches = sum(len(r) for r in psid_ranges.values())
    if total_batches == 0:
        print("No rows to process. Exiting.")
        return

    print(f"\n  Starting dedup INSERT ({total_batches} total batches, {MAX_WORKERS} workers)...", flush=True)

    results = []
    with tqdm(total=total_batches, desc="Dedup INSERT", unit="batch") as pbar:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
            futures = {
                pool.submit(run_psid, psid, psid_ranges[psid], pbar): psid
                for psid in psids
                if psid_ranges.get(psid)
            }
            for future in as_completed(futures):
                results.append(future.result())

    print()
    for r in sorted(results, key=lambda x: x["psid"]):
        tag = "DONE" if r["status"] == "done" \
              else "SKIP" if r["status"] == "skipped" \
              else "FAIL"
        print(f"  [{tag}] psid={r['psid']:<6}  {r['rows']:>12,} rows  ({r['secs']}s)")

    done    = sum(1 for r in results if r["status"] == "done")
    skipped = sum(1 for r in results if r["status"] == "skipped")
    failed  = [r for r in results if "FAILED" in str(r["status"])]
    total   = sum(r["rows"] for r in results)

    print(f"\n{'='*70}")
    print(f"  Done: {done}  Skipped: {skipped}  Failed: {len(failed)}  |  Total rows: {total:,}")
    print(f"{'='*70}")

    print(f"\n  Cleanup SQL (run after verifying data):")
    for psid in psids:
        print(f"    DROP TABLE IF EXISTS {_staging_src(psid)};")
        print(f"    DROP TABLE IF EXISTS {_staging_pk(psid)};")
    print(f"    DROP TABLE IF EXISTS {CHECKPOINT_TABLE};")

    if failed:
        print("\n  Failed psids:")
        for r in failed:
            print(f"    psid={r['psid']}: {r['status']}")
        sys.exit(1)


if __name__ == "__main__":
    main()
