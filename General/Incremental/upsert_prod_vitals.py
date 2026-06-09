#!/usr/bin/env python3
"""
upsert_prod_vitals.py — Batched INSERT into rgd_udm_staging.vitals from udm_staging.vitals

SQL equivalent:
    INSERT INTO rgd_udm_staging.vitals (... udm_active_flag)
    SELECT a.*, 'Y' AS udm_active_flag
    FROM udm_staging.vitals a
    INNER JOIN (
        SELECT udm_unq_id, nd_extracted_date,
               ROW_NUMBER() OVER (PARTITION BY udm_unq_id ORDER BY nd_extracted_date DESC) AS rn
        FROM udm_staging.vitals
        WHERE psid = {PSID}
    ) ranked
        ON  a.udm_unq_id        = ranked.udm_unq_id
        AND a.nd_extracted_date = ranked.nd_extracted_date
        AND ranked.rn = 1;

The ROW_NUMBER() deduplication subquery is pre-materialized ONCE into a staging table
(the latest nd_extracted_date per udm_unq_id for the given psid). Batches then
JOIN directly against this pre-computed result — no window function per batch.

Change SOURCE_TABLE, DEST_TABLE, and PSID at the top to run for any table/psid.

Pre-materialized staging:
  staging.upsert_prod_vit_ranked_{PSID}  — (udm_unq_id, nd_extracted_date) for rn=1 rows

Single pass with checkpoint/resume:
  - Eligible source PKs (joined against ranked staging) loaded once into PK staging table
  - Batches by ndid using actual key values (sparse-ID safe)
  - Commits after every batch (frees undo/log space)
  - Re-running skips already-completed work via checkpoint

Usage:
    python upsert_prod_vitals.py
"""

import sys
import time
from datetime import datetime
import pymysql
from tqdm import tqdm

# ── Configuration ─────────────────────────────────────────────────────
DB_CONFIG = {
    "host":            "172.16.2.42",
    "port":            3306,
    "user":            "nd-root-mysql",
    "password":        "kmsamd89undsd4",
    "database":        "udm_staging",
    "charset":         "utf8mb4",
    "connect_timeout": 30,
    "read_timeout":    21600,
    "write_timeout":   21600,
}

BATCH_SIZE = 50_000
BATCH_KEY  = "ndid"

# ── Change these to run against a different table / psid ──────────────
SOURCE_TABLE = "udm_staging.vitals"        # staging/delta table
DEST_TABLE   = "rgd_udm_staging.vitals"    # production table to insert into
PSID         = 11

# ─────────────────────────────────────────────────────────────────────
_SUFFIX = f"psid{PSID}"

STAGING_RANKED   = f"staging.upsert_prod_vit_ranked_n_{_SUFFIX}"   # ROW_NUMBER dedup result
STAGING_PK       = f"staging.upsert_prod_vit_pk_n_{_SUFFIX}"       # eligible source udm_inc_ids
CHECKPOINT_TABLE = f"staging.etl_checkpoint_upsert_prod_vit_n_{_SUFFIX}"
CHECKPOINT_KEY   = f"upsert_prod_vitals.{_SUFFIX}"


# ── Helpers ───────────────────────────────────────────────────────────

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


def _build_ranges(cur, staging_pk):
    cur.execute(f"SELECT COUNT(*) FROM {staging_pk}")
    total = cur.fetchone()[0]
    if total == 0:
        return [], 0

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

    return ranges, total


# ── Batch INSERT builder ──────────────────────────────────────────────

def build_batch_insert(pk_lo, pk_hi):
    """
    Inserts the latest staging row per udm_unq_id into the production table.
    Joins through pre-materialized STAGING_RANKED — no window function at runtime.
    """
    return f"""
INSERT INTO {DEST_TABLE}
    (vital_id, ndid, eid, vital_code, vital_name,
     vital_coding_system, vital_date, vital_time, vital_unit, vital_range,
     vital_result, created_datetime, created_by, updated_datetime, updated_by,
     ehr_source_name, source_path, data_type, psid, nd_extracted_date,
     enc_date_proxy, udm_unq_id, udm_active_flag)
SELECT
    a.vital_id,
    a.ndid,
    a.eid,
    a.vital_code,
    a.vital_name,
    a.vital_coding_system,
    a.vital_date,
    a.vital_time,
    a.vital_unit,
    a.vital_range,
    a.vital_result,
    a.created_datetime,
    a.created_by,
    a.updated_datetime,
    a.updated_by,
    a.ehr_source_name,
    a.source_path,
    a.data_type,
    a.psid,
    a.nd_extracted_date,
    a.enc_date_proxy,
    a.udm_unq_id,
    'Y' AS udm_active_flag
FROM {SOURCE_TABLE} a
INNER JOIN {STAGING_RANKED} r
    ON  a.udm_unq_id        = r.udm_unq_id
    AND a.nd_extracted_date = r.nd_extracted_date
WHERE a.{BATCH_KEY} >= {pk_lo}
  AND a.{BATCH_KEY} <  {pk_hi}
"""


# ── Checkpoint ────────────────────────────────────────────────────────

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


# ── Setup ─────────────────────────────────────────────────────────────

def setup_tables():
    conn = get_connection()
    cur  = conn.cursor()

    try:
        # ── 1. Checkpoint table ──────────────────────────────────────
        print("  Creating checkpoint table...")
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {CHECKPOINT_TABLE} (
                source_key    VARCHAR(200) NOT NULL PRIMARY KEY,
                status        ENUM('running','done','failed') NOT NULL DEFAULT 'running',
                rows_inserted BIGINT      DEFAULT 0,
                started_at    DATETIME    DEFAULT NULL,
                completed_at  DATETIME    DEFAULT NULL,
                error_msg     TEXT        DEFAULT NULL
            )
        """)
        conn.commit()
        print("    ready")

        # ── 2. Ranked dedup staging (ROW_NUMBER materialized once) ────
        # Computes the single latest nd_extracted_date per udm_unq_id for psid=PSID.
        # This replaces the inline window function subquery — computed once,
        # joined against in every batch INSERT.
        print(f"  Materializing ranked dedup staging (psid={PSID})...")
        if not _table_exists(cur, STAGING_RANKED):
            cur.execute(f"""
                CREATE TABLE {STAGING_RANKED} AS
                SELECT udm_unq_id, nd_extracted_date
                FROM (
                    SELECT
                        udm_unq_id,
                        nd_extracted_date,
                        ROW_NUMBER() OVER (
                            PARTITION BY udm_unq_id
                            ORDER BY nd_extracted_date DESC
                        ) AS rn
                    FROM {SOURCE_TABLE}
                    WHERE psid = {PSID}
                ) ranked
                WHERE rn = 1
            """)
            cur.execute(
                f"ALTER TABLE {STAGING_RANKED} "
                f"ADD INDEX idx_unq_id (udm_unq_id(100))"
            )
            conn.commit()
            cur.execute(f"SELECT COUNT(*) FROM {STAGING_RANKED}")
            n = cur.fetchone()[0]
            print(f"    created  ({n:,} distinct udm_unq_id rows)")
        else:
            cur.execute(f"SELECT COUNT(*) FROM {STAGING_RANKED}")
            n = cur.fetchone()[0]
            print(f"    already exists, reusing  ({n:,} rows)")

        # ── 3. PK staging — eligible source rows ──────────────────────
        # Joins source table against ranked staging to identify which
        # udm_inc_ids will actually be inserted — used for batch boundaries.
        print("  Creating PK staging (eligible source rows)...")
        if not _table_exists(cur, STAGING_PK):
            cur.execute(f"""
                CREATE TABLE {STAGING_PK} AS
                SELECT a.{BATCH_KEY}
                FROM {SOURCE_TABLE} a
                INNER JOIN {STAGING_RANKED} r
                    ON  a.udm_unq_id        = r.udm_unq_id
                    AND a.nd_extracted_date = r.nd_extracted_date
                WHERE a.{BATCH_KEY} IS NOT NULL
            """)
            cur.execute(f"ALTER TABLE {STAGING_PK} ADD INDEX idx_pk ({BATCH_KEY})")
            conn.commit()
            cur.execute(f"SELECT COUNT(*) FROM {STAGING_PK}")
            n = cur.fetchone()[0]
            print(f"    created  ({n:,} eligible rows)")
        else:
            cur.execute(f"SELECT COUNT(*) FROM {STAGING_PK}")
            n = cur.fetchone()[0]
            print(f"    already exists, reusing  ({n:,} rows)")

        ranges, total = _build_ranges(cur, STAGING_PK)
        print(f"    {total:,} rows → {len(ranges)} batches of ~{BATCH_SIZE:,}")
        return ranges

    finally:
        cur.close()
        conn.close()


# ── Runner ────────────────────────────────────────────────────────────

def run_insert(ranges, pbar):
    conn       = get_connection()
    t0         = time.time()
    total_rows = 0

    try:
        if is_done(conn):
            conn.close()
            pbar.update(len(ranges))
            return {"status": "skipped", "rows": 0, "secs": 0.0}

        mark(conn, "running")
        cur = conn.cursor()

        cur.execute("SET unique_checks = 0")
        cur.execute("SET foreign_key_checks = 0")

        for lo, hi in ranges:
            sql = build_batch_insert(lo, hi)
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
        err_msg = str(exc)
        print(f"\n  [ERROR] {err_msg}")
        try:
            mark(conn, "failed", total_rows, err_msg)
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass
        return {"status": f"FAILED: {exc}", "rows": total_rows, "secs": elapsed}


# ── Main ──────────────────────────────────────────────────────────────

def main():
    print(f"\n{'='*70}", flush=True)
    print(f"  Upsert Prod Vitals ETL — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  source       : {SOURCE_TABLE}  (psid={PSID})")
    print(f"  dest         : {DEST_TABLE}")
    print(f"  ranked stg   : {STAGING_RANKED}")
    print(f"  pk staging   : {STAGING_PK}")
    print(f"  checkpoint   : {CHECKPOINT_TABLE}")
    print(f"  batch_key    : {BATCH_KEY}")
    print(f"  batch_size   : {BATCH_SIZE:,}")
    print(f"{'='*70}\n", flush=True)

    print("Setup:")
    sys.stdout.flush()
    ranges = setup_tables()
    print()

    if not ranges:
        print("  No eligible rows — nothing to do.")
        sys.exit(0)

    with tqdm(total=len(ranges), desc="Overall", unit="batch") as pbar:
        result = run_insert(ranges, pbar)

    status = result["status"]
    rows   = result["rows"]
    secs   = result["secs"]

    print(f"\n{'='*70}")
    if status == "done":
        print(f"  DONE   {rows:,} rows inserted  ({secs}s)")
    elif status == "skipped":
        print(f"  SKIPPED — already marked done in checkpoint")
    else:
        print(f"  FAILED — {status}")
    print(f"{'='*70}")

    print(f"\n  Cleanup SQL (run after verifying data):")
    print(f"    DROP TABLE IF EXISTS {STAGING_RANKED};")
    print(f"    DROP TABLE IF EXISTS {STAGING_PK};")
    print(f"    DROP TABLE IF EXISTS {CHECKPOINT_TABLE};")
    print()

    if status.startswith("FAILED"):
        sys.exit(1)


if __name__ == "__main__":
    main()
