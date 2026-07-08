#!/usr/bin/env python3
"""
Standalone re-run of Pass 3 only: primary_diagnosis_flag_std
for: rgd_udm_silver.diagnosis

Pass 1 and Pass 2 are already done — this script skips them entirely
and only executes the primary_diagnosis_flag_std UPDATE.

Uses the existing Pass 3 PK staging table (staging.diag_std_pk_pass3)
if it exists, or creates it fresh.
Uses a fresh checkpoint key so it runs regardless of prior checkpoint state.

Usage:
    python diag_rem.py
"""

import sys
import time
from datetime import datetime
import pymysql
from tqdm import tqdm
import os
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())

# ── Configuration ─────────────────────────────────────────────────────
DB_CONFIG = {
    "host":            os.environ.get("DB_INTERNAL_HOST"),
    "port":            3306,
    "user":            os.environ.get("DB_INTERNAL_USER"),
    "password":        os.environ.get("DB_INTERNAL_PASSWORD"),
    "database":        "rgd_udm_silver",
    "charset":         "utf8mb4",
    "connect_timeout": 30,
    "read_timeout":    21600,
    "write_timeout":   21600,
}

BATCH_SIZE = 50_000

TARGET_TABLE     = "rgd_udm_silver.diagnosis"
STAGING_PK       = "staging.diag_std_pk_pass3"
CHECKPOINT_TABLE = "staging.etl_checkpoint_diag_standardisation"
CHECKPOINT_KEY   = "diagnosis.standardisation.pass3_primary_flag"
BATCH_KEY        = "udm_inc_id"


# ── Helpers ───────────────────────────────────────────────────────────

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


# ── Batch UPDATE builder ──────────────────────────────────────────────

def build_pass3(pk_lo, pk_hi):
    # CAST to CHAR for all string comparisons.
    # REGEXP '^[0-9]' guards UNSIGNED casts — prevents error 1292
    # when the column contains non-numeric values like 'Y', 'N'.
    return f"""
UPDATE {TARGET_TABLE} d
SET
    d.primary_diagnosis_flag_std = CASE
        WHEN LOWER(d.ehr_source_name) = 'ecw'
             AND CAST(d.primary_diagnosis_flag AS CHAR) = '1' THEN 'Y'
        WHEN LOWER(d.ehr_source_name) = 'ecw'
             AND CAST(d.primary_diagnosis_flag AS CHAR) = '0' THEN 'N'
        WHEN LOWER(d.ehr_source_name) = 'athenaone'
             AND CAST(d.primary_diagnosis_flag AS CHAR) = '0' THEN 'Y'
        WHEN LOWER(d.ehr_source_name) = 'athenaone'
             AND CAST(d.primary_diagnosis_flag AS CHAR) REGEXP '^[0-9]'
             AND CAST(d.primary_diagnosis_flag AS UNSIGNED) > 0 THEN 'N'
        WHEN LOWER(d.ehr_source_name) IN ('greenway', 'athenapractice')
             AND CAST(d.primary_diagnosis_flag AS CHAR) = '1' THEN 'Y'
        WHEN LOWER(d.ehr_source_name) IN ('greenway', 'athenapractice')
             AND CAST(d.primary_diagnosis_flag AS CHAR) REGEXP '^[0-9]'
             AND CAST(d.primary_diagnosis_flag AS UNSIGNED) > 1 THEN 'N'
        WHEN CAST(d.primary_diagnosis_flag AS CHAR) IN ('y', 'Y') THEN 'Y'
        WHEN CAST(d.primary_diagnosis_flag AS CHAR) IN ('n', 'N') THEN 'N'
        WHEN d.primary_diagnosis_flag IS NULL THEN NULL
        ELSE 'NS'
    END
WHERE d.{BATCH_KEY} >= {pk_lo}
  AND d.{BATCH_KEY} < {pk_hi}
"""


# ── Checkpoint ─────────────────────────────────────────────────────────

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
            (source_key, status, rows_updated, started_at, completed_at, error_msg)
        VALUES (%s, %s, %s, NOW(), IF(%s = 'done', NOW(), NULL), %s)
        ON DUPLICATE KEY UPDATE
            status       = VALUES(status),
            rows_updated = VALUES(rows_updated),
            completed_at = IF(VALUES(status) = 'done', NOW(), NULL),
            error_msg    = VALUES(error_msg)
    """, (CHECKPOINT_KEY, status, rows, status, error))
    conn.commit()
    cur.close()


# ── Setup ──────────────────────────────────────────────────────────────

def setup():
    """
    Reset Pass 3 checkpoint to allow re-run, then return batch ranges.
    Reuses existing PK staging table if present, creates it fresh if not.
    """
    conn = get_connection()
    cur  = conn.cursor()

    # Reset checkpoint so Pass 3 runs regardless of prior failed/done state
    print("  Resetting Pass 3 checkpoint...")
    cur.execute(
        f"DELETE FROM {CHECKPOINT_TABLE} WHERE source_key = %s",
        (CHECKPOINT_KEY,),
    )
    conn.commit()
    print("    reset")

    # Reuse or create PK staging
    print(f"  PK staging: {STAGING_PK}")
    if _table_exists(cur, STAGING_PK):
        print("    already exists, reusing")
    else:
        print("    not found — creating...")
        cur.execute(f"""
            CREATE TABLE {STAGING_PK} AS
            SELECT {BATCH_KEY}
            FROM {TARGET_TABLE}
            WHERE {BATCH_KEY} IS NOT NULL
        """)
        cur.execute(f"ALTER TABLE {STAGING_PK} ADD INDEX idx_pk ({BATCH_KEY})")
        conn.commit()
        print("    created")

    cur.execute(f"SELECT COUNT(*) FROM {STAGING_PK}")
    total = cur.fetchone()[0]
    print(f"    {total:,} rows")

    if total == 0:
        cur.close()
        conn.close()
        return []

    # Batch boundary sampling
    print("  Computing batch boundaries...")
    cur.execute(f"""
        SELECT {BATCH_KEY}
        FROM (
            SELECT {BATCH_KEY},
                   ROW_NUMBER() OVER (ORDER BY {BATCH_KEY}) AS rn
            FROM {STAGING_PK}
        ) t
        WHERE (rn - 1) % {BATCH_SIZE} = 0
        ORDER BY {BATCH_KEY}
    """)
    boundaries = [row[0] for row in cur.fetchall() if row[0] is not None]

    cur.execute(f"SELECT MAX({BATCH_KEY}) FROM {STAGING_PK}")
    max_pk = int(cur.fetchone()[0])

    cur.close()
    conn.close()

    ranges = []
    for i, lo in enumerate(boundaries):
        hi = boundaries[i + 1] if i + 1 < len(boundaries) else max_pk + 1
        ranges.append((lo, hi))

    print(f"    {len(ranges)} batches of ~{BATCH_SIZE:,} rows each")
    return ranges


# ── Runner ─────────────────────────────────────────────────────────────

def run(ranges, pbar):
    conn = get_connection()
    mark(conn, "running")
    t0 = time.time()
    total_rows = 0

    try:
        cur = conn.cursor()
        cur.execute("SET unique_checks = 0")
        cur.execute("SET foreign_key_checks = 0")

        for pk_lo, pk_hi in ranges:
            sql = build_pass3(pk_lo, pk_hi)
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


# ── Main ───────────────────────────────────────────────────────────────

def main():
    print(f"\n{'='*70}", flush=True)
    print(f"  Diagnosis Pass 3 (primary_diagnosis_flag_std) — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  target     : {TARGET_TABLE}")
    print(f"  staging_pk : {STAGING_PK}")
    print(f"  checkpoint : {CHECKPOINT_TABLE}  key={CHECKPOINT_KEY}")
    print(f"  batch_size : {BATCH_SIZE:,}")
    print(f"{'='*70}\n", flush=True)

    sys.stdout.flush()
    ranges = setup()

    if not ranges:
        print("\nNo rows found. Exiting.")
        return

    result = None
    with tqdm(total=len(ranges), desc="Pass3", unit="batch") as pbar:
        result = run(ranges, pbar)

    print()
    tag = " DONE" if result["status"] == "done" else " FAIL"
    print(f"  [{tag}] {TARGET_TABLE}  {result['rows']:>10,} rows updated  ({result['secs']}s)")

    print(f"\n{'='*70}")
    if result["status"].startswith("FAILED"):
        print(f"  FAILED: {result['status']}")
        sys.exit(1)
    else:
        print(f"  Status: done  |  Total rows updated: {result['rows']:,}")
    print(f"{'='*70}")


if __name__ == "__main__":
    main()
