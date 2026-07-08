#!/usr/bin/env python3
"""
unst_table.py — Recreate specific biogen_april tables (ros, examinations, radiology)

Drops + recreates the target tables and staging PK tables for these 3 sources,
then re-inserts from rgd_udm_silver using the same cohort + date filter logic
as biogen_subset.py.

The patient cohort table (staging.biogen_cohort_pats) is assumed to already exist.

Usage:
    python unst_table.py
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
    "database":        "rgd_udm_silver",
    "charset":         "utf8mb4",
    "connect_timeout": 30,
    "read_timeout":    21600,
    "write_timeout":   21600,
}

BATCH_SIZE = 50_000
BATCH_KEY  = "udm_inc_id"

DATE_LO = "2025-10-01"
DATE_HI = "2026-02-15"

COHORT_TABLE = "staging.biogen_cohort_pats"   # assumed to already exist
CKPT_TABLE   = "staging.biogen_april_checkpoint"


# ── Tables to recreate ───────────────────────────────────────────────
TABLES = [
    {
        "label":      "ros",
        "source":     "rgd_udm_silver.ros",
        "target":     "biogen_april.ros_04282026",
        "date_col":   "enc_date_proxy",
        "staging_pk": "staging.biogen_pk_ros",
        "ckpt_key":   "biogen_april.ros",
        "select_cols": """
    a.ndid,
    a.eid                AS encounter_id,
    a.enc_start_date     AS encounter_date,
    a.ros_category,
    a.ros_name           AS system_name,
    a.ros_option         AS Present,
    a.ros_notes          AS note,
    CAST(NULL AS SIGNED) AS incremental_id""",
    },
    {
        "label":      "examinations",
        "source":     "rgd_udm_silver.examination",
        "target":     "biogen_april.examinations_04282026",
        "date_col":   "enc_date_proxy",
        "staging_pk": "staging.biogen_pk_exam",
        "ckpt_key":   "biogen_april.examinations",
        "select_cols": """
    a.ndid,
    a.eid                AS encounter_id,
    a.enc_start_date     AS encounter_date,
    a.exam_date,
    a.examid             AS exam_id,
    a.exam_category,
    a.exam_name,
    a.exam_findings,
    CAST(NULL AS SIGNED) AS incremental_id""",
    },
    {
        "label":      "radiology",
        "source":     "rgd_udm_silver.radiology",
        "target":     "biogen_april.radiology_04282026",
        "date_col":   "enc_date_proxy",
        "staging_pk": "staging.biogen_pk_rad",
        "ckpt_key":   "biogen_april.radiology",
        "select_cols": """
    a.ndid,
    a.eid                    AS encounter_id,
    DATE(a.enc_date)         AS encounter_date,
    a.report_id,
    a.study_name             AS test_name,
    a.img_finding            AS test_parameter,
    a.img_status             AS resultstatus,
    a.img_date               AS resultdate,
    a.img_report_text        AS value,
    a.internal_notes         AS note,
    CAST(NULL AS SIGNED)     AS incremental_id""",
    },
]


# ── Helpers ──────────────────────────────────────────────────────────

def get_connection():
    return pymysql.connect(**DB_CONFIG)


def _table_exists(cur, full_table_name: str) -> bool:
    schema, table = full_table_name.split(".", 1)
    cur.execute(
        "SELECT COUNT(*) FROM information_schema.tables "
        "WHERE table_schema = %s AND table_name = %s",
        (schema, table),
    )
    return cur.fetchone()[0] > 0


def _build_ranges(cur, staging_pk: str):
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


# ── Checkpoint ───────────────────────────────────────────────────────

def mark(conn, ckpt_key: str, status: str, rows: int = 0, error: str = None) -> None:
    cur = conn.cursor()
    cur.execute(f"""
        INSERT INTO {CKPT_TABLE}
            (source_key, status, rows_inserted, started_at, completed_at, error_msg)
        VALUES (%s, %s, %s, NOW(), IF(%s = 'done', NOW(), NULL), %s)
        ON DUPLICATE KEY UPDATE
            status        = VALUES(status),
            rows_inserted = VALUES(rows_inserted),
            completed_at  = IF(VALUES(status) = 'done', NOW(), NULL),
            error_msg     = VALUES(error_msg)
    """, (ckpt_key, status, rows, status, error))
    conn.commit()
    cur.close()


# ── Reset + Setup ────────────────────────────────────────────────────

def reset_and_setup(tbl: dict):
    """
    1. DROP target table + staging PK table.
    2. DELETE checkpoint entry so it re-runs.
    3. Recreate target table (empty schema from SELECT WHERE 1=0).
    4. Recreate staging PK table (cohort JOIN + date filter).
    5. Return batch ranges.
    """
    label      = tbl["label"]
    source     = tbl["source"]
    target     = tbl["target"]
    date_col   = tbl["date_col"]
    staging_pk = tbl["staging_pk"]
    ckpt_key   = tbl["ckpt_key"]

    conn = get_connection()
    cur  = conn.cursor()

    try:
        # ── 1. Drop existing tables ──────────────────────────────────
        print(f"  Dropping {target} ...")
        cur.execute(f"DROP TABLE IF EXISTS {target}")
        conn.commit()
        print(f"    dropped")

        print(f"  Dropping {staging_pk} ...")
        cur.execute(f"DROP TABLE IF EXISTS {staging_pk}")
        conn.commit()
        print(f"    dropped")

        # ── 2. Clear checkpoint ──────────────────────────────────────
        cur.execute(
            f"DELETE FROM {CKPT_TABLE} WHERE source_key = %s",
            (ckpt_key,),
        )
        conn.commit()
        print(f"  Checkpoint cleared for {ckpt_key}")

        # ── 3. Recreate target table (empty) ─────────────────────────
        print(f"  Creating target table {target} (empty schema)...")
        cur.execute(f"""
            CREATE TABLE {target}
            SELECT {tbl['select_cols']}
            FROM {source} a
            INNER JOIN {COHORT_TABLE} c ON a.ndid = c.pat_id
            WHERE 1 = 0
        """)
        conn.commit()
        print(f"    created (empty)")

        # ── 4. Recreate staging PK table ─────────────────────────────
        print(f"  Creating staging PK table {staging_pk}...")
        date_filter = ""
        if date_col is not None:
            date_filter = (
                f"  AND a.{date_col} >= '{DATE_LO}'\n"
                f"  AND a.{date_col} <= '{DATE_HI}'"
            )
        cur.execute(f"""
            CREATE TABLE {staging_pk} AS
            SELECT a.{BATCH_KEY}
            FROM {source} a
            INNER JOIN {COHORT_TABLE} c ON a.ndid = c.pat_id
            WHERE a.{BATCH_KEY} IS NOT NULL
            {date_filter}
        """)
        cur.execute(f"ALTER TABLE {staging_pk} ADD INDEX idx_pk ({BATCH_KEY})")
        conn.commit()
        cur.execute(f"SELECT COUNT(*) FROM {staging_pk}")
        n = cur.fetchone()[0]
        print(f"    created  ({n:,} eligible rows)")

        # ── 5. Build ranges ──────────────────────────────────────────
        ranges, total = _build_ranges(cur, staging_pk)
        print(f"    {len(ranges)} batches of ~{BATCH_SIZE:,} rows  (total eligible: {total:,})")
        return ranges, total

    finally:
        cur.close()
        conn.close()


# ── Runner ────────────────────────────────────────────────────────────

def run_source(tbl: dict, ranges: list, pbar: tqdm) -> dict:
    label       = tbl["label"]
    source      = tbl["source"]
    target      = tbl["target"]
    staging_pk  = tbl["staging_pk"]
    ckpt_key    = tbl["ckpt_key"]
    select_cols = tbl["select_cols"]

    conn       = get_connection()
    t0         = time.time()
    total_rows = 0

    try:
        mark(conn, ckpt_key, "running")
        cur = conn.cursor()

        cur.execute("SET unique_checks = 0")
        cur.execute("SET foreign_key_checks = 0")

        for lo, hi in ranges:
            sql = f"""
INSERT INTO {target}
SELECT {select_cols}
FROM {source} a
INNER JOIN {staging_pk} pk ON a.{BATCH_KEY} = pk.{BATCH_KEY}
WHERE a.{BATCH_KEY} >= {lo} AND a.{BATCH_KEY} < {hi}
"""
            cur.execute(sql)
            conn.commit()
            total_rows += cur.rowcount
            pbar.update(1)

        cur.execute("SET unique_checks = 1")
        cur.execute("SET foreign_key_checks = 1")
        cur.close()

        elapsed = round(time.time() - t0, 1)
        mark(conn, ckpt_key, "done", total_rows)
        conn.close()
        return {"status": "done", "rows": total_rows, "secs": elapsed}

    except Exception as exc:
        elapsed = round(time.time() - t0, 1)
        err_msg = str(exc)
        print(f"\n  [ERROR] {label}: {err_msg}")
        try:
            mark(conn, ckpt_key, "failed", total_rows, err_msg)
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
    print(f"  Biogen — Recreate: ros, examinations, radiology")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  cohort table : {COHORT_TABLE}")
    print(f"  date range   : {DATE_LO}  to  {DATE_HI}")
    print(f"  batch size   : {BATCH_SIZE:,}")
    print(f"{'='*70}\n", flush=True)

    # Verify cohort table exists before doing anything destructive
    conn = get_connection()
    cur  = conn.cursor()
    if not _table_exists(cur, COHORT_TABLE):
        print(f"  [ERROR] Cohort table {COHORT_TABLE} does not exist.")
        print(f"  Run biogen_subset.py first to build it.")
        cur.close()
        conn.close()
        sys.exit(1)
    cur.execute(f"SELECT COUNT(*) FROM {COHORT_TABLE}")
    cohort_n = cur.fetchone()[0]
    cur.close()
    conn.close()
    print(f"  Cohort table found: {cohort_n:,} patients\n")

    # ── Reset + setup each table ──────────────────────────────────────
    all_ranges    = {}
    total_batches = 0

    for tbl in TABLES:
        label = tbl["label"]
        print(f"Reset + Setup [{label}]  ({tbl['source']}  ->  {tbl['target']})")
        sys.stdout.flush()
        ranges, _ = reset_and_setup(tbl)
        all_ranges[label] = ranges
        total_batches += len(ranges)
        print()

    print(f"  Total batches: {total_batches:,}\n")

    # ── Insert phase ──────────────────────────────────────────────────
    results    = {}
    any_failed = False

    with tqdm(total=total_batches, desc="Overall", unit="batch") as pbar:
        for tbl in TABLES:
            label  = tbl["label"]
            ranges = all_ranges.get(label, [])
            if not ranges:
                print(f"  [SKIP] {label} — no eligible rows")
                sys.stdout.flush()
                continue
            result = run_source(tbl, ranges, pbar)
            results[label] = result
            if result["status"].startswith("FAILED"):
                any_failed = True

    # ── Summary ───────────────────────────────────────────────────────
    print(f"\n{'='*70}")
    print(f"  {'Label':<20}  {'Status':<10}  {'Rows':>12}  {'Secs':>8}")
    print(f"  {'-'*20}  {'-'*10}  {'-'*12}  {'-'*8}")
    for tbl in TABLES:
        label = tbl["label"]
        if label not in results:
            print(f"  {label:<20}  {'SKIPPED':<10}  {'—':>12}  {'—':>8}")
            continue
        r = results[label]
        status_tag = "DONE" if r["status"] == "done" else "FAILED"
        print(f"  {label:<20}  {status_tag:<10}  {r['rows']:>12,}  {r['secs']:>8}s")
    print(f"{'='*70}\n")

    # ── Cleanup hint ──────────────────────────────────────────────────
    print("  Cleanup SQL (run after verifying output):")
    for tbl in TABLES:
        print(f"    DROP TABLE IF EXISTS {tbl['staging_pk']};")
    print()

    if any_failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
