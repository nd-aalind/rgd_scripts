#!/usr/bin/env python3
"""
biogen_notes.py — Biogen April notes ETL (standalone)

Creates biogen_april.note from:
  - rgd_udm_silver.notes_part1  (creates target)
  - rgd_udm_silver.notes_part2  (appends to same target)

Designed to run independently from biogen_subset.py without impacting other tables.
Reuses staging.biogen_cohort_pats if it exists (created by biogen_subset.py),
or creates it fresh if not.

Notes tables are large — this script:
  1. Ensures all required indexes exist on source tables before batching
  2. Uses BATCH_SIZE = 200,000 (larger than other tables — notes rows are smaller)
  3. Per-part staging PK tables with index on udm_inc_id
  4. Checkpoint/resume per part — re-run skips completed parts
  5. InnoDB session tuning + commit per batch

Usage:
    python biogen_notes.py
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
    "database":        "rgd_udm_silver",
    "charset":         "utf8mb4",
    "connect_timeout": 30,
    "read_timeout":    21600,
    "write_timeout":   21600,
}

BATCH_SIZE = 200_000      # larger batches — note rows are smaller than most tables
BATCH_KEY  = "udm_inc_id"

DATE_LO = "2025-10-01"
DATE_HI = "2026-02-15"

COHORT_TABLE = "staging.biogen_cohort_pats"   # shared with biogen_subset.py
TARGET_TABLE = "biogen_april.note_final_04282026"

STAGING_PK_PART1 = "staging.biogen_notes_pk_part11"
STAGING_PK_PART2 = "staging.biogen_notes_pk_part21"

CKPT_TABLE   = "staging.biogen_notes_checkpoint1"
CKPT_PART1   = "biogen_notes.part11"
CKPT_PART2   = "biogen_notes.part21"

SELECT_COLS = """
    a.ndid,
    a.eid                AS encounter_id,
    a.enc_start_date     AS encounter_date,
    a.note_type,
    a.note_source,
    a.note,
    CAST(NULL AS SIGNED) AS incremental_id"""

PARTS = [
    {
        "label":      "notes_part1",
        "source":     "rgd_udm_silver.notes_part1",
        "staging_pk": STAGING_PK_PART1,
        "ckpt_key":   CKPT_PART1,
        "creates_target": True,
    },
    {
        "label":      "notes_part2",
        "source":     "rgd_udm_silver.notes_part2",
        "staging_pk": STAGING_PK_PART2,
        "ckpt_key":   CKPT_PART2,
        "creates_target": False,
    },
]


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


def _index_exists(cur, schema, table, column):
    """Return True if any index already covers column as its first key part."""
    cur.execute(
        "SELECT COUNT(*) FROM information_schema.statistics "
        "WHERE table_schema = %s AND table_name = %s "
        "  AND column_name = %s AND seq_in_index = 1",
        (schema, table, column),
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


# ── Checkpoint ────────────────────────────────────────────────────────

def is_done(conn, ckpt_key):
    cur = conn.cursor()
    cur.execute(
        f"SELECT status FROM {CKPT_TABLE} WHERE source_key = %s",
        (ckpt_key,),
    )
    row = cur.fetchone()
    cur.close()
    return row is not None and row[0] == "done"


def mark(conn, ckpt_key, status, rows=0, error=None):
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


# ── Index setup ────────────────────────────────────────────────────────

def ensure_indexes(cur, conn):
    """
    Ensure indexes exist on source notes tables and the cohort table.
    Skips silently if already present.
    """
    needed = [
        # cohort table
        ("staging",         "biogen_cohort_pats", "pat_id"),
        # notes_part1 — join key, date filter, batch key
        ("rgd_udm_silver",  "notes_part1",        "ndid"),
        ("rgd_udm_silver",  "notes_part1",        "enc_start_date"),
        ("rgd_udm_silver",  "notes_part1",        "udm_inc_id"),
        # notes_part2
        ("rgd_udm_silver",  "notes_part2",        "ndid"),
        ("rgd_udm_silver",  "notes_part2",        "enc_start_date"),
        ("rgd_udm_silver",  "notes_part2",        "udm_inc_id"),
    ]

    print("  Checking source table indexes...")
    created = []
    for schema, table, col in needed:
        if not _index_exists(cur, schema, table, col):
            idx_name = f"idx_biogen_{col.lower()}"
            print(f"    creating index on {schema}.{table}({col})...")
            cur.execute(
                f"ALTER TABLE `{schema}`.`{table}` ADD INDEX `{idx_name}` (`{col}`)"
            )
            conn.commit()
            created.append(f"{table}.{col}")

    if created:
        print(f"    created {len(created)} index(es): {', '.join(created)}")
    else:
        print("    all indexes already present")


# ── Setup ──────────────────────────────────────────────────────────────

def setup_tables():
    conn = get_connection()
    cur  = conn.cursor()

    try:
        # ── 0. Ensure indexes ─────────────────────────────────────────
        ensure_indexes(cur, conn)

        # ── 1. Checkpoint table ───────────────────────────────────────
        print("  Creating checkpoint table...")
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {CKPT_TABLE} (
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

        # ── 2. Cohort table — reuse from biogen_subset or create fresh ──
        print(f"  Checking cohort table {COHORT_TABLE}...")
        if not _table_exists(cur, COHORT_TABLE):
            print("    not found — creating fresh cohort...")
            cur.execute(f"""
                CREATE TABLE {COHORT_TABLE} AS
                SELECT COALESCE(ndid_v1, ndid) AS pat_id
                FROM biogen.patient_list
                UNION ALL
                SELECT ndid AS pat_id
                FROM biogen.patient_list_athenaone
            """)
            cur.execute(f"ALTER TABLE {COHORT_TABLE} ADD INDEX idx_pat_id (pat_id)")
            conn.commit()
            cur.execute(f"SELECT COUNT(*) FROM {COHORT_TABLE}")
            print(f"    created ({cur.fetchone()[0]:,} rows)")
        else:
            cur.execute(f"SELECT COUNT(*) FROM {COHORT_TABLE}")
            print(f"    reusing ({cur.fetchone()[0]:,} rows)")

        # ── 3. Target table ───────────────────────────────────────────
        print(f"  Checking target table {TARGET_TABLE}...")
        if not _table_exists(cur, TARGET_TABLE):
            print("    creating (empty schema)...")
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {TARGET_TABLE}
                SELECT {SELECT_COLS}
                FROM rgd_udm_silver.notes_part1 a
                INNER JOIN {COHORT_TABLE} c ON a.ndid = c.pat_id
                WHERE 1 = 0
            """)
            conn.commit()
            print("    created (empty)")
        else:
            print("    already exists — will append")

        # ── 4. Per-part PK staging tables ────────────────────────────
        all_ranges = {}
        for part in PARTS:
            label      = part["label"]
            source     = part["source"]
            staging_pk = part["staging_pk"]

            print(f"  Creating PK staging for {label}...")
            if not _table_exists(cur, staging_pk):
                cur.execute(f"""
                    CREATE TABLE {staging_pk} AS
                    SELECT a.{BATCH_KEY}
                    FROM {source} a
                    INNER JOIN {COHORT_TABLE} c ON a.ndid = c.pat_id
                    WHERE a.{BATCH_KEY} IS NOT NULL
                      AND a.enc_start_date >= '{DATE_LO}'
                      AND a.enc_start_date <= '{DATE_HI}'
                """)
                cur.execute(
                    f"ALTER TABLE {staging_pk} ADD INDEX idx_pk ({BATCH_KEY})"
                )
                conn.commit()
                cur.execute(f"SELECT COUNT(*) FROM {staging_pk}")
                n = cur.fetchone()[0]
                print(f"    created ({n:,} eligible rows)")
            else:
                cur.execute(f"SELECT COUNT(*) FROM {staging_pk}")
                n = cur.fetchone()[0]
                print(f"    already exists, reusing ({n:,} rows)")

            ranges, total = _build_ranges(cur, staging_pk)
            print(f"    {total:,} rows → {len(ranges)} batches of ~{BATCH_SIZE:,}")
            all_ranges[part["ckpt_key"]] = ranges

        return all_ranges

    finally:
        cur.close()
        conn.close()


# ── Runner ─────────────────────────────────────────────────────────────

def run_part(part, ranges, pbar):
    label      = part["label"]
    source     = part["source"]
    staging_pk = part["staging_pk"]
    ckpt_key   = part["ckpt_key"]

    conn = get_connection()
    t0   = time.time()
    total_rows = 0

    try:
        if is_done(conn, ckpt_key):
            conn.close()
            pbar.update(len(ranges))
            return {"status": "skipped", "rows": 0, "secs": 0.0}

        mark(conn, ckpt_key, "running")

        cur = conn.cursor()
        cur.execute("SET unique_checks = 0")
        cur.execute("SET foreign_key_checks = 0")

        for lo, hi in ranges:
            sql = f"""
INSERT INTO {TARGET_TABLE}
SELECT {SELECT_COLS}
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


# ── Main ───────────────────────────────────────────────────────────────

def main():
    print(f"\n{'='*70}", flush=True)
    print(f"  Biogen Notes ETL — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  target      : {TARGET_TABLE}")
    print(f"  cohort      : {COHORT_TABLE}")
    print(f"  date range  : {DATE_LO}  to  {DATE_HI}")
    print(f"  batch key   : {BATCH_KEY}")
    print(f"  batch size  : {BATCH_SIZE:,}")
    print(f"  sources     : notes_part1 + notes_part2")
    print(f"{'='*70}\n", flush=True)

    print("  Setting up tables...", flush=True)
    all_ranges = setup_tables()
    print()

    results = {}
    any_failed = False

    total_batches = sum(len(r) for r in all_ranges.values())
    with tqdm(total=total_batches, desc="Overall", unit="batch") as pbar:
        for part in PARTS:
            ckpt_key = part["ckpt_key"]
            ranges   = all_ranges.get(ckpt_key, [])

            if not ranges:
                print(f"\n  [SKIP] {part['label']} — no eligible rows")
                continue

            print(f"\n  Starting {part['label']} ({len(ranges)} batches)...")
            result = run_part(part, ranges, pbar)
            results[ckpt_key] = result

            if result["status"].startswith("FAILED"):
                any_failed = True
                print(f"  FAILED at {part['label']} — continuing with next part...")

    print(f"\n{'='*70}")
    print(f"  Per-part summary:")
    total_rows = 0
    for part in PARTS:
        ckpt_key = part["ckpt_key"]
        res = results.get(ckpt_key, {"status": "not run", "rows": 0, "secs": 0})
        status = res["status"]
        rows   = res["rows"]
        secs   = res["secs"]
        if status == "done":
            tag = " DONE"; total_rows += rows
        elif status == "skipped":
            tag = " SKIP"
        elif status == "not run":
            tag = "  ---"
        else:
            tag = " FAIL"; any_failed = True
        print(f"  [{tag}] {part['label']:<20}  {rows:>12,} rows  ({secs}s)")
        if status.startswith("FAILED"):
            print(f"         {status}")

    # Final count in target
    conn = get_connection()
    cur  = conn.cursor()
    if _table_exists(cur, TARGET_TABLE):
        cur.execute(f"SELECT COUNT(*), COUNT(DISTINCT ndid), COUNT(DISTINCT encounter_id) FROM {TARGET_TABLE}")
        row = cur.fetchone()
        print(f"\n  {TARGET_TABLE}:")
        print(f"    total rows     : {row[0]:,}")
        print(f"    distinct ndid  : {row[1]:,}")
        print(f"    distinct enc   : {row[2]:,}")
    cur.close()
    conn.close()

    print(f"\n  Total rows inserted : {total_rows:,}")
    print(f"{'='*70}")

    print(f"\n  Cleanup SQL (run after verifying data):")
    print(f"    DROP TABLE IF EXISTS {STAGING_PK_PART1};")
    print(f"    DROP TABLE IF EXISTS {STAGING_PK_PART2};")
    print(f"    DROP TABLE IF EXISTS {CKPT_TABLE};")
    print(f"    -- DROP TABLE IF EXISTS {COHORT_TABLE};  -- shared with biogen_subset.py")

    if any_failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
