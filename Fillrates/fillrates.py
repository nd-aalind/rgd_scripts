#!/usr/bin/env python3
"""
Fill Rate Report Generator — rgd_udm_silver
============================================

For every column in every table (auto-discovered from information_schema), computes:
    schema_name | table_name | column_name | total_count | null_records | fill_rate_pct

A NULL/empty record is any row where the column IS NULL OR = ''.

Strategy (optimized):
  - Columns discovered dynamically from information_schema — no hardcoding needed
  - One aggregate SELECT per table → single full scan, all columns at once
    (avoids N separate scans for N columns — massively cheaper on large tables)
  - Tables processed in parallel via ThreadPoolExecutor (MAX_WORKERS)
  - Checkpoint/resume: re-run skips tables already marked 'done'
  - Results upserted into staging.fill_rate_report (safe to re-run)

NOTE: Large tables will still require a full sequential scan. Approximate sizes:
    medications_part1 ~381M rows, vitals ~298M, notes_part2 ~177M,
    encounters ~240M, notes_part1 ~142M. Run during off-peak hours.

Usage:
    python fillrates.py                                    # all tables in SOURCE_TABLES
    python fillrates.py --tables patients encounters       # specific tables only
    python fillrates.py --workers 6                        # increase parallelism
    python fillrates.py --reset                            # drop checkpoint, recompute all
"""

import argparse
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import pymysql
from tqdm import tqdm
import os
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())

# ── Configuration ─────────────────────────────────────────────────────────────
DB_CONFIG = {
    "host":            os.environ.get("DB_HOST"),
    "port":            3306,
    "user":            os.environ.get("DB_SUDHA_USER"),
    "password":        os.environ.get("DB_SUDHA_PASSWORD"),
    "database":        "mind",
    "charset":         "utf8mb4",
    "connect_timeout": 30,
    "read_timeout":    21600,
    "write_timeout":   21600,
}

MAX_WORKERS      = 4                                    # tables computed in parallel
SOURCE_SCHEMA    = "rgd_udm_silver"
REPORT_TABLE     = "staging.fill_rate_report_5"
CHECKPOINT_TABLE = "staging.etl_checkpoint_fillrates_v5"

# ── Source definitions ────────────────────────────────────────────────────────
# Default tables to process when --tables is not passed.
# Columns are discovered dynamically from information_schema at runtime.
SOURCE_TABLES = [
    "patients","procedures"
]


# ── Helpers ───────────────────────────────────────────────────────────────────

def get_connection():
    return pymysql.connect(**DB_CONFIG)


# ── Checkpoint ────────────────────────────────────────────────────────────────

def is_done(conn, source_key):
    cur = conn.cursor()
    cur.execute(
        f"SELECT status FROM {CHECKPOINT_TABLE} WHERE source_key = %s",
        (source_key,),
    )
    row = cur.fetchone()
    cur.close()
    return row is not None and row[0] == "done"


def mark(conn, source_key, status, error=None):
    cur = conn.cursor()
    cur.execute(f"""
        INSERT INTO {CHECKPOINT_TABLE}
            (source_key, status, started_at, completed_at, error_msg)
        VALUES (%s, %s, NOW(), IF(%s = 'done', NOW(), NULL), %s)
        ON DUPLICATE KEY UPDATE
            status       = VALUES(status),
            completed_at = IF(VALUES(status) = 'done', NOW(), NULL),
            error_msg    = VALUES(error_msg)
    """, (source_key, status, status, error))
    conn.commit()
    cur.close()


# ── Setup ─────────────────────────────────────────────────────────────────────

def setup_tables(reset=False):
    conn = get_connection()
    cur  = conn.cursor()

    if reset:
        print("  [reset] Dropping checkpoint table to force full recompute...")
        cur.execute(f"DROP TABLE IF EXISTS {CHECKPOINT_TABLE}")
        conn.commit()

    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {REPORT_TABLE} (
            id            BIGINT AUTO_INCREMENT PRIMARY KEY,
            schema_name   VARCHAR(100)   NOT NULL,
            table_name    VARCHAR(100)   NOT NULL,
            column_name   VARCHAR(200)   NOT NULL,
            total_count   BIGINT         DEFAULT NULL,
            null_records  BIGINT         DEFAULT NULL,
            fill_rate_pct DECIMAL(6, 2)  DEFAULT NULL,
            computed_at   DATETIME       DEFAULT NULL,
            UNIQUE KEY uk_col (schema_name, table_name, column_name)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
    """)

    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {CHECKPOINT_TABLE} (
            source_key   VARCHAR(200) NOT NULL PRIMARY KEY,
            status       ENUM('running', 'done', 'failed') NOT NULL DEFAULT 'running',
            started_at   DATETIME DEFAULT NULL,
            completed_at DATETIME DEFAULT NULL,
            error_msg    TEXT     DEFAULT NULL
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
    """)

    conn.commit()
    cur.close()
    conn.close()


# ── Query builder ─────────────────────────────────────────────────────────────

# String types — empty string '' is a valid "empty" value to flag
_STRING_TYPES = {"varchar", "char", "tinytext", "text", "mediumtext", "longtext",
                 "enum", "set", "json"}


def get_column_types(cur, table):
    """Returns {column_name: data_type} from information_schema."""
    cur.execute("""
        SELECT COLUMN_NAME, DATA_TYPE
        FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
        ORDER BY ORDINAL_POSITION
    """, (SOURCE_SCHEMA, table))
    return {row[0]: row[1].lower() for row in cur.fetchall()}


def get_approx_row_count(cur, table):
    """
    Fast approximate row count from information_schema.TABLES (no scan).
    InnoDB estimates — accurate within ~5-20% for large tables.
    Returns None if statistics unavailable.
    """
    cur.execute("""
        SELECT TABLE_ROWS
        FROM information_schema.TABLES
        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
    """, (SOURCE_SCHEMA, table))
    row = cur.fetchone()
    return row[0] if row else None


def build_null_query(table, columns):
    """
    Phase 1 — COUNT(col) scan: one full table scan, all columns at once.
    COUNT(col) natively skips NULLs — no CASE WHEN, no type issues, faster.
    Returns: total_count, then filled_count per column (NOT null count).
    null_count = total_count - COUNT(col)
    """
    col_exprs = ",\n    ".join(
        f"COUNT(`{col}`) AS `{col}`"
        for col in columns
    )
    return f"""
SELECT
    COUNT(*) AS __total__,
    {col_exprs}
FROM `{SOURCE_SCHEMA}`.`{table}`
"""


def build_empty_string_query(table, string_cols):
    """
    Phase 2 — empty string scan: runs ONLY on string-typed columns.
    Separate lightweight pass so DATE/numeric cols are never compared to ''.
    Also catches 'None' stored as string.
    """
    col_exprs = ",\n    ".join(
        f"SUM(`{col}` IN ('', 'None')) AS `{col}`"
        for col in string_cols
    )
    return f"""
SELECT
    {col_exprs}
FROM `{SOURCE_SCHEMA}`.`{table}`
"""


# ── Worker ────────────────────────────────────────────────────────────────────

def run_source(table, pbar):
    """Compute fill rates for one table, write results, update checkpoint."""
    key  = f"{SOURCE_SCHEMA}.{table}"
    conn = get_connection()

    if is_done(conn, key):
        conn.close()
        pbar.update(1)
        return {"table": table, "status": "skipped", "cols": 0, "secs": 0}

    mark(conn, key, "running")
    t0 = time.time()

    try:
        cur = conn.cursor()

        # ── Discover columns dynamically (instant — no scan) ──────────
        col_types = get_column_types(cur, table)
        if not col_types:
            raise ValueError(f"Table '{table}' not found in {SOURCE_SCHEMA} or has no columns")
        columns     = list(col_types.keys())
        string_cols = [c for c in columns if col_types[c] in _STRING_TYPES]

        # ── Approximate row count (instant — no scan) ─────────────────
        approx_rows = get_approx_row_count(cur, table)
        if approx_rows:
            print(f"\n  [{table}] ~{approx_rows:,} rows, {len(string_cols)} string cols",
                  flush=True)

        # ── Phase 1: COUNT(col) — one full scan, catches NULLs ───────
        sql_null = build_null_query(table, columns)
        cur.execute(sql_null)
        row1 = cur.fetchone()

        total_count  = int(row1[0])
        filled_count = {col: int(ct or 0) for col, ct in zip(columns, row1[1:])}

        # null_count[col] = total - COUNT(col)  [NULLs only at this point]
        null_counts = {col: total_count - filled_count[col] for col in columns}

        # ── Phase 2: empty string scan — string cols only ─────────────
        empty_counts = {col: 0 for col in columns}
        if string_cols:
            sql_empty = build_empty_string_query(table, string_cols)
            cur.execute(sql_empty)
            row2 = cur.fetchone()
            for col, ec in zip(string_cols, row2):
                empty_counts[col] = int(ec or 0)

        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # ── Upsert + commit per column — results land in DB immediately
        # after the scan. A crash mid-write keeps everything already saved.
        for col in columns:
            empty_total = null_counts[col] + empty_counts[col]
            fill_rate   = (
                round(100.0 * (total_count - empty_total) / total_count, 2)
                if total_count else 0.0
            )
            cur.execute(f"""
                INSERT INTO {REPORT_TABLE}
                    (schema_name, table_name, column_name,
                     total_count, null_records, fill_rate_pct, computed_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    total_count   = VALUES(total_count),
                    null_records  = VALUES(null_records),
                    fill_rate_pct = VALUES(fill_rate_pct),
                    computed_at   = VALUES(computed_at)
            """, (SOURCE_SCHEMA, table, col, total_count, empty_total, fill_rate, now))
            conn.commit()   # ← persisted immediately, visible in DB right away

        cur.close()

        elapsed = round(time.time() - t0, 1)
        mark(conn, key, "done")
        conn.close()
        pbar.update(1)
        return {"table": table, "status": "done", "cols": len(columns), "secs": elapsed}

    except Exception as exc:
        elapsed = round(time.time() - t0, 1)
        mark(conn, key, "failed", str(exc))
        try:
            conn.close()
        except Exception:
            pass
        pbar.update(1)
        return {"table": table, "status": f"FAILED: {exc}", "cols": 0, "secs": elapsed}


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Fill rate report — rgd_udm_silver")
    parser.add_argument("--tables", nargs="+", metavar="TABLE",
                        help="One or more table names to process (default: all SOURCE_TABLES)")
    parser.add_argument("--workers", type=int, default=MAX_WORKERS,
                        help=f"Parallel workers (default: {MAX_WORKERS})")
    parser.add_argument("--reset", action="store_true",
                        help="Drop checkpoint table and recompute all tables from scratch")
    args = parser.parse_args()

    tables = args.tables if args.tables else SOURCE_TABLES

    print(f"\n{'='*70}")
    print(f"  Fill Rate Report — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  schema     : {SOURCE_SCHEMA}")
    print(f"  tables     : {len(tables)}")
    print(f"  columns    : auto-discovered from information_schema")
    print(f"  report     : {REPORT_TABLE}")
    print(f"  checkpoint : {CHECKPOINT_TABLE}")
    print(f"  workers    : {args.workers}")
    if args.tables:
        print(f"  filter     : {', '.join(tables)}")
    if args.reset:
        print(f"  mode       : RESET — all tables will be recomputed")
    print(f"{'='*70}\n")

    print("Setting up report and checkpoint tables...")
    setup_tables(reset=args.reset)
    print("  ready\n")

    results = []
    with tqdm(total=len(tables), desc="Tables", unit="table") as pbar:
        with ThreadPoolExecutor(max_workers=args.workers) as pool:
            futures = {
                pool.submit(run_source, tbl, pbar): tbl
                for tbl in tables
            }
            for future in as_completed(futures):
                results.append(future.result())

    # ── Summary ───────────────────────────────────────────────────────
    print()
    print(f"{'─'*70}")
    print(f"  {'TABLE':<28} {'STATUS':<8} {'COLS':>5} {'TIME':>8}")
    print(f"{'─'*70}")
    for r in sorted(results, key=lambda x: x["table"]):
        tag = "DONE" if r["status"] == "done" \
              else "SKIP" if r["status"] == "skipped" \
              else "FAIL"
        print(f"  [{tag}] {r['table']:<26} {r['cols']:>5} cols  {r['secs']:>6}s")

    done    = sum(1 for r in results if r["status"] == "done")
    skipped = sum(1 for r in results if r["status"] == "skipped")
    failed  = [r for r in results if "FAILED" in r["status"]]

    print(f"{'='*70}")
    print(f"  Done: {done}  Skipped: {skipped}  Failed: {len(failed)}")
    print(f"\n  Results written to: {REPORT_TABLE}")
    print(f"\n  Preview:")
    print(f"    SELECT schema_name, table_name, column_name,")
    print(f"           total_count, null_records, fill_rate_pct")
    print(f"    FROM {REPORT_TABLE}")
    print(f"    ORDER BY table_name, fill_rate_pct ASC;")
    print(f"\n  Low fill-rate columns (< 50%):")
    print(f"    SELECT schema_name, table_name, column_name, fill_rate_pct")
    print(f"    FROM {REPORT_TABLE}")
    print(f"    WHERE fill_rate_pct < 50")
    print(f"    ORDER BY fill_rate_pct ASC;")
    print(f"\n  Cleanup SQL (when done):")
    print(f"    DROP TABLE IF EXISTS {REPORT_TABLE};")
    print(f"    DROP TABLE IF EXISTS {CHECKPOINT_TABLE};")
    print(f"{'='*70}\n")

    if failed:
        print("  Failed tables:")
        for r in failed:
            print(f"    {r['table']}: {r['status']}")
        sys.exit(1)


if __name__ == "__main__":
    main()
