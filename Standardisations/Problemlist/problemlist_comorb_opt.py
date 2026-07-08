#!/usr/bin/env python3
"""
problemlist_comorb_opt.py — Optimized batched comorbidity UPDATE for problemlist

Original SQL:
    UPDATE udm_staging.problemlist_rgd_v2 pl
    LEFT JOIN semantics.comorbidity_llm_map cm ON pl.problem_desc LIKE cm.problem_desc
    SET pl.charlson_comorbidity   = cm.Charlson,
        pl.elixhauser_comorbidity = cm.Elixhauser
    WHERE pl.problem_desc IS NOT NULL
      AND (charlson_comorbidity IS NULL OR elixhauser_comorbidity IS NULL)

The LIKE join is run ONCE upfront on all eligible rows → stored in STAGING_MATCH_MAP
(ndid → charlson_comorbidity, elixhauser_comorbidity).
Batches then use a fast integer equality join on ndid — no LIKE per batch.

Pre-materialized tables (computed ONCE):
  - staging.prob_comorb_llm_map_v1         (semantics.comorbidity_llm_map with prefix index)
  - staging.prob_comorb_match_v1_<tbl>     (ndid → matched comorbidities, LIKE join done once)

Usage:
    python problemlist_comorb_opt.py
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
    "host":            os.environ.get("DB_HOST"),
    "port":            3306,
    "user":            os.environ.get("DB_USER"),
    "password":        os.environ.get("DB_PASSWORD"),
    "database":        "tng_athena_one",
    "charset":         "utf8mb4",
    "connect_timeout": 30,
    "read_timeout":    21600,
    "write_timeout":   21600,
}

BATCH_SIZE   = 50_000
TARGET_TABLE = "udm_staging.problemlist_rgd_v2"
BATCH_KEY    = "ndid"

_TABLE_SUFFIX = TARGET_TABLE.replace(".", "_").replace("-", "_")

# Shared lookup (reused if already built by another run)
STAGING_LLM_MAP     = "staging.prob_comorb_llm_map_v1"

# Per-run tables
STAGING_UNIQUE_DESCS = f"staging.prob_comorb_udescs_v2_{_TABLE_SUFFIX}"   # distinct problem_desc values
STAGING_DESC_MAP     = f"staging.prob_comorb_descmap_v2_{_TABLE_SUFFIX}"  # desc → comorbidities (LIKE done once)
STAGING_PK           = f"staging.prob_comorb_pk_v2_{_TABLE_SUFFIX}"
CHECKPOINT_TABLE     = f"staging.etl_checkpoint_prob_comorb1_{_TABLE_SUFFIX}"
CHECKPOINT_KEY       = f"problemlist.comorb1.llm.{_TABLE_SUFFIX}"


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


def _col_exists(cur, full_table_name, col_name):
    schema, table = full_table_name.split(".", 1)
    cur.execute(
        "SELECT COUNT(*) FROM information_schema.columns "
        "WHERE table_schema = %s AND table_name = %s AND column_name = %s",
        (schema, table, col_name),
    )
    return cur.fetchone()[0] > 0


def _index_exists(cur, schema, table, column):
    cur.execute("""
        SELECT COUNT(*) FROM information_schema.statistics
        WHERE table_schema = %s AND table_name = %s AND column_name = %s
    """, (schema, table, column))
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


# ── Batch UPDATE builder ──────────────────────────────────────────────

def build_batch_update(pk_lo, pk_hi):
    """
    Two indexed joins — no LIKE per batch:
      1. STAGING_PK (integer ndid) — restricts to eligible rows
      2. STAGING_DESC_MAP (prefix(100) equality on problem_desc) — applies pre-computed comorbidities
    The expensive LIKE join was run once during setup on unique descs only.
    """
    return f"""
UPDATE {TARGET_TABLE} p
JOIN {STAGING_PK} pkc ON p.{BATCH_KEY} = pkc.{BATCH_KEY}
LEFT JOIN {STAGING_DESC_MAP} dm ON p.problem_desc = dm.problem_desc
SET p.charlson_comorbidity   = dm.charlson_comorbidity,
    p.elixhauser_comorbidity = dm.elixhauser_comorbidity
WHERE p.{BATCH_KEY} >= {pk_lo}
  AND p.{BATCH_KEY} <  {pk_hi}
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


# ── DDL: ensure comorbidity columns exist ─────────────────────────────

def ensure_std_columns():
    cols = [
        ("charlson_comorbidity",   "VARCHAR(255)"),
        ("elixhauser_comorbidity", "VARCHAR(255)"),
    ]
    print(f"  Checking comorbidity columns on {TARGET_TABLE}...", flush=True)
    conn = get_connection()
    cur  = conn.cursor()
    try:
        for col_name, col_type in cols:
            if not _col_exists(cur, TARGET_TABLE, col_name):
                print(f"    adding: {col_name} {col_type} ...", flush=True)
                cur.execute(
                    f"ALTER TABLE {TARGET_TABLE} ADD COLUMN {col_name} {col_type} DEFAULT NULL"
                )
                conn.commit()
                print(f"    added: {col_name}")
            else:
                print(f"    exists: {col_name}")
    except Exception as exc:
        print(f"\n  ERROR: Could not alter {TARGET_TABLE}: {exc}")
        print(f"  Check for metadata locks:")
        print(f"    SELECT id, user, state FROM information_schema.processlist")
        print(f"    WHERE state LIKE '%lock%' ORDER BY time DESC;")
        sys.exit(1)
    finally:
        cur.close()
        conn.close()


# ── Setup ──────────────────────────────────────────────────────────────

def setup_tables():
    ensure_std_columns()

    conn = get_connection()
    cur  = conn.cursor()

    # ── 0. Indexes on target table ────────────────────────────────────
    print("  Checking indexes...", flush=True)
    tgt_schema, tgt_table = TARGET_TABLE.split(".", 1)
    for schema, table, col in [
        (tgt_schema,  tgt_table,           BATCH_KEY),
        (tgt_schema,  tgt_table,           "problem_desc"),
        ("semantics", "comorbidity_llm_map","problem_desc"),
    ]:
        print(f"    {schema}.{table} ({col})...", end=" ", flush=True)
        if not _index_exists(cur, schema, table, col):
            print("missing — creating...", flush=True)
            try:
                # TEXT columns need a prefix length
                col_spec = f"{col}(100)" if col == "problem_desc" else col
                cur.execute(f"CREATE INDEX idx_{col} ON `{schema}`.`{table}` ({col_spec})")
                conn.commit()
                print("      done")
            except Exception as exc:
                print(f"      warning: {exc}")
        else:
            print("exists")

    # ── 1. Materialize semantics.comorbidity_llm_map ──────────────────
    print("  Materializing semantics.comorbidity_llm_map...", flush=True)
    if not _table_exists(cur, STAGING_LLM_MAP):
        cur.execute(f"""
            CREATE TABLE {STAGING_LLM_MAP} AS
            SELECT problem_desc,
                   Charlson   AS charlson_comorbidity,
                   Elixhauser AS elixhauser_comorbidity
            FROM semantics.comorbidity_llm_map
        """)
        cur.execute(f"ALTER TABLE {STAGING_LLM_MAP} ADD INDEX idx_desc (problem_desc(100))")
        conn.commit()
        cur.execute(f"SELECT COUNT(*) FROM {STAGING_LLM_MAP}")
        print(f"    {cur.fetchone()[0]:,} LLM map rows")
    else:
        cur.execute(f"SELECT COUNT(*) FROM {STAGING_LLM_MAP}")
        print(f"    already exists, reusing  ({cur.fetchone()[0]:,} rows)")

    # ── 2. Unique problem_desc values from eligible rows ──────────────
    # Extract distinct descs FIRST so the LIKE join in step 3 scans
    # U unique values × 2,626 patterns instead of N total rows × 2,626.
    print("  Collecting unique problem_desc values...", flush=True)
    if not _table_exists(cur, STAGING_UNIQUE_DESCS):
        cur.execute(f"""
            CREATE TABLE {STAGING_UNIQUE_DESCS} AS
            SELECT DISTINCT problem_desc
            FROM {TARGET_TABLE}
            WHERE problem_desc IS NOT NULL
              AND TRIM(problem_desc) != ''
              AND (charlson_comorbidity IS NULL OR elixhauser_comorbidity IS NULL)
        """)
        cur.execute(f"ALTER TABLE {STAGING_UNIQUE_DESCS} ADD INDEX idx_desc (problem_desc(100))")
        conn.commit()
        cur.execute(f"SELECT COUNT(*) FROM {STAGING_UNIQUE_DESCS}")
        u = cur.fetchone()[0]
        print(f"    {u:,} unique descriptions")
    else:
        cur.execute(f"SELECT COUNT(*) FROM {STAGING_UNIQUE_DESCS}")
        u = cur.fetchone()[0]
        print(f"    already exists, reusing  ({u:,} unique descriptions)")

    if u == 0:
        print("  No eligible rows — all comorbidities already populated.")
        cur.close()
        conn.close()
        return []

    # ── 3. LIKE join on unique descs only (U × 2,626 vs N × 2,626) ───
    # This is the expensive step but runs once and only on distinct values.
    # Result is problem_desc → charlson/elixhauser; batch UPDATEs then use
    # a fast prefix(100) equality join on problem_desc — no LIKE per batch.
    print("  Building desc→comorbidity map (LIKE join on unique descs)...", flush=True)
    cur.execute(f"SELECT COUNT(*) FROM {STAGING_LLM_MAP}")
    pattern_count = cur.fetchone()[0]
    print(f"    {u:,} unique descs × {pattern_count:,} patterns = {u * pattern_count:,} comparisons")
    if not _table_exists(cur, STAGING_DESC_MAP):
        print("    running...", flush=True)
        cur.execute(f"""
            CREATE TABLE {STAGING_DESC_MAP} AS
            SELECT ud.problem_desc,
                   MAX(cm.charlson_comorbidity)   AS charlson_comorbidity,
                   MAX(cm.elixhauser_comorbidity) AS elixhauser_comorbidity
            FROM {STAGING_UNIQUE_DESCS} ud
            LEFT JOIN {STAGING_LLM_MAP} cm
                ON ud.problem_desc LIKE cm.problem_desc
            GROUP BY ud.problem_desc
        """)
        cur.execute(f"ALTER TABLE {STAGING_DESC_MAP} ADD INDEX idx_desc (problem_desc(100))")
        conn.commit()
        cur.execute(f"SELECT COUNT(*) FROM {STAGING_DESC_MAP}")
        n = cur.fetchone()[0]
        print(f"    {n:,} descriptions mapped")
    else:
        cur.execute(f"SELECT COUNT(*) FROM {STAGING_DESC_MAP}")
        n = cur.fetchone()[0]
        print(f"    already exists, reusing  ({n:,} descriptions)")

    # ── 4. PK staging — eligible ndids for batch ranges ───────────────
    print("  Creating PK staging...", flush=True)
    if not _table_exists(cur, STAGING_PK):
        cur.execute(f"""
            CREATE TABLE {STAGING_PK} AS
            SELECT {BATCH_KEY}
            FROM {TARGET_TABLE}
            WHERE problem_desc IS NOT NULL
              AND TRIM(problem_desc) != ''
              AND (charlson_comorbidity IS NULL OR elixhauser_comorbidity IS NULL)
              AND {BATCH_KEY} IS NOT NULL
            ORDER BY {BATCH_KEY}
        """)
        cur.execute(f"ALTER TABLE {STAGING_PK} ADD INDEX idx_pk ({BATCH_KEY})")
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")

    ranges, total = _build_ranges(cur, STAGING_PK)
    print(f"    {total:,} rows → {len(ranges)} batches of {BATCH_SIZE:,}")

    # ── 4. Checkpoint table ───────────────────────────────────────────
    print("  Creating checkpoint table...", flush=True)
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {CHECKPOINT_TABLE} (
            source_key   VARCHAR(200) NOT NULL PRIMARY KEY,
            status       ENUM('running','done','failed') NOT NULL DEFAULT 'running',
            rows_updated BIGINT      DEFAULT 0,
            started_at   DATETIME    DEFAULT NULL,
            completed_at DATETIME    DEFAULT NULL,
            error_msg    TEXT        DEFAULT NULL
        )
    """)
    conn.commit()
    print("    ready")

    cur.close()
    conn.close()
    return ranges


# ── Runner ─────────────────────────────────────────────────────────────

_LOCK_TIMEOUT_ERRNO = 1205   # InnoDB: Lock wait timeout exceeded
_LOCK_RETRIES       = 5
_LOCK_RETRY_SLEEP   = 15    # seconds between retries


def _execute_with_retry(conn, cur, sql):
    """Execute one UPDATE, retrying up to _LOCK_RETRIES times on lock timeouts."""
    for attempt in range(_LOCK_RETRIES):
        try:
            cur.execute(sql)
            return
        except pymysql.err.OperationalError as exc:
            if exc.args[0] == _LOCK_TIMEOUT_ERRNO and attempt < _LOCK_RETRIES - 1:
                pbar_msg = f"\n  Lock timeout — retry {attempt + 1}/{_LOCK_RETRIES - 1} in {_LOCK_RETRY_SLEEP}s..."
                print(pbar_msg, flush=True)
                time.sleep(_LOCK_RETRY_SLEEP)
                conn.ping(reconnect=True)
                cur = conn.cursor()
                cur.execute("SET unique_checks = 0")
                cur.execute("SET foreign_key_checks = 0")
            else:
                raise


def run_pass(ranges, pbar):
    conn = get_connection()

    if is_done(conn):
        conn.close()
        pbar.update(len(ranges))
        return {"status": "skipped", "rows": 0, "secs": 0}

    mark(conn, "running")
    t0 = time.time()
    total_rows = 0

    try:
        cur = conn.cursor()
        cur.execute("SET unique_checks = 0")
        cur.execute("SET foreign_key_checks = 0")

        for pk_lo, pk_hi in ranges:
            _execute_with_retry(conn, cur, build_batch_update(pk_lo, pk_hi))
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
    print(f"  Problemlist Comorbidity LLM UPDATE — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  target      : {TARGET_TABLE}")
    print(f"  llm_map     : semantics.comorbidity_llm_map")
    print(f"  batch_key   : {BATCH_KEY}")
    print(f"  batch_size  : {BATCH_SIZE:,}")
    print(f"  desc_map    : {STAGING_DESC_MAP}")
    print(f"  checkpoint  : {CHECKPOINT_TABLE}")
    print(f"{'='*70}\n", flush=True)

    ranges = setup_tables()

    if not ranges:
        print("Nothing to process. Exiting.")
        return

    print(f"\n  Starting comorbidity UPDATE ({len(ranges)} batches)...", flush=True)
    with tqdm(total=len(ranges), desc="Comorbidity", unit="batch") as pbar:
        result = run_pass(ranges, pbar)

    print()
    status = result["status"]
    tag    = "DONE" if status == "done" else "SKIP" if status == "skipped" else "FAIL"
    print(f"  [{tag}]  {result['rows']:>10,} rows updated  ({result['secs']}s)")

    print(f"\n{'='*70}")
    print(f"  Status : {status}")
    print(f"  Rows   : {result['rows']:,}")
    print(f"{'='*70}")

    print(f"\n  Cleanup SQL (run after verifying data):")
    print(f"    -- Shared (keep if other scripts use it):")
    print(f"    -- DROP TABLE IF EXISTS {STAGING_LLM_MAP};")
    print(f"    -- Per-run:")
    print(f"    DROP TABLE IF EXISTS {STAGING_UNIQUE_DESCS};")
    print(f"    DROP TABLE IF EXISTS {STAGING_DESC_MAP};")
    print(f"    DROP TABLE IF EXISTS {STAGING_PK};")
    print(f"    DROP TABLE IF EXISTS {CHECKPOINT_TABLE};")

    if status.startswith("FAILED"):
        print(f"\n  Error: {status}")
        sys.exit(1)


if __name__ == "__main__":
    main()
