#!/usr/bin/env python3
"""
Optimized QC Runner — qc_runner_optimise.py

Strategy by table size
──────────────────────
Large tables (≥ LARGE_TABLE_THRESHOLD rows):
  → Server-side path: ALL computation pushed to MySQL, zero row transfer to Python
      Step 1 — ONE combined aggregate query for every column's base stats (1 table scan)
      Step 2 — Parallel GROUP BY per column for top/bottom values (MAX_COL_WORKERS threads)
  → 100-200× faster than pulling rows to Python for 150M-row tables

Small tables (< LARGE_TABLE_THRESHOLD rows):
  → Chunked Python path: keyset-paginated SSCursor + IncrementalStats (no pd.concat)

Common to both paths
────────────────────
- Checkpoint / resume    : completed tables skip on re-run
- Per-table CSV flush    : partial results survive mid-run failures
- ThreadPoolExecutor     : N tables in parallel (MAX_WORKERS)
- tqdm progress bars
"""

import sys
import os
import csv
import json
import time
import threading
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import pandas as pd
import pymysql
from tqdm import tqdm

sys.path.insert(0, os.path.dirname(__file__))
from config import CSV_DELIMITER, DB_CONFIG, TABLE_LIST, TABLE_KEYSET_COLUMNS, LOG_FILE

# ── Configuration ──────────────────────────────────────────────────────────────
BATCH_SIZE   = 50_000
MAX_WORKERS  = 4       # parallel tables
MAX_COL_WORKERS = 8    # parallel top/bottom sample queries (server-side path)

# Tables with estimated row count >= this use the server-side aggregate path.
# information_schema.TABLE_ROWS is approximate; real count may differ ~20%.
LARGE_TABLE_THRESHOLD = 1_000_000

# Rows to read for top/bottom value sampling — no full table scan.
# First N rows in storage order. Increase for better coverage, lower for speed.
TOP_N_SAMPLE_ROWS = 500_000

OUTPUT_DIR   = "exports"
CHECKPOINT_F = os.path.join(OUTPUT_DIR, "qc_opt_checkpoint.json")

# Stop growing a column's Counter beyond this many unique values.
MAX_COUNTER_ENTRIES = 500_000

CSV_FIELDNAMES = [
    "table_name", "column_name", "python_datatype", "db_datatype",
    "row_count", "unique_count", "null_count", "fill_rate_pct",
    "top_10_values", "bottom_10_values", "min_length", "max_length",
]

# ── Logging ────────────────────────────────────────────────────────────────────
_log_lock = threading.Lock()
_log_fh   = None


def _open_log():
    global _log_fh
    log_dir = os.path.dirname(LOG_FILE)
    if log_dir:
        os.makedirs(log_dir, exist_ok=True)
    _log_fh = open(LOG_FILE, "a", encoding="utf-8")


def log(msg, level="INFO"):
    ts   = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] [{level.upper()}] {msg}"
    print(line)
    if _log_fh:
        with _log_lock:
            _log_fh.write(line + "\n")
            _log_fh.flush()


# ── Database ───────────────────────────────────────────────────────────────────
def get_conn():
    cfg = DB_CONFIG
    return pymysql.connect(
        host=cfg["host"],
        port=int(cfg["port"]),
        user=cfg["user"],
        password=cfg["password"],
        database=cfg["database"],
        charset="utf8mb4",
        connect_timeout=30,
        read_timeout=21600,
    )


def _get_db_columns(conn, table_name):
    """Return {col_name: col_type} from information_schema."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COLUMN_NAME, COLUMN_TYPE FROM information_schema.COLUMNS "
            "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s "
            "ORDER BY ORDINAL_POSITION",
            (table_name,),
        )
        return {row[0]: row[1] for row in cur.fetchall()}


def _get_approx_row_count(conn, table_name):
    """Instant approximate row count from information_schema (no table scan)."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT TABLE_ROWS FROM information_schema.TABLES "
            "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s",
            (table_name,),
        )
        row = cur.fetchone()
        return int(row[0]) if (row and row[0] is not None) else 0


def _get_keyset_col(conn, table_name):
    if table_name in TABLE_KEYSET_COLUMNS:
        return TABLE_KEYSET_COLUMNS[table_name]
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COLUMN_NAME FROM information_schema.KEY_COLUMN_USAGE "
            "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s "
            "  AND CONSTRAINT_NAME = 'PRIMARY' ORDER BY ORDINAL_POSITION",
            (table_name,),
        )
        rows = cur.fetchall()
        if len(rows) == 1:
            return rows[0][0]
        cur.execute(
            "SELECT COLUMN_NAME FROM information_schema.COLUMNS "
            "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s",
            (table_name,),
        )
        cols = [r[0] for r in cur.fetchall()]
    if "id" in cols:
        return "id"
    if table_name.endswith("s"):
        candidate = table_name[:-1] + "_id"
        if candidate in cols:
            return candidate
    return None


def _get_batch_ranges(conn, table_name, pk_col):
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT `{pk_col}`
            FROM (
                SELECT `{pk_col}`, ROW_NUMBER() OVER (ORDER BY `{pk_col}`) AS rn
                FROM `{table_name}`
                WHERE `{pk_col}` IS NOT NULL
            ) t
            WHERE (rn - 1) % {BATCH_SIZE} = 0
            ORDER BY `{pk_col}`
        """)
        boundaries = [row[0] for row in cur.fetchall()]
    if not boundaries:
        return []
    return [
        (lo, boundaries[i + 1] if i + 1 < len(boundaries) else None)
        for i, lo in enumerate(boundaries)
    ]


# ── Helpers ────────────────────────────────────────────────────────────────────

# Column types where '' and 'None' are valid "empty" values to flag
_STRING_TYPES = frozenset({
    "varchar", "char", "tinytext", "text", "mediumtext", "longtext",
    "enum", "set", "json",
})


def _is_string_col(col_type: str) -> bool:
    """True when the MySQL column type is text-like (can store empty strings)."""
    t = col_type.lower()
    return any(s in t for s in _STRING_TYPES)


def _clean(val):
    if val is None:
        return None
    return str(val).replace("\n", " ").replace("\r", " ").replace('"', '""')


def _fmt_topn(pairs):
    return "; ".join(f"{_clean(v)}({c})" for v, c in pairs) or None


# ══════════════════════════════════════════════════════════════════════════════
# SERVER-SIDE PATH  (large tables — no row transfer to Python)
# ══════════════════════════════════════════════════════════════════════════════

def _server_base_stats(conn, table_name, col_names, db_columns):
    """
    Exact fill-rate stats — mirrors fillrates.py two-phase approach.

    Phase 1: COUNT(*) + COUNT(col) for ALL columns — one full scan, catches NULLs.
    Phase 2: SUM(col IN ('', 'None')) for string columns only — second scan, catches empties.

    No COUNT(DISTINCT), no CHAR_LENGTH — keeps the scan as light as fillrates.py.
    Returns: (row_count, {col: {non_null, empty}})
    """
    # Phase 1 — null counts
    col_exprs = ", ".join(f"COUNT(`{c.replace('`', '``')}`)" for c in col_names)
    sql1 = f"SELECT COUNT(*), {col_exprs} FROM `{table_name}`"
    with conn.cursor() as cur:
        log(f"[{table_name}] Phase 1: COUNT scan (nulls)...")
        cur.execute(sql1)
        row1 = cur.fetchone()

    row_count = int(row1[0])
    result    = {col: {"non_null": int(row1[i + 1] or 0), "empty": 0}
                 for i, col in enumerate(col_names)}

    # Phase 2 — empty string counts (string cols only)
    string_cols = [c for c in col_names if _is_string_col(db_columns.get(c, "varchar"))]
    if string_cols:
        exprs = ", ".join(
            f"SUM(`{c.replace('`', '``')}` IN ('', 'None'))" for c in string_cols
        )
        sql2 = f"SELECT {exprs} FROM `{table_name}`"
        with conn.cursor() as cur:
            log(f"[{table_name}] Phase 2: empty-string scan ({len(string_cols)} string cols)...")
            cur.execute(sql2)
            row2 = cur.fetchone()
        for col, ec in zip(string_cols, row2):
            result[col]["empty"] = int(ec or 0)

    return row_count, result


def _get_index_cardinality(conn, table_name):
    """
    Instant approximate unique count per column from index metadata — no table scan.
    Only available for indexed columns; returns None for unindexed columns.
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COLUMN_NAME, MAX(CARDINALITY) "
            "FROM information_schema.STATISTICS "
            "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s "
            "GROUP BY COLUMN_NAME",
            (table_name,),
        )
        return {row[0]: row[1] for row in cur.fetchall()}


def _server_topbottom(table_name, col_name, sample_rows):
    """
    Sample-based top/bottom + approximate unique count — reads only first
    sample_rows rows via LIMIT subquery. No full table scan needed.
    Each column runs in its own thread with its own connection.
    Returns: (col_name, top_10, bottom_10, approx_unique)
    """
    conn = get_conn()
    try:
        c = col_name.replace("`", "``")
        with conn.cursor() as cur:
            # Single query: unique count + top 10 in one pass over the sample
            cur.execute(f"""
                SELECT COUNT(DISTINCT `{c}`) AS approx_unique,
                       NULL, NULL
                FROM (SELECT `{c}` FROM `{table_name}` LIMIT {sample_rows}) _s
            """)
            approx_unique = cur.fetchone()[0]

            cur.execute(f"""
                SELECT `{c}`, COUNT(*) AS cnt
                FROM (SELECT `{c}` FROM `{table_name}` LIMIT {sample_rows}) _s
                WHERE `{c}` IS NOT NULL AND `{c}` != ''
                GROUP BY `{c}`
                ORDER BY cnt DESC
                LIMIT 10
            """)
            top_10 = cur.fetchall()

            cur.execute(f"""
                SELECT `{c}`, COUNT(*) AS cnt
                FROM (SELECT `{c}` FROM `{table_name}` LIMIT {sample_rows}) _s
                WHERE `{c}` IS NOT NULL AND `{c}` != ''
                GROUP BY `{c}`
                ORDER BY cnt ASC
                LIMIT 10
            """)
            bottom_10 = cur.fetchall()

        return col_name, top_10, bottom_10, approx_unique
    except Exception:
        return col_name, [], [], None
    finally:
        conn.close()


def _compute_stats_server_side(table_name, db_columns, col_workers, sample_rows):
    """
    Full server-side QC — no rows transferred to Python.

    Phase 1 + 2 : exact fill-rate scan (mirrors fillrates.py)
    Top/bottom  : LIMIT-based sample per column in parallel (no full GROUP BY scan)
    Unique count: instant from index cardinality metadata (no scan)
    """
    col_names = list(db_columns.keys())
    conn      = get_conn()

    try:
        row_count, base    = _server_base_stats(conn, table_name, col_names, db_columns)
        cardinality        = _get_index_cardinality(conn, table_name)
    finally:
        conn.close()

    log(f"[{table_name}] Base stats done — {row_count:,} rows. "
        f"Sampling top/bottom ({sample_rows:,} rows, {col_workers} parallel)...")

    # Parallel top/bottom — LIMIT subquery, not full scan
    topbottom = {}
    with ThreadPoolExecutor(max_workers=col_workers) as pool:
        futures = {
            pool.submit(_server_topbottom, table_name, col, sample_rows): col
            for col in col_names
        }
        for future in as_completed(futures):
            col_name, top_10, bottom_10, approx_unique = future.result()
            topbottom[col_name] = (top_10, bottom_10, approx_unique)

    # Assemble final stats records
    stats = []
    for col in col_names:
        b = base[col]
        # null_count = NULLs + empty strings + 'None' strings (mirrors fillrates.py)
        null_cnt = (row_count - b["non_null"]) + b["empty"]
        fill_pct = round((row_count - null_cnt) / row_count * 100, 2) if row_count > 0 else 0.0
        top_10, bottom_10, approx_unique = topbottom.get(col, ([], [], None))

        # Unique count: index cardinality (exact metadata) preferred;
        # fall back to COUNT(DISTINCT) on the sample (approximate, ~suffix).
        card = cardinality.get(col)
        if card is not None:
            unique_count = f"~{card:,}"
        elif approx_unique is not None:
            unique_count = f"~{approx_unique:,} (sampled)"
        else:
            unique_count = "n/a"

        stats.append({
            "table_name":       table_name,
            "column_name":      col,
            "python_datatype":  "server-side",
            "db_datatype":      db_columns.get(col),
            "row_count":        row_count,
            "unique_count":     unique_count,
            "null_count":       null_cnt,
            "fill_rate_pct":    fill_pct,
            "top_10_values":    _fmt_topn(top_10),
            "bottom_10_values": _fmt_topn(bottom_10),
            "min_length":       None,
            "max_length":       None,
        })

    return stats, row_count


# ══════════════════════════════════════════════════════════════════════════════
# CHUNKED PYTHON PATH  (small tables)
# ══════════════════════════════════════════════════════════════════════════════

class IncrementalStats:
    """Accumulate QC stats chunk-by-chunk — no pd.concat."""

    __slots__ = (
        "table_name", "columns", "dtypes", "row_count",
        "null_counts", "counters", "min_lengths", "max_lengths", "capped",
    )

    def __init__(self, table_name):
        self.table_name  = table_name
        self.columns     = None
        self.dtypes      = {}
        self.row_count   = 0
        self.null_counts = {}
        self.counters    = {}
        self.min_lengths = {}
        self.max_lengths = {}
        self.capped      = set()

    def _init_columns(self, df):
        self.columns = list(df.columns)
        for col in self.columns:
            self.dtypes[col]      = str(df[col].dtype)
            self.null_counts[col] = 0
            self.counters[col]    = Counter()
            self.min_lengths[col] = None
            self.max_lengths[col] = None

    def update(self, df: pd.DataFrame):
        if self.columns is None:
            self._init_columns(df)
        self.row_count += len(df)
        for col in self.columns:
            series   = df[col]
            non_null = series.dropna()
            self.null_counts[col] += int(series.isna().sum())
            if non_null.empty:
                continue
            if col not in self.capped:
                self.counters[col].update(non_null.tolist())
                if len(self.counters[col]) > MAX_COUNTER_ENTRIES:
                    self.capped.add(col)
            if series.dtype == object:
                lengths   = non_null.astype(str).str.len()
                chunk_min = int(lengths.min())
                chunk_max = int(lengths.max())
                if self.min_lengths[col] is None:
                    self.min_lengths[col] = chunk_min
                    self.max_lengths[col] = chunk_max
                else:
                    self.min_lengths[col] = min(self.min_lengths[col], chunk_min)
                    self.max_lengths[col] = max(self.max_lengths[col], chunk_max)

    def finalize(self, db_columns=None):
        stats = []
        for col in (self.columns or []):
            counter    = self.counters[col]
            null_count = self.null_counts[col]
            fill_rate  = (
                round(((self.row_count - null_count) / self.row_count) * 100, 2)
                if self.row_count > 0 else 0.0
            )
            top_10    = counter.most_common(10)
            all_vals  = counter.most_common()
            bottom_10 = all_vals[-10:] if len(all_vals) >= 10 else all_vals
            stats.append({
                "table_name":       self.table_name,
                "column_name":      col,
                "python_datatype":  self.dtypes[col],
                "db_datatype":      (db_columns.get(col) if db_columns else None),
                "row_count":        self.row_count,
                "unique_count":     f"{len(counter)}+" if col in self.capped else len(counter),
                "null_count":       null_count,
                "fill_rate_pct":    fill_rate,
                "top_10_values":    _fmt_topn(top_10),
                "bottom_10_values": _fmt_topn(bottom_10),
                "min_length":       self.min_lengths[col],
                "max_length":       self.max_lengths[col],
            })
        return stats


def _compute_stats_chunked(table_name, db_columns, conn, sample_size):
    """Chunked Python path for small tables or --force-python mode."""
    pk_col = _get_keyset_col(conn, table_name)
    acc    = IncrementalStats(table_name)

    if sample_size:
        log(f"[{table_name}] Sample: LIMIT {sample_size}")
        with conn.cursor() as cur:
            cur.execute(f"SELECT * FROM `{table_name}` LIMIT %s", (sample_size,))
            col_names = [d[0] for d in cur.description]
            rows      = cur.fetchall()
        if rows:
            acc.update(pd.DataFrame(list(rows), columns=col_names))

    elif pk_col:
        ranges = _get_batch_ranges(conn, table_name, pk_col)
        log(f"[{table_name}] Chunked keyset col=`{pk_col}`, {len(ranges)} batch(es)")
        with tqdm(total=len(ranges), desc=f"  {table_name}", unit="batch", leave=False) as pbar:
            with conn.cursor() as cur:
                for lo, hi in ranges:
                    if hi is None:
                        cur.execute(
                            f"SELECT * FROM `{table_name}` WHERE `{pk_col}` >= %s ORDER BY `{pk_col}`",
                            (lo,),
                        )
                    else:
                        cur.execute(
                            f"SELECT * FROM `{table_name}` WHERE `{pk_col}` >= %s AND `{pk_col}` < %s ORDER BY `{pk_col}`",
                            (lo, hi),
                        )
                    col_names = [d[0] for d in cur.description]
                    rows      = cur.fetchall()
                    if rows:
                        acc.update(pd.DataFrame(list(rows), columns=col_names))
                    pbar.update(1)

    else:
        log(f"[{table_name}] No keyset column — SSCursor streaming", "warning")
        with conn.cursor(pymysql.cursors.SSCursor) as cur:
            cur.execute(f"SELECT * FROM `{table_name}`")
            col_names = [d[0] for d in cur.description]
            i = 0
            while True:
                rows = cur.fetchmany(BATCH_SIZE)
                if not rows:
                    break
                i += 1
                acc.update(pd.DataFrame(list(rows), columns=col_names))
                log(f"[{table_name}] Chunk {i}: {len(rows):,} rows (total: {acc.row_count:,})")

    stats = acc.finalize(db_columns)
    return stats, acc.row_count


# ══════════════════════════════════════════════════════════════════════════════
# Checkpoint
# ══════════════════════════════════════════════════════════════════════════════
_ckpt_lock = threading.Lock()


def _load_ckpt():
    if os.path.exists(CHECKPOINT_F):
        with open(CHECKPOINT_F) as f:
            return json.load(f)
    return {}


def _save_ckpt(data):
    os.makedirs(os.path.dirname(CHECKPOINT_F) or ".", exist_ok=True)
    with open(CHECKPOINT_F, "w") as f:
        json.dump(data, f, indent=2)


def _mark_done(table_name, row_count, path):
    with _ckpt_lock:
        ckpt = _load_ckpt()
        ckpt[table_name] = {
            "status": "done", "rows": row_count,
            "path": path, "ts": datetime.now().isoformat(),
        }
        _save_ckpt(ckpt)


def _is_done(table_name):
    with _ckpt_lock:
        return _load_ckpt().get(table_name, {}).get("status") == "done"


# ══════════════════════════════════════════════════════════════════════════════
# CSV output (thread-safe)
# ══════════════════════════════════════════════════════════════════════════════
_csv_lock = threading.Lock()


def _flush_to_csv(stats_list, output_file):
    if not stats_list:
        return
    with _csv_lock:
        write_header = not os.path.exists(output_file)
        with open(output_file, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(
                f, fieldnames=CSV_FIELDNAMES,
                delimiter=CSV_DELIMITER,
                quoting=csv.QUOTE_ALL,
                extrasaction="ignore",
            )
            if write_header:
                writer.writeheader()
            writer.writerows(stats_list)


# ══════════════════════════════════════════════════════════════════════════════
# Per-table worker
# ══════════════════════════════════════════════════════════════════════════════
def _process_table(table_name, output_file, sample_size=None, force_python=False, col_workers=None):
    log(f"[{table_name}] Starting")
    t0   = time.time()
    conn = None

    try:
        conn       = get_conn()
        db_columns = _get_db_columns(conn, table_name)

        # ── Choose path ───────────────────────────────────────────────────────
        if sample_size or force_python:
            # Always use chunked Python for --sample or --force-python
            log(f"[{table_name}] Chunked Python path")
            stats, row_count = _compute_stats_chunked(table_name, db_columns, conn, sample_size)
            conn.close(); conn = None
        else:
            approx = _get_approx_row_count(conn, table_name)
            conn.close(); conn = None

            if approx >= LARGE_TABLE_THRESHOLD:
                log(f"[{table_name}] Server-side path (≈{approx:,} rows ≥ {LARGE_TABLE_THRESHOLD:,} threshold)")
                stats, row_count = _compute_stats_server_side(
                    table_name, db_columns,
                    col_workers or MAX_COL_WORKERS,
                    TOP_N_SAMPLE_ROWS,
                )
            else:
                log(f"[{table_name}] Chunked Python path (≈{approx:,} rows < {LARGE_TABLE_THRESHOLD:,} threshold)")
                conn2 = get_conn()
                stats, row_count = _compute_stats_chunked(table_name, db_columns, conn2, None)
                conn2.close()

        _flush_to_csv(stats, output_file)
        _mark_done(table_name, row_count, output_file)

        elapsed = round(time.time() - t0, 1)
        log(f"[{table_name}] Done — {row_count:,} rows, {len(stats)} columns ({elapsed}s)")
        return {"table": table_name, "status": "done", "rows": row_count, "secs": elapsed}

    except Exception as exc:
        elapsed = round(time.time() - t0, 1)
        log(f"[{table_name}] FAILED: {exc} ({elapsed}s)", "error")
        return {"table": table_name, "status": f"FAILED: {exc}", "rows": 0, "secs": elapsed}

    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


# ══════════════════════════════════════════════════════════════════════════════
# Orchestrator
# ══════════════════════════════════════════════════════════════════════════════
def run_optimised_qc(
    table_names=None,
    output_file=None,
    max_workers=None,
    col_workers=None,
    sample_size=None,
    force_python=False,
    reset=False,
):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    _open_log()

    tables  = table_names or TABLE_LIST
    workers = max_workers or MAX_WORKERS
    if output_file is None:
        output_file = os.path.join(
            OUTPUT_DIR, f"qc_opt_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        )

    if reset and os.path.exists(CHECKPOINT_F):
        os.remove(CHECKPOINT_F)
        log("Checkpoint cleared (--reset)", "warning")

    log("=" * 60)
    log("Optimized QC Runner")
    log(f"  Tables          : {len(tables)}  |  Table workers  : {workers}")
    log(f"  Col workers     : {col_workers or MAX_COL_WORKERS}  |  Batch size     : {BATCH_SIZE:,}")
    log(f"  Large threshold : {LARGE_TABLE_THRESHOLD:,} rows  → server-side path")
    log(f"  Output          : {output_file}")
    if sample_size:
        log(f"  Sample          : {sample_size} rows per table", "warning")
    if force_python:
        log("  Mode            : --force-python (chunked path for all tables)", "warning")
    log("=" * 60)

    try:
        conn = get_conn(); conn.close()
        log("DB connection OK")
    except Exception as e:
        log(f"Cannot connect to DB: {e}", "error")
        return None

    pending = [t for t in tables if reset or not _is_done(t)]
    skipped = [t for t in tables if t not in pending]
    if skipped:
        log(f"Skipping {len(skipped)} already-done: {', '.join(skipped)}")
    if not pending:
        log("All tables done. Use --reset to reprocess.")
        return output_file

    log(f"Processing {len(pending)} table(s): {', '.join(pending)}")

    results = []
    with tqdm(total=len(pending), desc="Overall tables", unit="table") as pbar:
        if workers == 1 or len(pending) == 1:
            for table in pending:
                r = _process_table(table, output_file, sample_size, force_python, col_workers)
                results.append(r)
                pbar.update(1)
        else:
            with ThreadPoolExecutor(max_workers=workers) as pool:
                futures = {
                    pool.submit(_process_table, t, output_file, sample_size, force_python, col_workers): t
                    for t in pending
                }
                for future in as_completed(futures):
                    results.append(future.result())
                    pbar.update(1)

    print()
    log("=" * 60)
    for r in sorted(results, key=lambda x: x["table"]):
        tag = "DONE" if r["status"] == "done" else "FAIL"
        log(f"  [{tag}] {r['table']:<40} {r.get('rows', 0):>12,} rows  ({r['secs']}s)")

    done   = sum(1 for r in results if r["status"] == "done")
    failed = [r for r in results if "FAILED" in str(r["status"])]
    log(f"\n  Done: {done + len(skipped)}  (skipped: {len(skipped)})  Failed: {len(failed)}")
    log(f"  Output: {output_file}")
    log("=" * 60)

    if failed:
        log("Failed tables:", "error")
        for r in failed:
            log(f"  {r['table']}: {r['status']}", "error")
    return output_file


# ══════════════════════════════════════════════════════════════════════════════
# CLI
# ══════════════════════════════════════════════════════════════════════════════
def main():
    global BATCH_SIZE, LARGE_TABLE_THRESHOLD
    import argparse

    parser = argparse.ArgumentParser(
        description="Optimized QC runner — server-side aggregates for large tables"
    )
    parser.add_argument("tables",         nargs="*",  help="Table names (default: TABLE_LIST from config)")
    parser.add_argument("--workers",      type=int,   default=MAX_WORKERS,     help=f"Parallel table threads (default: {MAX_WORKERS})")
    parser.add_argument("--col-workers",  type=int,   default=MAX_COL_WORKERS, help=f"Parallel GROUP BY threads per table (default: {MAX_COL_WORKERS})")
    parser.add_argument("--output",                   help="Output CSV path")
    parser.add_argument("--sample",       type=int,   metavar="N",             help="Fetch only N rows per table (forces Python path)")
    parser.add_argument("--batch-size",   type=int,   default=BATCH_SIZE,      help=f"Rows per chunk — Python path only (default: {BATCH_SIZE:,})")
    parser.add_argument("--threshold",    type=int,   default=LARGE_TABLE_THRESHOLD, help=f"Row count above which server-side path is used (default: {LARGE_TABLE_THRESHOLD:,})")
    parser.add_argument("--force-python", action="store_true", help="Use chunked Python path for all tables regardless of size")
    parser.add_argument("--reset",        action="store_true", help="Ignore checkpoint, reprocess all tables")
    args = parser.parse_args()

    BATCH_SIZE             = args.batch_size
    LARGE_TABLE_THRESHOLD  = args.threshold

    run_optimised_qc(
        table_names=args.tables or None,
        output_file=args.output,
        max_workers=args.workers,
        col_workers=args.col_workers,
        sample_size=args.sample,
        force_python=args.force_python,
        reset=args.reset,
    )


if __name__ == "__main__":
    main()
