import pandas as pd
import os
import csv
import sys
import time
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from sqlalchemy import inspect, create_engine, text
from datetime import datetime

from config import CSV_DELIMITER, DB_CONFIG, TABLE_LIST, TABLE_KEYSET_COLUMNS, LOG_FILE, LOG_FORMAT

# #region agent log
_DEBUG_LOG_PATH = os.path.join(os.path.dirname(__file__), ".cursor", "debug-5d27dc.log")
def _debug_log(message, data=None, hypothesis_id=None, run_id=None):
    try:
        payload = {"sessionId": "5d27dc", "location": "qc_checker.py", "message": message, "timestamp": int(time.time() * 1000)}
        if data is not None:
            payload["data"] = data
        if hypothesis_id is not None:
            payload["hypothesisId"] = hypothesis_id
        if run_id is not None:
            payload["runId"] = run_id
        with open(_DEBUG_LOG_PATH, "a", encoding="utf-8") as f:
            f.write(json.dumps(payload) + "\n")
    except Exception:
        pass
# #endregion

# Log file handle and init (set on first log_message call)
_log_file_handle = None


def _ensure_log_file():
    """Create log directory and open log file for appending."""
    global _log_file_handle
    if _log_file_handle is not None:
        return
    log_dir = os.path.dirname(LOG_FILE)
    if log_dir:
        os.makedirs(log_dir, exist_ok=True)
    _log_file_handle = open(LOG_FILE, "a", encoding="utf-8")


def log_message(msg, level="info"):
    """Log to console and to LOG_FILE (creates file and directory if needed)."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    prefix = f"[{level.upper()}]"
    line = f"[{timestamp}] {prefix} {msg}"
    print(line)
    try:
        _ensure_log_file()
        _log_file_handle.write(line + "\n")
        _log_file_handle.flush()
    except Exception:
        pass  # Don't break if file write fails


def get_database_engine(db_config=None):
    """Create database engine from config or provided db_config."""
    cfg = db_config if db_config is not None else DB_CONFIG
    if not all(cfg.values()):
        raise ValueError("Database configuration is incomplete. Check your .env file.")
    password = "ndSID%402025" 
    conn_str = (
        f"mysql+pymysql://{cfg['user']}:{password}@"
        f"{cfg['host']}:{cfg['port']}/{cfg['database']}"
    )
    return create_engine(conn_str)


def clean_text_for_csv(text):
    """Clean text to be CSV-safe by replacing newlines and other problematic characters"""
    if pd.isna(text) or text is None:
        return None
    return str(text).replace('\n', ' ').replace('\r', ' ').replace('"', '""')


# Default chunk size for reading large tables (larger = fewer round-trips, more memory per chunk)
DEFAULT_CHUNK_SIZE = 50_000


def _get_pk_column(engine, table_name):
    """Return the first primary key column name for the table, or None if no single-column PK."""
    try:
        inspector = inspect(engine)
        pk = inspector.get_pk_constraint(table_name)
        constrained = pk.get("constrained_columns") if pk else []
        return constrained[0] if len(constrained) == 1 else None
    except Exception:
        return None


def _get_keyset_column(engine, table_name):
    """Return a column suitable for keyset pagination: config override, single-column PK, or 'id' if present."""
    try:
        inspector = inspect(engine)
        columns = [c["name"] for c in inspector.get_columns(table_name)]
    except Exception:
        return None
    # 1) Config override (e.g. TABLE_KEYSET_COLUMNS = {"allergies": "allergy_id"})
    if table_name in TABLE_KEYSET_COLUMNS:
        col = TABLE_KEYSET_COLUMNS[table_name]
        if col in columns:
            return col
    # 2) Single-column primary key
    pk_col = _get_pk_column(engine, table_name)
    if pk_col:
        return pk_col
    # 3) Fallback: column named "id"
    if "id" in columns:
        return "id"
    # 4) Heuristic: table "allergies" -> "allergy_id", "notes" -> "note_id"
    if table_name.endswith("s") and len(table_name) > 1:
        candidate = table_name[:-1] + "_id"
        if candidate in columns:
            return candidate
    return None


def generate_qc_stats(df, table_name, engine=None):
   
    stats_list = []
    
    try:
        # Get database column metadata if engine is provided
        db_columns = {}
        if engine:
            try:
                inspector = inspect(engine)
                db_columns = {col["name"]: str(col["type"]) for col in inspector.get_columns(table_name)}
            except Exception as e:
                log_message(f"Could not fetch DB metadata for {table_name}: {e}", "warning")
        
        # Process each column in the DataFrame
        for col_name in df.columns:
            py_type = str(df[col_name].dtype)
            unique_count = df[col_name].nunique(dropna=True)
            
            # Calculate fill rate (percentage of non-null values)
            null_count = df[col_name].isna().sum()
            fill_rate = round(((len(df) - null_count) / len(df)) * 100, 2)
            
            # Get top 10 most common values
            top_values = df[col_name].value_counts().head(10)
            top_n_values = "; ".join([f"{clean_text_for_csv(val)}({count})" for val, count in top_values.items()])
            
            # Get bottom 10 least common values (excluding nulls)
            bottom_values = df[col_name].value_counts().tail(10)
            bottom_n_values = "; ".join([f"{clean_text_for_csv(val)}({count})" for val, count in bottom_values.items()])
            
            # Calculate min and max length for string/object columns
            min_length = None
            max_length = None
            if df[col_name].dtype == 'object':
                non_null_series = df[col_name].dropna().astype(str)
                if len(non_null_series) > 0:
                    lengths = non_null_series.str.len()
                    min_length = int(lengths.min())
                    max_length = int(lengths.max())

            # Build stats record
            stats_record = {
                "table_name": table_name,
                "column_name": col_name,
                "python_datatype": py_type,
                "row_count": len(df),
                "unique_count": unique_count,
                "null_count": int(null_count),
                "fill_rate_pct": fill_rate,
                "top_10_values": top_n_values if top_n_values else None,
                "bottom_10_values": bottom_n_values if bottom_n_values else None,
                "min_length": min_length,
                "max_length": max_length
            }
            
            # Add database datatype if available
            if engine and col_name in db_columns:
                stats_record["db_datatype"] = db_columns[col_name]
            
            stats_list.append(stats_record)

        log_message(f"Stats generated for {table_name}", "success")

    except Exception as e:
        log_message(f"Error generating stats for {table_name}: {e}", "error")
    
    return stats_list


def _fetch_table_chunked(engine, table_name, chunk_size, sample_size=None, log_prefix=""):
    """Yield DataFrames from table: either one chunk (if sample_size) or chunks of chunk_size.
    Uses keyset pagination (WHERE pk > :last ORDER BY pk LIMIT N) when a keyset column is available
    to avoid a single long-running full-table query whose first batch is very slow."""
    # #region agent log
    _t_fetch_start = time.perf_counter()
    # #endregion
    if sample_size:
        log_message(f"{log_prefix}Reading sample: LIMIT {sample_size}", "info")
        query = f"SELECT * FROM `{table_name}` LIMIT {sample_size}"
        chunk = pd.read_sql(query, engine)
        # #region agent log
        _debug_log("fetch_sample_done", {"table": table_name, "rows": len(chunk), "duration_sec": round(time.perf_counter() - _t_fetch_start, 3)}, "H1")
        # #endregion
        if len(chunk) > 0:
            log_message(f"{log_prefix}Read 1 chunk (sample): {len(chunk):,} rows", "info")
            yield chunk
        return

    pk_col = _get_keyset_column(engine, table_name)
    if pk_col:
        log_message(f"{log_prefix}Reading table in chunks of {chunk_size:,} rows (keyset pagination on `{pk_col}`)", "info")
        last_val = None
        i = 0
        with engine.connect() as conn:
            while True:
                i += 1
                if last_val is None:
                    query = text(f"SELECT * FROM `{table_name}` ORDER BY `{pk_col}` LIMIT :lim")
                    chunk = pd.read_sql(query, conn, params={"lim": chunk_size})
                else:
                    query = text(f"SELECT * FROM `{table_name}` WHERE `{pk_col}` > :last ORDER BY `{pk_col}` LIMIT :lim")
                    chunk = pd.read_sql(query, conn, params={"last": last_val, "lim": chunk_size})
                log_message(f"{log_prefix}Reading chunk {i}...", "info")
                log_message(f"{log_prefix}Chunk {i} read: {len(chunk):,} rows (chunks read so far: {i})", "info")
                # #region agent log
                _debug_log("fetch_chunk_done", {"table": table_name, "chunk_num": i, "chunk_rows": len(chunk), "cumulative_sec": round(time.perf_counter() - _t_fetch_start, 3)}, "H1")
                # #endregion
                if chunk.empty:
                    break
                yield chunk
                last_val = chunk[pk_col].iloc[-1]
        return

    query = f"SELECT * FROM `{table_name}`"
    log_message(f"{log_prefix}Reading table in chunks of {chunk_size:,} rows (single-query stream)", "info")
    for i, chunk in enumerate(pd.read_sql(query, engine, chunksize=chunk_size), start=1):
        log_message(f"{log_prefix}Reading chunk {i}...", "info")
        log_message(f"{log_prefix}Chunk {i} read: {len(chunk):,} rows (chunks read so far: {i})", "info")
        # #region agent log
        _debug_log("fetch_chunk_done", {"table": table_name, "chunk_num": i, "chunk_rows": len(chunk), "cumulative_sec": round(time.perf_counter() - _t_fetch_start, 3)}, "H1")
        # #endregion
        yield chunk


def _process_single_table(engine, table_name, chunk_size, sample_size, log_prefix=""):
    """
    Read table from MySQL in chunks, append into one DataFrame, then run QC once on the whole table.
    """
    # #region agent log
    _t_table_start = time.perf_counter()
    _debug_log("table_start", {"table": table_name}, "H2")
    # #endregion
    try:
        log_message(f"{log_prefix}Starting table: {table_name}", "info")

        chunks = []
        for chunk in _fetch_table_chunked(engine, table_name, chunk_size, sample_size, log_prefix):
            chunks.append(chunk)
            total_rows = sum(len(c) for c in chunks)
            log_message(f"{log_prefix}Appended chunk {len(chunks)}: {len(chunk):,} rows. Chunks read: {len(chunks)}, total rows in memory: {total_rows:,}", "info")

        # #region agent log
        _t_fetch_done = time.perf_counter()
        _fetch_sec = _t_fetch_done - _t_table_start
        _total_rows = sum(len(c) for c in chunks)
        _debug_log("fetch_all_done", {"table": table_name, "fetch_sec": round(_fetch_sec, 3), "chunks": len(chunks), "total_rows": _total_rows}, "H2")
        # #endregion

        if not chunks:
            log_message(f"{log_prefix}No data for {table_name}", "warning")
            return []

        log_message(f"{log_prefix}Concatenating {len(chunks)} chunk(s) into one DataFrame...", "info")
        df = pd.concat(chunks, axis=0, ignore_index=True)
        # #region agent log
        _t_concat_done = time.perf_counter()
        _debug_log("concat_done", {"table": table_name, "concat_sec": round(_t_concat_done - _t_fetch_done, 3), "rows": len(df)}, "H4")
        # #endregion
        log_message(f"{log_prefix}Whole table loaded: {len(df):,} rows, {len(df.columns)} columns", "success")

        log_message(f"{log_prefix}Running QC checks on whole table...", "info")
        stats_list = generate_qc_stats(df, table_name, engine)
        # #region agent log
        _t_qc_done = time.perf_counter()
        _qc_sec = _t_qc_done - _t_concat_done
        _total_sec = _t_qc_done - _t_table_start
        _debug_log("qc_done", {"table": table_name, "qc_sec": round(_qc_sec, 3), "total_sec": round(_total_sec, 3), "fetch_pct": round(100 * _fetch_sec / _total_sec, 1) if _total_sec else 0, "qc_pct": round(100 * _qc_sec / _total_sec, 1) if _total_sec else 0}, "H3")
        _debug_log("table_done_breakdown", {"table": table_name, "fetch_sec": round(_fetch_sec, 3), "concat_sec": round(_t_concat_done - _t_fetch_done, 3), "qc_sec": round(_qc_sec, 3), "total_sec": round(_total_sec, 3)}, "H5")
        # #endregion
        log_message(f"{log_prefix}Stats generated for {table_name}: {len(stats_list)} columns", "success")
        return stats_list
    except Exception as e:
        log_message(f"{log_prefix}Error processing {table_name}: {e}", "error")
        raise


def run_qc_checks(table_names, output_file=None, db_config=None, sample_size=None, chunk_size=None, max_workers=8):
    """
    Run QC checks: fetch each table from MySQL in chunks, append into one DataFrame per table,
    then run generate_qc_stats once on the whole DF. Multiple tables are processed in parallel.
    """
    log_message("=" * 60)
    log_message("Starting QC Checks (MySQL: chunked read → one DF per table → QC once)")
    log_message("=" * 60)

    chunk_size = chunk_size or DEFAULT_CHUNK_SIZE
    required_keys = ["host", "port", "database", "user", "password"]
    cfg = db_config if db_config is not None else DB_CONFIG
    if not all(k in cfg for k in required_keys):
        log_message(f"Database configuration incomplete. Required keys: {required_keys}", "error")
        return None

    log_message("Step 1: Creating database engine (get_database_engine)...", "info")
    try:
        engine = get_database_engine(db_config)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        log_message("Engine created and connection test OK", "success")
        log_message(f"   Database: {cfg['database']} @ {cfg['host']}:{cfg['port']}", "info")
    except Exception as e:
        log_message(f"Could not connect to database: {e}", "error")
        return None

    log_message(f"Step 2: Processing {len(table_names)} table(s): {', '.join(table_names)}", "info")
    if sample_size:
        log_message(f"   Sample size: {sample_size} rows per table", "warning")
    log_message(f"   Chunk size: {chunk_size:,} | Max parallel tables: {max_workers}", "info")

    all_stats = []
    if len(table_names) == 1:
        log_message("Single table: running in main thread", "info")
        table_stats = _process_single_table(engine, table_names[0], chunk_size, sample_size, log_prefix="   ")
        all_stats.extend(table_stats)
    else:
        log_message(f"Multiple tables: running up to {max_workers} threads", "info")
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_table = {
                executor.submit(_process_single_table, engine, t, chunk_size, sample_size, log_prefix=f"   [{t}] "): t
                for t in table_names
            }
            for future in as_completed(future_to_table):
                table_name = future_to_table[future]
                try:
                    table_stats = future.result()
                    all_stats.extend(table_stats)
                    log_message(f"   Completed table: {table_name}", "success")
                except Exception as e:
                    log_message(f"   Table {table_name} failed: {e}", "error")

    if not all_stats:
        log_message("No stats generated", "error")
        return None

    stats_df = pd.DataFrame(all_stats)
    if output_file is None:
        output_file = f"qc_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

    log_message("Step 3: Writing results to CSV...", "info")
    try:
        stats_df.to_csv(output_file, sep=CSV_DELIMITER, index=False, quoting=csv.QUOTE_ALL)
        log_message(f"Results saved to: {output_file}", "success")
        log_message(f"   Total columns: {len(all_stats)} | Tables: {len(set(s['table_name'] for s in all_stats))}", "info")
    except Exception as e:
        log_message(f"Error saving results: {e}", "error")

    log_message("=" * 60)
    log_message("QC Checks Completed", "success")
    log_message("=" * 60)
    return stats_df


def main():
    """Main function for command-line usage"""
    args = sys.argv[1:]
    
    # Parse --sample N
    sample_size = None
    if "--sample" in args:
        idx = args.index("--sample")
        if idx + 1 < len(args) and args[idx + 1].isdigit():
            sample_size = int(args[idx + 1])
            args = args[:idx] + args[idx + 2:]
        else:
            print("Error: --sample requires a number (e.g. --sample 1000)")
            sys.exit(1)
    
    # If table names passed via CLI, use those; otherwise use TABLE_LIST from config
    if args:
        table_names = args
        log_message(f"Using table(s) from CLI: {table_names}", "info")
    else:
        if not TABLE_LIST:
            print("\nUsage: python qc_checker.py [table_name1] [table_name2] ... [--sample N]")
            print("\n  With no arguments: runs QC on all tables in config.TABLE_LIST")
            print("  With arguments: runs QC on the given table(s) only")
            print("  --sample N: fetch only N rows per table (for testing)")
            print("\nExample (single table):")
            print("  python qc_checker.py allergies")
            print("\nExample (with sample):")
            print("  python qc_checker.py allergies --sample 1000")
            sys.exit(1)
        table_names = TABLE_LIST
        log_message(f"Using table(s) from config.TABLE_LIST: {table_names}", "info")
    
    if sample_size:
        log_message(f"Sample size: {sample_size} rows per table", "info")
    
    # Run QC checks (will fetch data from MySQL)
    run_qc_checks(table_names, sample_size=sample_size)


if __name__ == "__main__":
    main()
