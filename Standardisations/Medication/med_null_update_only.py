#!/usr/bin/env python3
"""
Standardisation UPDATE — null-only variant.

Identical to med_new_update.py EXCEPT:
  Pass 1 PK staging filters WHERE mapping_type IS NULL
  (skips rows already standardised in a previous run)

All lookup staging tables (FDB, med_code_norm, med_name_norm, med_strength_norm)
are REUSED from the v5 tables created by med_new_update.py — no rebuild needed.

Usage:
    python med_null_only_update.py
    nohup python med_null_only_update.py &
    tail -f med_null_only_*.log
"""

import logging
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import pymysql
from tqdm import tqdm
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())

# ── Configuration ─────────────────────────────────────────────────────
DB_CONFIG = {
    "host":            os.environ.get("DB_HOST"),
    "port":            3306,
    "user":            os.environ.get("DB_USER"),
    "password":        os.environ.get("DB_PASSWORD"),
    "database":        "rgd_udm_silver",
    "charset":         "utf8mb4",
    "connect_timeout": 30,
    "read_timeout":    21600,
    "write_timeout":   21600,
}

BATCH_SIZE  = 50_000
MAX_WORKERS = 8

TARGET_TABLE = "rgd_udm_silver.medication"
FDB_TABLE    = "FDB.RNDC14_NDC_MSTR"
BATCH_KEY    = "ndid"

_TABLE_SUFFIX = TARGET_TABLE.replace(".", "_").replace("-", "_")

# ── Reuse lookup staging from med_new_update.py (v5) ──────────────────
STAGING_FDB           = f"staging.med_fdb_ndc_mstr_v7_{_TABLE_SUFFIX}"
STAGING_MED_CODE_NORM = f"staging.med_code_norm_v7_{_TABLE_SUFFIX}"
STAGING_MED_NAME_NORM = f"staging.med_name_norm_v7_{_TABLE_SUFFIX}"
STAGING_MED_STR_NORM  = f"staging.med_strength_norm_v7_{_TABLE_SUFFIX}"

# ── New PK staging + checkpoint (null-only, won't conflict with v5) ───
STAGING_PK_P1    = f"staging.med_std_pk_null_p17_{_TABLE_SUFFIX}"
STAGING_PK_P2    = f"staging.med_std_pk_null_p27_{_TABLE_SUFFIX}"
CHECKPOINT_TABLE = f"staging.etl_checkpoint_med_std_null15_{_TABLE_SUFFIX}"


# ── Logging ───────────────────────────────────────────────────────────

logger = logging.getLogger("med_null_only")


def setup_logging():
    log_dir  = os.path.dirname(os.path.abspath(__file__))
    ts       = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(log_dir, f"med_null_only_{ts}.log")

    fmt = logging.Formatter(
        "%(asctime)s [%(levelname)-8s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)

    class _TqdmHandler(logging.StreamHandler):
        def emit(self, record):
            try:
                tqdm.write(self.format(record))
            except Exception:
                self.handleError(record)

    ch = _TqdmHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)

    logger.setLevel(logging.DEBUG)
    logger.addHandler(fh)
    logger.addHandler(ch)
    logger.info("Log file: %s", log_file)
    return log_file


# ── Batch UPDATE builders ─────────────────────────────────────────────

def build_pass1(pk_lo, pk_hi):
    return f"""
UPDATE {TARGET_TABLE} m
JOIN {STAGING_MED_CODE_NORM} cn ON cn.med_code           = m.med_code
JOIN {STAGING_FDB}           n  ON cn.med_code_normalized = n.ndc_normalized
SET
    m.med_code_std    = n.NDC,
    m.med_name_std_LN = n.LN60,
    m.med_name_std_BN = n.BN,
    m.mapping_type    = 'Exact match (FDB-NDC)',
    m.conf            = 1.0
WHERE m.{BATCH_KEY} >= {pk_lo} AND m.{BATCH_KEY} < {pk_hi}
  AND m.mapping_type IS NULL
"""


def build_pass2(pk_lo, pk_hi):
    return f"""
UPDATE {TARGET_TABLE} m
JOIN {STAGING_MED_NAME_NORM} nn ON nn.med_name      = m.med_name
JOIN {STAGING_FDB}           n  ON nn.med_name_upper = n.ln60_upper
SET
    m.med_code_std    = n.NDC,
    m.med_name_std_LN = n.LN60,
    m.med_name_std_BN = n.BN,
    m.mapping_type    = 'Exact match (FDB-LN60)',
    m.conf            = 1.0
WHERE m.{BATCH_KEY} >= {pk_lo} AND m.{BATCH_KEY} < {pk_hi}
  AND m.med_name_std_LN IS NULL AND m.mapping_type IS NULL
"""


def build_pass3(pk_lo, pk_hi):
    return f"""
UPDATE {TARGET_TABLE} m
JOIN {STAGING_MED_NAME_NORM} nn ON nn.med_name      = m.med_name
JOIN {STAGING_FDB}           n  ON nn.med_name_upper = n.ln_upper
SET
    m.med_code_std    = n.NDC,
    m.med_name_std_LN = n.LN60,
    m.med_name_std_BN = n.BN,
    m.mapping_type    = 'Exact match (FDB-LN)',
    m.conf            = 1.0
WHERE m.{BATCH_KEY} >= {pk_lo} AND m.{BATCH_KEY} < {pk_hi}
  AND m.med_name_std_LN IS NULL AND m.mapping_type IS NULL
"""


def build_pass4(pk_lo, pk_hi):
    return f"""
UPDATE {TARGET_TABLE} m
JOIN {STAGING_MED_NAME_NORM} nn ON nn.med_name           = m.med_name
JOIN {STAGING_MED_STR_NORM}  sn ON sn.med_strength       = m.med_strength
JOIN {STAGING_FDB}           n  ON nn.med_name_upper      = n.bn_upper
                                AND sn.med_strength_numeric = n.ln60_strength
SET
    m.med_code_std    = n.NDC,
    m.med_name_std_LN = n.LN60,
    m.med_name_std_BN = n.BN,
    m.mapping_type    = 'Exact match (FDB-BN)',
    m.conf            = 1.0
WHERE m.{BATCH_KEY} >= {pk_lo} AND m.{BATCH_KEY} < {pk_hi}
  AND m.med_name_std_LN IS NULL AND m.mapping_type IS NULL
"""


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


def _index_exists(cur, full_table_name, index_name):
    schema, table = full_table_name.split(".", 1)
    cur.execute(
        "SELECT COUNT(*) FROM information_schema.statistics "
        "WHERE table_schema = %s AND table_name = %s AND index_name = %s",
        (schema, table, index_name),
    )
    return cur.fetchone()[0] > 0


def _add_index(cur, conn, full_table_name, index_name, col_expr):
    if not _index_exists(cur, full_table_name, index_name):
        cur.execute(f"ALTER TABLE {full_table_name} ADD INDEX {index_name} ({col_expr})")
        conn.commit()


def _col_exists(cur, full_table_name, col_name):
    schema, table = full_table_name.split(".", 1)
    cur.execute(
        "SELECT COUNT(*) FROM information_schema.columns "
        "WHERE table_schema = %s AND table_name = %s AND column_name = %s",
        (schema, table, col_name),
    )
    return cur.fetchone()[0] > 0


def _ensure_column(cur, conn, full_table_name, column, col_def):
    schema, table = full_table_name.split(".", 1)
    cur.execute(
        "SELECT COUNT(*) FROM information_schema.columns "
        "WHERE table_schema = %s AND table_name = %s AND column_name = %s",
        (schema, table, column),
    )
    if cur.fetchone()[0] == 0:
        cur.execute(f"ALTER TABLE {full_table_name} ADD COLUMN {column} {col_def}")
        conn.commit()


def _build_all_ranges(cur, staging_table):
    cur.execute(f"SELECT COUNT(*) FROM {staging_table}")
    total = cur.fetchone()[0]
    if total == 0:
        return [], 0

    cur.execute(f"""
        SELECT {BATCH_KEY}
        FROM (
            SELECT {BATCH_KEY},
                   ROW_NUMBER() OVER (ORDER BY {BATCH_KEY}) AS rn
            FROM {staging_table}
        ) t
        WHERE (rn - 1) % {BATCH_SIZE} = 0
        ORDER BY {BATCH_KEY}
    """)
    boundaries = [row[0] for row in cur.fetchall() if row[0] is not None]

    cur.execute(f"SELECT MAX({BATCH_KEY}) FROM {staging_table}")
    max_pk = int(cur.fetchone()[0])

    ranges = []
    for i, lo in enumerate(boundaries):
        hi = boundaries[i + 1] if i + 1 < len(boundaries) else max_pk + 1
        ranges.append((lo, hi))

    return ranges, total


def _split_chunks(ranges, n):
    size = (len(ranges) + n - 1) // n
    return [ranges[i: i + size] for i in range(0, len(ranges), size)]


# ── Checkpoint ────────────────────────────────────────────────────────

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
            (source_key, status, rows_updated, started_at, completed_at, error_msg)
        VALUES (%s, %s, %s, NOW(), IF(%s = 'done', NOW(), NULL), %s)
        ON DUPLICATE KEY UPDATE
            status       = VALUES(status),
            rows_updated = VALUES(rows_updated),
            completed_at = IF(VALUES(status) = 'done', NOW(), NULL),
            error_msg    = VALUES(error_msg)
    """, (ck_key, status, rows, status, error))
    conn.commit()
    cur.close()


def get_last_batch(conn, ck_key):
    cur = conn.cursor()
    cur.execute(
        f"SELECT last_batch FROM {CHECKPOINT_TABLE} WHERE source_key = %s",
        (ck_key,),
    )
    row = cur.fetchone()
    cur.close()
    return row[0] if (row and row[0]) else 0


def update_last_batch(conn, ck_key, batch_num):
    cur = conn.cursor()
    cur.execute(
        f"UPDATE {CHECKPOINT_TABLE} SET last_batch = %s WHERE source_key = %s",
        (batch_num, ck_key),
    )
    conn.commit()
    cur.close()


# ── Setup ─────────────────────────────────────────────────────────────

def setup_tables():
    conn = get_connection()
    cur  = conn.cursor()

    cur.execute("SET SESSION lock_wait_timeout = 3600")
    cur.execute("SET SESSION innodb_lock_wait_timeout = 3600")

    # ── 1. Ensure std columns exist on target ─────────────────────────
    logger.info("Checking std columns on %s ...", TARGET_TABLE)
    for col, ddl in [
        ("med_code_std",    "VARCHAR(100) DEFAULT NULL"),
        ("med_name_std_LN", "VARCHAR(500) DEFAULT NULL"),
        ("med_name_std_BN", "VARCHAR(500) DEFAULT NULL"),
        ("mapping_type",    "VARCHAR(100) DEFAULT NULL"),
        ("conf",            "DECIMAL(5,2) DEFAULT NULL"),
    ]:
        if not _col_exists(cur, TARGET_TABLE, col):
            logger.info("  Adding column %s ...", col)
            cur.execute(f"ALTER TABLE {TARGET_TABLE} ADD COLUMN {col} {ddl}")
            conn.commit()
        else:
            logger.info("  exists: %s", col)

    # ── 2. FDB lookup staging ─────────────────────────────────────────
    logger.info("Materializing FDB lookup -> %s ...", STAGING_FDB)
    if not _table_exists(cur, STAGING_FDB):
        cur.execute(f"""
            CREATE TABLE {STAGING_FDB} AS
            SELECT
                NDC, LN60, LN, BN,
                REPLACE(NDC, '-', '')     AS ndc_normalized,
                UPPER(TRIM(LN60))         AS ln60_upper,
                UPPER(TRIM(LN))           AS ln_upper,
                UPPER(TRIM(BN))           AS bn_upper,
                CAST(
                    REGEXP_SUBSTR(LOWER(LN60), '[0-9]+\\.?[0-9]*')
                    AS DECIMAL(15,4)
                )                         AS ln60_strength
            FROM {FDB_TABLE}
        """)
        conn.commit()
        logger.info("  created")
    else:
        logger.info("  already exists, reusing")
    _add_index(cur, conn, STAGING_FDB, "idx_ndc",      "ndc_normalized")
    _add_index(cur, conn, STAGING_FDB, "idx_ln60",     "ln60_upper")
    _add_index(cur, conn, STAGING_FDB, "idx_ln",       "ln_upper")
    _add_index(cur, conn, STAGING_FDB, "idx_bn",       "bn_upper")
    _add_index(cur, conn, STAGING_FDB, "idx_strength", "ln60_strength")
    cur.execute(f"SELECT COUNT(*) FROM {STAGING_FDB}")
    logger.info("  %s FDB rows", f"{cur.fetchone()[0]:,}")

    # ── 3. Med code normalization dictionary ──────────────────────────
    logger.info("Materializing med_code normalization -> %s ...", STAGING_MED_CODE_NORM)
    if not _table_exists(cur, STAGING_MED_CODE_NORM):
        cur.execute(f"""
            CREATE TABLE {STAGING_MED_CODE_NORM} AS
            SELECT DISTINCT
                med_code,
                REPLACE(med_code, '-', '') AS med_code_normalized
            FROM {TARGET_TABLE}
            WHERE med_code IS NOT NULL AND TRIM(med_code) != ''
        """)
        conn.commit()
        logger.info("  created")
    else:
        logger.info("  already exists, reusing")
    _add_index(cur, conn, STAGING_MED_CODE_NORM, "idx_raw",  "med_code")
    _add_index(cur, conn, STAGING_MED_CODE_NORM, "idx_norm", "med_code_normalized")
    cur.execute(f"SELECT COUNT(*) FROM {STAGING_MED_CODE_NORM}")
    logger.info("  %s distinct med_code values", f"{cur.fetchone()[0]:,}")

    # ── 4. Med name normalization dictionary ──────────────────────────
    logger.info("Materializing med_name normalization -> %s ...", STAGING_MED_NAME_NORM)
    if not _table_exists(cur, STAGING_MED_NAME_NORM):
        cur.execute(f"""
            CREATE TABLE {STAGING_MED_NAME_NORM} AS
            SELECT DISTINCT
                med_name,
                UPPER(TRIM(med_name)) AS med_name_upper
            FROM {TARGET_TABLE}
            WHERE med_name IS NOT NULL AND TRIM(med_name) != ''
        """)
        conn.commit()
        logger.info("  created")
    else:
        logger.info("  already exists, reusing")
    _add_index(cur, conn, STAGING_MED_NAME_NORM, "idx_raw",   "med_name(255)")
    _add_index(cur, conn, STAGING_MED_NAME_NORM, "idx_upper", "med_name_upper(255)")
    cur.execute(f"SELECT COUNT(*) FROM {STAGING_MED_NAME_NORM}")
    logger.info("  %s distinct med_name values", f"{cur.fetchone()[0]:,}")

    # ── 5. Med strength normalization dictionary ───────────────────────
    logger.info("Materializing med_strength normalization -> %s ...", STAGING_MED_STR_NORM)
    if not _table_exists(cur, STAGING_MED_STR_NORM):
        cur.execute(f"""
            CREATE TABLE {STAGING_MED_STR_NORM} AS
            SELECT DISTINCT
                med_strength,
                CAST(
                    REGEXP_SUBSTR(LOWER(med_strength), '[0-9]+\\.?[0-9]*')
                    AS DECIMAL(15,4)
                ) AS med_strength_numeric
            FROM {TARGET_TABLE}
            WHERE med_strength IS NOT NULL AND TRIM(med_strength) != ''
        """)
        conn.commit()
        logger.info("  created")
    else:
        logger.info("  already exists, reusing")
    _add_index(cur, conn, STAGING_MED_STR_NORM, "idx_raw",     "med_strength")
    _add_index(cur, conn, STAGING_MED_STR_NORM, "idx_numeric", "med_strength_numeric")
    cur.execute(f"SELECT COUNT(*) FROM {STAGING_MED_STR_NORM}")
    logger.info("  %s distinct med_strength values", f"{cur.fetchone()[0]:,}")

    # Pass 1 PK staging — NULL rows only (the key difference vs med_new_update.py)
    logger.info("Creating null-only Pass 1 PK staging -> %s ...", STAGING_PK_P1)
    try:
        cur.execute(f"""
            CREATE TABLE {STAGING_PK_P1} AS
            SELECT {BATCH_KEY}
            FROM {TARGET_TABLE}
            WHERE {BATCH_KEY} IS NOT NULL
              AND mapping_type IS NULL
        """)
        cur.execute(f"ALTER TABLE {STAGING_PK_P1} ADD INDEX idx_pk ({BATCH_KEY})")
        conn.commit()
        logger.info("  created")
    except Exception as e:
        if getattr(e, 'args', [None])[0] == 1050:
            conn.rollback()
            logger.info("  already exists, reusing")
        else:
            raise

    ranges_p1, total_p1 = _build_all_ranges(cur, STAGING_PK_P1)
    logger.info("  %s unmatched rows -> %s batches", f"{total_p1:,}", len(ranges_p1))

    # Checkpoint table
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {CHECKPOINT_TABLE} (
            source_key   VARCHAR(200) NOT NULL PRIMARY KEY,
            status       ENUM('running','done','failed') NOT NULL DEFAULT 'running',
            rows_updated BIGINT      DEFAULT 0,
            last_batch   INT         DEFAULT 0,
            started_at   DATETIME    DEFAULT NULL,
            completed_at DATETIME    DEFAULT NULL,
            error_msg    TEXT        DEFAULT NULL
        )
    """)
    conn.commit()
    _ensure_column(cur, conn, CHECKPOINT_TABLE, "last_batch", "INT DEFAULT 0")

    cur.close()
    conn.close()
    return ranges_p1, total_p1


def build_p2_staging():
    conn = get_connection()
    cur  = conn.cursor()

    cur.execute("SET SESSION lock_wait_timeout = 3600")
    cur.execute("SET SESSION innodb_lock_wait_timeout = 3600")

    logger.info("Creating null-only Pass 2 PK staging -> %s ...", STAGING_PK_P2)
    try:
        cur.execute(f"""
            CREATE TABLE {STAGING_PK_P2} AS
            SELECT {BATCH_KEY}
            FROM {TARGET_TABLE}
            WHERE med_name_std_LN IS NULL
              AND mapping_type IS NULL
              AND {BATCH_KEY} IS NOT NULL
        """)
        cur.execute(f"ALTER TABLE {STAGING_PK_P2} ADD INDEX idx_pk ({BATCH_KEY})")
        conn.commit()
        logger.info("  created")
    except Exception as e:
        if getattr(e, 'args', [None])[0] == 1050:
            conn.rollback()
            logger.info("  already exists, reusing")
        else:
            raise

    ranges_p2, total_p2 = _build_all_ranges(cur, STAGING_PK_P2)
    logger.info("  %s unmatched rows -> %s batches", f"{total_p2:,}", len(ranges_p2))

    cur.close()
    conn.close()
    return ranges_p2, total_p2


# ── Worker ────────────────────────────────────────────────────────────

def run_worker(worker_id, pass_num, build_fn, ranges_chunk, pbar):
    ck_key = f"med.std.null.pass{pass_num}.worker{worker_id}.{_TABLE_SUFFIX}"
    conn   = get_connection()

    if is_done(conn, ck_key):
        conn.close()
        pbar.update(len(ranges_chunk))
        logger.info("  [Pass %s] Worker %s: skipped (already done)", pass_num, worker_id)
        return {"worker": worker_id, "pass": pass_num,
                "status": "skipped", "rows": 0, "secs": 0}

    last_done = get_last_batch(conn, ck_key)
    if last_done > 0:
        logger.info(
            "  [Pass %s] Worker %s: resuming from batch %s/%s",
            pass_num, worker_id, last_done + 1, len(ranges_chunk),
        )
    else:
        logger.info("  [Pass %s] Worker %s: starting (%s batches)", pass_num, worker_id, len(ranges_chunk))
    mark(conn, ck_key, "running")

    t0         = time.time()
    total_rows = 0
    log_every  = max(1, len(ranges_chunk) // 10)

    try:
        cur = conn.cursor()
        cur.execute("SET SESSION innodb_lock_wait_timeout = 3600")
        cur.execute("SET SESSION lock_wait_timeout = 3600")
        cur.execute("SET unique_checks = 0")
        cur.execute("SET foreign_key_checks = 0")

        for batch_num, (pk_lo, pk_hi) in enumerate(ranges_chunk, 1):
            if batch_num <= last_done:
                pbar.update(1)
                continue
            sql = build_fn(pk_lo, pk_hi)
            cur.execute(sql)
            conn.commit()
            total_rows += cur.rowcount
            pbar.update(1)
            update_last_batch(conn, ck_key, batch_num)

            if batch_num % log_every == 0:
                pct = batch_num / len(ranges_chunk) * 100
                logger.info(
                    "  [Pass %s] Worker %s: %s/%s batches (%.0f%%)  rows so far: %s",
                    pass_num, worker_id, batch_num, len(ranges_chunk), pct, f"{total_rows:,}",
                )

        cur.execute("SET unique_checks = 1")
        cur.execute("SET foreign_key_checks = 1")
        cur.close()

        elapsed = round(time.time() - t0, 1)
        mark(conn, ck_key, "done", total_rows)
        conn.close()
        logger.info(
            "  [Pass %s] Worker %s: DONE  %s rows  (%ss)",
            pass_num, worker_id, f"{total_rows:,}", elapsed,
        )
        return {"worker": worker_id, "pass": pass_num,
                "status": "done", "rows": total_rows, "secs": elapsed}

    except Exception as exc:
        elapsed = round(time.time() - t0, 1)
        mark(conn, ck_key, "failed", total_rows, str(exc))
        logger.error(
            "  [Pass %s] Worker %s: FAILED after %ss  rows so far: %s  error: %s",
            pass_num, worker_id, elapsed, f"{total_rows:,}", exc,
        )
        try:
            conn.close()
        except Exception:
            pass
        return {"worker": worker_id, "pass": pass_num,
                "status": f"FAILED: {exc}", "rows": total_rows, "secs": elapsed}


def run_pass(pass_num, build_fn, all_ranges, label):
    chunks     = _split_chunks(all_ranges, MAX_WORKERS)
    results    = []
    any_failed = False

    logger.info("─" * 60)
    logger.info(label)
    logger.info(
        "  %s batches  x  %s rows/batch  ->  %s workers",
        len(all_ranges), f"{BATCH_SIZE:,}", MAX_WORKERS,
    )

    with tqdm(total=len(all_ranges), desc=f"Pass {pass_num}", unit="batch",
              file=sys.stderr) as pbar:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
            futures = {
                pool.submit(run_worker, i, pass_num, build_fn, chunks[i], pbar): i
                for i in range(len(chunks))
            }
            for future in as_completed(futures):
                res = future.result()
                results.append(res)
                if "FAILED" in str(res["status"]):
                    any_failed = True

    done    = sum(1 for r in results if r["status"] == "done")
    skipped = sum(1 for r in results if r["status"] == "skipped")
    rows    = sum(r["rows"] for r in results)
    logger.info(
        "Pass %s complete: %s done, %s skipped  |  rows updated: %s",
        pass_num, done, skipped, f"{rows:,}",
    )
    for r in results:
        if "FAILED" in str(r["status"]):
            logger.error("  [FAIL] worker %s: %s", r["worker"], r["status"])
    return results, any_failed


# ── Main ──────────────────────────────────────────────────────────────

def main():
    log_path = setup_logging()

    logger.info("=" * 70)
    logger.info("Medications Standardisation (null-only) — %s", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    logger.info("  log file   : %s", log_path)
    logger.info("  target     : %s", TARGET_TABLE)
    logger.info("  fdb        : %s", FDB_TABLE)
    logger.info("  batch_key  : %s", BATCH_KEY)
    logger.info("  batch_size : %s", f"{BATCH_SIZE:,}")
    logger.info("  workers    : %s", MAX_WORKERS)
    logger.info("  passes     : 4  (NDC code | LN60 name | LN name | BN+strength)")
    logger.info("  mode       : NULL rows only (skips already-standardised rows)")
    logger.info("=" * 70)

    ranges_p1, total_p1 = setup_tables()

    if not ranges_p1:
        logger.info("No unmatched rows found — all rows already standardised.")
        return

    p1_results, p1_failed = run_pass(
        1, build_pass1, ranges_p1,
        f"Pass 1 — NDC code match  ({total_p1:,} unmatched rows)",
    )
    if p1_failed:
        logger.error("Pass 1 had failures — aborting.")
        sys.exit(1)

    ranges_p2, total_p2 = build_p2_staging()

    if not ranges_p2:
        logger.info("Passes 2-4: no unmatched rows remaining.")
        p2_results = p3_results = p4_results = []
    else:
        p2_results, p2_failed = run_pass(
            2, build_pass2, ranges_p2,
            f"Pass 2 — LN60 name match  ({total_p2:,} unmatched rows)",
        )
        if p2_failed:
            logger.error("Pass 2 had failures — aborting.")
            sys.exit(1)

        p3_results, p3_failed = run_pass(
            3, build_pass3, ranges_p2,
            f"Pass 3 — LN name match  ({total_p2:,} unmatched rows)",
        )
        if p3_failed:
            logger.error("Pass 3 had failures — aborting.")
            sys.exit(1)

        p4_results, p4_failed = run_pass(
            4, build_pass4, ranges_p2,
            f"Pass 4 — BN+strength match  ({total_p2:,} unmatched rows)",
        )
        if p4_failed:
            logger.error("Pass 4 had failures.")
            sys.exit(1)

    all_results = p1_results + p2_results + p3_results + p4_results
    total_rows  = sum(r["rows"] for r in all_results)
    logger.info("=" * 70)
    logger.info("Total rows updated : %s", f"{total_rows:,}")
    logger.info("Target             : %s", TARGET_TABLE)
    logger.info("=" * 70)

    logger.info("Cleanup SQL (run after verifying data):")
    logger.info("  DROP TABLE IF EXISTS %s;", STAGING_FDB)
    logger.info("  DROP TABLE IF EXISTS %s;", STAGING_MED_CODE_NORM)
    logger.info("  DROP TABLE IF EXISTS %s;", STAGING_MED_NAME_NORM)
    logger.info("  DROP TABLE IF EXISTS %s;", STAGING_MED_STR_NORM)
    logger.info("  DROP TABLE IF EXISTS %s;", STAGING_PK_P1)
    logger.info("  DROP TABLE IF EXISTS %s;", STAGING_PK_P2)
    logger.info("  DROP TABLE IF EXISTS %s;", CHECKPOINT_TABLE)


if __name__ == "__main__":
    main()
