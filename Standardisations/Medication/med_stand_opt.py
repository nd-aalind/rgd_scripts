#!/usr/bin/env python3
"""
Optimized batched standardisation UPDATEs for: rgd_udm_silver.medications

Four sequential passes, each parallelized across workers via ThreadPoolExecutor:

  Pass 1 — All rows:
    JOIN on med_code_normalized = ndc_normalized  (NDC code match)
    SET med_code_std, med_name_std_LN, med_name_std_BN,
        mapping_type='Exact match (FDB-NDC)', conf=1.0

  Pass 2 — Unmatched (med_name_std_LN IS NULL AND mapping_type IS NULL):
    JOIN on med_name_upper = ln60_upper  (LN60 drug name match)
    SET mapping_type='Exact match (FDB-LN60)', conf=1.0

  Pass 3 — Still unmatched:
    JOIN on med_name_upper = ln_upper  (LN drug name match)
    SET mapping_type='Exact match (FDB-LN)', conf=1.0

  Pass 4 — Still unmatched:
    JOIN on med_name_upper = bn_upper AND med_strength_numeric = ln60_strength
    SET mapping_type='Exact match (FDB-BN)', conf=1.0

Three DISTINCT dictionary staging tables pre-compute all REPLACE / UPPER / TRIM
/ CAST operations on the medications side (one scan only), indexed on both the
raw and normalized columns.  FDB staging does the same for the FDB side.
Every batch UPDATE is therefore a pure indexed equality join — zero functions
at query time.

Optimizations:
- All REPLACE / UPPER / TRIM / CAST pre-computed once across both sides
- 9 total indexes across 4 staging tables — every JOIN is fully indexed
- PK staging pre-filters eligible rows (one full scan only)
- Server-side boundary sampling (sparse-ID safe)
- Workers get non-overlapping ranges → no row-level lock contention
- Commit after every batch
- Checkpoint/resume per (pass, worker)
- InnoDB checks disabled per-session
- Progress bar via tqdm
- Dual logging: terminal (stdout) + timestamped log file

Usage:
    python med_stand_opt.py
    # On a VM (survives logout):
    nohup python med_stand_opt.py &
    tail -f med_stand_opt_<timestamp>.log
"""

import logging
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
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
    "database":        "rgd_udm_silver",
    "charset":         "utf8mb4",
    "connect_timeout": 30,
    "read_timeout":    21600,
    "write_timeout":   21600,
}

BATCH_SIZE  = 50_000
MAX_WORKERS = 4

TARGET_TABLE = "udm_staging.medication_final_dcnd"
FDB_TABLE    = "FDB.RNDC14_NDC_MSTR"

# ── Set to the sequential row identifier of the medications table ──────
BATCH_KEY = "ndid"

_TABLE_SUFFIX = TARGET_TABLE.replace(".", "_").replace("-", "_")

# ── Staging tables ────────────────────────────────────────────────────
STAGING_FDB           = "staging.med_fdb_ndc_mstr_v5"     # FDB normalized lookup
STAGING_MED_CODE_NORM = "staging.med_code_norm_v5"         # DISTINCT med_code → normalized
STAGING_MED_NAME_NORM = "staging.med_name_norm_v5"         # DISTINCT med_name → upper
STAGING_MED_STR_NORM  = "staging.med_strength_norm_v5"     # DISTINCT med_strength → numeric
STAGING_PK_P1         = f"staging.med_std_pk5_{_TABLE_SUFFIX}"
STAGING_PK_P2         = f"staging.med_std_pk5p2_{_TABLE_SUFFIX}"
CHECKPOINT_TABLE      = f"staging.etl_checkpoint_med_std5_{_TABLE_SUFFIX}"


# ── Logging setup ─────────────────────────────────────────────────────

def _setup_logging():
    """Write to both terminal (stdout) and a timestamped log file."""
    ts       = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path = f"med_stand_opt_{ts}.log"

    fmt = logging.Formatter(
        fmt="%(asctime)s  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setFormatter(fmt)

    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)

    root = logging.getLogger()
    root.setLevel(logging.INFO)
    root.addHandler(fh)
    root.addHandler(sh)

    return log_path


logger = logging.getLogger("med_stand")


# ── Batch UPDATE builders ─────────────────────────────────────────────
# Every JOIN is a pre-indexed equality — no REPLACE / UPPER / TRIM / CAST
# at query time.

def build_pass1(pk_lo, pk_hi):
    """NDC code match: med_code_normalized = ndc_normalized."""
    return f"""
UPDATE {TARGET_TABLE} m
JOIN {STAGING_MED_CODE_NORM} cn ON cn.med_code          = m.med_code
JOIN {STAGING_FDB}           n  ON cn.med_code_normalized = n.ndc_normalized
SET
    m.med_code_std    = n.NDC,
    m.med_name_std_LN = n.LN60,
    m.med_name_std_BN = n.BN,
    m.mapping_type    = 'Exact match (FDB-NDC)',
    m.conf            = 1.0
WHERE m.{BATCH_KEY} >= {pk_lo} AND m.{BATCH_KEY} < {pk_hi}
"""


def build_pass2(pk_lo, pk_hi):
    """LN60 drug name match: med_name_upper = ln60_upper."""
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
    """LN drug name match: med_name_upper = ln_upper."""
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
    """BN brand name + strength match: med_name_upper = bn_upper AND numeric strength equal."""
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
    schema, table = full_table_name.split(".")
    cur.execute(
        "SELECT COUNT(*) FROM information_schema.tables "
        "WHERE table_schema = %s AND table_name = %s",
        (schema, table),
    )
    return cur.fetchone()[0] > 0


def _col_exists(cur, full_table_name, col_name):
    schema, table = full_table_name.split(".")
    cur.execute(
        "SELECT COUNT(*) FROM information_schema.columns "
        "WHERE table_schema = %s AND table_name = %s AND column_name = %s",
        (schema, table, col_name),
    )
    return cur.fetchone()[0] > 0


def _index_exists(cur, full_table_name, index_name):
    schema, table = full_table_name.split(".")
    cur.execute(
        "SELECT COUNT(*) FROM information_schema.statistics "
        "WHERE table_schema = %s AND table_name = %s AND index_name = %s",
        (schema, table, index_name),
    )
    return cur.fetchone()[0] > 0


def _add_index(cur, conn, full_table_name, index_name, col_expr):
    """Add index only if it doesn't already exist."""
    if not _index_exists(cur, full_table_name, index_name):
        cur.execute(f"ALTER TABLE {full_table_name} ADD INDEX {index_name} ({col_expr})")
        conn.commit()


def _build_all_ranges(cur, staging_table):
    """Build all (lo, hi) batch ranges from a PK staging table."""
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
    """Split ranges into n roughly equal chunks for workers."""
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


# ── Setup ─────────────────────────────────────────────────────────────

def setup_tables():
    """Create all staging tables and return Pass 1 batch ranges."""
    conn = get_connection()
    cur  = conn.cursor()

    cur.execute("SET SESSION lock_wait_timeout = 3600")
    cur.execute("SET SESSION innodb_lock_wait_timeout = 3600")

    # ── 1. Ensure std columns exist on target ─────────────────────────
    logger.info(f"Checking std columns on {TARGET_TABLE}...")
    for col, ddl in [
        ("med_code_std",    "VARCHAR(100) DEFAULT NULL"),
        ("med_name_std_LN", "VARCHAR(500) DEFAULT NULL"),
        ("med_name_std_BN", "VARCHAR(500) DEFAULT NULL"),
        ("mapping_type",    "VARCHAR(100) DEFAULT NULL"),
        ("conf",            "DECIMAL(5,2) DEFAULT NULL"),
    ]:
        if not _col_exists(cur, TARGET_TABLE, col):
            logger.info(f"  Adding column {col}...")
            cur.execute(f"ALTER TABLE {TARGET_TABLE} ADD COLUMN {col} {ddl}")
            conn.commit()
        else:
            logger.info(f"  exists: {col}")
    logger.info("  All std columns present.")

    # ── 2. FDB lookup staging ─────────────────────────────────────────
    logger.info(f"Materializing {FDB_TABLE} lookup -> {STAGING_FDB} ...")
    if not _table_exists(cur, STAGING_FDB):
        cur.execute(f"""
            CREATE TABLE {STAGING_FDB} AS
            SELECT
                NDC,
                LN60,
                LN,
                BN,
                REPLACE(NDC, '-', '')     AS ndc_normalized,
                UPPER(TRIM(LN60))         AS ln60_upper,
                UPPER(TRIM(LN))           AS ln_upper,
                UPPER(TRIM(BN))           AS bn_upper,
                CAST(
                    REGEXP_SUBSTR(LOWER(LN60), '[0-9]+\\.?[0-9]*')
                    AS DECIMAL(15,4)
                )                         AS ln60_strength
            FROM FDB.RNDC14_NDC_MSTR
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
    logger.info(f"  {cur.fetchone()[0]:,} FDB rows")

    # ── 3. Medication code normalization dictionary ────────────────────
    logger.info(f"Materializing med_code normalization dictionary -> {STAGING_MED_CODE_NORM} ...")
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
    logger.info(f"  {cur.fetchone()[0]:,} distinct med_code values")

    # ── 4. Medication name normalization dictionary ────────────────────
    logger.info(f"Materializing med_name normalization dictionary -> {STAGING_MED_NAME_NORM} ...")
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
    logger.info(f"  {cur.fetchone()[0]:,} distinct med_name values")

    # ── 5. Medication strength normalization dictionary ────────────────
    logger.info(f"Materializing med_strength normalization dictionary -> {STAGING_MED_STR_NORM} ...")
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
    logger.info(f"  {cur.fetchone()[0]:,} distinct med_strength values")

    # ── 6. Pass 1 PK staging (all rows) ──────────────────────────────
    logger.info(f"Creating Pass 1 PK staging -> {STAGING_PK_P1} ...")
    if not _table_exists(cur, STAGING_PK_P1):
        cur.execute(f"""
            CREATE TABLE {STAGING_PK_P1} AS
            SELECT {BATCH_KEY}
            FROM {TARGET_TABLE}
            WHERE {BATCH_KEY} IS NOT NULL
        """)
        cur.execute(f"ALTER TABLE {STAGING_PK_P1} ADD INDEX idx_pk ({BATCH_KEY})")
        conn.commit()
        logger.info("  created")
    else:
        logger.info("  already exists, reusing")
    ranges_p1, total_p1 = _build_all_ranges(cur, STAGING_PK_P1)
    logger.info(f"  {total_p1:,} rows -> {len(ranges_p1)} batches")

    # ── 7. Checkpoint table ───────────────────────────────────────────
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

    cur.close()
    conn.close()
    return ranges_p1, total_p1


def build_p2_staging():
    """Called AFTER Pass 1 — rows still unmatched (shared by passes 2, 3, 4)."""
    conn = get_connection()
    cur  = conn.cursor()

    cur.execute("SET SESSION lock_wait_timeout = 3600")
    cur.execute("SET SESSION innodb_lock_wait_timeout = 3600")

    logger.info(f"Creating Pass 2 PK staging (unmatched rows after Pass 1) -> {STAGING_PK_P2} ...")
    if not _table_exists(cur, STAGING_PK_P2):
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
    else:
        logger.info("  already exists, reusing")
    ranges_p2, total_p2 = _build_all_ranges(cur, STAGING_PK_P2)
    logger.info(f"  {total_p2:,} rows -> {len(ranges_p2)} batches")

    cur.close()
    conn.close()
    return ranges_p2, total_p2


# ── Worker ─────────────────────────────────────────────────────────────

def run_worker(worker_id, pass_num, build_fn, ranges_chunk, pbar):
    ck_key = f"med.std.pass{pass_num}.worker{worker_id}.{_TABLE_SUFFIX}"
    conn   = get_connection()

    if is_done(conn, ck_key):
        conn.close()
        pbar.update(len(ranges_chunk))
        logger.info(f"  [Pass {pass_num}] Worker {worker_id}: skipped (already done)")
        return {"worker": worker_id, "pass": pass_num,
                "status": "skipped", "rows": 0, "secs": 0}

    logger.info(f"  [Pass {pass_num}] Worker {worker_id}: starting ({len(ranges_chunk)} batches)")
    mark(conn, ck_key, "running")
    t0         = time.time()
    total_rows = 0
    log_every  = max(1, len(ranges_chunk) // 10)   # log progress ~10 times per worker

    try:
        cur = conn.cursor()
        cur.execute("SET unique_checks = 0")
        cur.execute("SET foreign_key_checks = 0")

        for batch_num, (pk_lo, pk_hi) in enumerate(ranges_chunk, 1):
            sql = build_fn(pk_lo, pk_hi)
            cur.execute(sql)
            conn.commit()
            total_rows += cur.rowcount
            pbar.update(1)

            if batch_num % log_every == 0:
                pct = batch_num / len(ranges_chunk) * 100
                logger.info(
                    f"  [Pass {pass_num}] Worker {worker_id}: "
                    f"{batch_num}/{len(ranges_chunk)} batches ({pct:.0f}%)  "
                    f"rows so far: {total_rows:,}"
                )

        cur.execute("SET unique_checks = 1")
        cur.execute("SET foreign_key_checks = 1")
        cur.close()

        elapsed = round(time.time() - t0, 1)
        mark(conn, ck_key, "done", total_rows)
        conn.close()
        logger.info(
            f"  [Pass {pass_num}] Worker {worker_id}: DONE  "
            f"{total_rows:,} rows  ({elapsed}s)"
        )
        return {"worker": worker_id, "pass": pass_num,
                "status": "done", "rows": total_rows, "secs": elapsed}

    except Exception as exc:
        elapsed = round(time.time() - t0, 1)
        mark(conn, ck_key, "failed", total_rows, str(exc))
        logger.error(
            f"  [Pass {pass_num}] Worker {worker_id}: FAILED after {elapsed}s  "
            f"rows so far: {total_rows:,}  error: {exc}"
        )
        try:
            conn.close()
        except Exception:
            pass
        return {"worker": worker_id, "pass": pass_num,
                "status": f"FAILED: {exc}", "rows": total_rows, "secs": elapsed}


def run_pass(pass_num, build_fn, all_ranges, label):
    """Split ranges into worker chunks and run in parallel."""
    chunks     = _split_chunks(all_ranges, MAX_WORKERS)
    results    = []
    any_failed = False

    logger.info(f"{'─'*60}")
    logger.info(f"{label}")
    logger.info(
        f"  {len(all_ranges)} batches  x  {BATCH_SIZE:,} rows/batch  ->  {MAX_WORKERS} workers"
    )

    # tqdm goes to stderr so it shows in the terminal but not in the log file
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
        f"Pass {pass_num} complete: {done} done, {skipped} skipped  |  "
        f"rows updated: {rows:,}"
    )
    for r in results:
        if "FAILED" in str(r["status"]):
            logger.error(f"  [FAIL] worker {r['worker']}: {r['status']}")
    return results, any_failed


# ── Main ───────────────────────────────────────────────────────────────

def main():
    log_path = _setup_logging()

    logger.info("=" * 70)
    logger.info(f"Medications Standardisation UPDATE — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"  log file   : {log_path}")
    logger.info(f"  target     : {TARGET_TABLE}")
    logger.info(f"  fdb        : {FDB_TABLE}")
    logger.info(f"  batch_key  : {BATCH_KEY}")
    logger.info(f"  batch_size : {BATCH_SIZE:,}")
    logger.info(f"  workers    : {MAX_WORKERS}")
    logger.info(f"  passes     : 4  (NDC code | LN60 name | LN name | BN+strength)")
    logger.info("=" * 70)

    ranges_p1, total_p1 = setup_tables()

    if not ranges_p1:
        logger.info("No rows to process. Exiting.")
        return

    # ── Pass 1: NDC code match ─────────────────────────────────────────
    p1_results, p1_failed = run_pass(
        1, build_pass1, ranges_p1,
        f"Pass 1 — NDC code match  ({total_p1:,} rows)"
    )
    if p1_failed:
        logger.error("Pass 1 had failures — aborting.")
        sys.exit(1)

    # ── Build unmatched staging (shared by passes 2, 3, 4) ────────────
    ranges_p2, total_p2 = build_p2_staging()

    if not ranges_p2:
        logger.info("Passes 2-4: no unmatched rows — done.")
        p2_results = p3_results = p4_results = []
    else:
        # ── Pass 2: LN60 name match ────────────────────────────────────
        p2_results, p2_failed = run_pass(
            2, build_pass2, ranges_p2,
            f"Pass 2 — LN60 name match  ({total_p2:,} rows)"
        )
        if p2_failed:
            logger.error("Pass 2 had failures — aborting.")
            sys.exit(1)

        # ── Pass 3: LN name match ──────────────────────────────────────
        p3_results, p3_failed = run_pass(
            3, build_pass3, ranges_p2,
            f"Pass 3 — LN name match  ({total_p2:,} rows)"
        )
        if p3_failed:
            logger.error("Pass 3 had failures — aborting.")
            sys.exit(1)

        # ── Pass 4: BN brand name + strength ──────────────────────────
        p4_results, p4_failed = run_pass(
            4, build_pass4, ranges_p2,
            f"Pass 4 — BN+strength match  ({total_p2:,} rows)"
        )
        if p4_failed:
            logger.error("Pass 4 had failures.")
            sys.exit(1)

    # ── Summary ────────────────────────────────────────────────────────
    all_results = p1_results + p2_results + p3_results + p4_results
    total_rows  = sum(r["rows"] for r in all_results)
    logger.info("=" * 70)
    logger.info(f"Total rows updated : {total_rows:,}")
    logger.info(f"Target             : {TARGET_TABLE}")
    logger.info("=" * 70)

    logger.info("Cleanup SQL (run after verifying data):")
    logger.info(f"  -- DROP TABLE IF EXISTS {STAGING_FDB};")
    logger.info(f"  -- DROP TABLE IF EXISTS {STAGING_MED_CODE_NORM};")
    logger.info(f"  -- DROP TABLE IF EXISTS {STAGING_MED_NAME_NORM};")
    logger.info(f"  -- DROP TABLE IF EXISTS {STAGING_MED_STR_NORM};")
    logger.info(f"  DROP TABLE IF EXISTS {STAGING_PK_P1};")
    logger.info(f"  DROP TABLE IF EXISTS {STAGING_PK_P2};")
    logger.info(f"  DROP TABLE IF EXISTS {CHECKPOINT_TABLE};")


if __name__ == "__main__":
    main()
