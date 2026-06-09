#!/usr/bin/env python3
"""
Optimized ETL loader for: udm_staging.familyhistory_final  (Greenway source)

Source query (single branch):
    PatHistFamily a
    JOIN  PatHistCatPatHistItem b  ON a.PatHistCatPatHistItemID = b.PatHistCatPatHistItemID
                                   AND b.nd_ActiveFlag = 'Y'
    LEFT JOIN PatHistItem pi        ON pi.PatHistItemID = b.PatHistItemID
                                   AND pi.nd_ActiveFlag = 'Y'
    LEFT JOIN PatHistFamilyRelation c ON a.PatHistCatPatHistItemID = c.PatHistCatPatHistItemID
                                     AND a.PatientID = c.PatientID
                                     AND c.nd_ActiveFlag = 'Y'

  (PatHistCatMaster is joined in the original SQL but nothing is selected from it — omitted.)

Pre-materialized staging lookups (computed ONCE, reused across all batches):
  STAGING_CPI  — PatHistCatPatHistItem + PatHistItem (nd_ActiveFlag='Y')
                 indexed on PatHistCatPatHistItemID
  STAGING_REL  — PatHistFamilyRelation (nd_ActiveFlag='Y')
                 indexed on (PatHistCatPatHistItemID, PatientID)

Optimizations:
- Shared lookups materialized once — not re-scanned per batch
- PK staging pre-filters eligible PatHistFamilyIDs
- Batch by actual PK values (sparse-ID safe via ROW_NUMBER keyset pagination)
- Checkpoint/resume — re-run skips if already completed
- Commit after every batch (frees InnoDB undo log)
- InnoDB checks disabled per-session for bulk insert speed
- Dual logging: terminal (stdout) + timestamped log file

Usage:
    python opt_fam_hist_gw.py
    # On a VM (survives logout):
    nohup python opt_fam_hist_gw.py &
    tail -f fam_hist_gw_*.log
"""

import logging
import sys
import time
from datetime import datetime
import pymysql
from tqdm import tqdm

# ── Configuration ─────────────────────────────────────────────────────
DB_CONFIG = {
    "host":            "ndai-dev-rds-instance.cwp60ymu4ko0.us-east-1.rds.amazonaws.com",
    "port":            3306,
    "user":            "Aalind",
    "password":        "A@L1nd@123",
    "database":        "udm_staging",
    "charset":         "utf8mb4",
    "connect_timeout": 30,
    "read_timeout":    21600,
    "write_timeout":   21600,
}

BATCH_SIZE = 50_000

# ── Change these two variables to run for a different schema/psid ─────
SOURCE_SCHEMA = "greenway"   # e.g. "greenway", "dcnd_gw", "raleigh_gw", ...
PSID          = 9

DEST_TABLE       = "udm_staging.familyhistory_final"
STAGING_CPI      = f"staging.fh_gw_cpi_v1_{SOURCE_SCHEMA}"
STAGING_REL      = f"staging.fh_gw_rel_v1_{SOURCE_SCHEMA}"
STAGING_PK       = f"staging.fh_gw_pk_v1_{SOURCE_SCHEMA}"
CHECKPOINT_TABLE = f"staging.etl_checkpoint_fh_gw_v1_{SOURCE_SCHEMA}"
CHECKPOINT_KEY   = f"familyhistory.gw.insert.{SOURCE_SCHEMA}"

BATCH_KEY = "PatHistFamilyID"


# ── Logging setup ─────────────────────────────────────────────────────

def _setup_logging():
    ts       = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path = f"fam_hist_gw_{ts}.log"

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


logger = logging.getLogger("fam_hist_gw")


# ── Date CASE helper ──────────────────────────────────────────────────

def date_case(col):
    """
    Safely converts a DATE/VARCHAR column to DATE.
    {{4}} / {{2}} inside this f-string produce literal {4}/{2}
    in the returned string — correct MySQL REGEXP quantifiers.
    """
    return (
        f"CASE\n"
        f"        WHEN {col} IS NULL OR {col} IN ('None', '') THEN NULL\n"
        f"        WHEN LEFT({col}, 10) REGEXP '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}$'\n"
        f"            THEN STR_TO_DATE(LEFT({col}, 10), '%Y-%m-%d')\n"
        f"        WHEN LEFT({col}, 10) REGEXP '^[0-9]{{2}}-[0-9]{{2}}-[0-9]{{4}}$'\n"
        f"            THEN STR_TO_DATE(LEFT({col}, 10), '%m-%d-%Y')\n"
        f"        ELSE NULL\n"
        f"    END"
    )


# ── Batch INSERT builder ──────────────────────────────────────────────

def build_batch_insert(pk_lo, pk_hi):
    return f"""
INSERT INTO {DEST_TABLE}
    (family_hist_id, ndid, eid, enc_date,
     onset_date, onset_age, family_hist_date,
     hist_category, fam_hist_relation, family_relationship_code,
     family_hist_details, family_hist_code, family_hist_coding_system,
     family_hist_notes, family_hist_value, itemname, itemdesc,
     created_datetime, created_by, updated_datetime, updated_by,
     ehr_source_name, source_path, data_type,
     psid, nd_extracted_date)
SELECT
    a.{BATCH_KEY},
    a.PatientID,
    NULL,
    NULL,
    {date_case('a.CreateDate')},
    NULL,
    {date_case('a.CreateDate')},
    'FamilyHistory',
    rel.Relation,
    NULL,
    a.PFHNote,
    cpi.AltSystemCode,
    cpi.AltSystem,
    NULL,
    NULL,
    NULL,
    NULL,
    CURRENT_TIMESTAMP(),
    'ND',
    CURRENT_TIMESTAMP(),
    'ND',
    'Greenway',
    'bronze_layer',
    'Structured',
    {PSID},
    a.nd_extracted_date
FROM {SOURCE_SCHEMA}.PatHistFamily a
JOIN {STAGING_CPI} cpi
    ON cpi.PatHistCatPatHistItemID = a.PatHistCatPatHistItemID
LEFT JOIN {STAGING_REL} rel
    ON rel.PatHistCatPatHistItemID = a.PatHistCatPatHistItemID
   AND rel.PatientID               = a.PatientID
WHERE a.{BATCH_KEY} >= {pk_lo}
  AND a.{BATCH_KEY} <  {pk_hi}
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
    """Create staging lookups, PK staging, dest/checkpoint tables. Return batch ranges."""
    conn = get_connection()
    cur  = conn.cursor()

    cur.execute("SET SESSION lock_wait_timeout = 3600")
    cur.execute("SET SESSION innodb_lock_wait_timeout = 3600")

    # ── 1. CPI lookup (PatHistCatPatHistItem + PatHistItem, nd_ActiveFlag='Y') ──
    logger.info(f"Creating CPI lookup -> {STAGING_CPI} ...")
    if not _table_exists(cur, STAGING_CPI):
        cur.execute(f"""
            CREATE TABLE {STAGING_CPI} AS
            SELECT
                b.PatHistCatPatHistItemID,
                pi.AltSystemCode,
                pi.AltSystem
            FROM {SOURCE_SCHEMA}.PatHistCatPatHistItem b
            LEFT JOIN {SOURCE_SCHEMA}.PatHistItem pi
                ON pi.PatHistItemID  = b.PatHistItemID
               AND pi.nd_ActiveFlag  = 'Y'
            WHERE b.nd_ActiveFlag = 'Y'
        """)
        conn.commit()
        logger.info("  created")
    else:
        logger.info("  already exists, reusing")
    _add_index(cur, conn, STAGING_CPI, "idx_cpi_id", "PatHistCatPatHistItemID")
    cur.execute(f"SELECT COUNT(*) FROM {STAGING_CPI}")
    logger.info(f"  {cur.fetchone()[0]:,} CPI rows")

    # ── 2. Family relation lookup (PatHistFamilyRelation, nd_ActiveFlag='Y') ──
    logger.info(f"Creating relation lookup -> {STAGING_REL} ...")
    if not _table_exists(cur, STAGING_REL):
        cur.execute(f"""
            CREATE TABLE {STAGING_REL} AS
            SELECT
                PatHistCatPatHistItemID,
                PatientID,
                Relation
            FROM {SOURCE_SCHEMA}.PatHistFamilyRelation
            WHERE nd_ActiveFlag = 'Y'
        """)
        conn.commit()
        logger.info("  created")
    else:
        logger.info("  already exists, reusing")
    _add_index(cur, conn, STAGING_REL, "idx_rel_cpi_pat",
               "PatHistCatPatHistItemID, PatientID")
    cur.execute(f"SELECT COUNT(*) FROM {STAGING_REL}")
    logger.info(f"  {cur.fetchone()[0]:,} relation rows")

    # ── 3. PK staging (PatHistFamilyID — no active flag filter; JOIN handles it) ──
    logger.info(f"Creating PK staging -> {STAGING_PK} ...")
    if not _table_exists(cur, STAGING_PK):
        cur.execute(f"""
            CREATE TABLE {STAGING_PK} AS
            SELECT {BATCH_KEY}
            FROM {SOURCE_SCHEMA}.PatHistFamily
            WHERE {BATCH_KEY} IS NOT NULL
        """)
        conn.commit()
        logger.info("  created")
    else:
        logger.info("  already exists, reusing")
    _add_index(cur, conn, STAGING_PK, "idx_pk", BATCH_KEY)

    cur.execute(f"SELECT COUNT(*) FROM {STAGING_PK}")
    total = cur.fetchone()[0]
    logger.info(f"  {total:,} rows to process")

    # ── 4. Destination table ──────────────────────────────────────────
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {DEST_TABLE} (
            family_hist_id            BIGINT        DEFAULT NULL,
            ndid                      BIGINT        DEFAULT NULL,
            eid                       BIGINT        DEFAULT NULL,
            enc_date                  DATE          DEFAULT NULL,
            onset_date                DATE          DEFAULT NULL,
            onset_age                 VARCHAR(100)  DEFAULT NULL,
            family_hist_date          DATE          DEFAULT NULL,
            hist_category             VARCHAR(100)  DEFAULT NULL,
            fam_hist_relation         VARCHAR(200)  DEFAULT NULL,
            family_relationship_code  VARCHAR(10)   DEFAULT NULL,
            family_hist_details       TEXT,
            family_hist_code          VARCHAR(50)   DEFAULT NULL,
            family_hist_coding_system VARCHAR(50)   DEFAULT NULL,
            family_hist_notes         TEXT,
            family_hist_value         TEXT,
            itemname                  VARCHAR(200)  DEFAULT NULL,
            itemdesc                  TEXT,
            created_datetime          DATETIME      DEFAULT NULL,
            created_by                VARCHAR(50)   DEFAULT NULL,
            updated_datetime          DATETIME      DEFAULT NULL,
            updated_by                VARCHAR(50)   DEFAULT NULL,
            ehr_source_name           VARCHAR(100)  DEFAULT NULL,
            source_path               VARCHAR(100)  DEFAULT NULL,
            data_type                 VARCHAR(50)   DEFAULT NULL,
            psid                      INT           DEFAULT NULL,
            nd_extracted_date         DATE          DEFAULT NULL
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
    """)
    conn.commit()

    # ── 5. Checkpoint table ───────────────────────────────────────────
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {CHECKPOINT_TABLE} (
            source_key    VARCHAR(150) NOT NULL PRIMARY KEY,
            status        ENUM('running','done','failed') NOT NULL DEFAULT 'running',
            rows_inserted BIGINT      DEFAULT 0,
            started_at    DATETIME    DEFAULT NULL,
            completed_at  DATETIME    DEFAULT NULL,
            error_msg     TEXT        DEFAULT NULL
        )
    """)
    conn.commit()

    # ── 6. Batch boundary sampling ────────────────────────────────────
    if total == 0:
        cur.close()
        conn.close()
        return []

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

    ranges = []
    for i, lo in enumerate(boundaries):
        hi = boundaries[i + 1] if i + 1 < len(boundaries) else max_pk + 1
        ranges.append((lo, hi))

    logger.info(f"  {len(ranges)} batches of ~{BATCH_SIZE:,} rows each")

    cur.close()
    conn.close()
    return ranges


# ── Runner ────────────────────────────────────────────────────────────

def run_insert(ranges, pbar):
    conn = get_connection()

    if is_done(conn):
        conn.close()
        pbar.update(len(ranges))
        logger.info("  already done — skipping")
        return {"status": "skipped", "rows": 0, "secs": 0}

    mark(conn, "running")
    t0         = time.time()
    total_rows = 0
    log_every  = max(1, len(ranges) // 10)

    try:
        cur = conn.cursor()
        cur.execute("SET unique_checks = 0")
        cur.execute("SET foreign_key_checks = 0")

        for batch_num, (pk_lo, pk_hi) in enumerate(ranges, 1):
            cur.execute(build_batch_insert(pk_lo, pk_hi))
            conn.commit()
            total_rows += cur.rowcount
            pbar.update(1)

            if batch_num % log_every == 0:
                pct = batch_num / len(ranges) * 100
                logger.info(
                    f"  {batch_num}/{len(ranges)} batches ({pct:.0f}%)  "
                    f"rows so far: {total_rows:,}"
                )

        cur.execute("SET unique_checks = 1")
        cur.execute("SET foreign_key_checks = 1")
        cur.close()

        elapsed = round(time.time() - t0, 1)
        mark(conn, "done", total_rows)
        conn.close()
        logger.info(f"  DONE  {total_rows:,} rows inserted  ({elapsed}s)")
        return {"status": "done", "rows": total_rows, "secs": elapsed}

    except Exception as exc:
        elapsed = round(time.time() - t0, 1)
        mark(conn, "failed", total_rows, str(exc))
        logger.error(
            f"  FAILED after {elapsed}s  rows so far: {total_rows:,}  error: {exc}"
        )
        try:
            conn.close()
        except Exception:
            pass
        return {"status": f"FAILED: {exc}", "rows": total_rows, "secs": elapsed}


# ── Main ──────────────────────────────────────────────────────────────

def main():
    log_path = _setup_logging()

    logger.info("=" * 70)
    logger.info(f"Greenway Family History ETL — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"  log file   : {log_path}")
    logger.info(f"  source     : {SOURCE_SCHEMA}.PatHistFamily  (psid={PSID})")
    logger.info(f"  dest       : {DEST_TABLE}")
    logger.info(f"  cpi_lookup : {STAGING_CPI}")
    logger.info(f"  rel_lookup : {STAGING_REL}")
    logger.info(f"  staging_pk : {STAGING_PK}")
    logger.info(f"  checkpoint : {CHECKPOINT_TABLE}")
    logger.info(f"  batch_size : {BATCH_SIZE:,}")
    logger.info("=" * 70)

    ranges = setup_tables()

    if not ranges:
        logger.info(f"No eligible rows in {SOURCE_SCHEMA}.PatHistFamily. Exiting.")
        return

    logger.info(f"{'─'*70}")
    logger.info(f"Starting INSERT: {len(ranges)} batches  x  {BATCH_SIZE:,} rows/batch")

    result = None
    with tqdm(total=len(ranges), desc="Inserting", unit="batch",
              file=sys.stderr) as pbar:
        result = run_insert(ranges, pbar)

    logger.info("=" * 70)
    logger.info(
        f"Status: {result['status']}  |  "
        f"rows inserted: {result['rows']:,}  ({result['secs']}s)"
    )
    logger.info("=" * 70)

    logger.info("Cleanup SQL (run after verifying data):")
    logger.info(f"  DROP TABLE IF EXISTS {STAGING_CPI};")
    logger.info(f"  DROP TABLE IF EXISTS {STAGING_REL};")
    logger.info(f"  DROP TABLE IF EXISTS {STAGING_PK};")
    logger.info(f"  DROP TABLE IF EXISTS {CHECKPOINT_TABLE};")

    if result["status"].startswith("FAILED"):
        sys.exit(1)


if __name__ == "__main__":
    main()
