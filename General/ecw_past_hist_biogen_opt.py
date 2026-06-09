#!/usr/bin/env python3
"""
Optimized ETL: Build ECW past history records for biogen from a source schema.

Source tables (all under SOURCE_SCHEMA):
  enc, encounterdata, surgicalhistory, family, social, structsocialhistory, items

Target table: DEST_TABLE (INSERT, not recreate — append mode per source run)

Configure at the top before running:
  SOURCE_SCHEMA    — e.g. "texas", "kinsula_leq"
  PATIENT_PROVIDER — value to match biogen.patient_list.patient_provider
  USE_ACTIVE_FLAG  — True: adds nd_ActiveFlag='Y' filter on enc/encounterdata
                     False: no active flag filter (for schemas that don't have it)
  DEST_TABLE       — target table to INSERT into

Strategy:
  1. Pre-materialize GROUP_CONCAT CTEs (surgical, family, social) ONCE into staging.
  2. Pre-materialize eligible encounter PKs (joined to biogen.patient_list, visit_date not null).
  3. Batch INSERT the final JOIN result by encounterID ranges.

Optimizations applied:
- GROUP_CONCAT CTEs materialized once (not re-run per batch)
- SET SESSION group_concat_max_len = 1 GB (avoids row-cut errors)
- Per-source PK staging for batch boundary sampling
- Commit after every batch (frees undo/log space)
- Checkpoint/resume — re-run skips if already completed
- Disabled InnoDB checks per-session for bulk insert speed
- Progress bar via tqdm

Usage:
    python ecw_past_hist_biogen_opt.py
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
    "database":        "fcn_latest",
    "charset":         "utf8mb4",
    "connect_timeout": 30,
    "read_timeout":    21600,
    "write_timeout":   21600,
}

SOURCE_SCHEMA    = "fcn_latest"              # ← change per run
PATIENT_PROVIDER = "FCN"             # ← change per run (matches biogen.patient_list.patient_provider)
USE_ACTIVE_FLAG  = True                # ← set False if schema lacks nd_ActiveFlag column
DEST_TABLE       = "biogen_april.pasthistory_ecw_may_fcn"  # ← target table

BATCH_SIZE = 50_000
BATCH_KEY  = "encounterID"

# ── Derived staging / checkpoint names (namespaced by source schema) ──
_sfx = SOURCE_SCHEMA

STAGING_SURGICAL = f"staging.ecw_ph_surgical_n_{_sfx}"
STAGING_FAMILY   = f"staging.ecw_ph_family_n_{_sfx}"
STAGING_SOCIAL   = f"staging.ecw_ph_social_n_{_sfx}"
STAGING_PK       = f"staging.ecw_ph_pk_n_{_sfx}"

CHECKPOINT_TABLE = f"staging.etl_checkpoint_ecw_phn_{_sfx}"
CHECKPOINT_KEY   = f"ecw_ph.{_sfx}"

PATIENT_LIST_TABLE = "biogen_april.biogen_ecw_pl_may_new_2"


# ── Active flag helper ─────────────────────────────────────────────────

def _active(alias=None):
    """Returns WHERE/AND clause fragment for nd_ActiveFlag if enabled."""
    if not USE_ACTIVE_FLAG:
        return ""
    col = f"{alias}.nd_ActiveFlag" if alias else "nd_ActiveFlag"
    return f" AND {col} = 'Y'"



# ── Date-parse CASE helper ─────────────────────────────────────────────

def _date_case(col):
    return (
        f"CASE "
        f"WHEN {col} IN ('None', '') THEN NULL "
        f"WHEN LEFT({col}, 10) REGEXP '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}$' "
        f"    THEN STR_TO_DATE(LEFT({col}, 10), '%Y-%m-%d') "
        f"WHEN LEFT({col}, 10) REGEXP '^[0-9]{{2}}-[0-9]{{2}}-[0-9]{{4}}$' "
        f"    THEN STR_TO_DATE(LEFT({col}, 10), '%m-%d-%Y') "
        f"ELSE NULL END"
    )


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


# ── Batch INSERT builder ───────────────────────────────────────────────

def build_batch_insert(pk_lo, pk_hi):
    return f"""
INSERT INTO {DEST_TABLE}
SELECT
    pk.patientid,
    pk.encounterID,
    pk.visit_date,
    CASE WHEN ed.pasthistory IN ('None') THEN NULL
         ELSE ed.pasthistory END                                     AS medical_history,
    sh.past_surgical_history,
    f.family_history_notes,
    s.social_history_full
FROM {STAGING_PK} pk
LEFT JOIN {SOURCE_SCHEMA}.encounterdata ed
    ON pk.{BATCH_KEY} = ed.encounterid{_active('ed')}
LEFT JOIN {STAGING_SURGICAL} sh ON pk.{BATCH_KEY} = sh.encounterID
LEFT JOIN {STAGING_FAMILY}   f  ON pk.{BATCH_KEY} = f.encounterid
LEFT JOIN {STAGING_SOCIAL}   s  ON pk.{BATCH_KEY} = s.encounterid
WHERE pk.{BATCH_KEY} >= {pk_lo}
  AND pk.{BATCH_KEY} <  {pk_hi}
"""


# ── Setup ──────────────────────────────────────────────────────────────

def _materialize(cur, conn, name, label, sql, extra_indexes=None):
    """Create a staging table if not present or empty."""
    if _table_exists(cur, name):
        cur.execute(f"SELECT COUNT(*) FROM {name}")
        if cur.fetchone()[0] > 0:
            print(f"    {label}: already exists, reusing")
            return
        print(f"    found empty (prior failed run) — dropping...")
        cur.execute(f"DROP TABLE {name}")
        conn.commit()

    print(f"    Materializing {label}...")
    cur.execute(f"CREATE TABLE {name} AS {sql}")
    cur.execute(f"ALTER TABLE {name} ADD INDEX idx_enc (encounterID)")
    if extra_indexes:
        for idx in extra_indexes:
            cur.execute(f"ALTER TABLE {name} ADD INDEX {idx}")
    conn.commit()
    cur.execute(f"SELECT COUNT(*) FROM {name}")
    print(f"      {cur.fetchone()[0]:,} rows")


def setup_tables():
    conn = get_connection()
    cur  = conn.cursor()

    # Ensure staging schema exists
    cur.execute("CREATE DATABASE IF NOT EXISTS staging")
    conn.commit()

    # Increase GROUP_CONCAT limit for this session
    cur.execute("SET SESSION group_concat_max_len = 1073741824")  # 1 GB

    # ── 1. Destination table — create if not exists (append mode) ─────
    print(f"  Checking destination table {DEST_TABLE}...")
    if not _table_exists(cur, DEST_TABLE):
        cur.execute(f"""
            CREATE TABLE {DEST_TABLE} (
                patientid            BIGINT,
                encounterid          BIGINT,
                visit_date           DATE,
                medical_history      LONGTEXT,
                past_surgical_history LONGTEXT,
                family_history_notes LONGTEXT,
                social_history_full  LONGTEXT
            ) ENGINE=InnoDB ROW_FORMAT=DYNAMIC
        """)
        conn.commit()
        print(f"    created (empty)")
    else:
        print(f"    already exists — will append")

    # ── 2. Surgical staging ──────────────────────────────────────────
    _materialize(cur, conn, STAGING_SURGICAL, "surgical_cte", f"""
        SELECT
            encounterID,
            GROUP_CONCAT(CONCAT(COALESCE(date, ''), ' ^ ', COALESCE(reason, ''))
                         SEPARATOR ' + ') AS past_surgical_history
        FROM {SOURCE_SCHEMA}.surgicalhistory
        GROUP BY encounterID
    """)

    # ── 3. Family staging ────────────────────────────────────────────
    _materialize(cur, conn, STAGING_FAMILY, "family_cte", f"""
        SELECT
            encounterid,
            GROUP_CONCAT(CONCAT(COALESCE(name, ''), ' ^ ', COALESCE(notes, ''))
                         SEPARATOR ' + ') AS family_history_notes
        FROM {SOURCE_SCHEMA}.family
        GROUP BY encounterid
    """)

    # ── 4. Social staging (social + structsocialhistory both joined to items) ──
    _materialize(cur, conn, STAGING_SOCIAL, "social_cte", f"""
        SELECT
            encounterid,
            GROUP_CONCAT(CONCAT(COALESCE(itemname, ''), ' ^ ', COALESCE(notes, ''))
                         SEPARATOR ' + ') AS social_history_full
        FROM (
            SELECT a.encounterid, b.itemname, a.value AS notes
            FROM {SOURCE_SCHEMA}.social a
            LEFT JOIN {SOURCE_SCHEMA}.items b ON a.itemid = b.itemid
            UNION ALL
            SELECT a.encounterid, b.itemname, a.value AS notes
            FROM {SOURCE_SCHEMA}.structsocialhistory a
            LEFT JOIN {SOURCE_SCHEMA}.items b ON a.itemid = b.itemid
        ) combined_social
        GROUP BY encounterid
    """)

    # ── 5. PK staging — eligible encounters (patient_list join + filters) ──
    print(f"  Creating PK staging...")
    needs_create = False
    if not _table_exists(cur, STAGING_PK):
        needs_create = True
    else:
        cur.execute(f"SELECT COUNT(*) FROM {STAGING_PK}")
        if cur.fetchone()[0] == 0:
            cur.execute(f"DROP TABLE {STAGING_PK}")
            conn.commit()
            needs_create = True
        else:
            print(f"    already exists, reusing")

    if needs_create:
        visit_date = _date_case("CAST(e.date AS CHAR)")
        active_filter = _active("e")
        # Use ndid_v1 for Dent EMD, ndid for all other providers
        pl_join_col = "ndid_v1" if PATIENT_PROVIDER == "Dent EMD" else "ndid"
        cur.execute(f"""
            CREATE TABLE {STAGING_PK} AS
            SELECT e.encounterID, ({visit_date}) AS visit_date, e.patientid
            FROM {SOURCE_SCHEMA}.enc e
            INNER JOIN {PATIENT_LIST_TABLE} pl
                ON e.patientid = pl.{pl_join_col}
               AND pl.patient_provider = '{PATIENT_PROVIDER}'
            WHERE ({visit_date}) IS NOT NULL
              AND e.encounterID IS NOT NULL
              {active_filter}
        """)
        cur.execute(f"ALTER TABLE {STAGING_PK} ADD INDEX idx_pk ({BATCH_KEY})")
        conn.commit()
        cur.execute(f"SELECT COUNT(*) FROM {STAGING_PK}")
        print(f"    {cur.fetchone()[0]:,} eligible encounters")

    # ── 6. Checkpoint table ──────────────────────────────────────────
    print(f"  Creating checkpoint table...")
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
    print(f"    ready")

    # ── 7. Batch ranges ──────────────────────────────────────────────
    ranges, total = _build_ranges(cur, STAGING_PK)
    print(f"    {total:,} rows → {len(ranges)} batches of ~{BATCH_SIZE:,}")

    cur.close()
    conn.close()
    return ranges


# ── Runner ─────────────────────────────────────────────────────────────

def run_insert(ranges, pbar):
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
            sql = build_batch_insert(pk_lo, pk_hi)
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
    print(f"  ECW Past History Biogen ETL �� {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  source_schema    : {SOURCE_SCHEMA}")
    print(f"  patient_provider : {PATIENT_PROVIDER}")
    print(f"  use_active_flag  : {USE_ACTIVE_FLAG}")
    print(f"  dest             : {DEST_TABLE}")
    print(f"  batch_size       : {BATCH_SIZE:,}")
    print(f"{'='*70}\n", flush=True)

    print("  Setting up tables...", flush=True)
    ranges = setup_tables()

    if not ranges:
        print(f"\n  No eligible encounters found. Exiting.")
        return

    result = None
    with tqdm(total=len(ranges), desc=SOURCE_SCHEMA, unit="batch") as pbar:
        result = run_insert(ranges, pbar)

    print()
    if result["status"] == "done":
        tag = " DONE"
    elif result["status"] == "skipped":
        tag = " SKIP"
    else:
        tag = " FAIL"

    print(f"\n{'='*70}")
    print(f"  [{tag}] {SOURCE_SCHEMA:<15}  {result['rows']:>10,} rows inserted  ({result['secs']}s)")
    if result["status"].startswith("FAILED"):
        print(f"  ERROR: {result['status']}")
    print(f"{'='*70}")

    print(f"\n  Cleanup SQL (run after verifying data):")
    for t in [STAGING_SURGICAL, STAGING_FAMILY, STAGING_SOCIAL,
              STAGING_PK, CHECKPOINT_TABLE]:
        print(f"    DROP TABLE IF EXISTS {t};")

    if result["status"].startswith("FAILED"):
        sys.exit(1)


if __name__ == "__main__":
    main()
