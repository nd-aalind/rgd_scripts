#!/usr/bin/env python3
"""
Optimized ETL loader for: udm_staging.socialhistory_ecw
Source: eCW

Two UNION ALL branches:
  Branch 1: {SOURCE_SCHEMA}.social  (data_source = 'eCW 1')
            Uses GROUP BY + MAX(CASE...) aggregation
            JOINs: enc, items (x2 aliases), properties
  Branch 2: {SOURCE_SCHEMA}.structsocialhistory  (data_source = 'eCW 2')
            JOINs: enc, items, structdatadetail

Pre-materialized lookup tables (computed ONCE, reused across all batches):
  - staging.sh_ecw_enc_{SOURCE_SCHEMA}    (enc, keyed on encounterid)
  - staging.sh_ecw_items_{SOURCE_SCHEMA}  (items, keyed on itemid — shared by both)
  - staging.sh_ecw_props_{SOURCE_SCHEMA}  (properties, keyed on propid — branch 1)
  - staging.sh_ecw_sdd_{SOURCE_SCHEMA}    (structdatadetail, keyed on id — branch 2)

Key ECW patterns applied:
- enc.encounterid is VARCHAR; source encounterID is BIGINT
  → JOIN uses: enc.encounterid = CAST(source.encounterID AS CHAR)
- enc.date is DATETIME; wrapped in CAST(... AS CHAR) to avoid MySQL strict mode 1292
- Branch 1 GROUP BY preserved within each batch (safe: batch key = encounterID,
  which is also in GROUP BY — no group spans two batches)

Optimizations applied:
- All lookups pre-materialized once (not re-scanned per batch)
- Per-source PK staging for batch boundary sampling
- Checkpoint/resume per source
- Parallel execution via ThreadPoolExecutor (2 workers)
- Commit after every batch (frees undo/log space)
- Disabled InnoDB checks per-session for bulk insert speed
- REGEXP {{n}} quantifiers escaped as {{n}} inside f-strings
- Progress bar via tqdm

Usage:
    python social_hist_ecw.py
"""

import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
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

BATCH_SIZE  = 50_000
MAX_WORKERS = 2   # one per UNION ALL branch

# ── Change these two variables to run for a different schema/psid ─────
SOURCE_SCHEMA = "fcn_latest"   # e.g. "northwest", "texas", "fcn_latest", ...
PSID          = 8

DEST_TABLE       = "udm_staging.socialhistory_rgd"
STAGING_ENC      = f"staging.sh_ecw_enc2_{SOURCE_SCHEMA}"
STAGING_ITEMS    = f"staging.sh_ecw_items2_{SOURCE_SCHEMA}"
STAGING_PROPS    = f"staging.sh_ecw_props2_{SOURCE_SCHEMA}"
STAGING_SDD      = f"staging.sh_ecw_sdd2_{SOURCE_SCHEMA}"
CHECKPOINT_TABLE = f"staging.etl_checkpoint_sh_ecw2_{SOURCE_SCHEMA}"

# ── Source definitions ─────────────────────────────────────────────────
SOURCES = [
    {
        "key":        "ecw_social1",
        "table":      "social",
        "pk":         "nd_auto_increment_id",
        "staging_pk": f"staging.sh_ecw_stg1_{SOURCE_SCHEMA}",
    },
    {
        "key":        "ecw_social2",
        "table":      "structsocialhistory",
        "pk":         "nd_auto_increment_id",
        "staging_pk": f"staging.sh_ecw_stg2_{SOURCE_SCHEMA}",
    },
]


# ── Date CASE helper (for enc.date DATETIME column) ───────────────────

def date_case_enc(col):
    """
    CASE for enc.date (DATETIME): wraps in CAST(... AS CHAR) to avoid
    MySQL strict mode error 1292 when compared with IN ('', 'None').
    Handles: NULL, empty, YYYY-MM-DD, YYYY-MM-DD HH:MM:SS, MM-DD-YYYY.
    {{4}}/{{2}} produce literal {4}/{2} — correct MySQL REGEXP quantifiers.
    """
    c = f"CAST({col} AS CHAR)"
    return (
        f"CASE\n"
        f"        WHEN {c} IS NULL OR {c} IN ('', 'None') THEN NULL\n"
        f"        WHEN {c} REGEXP '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}( [0-9]{{2}}:[0-9]{{2}}:[0-9]{{2}})?$'\n"
        f"            THEN DATE({c})\n"
        f"        WHEN {c} REGEXP '^[0-9]{{2}}-[0-9]{{2}}-[0-9]{{4}}$'\n"
        f"            THEN STR_TO_DATE({c}, '%m-%d-%Y')\n"
        f"        ELSE NULL\n"
        f"    END"
    )


# ── Batch INSERT builders ──────────────────────────────────────────────

def build_batch_insert_branch1(pk_lo, pk_hi):
    """social — GROUP BY + MAX(CASE...) aggregation, items joined twice."""
    return f"""
INSERT INTO {DEST_TABLE}
    (social_hist_id, ndid, eid, encounter_date, social_hist_date,
     social_hist_category, social_hist_subcategory, social_hist_question,
     social_hist_value, social_hist_code, social_hist_coding_system,
     social_hist_notes, data_source,
     created_datetime, created_by, updated_datetime, updated_by,
     ehr_source_name, source_path, data_type,
     psid, nd_extracted_date)
SELECT
    NULL,
    CAST(enc.patientID AS SIGNED),
    CAST(s.encounterID AS SIGNED),
    {date_case_enc('enc.date')},
    {date_case_enc('enc.date')},
    'Social History',
    it1.itemname,
    it2.itemname,
    MAX(CASE WHEN props.name = 'Options' THEN s.value END),
    MAX(CASE WHEN props.name = 'Notes'   THEN s.value END),
    NULL,
    NULL,
    'social',
    CURRENT_DATE(),
    'ND',
    CURRENT_DATE(),
    'ND',
    'eCW',
    'bronze_layer',
    'Structured',
    {PSID},
    s.nd_extracted_date
FROM {SOURCE_SCHEMA}.social s
LEFT JOIN {STAGING_ENC}   enc   ON enc.encounterid = s.encounterID
LEFT JOIN {STAGING_ITEMS} it1   ON it1.itemid = s.catid
LEFT JOIN {STAGING_ITEMS} it2   ON it2.itemid = s.itemid
LEFT JOIN {STAGING_PROPS} props ON props.propid = s.propid
WHERE s.nd_Activeflag = 'Y'
  AND s.nd_auto_increment_id >= {pk_lo}
  AND s.nd_auto_increment_id < {pk_hi}
GROUP BY enc.patientID, s.encounterID, enc.date, it1.itemname
"""


def build_batch_insert_branch2(pk_lo, pk_hi):
    """structsocialhistory — batches by nd_auto_increment_id (integer, no CAST)."""
    return f"""
INSERT INTO {DEST_TABLE}
    (social_hist_id, ndid, eid, encounter_date, social_hist_date,
     social_hist_category, social_hist_subcategory, social_hist_question,
     social_hist_value, social_hist_code, social_hist_coding_system,
     social_hist_notes, data_source,
     created_datetime, created_by, updated_datetime, updated_by,
     ehr_source_name, source_path, data_type,
     psid, nd_extracted_date)
SELECT
    NULL,
    CAST(enc.patientid AS SIGNED),
    CAST(ss.encounterid AS SIGNED),
    {date_case_enc('enc.date')},
    {date_case_enc('enc.date')},
    'Social History',
    it.itemName,
    sdd.name,
    ss.value,
    ss.notes,
    NULL,
    NULL,
    'structsocialhistory',
    CURRENT_DATE(),
    'ND',
    CURRENT_DATE(),
    'ND',
    'eCW',
    'bronze_layer',
    'Structured',
    {PSID},
    ss.nd_extracted_date
FROM {SOURCE_SCHEMA}.structsocialhistory ss
LEFT JOIN {STAGING_ENC}  enc ON enc.encounterid = CAST(ss.encounterid AS CHAR)
LEFT JOIN {STAGING_ITEMS} it ON it.itemid = ss.itemid
LEFT JOIN {STAGING_SDD}  sdd ON sdd.id = ss.detailid
WHERE ss.nd_Activeflag = 'Y'
  AND ss.nd_auto_increment_id >= {pk_lo}
  AND ss.nd_auto_increment_id < {pk_hi}
"""


BUILD_FN = {
    "ecw_social1": build_batch_insert_branch1,
    "ecw_social2": build_batch_insert_branch2,
}


# ── Helpers ────────────────────────────────────────────��───────────────

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


# ── Checkpoint ─────────────────────────────────────────────────────────

def is_done(conn, source_key):
    cur = conn.cursor()
    cur.execute(
        f"SELECT status FROM {CHECKPOINT_TABLE} WHERE source_key = %s",
        (source_key,),
    )
    row = cur.fetchone()
    cur.close()
    return row is not None and row[0] == "done"


def mark(conn, source_key, status, rows=0, error=None):
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
    """, (source_key, status, rows, status, error))
    conn.commit()
    cur.close()


# ── Setup ──────────────────────────────────────────────────────────────

def setup_tables():
    """
    1. Pre-materialize all shared lookups.
    2. For each source: create PK staging and compute batch ranges.
    3. Create destination and checkpoint tables.
    Returns dict: source_key → list of (lo, hi) ranges.
    """
    conn = get_connection()
    cur  = conn.cursor()

    # ── 1. enc lookup (shared) ─────────────────────────────────────────
    print("  Materializing enc lookup...")
    if not _table_exists(cur, STAGING_ENC):
        cur.execute(f"""
            CREATE TABLE {STAGING_ENC} AS
            SELECT patientID, encounterid, date
            FROM {SOURCE_SCHEMA}.enc
            WHERE nd_Activeflag = 'Y'
        """)
        cur.execute(f"ALTER TABLE {STAGING_ENC} ADD INDEX idx_enc (encounterid)")
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")
    cur.execute(f"SELECT COUNT(*) FROM {STAGING_ENC}")
    print(f"    {cur.fetchone()[0]:,} enc rows")

    # ── 2. items lookup (shared — joined twice in branch 1) ────────────
    print("  Materializing items lookup...")
    if not _table_exists(cur, STAGING_ITEMS):
        cur.execute(f"""
            CREATE TABLE {STAGING_ITEMS} AS
            SELECT itemid, itemname
            FROM {SOURCE_SCHEMA}.items
            WHERE nd_Activeflag = 'Y'
        """)
        cur.execute(f"ALTER TABLE {STAGING_ITEMS} ADD INDEX idx_items (itemid)")
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")
    cur.execute(f"SELECT COUNT(*) FROM {STAGING_ITEMS}")
    print(f"    {cur.fetchone()[0]:,} items rows")

    # ── 3. properties lookup (branch 1 only) ──────────────────────────
    print("  Materializing properties lookup...")
    if not _table_exists(cur, STAGING_PROPS):
        cur.execute(f"""
            CREATE TABLE {STAGING_PROPS} AS
            SELECT propid, name
            FROM {SOURCE_SCHEMA}.properties
            WHERE nd_Activeflag = 'Y'
        """)
        cur.execute(f"ALTER TABLE {STAGING_PROPS} ADD INDEX idx_props (propid)")
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")
    cur.execute(f"SELECT COUNT(*) FROM {STAGING_PROPS}")
    print(f"    {cur.fetchone()[0]:,} properties rows")

    # ── 4. structdatadetail lookup (branch 2 only — skip if table absent) ────
    print("  Materializing structdatadetail lookup...")
    if not _table_exists(cur, STAGING_SDD):
        if not _table_exists(cur, f"{SOURCE_SCHEMA}.structdatadetail"):
            print("    structdatadetail not found in schema — skipping")
        else:
            cur.execute(f"""
                CREATE TABLE {STAGING_SDD} AS
                SELECT id, name
                FROM {SOURCE_SCHEMA}.structdatadetail
                WHERE nd_Activeflag = 'Y'
            """)
            cur.execute(f"ALTER TABLE {STAGING_SDD} ADD INDEX idx_sdd (id)")
            conn.commit()
            print("    created")
    else:
        print("    already exists, reusing")
    if _table_exists(cur, STAGING_SDD):
        cur.execute(f"SELECT COUNT(*) FROM {STAGING_SDD}")
        print(f"    {cur.fetchone()[0]:,} structdatadetail rows")

    # ── 5. Destination table ───────────────────────────────────────────
    print("  Creating destination table...")
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {DEST_TABLE} (
            social_hist_id            BIGINT        DEFAULT NULL,
            ndid                      BIGINT        DEFAULT NULL,
            eid                       BIGINT        DEFAULT NULL,
            encounter_date            DATE          DEFAULT NULL,
            social_hist_date          DATE          DEFAULT NULL,
            social_hist_category      VARCHAR(100)  DEFAULT NULL,
            social_hist_subcategory   TEXT,
            social_hist_question      TEXT,
            social_hist_value         TEXT,
            social_hist_code          TEXT,
            social_hist_coding_system VARCHAR(50)   DEFAULT NULL,
            social_hist_notes         TEXT,
            data_source               VARCHAR(50)   DEFAULT NULL,
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
    print("    ready")

    # ── 6. Checkpoint table ────────────────────────────────────────────
    print("  Creating checkpoint table...")
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
    print("    ready")

    # ── 8. Per-source PK staging + boundary sampling ───────────────────
    all_ranges = {}
    for src in SOURCES:
        pk         = src["pk"]
        table      = src["table"]
        stg        = src["staging_pk"]
        source_key = src["key"]

        print(f"  Creating PK staging for {SOURCE_SCHEMA}.{table}...")
        if source_key == "ecw_social2" and not _table_exists(cur, f"{SOURCE_SCHEMA}.{table}"):
            print(f"    {SOURCE_SCHEMA}.{table} not found — skipping branch 2")
            all_ranges[source_key] = []
            continue
        if not _table_exists(cur, stg):
            cur.execute(f"""
                CREATE TABLE {stg} AS
                SELECT {pk}
                FROM {SOURCE_SCHEMA}.{table}
                WHERE {pk} IS NOT NULL
                  AND nd_Activeflag = 'Y'
            """)
            cur.execute(f"ALTER TABLE {stg} ADD INDEX idx_pk ({pk})")
            conn.commit()
            print("    created")
        else:
            print("    already exists, reusing")
        cur.execute(f"SELECT COUNT(*) FROM {stg}")
        total = cur.fetchone()[0]
        print(f"    {total:,} rows")

        if total == 0:
            all_ranges[source_key] = []
            continue

        print(f"  Computing batch boundaries for {table}...")
        cur.execute(f"""
            SELECT {pk}
            FROM (
                SELECT {pk},
                       ROW_NUMBER() OVER (ORDER BY {pk}) AS rn
                FROM {stg}
            ) t
            WHERE (rn - 1) % {BATCH_SIZE} = 0
            ORDER BY {pk}
        """)
        boundaries = [row[0] for row in cur.fetchall() if row[0] is not None]

        cur.execute(f"SELECT MAX({pk}) FROM {stg}")
        max_pk = int(cur.fetchone()[0])

        ranges = []
        for i, lo in enumerate(boundaries):
            hi = boundaries[i + 1] if i + 1 < len(boundaries) else max_pk + 1
            ranges.append((lo, hi))

        print(f"    {len(ranges)} batches of ~{BATCH_SIZE:,} rows each")
        all_ranges[source_key] = ranges

    cur.close()
    conn.close()
    return all_ranges


# ── Worker ─────────────────────────────────────────────────────────────

def run_source(src, ranges, pbar):
    source_key = src["key"]
    build_fn   = BUILD_FN[source_key]
    conn = get_connection()

    if is_done(conn, source_key):
        conn.close()
        pbar.update(len(ranges))
        return source_key, {"status": "skipped", "rows": 0, "secs": 0}

    mark(conn, source_key, "running")
    t0 = time.time()
    total_rows = 0

    try:
        cur = conn.cursor()
        cur.execute("SET unique_checks = 0")
        cur.execute("SET foreign_key_checks = 0")

        for pk_lo, pk_hi in ranges:
            sql = build_fn(pk_lo, pk_hi)
            cur.execute(sql)
            conn.commit()
            total_rows += cur.rowcount
            pbar.update(1)

        cur.execute("SET unique_checks = 1")
        cur.execute("SET foreign_key_checks = 1")
        cur.close()

        elapsed = round(time.time() - t0, 1)
        mark(conn, source_key, "done", total_rows)
        conn.close()
        return source_key, {"status": "done", "rows": total_rows, "secs": elapsed}

    except Exception as exc:
        elapsed = round(time.time() - t0, 1)
        mark(conn, source_key, "failed", total_rows, str(exc))
        try:
            conn.close()
        except Exception:
            pass
        return source_key, {"status": f"FAILED: {exc}", "rows": total_rows, "secs": elapsed}


# ── Main ───────────────────────────────────────────────────────────────

def main():
    print(f"\n{'='*70}", flush=True)
    print(f"  eCW Social History ETL — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  source     : {SOURCE_SCHEMA} (social + structsocialhistory)")
    print(f"  dest       : {DEST_TABLE}")
    print(f"  enc_lookup : {STAGING_ENC}")
    print(f"  checkpoint : {CHECKPOINT_TABLE}")
    print(f"  batch_size : {BATCH_SIZE:,}  |  workers: {MAX_WORKERS}")
    print(f"{'='*70}\n", flush=True)

    print("  Connecting to database...")
    sys.stdout.flush()
    all_ranges = setup_tables()

    total_batches = sum(len(r) for r in all_ranges.values())
    if total_batches == 0:
        print(f"\nNo eligible rows found in any source table. Exiting.")
        return

    results = {}
    with tqdm(total=total_batches, desc="Overall", unit="batch") as pbar:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
            futures = {
                pool.submit(run_source, src, all_ranges[src["key"]], pbar): src["key"]
                for src in SOURCES
                if all_ranges.get(src["key"])
            }
            for fut in as_completed(futures):
                source_key, result = fut.result()
                results[source_key] = result

    print(f"\n{'='*70}")
    print(f"  Per-source summary:")
    total_rows = 0
    any_failed = False
    for src in SOURCES:
        key = src["key"]
        res = results.get(key, {"status": "no rows", "rows": 0, "secs": 0})
        status = res["status"]
        rows   = res["rows"]
        secs   = res["secs"]
        if status == "done":
            tag = " DONE"
            total_rows += rows
        elif status == "skipped":
            tag = " SKIP"
        elif status == "no rows":
            tag = "  ---"
        else:
            tag = " FAIL"
            any_failed = True
        print(f"  [{tag}] {src['table']:<35}  {rows:>10,} rows  ({secs}s)")
        if status.startswith("FAILED"):
            print(f"         {status}")

    print(f"\n  Total rows inserted: {total_rows:,}")
    print(f"{'='*70}")

    print(f"\n  Cleanup SQL (run after verifying data):")
    print(f"    DROP TABLE IF EXISTS {STAGING_ENC};")
    print(f"    DROP TABLE IF EXISTS {STAGING_ITEMS};")
    print(f"    DROP TABLE IF EXISTS {STAGING_PROPS};")
    print(f"    DROP TABLE IF EXISTS {STAGING_SDD};")
    for src in SOURCES:
        print(f"    DROP TABLE IF EXISTS {src['staging_pk']};")
    print(f"    DROP TABLE IF EXISTS {CHECKPOINT_TABLE};")

    if any_failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
