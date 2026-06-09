#!/usr/bin/env python3
"""
Optimized Family History ETL for: udm_staging.athenaone_familyhistory
Source: eCW

Source (single INSERT job):
  familyhxdetails f
    LEFT JOIN enc   ON f.encounterid = enc.encounterid
    LEFT JOIN items ON items.itemID  = f.itemid

Pre-materialized lookup tables (computed ONCE, reused across all batches):
  - staging.fh_ecw_enc_{SOURCE_SCHEMA}    (enc, keyed on encounterid)
  - staging.fh_ecw_items_{SOURCE_SCHEMA}  (items, keyed on itemID)

Batch key: SlNo (CAST AS SIGNED to handle TEXT PK, avoids error 1170)

Optimizations applied:
- enc lookup pre-materialized once (not re-scanned per batch)
- Source batched by SlNo (CAST AS SIGNED)
- Server-side boundary sampling (avoids loading all PKs into memory)
- Commit after every batch (frees undo/log space)
- Checkpoint/resume — re-run skips if already completed
- Disabled InnoDB checks per-session for bulk insert speed
- REGEXP {n} quantifiers escaped as {{n}} inside f-strings
- Progress bar via tqdm

Usage:
    python opt_fami_hist_ecw.py
"""

import sys
import time
from datetime import datetime
import pymysql
from tqdm import tqdm

# ── Configuration ────────────────────────────────────────────────────
DB_CONFIG = {
    "host":            "ndai-dev-rds-instance.cwp60ymu4ko0.us-east-1.rds.amazonaws.com",
    "port":            3306,
    "user":            "Aalind",
    "password":        "A@L1nd@123",
    "database":        'udm_staging',
    "charset":         "utf8mb4",
    "connect_timeout": 30,
    "read_timeout":    21600,
    "write_timeout":   21600,
}

BATCH_SIZE = 50_000

# ── Change these two variables to run for a different schema/psid ────
SOURCE_SCHEMA = "dent"   # e.g. "northwest", ...
PSID          = 1

DEST_TABLE       = "udm_staging.familyhistory_final"
STAGING_ENC      = f"staging.fh_ecw_enc_v2_{SOURCE_SCHEMA}"
STAGING_ITEMS    = f"staging.fh_ecw_items_v2_{SOURCE_SCHEMA}"
STAGING_PK       = f"staging.tmp_fh_ecw_staging_v2_{SOURCE_SCHEMA}"
CHECKPOINT_TABLE = f"staging.etl_checkpoint_fh_ecw__v5_{SOURCE_SCHEMA}"
CHECKPOINT_KEY   = f"familyhistory1.ecw.insert.{SOURCE_SCHEMA}"

BATCH_KEY = "encounterid"


# ── Date CASE helper ─────────────────────────────────────────────────

def date_case_enc(col):
    """
    CASE for enc.encounter_date (pre-computed DATE in STAGING_ENC).
    Wraps col in CAST(... AS CHAR) so DATETIME columns don't trigger
    MySQL strict mode error 1292 when compared with IN ('', 'None').
    {{4}}/{{2}} produce literal {4}/{2} for MySQL REGEXP quantifiers.
    """
    c = f"CAST({col} AS CHAR)"
    return (
        f"CASE\n"
        f"            WHEN {c} IS NULL OR {c} IN ('', 'None') THEN NULL\n"
        f"            WHEN {c} REGEXP '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}'\n"
        f"                THEN DATE({c})\n"
        f"            WHEN {c} REGEXP '^[0-9]{{2}}-[0-9]{{2}}-[0-9]{{4}}'\n"
        f"                THEN STR_TO_DATE({c}, '%m-%d-%Y')\n"
        f"            ELSE NULL\n"
        f"        END"
    )


# ── Index helper ──────────────────────────────────────────────────────

def _ensure_index(cur, conn, full_table_name, index_name, columns, prefix_len=None):
    """Creates index on full_table_name if it does not already exist."""
    schema, table = full_table_name.split(".", 1)
    cur.execute(
        "SELECT COUNT(*) FROM information_schema.statistics "
        "WHERE table_schema = %s AND table_name = %s AND index_name = %s",
        (schema, table, index_name),
    )
    if cur.fetchone()[0] > 0:
        print(f"    index {index_name} on {full_table_name} already exists — skipping")
        return
    col_list = ", ".join(f"{c}({prefix_len})" if prefix_len else c for c in columns)
    print(f"    creating index {index_name} on {full_table_name}({col_list}) ...")
    cur.execute(f"ALTER TABLE {full_table_name} ADD INDEX {index_name} ({col_list})")
    conn.commit()
    print(f"    done")


# ── Batch INSERT builder ──────────────────────────────────────────────

def build_batch_insert(pk_lo, pk_hi):
    """
    Inserts one batch into the unified family history table.
    Column order matches optimise_family_history_athenaone.py exactly.
    enc.nd_Activeflag='Y' baked into STAGING_ENC at materialization time.
    items.nd_ActiveFlag='Y' baked into STAGING_ITEMS at materialization time.
    fhx.nd_Activeflag='Y' applied in WHERE clause.
    """
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
    NULL,
    CAST(enc.patientID AS SIGNED),
    CAST(fhx.encounterid AS SIGNED),
    {date_case_enc('enc.encounter_date')},
    NULL,
    fhx.diagnosedAge,
    {date_case_enc('enc.encounter_date')},
    'Family History',
    fhx.name,
    CASE
        WHEN TRIM(UPPER(fhx.name)) = 'MOTHER'       THEN '32'
        WHEN TRIM(UPPER(fhx.name)) = 'FATHER'       THEN '33'
        WHEN TRIM(UPPER(fhx.name)) = 'UNSPECIFIED'  THEN '21'
        WHEN TRIM(UPPER(fhx.name)) IN ('MATERNALAUNT','SISTER','BROTHER',
             'MATERNALUNCLE','PATERNALAUNT','PATERNALUNCLE',
             'SIBLINGS','PATERNAL AUNT','MATERNAL AUNT')           THEN 'G8'
        WHEN TRIM(UPPER(fhx.name)) IN ('SON','DAUGHTER')           THEN '19'
        WHEN TRIM(UPPER(fhx.name)) IN ('PATERNALGRANDFATHER','PATERNALGRANDMOTHER',
             'GRANDPARENTS','MATERNALGRANDMOTHER','MATERNALGRANDFATHER',
             'MATERNAL GRAND MOTHER','PATERNAL GRAND MOTHER',
             'PATERNAL GRAND FATHER','MATERNAL GRAND FATHER')      THEN '4'
        WHEN TRIM(UPPER(fhx.name)) IN ('NONCONTRIBUTORY')          THEN '21'
    END,
    NULL,
    CAST(COALESCE(fhx.icdCode, fhx.snomedCode) AS CHAR(50)),
    CASE
        WHEN fhx.icdCode    IS NOT NULL THEN 'ICD'
        WHEN fhx.snomedCode IS NOT NULL THEN 'SNOMED'
        ELSE NULL
    END,
    fhx.diagnosedYear,
    fhx.icdDesc,
    it.itemname,
    it.itemdesc,
    CURRENT_TIMESTAMP(),
    'ND',
    CURRENT_TIMESTAMP(),
    'ND',
    'eCW',
    'bronze_layer',
    'Structured',
    {PSID},
    fhx.nd_extracted_date
FROM {SOURCE_SCHEMA}.familyhxdetails fhx
LEFT JOIN {STAGING_ENC}   enc ON enc.encounterid = CAST(fhx.encounterid AS CHAR(100))
LEFT JOIN {STAGING_ITEMS} it  ON it.itemID       = fhx.itemid
WHERE fhx.nd_Activeflag = 'Y'
  AND CAST(fhx.{BATCH_KEY} AS SIGNED) >= {pk_lo}
  AND CAST(fhx.{BATCH_KEY} AS SIGNED) <  {pk_hi}
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
    """Create lookup, PK staging, destination, checkpoint tables. Return batch ranges."""
    conn = get_connection()
    cur  = conn.cursor()

    # ── 1. Ensure indexes on source join/filter columns ──────────────
    # encounterid is the JOIN key; nd_Activeflag is filtered in every batch.
    print("  Ensuring indexes on familyhxdetails...")
    _ensure_index(cur, conn, f"{SOURCE_SCHEMA}.familyhxdetails",
                  "idx_encounterid",  ["encounterid"])
    _ensure_index(cur, conn, f"{SOURCE_SCHEMA}.familyhxdetails",
                  "idx_nd_activeflag", ["nd_Activeflag"])

    # ── 2. Pre-materialized enc lookup (active rows only) ─────────────
    # enc.nd_Activeflag='Y' baked in here — avoids re-scanning the full
    # enc table on every batch JOIN. encounter_date pre-computed as DATE
    # to sidestep DATETIME → '' cast issues in batch CASE expressions.
    print("  Materializing enc lookup (nd_Activeflag='Y')...")
    if not _table_exists(cur, STAGING_ENC):
        cur.execute(f"""
            CREATE TABLE {STAGING_ENC} AS
            SELECT
                CAST(encounterid AS CHAR(100)) AS encounterid,
                patientID,
                DATE(date)                     AS encounter_date
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

    # ── 3. Pre-materialized items lookup (active rows only) ───────────
    print("  Materializing items lookup (nd_ActiveFlag='Y')...")
    if not _table_exists(cur, STAGING_ITEMS):
        cur.execute(f"""
            CREATE TABLE {STAGING_ITEMS} AS
            SELECT itemID, itemname, itemdesc
            FROM {SOURCE_SCHEMA}.items
            WHERE nd_ActiveFlag = 'Y'
        """)
        cur.execute(f"ALTER TABLE {STAGING_ITEMS} ADD INDEX idx_itemid (itemID)")
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")
    cur.execute(f"SELECT COUNT(*) FROM {STAGING_ITEMS}")
    print(f"    {cur.fetchone()[0]:,} items rows")

    # ── 4. Destination table ──────────────────────────────────────────
    # Schema matches optimise_family_history_athenaone.py exactly.
    print("  Creating destination table...")
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
    print("    ready")

    # ── 5. Checkpoint table ──────────────────────────────────────────
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

    # ── 6. PK staging table (CAST encounterid AS SIGNED — TEXT col) ────
    # Also filters nd_Activeflag='Y' so PK boundaries match what the
    # batch INSERT will actually process.
    print("  Creating PK staging for familyhxdetails...")
    if not _table_exists(cur, STAGING_PK):
        cur.execute(f"""
            CREATE TABLE {STAGING_PK} AS
            SELECT CAST({BATCH_KEY} AS SIGNED) AS {BATCH_KEY}
            FROM {SOURCE_SCHEMA}.familyhxdetails
            WHERE {BATCH_KEY} IS NOT NULL
              AND nd_Activeflag = 'Y'
        """)
        cur.execute(f"ALTER TABLE {STAGING_PK} ADD INDEX idx_pk ({BATCH_KEY})")
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")

    cur.execute(f"SELECT COUNT(*) FROM {STAGING_PK}")
    total = cur.fetchone()[0]
    print(f"    {total:,} rows to insert")

    # ── 6. Batch boundary sampling ────────────────────────────────────
    print("  Computing batch boundaries...")
    sys.stdout.flush()

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

    cur.close()
    conn.close()

    ranges = []
    for i, lo in enumerate(boundaries):
        hi = boundaries[i + 1] if i + 1 < len(boundaries) else max_pk + 1
        ranges.append((lo, hi))

    print(f"    {len(ranges)} batches of ~{BATCH_SIZE:,} rows each")
    return ranges


# ── Runner ────────────────────────────────────────────────────────────

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


# ── Main ──────────────────────────────────────────────────────────────

def main():
    print(f"\n{'='*70}", flush=True)
    print(f"  eCW Family History ETL — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  source     : {SOURCE_SCHEMA}.familyhxdetails  (psid={PSID})")
    print(f"  dest       : {DEST_TABLE}")
    print(f"  staging    : {STAGING_PK}")
    print(f"  checkpoint : {CHECKPOINT_TABLE}")
    print(f"  batch_size : {BATCH_SIZE:,}")
    print(f"{'='*70}\n", flush=True)

    print("  Connecting to database...")
    sys.stdout.flush()
    ranges = setup_tables()

    if not ranges:
        print(f"\nNo eligible rows in {SOURCE_SCHEMA}.familyhxdetails. Exiting.")
        return

    result = None
    with tqdm(total=len(ranges), desc="familyhxdetails", unit="batch") as pbar:
        result = run_insert(ranges, pbar)

    print()
    if result["status"] == "done":
        tag = " DONE"
    elif result["status"] == "skipped":
        tag = " SKIP"
    else:
        tag = " FAIL"
    print(f"  [{tag}] {SOURCE_SCHEMA}.familyhxdetails  "
          f"{result['rows']:>10,} rows inserted  ({result['secs']}s)")

    print(f"\n{'='*70}")
    if result["status"].startswith("FAILED"):
        print(f"  FAILED: {result['status']}")
    else:
        print(f"  Status: {result['status']}  |  Total rows inserted: {result['rows']:,}")
    print(f"{'='*70}")

    print(f"\n  Cleanup SQL:")
    print(f"    DROP TABLE IF EXISTS {STAGING_ENC};")
    print(f"    DROP TABLE IF EXISTS {STAGING_ITEMS};")
    print(f"    DROP TABLE IF EXISTS {STAGING_PK};")
    print(f"    DROP TABLE IF EXISTS {CHECKPOINT_TABLE};")

    if result["status"].startswith("FAILED"):
        sys.exit(1)


if __name__ == "__main__":
    main()
