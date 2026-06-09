#!/usr/bin/env python3
"""
Optimized ETL loader for: udm_staging.familyhistory_final
Source: Athena Practice (AP / Noran)

Source: {SOURCE_SCHEMA}.FamilyHealthHistory
  LEFT JOIN FHxRelationship (nd_Activeflag='Y') → pre-materialized
  LEFT JOIN MasterDiagnosis (nd_Activeflag='Y') → pre-materialized
  Filter : Inactive = 'N' AND FiledInError = 'N' AND nd_Activeflag = 'Y'
  Batch  : FamilyHealthHistoryID (primary key)

Pre-materialized lookup tables (computed ONCE, reused across all batches):
  - staging.fh_ap_fhxrel_{schema}  (FHxRelationship active, keyed on FHxRelationshipID)
  - staging.fh_ap_diag_{schema}    (MasterDiagnosis active, keyed on MasterDiagnosisID)

Column mapping to shared dest table (same structure as opt_fami_hist_ecw.py):
  Relation              → fam_hist_relation
  FHxRelationship.Code  → family_relationship_code
  f.Description         → family_hist_details
  MasterDiagnosis.Code  → family_hist_code
  f.FHxComments         → family_hist_notes
  itemname/itemdesc     → NULL (ECW-specific columns)

Optimizations applied:
- Pre-materialize FHxRelationship + MasterDiagnosis lookups (not re-scanned per batch)
- Batch by actual primary key values (sparse ID safe)
- Server-side boundary sampling
- Commit after every batch (frees undo/log space)
- Checkpoint/resume — re-run skips if already completed
- Disabled InnoDB checks per-session for bulk insert speed
- REGEXP {{n}} quantifiers correctly escaped inside f-strings
- Progress bar via tqdm

Usage:
    python opt_fam_hist_ap.py
"""

import sys
import time
from datetime import datetime
import pymysql
from tqdm import tqdm

# ── Configuration ────────────────────────────────────────────────────
DB_CONFIG = {
    "host":            "172.16.2.42",
    "port":            3306,
    "user":            "nd-root-mysql",
    "password":        "kmsamd89undsd4",
    "database":        "noran",
    "charset":         "utf8mb4",
    "connect_timeout": 30,
    "read_timeout":    21600,
    "write_timeout":   21600,
}

BATCH_SIZE = 50_000

# ── Change these two variables to run for a different schema/psid ────
SOURCE_SCHEMA = "noran"   # e.g. "noran", "dcnd_ap", ...
PSID          = 7

DEST_TABLE       = "udm_staging.familyhistory_final"
STAGING_FHXREL   = f"staging.fh_ap_fhxrel_v2_{SOURCE_SCHEMA}"
STAGING_DIAG     = f"staging.fh_ap_diag_v2_{SOURCE_SCHEMA}"
STAGING_PK       = f"staging.tmp_fh_ap_staging_v2_{SOURCE_SCHEMA}"
CHECKPOINT_TABLE = f"staging.etl_checkpoint_fh_ap_v2_{SOURCE_SCHEMA}"
CHECKPOINT_KEY   = f"familyhistory.ap.insert.{SOURCE_SCHEMA}"

BATCH_KEY = "FamilyHealthHistoryID"


# ── Date CASE helper ──────────────────────────────────────────────────

def date_case(col):
    """
    Converts a DATE/DATETIME/VARCHAR column to DATE safely.
    Wraps col in CAST(... AS CHAR) to avoid MySQL strict mode error 1292.
    Handles: NULL, empty, YYYY-MM-DD, YYYY-MM-DD HH:MM:SS,
             MM-DD-YYYY HH:MM:SS, MM-DD-YYYY.
    {{4}}/{{2}} produce literal {4}/{2} — correct MySQL REGEXP quantifiers.
    """
    c = f"CAST({col} AS CHAR)"
    return (
        f"CASE\n"
        f"        WHEN {c} IS NULL OR {c} IN ('', 'None') THEN NULL\n"
        f"        WHEN {c} REGEXP '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}( [0-9]{{2}}:[0-9]{{2}}:[0-9]{{2}})?$'\n"
        f"            THEN DATE({c})\n"
        f"        WHEN {c} REGEXP '^[0-9]{{2}}-[0-9]{{2}}-[0-9]{{4}} [0-9]{{2}}:[0-9]{{2}}:[0-9]{{2}}$'\n"
        f"            THEN DATE(STR_TO_DATE({c}, '%m-%d-%Y %H:%i:%s'))\n"
        f"        WHEN {c} REGEXP '^[0-9]{{2}}-[0-9]{{2}}-[0-9]{{4}}$'\n"
        f"            THEN STR_TO_DATE({c}, '%m-%d-%Y')\n"
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
    f.FamilyHealthHistoryID,
    f.PID,
    NULL,
    NULL,
    NULL,
    NULL,
    {date_case('f.SignedDate')},
    'Family History',
    rel.Relation,
    rel.Code,
    f.Description,
    diag.Code,
    'SNOMED',
    f.FHxComments,
    NULL,
    NULL,
    NULL,
    CURRENT_DATE(),
    'ND',
    CURRENT_DATE(),
    'ND',
    'Athenaone',
    'bronze_layer',
    'Structured',
    {PSID},
    f.nd_extracted_date
FROM {SOURCE_SCHEMA}.FamilyHealthHistory f
LEFT JOIN {STAGING_FHXREL} rel
    ON rel.FHxRelationshipID = f.FHxRelationshipID
LEFT JOIN {STAGING_DIAG} diag
    ON diag.MasterDiagnosisID = f.SnomedMasterDiagnosisID
WHERE f.nd_Activeflag = 'Y'
  AND f.Inactive = 'N'
  AND f.FiledInError = 'N'
  AND f.{BATCH_KEY} >= {pk_lo}
  AND f.{BATCH_KEY} < {pk_hi}
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
    """Create lookup tables, PK staging, destination, checkpoint. Return batch ranges."""
    conn = get_connection()
    cur  = conn.cursor()

    # ── 1. FHxRelationship lookup (active rows, stores Relation + Code) ──
    print("  Materializing FHxRelationship lookup (nd_Activeflag='Y')...")
    if not _table_exists(cur, STAGING_FHXREL):
        cur.execute(f"""
            CREATE TABLE {STAGING_FHXREL} AS
            SELECT FHxRelationshipID, Relation, Code
            FROM {SOURCE_SCHEMA}.FHxRelationship
            WHERE nd_Activeflag = 'Y'
        """)
        cur.execute(f"ALTER TABLE {STAGING_FHXREL} ADD INDEX idx_fhxrel (FHxRelationshipID)")
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")
    cur.execute(f"SELECT COUNT(*) FROM {STAGING_FHXREL}")
    print(f"    {cur.fetchone()[0]:,} rows")

    # ── 2. MasterDiagnosis lookup (active rows) ───────────────────────
    print("  Materializing MasterDiagnosis lookup (nd_Activeflag='Y')...")
    if not _table_exists(cur, STAGING_DIAG):
        cur.execute(f"""
            CREATE TABLE {STAGING_DIAG} AS
            SELECT MasterDiagnosisID, Code
            FROM {SOURCE_SCHEMA}.MasterDiagnosis
            WHERE nd_Activeflag = 'Y'
        """)
        cur.execute(f"ALTER TABLE {STAGING_DIAG} ADD INDEX idx_diag (MasterDiagnosisID)")
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")
    cur.execute(f"SELECT COUNT(*) FROM {STAGING_DIAG}")
    print(f"    {cur.fetchone()[0]:,} rows")

    # ── 3. Destination table (same structure as opt_fami_hist_ecw.py) ──
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

    # ── 4. Checkpoint table ───────────────────────────────────────────
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

    # ── 5. PK staging table ───────────────────────────────────────────
    print("  Creating PK staging table...")
    if not _table_exists(cur, STAGING_PK):
        cur.execute(f"""
            CREATE TABLE {STAGING_PK} AS
            SELECT {BATCH_KEY}
            FROM {SOURCE_SCHEMA}.FamilyHealthHistory
            WHERE {BATCH_KEY} IS NOT NULL
              AND nd_Activeflag = 'Y'
              AND Inactive = 'N'
              AND FiledInError = 'N'
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
    t0         = time.time()
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
    print(f"  AP Family History ETL — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  source     : {SOURCE_SCHEMA}.FamilyHealthHistory  (psid={PSID})")
    print(f"  dest       : {DEST_TABLE}")
    print(f"  fhxrel     : {STAGING_FHXREL}")
    print(f"  diag       : {STAGING_DIAG}")
    print(f"  staging    : {STAGING_PK}")
    print(f"  checkpoint : {CHECKPOINT_TABLE}")
    print(f"  batch_size : {BATCH_SIZE:,}")
    print(f"{'='*70}\n", flush=True)

    print("  Connecting to database...")
    sys.stdout.flush()
    ranges = setup_tables()

    if not ranges:
        print(f"\nNo eligible rows in {SOURCE_SCHEMA}.FamilyHealthHistory. Exiting.")
        return

    result = None
    with tqdm(total=len(ranges), desc="FamilyHealthHistory", unit="batch") as pbar:
        result = run_insert(ranges, pbar)

    print()
    if result["status"] == "done":
        tag = " DONE"
    elif result["status"] == "skipped":
        tag = " SKIP"
    else:
        tag = " FAIL"
    print(f"  [{tag}] {SOURCE_SCHEMA}.FamilyHealthHistory  "
          f"{result['rows']:>10,} rows inserted  ({result['secs']}s)")

    print(f"\n{'='*70}")
    if result["status"].startswith("FAILED"):
        print(f"  FAILED: {result['status']}")
    else:
        print(f"  Status: {result['status']}  |  Total rows inserted: {result['rows']:,}")
    print(f"{'='*70}")

    print(f"\n  Cleanup SQL (run after verifying data):")
    print(f"    DROP TABLE IF EXISTS {STAGING_FHXREL};")
    print(f"    DROP TABLE IF EXISTS {STAGING_DIAG};")
    print(f"    DROP TABLE IF EXISTS {STAGING_PK};")
    print(f"    DROP TABLE IF EXISTS {CHECKPOINT_TABLE};")

    if result["status"].startswith("FAILED"):
        sys.exit(1)


if __name__ == "__main__":
    main()
