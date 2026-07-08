#!/usr/bin/env python3
"""
Optimized batched standardisation UPDATEs for: rgd_udm_silver.labs

Change TARGET_TABLE at the top to run against any labs table.

Three sequential passes — each with checkpoint/resume:

  Pass 1 — WHERE result_code IS NOT NULL:
    SET panel_code_std=NULL, result_panel_std=NULL, panel_match_type=NULL,
        component_code_std, result_component_std, result_desc_std,
        component_match_type='Exact, conf 1', specimen_source_std
    JOIN semantics.loinc (pre-materialized as STAGING_LOINC)

  Pass 2 — WHERE specimen_source_std IS NULL:
    SET specimen_source_std via REGEXP CASE on specimen_source
    No JOIN — pure CASE expression

  Pass 3 — WHERE specimen_source_std IS NULL (after Pass 2):
    SET specimen_source_std via REGEXP CASE on result_name + result_parameter
    No JOIN — deferred PK staging rebuilt after Pass 2 completes

Pre-materialized lookup tables (computed ONCE, reused across all batches):
  - staging.labs_std_loinc    (semantics.loinc — indexed on LOINC_NUM)

Std columns added to target table if not present.

Optimizations applied:
- LOINC lookup pre-materialized once (not re-scanned per batch)
- Per-pass PK staging tables (filtered to eligible rows only)
- Pass 3 staging rebuilt after Pass 2 to pick up newly-handled rows
- Server-side boundary sampling (avoids loading all PKs into memory)
- Commit after every batch (frees undo/log space)
- Checkpoint/resume per pass — re-run skips completed passes
- Disabled InnoDB checks per-session for bulk update speed
- Progress bar via tqdm

Usage:
    python labs_opt_stand.py
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
    "host":            os.environ.get("DB_INTERNAL_HOST"),
    "port":            3306,
    "user":            os.environ.get("DB_INTERNAL_USER"),
    "password":        os.environ.get("DB_INTERNAL_PASSWORD"),
    "database":        "rgd_udm_silver",
    "charset":         "utf8mb4",
    "connect_timeout": 30,
    "read_timeout":    21600,
    "write_timeout":   21600,
}

BATCH_SIZE = 50_000

# ── Change this to run against a different labs table ─────────────────
TARGET_TABLE = "kinsula_leq.labs"

# ─────────────────────────────────────────────────────────────────────
_TABLE_SUFFIX = TARGET_TABLE.replace(".", "_").replace("-", "_")

STAGING_LOINC    = "staging.labs_std_loinc"                            # semantics.loinc (shared)
STAGING_PK_PASS1 = f"staging.labs_std_pk1_{_TABLE_SUFFIX}"
STAGING_PK_PASS2 = f"staging.labs_std_pk2_{_TABLE_SUFFIX}"
STAGING_PK_PASS3 = f"staging.labs_std_pk3_{_TABLE_SUFFIX}"
CHECKPOINT_TABLE = f"staging.etl_checkpoint_labs_std_{_TABLE_SUFFIX}"
CHECKPOINT_PASS1 = f"labs.std.pass1.loinc_lookup.{_TABLE_SUFFIX}"
CHECKPOINT_PASS2 = f"labs.std.pass2.specimen_source_regexp.{_TABLE_SUFFIX}"
CHECKPOINT_PASS3 = f"labs.std.pass3.name_param_regexp.{_TABLE_SUFFIX}"

BATCH_KEY = "udm_inc_id"


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


def _safe_add_index(cur, conn, table, col, idx_name, prefix=50):
    """Add index; handles TEXT columns (need prefix), already-exists, prefix-too-long."""
    for i, key_spec in enumerate((f"`{col}`({prefix})", f"`{col}`")):
        try:
            cur.execute(f"ALTER TABLE {table} ADD INDEX `{idx_name}` ({key_spec})")
            conn.commit()
            return
        except pymysql.err.OperationalError as e:
            code = e.args[0]
            if code == 1061:   # duplicate key name — already exists
                return
            if code == 1089 and i == 0:  # prefix length > column length — retry without
                continue
            raise


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


# ── Batch UPDATE builders ─────────────────────────────────────────────

def build_pass1(pk_lo, pk_hi):
    """Pass 1: LOINC lookup — sets all component/panel std columns + specimen_source_std."""
    return f"""
UPDATE {TARGET_TABLE} l
JOIN {STAGING_LOINC} loinc ON l.result_code = loinc.LOINC_NUM
SET
    l.panel_code_std       = NULL,
    l.result_panel_std     = NULL,
    l.panel_match_type     = NULL,
    l.component_code_std   = loinc.LOINC_NUM,
    l.result_component_std = loinc.COMPONENT,
    l.result_desc_std      = loinc.LONG_COMMON_NAME,
    l.component_match_type = 'Exact, conf 1',
    l.specimen_source_std  = loinc.loinc_system
WHERE l.result_code IS NOT NULL
  AND l.{BATCH_KEY} >= {pk_lo}
  AND l.{BATCH_KEY} < {pk_hi}
"""


def build_pass2(pk_lo, pk_hi):
    """Pass 2: REGEXP CASE on specimen_source — sets specimen_source_std where still NULL."""
    return f"""
UPDATE {TARGET_TABLE} l
SET l.specimen_source_std = CASE
    -- BLOOD & COMPONENTS
    WHEN l.specimen_source REGEXP 'Ser/Plas|S/P'                             THEN 'Ser/Plas'
    WHEN l.specimen_source REGEXP 'Serum'                                    THEN 'Ser'
    WHEN l.specimen_source REGEXP 'Plasma'                                   THEN 'Plas'
    WHEN l.specimen_source REGEXP 'Venous'                                   THEN 'BldV'
    WHEN l.specimen_source REGEXP 'Arterial'                                 THEN 'BldA'
    WHEN l.specimen_source REGEXP 'Capillary'                                THEN 'BldC'
    WHEN l.specimen_source REGEXP 'cord blood|blood cord'                    THEN 'BldCo'
    WHEN l.specimen_source REGEXP 'Blood|BLD|^BL$'                           THEN 'Bld'
    -- CEREBROSPINAL FLUID
    WHEN l.specimen_source REGEXP 'CSF|Cerebrospinal|Spinal'                 THEN 'CSF'
    -- STOOL
    WHEN l.specimen_source REGEXP 'Stool|Fecal|Faeces'                       THEN 'Stool'
    -- RESPIRATORY
    WHEN l.specimen_source REGEXP 'Throat'                                   THEN 'Thrt'
    WHEN l.specimen_source REGEXP 'Sputum'                                   THEN 'Spt'
    WHEN l.specimen_source REGEXP 'BAL|Bronchial|Alveolar'                   THEN 'BAL'
    WHEN l.specimen_source REGEXP 'Respiratory'                              THEN 'Resp'
    WHEN l.specimen_source REGEXP 'Swab'                                     THEN 'XXX.swab'
    -- GENITAL/OBSTETRIC
    WHEN l.specimen_source REGEXP 'Vaginal/Rectal|Vagina/rect|vag/rec'       THEN 'Vag+Rectum'
    WHEN l.specimen_source REGEXP 'Vaginal|VAG|Vulva'                        THEN 'Vag'
    WHEN l.specimen_source REGEXP 'Cervix|Cervical|Cvx'                      THEN 'Cvx'
    WHEN l.specimen_source REGEXP 'Endomet'                                  THEN 'Endomet'
    WHEN l.specimen_source REGEXP 'Placenta'                                 THEN 'Placenta'
    WHEN l.specimen_source REGEXP 'Rectal|rectum'                            THEN 'Rectum'
    -- BODY FLUIDS
    WHEN l.specimen_source REGEXP 'Pleural|PLFL|BPLEU'                       THEN 'Plr fld'
    WHEN l.specimen_source REGEXP 'Peritoneal|Ascites|Perit'                 THEN 'Perit fld'
    WHEN l.specimen_source REGEXP 'Synovial|syn fl'                          THEN 'Syn fld'
    -- TISSUE & BIOPSY
    WHEN l.specimen_source REGEXP 'skin'                                     THEN 'Skin'
    WHEN l.specimen_source REGEXP 'Tissue|TISS'                              THEN 'Tiss'
    -- URINE
    WHEN l.specimen_source REGEXP 'Urine|URIN|^UR$|^U$|URNE|URN|UCC'        THEN 'Urine'
    -- MISC
    WHEN l.specimen_source REGEXP 'Abscess|ABS'                              THEN 'Abscess'
    WHEN l.specimen_source REGEXP 'Wound|WND'                                THEN 'Wound'
    WHEN l.specimen_source REGEXP 'Saliva'                                   THEN 'Saliva'
    WHEN l.specimen_source REGEXP 'Sweat'                                    THEN 'Sweat'
    WHEN l.specimen_source REGEXP 'Calculus|Stone|Calculi'                   THEN 'Calculus'
    ELSE NULL
END
WHERE l.specimen_source_std IS NULL
  AND l.{BATCH_KEY} >= {pk_lo}
  AND l.{BATCH_KEY} < {pk_hi}
"""


def build_pass3(pk_lo, pk_hi):
    """Pass 3: REGEXP CASE on result_name + result_parameter — sets specimen_source_std where still NULL."""
    return f"""
UPDATE {TARGET_TABLE} l
SET l.specimen_source_std = CASE
    -- 1. CEREBROSPINAL FLUID
    WHEN l.result_name      REGEXP 'CSF|spinal'
      OR l.result_parameter REGEXP 'CSF|spinal'                              THEN 'CSF'

    -- 2. SERUM/PLASMA (combo first)
    WHEN l.result_name      REGEXP 'serum/plasma|serum or plasma|serum / plasma|serum/plas|S/P'
      OR l.result_parameter REGEXP 'serum/plasma|serum or plasma|serum / plasma|serum/plas|S/P'
                                                                             THEN 'Ser/Plas'

    -- 3. SERUM
    WHEN l.result_name      REGEXP 'serum|,[[:space:]]*s|\\\\(s\\\\)'
      OR l.result_parameter REGEXP 'serum|,[[:space:]]*s|\\\\(s\\\\)'        THEN 'Ser'

    -- 4. PLASMA
    WHEN l.result_name      REGEXP 'plasma|,[[:space:]]*p|\\\\(p\\\\)'
      OR l.result_parameter REGEXP 'plasma|,[[:space:]]*p|\\\\(p\\\\)'       THEN 'Plas'

    -- 5. WHOLE BLOOD
    WHEN l.result_name      REGEXP 'blood|bld|,[[:space:]]*b|whole blood|venous'
      OR l.result_parameter REGEXP 'blood|bld|,[[:space:]]*b|whole blood|venous'
                                                                             THEN 'Bld'

    -- 6. URINE
    WHEN l.result_name      REGEXP 'urin| ur|urate|\\\\(u\\\\)'
      OR l.result_parameter REGEXP 'urin| ur|urate|\\\\(u\\\\)'              THEN 'Urine'

    -- 7. STOOL / FECAL
    WHEN l.result_name      REGEXP 'stool|faeces|fecal|feces'
      OR l.result_parameter REGEXP 'stool|faeces|fecal|feces'               THEN 'Stool'

    -- 8. SWABS & OTHER
    WHEN l.result_name REGEXP 'swab|throat|nasal|wound|eye|ear'             THEN 'Swab'

    -- 9. BODY FLUIDS
    WHEN l.result_name REGEXP 'pleural|peritoneal|ascites|fluid|synovial|dialysate'
                                                                             THEN 'Body Fld'

    -- 10. HEMATOLOGY PANELS (Inferred Whole Blood)
    WHEN l.result_name      REGEXP 'CBC|Hemogram|Hgb/Hct|Platelet|Complete Blood|A1c|Glycated|Glycohemoglobin'
      OR l.result_parameter REGEXP 'CBC|Hemogram|Hgb/Hct|Diff|A1c|Glycated|Glycohemoglobin'
                                                                             THEN 'Bld'

    -- 11. METABOLIC PANELS (Inferred Serum/Plasma)
    WHEN l.result_name      REGEXP 'CMP|BMP|Metabolic|Basic met|Comp met|Chem[[:space:]]*[[:digit:]]+|SMA|Lipid|Cholest|Triglyceride|HDL|LDL|TSH|Thyroid|T3|T4|Vitamin|Folate|B12|Ferritin'
      OR l.result_parameter REGEXP 'CMP|BMP|Metabolic|Lipid|Cholest|Triglyceride|HDL|LDL|TSH|Thyroid|T3|T4|Vitamin|Folate|B12|Ferritin'
                                                                             THEN 'Ser/Plas'

    ELSE NULL
END
WHERE l.specimen_source_std IS NULL
  AND l.{BATCH_KEY} >= {pk_lo}
  AND l.{BATCH_KEY} < {pk_hi}
"""


# ── Checkpoint ─────────────────────────────────────────────────────────

def is_done(conn, checkpoint_key):
    cur = conn.cursor()
    cur.execute(
        f"SELECT status FROM {CHECKPOINT_TABLE} WHERE source_key = %s",
        (checkpoint_key,),
    )
    row = cur.fetchone()
    cur.close()
    return row is not None and row[0] == "done"


def mark(conn, checkpoint_key, status, rows=0, error=None):
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
    """, (checkpoint_key, status, rows, status, error))
    conn.commit()
    cur.close()


# ── DDL: ensure std columns exist ─────────────────────────────────────

_LONGTEXT_COLS  = {"result_desc_std"}
_LONGTEXT_TYPES = {"longtext"}


def ensure_std_columns():
    std_cols = [
        ("panel_code_std",       "VARCHAR(50)"),
        ("result_panel_std",     "VARCHAR(500)"),
        ("panel_match_type",     "VARCHAR(50)"),
        ("component_code_std",   "VARCHAR(50)"),
        ("result_component_std", "VARCHAR(500)"),
        ("result_desc_std",      "LONGTEXT"),
        ("component_match_type", "VARCHAR(50)"),
        ("specimen_source_std",  "VARCHAR(100)"),
    ]
    print(f"  Checking std columns on {TARGET_TABLE}...")
    ddl_conn = get_connection()
    ddl_cur  = ddl_conn.cursor()
    ddl_error = None
    added = []
    try:
        for col_name, col_type in std_cols:
            if not _col_exists(ddl_cur, TARGET_TABLE, col_name):
                print(f"    adding: {col_name} {col_type} ...")
                ddl_cur.execute(
                    f"ALTER TABLE {TARGET_TABLE} ADD COLUMN {col_name} {col_type} DEFAULT NULL"
                )
                ddl_conn.commit()
                added.append(col_name)
                print(f"    added: {col_name}")
            elif col_name in _LONGTEXT_COLS:
                schema, table = TARGET_TABLE.split(".")
                ddl_cur.execute(
                    "SELECT DATA_TYPE FROM information_schema.columns "
                    "WHERE table_schema = %s AND table_name = %s AND column_name = %s",
                    (schema, table, col_name),
                )
                row = ddl_cur.fetchone()
                current_type = row[0].lower() if row else "longtext"
                if current_type not in _LONGTEXT_TYPES:
                    print(f"    widening: {col_name} ({current_type.upper()} → LONGTEXT) ...")
                    ddl_cur.execute(
                        f"ALTER TABLE {TARGET_TABLE} MODIFY COLUMN {col_name} LONGTEXT DEFAULT NULL"
                    )
                    ddl_conn.commit()
                    print(f"    widened: {col_name}")
                else:
                    print(f"    exists: {col_name} (LONGTEXT)")
            else:
                print(f"    exists: {col_name}")
    except Exception as exc:
        ddl_error = exc
        try:
            ddl_conn.rollback()
        except Exception:
            pass
    finally:
        try:
            ddl_cur.close()
        except Exception:
            pass
        try:
            ddl_conn.close()
        except Exception:
            pass

    if ddl_error:
        print(f"\n  ERROR: Could not add column — metadata lock on {TARGET_TABLE}.")
        print(f"  Find the blocker:")
        print(f"    SELECT id, user, state, info FROM information_schema.processlist")
        print(f"    WHERE state LIKE '%lock%' OR state LIKE '%wait%' ORDER BY time DESC;")
        print(f"  Then: KILL <id>;")
        print(f"\n  Original error: {ddl_error}")
        sys.exit(1)

    if added:
        print(f"    Columns added: {', '.join(added)}")
    else:
        print(f"    All std columns already present.")


# ── Setup ──────────────────────────────────────────────────────────────

def setup_tables():
    ensure_std_columns()

    conn = get_connection()
    cur  = conn.cursor()

    # ── 1. LOINC lookup (semantics.loinc) ─────────────────────────────
    print("  Materializing semantics.loinc lookup...")
    if not _table_exists(cur, STAGING_LOINC):
        cur.execute(f"""
            CREATE TABLE {STAGING_LOINC} AS
            SELECT
                LOINC_NUM,
                COMPONENT,
                LONG_COMMON_NAME,
                `SYSTEM` AS loinc_system
            FROM semantics.loinc
        """)
        _safe_add_index(cur, conn, STAGING_LOINC, "LOINC_NUM", "idx_loinc_num")
        print("    created")
    else:
        print("    already exists, reusing")
    cur.execute(f"SELECT COUNT(*) FROM {STAGING_LOINC}")
    print(f"    {cur.fetchone()[0]:,} rows")

    # ── 2. PK staging — Pass 1: WHERE result_code IS NOT NULL ─────────
    print("  Creating PK staging for Pass 1 (result_code IS NOT NULL)...")
    if not _table_exists(cur, STAGING_PK_PASS1):
        cur.execute(f"""
            CREATE TABLE {STAGING_PK_PASS1} AS
            SELECT {BATCH_KEY}
            FROM {TARGET_TABLE}
            WHERE result_code IS NOT NULL
              AND {BATCH_KEY} IS NOT NULL
        """)
        cur.execute(f"ALTER TABLE {STAGING_PK_PASS1} ADD INDEX idx_pk ({BATCH_KEY})")
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")
    ranges_p1, total_p1 = _build_ranges(cur, STAGING_PK_PASS1)
    print(f"    {total_p1:,} rows → {len(ranges_p1)} batches")

    # ── 3. PK staging — Pass 2: WHERE specimen_source_std IS NULL ─────
    print("  Creating PK staging for Pass 2 (specimen_source_std IS NULL)...")
    if not _table_exists(cur, STAGING_PK_PASS2):
        cur.execute(f"""
            CREATE TABLE {STAGING_PK_PASS2} AS
            SELECT {BATCH_KEY}
            FROM {TARGET_TABLE}
            WHERE specimen_source_std IS NULL
              AND {BATCH_KEY} IS NOT NULL
        """)
        cur.execute(f"ALTER TABLE {STAGING_PK_PASS2} ADD INDEX idx_pk ({BATCH_KEY})")
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")
    ranges_p2, total_p2 = _build_ranges(cur, STAGING_PK_PASS2)
    print(f"    {total_p2:,} rows → {len(ranges_p2)} batches")

    # ── 4. PK staging — Pass 3: deferred — rebuilt after Pass 2 ───────
    # Pass 3 picks up rows still NULL after Pass 2 — built in rebuild_pass3_staging()
    print("  Pass 3 PK staging will be (re)built after Pass 2 completes.")

    # ── 5. Checkpoint table ────────────────────────────────────────────
    print("  Creating checkpoint table...")
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

    return {
        CHECKPOINT_PASS1: ranges_p1,
        CHECKPOINT_PASS2: ranges_p2,
    }


def rebuild_pass3_staging():
    """Drop and recreate STAGING_PK_PASS3 after Pass 2 to capture remaining NULL rows."""
    print("\n  Rebuilding Pass 3 PK staging (specimen_source_std IS NULL after Pass 2)...")
    conn = get_connection()
    cur  = conn.cursor()
    try:
        cur.execute(f"DROP TABLE IF EXISTS {STAGING_PK_PASS3}")
        cur.execute(f"""
            CREATE TABLE {STAGING_PK_PASS3} AS
            SELECT {BATCH_KEY}
            FROM {TARGET_TABLE}
            WHERE specimen_source_std IS NULL
              AND {BATCH_KEY} IS NOT NULL
        """)
        cur.execute(f"ALTER TABLE {STAGING_PK_PASS3} ADD INDEX idx_pk ({BATCH_KEY})")
        conn.commit()
        ranges_p3, total_p3 = _build_ranges(cur, STAGING_PK_PASS3)
        print(f"    {total_p3:,} rows → {len(ranges_p3)} batches")
        return ranges_p3
    finally:
        cur.close()
        conn.close()


# ── Runner ─────────────────────────────────────────────────────────────

def run_pass(checkpoint_key, build_fn, ranges, pbar):
    conn = get_connection()

    if is_done(conn, checkpoint_key):
        conn.close()
        pbar.update(len(ranges))
        return {"status": "skipped", "rows": 0, "secs": 0}

    mark(conn, checkpoint_key, "running")
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
        mark(conn, checkpoint_key, "done", total_rows)
        conn.close()
        return {"status": "done", "rows": total_rows, "secs": elapsed}

    except Exception as exc:
        elapsed = round(time.time() - t0, 1)
        mark(conn, checkpoint_key, "failed", total_rows, str(exc))
        try:
            conn.close()
        except Exception:
            pass
        return {"status": f"FAILED: {exc}", "rows": total_rows, "secs": elapsed}


# ── Main ───────────────────────────────────────────────────────────────

def main():
    print(f"\n{'='*70}", flush=True)
    print(f"  Labs Standardisation UPDATE — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  target     : {TARGET_TABLE}")
    print(f"  batch_key  : {BATCH_KEY}")
    print(f"  batch_size : {BATCH_SIZE:,}")
    print(f"  passes     : 3  (LOINC lookup | specimen_source REGEXP | name/param REGEXP)")
    print(f"{'='*70}\n", flush=True)

    print("  Connecting to database...")
    sys.stdout.flush()
    all_ranges = setup_tables()

    passes = [
        (CHECKPOINT_PASS1, "Pass 1 — LOINC lookup (result_code IS NOT NULL)",     build_pass1),
        (CHECKPOINT_PASS2, "Pass 2 — specimen_source REGEXP (std IS NULL)",        build_pass2),
    ]

    results = {}
    any_failed = False

    # ── Passes 1 and 2 ────────────────────────────────────────────────
    p1_ranges = all_ranges.get(CHECKPOINT_PASS1, [])
    p2_ranges = all_ranges.get(CHECKPOINT_PASS2, [])
    total_batches_12 = len(p1_ranges) + len(p2_ranges)

    with tqdm(total=total_batches_12, desc="Passes 1-2", unit="batch") as pbar:
        for ck, label, build_fn in passes:
            ranges = all_ranges.get(ck, [])
            if not ranges:
                print(f"\n  [SKIP] {label} — no eligible rows")
                continue

            print(f"\n  Starting {label} ({len(ranges)} batches)...")
            result = run_pass(ck, build_fn, ranges, pbar)
            results[ck] = result

            if result["status"].startswith("FAILED"):
                print(f"\n  FAILED at {label}: {result['status']}")
                print("  Aborting remaining passes.")
                any_failed = True
                break

    if any_failed:
        _print_summary(passes, results)
        sys.exit(1)

    # ── Pass 3: rebuild staging after Pass 2, then run ────────────────
    ranges_p3 = rebuild_pass3_staging()

    if not ranges_p3:
        print(f"\n  [SKIP] Pass 3 — result_name/param REGEXP — no eligible rows")
        results[CHECKPOINT_PASS3] = {"status": "skipped", "rows": 0, "secs": 0}
    else:
        print(f"\n  Starting Pass 3 — result_name/param REGEXP ({len(ranges_p3)} batches)...")
        with tqdm(total=len(ranges_p3), desc="Pass 3", unit="batch") as pbar:
            result = run_pass(CHECKPOINT_PASS3, build_pass3, ranges_p3, pbar)
        results[CHECKPOINT_PASS3] = result

        if result["status"].startswith("FAILED"):
            any_failed = True
            print(f"\n  FAILED at Pass 3: {result['status']}")

    all_passes = passes + [(CHECKPOINT_PASS3, "Pass 3 — result_name/param REGEXP (std IS NULL)", build_pass3)]
    _print_summary(all_passes, results)

    if any_failed:
        sys.exit(1)


def _print_summary(passes, results):
    print(f"\n{'='*70}")
    print(f"  Per-pass summary:")
    total_rows = 0
    for ck, label, _ in passes:
        res = results.get(ck, {"status": "not run", "rows": 0, "secs": 0})
        status = res["status"]
        rows   = res["rows"]
        secs   = res["secs"]
        if status == "done":
            tag = " DONE"; total_rows += rows
        elif status == "skipped":
            tag = " SKIP"
        elif status == "not run":
            tag = "  ---"
        else:
            tag = " FAIL"
        print(f"  [{tag}] {label:<58}  {rows:>10,} rows  ({secs}s)")
        if status.startswith("FAILED"):
            print(f"         {status}")

    print(f"\n  Total rows updated: {total_rows:,}")
    print(f"{'='*70}")

    print(f"\n  Cleanup SQL (run after verifying data):")
    print(f"    -- Shared LOINC lookup (only drop when all labs tables are done):")
    print(f"    -- DROP TABLE IF EXISTS {STAGING_LOINC};")
    print(f"    -- Per-run tables:")
    print(f"    DROP TABLE IF EXISTS {STAGING_PK_PASS1};")
    print(f"    DROP TABLE IF EXISTS {STAGING_PK_PASS2};")
    print(f"    DROP TABLE IF EXISTS {STAGING_PK_PASS3};")
    print(f"    DROP TABLE IF EXISTS {CHECKPOINT_TABLE};")


if __name__ == "__main__":
    main()
