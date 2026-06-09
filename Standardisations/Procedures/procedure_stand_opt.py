#!/usr/bin/env python3
"""
Optimized batched standardisation UPDATEs for: rgd_udm_silver.procedures

Change TARGET_TABLE at the top to run against any procedures table.

Two sequential passes — each with checkpoint/resume:

  Pass 1 — All rows:
    SET proc_code_std, proc_coding_system_std, proc_name_std, proc_description_std
    JOIN semantics.hcpcs + tncpa.PROCEDURECODEREFERENCE — both pre-materialized

  Pass 2 — WHERE proc_code IS NULL AND proc_name IS NOT NULL:
    SET proc_code_std, proc_name_std, proc_description_std
    LIKE-pattern CASE fallback — no JOIN

Pre-materialized lookup tables (computed ONCE, reused across all batches):
  - staging.proc_std_hcpcs    (semantics.hcpcs — indexed on HCPC)
  - staging.proc_std_cpt      (tncpa.PROCEDURECODEREFERENCE — indexed on PROCEDURECODE)

Std columns added to target table if not present (with metadata lock guard).

Optimizations applied:
- Both lookup tables pre-materialized once (not re-scanned per batch)
- Per-pass PK staging tables (filtered to eligible rows only)
- Server-side boundary sampling (avoids loading all PKs into memory)
- Commit after every batch (frees undo/log space)
- Checkpoint/resume per pass — re-run skips completed passes
- Disabled InnoDB checks per-session for bulk update speed
- Progress bar via tqdm

Usage:
    python procedure_stand_opt.py
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
    "database":        "rgd_udm_silver",
    "charset":         "utf8mb4",
    "connect_timeout": 30,
    "read_timeout":    21600,
    "write_timeout":   21600,
}

BATCH_SIZE = 50_000

# ── Change this to run against a different procedures table ───────────
TARGET_TABLE = "rgd_udm_silver.procedures"

# ─────────────────────────────────────────────────────────────────────
# Derive a short suffix from the table name for unique staging/checkpoint
# names — allows multiple tables to run without collision.
_TABLE_SUFFIX = TARGET_TABLE.replace(".", "_").replace("-", "_")

STAGING_HCPCS    = "staging.proc_std_hcpcsfn_r"        # semantics.hcpcs (shared across runs)
STAGING_CPT      = "staging.proc_std_cptfn_r"           # tncpa.PROCEDURECODEREFERENCE (shared)
STAGING_PK_PASS1 = f"staging.proc_std_pk1n3fn_r_{_TABLE_SUFFIX}"
STAGING_PK_PASS2 = f"staging.proc_std_pk2n3fn_r_{_TABLE_SUFFIX}"
CHECKPOINT_TABLE = f"staging.etl_checkpoint_proc_stdn_fn_r_{_TABLE_SUFFIX}"
CHECKPOINT_PASS1 = f"procedures.std.pass1n3.lookupfn_r.{_TABLE_SUFFIX}"
CHECKPOINT_PASS2 = f"procedures.std.pass2n3.name_fallbackfn_r.{_TABLE_SUFFIX}"

BATCH_KEY = "udm_inc_id"   # integer PK on procedures tables


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
    """Pass 1: code/modifier/name/description lookup via HCPCS + CPT reference tables."""
    return f"""
UPDATE {TARGET_TABLE} p
LEFT JOIN {STAGING_HCPCS} h ON LEFT(TRIM(p.proc_code), 5) = h.HCPC
LEFT JOIN {STAGING_CPT}   t ON LEFT(TRIM(p.proc_code), 5) = t.PROCEDURECODE
SET
    p.proc_code_std = CASE
        WHEN (p.proc_code IS NULL OR TRIM(p.proc_code) = '')
             AND (p.proc_name IS NULL OR TRIM(p.proc_name) = '') THEN NULL
        ELSE LEFT(COALESCE(h.HCPC, t.PROCEDURECODE, 'NS'), 5)
    END,
    p.proc_modifier_std = CASE
        WHEN (p.proc_code IS NULL OR TRIM(p.proc_code) = '')
             AND (p.proc_name IS NULL OR TRIM(p.proc_name) = '') THEN NULL
        WHEN p.proc_code LIKE '%,%'
            THEN TRIM(SUBSTRING(p.proc_code, INSTR(p.proc_code, ',') + 1))
        WHEN p.proc_code NOT LIKE '%,%' THEN NULL
        ELSE 'NS'
    END,
    p.proc_coding_system_std = CASE
        WHEN (p.proc_code IS NULL OR TRIM(p.proc_code) = '')
             AND (p.proc_name IS NULL OR TRIM(p.proc_name) = '') THEN NULL
        WHEN h.HCPC IS NOT NULL THEN 'HCPCS'
        WHEN LEFT(t.PROCEDURECODE, 5) REGEXP '^[0-9]+$' THEN 'CPT'
        WHEN LEFT(t.PROCEDURECODE, 5) REGEXP '^(?=.*[A-Za-z])(?=.*[0-9])[A-Za-z0-9]+$' THEN 'HCPCS'
        ELSE 'NS'
    END,
    p.proc_name_std = CASE
        WHEN (p.proc_code IS NULL OR TRIM(p.proc_code) = '')
             AND (p.proc_name IS NULL OR TRIM(p.proc_name) = '') THEN NULL
        ELSE COALESCE(h.SHORT_DESCRIPTION, t.COMMONDESCRIPTION, 'NS')
    END,
    p.proc_description_std = CASE
        WHEN (p.proc_code IS NULL OR TRIM(p.proc_code) = '')
             AND (p.proc_name IS NULL OR TRIM(p.proc_name) = '') THEN NULL
        ELSE COALESCE(h.LONG_DESCRIPTION, t.DESCRIPTION, 'NS')
    END
WHERE p.{BATCH_KEY} >= {pk_lo}
  AND p.{BATCH_KEY} < {pk_hi}
"""


def build_pass2(pk_lo, pk_hi):
    """Pass 2: LIKE-pattern name fallback where proc_code IS NULL."""
    return f"""
UPDATE {TARGET_TABLE}
SET
    proc_modifier_std = NULL,
    proc_code_std = CASE
        WHEN LOWER(proc_name) LIKE '%occipital nerve block%' THEN '64405'
        WHEN LOWER(proc_name) LIKE '%trigger point%' THEN '20552'
        WHEN LOWER(proc_name) LIKE '%consultation%'
             AND LOWER(proc_name) LIKE '%telemedicine%' THEN '99242'
        WHEN LOWER(proc_name) LIKE '%testosterone%' THEN 'J1071'
        WHEN LOWER(proc_name) LIKE '%b12%' THEN 'J3420'
        WHEN LOWER(proc_name) LIKE '%ocrelizumab%' THEN 'J2350'
        ELSE 'NS'
    END,
    proc_name_std = CASE
        WHEN LOWER(proc_name) LIKE '%occipital nerve block%' THEN 'Injection, anesthetic agent; greater occipital nerve'
        WHEN LOWER(proc_name) LIKE '%trigger point%' THEN 'Injection(s), trigger point(s)'
        WHEN LOWER(proc_name) LIKE '%consultation%'
             AND LOWER(proc_name) LIKE '%telemedicine%' THEN 'Office consultation'
        WHEN LOWER(proc_name) LIKE '%testosterone%' THEN 'Injection, testosterone cypionate'
        WHEN LOWER(proc_name) LIKE '%b12%' THEN 'Injection, vitamin B-12 cyanocobalamin'
        WHEN LOWER(proc_name) LIKE '%ocrelizumab%' THEN 'Injection, ocrelizumab, 1 mg'
        ELSE 'NS'
    END,
    proc_description_std = CASE
        WHEN LOWER(proc_name) LIKE '%occipital nerve block%' THEN 'Injection of anesthetic agent for greater occipital nerve block'
        WHEN LOWER(proc_name) LIKE '%trigger point%' THEN 'Injection of one or more trigger points in muscle'
        WHEN LOWER(proc_name) LIKE '%consultation%'
             AND LOWER(proc_name) LIKE '%telemedicine%' THEN 'Office consultation for a new or established patient via telemedicine'
        WHEN LOWER(proc_name) LIKE '%testosterone%' THEN 'Injection of testosterone preparation'
        WHEN LOWER(proc_name) LIKE '%b12%' THEN 'Injection of vitamin B12 (cyanocobalamin)'
        WHEN LOWER(proc_name) LIKE '%ocrelizumab%' THEN 'Injection of ocrelizumab, per 1 mg'
        ELSE 'NS'
    END
WHERE proc_code_std = 'NS'
  AND proc_name IS NOT NULL
  AND {BATCH_KEY} >= {pk_lo}
  AND {BATCH_KEY} < {pk_hi}
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

# Columns that must be LONGTEXT — widen automatically if they exist as TEXT/MEDIUMTEXT
_LONGTEXT_COLS  = {"proc_name_std", "proc_description_std"}
_LONGTEXT_TYPES = {"longtext"}          # already wide enough


def ensure_std_columns():
    std_cols = [
        ("proc_code_std",          "VARCHAR(20)"),
        ("proc_modifier_std",      "VARCHAR(100)"),
        ("proc_coding_system_std", "VARCHAR(20)"),
        ("proc_name_std",          "LONGTEXT"),
        ("proc_description_std",   "LONGTEXT"),
    ]
    print(f"  Checking std columns on {TARGET_TABLE}...")
    ddl_conn = get_connection()
    ddl_cur  = ddl_conn.cursor()
    # No lock_wait_timeout here — LONGTEXT ALTER on a large table takes time; let it run.
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
                # Check current type and widen if needed
                schema, table = TARGET_TABLE.split(".")
                ddl_cur.execute(
                    "SELECT DATA_TYPE FROM information_schema.columns "
                    "WHERE table_schema = %s AND table_name = %s AND column_name = %s",
                    (schema, table, col_name),
                )
                row = ddl_cur.fetchone()
                current_type = row[0].lower() if row else "longtext"
                if current_type not in _LONGTEXT_TYPES:
                    print(f"    widening: {col_name} ({current_type.upper()} → LONGTEXT) — this may take a few minutes on large tables ...")
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

    # ── 1. HCPCS lookup (semantics.hcpcs) ─────────────────────────────
    # Column names in semantics.hcpcs may contain spaces — alias them cleanly.
    print("  Materializing semantics.hcpcs lookup...")
    if not _table_exists(cur, STAGING_HCPCS):
        cur.execute(f"""
            CREATE TABLE {STAGING_HCPCS} AS
            SELECT
                HCPC,
                `SHORT DESCRIPTION` AS SHORT_DESCRIPTION,
                `LONG DESCRIPTION`  AS LONG_DESCRIPTION
            FROM semantics.hcpcs
        """)
        cur.execute(f"ALTER TABLE {STAGING_HCPCS} ADD INDEX idx_hcpc (HCPC(20))")
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")
    cur.execute(f"SELECT COUNT(*) FROM {STAGING_HCPCS}")
    print(f"    {cur.fetchone()[0]:,} rows")

    # ── 2. CPT / procedure code reference (tncpa.PROCEDURECODEREFERENCE)
    print("  Materializing tncpa.PROCEDURECODEREFERENCE lookup...")
    if not _table_exists(cur, STAGING_CPT):
        cur.execute(f"""
            CREATE TABLE {STAGING_CPT} AS
            SELECT
                PROCEDURECODE,
                COMMONDESCRIPTION,
                DESCRIPTION
            FROM tncpa.PROCEDURECODEREFERENCE
        """)
        cur.execute(f"ALTER TABLE {STAGING_CPT} ADD INDEX idx_code (PROCEDURECODE(20))")
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")
    cur.execute(f"SELECT COUNT(*) FROM {STAGING_CPT}")
    print(f"    {cur.fetchone()[0]:,} rows")

    # ── 3. PK staging — Pass 1: all rows ──────────────────────────────
    print("  Creating PK staging for Pass 1 (all rows)...")
    if not _table_exists(cur, STAGING_PK_PASS1):
        cur.execute(f"""
            CREATE TABLE {STAGING_PK_PASS1} AS
            SELECT {BATCH_KEY}
            FROM {TARGET_TABLE}
            WHERE {BATCH_KEY} IS NOT NULL
        """)
        cur.execute(f"ALTER TABLE {STAGING_PK_PASS1} ADD INDEX idx_pk ({BATCH_KEY})")
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")
    ranges_p1, total_p1 = _build_ranges(cur, STAGING_PK_PASS1)
    print(f"    {total_p1:,} rows → {len(ranges_p1)} batches")

    # ── 4. PK staging — Pass 2: proc_code_std = 'NS' AND proc_name NOT NULL
    print("  Creating PK staging for Pass 2 (proc_code_std = 'NS' rows)...")
    if not _table_exists(cur, STAGING_PK_PASS2):
        cur.execute(f"""
            CREATE TABLE {STAGING_PK_PASS2} AS
            SELECT {BATCH_KEY}
            FROM {TARGET_TABLE}
            WHERE proc_code_std = 'NS'
              AND proc_name IS NOT NULL
              AND {BATCH_KEY} IS NOT NULL
        """)
        cur.execute(f"ALTER TABLE {STAGING_PK_PASS2} ADD INDEX idx_pk ({BATCH_KEY})")
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")
    ranges_p2, total_p2 = _build_ranges(cur, STAGING_PK_PASS2)
    print(f"    {total_p2:,} rows → {len(ranges_p2)} batches")

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
    print(f"  Procedures Standardisation UPDATE — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  target     : {TARGET_TABLE}")
    print(f"  batch_key  : {BATCH_KEY}")
    print(f"  batch_size : {BATCH_SIZE:,}")
    print(f"  passes     : 2  (code/name lookup | LIKE name fallback)")
    print(f"{'='*70}\n", flush=True)

    print("  Connecting to database...")
    sys.stdout.flush()
    all_ranges = setup_tables()

    passes = [
        (CHECKPOINT_PASS1, "Pass 1 — HCPCS+CPT lookup (all rows)",              build_pass1),
        (CHECKPOINT_PASS2, "Pass 2 — LIKE name fallback (NULL proc_code rows)", build_pass2),
    ]

    results = {}
    any_failed = False
    total_batches = sum(len(all_ranges.get(ck, [])) for ck, _, _ in passes)

    with tqdm(total=total_batches, desc="Overall", unit="batch") as pbar:
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
            tag = " FAIL"; any_failed = True
        print(f"  [{tag}] {label:<52}  {rows:>10,} rows  ({secs}s)")
        if status.startswith("FAILED"):
            print(f"         {status}")

    print(f"\n  Total rows updated: {total_rows:,}")
    print(f"{'='*70}")

    print(f"\n  Cleanup SQL (run after verifying data):")
    # Lookup tables are shared — only drop if you're done with ALL procedure tables
    print(f"    -- Shared lookups (only drop when all tables are done):")
    print(f"    -- DROP TABLE IF EXISTS {STAGING_HCPCS};")
    print(f"    -- DROP TABLE IF EXISTS {STAGING_CPT};")
    print(f"    -- Per-run tables:")
    print(f"    DROP TABLE IF EXISTS {STAGING_PK_PASS1};")
    print(f"    DROP TABLE IF EXISTS {STAGING_PK_PASS2};")
    print(f"    DROP TABLE IF EXISTS {CHECKPOINT_TABLE};")

    if any_failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
