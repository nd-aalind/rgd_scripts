#!/usr/bin/env python3
"""
biogen_incremental.py — Biogen incremental (May) cohort ETL

Creates all biogen_april.*_inc_may tables as a patient-cohort + date-filtered
subset of rgd_udm_silver.*.

Patient cohort : biogen_april.patients_demo_inc_combined (pre-existing, JOIN on ndid)
Date range     : 2026-02-16 to 2026-03-31
Extra columns  : udm_active_flag, udm_unq_id appended to every table
DISTINCT       : applied on every source

Batching by udm_inc_id (integer PK on all silver tables).

Per source:
  1. Materialise staging PK table of filtered udm_inc_id values
  2. Batch INSERT (SELECT DISTINCT) via staging PK join
  3. Checkpoint/resume — re-running skips already-done sources
  4. InnoDB tuning (session-scoped), commit per batch

Safety:
  - NEVER writes to any source/production table
  - All connections closed in finally blocks
  - Per-source failure isolation: one failed source does not abort the run

Usage:
    python biogen_incremental.py
"""

import sys
import time
from datetime import datetime
import pymysql
from tqdm import tqdm
import os
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())

# ── Configuration ────────────────────────────────────────────────────
DB_CONFIG = {
    "host":            os.environ.get("DB_HOST"),
    "port":            3306,
    "user":            os.environ.get("DB_ADMIN_USER"),
    "password":        os.environ.get("DB_ADMIN_PASSWORD"),
    "database":        "rgd_udm_silver",
    "charset":         "utf8mb4",
    "connect_timeout": 30,
    "read_timeout":    21600,
    "write_timeout":   21600,
}

BATCH_SIZE = 50_000
BATCH_KEY  = "udm_inc_id"

DATE_LO = "2026-02-16"
DATE_HI = "2026-03-31"

# Pre-existing table — verified at startup, not created by this script.
# Column is ndid (not pat_id), so JOIN is: a.ndid = c.ndid
COHORT_TABLE = "biogen_april.patients_demo_inc_combined"
CKPT_TABLE   = "staging.biogen_inc_may_v2_checkpoint"


# ── Table definitions ────────────────────────────────────────────────
# All tables: udm_active_flag + udm_unq_id appended.
TABLES = [
    {
        "label":          "encounters",
        "source":         "incremental_test.encounters",
        "target":         "biogen_april.encounters_inc_may_v2",
        "date_col":       "enc_date_proxy",
        "staging_pk":     "staging.biogen_inc_may_v2_pk_enc",
        "ckpt_key":       "biogen_april.encounters_inc_may_v2",
        "creates_target": True,
        "optional":       False,
        "select_cols": """
    a.ndid,
    a.eid                AS encounter_id,
    a.enc_date           AS encounter_date,
    a.enc_reason         AS encounter_reason,
    a.provider_name      AS at_phy_name,
    CAST(NULL AS SIGNED) AS incremental_id,
    a.udm_active_flag,
    a.udm_unq_id""",
    },
    {
        "label":          "diagnosis",
        "source":         "incremental_test.diagnosis",
        "target":         "biogen_april.diagnosis_inc_may_v2",
        "date_col":       "enc_date_proxy",
        "staging_pk":     "staging.biogen_inc_may_v2_pk_diag",
        "ckpt_key":       "biogen_april.diagnosis_inc_may_v2",
        "creates_target": True,
        "optional":       False,
        "select_cols": """
    a.ndid,
    a.eid                AS encounter_id,
    a.enc_date           AS encounter_date,
    a.diag_date          AS diagnosis_recorded_date,
    a.diag_code,
    a.diag_desc,
    a.diag_coding_system,
    CAST(NULL AS SIGNED) AS incremental_id,
    a.udm_active_flag,
    a.udm_unq_id""",
    },
    {
        "label":          "procedures",
        "source":         "incremental_test.procedures",
        "target":         "biogen_april.procedures_inc_may_v2",
        "date_col":       "enc_date_proxy",
        "staging_pk":     "staging.biogen_inc_may_v2_pk_proc",
        "ckpt_key":       "biogen_april.procedures_inc_may_v2",
        "creates_target": True,
        "optional":       False,
        "select_cols": """
    a.ndid,
    a.eid                AS encounter_id,
    a.encounter_date,
    a.proc_start_date    AS procedure_date,
    a.proc_code,
    a.proc_name,
    a.proc_coding_system,
    CAST(NULL AS SIGNED) AS incremental_id,
    a.udm_active_flag,
    a.udm_unq_id""",
    },
    {
        "label":          "allergies",
        "source":         "incremental_test.allergies",
        "target":         "biogen_april.allergies_inc_may_v2",
        "date_col":       "enc_date_proxy",
        "staging_pk":     "staging.biogen_inc_may_v2_pk_allrg",
        "ckpt_key":       "biogen_april.allergies_inc_may_v2",
        "creates_target": True,
        "optional":       False,
        "select_cols": """
    a.ndid,
    a.eid                        AS encounter_id,
    a.enc_date_proxy             AS encounter_date,
    a.allergen_code,
    a.allergen_coding_system,
    a.allergy_type,
    a.allergen_name,
    a.allergy_reaction_name      AS allergy_reaction_type,
    a.allergy_status,
    CAST(NULL AS SIGNED)         AS incremental_id,
    a.udm_active_flag,
    a.udm_unq_id""",
    },
    {
        "label":          "vitals",
        "source":         "incremental_test.vitals",
        "target":         "biogen_april.vitals_inc_may_v2",
        "date_col":       "enc_date_proxy",
        "staging_pk":     "staging.biogen_inc_may_v2_pk_vitals",
        "ckpt_key":       "biogen_april.vitals_inc_may_v2",
        "creates_target": True,
        "optional":       False,
        "select_cols": """
    a.ndid,
    a.eid                AS encounter_id,
    a.vital_date,
    a.vital_coding_system,
    a.vital_id,
    a.vital_name,
    a.vital_result,
    a.vital_unit,
    CAST(NULL AS SIGNED) AS incremental_id,
    a.udm_active_flag,
    a.udm_unq_id""",
    },
    {
        "label":          "labs",
        "source":         "incremental_test.labs",
        "target":         "biogen_april.labs_inc_may_v2",
        "date_col":       "enc_date_proxy",
        "staging_pk":     "staging.biogen_inc_may_v2_pk_labs",
        "ckpt_key":       "biogen_april.labs_inc_may_v2",
        "creates_target": True,
        "optional":       False,
        "select_cols": """
    a.ndid,
    a.eid                        AS encounter_id,
    a.sample_collection_date,
    a.result_date,
    a.result_name,
    a.result_id,
    a.result_code,
    a.result_coding_system,
    a.result_value,
    a.result_unit,
    a.result_range,
    a.ordering_provider_name,
    CAST(NULL AS SIGNED)         AS incremental_id,
    a.udm_active_flag,
    a.udm_unq_id""",
    },
    {
        "label":          "radiology",
        "source":         "incremental_test.radiology",
        "target":         "biogen_april.radiology_inc_may_v2",
        "date_col":       "enc_date_proxy",
        "staging_pk":     "staging.biogen_inc_may_v2_pk_rad",
        "ckpt_key":       "biogen_april.radiology_inc_may_v2",
        "creates_target": True,
        "optional":       False,
        "select_cols": """
    a.ndid,
    a.eid                    AS encounter_id,
    DATE(a.enc_date)         AS encounter_date,
    a.report_id,
    a.study_name             AS test_name,
    a.img_finding            AS test_parameter,
    a.img_status             AS resultstatus,
    a.img_date               AS resultdate,
    a.img_report_text        AS value,
    a.internal_notes         AS note,
    CAST(NULL AS SIGNED)     AS incremental_id,
    a.udm_active_flag,
    a.udm_unq_id""",
    },
    {
        "label":          "ros",
        "source":         "incremental_test.ros",
        "target":         "biogen_april.ros_inc_may_v2",
        "date_col":       "enc_date_proxy",
        "staging_pk":     "staging.biogen_inc_may_v2_pk_ros",
        "ckpt_key":       "biogen_april.ros_inc_may_v2",
        "creates_target": True,
        "optional":       False,
        "select_cols": """
    a.ndid,
    a.eid                AS encounter_id,
    a.enc_start_date     AS encounter_date,
    a.ros_category,
    a.ros_name           AS system_name,
    a.ros_option         AS Present,
    a.ros_notes          AS note,
    CAST(NULL AS SIGNED) AS incremental_id,
    a.udm_active_flag,
    a.udm_unq_id""",
    },
    {
        "label":          "examination",
        "source":         "incremental_test.examination",
        "target":         "biogen_april.examination_inc_may_v2",
        "date_col":       "enc_date_proxy",
        "staging_pk":     "staging.biogen_inc_may_v2_pk_exam",
        "ckpt_key":       "biogen_april.examinations_inc_may_v2",
        "creates_target": True,
        "optional":       False,
        "select_cols": """
    a.ndid,
    a.eid                AS encounter_id,
    a.enc_start_date     AS encounter_date,
    a.exam_date,
    a.examid             AS exam_id,
    a.exam_category,
    a.exam_name,
    a.exam_findings,
    CAST(NULL AS SIGNED) AS incremental_id,
    a.udm_active_flag,
    a.udm_unq_id""",
    },
    {
        "label":          "note_part1",
        "source":         "incremental_test.notes_part1",
        "target":         "biogen_april.note_inc_may_v2",
        "date_col":       "enc_start_date",
        "staging_pk":     "staging.biogen_inc_may_v2_pk_note1",
        "ckpt_key":       "biogen_april.note_inc_may_v2_part1",
        "creates_target": True,
        "optional":       False,
        "select_cols": """
    a.ndid,
    a.eid                AS encounter_id,
    a.enc_start_date     AS encounter_date,
    a.note_type,
    a.note_source,
    a.note,
    CAST(NULL AS SIGNED) AS incremental_id,
    a.udm_active_flag,
    a.udm_unq_id""",
    },
    {
        "label":          "note_part2",
        "source":         "incremental_test.notes_part2",
        "target":         "biogen_april.note_inc_may_v2",
        "date_col":       "enc_start_date",
        "staging_pk":     "staging.biogen_inc_may_v2_pk_note2",
        "ckpt_key":       "biogen_april.note_inc_may_v2_part2",
        "creates_target": False,
        "optional":       False,
        "select_cols": """
    a.ndid,
    a.eid                AS encounter_id,
    a.enc_start_date     AS encounter_date,
    a.note_type,
    a.note_source,
    a.note,
    CAST(NULL AS SIGNED) AS incremental_id,
    a.udm_active_flag,
    a.udm_unq_id""",
    },
    {
        "label":          "medications",
        "source":         "incremental_test.medications",
        "target":         "biogen_april.medications_inc_may_v2",
        "date_col":       "enc_start_date",
        "staging_pk":     "staging.biogen_inc_may_v2_pk_meds",
        "ckpt_key":       "biogen_april.medications_inc_may_v2",
        "creates_target": True,
        "optional":       True,
        "select_cols": """
    a.ndid,
    a.med_id,
    a.eid                AS encounter_id,
    a.enc_start_date,
    a.med_start_date,
    a.med_end_date,
    a.med_code,
    a.med_name,
    a.med_coding_system,
    a.med_status,
    a.med_formulation,
    a.med_strength,
    a.med_pb_qty,
    a.med_days_supply,
    a.med_refills,
    a.med_directions,
    a.med_fill_type,
    CAST(NULL AS SIGNED) AS incremental_id,
    a.udm_active_flag,
    a.udm_unq_id""",
    },
    {
        "label":          "past_history",
        "source":         "incremental_test.past_history",
        "target":         "biogen_april.past_history_inc_may_v2",
        "date_col":       "visit_date",
        "staging_pk":     "staging.biogen_inc_may_v2_pk_phist",
        "ckpt_key":       "biogen_april.past_history_inc_may_v2",
        "creates_target": True,
        "optional":       True,
        "select_cols": """
    a.ndid,
    a.eid                      AS encounter_id,
    a.visit_date,
    a.medical_history,
    a.past_surgical_history,
    a.family_history_note,
    a.social_history_full,
    CAST(NULL AS SIGNED)       AS incremental_id,
    a.udm_active_flag,
    a.udm_unq_id""",
    },
    {
        "label":          "patient_demographics",
        "source":         "incremental_test.patient_demographics",
        "target":         "biogen_april.patient_demographics_inc_may_v2",
        "date_col":       None,
        "staging_pk":     "staging.biogen_inc_may_v2_pk_pdemo",
        "ckpt_key":       "biogen_april.patient_demographics_inc_may_v2",
        "creates_target": True,
        "optional":       True,
        "select_cols": """
    a.ndid,
    a.year_of_birth,
    a.gender,
    a.pat_lan,
    a.pat_country,
    a.pat_zip,
    a.pat_race,
    a.pat_ms,
    a.pat_ds,
    a.deceasedDate,
    CAST(NULL AS SIGNED) AS incremental_id,
    a.udm_active_flag,
    a.udm_unq_id""",
    },
]


# ── Report config ────────────────────────────────────────────────────
REPORT_TARGETS = [
    {"target": "biogen_april.encounters_inc_may_v2",           "enc_col": "encounter_id"},
    {"target": "biogen_april.diagnosis_inc_may_v2",            "enc_col": "encounter_id"},
    {"target": "biogen_april.procedures_inc_may_v2",           "enc_col": "encounter_id"},
    {"target": "biogen_april.allergies_inc_may_v2",            "enc_col": "encounter_id"},
    {"target": "biogen_april.vitals_inc_may_v2",               "enc_col": "encounter_id"},
    {"target": "biogen_april.labs_inc_may_v2",                 "enc_col": "encounter_id"},
    {"target": "biogen_april.radiology_inc_may_v2",            "enc_col": "encounter_id"},
    {"target": "biogen_april.ros_inc_may_v2",                  "enc_col": "encounter_id"},
    {"target": "biogen_april.examination_inc_may_v2",         "enc_col": "encounter_id"},
    {"target": "biogen_april.note_inc_may_v2",                 "enc_col": "encounter_id"},
    {"target": "biogen_april.medications_inc_may_v2",          "enc_col": "encounter_id"},
    {"target": "biogen_april.past_history_inc_may_v2",         "enc_col": "encounter_id"},
    {"target": "biogen_april.patient_demographics_inc_may_v2", "enc_col": None},
]


# ── Helpers ──────────────────────────────────────────────────────────

def get_connection():
    return pymysql.connect(**DB_CONFIG)


def _table_exists(cur, full_table_name: str) -> bool:
    schema, table = full_table_name.split(".", 1)
    cur.execute(
        "SELECT COUNT(*) FROM information_schema.tables "
        "WHERE table_schema = %s AND table_name = %s",
        (schema, table),
    )
    return cur.fetchone()[0] > 0


def _col_exists(cur, schema, table, column):
    cur.execute("""
        SELECT COUNT(*) FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s AND column_name = %s
    """, (schema, table, column))
    return cur.fetchone()[0] > 0


def _index_exists(cur, schema, table, column):
    cur.execute("""
        SELECT COUNT(*) FROM information_schema.statistics
        WHERE table_schema = %s AND table_name = %s AND column_name = %s
    """, (schema, table, column))
    return cur.fetchone()[0] > 0


def _ensure_index(cur, conn, schema, table, column):
    if not _index_exists(cur, schema, table, column):
        print(f"    + CREATE INDEX idx_{column} ON {schema}.{table} ({column})")
        cur.execute(f"CREATE INDEX idx_{column} ON {schema}.{table} ({column})")
        conn.commit()
    else:
        print(f"    ✓ {schema}.{table}({column})")


def _build_ranges(cur, staging_pk: str, key: str = None):
    key = key or BATCH_KEY
    cur.execute(f"SELECT COUNT(*) FROM {staging_pk}")
    total = cur.fetchone()[0]

    if total == 0:
        return [], 0

    cur.execute(f"""
        SELECT {key}
        FROM (
            SELECT {key},
                   ROW_NUMBER() OVER (ORDER BY {key}) AS rn
            FROM {staging_pk}
        ) t
        WHERE (rn - 1) % {BATCH_SIZE} = 0
        ORDER BY {key}
    """)
    boundaries = [row[0] for row in cur.fetchall() if row[0] is not None]

    cur.execute(f"SELECT MAX({key}) FROM {staging_pk}")
    max_pk = int(cur.fetchone()[0])

    ranges = []
    for i, lo in enumerate(boundaries):
        hi = boundaries[i + 1] if i + 1 < len(boundaries) else max_pk + 1
        ranges.append((lo, hi))

    return ranges, total


# ── Checkpoint ───────────────────────────────────────────────────────

def is_done(conn, ckpt_key: str) -> bool:
    cur = conn.cursor()
    cur.execute(
        f"SELECT status FROM {CKPT_TABLE} WHERE source_key = %s",
        (ckpt_key,),
    )
    row = cur.fetchone()
    cur.close()
    return row is not None and row[0] == "done"


def mark(conn, ckpt_key: str, status: str, rows: int = 0, error: str = None) -> None:
    cur = conn.cursor()
    cur.execute(f"""
        INSERT INTO {CKPT_TABLE}
            (source_key, status, rows_inserted, started_at, completed_at, error_msg)
        VALUES (%s, %s, %s, NOW(), IF(%s = 'done', NOW(), NULL), %s)
        ON DUPLICATE KEY UPDATE
            status        = VALUES(status),
            rows_inserted = VALUES(rows_inserted),
            completed_at  = IF(VALUES(status) = 'done', NOW(), NULL),
            error_msg     = VALUES(error_msg)
    """, (ckpt_key, status, rows, status, error))
    conn.commit()
    cur.close()


# ── Setup ─────────────────────────────────────────────────────────────

def setup_global() -> None:
    conn = get_connection()
    cur  = conn.cursor()

    try:
        print("  Creating checkpoint table...")
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {CKPT_TABLE} (
                source_key    VARCHAR(200) NOT NULL PRIMARY KEY,
                status        ENUM('running','done','failed') NOT NULL DEFAULT 'running',
                rows_inserted BIGINT      DEFAULT 0,
                started_at    DATETIME    DEFAULT NULL,
                completed_at  DATETIME    DEFAULT NULL,
                error_msg     TEXT        DEFAULT NULL
            )
        """)
        conn.commit()
        print("    ready")

        print(f"  Checking cohort table {COHORT_TABLE}...")
        if _table_exists(cur, COHORT_TABLE):
            cur.execute(f"SELECT COUNT(*) FROM {COHORT_TABLE}")
            n = cur.fetchone()[0]
            print(f"    found  ({n:,} rows)")
        else:
            print(f"  [ERROR] {COHORT_TABLE} does not exist. "
                  f"Run the patients_demo_inc_combined ETL first.")
            sys.exit(1)

    finally:
        cur.close()
        conn.close()


def setup_source(tbl: dict):
    label      = tbl["label"]
    source     = tbl["source"]
    target     = tbl["target"]
    date_col   = tbl["date_col"]
    staging_pk = tbl["staging_pk"]

    conn = get_connection()
    cur  = conn.cursor()

    try:
        if not _table_exists(cur, source):
            print(f"  [WARN] {label}: source {source} does not exist — skipping")
            return [], 0

        if tbl["creates_target"]:
            if not _table_exists(cur, target):
                print(f"  Creating target table {target} (empty schema)...")
                cur.execute(f"""
                    CREATE TABLE IF NOT EXISTS {target}
                    SELECT {tbl['select_cols']}
                    FROM {source} a
                    INNER JOIN {COHORT_TABLE} c ON a.ndid = c.ndid
                    WHERE 1 = 0
                """)
                conn.commit()
                print(f"    created (empty)")
            else:
                print(f"  Target {target} already exists, appending to it")
        else:
            if not _table_exists(cur, target):
                print(
                    f"  [WARN] {label}: target {target} does not exist "
                    f"(creates_target=False) — skipping"
                )
                return [], 0

        # Detect effective batch key — fall back to ndid if udm_inc_id is absent
        src_schema, src_table = source.split(".", 1)
        cur.execute("""
            SELECT COUNT(*) FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s AND column_name = %s
        """, (src_schema, src_table, BATCH_KEY))
        eff_key = BATCH_KEY if cur.fetchone()[0] > 0 else "ndid"
        if eff_key != BATCH_KEY:
            print(f"  [INFO] {label}: '{BATCH_KEY}' not in {source} — batching by '{eff_key}'")
        tbl["_eff_key"] = eff_key

        print(f"  Creating staging PK table {staging_pk}...")
        if not _table_exists(cur, staging_pk):
            date_filter = ""
            if date_col is not None:
                lo_clause = f"  AND a.{date_col} >= '{DATE_LO}'\n" if DATE_LO else ""
                hi_clause = f"  AND a.{date_col} <= '{DATE_HI}'" if DATE_HI else ""
                date_filter = lo_clause + hi_clause
            cur.execute(f"""
                CREATE TABLE {staging_pk} AS
                SELECT a.{eff_key}
                FROM {source} a
                INNER JOIN {COHORT_TABLE} c ON a.ndid = c.ndid
                WHERE a.{eff_key} IS NOT NULL
                {date_filter}
            """)
            cur.execute(
                f"ALTER TABLE {staging_pk} ADD INDEX idx_pk ({eff_key})"
            )
            conn.commit()
            cur.execute(f"SELECT COUNT(*) FROM {staging_pk}")
            n = cur.fetchone()[0]
            print(f"    created  ({n:,} eligible rows)")
        else:
            cur.execute(f"SELECT COUNT(*) FROM {staging_pk}")
            n = cur.fetchone()[0]
            print(f"    already exists, reusing  ({n:,} rows)")

        ranges, total = _build_ranges(cur, staging_pk, eff_key)
        print(f"    {len(ranges)} batches of ~{BATCH_SIZE:,} rows each  (total eligible: {total:,})")
        return ranges, total

    finally:
        cur.close()
        conn.close()


# ── Runner ────────────────────────────────────────────────────────────

def run_source(tbl: dict, ranges: list, pbar: tqdm) -> dict:
    label       = tbl["label"]
    source      = tbl["source"]
    target      = tbl["target"]
    staging_pk  = tbl["staging_pk"]
    ckpt_key    = tbl["ckpt_key"]
    select_cols = tbl["select_cols"]

    conn       = get_connection()
    t0         = time.time()
    total_rows = 0

    try:
        if is_done(conn, ckpt_key):
            pbar.update(len(ranges))
            conn.close()
            return {"status": "skipped", "rows": 0, "secs": 0.0}

        mark(conn, ckpt_key, "running")

        cur = conn.cursor()
        cur.execute("SET unique_checks = 0")
        cur.execute("SET foreign_key_checks = 0")

        distinct = "DISTINCT " if tbl.get("distinct") else ""
        eff_key  = tbl.get("_eff_key", BATCH_KEY)
        for lo, hi in ranges:
            sql = f"""
INSERT INTO {target}
SELECT {distinct}{select_cols}
FROM {source} a
INNER JOIN {staging_pk} pk ON a.{eff_key} = pk.{eff_key}
WHERE a.{eff_key} >= {lo} AND a.{eff_key} < {hi}
"""
            cur.execute(sql)
            conn.commit()
            total_rows += cur.rowcount
            pbar.update(1)

        cur.execute("SET unique_checks = 1")
        cur.execute("SET foreign_key_checks = 1")
        cur.close()

        elapsed = round(time.time() - t0, 1)
        mark(conn, ckpt_key, "done", total_rows)
        conn.close()
        return {"status": "done", "rows": total_rows, "secs": elapsed}

    except Exception as exc:
        elapsed = round(time.time() - t0, 1)
        err_msg = str(exc)
        print(f"\n  [ERROR] {label}: {err_msg}")
        try:
            mark(conn, ckpt_key, "failed", total_rows, err_msg)
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass
        return {"status": f"FAILED: {exc}", "rows": total_rows, "secs": elapsed}


# ── Final report ──────────────────────────────────────────────────────

def print_report() -> None:
    conn = get_connection()
    cur  = conn.cursor()

    print(f"\n{'='*80}")
    print(f"  FINAL REPORT — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*80}")
    print(f"  {'Table':<52}  {'Rows':>12}  {'Distinct ndid':>15}  {'Distinct enc':>14}")
    print(f"  {'-'*52}  {'-'*12}  {'-'*15}  {'-'*14}")

    try:
        for entry in REPORT_TARGETS:
            target  = entry["target"]
            enc_col = entry["enc_col"]

            if not _table_exists(cur, target):
                print(f"  {target:<52}  {'(table missing)':>12}")
                continue

            enc_expr = f"COUNT(DISTINCT {enc_col})" if enc_col else "NULL"
            cur.execute(f"""
                SELECT
                    COUNT(*)              AS rows,
                    COUNT(DISTINCT ndid)  AS ndids,
                    {enc_expr}            AS encs
                FROM {target}
            """)
            row     = cur.fetchone()
            rows_n  = row[0]
            ndids_n = row[1]
            encs_n  = row[2] if enc_col else None

            enc_str = f"{encs_n:,}" if encs_n is not None else "N/A"
            print(
                f"  {target:<52}  {rows_n:>12,}  {ndids_n:>15,}  {enc_str:>14}"
            )
    finally:
        cur.close()
        conn.close()

    print(f"{'='*80}\n")


# ── Index setup ───────────────────────────────────────────────────────

def ensure_indexes() -> None:
    """
    Ensure all indexes required for fast staging PK creation and batch INSERTs:
      - cohort table : ndid
      - each source  : ndid (cohort JOIN)
                       udm_inc_id (batch key, if column exists)
                       date_col   (date filter in staging PK query, if column exists)
    """
    conn = get_connection()
    cur  = conn.cursor()

    try:
        print("Ensuring indexes:")

        # ── Cohort table ─────────────────────────────────────────────
        coh_schema, coh_table = COHORT_TABLE.split(".", 1)
        if _table_exists(cur, COHORT_TABLE):
            _ensure_index(cur, conn, coh_schema, coh_table, "ndid")

        # ── Source tables ─────────────────────────────────────────────
        seen = set()
        for tbl in TABLES:
            source   = tbl["source"]
            date_col = tbl["date_col"]

            if source in seen:
                continue
            seen.add(source)

            if not _table_exists(cur, source):
                continue

            src_schema, src_table = source.split(".", 1)

            # ndid — cohort join key
            _ensure_index(cur, conn, src_schema, src_table, "ndid")

            # udm_inc_id — primary batch key (skip if absent; ndid used instead)
            if _col_exists(cur, src_schema, src_table, BATCH_KEY):
                _ensure_index(cur, conn, src_schema, src_table, BATCH_KEY)

            # date_col — staging PK filter
            if date_col and _col_exists(cur, src_schema, src_table, date_col):
                _ensure_index(cur, conn, src_schema, src_table, date_col)

    finally:
        cur.close()
        conn.close()

    print()


# ── Main ──────────────────────────────────────────────────────────────

def main():
    print(f"\n{'='*70}", flush=True)
    print(f"  Biogen Incremental (May) ETL — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  cohort table : {COHORT_TABLE}")
    print(f"  checkpoint   : {CKPT_TABLE}")
    print(f"  date range   : {DATE_LO}  to  {DATE_HI}")
    print(f"  batch key    : {BATCH_KEY}")
    print(f"  batch size   : {BATCH_SIZE:,}")
    print(f"  sources      : {len(TABLES)}")
    print(f"{'='*70}\n", flush=True)

    print("Global setup:")
    sys.stdout.flush()
    setup_global()
    print()

    ensure_indexes()

    all_ranges    = {}
    total_batches = 0

    for tbl in TABLES:
        label = tbl["label"]
        print(f"Setup [{label}]  ({tbl['source']}  ->  {tbl['target']})")
        sys.stdout.flush()
        ranges, _ = setup_source(tbl)
        all_ranges[label] = ranges
        total_batches += len(ranges)
        print()

    print(f"  Total batches across all sources: {total_batches:,}")
    print()

    results    = {}
    any_failed = False

    with tqdm(total=total_batches, desc="Overall", unit="batch") as pbar:
        for tbl in TABLES:
            label  = tbl["label"]
            ranges = all_ranges.get(label, [])

            if not ranges:
                print(f"  [SKIP] {label} — no eligible rows or already done")
                sys.stdout.flush()
                continue

            result = run_source(tbl, ranges, pbar)
            results[label] = result

            if result["status"].startswith("FAILED"):
                any_failed = True

    print(f"\n{'='*70}")
    print(f"  Per-source summary")
    print(f"{'='*70}")
    print(f"  {'Label':<24}  {'Status':<10}  {'Rows':>12}  {'Secs':>8}")
    print(f"  {'-'*24}  {'-'*10}  {'-'*12}  {'-'*8}")

    for tbl in TABLES:
        label = tbl["label"]
        if label not in results:
            if not all_ranges.get(label):
                print(f"  {label:<24}  {'SKIPPED':<10}  {'—':>12}  {'—':>8}")
            continue
        r = results[label]
        status_tag = "DONE"    if r["status"] == "done"    else \
                     "SKIPPED" if r["status"] == "skipped" else "FAILED"
        rows_str = f"{r['rows']:,}"
        secs_str = f"{r['secs']}s"
        print(f"  {label:<24}  {status_tag:<10}  {rows_str:>12}  {secs_str:>8}")

    print(f"{'='*70}\n")

    print_report()

    print("  Cleanup SQL (run after ETL is fully verified):")
    for tbl in TABLES:
        print(f"    DROP TABLE IF EXISTS {tbl['staging_pk']};")
    print(f"    DROP TABLE IF EXISTS {CKPT_TABLE};")
    print()

    if any_failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
