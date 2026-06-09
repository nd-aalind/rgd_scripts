#!/usr/bin/env python3
"""
biogen_additional_subset.py — Biogen additional cohort ETL

Creates all biogen_april.*_additional tables as a patient-cohort + date-filtered
subset of rgd_udm_silver.*, using a separate patient list.

Patient cohort is pre-materialised once into staging.biogen_cohort_pats_add from:
  - biogen_incremental.patient_list_extra  (ndid AS pat_id)

Date range: 2025-10-01 to 2026-02-15 applied via each table's date_col.
Batching by udm_inc_id (integer PK on all silver tables).

Per source:
  1. Materialise staging PK table of filtered udm_inc_id values
  2. Batch INSERT via staging PK join
  3. Checkpoint/resume — re-running skips already-done sources
  4. InnoDB tuning (session-scoped), commit per batch

Safety:
  - NEVER writes to any source/production table
  - All connections closed in finally blocks
  - Per-source failure isolation: one failed source does not abort the run

Usage:
    python biogen_additional_subset.py
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
    "user":            "admin",
    "password":        "ClAx5UNkjnM8JgLG",
    "database":        "rgd_udm_silver",
    "charset":         "utf8mb4",
    "connect_timeout": 30,
    "read_timeout":    21600,
    "write_timeout":   21600,
}

BATCH_SIZE = 50_000
BATCH_KEY  = "udm_inc_id"

DATE_LO = ""
DATE_HI = "2026-02-15"

COHORT_TABLE = "staging.biogen_cohort_pats_add_n"
CKPT_TABLE   = "staging.biogen_add_checkpoint_n"


# ── Table definitions ────────────────────────────────────────────────
TABLES = [
    {
        "label":          "encounters",
        "source":         "rgd_udm_silver.encounters",
        "target":         "biogen_april.encounters_additional",
        "date_col":       "enc_date_proxy",
        "staging_pk":     "staging.biogen_add_pk_enc",
        "ckpt_key":       "biogen_april.encounters_additional",
        "creates_target": True,
        "optional":       False,
        "distinct":       True,
        "select_cols": """
    a.ndid,
    a.eid                AS encounter_id,
    a.enc_date           AS encounter_date,
    a.enc_reason         AS encounter_reason,
    a.provider_name      AS at_phy_name,
    CAST(NULL AS SIGNED) AS incremental_id""",
    },
    {
        "label":          "diagnosis",
        "source":         "rgd_udm_silver.diagnosis",
        "target":         "biogen_april.diagnosis_additional",
        "date_col":       "enc_date_proxy",
        "staging_pk":     "staging.biogen_add_pk_diag",
        "ckpt_key":       "biogen_april.diagnosis_additional",
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
    CAST(NULL AS SIGNED) AS incremental_id""",
    },
    {
        "label":          "procedures",
        "source":         "rgd_udm_silver.procedures",
        "target":         "biogen_april.procedures_additional",
        "date_col":       "enc_date_proxy",
        "staging_pk":     "staging.biogen_add_pk_proc",
        "ckpt_key":       "biogen_april.procedures_additional",
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
    CAST(NULL AS SIGNED) AS incremental_id""",
    },
    {
        "label":          "allergies",
        "source":         "rgd_udm_silver.allergies",
        "target":         "biogen_april.allergies_additional",
        "date_col":       "enc_date_proxy",
        "staging_pk":     "staging.biogen_add_pk_allrg",
        "ckpt_key":       "biogen_april.allergies_additional",
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
    CAST(NULL AS SIGNED)         AS incremental_id""",
    },
    {
        "label":          "vitals",
        "source":         "rgd_udm_silver.vitals",
        "target":         "biogen_april.vitals_additional",
        "date_col":       "enc_date_proxy",
        "staging_pk":     "staging.biogen_add_pk_vitals",
        "ckpt_key":       "biogen_april.vitals_additional",
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
    CAST(NULL AS SIGNED) AS incremental_id""",
    },
    {
        "label":          "labs",
        "source":         "rgd_udm_silver.labs",
        "target":         "biogen_april.labs_additional",
        "date_col":       "enc_date_proxy",
        "staging_pk":     "staging.biogen_add_pk_labs",
        "ckpt_key":       "biogen_april.labs_additional",
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
    CAST(NULL AS SIGNED)         AS incremental_id""",
    },
    {
        "label":          "radiology",
        "source":         "rgd_udm_silver.radiology",
        "target":         "biogen_april.radiology_additional",
        "date_col":       "enc_date_proxy",
        "staging_pk":     "staging.biogen_add_pk_rad",
        "ckpt_key":       "biogen_april.radiology_additional",
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
    CAST(NULL AS SIGNED)     AS incremental_id""",
    },
    {
        "label":          "ros",
        "source":         "rgd_udm_silver.ros",
        "target":         "biogen_april.ros_additional",
        "date_col":       "enc_date_proxy",
        "staging_pk":     "staging.biogen_add_pk_ros",
        "ckpt_key":       "biogen_april.ros_additional",
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
    CAST(NULL AS SIGNED) AS incremental_id""",
    },
    {
        "label":          "examinations",
        "source":         "rgd_udm_silver.examination",
        "target":         "biogen_april.examinations_additional",
        "date_col":       "enc_date_proxy",
        "staging_pk":     "staging.biogen_add_pk_exam",
        "ckpt_key":       "biogen_april.examinations_additional",
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
    CAST(NULL AS SIGNED) AS incremental_id""",
    },
    {
        "label":          "note_part1",
        "source":         "rgd_udm_silver.notes_part1",
        "target":         "biogen_april.note_additional",
        "date_col":       "enc_start_date",
        "staging_pk":     "staging.biogen_add_pk_note1",
        "ckpt_key":       "biogen_april.note_additional_part1",
        "creates_target": True,
        "optional":       False,
        "select_cols": """
    a.ndid,
    a.eid                AS encounter_id,
    a.enc_start_date     AS encounter_date,
    a.note_type,
    a.note_source,
    a.note,
    CAST(NULL AS SIGNED) AS incremental_id""",
    },
    {
        "label":          "note_part2",
        "source":         "rgd_udm_silver.notes_part2",
        "target":         "biogen_april.note_additional",
        "date_col":       "enc_start_date",
        "staging_pk":     "staging.biogen_add_pk_note2",
        "ckpt_key":       "biogen_april.note_additional_part2",
        "creates_target": False,
        "optional":       False,
        "select_cols": """
    a.ndid,
    a.eid                AS encounter_id,
    a.enc_start_date     AS encounter_date,
    a.note_type,
    a.note_source,
    a.note,
    CAST(NULL AS SIGNED) AS incremental_id""",
    },
    {
        "label":          "medications",
        "source":         "rgd_udm_silver.medications",
        "target":         "biogen_april.medications_additional",
        "date_col":       "enc_start_date",
        "staging_pk":     "staging.biogen_add_pk_meds",
        "ckpt_key":       "biogen_april.medications_additional",
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
    CAST(NULL AS SIGNED) AS incremental_id""",
    },
    {
        "label":          "past_history",
        "source":         "rgd_udm_silver.past_history",
        "target":         "biogen_april.past_history_additional",
        "date_col":       "visit_date",
        "staging_pk":     "staging.biogen_add_pk_phist",
        "ckpt_key":       "biogen_april.past_history_additional",
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
    CAST(NULL AS SIGNED)       AS incremental_id""",
    },
    {
        "label":          "patient_demographics",
        "source":         "rgd_udm_silver.patient_demographics",
        "target":         "biogen_april.patient_demographics_additional",
        "date_col":       None,
        "staging_pk":     "staging.biogen_add_pk_pdemo",
        "ckpt_key":       "biogen_april.patient_demographics_additional",
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
    CAST(NULL AS SIGNED) AS incremental_id""",
    },
]


# ── Report config ────────────────────────────────────────────────────
REPORT_TARGETS = [
    {"target": "biogen_april.encounters_additional",           "enc_col": "encounter_id"},
    {"target": "biogen_april.diagnosis_additional",            "enc_col": "encounter_id"},
    {"target": "biogen_april.procedures_additional",           "enc_col": "encounter_id"},
    {"target": "biogen_april.allergies_additional",            "enc_col": "encounter_id"},
    {"target": "biogen_april.vitals_additional",               "enc_col": "encounter_id"},
    {"target": "biogen_april.labs_additional",                 "enc_col": "encounter_id"},
    {"target": "biogen_april.radiology_additional",            "enc_col": "encounter_id"},
    {"target": "biogen_april.ros_additional",                  "enc_col": "encounter_id"},
    {"target": "biogen_april.examinations_additional",         "enc_col": "encounter_id"},
    {"target": "biogen_april.note_additional",                 "enc_col": "encounter_id"},
    {"target": "biogen_april.medications_additional",          "enc_col": "encounter_id"},
    {"target": "biogen_april.past_history_additional",         "enc_col": "encounter_id"},
    {"target": "biogen_april.patient_demographics_additional", "enc_col": None},
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


def _build_ranges(cur, staging_pk: str):
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

        print(f"  Creating cohort table {COHORT_TABLE}...")
        if not _table_exists(cur, COHORT_TABLE):
            cur.execute(f"""
                CREATE TABLE {COHORT_TABLE} AS
                SELECT ndid AS pat_id
                FROM biogen_incremental.patient_list_extra
            """)
            cur.execute(
                f"ALTER TABLE {COHORT_TABLE} ADD INDEX idx_pat_id (pat_id)"
            )
            conn.commit()
            cur.execute(f"SELECT COUNT(*) FROM {COHORT_TABLE}")
            n = cur.fetchone()[0]
            print(f"    created  ({n:,} rows)")
        else:
            cur.execute(f"SELECT COUNT(*) FROM {COHORT_TABLE}")
            n = cur.fetchone()[0]
            print(f"    already exists, reusing  ({n:,} rows)")

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
        if tbl["optional"]:
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
                    INNER JOIN {COHORT_TABLE} c ON a.ndid = c.pat_id
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

        print(f"  Creating staging PK table {staging_pk}...")
        if not _table_exists(cur, staging_pk):
            date_filter = ""
            if date_col is not None:
                lo_clause = f"  AND a.{date_col} >= '{DATE_LO}'\n" if DATE_LO else ""
                hi_clause = f"  AND a.{date_col} <= '{DATE_HI}'" if DATE_HI else ""
                date_filter = lo_clause + hi_clause
            cur.execute(f"""
                CREATE TABLE {staging_pk} AS
                SELECT a.{BATCH_KEY}
                FROM {source} a
                INNER JOIN {COHORT_TABLE} c ON a.ndid = c.pat_id
                WHERE a.{BATCH_KEY} IS NOT NULL
                {date_filter}
            """)
            cur.execute(
                f"ALTER TABLE {staging_pk} ADD INDEX idx_pk ({BATCH_KEY})"
            )
            conn.commit()
            cur.execute(f"SELECT COUNT(*) FROM {staging_pk}")
            n = cur.fetchone()[0]
            print(f"    created  ({n:,} eligible rows)")
        else:
            cur.execute(f"SELECT COUNT(*) FROM {staging_pk}")
            n = cur.fetchone()[0]
            print(f"    already exists, reusing  ({n:,} rows)")

        ranges, total = _build_ranges(cur, staging_pk)
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
        for lo, hi in ranges:
            sql = f"""
INSERT INTO {target}
SELECT {distinct}{select_cols}
FROM {source} a
INNER JOIN {staging_pk} pk ON a.{BATCH_KEY} = pk.{BATCH_KEY}
WHERE a.{BATCH_KEY} >= {lo} AND a.{BATCH_KEY} < {hi}
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


# ── Main ──────────────────────────────────────────────────────────────

def main():
    print(f"\n{'='*70}", flush=True)
    print(f"  Biogen Additional Subset ETL — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  cohort table : {COHORT_TABLE}")
    print(f"  cohort source: biogen_incremental.patient_list_extra")
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
            ranges = all_ranges.get(label, [])
            if not ranges:
                print(f"  {label:<24}  {'SKIPPED':<10}  {'—':>12}  {'—':>8}")
            continue
        r = results[label]
        if r["status"] == "done":
            status_tag = "DONE"
        elif r["status"] == "skipped":
            status_tag = "SKIPPED"
        else:
            status_tag = "FAILED"
        print(f"  {label:<24}  {status_tag:<10}  {r['rows']:>12,}  {r['secs']}s")

    print(f"{'='*70}\n")

    print_report()

    print("  Cleanup SQL (run after ETL is fully verified):")
    for tbl in TABLES:
        print(f"    DROP TABLE IF EXISTS {tbl['staging_pk']};")
    print(f"    DROP TABLE IF EXISTS {COHORT_TABLE};")
    print(f"    DROP TABLE IF EXISTS {CKPT_TABLE};")
    print()

    if any_failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
