#!/usr/bin/env python3
"""
create_table.py — Biogen April INSERT-only runner

Assumes all setup from biogen_subset.py has already run:
  - staging.biogen_cohort_pats    exists
  - staging.biogen_april_checkpoint exists
  - All staging.biogen_pk_* tables exist (for tables whose staging completed)
  - All biogen_april.* target tables exist (empty schema already created)

This script ONLY does batch INSERTs — no DDL, no staging table creation.

For each table in TABLES:
  - If staging PK table does not exist  → SKIP (print warning)
  - If target table does not exist      → SKIP (print warning)
  - If checkpoint already 'done'        → SKIP
  - Otherwise: batch INSERT with InnoDB tuning, commit per batch

Re-runnable — checkpoint/resume via staging.biogen_april_checkpoint.
Per-source failure isolation — one failed table does not abort the run.

Usage:
    python create_table.py
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
    "database":        "rgd_udm_silver",
    "charset":         "utf8mb4",
    "connect_timeout": 30,
    "read_timeout":    21600,
    "write_timeout":   21600,
}

BATCH_SIZE = 50_000
BATCH_KEY  = "udm_inc_id"

DATE_LO = "2025-10-01"
DATE_HI = "2026-02-15"

COHORT_TABLE = "staging.biogen_cohort_pats"
CKPT_TABLE   = "staging.biogen_april_checkpoint"


# ── Table definitions (same as biogen_subset.py) ─────────────────────
TABLES = [
    {
        "label":      "encounters",
        "source":     "rgd_udm_silver.encounters",
        "target":     "biogen_april.encounters",
        "date_col":   "enc_date_proxy",
        "staging_pk": "staging.biogen_pk_enc",
        "ckpt_key":   "biogen_april.encounters",
        "select_cols": """
    a.ndid,
    a.eid                AS encounter_id,
    a.enc_date           AS encounter_date,
    a.enc_reason         AS encounter_reason,
    a.provider_name      AS at_phy_name,
    CAST(NULL AS SIGNED) AS incremental_id""",
    },
    {
        "label":      "diagnosis",
        "source":     "rgd_udm_silver.diagnosis",
        "target":     "biogen_april.diagnosis",
        "date_col":   "enc_date_proxy",
        "staging_pk": "staging.biogen_pk_diag",
        "ckpt_key":   "biogen_april.diagnosis",
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
        "label":      "procedures",
        "source":     "rgd_udm_silver.procedures",
        "target":     "biogen_april.procedures",
        "date_col":   "enc_date_proxy",
        "staging_pk": "staging.biogen_pk_proc",
        "ckpt_key":   "biogen_april.procedures",
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
        "label":      "allergies",
        "source":     "rgd_udm_silver.allergies",
        "target":     "biogen_april.allergies",
        "date_col":   "enc_date_proxy",
        "staging_pk": "staging.biogen_pk_allrg",
        "ckpt_key":   "biogen_april.allergies",
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
        "label":      "vitals",
        "source":     "rgd_udm_silver.vitals",
        "target":     "biogen_april.vitals",
        "date_col":   "enc_date_proxy",
        "staging_pk": "staging.biogen_pk_vitals",
        "ckpt_key":   "biogen_april.vitals",
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
        "label":      "labs",
        "source":     "rgd_udm_silver.labs",
        "target":     "biogen_april.labs",
        "date_col":   "enc_date_proxy",
        "staging_pk": "staging.biogen_pk_labs",
        "ckpt_key":   "biogen_april.labs",
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
        "label":      "radiology",
        "source":     "rgd_udm_silver.radiology",
        "target":     "biogen_april.radiology",
        "date_col":   "enc_date_proxy",
        "staging_pk": "staging.biogen_pk_rad",
        "ckpt_key":   "biogen_april.radiology",
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
        "label":      "ros",
        "source":     "rgd_udm_silver.ros",
        "target":     "biogen_april.ros",
        "date_col":   "enc_date_proxy",
        "staging_pk": "staging.biogen_pk_ros",
        "ckpt_key":   "biogen_april.ros",
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
        "label":      "examinations",
        "source":     "rgd_udm_silver.examination",
        "target":     "biogen_april.examinations",
        "date_col":   "enc_date_proxy",
        "staging_pk": "staging.biogen_pk_exam",
        "ckpt_key":   "biogen_april.examinations",
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
        "label":      "note_part1",
        "source":     "rgd_udm_silver.notes_part1",
        "target":     "biogen_april.note",
        "date_col":   "enc_start_date",
        "staging_pk": "staging.biogen_pk_note1",
        "ckpt_key":   "biogen_april.note_part1",
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
        "label":      "note_part2",
        "source":     "rgd_udm_silver.notes_part2",
        "target":     "biogen_april.note",
        "date_col":   "enc_start_date",
        "staging_pk": "staging.biogen_pk_note2",
        "ckpt_key":   "biogen_april.note_part2",
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
        "label":      "medications",
        "source":     "rgd_udm_silver.medications",
        "target":     "biogen_april.medications",
        "date_col":   "enc_start_date",
        "staging_pk": "staging.biogen_pk_meds",
        "ckpt_key":   "biogen_april.medications",
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
        "label":      "past_history",
        "source":     "rgd_udm_silver.past_history",
        "target":     "biogen_april.past_history",
        "date_col":   "visit_date",
        "staging_pk": "staging.biogen_pk_phist",
        "ckpt_key":   "biogen_april.past_history",
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
        "label":      "patient_demographics",
        "source":     "rgd_udm_silver.patient_demographics",
        "target":     "biogen_april.patient_demographics",
        "date_col":   None,
        "staging_pk": "staging.biogen_pk_pdemo",
        "ckpt_key":   "biogen_april.patient_demographics",
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
    {"target": "biogen_april.encounters",           "enc_col": "encounter_id"},
    {"target": "biogen_april.diagnosis",            "enc_col": "encounter_id"},
    {"target": "biogen_april.procedures",           "enc_col": "encounter_id"},
    {"target": "biogen_april.allergies",            "enc_col": "encounter_id"},
    {"target": "biogen_april.vitals",               "enc_col": "encounter_id"},
    {"target": "biogen_april.labs",                 "enc_col": "encounter_id"},
    {"target": "biogen_april.radiology",            "enc_col": "encounter_id"},
    {"target": "biogen_april.ros",                  "enc_col": "encounter_id"},
    {"target": "biogen_april.examinations",         "enc_col": "encounter_id"},
    {"target": "biogen_april.note",                 "enc_col": "encounter_id"},
    {"target": "biogen_april.medications",          "enc_col": "encounter_id"},
    {"target": "biogen_april.past_history",         "enc_col": "encounter_id"},
    {"target": "biogen_april.patient_demographics", "enc_col": None},
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
    """Server-side boundary sampling — returns (ranges, total)."""
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


# ── Pre-flight check ─────────────────────────────────────────────────

def preflight(tbl: dict):
    """
    Returns (ranges, total) if this table is ready to insert.
    Returns ([], 0) with a printed reason if it should be skipped.
    """
    label      = tbl["label"]
    staging_pk = tbl["staging_pk"]
    target     = tbl["target"]
    ckpt_key   = tbl["ckpt_key"]

    conn = get_connection()
    cur  = conn.cursor()

    try:
        # Already done?
        if is_done(conn, ckpt_key):
            print(f"  [{label}]  checkpoint=done — skipping")
            return [], 0

        # Staging PK table must exist
        if not _table_exists(cur, staging_pk):
            print(f"  [{label}]  staging PK {staging_pk} missing — skipping "
                  f"(run biogen_subset.py setup first)")
            return [], 0

        # Target table must exist
        if not _table_exists(cur, target):
            print(f"  [{label}]  target {target} missing — skipping "
                  f"(run biogen_subset.py setup first)")
            return [], 0

        ranges, total = _build_ranges(cur, staging_pk)
        if total == 0:
            print(f"  [{label}]  staging PK is empty — skipping")
            return [], 0

        print(f"  [{label}]  {total:,} rows  →  {len(ranges)} batches  "
              f"(source: {tbl['source']})")
        return ranges, total

    finally:
        cur.close()
        conn.close()


# ── Runner ────────────────────────────────────────────────────────────

def run_source(tbl: dict, ranges: list, pbar) -> dict:
    """
    Batch INSERT for one source. Returns status dict.
    On exception: marks checkpoint failed, does NOT re-raise.
    """
    label       = tbl["label"]
    source      = tbl["source"]
    target      = tbl["target"]
    staging_pk  = tbl["staging_pk"]
    ckpt_key    = tbl["ckpt_key"]
    select_cols = tbl["select_cols"]
    date_col    = tbl["date_col"]

    # Build date filter for the INSERT WHERE clause
    if date_col is not None:
        date_filter = (
            f"  AND a.{date_col} >= '{DATE_LO}'\n"
            f"  AND a.{date_col} <= '{DATE_HI}'"
        )
    else:
        date_filter = ""

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

        for lo, hi in ranges:
            sql = f"""
INSERT INTO {target}
SELECT {select_cols}
FROM {source} a
INNER JOIN {staging_pk} pk ON a.{BATCH_KEY} = pk.{BATCH_KEY}
WHERE a.{BATCH_KEY} >= {lo}
  AND a.{BATCH_KEY} < {hi}
{date_filter}
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
    print(f"  {'Table':<44}  {'Rows':>12}  {'Distinct ndid':>15}  {'Distinct enc':>14}")
    print(f"  {'-'*44}  {'-'*12}  {'-'*15}  {'-'*14}")

    try:
        for entry in REPORT_TARGETS:
            target  = entry["target"]
            enc_col = entry["enc_col"]

            if not _table_exists(cur, target):
                print(f"  {target:<44}  {'(table missing)':>12}")
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
                f"  {target:<44}  {rows_n:>12,}  {ndids_n:>15,}  {enc_str:>14}"
            )
    finally:
        cur.close()
        conn.close()

    print(f"{'='*80}\n")


# ── Main ──────────────────────────────────────────────────────────────

def main():
    print(f"\n{'='*70}", flush=True)
    print(f"  Biogen April INSERT runner — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  checkpoint : {CKPT_TABLE}")
    print(f"  date range : {DATE_LO}  to  {DATE_HI}")
    print(f"  batch size : {BATCH_SIZE:,}")
    print(f"  NOTE: skips any table whose staging PK table does not yet exist")
    print(f"{'='*70}\n", flush=True)

    # ── Pre-flight: check each table, build ranges ────────────────────
    print("Pre-flight checks:")
    all_ranges    = {}
    total_batches = 0

    for tbl in TABLES:
        ranges, _ = preflight(tbl)
        all_ranges[tbl["label"]] = ranges
        total_batches += len(ranges)

    ready = sum(1 for r in all_ranges.values() if r)
    print(f"\n  {ready} / {len(TABLES)} sources ready  "
          f"({total_batches} total batches)\n")

    if total_batches == 0:
        print("  Nothing to insert. Exiting.")
        return

    # ── Insert phase ──────────────────────────────────────────────────
    results    = {}
    any_failed = False

    with tqdm(total=total_batches, desc="Overall", unit="batch") as pbar:
        for tbl in TABLES:
            label  = tbl["label"]
            ranges = all_ranges.get(label, [])

            if not ranges:
                continue

            result = run_source(tbl, ranges, pbar)
            results[label] = result

            if result["status"].startswith("FAILED"):
                any_failed = True

    # ── Per-source summary ────────────────────────────────────────────
    print(f"\n{'='*70}")
    print(f"  Per-source summary")
    print(f"{'='*70}")
    print(f"  {'Label':<24}  {'Status':<10}  {'Rows':>12}  {'Secs':>8}")
    print(f"  {'-'*24}  {'-'*10}  {'-'*12}  {'-'*8}")

    for tbl in TABLES:
        label = tbl["label"]
        if label not in results:
            ranges = all_ranges.get(label, [])
            tag = "SKIPPED" if not ranges else "—"
            print(f"  {label:<24}  {tag:<10}  {'—':>12}  {'—':>8}")
            continue
        r = results[label]
        if r["status"] == "done":
            tag = "DONE"
        elif r["status"] == "skipped":
            tag = "SKIPPED"
        else:
            tag = "FAILED"
        print(
            f"  {label:<24}  {tag:<10}  {r['rows']:>12,}  {r['secs']:>7}s"
        )

    print(f"{'='*70}\n")

    # ── Final report ──────────────────────────────────────────────────
    print_report()

    if any_failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
