#!/usr/bin/env python3
"""
Fill Rate Report Generator — rgd_udm_silver
============================================

For every column in every table defined in SOURCES, computes:
    schema_name | table_name | column_name | total_count | null_records | fill_rate_pct

A NULL/empty record is any row where the column IS NULL OR = ''.

Strategy (optimized):
  - One aggregate SELECT per table → single full scan, all columns at once
    (avoids N separate scans for N columns — massively cheaper on large tables)
  - Tables processed in parallel via ThreadPoolExecutor (MAX_WORKERS)
  - Checkpoint/resume: re-run skips tables already marked 'done'
  - Results upserted into staging.fill_rate_report (safe to re-run)

NOTE: Large tables will still require a full sequential scan. Approximate sizes:
    medications_part1 ~381M rows, vitals ~298M, notes_part2 ~177M,
    encounters ~240M, notes_part1 ~142M. Run during off-peak hours.

Usage:
    python fillrates.py
    python fillrates.py --workers 6        # increase parallelism
    python fillrates.py --reset            # drop checkpoint, recompute all tables
"""

import argparse
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import pymysql
from tqdm import tqdm

# ── Configuration ─────────────────────────────────────────────────────────────
DB_CONFIG = {
    "host":            "172.16.2.42",
    "port":            3306,
    "user":            "nd-root-mysql",
    "password":        "kmsamd89undsd4",
    "database":        "rgd_udm_silver",
    "charset":         "utf8mb4",
    "connect_timeout": 30,
    "read_timeout":    21600,   # 6 h — large aggregate scans can be slow
    "write_timeout":   21600,
}

MAX_WORKERS      = 4                                    # tables computed in parallel
SOURCE_SCHEMA    = "rgd_udm_silver"
REPORT_TABLE     = "staging.fill_rate_report_3"
CHECKPOINT_TABLE = "staging.etl_checkpoint_fillrates_v3"

# ── Source definitions ────────────────────────────────────────────────────────
# Each entry: table name + ordered list of columns (from DDL in rgd_fill_rates.sql)
SOURCES = [
    {
        "table": "patients",
        "columns": [
            "ndid", "clinicid", "registration_date", "dob", "gender",
            "pat_language", "pat_city", "pat_state", "pat_country", "pat_zip",
            "pat_race_code", "pat_race", "pat_ethnicity_code", "pat_ethnicity",
            "pat_marital_status", "pat_deceased_status", "deceased_date",
            "deceased_reason", "primary_provider_id", "ehr_active_flag",
            "pat_insurance", "pcp_name", "rendering_provider_id",
            "referring_provider_id", "disability_status", "residence_type",
            "created_datetime", "created_by", "ehr_source_name", "source_path",
            "data_type", "psid", "referral_flag", "referral_from", "test_pat_flag",
            "udm_inc_id", "nd_extracted_date", "gender_hl7_std", "gender_CDISC_std",
            "gender_OMOP_std", "gender_OMOP_concept_id", "pat_race_code_std",
            "pat_race_std", "pat_ethnicity_code_std", "pat_ethnicity_std",
            "pat_deceased_status_std", "enc_date_proxy", "pat_marital_status_std",
        ],
    },
    {
        "table": "encounters",
        "columns": [
            "eid", "ndid", "enc_date", "enc_end_date", "enc_start_time",
            "enc_end_time", "encounter_category", "enc_type", "enc_reason",
            "enc_appt_id", "enc_department", "doc_speciality", "enc_facility_id",
            "enc_location", "enc_status", "enc_document_flg", "enc_document_id",
            "pregnancy_flg", "inpatient_flg", "provider_id",
            "supervising_provider_id", "primary_provider_id", "referral_flg",
            "referring_provider_id", "referring_provider_name",
            "referring_provider_npi", "locked", "provider_name", "care_team",
            "payer", "referral_reason", "visit_type", "rendering_provider_name",
            "created_datetime", "created_by", "ehr_source_name", "source_path",
            "data_type", "psid", "nd_extracted_date", "udm_inc_id", "enc_date_proxy",
        ],
    },
    {
        "table": "diagnosis",
        "columns": [
            "diag_id", "ndid", "eid", "enc_date", "encounter_end_date",
            "diag_date", "diag_code", "diag_desc", "diag_coding_system",
            "diag_code_stripped", "primary_diagnosis_flag", "parent_diagnosis_code",
            "parent_diagnosis_desc", "icd_codeset", "icd_codeset_desc",
            "icd_codeset_group", "icd_codeset_system", "snomed_code",
            "diag_severity", "diag_status", "diag_end_date",
            "provisional_diag_flag", "differential_diag_flag", "comments_notes",
            "diag_risk", "specify", "nd_extracted_date", "created_datetime",
            "created_by", "ehr_source_name", "source_path", "data_type", "psid",
            "udm_inc_id", "enc_date_proxy", "icd10_desc_std", "icd9_desc_std",
            "diag_coding_system_std", "primary_diagnosis_flag_std", "diag_desc_std",
        ],
    },
    {
        "table": "procedures",
        "columns": [
            "proc_id", "ndid", "eid", "encounter_date", "proc_start_date",
            "proc_last_date", "proc_category", "proc_code", "proc_name",
            "proc_coding_system", "proc_units", "proc_description", "proc_notes",
            "anesthesia_flag", "anesthesia_detail_id", "ordering_provider_id",
            "ordering_provider_name", "ordering_provider_npi",
            "rendering_provider_id", "rendering_provider_name",
            "rendering_provider_npi", "referring_provider_id",
            "referring_provider_name", "referring_provider_npi",
            "place_of_service_Id", "place_of_service_desc", "order_date",
            "Diagnosis_Indication", "nd_extracted_date", "created_datetime",
            "created_by", "ehr_source_name", "source_path", "data_type", "psid",
            "incremental_id", "udm_inc_id", "proc_code_std",
            "proc_coding_system_std", "enc_date_proxy", "proc_modifier_std",
            "proc_name_std", "proc_description_std",
        ],
    },
    {
        "table": "medications_part1",
        "columns": [
            "source", "med_id", "ndid", "eid", "enc_date", "written_date",
            "med_administered_datetime", "doc_orderdatetime", "med_start_date",
            "med_end_date", "med_createddatetime", "doc_createddatetime",
            "last_dispensed_date", "sample_expiration_date",
            "administer_expiration_date", "earliest_fill_date", "med_code",
            "med_name", "med_coding_system", "med_status", "med_status_flag",
            "med_indication", "med_formulation", "med_route", "med_strength",
            "med_strength_unit", "med_frequency", "med_pb_qty", "med_days_supply",
            "med_refills", "med_directions", "fill_date", "med_fill_type",
            "discont_date", "discont_reason", "created_datetime", "created_by",
            "updated_datetime", "updated_by", "ehr_source_name", "source_path",
            "data_type", "psid", "nd_extracted_date", "udm_inc_id", "enc_date_proxy",
        ],
    },
    {
        "table": "medications_part2",
        "columns": [
            "source", "med_id", "ndid", "eid", "enc_date", "written_date",
            "med_administered_datetime", "doc_orderdatetime", "med_start_date",
            "med_end_date", "med_createddatetime", "doc_createddatetime",
            "last_dispensed_date", "sample_expiration_date",
            "administer_expiration_date", "earliest_fill_date", "med_code",
            "med_name", "med_coding_system", "med_status", "med_status_flag",
            "med_indication", "med_formulation", "med_route", "med_strength",
            "med_strength_unit", "med_frequency", "med_pb_qty", "med_days_supply",
            "med_refills", "med_directions", "fill_date", "med_fill_type",
            "discont_date", "discont_reason", "created_datetime", "created_by",
            "updated_datetime", "updated_by", "ehr_source_name", "source_path",
            "data_type", "psid", "nd_extracted_date", "udm_inc_id", "enc_date_proxy",
        ],
    },
    {
        "table": "ros",
        "columns": [
            "ndid", "eid", "enc_start_date", "ros_date", "ros_name",
            "ros_category", "ros_option", "ros_notes", "created_datetime",
            "created_by", "ehr_source_name", "source_path", "data_type", "psid",
            "ros_id", "ros_parameter", "nd_extracted_date", "udm_inc_id",
            "enc_date_proxy",
        ],
    },
    {
        "table": "examination",
        "columns": [
            "examid", "ndid", "eid", "enc_start_date", "exam_date",
            "exam_category", "exam_name", "exam_findings", "created_datetime",
            "created_by", "ehr_source_name", "source_path", "data_type", "psid",
            "finding_type", "exam_parameters", "nd_extracted_date", "udm_inc_id",
            "enc_date_proxy",
        ],
    },
    {
        "table": "notes_part1",
        "columns": [
            "ndid", "eid", "enc_start_date", "note", "note_type", "note_source",
            "created_datetime", "created_by", "ehr_source_name", "source_path",
            "data_type", "psid", "nd_extracted_date", "udm_inc_id", "enc_date_proxy",
        ],
    },
    {
        "table": "notes_part2",
        "columns": [
            "ndid", "eid", "enc_start_date", "note", "note_type", "note_source",
            "created_datetime", "created_by", "ehr_source_name", "source_path",
            "data_type", "psid", "nd_extracted_date", "enc_date_proxy", "udm_inc_id",
        ],
    },
    {
        "table": "vitals",
        "columns": [
            "vital_id", "ndid", "eid", "vital_code", "vital_name",
            "vital_coding_system", "vital_date", "vital_time", "vital_unit",
            "vital_range", "vital_result", "created_datetime", "created_by",
            "updated_datetime", "updated_by", "ehr_source_name", "source_path",
            "data_type", "psid", "nd_extracted_date", "udm_inc_id", "enc_date_proxy",
        ],
    },
    {
        "table": "labs",
        "columns": [
            "result_id", "ndid", "eid", "enc_date", "lab_order_date",
            "sample_collection_date", "result_date", "result_name",
            "result_parameter", "result_code", "result_coding_system",
            "result_value", "result_unit", "result_range", "result_value_type",
            "ordering_provider_name", "order_id", "test_performing_lab_id",
            "test_performing_lab_name", "normalcy_flag", "result_status",
            "report_id", "report_status", "report_description", "specimen_source",
            "specimen_number", "internal_note", "note_to_patient", "facility",
            "interpretation", "received", "highpriority", "cancelled",
            "futureorder", "inhouse", "ordered_fasting",
            "do_not_publish_to_patient", "interfacestatus", "created_datetime",
            "created_by", "ehr_source_name", "source_path", "data_type", "psid",
            "nd_extracted_date", "Result_Document_ID", "Report_date",
            "udm_inc_id", "enc_date_proxy",
        ],
    },
    {
        "table": "radiology",
        "columns": [
            "result_id", "ndid", "eid", "enc_date", "img_date", "study_name",
            "modality", "img_status", "img_report_text", "img_finding",
            "report_id", "report_date", "report_status", "img_reason",
            "order_date", "order_status", "order_prescription", "provider_id",
            "provider_name", "provider_npi", "internal_notes", "note_to_patient",
            "facility", "interpretation", "result", "report_text",
            "created_datetime", "created_by", "ehr_source_name", "source_path",
            "data_type", "psid", "nd_extracted_date", "udm_inc_id", "enc_date_proxy",
        ],
    },
]


# ── Helpers ───────────────────────────────────────────────────────────────────

def get_connection():
    return pymysql.connect(**DB_CONFIG)


# ── Checkpoint ────────────────────────────────────────────────────────────────

def is_done(conn, source_key):
    cur = conn.cursor()
    cur.execute(
        f"SELECT status FROM {CHECKPOINT_TABLE} WHERE source_key = %s",
        (source_key,),
    )
    row = cur.fetchone()
    cur.close()
    return row is not None and row[0] == "done"


def mark(conn, source_key, status, error=None):
    cur = conn.cursor()
    cur.execute(f"""
        INSERT INTO {CHECKPOINT_TABLE}
            (source_key, status, started_at, completed_at, error_msg)
        VALUES (%s, %s, NOW(), IF(%s = 'done', NOW(), NULL), %s)
        ON DUPLICATE KEY UPDATE
            status       = VALUES(status),
            completed_at = IF(VALUES(status) = 'done', NOW(), NULL),
            error_msg    = VALUES(error_msg)
    """, (source_key, status, status, error))
    conn.commit()
    cur.close()


# ── Setup ─────────────────────────────────────────────────────────────────────

def setup_tables(reset=False):
    conn = get_connection()
    cur  = conn.cursor()

    if reset:
        print("  [reset] Dropping checkpoint table to force full recompute...")
        cur.execute(f"DROP TABLE IF EXISTS {CHECKPOINT_TABLE}")
        conn.commit()

    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {REPORT_TABLE} (
            id            BIGINT AUTO_INCREMENT PRIMARY KEY,
            schema_name   VARCHAR(100)   NOT NULL,
            table_name    VARCHAR(100)   NOT NULL,
            column_name   VARCHAR(200)   NOT NULL,
            total_count   BIGINT         DEFAULT NULL,
            null_records  BIGINT         DEFAULT NULL,
            fill_rate_pct DECIMAL(6, 2)  DEFAULT NULL,
            computed_at   DATETIME       DEFAULT NULL,
            UNIQUE KEY uk_col (schema_name, table_name, column_name)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
    """)

    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {CHECKPOINT_TABLE} (
            source_key   VARCHAR(200) NOT NULL PRIMARY KEY,
            status       ENUM('running', 'done', 'failed') NOT NULL DEFAULT 'running',
            started_at   DATETIME DEFAULT NULL,
            completed_at DATETIME DEFAULT NULL,
            error_msg    TEXT     DEFAULT NULL
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
    """)

    conn.commit()
    cur.close()
    conn.close()


# ── Query builder ─────────────────────────────────────────────────────────────

# String types — empty string '' is a valid "empty" value to flag
_STRING_TYPES = {"varchar", "char", "tinytext", "text", "mediumtext", "longtext",
                 "enum", "set", "json"}


def get_column_types(cur, table):
    """Returns {column_name: data_type} from information_schema."""
    cur.execute("""
        SELECT COLUMN_NAME, DATA_TYPE
        FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
        ORDER BY ORDINAL_POSITION
    """, (SOURCE_SCHEMA, table))
    return {row[0]: row[1].lower() for row in cur.fetchall()}


def get_approx_row_count(cur, table):
    """
    Fast approximate row count from information_schema.TABLES (no scan).
    InnoDB estimates — accurate within ~5-20% for large tables.
    Returns None if statistics unavailable.
    """
    cur.execute("""
        SELECT TABLE_ROWS
        FROM information_schema.TABLES
        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
    """, (SOURCE_SCHEMA, table))
    row = cur.fetchone()
    return row[0] if row else None


def build_null_query(table, columns):
    """
    Phase 1 — COUNT(col) scan: one full table scan, all columns at once.
    COUNT(col) natively skips NULLs — no CASE WHEN, no type issues, faster.
    Returns: total_count, then filled_count per column (NOT null count).
    null_count = total_count - COUNT(col)
    """
    col_exprs = ",\n    ".join(
        f"COUNT(`{col}`) AS `{col}`"
        for col in columns
    )
    return f"""
SELECT
    COUNT(*) AS __total__,
    {col_exprs}
FROM `{SOURCE_SCHEMA}`.`{table}`
"""


def build_empty_string_query(table, string_cols):
    """
    Phase 2 — empty string scan: runs ONLY on string-typed columns.
    Separate lightweight pass so DATE/numeric cols are never compared to ''.
    Also catches 'None' stored as string.
    """
    col_exprs = ",\n    ".join(
        f"SUM(`{col}` IN ('', 'None')) AS `{col}`"
        for col in string_cols
    )
    return f"""
SELECT
    {col_exprs}
FROM `{SOURCE_SCHEMA}`.`{table}`
"""


# ── Worker ────────────────────────────────────────────────────────────────────

def run_source(source, pbar):
    """Compute fill rates for one table, write results, update checkpoint."""
    table   = source["table"]
    columns = source["columns"]
    key     = f"{SOURCE_SCHEMA}.{table}"

    conn = get_connection()

    if is_done(conn, key):
        conn.close()
        pbar.update(1)
        return {"table": table, "status": "skipped", "cols": len(columns), "secs": 0}

    mark(conn, key, "running")
    t0 = time.time()

    try:
        cur = conn.cursor()

        # ── Column types (instant — no scan) ─────────────────────────
        col_types   = get_column_types(cur, table)
        string_cols = [c for c in columns if col_types.get(c, "varchar") in _STRING_TYPES]

        # ── Approximate row count (instant — no scan) ─────────────────
        approx_rows = get_approx_row_count(cur, table)
        if approx_rows:
            print(f"\n  [{table}] ~{approx_rows:,} rows, {len(string_cols)} string cols",
                  flush=True)

        # ── Phase 1: COUNT(col) — one full scan, catches NULLs ───────
        sql_null = build_null_query(table, columns)
        cur.execute(sql_null)
        row1 = cur.fetchone()

        total_count  = int(row1[0])
        filled_count = {col: int(ct or 0) for col, ct in zip(columns, row1[1:])}

        # null_count[col] = total - COUNT(col)  [NULLs only at this point]
        null_counts = {col: total_count - filled_count[col] for col in columns}

        # ── Phase 2: empty string scan — string cols only ─────────────
        empty_counts = {col: 0 for col in columns}
        if string_cols:
            sql_empty = build_empty_string_query(table, string_cols)
            cur.execute(sql_empty)
            row2 = cur.fetchone()
            for col, ec in zip(string_cols, row2):
                empty_counts[col] = int(ec or 0)

        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # ── Upsert + commit per column — results land in DB immediately
        # after the scan. A crash mid-write keeps everything already saved.
        for col in columns:
            empty_total = null_counts[col] + empty_counts[col]
            fill_rate   = (
                round(100.0 * (total_count - empty_total) / total_count, 2)
                if total_count else 0.0
            )
            cur.execute(f"""
                INSERT INTO {REPORT_TABLE}
                    (schema_name, table_name, column_name,
                     total_count, null_records, fill_rate_pct, computed_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    total_count   = VALUES(total_count),
                    null_records  = VALUES(null_records),
                    fill_rate_pct = VALUES(fill_rate_pct),
                    computed_at   = VALUES(computed_at)
            """, (SOURCE_SCHEMA, table, col, total_count, empty_total, fill_rate, now))
            conn.commit()   # ← persisted immediately, visible in DB right away

        cur.close()

        elapsed = round(time.time() - t0, 1)
        mark(conn, key, "done")
        conn.close()
        pbar.update(1)
        return {"table": table, "status": "done", "cols": len(columns), "secs": elapsed}

    except Exception as exc:
        elapsed = round(time.time() - t0, 1)
        mark(conn, key, "failed", str(exc))
        try:
            conn.close()
        except Exception:
            pass
        pbar.update(1)
        return {"table": table, "status": f"FAILED: {exc}", "cols": 0, "secs": elapsed}


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Fill rate report — rgd_udm_silver")
    parser.add_argument("--workers", type=int, default=MAX_WORKERS,
                        help=f"Parallel workers (default: {MAX_WORKERS})")
    parser.add_argument("--reset", action="store_true",
                        help="Drop checkpoint table and recompute all tables from scratch")
    args = parser.parse_args()

    total_cols = sum(len(s["columns"]) for s in SOURCES)

    print(f"\n{'='*70}")
    print(f"  Fill Rate Report — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  schema     : {SOURCE_SCHEMA}")
    print(f"  tables     : {len(SOURCES)}")
    print(f"  columns    : {total_cols}")
    print(f"  report     : {REPORT_TABLE}")
    print(f"  checkpoint : {CHECKPOINT_TABLE}")
    print(f"  workers    : {args.workers}")
    if args.reset:
        print(f"  mode       : RESET — all tables will be recomputed")
    print(f"{'='*70}\n")

    print("Setting up report and checkpoint tables...")
    setup_tables(reset=args.reset)
    print("  ready\n")

    results = []
    with tqdm(total=len(SOURCES), desc="Tables", unit="table") as pbar:
        with ThreadPoolExecutor(max_workers=args.workers) as pool:
            futures = {
                pool.submit(run_source, src, pbar): src
                for src in SOURCES
            }
            for future in as_completed(futures):
                results.append(future.result())

    # ── Summary ───────────────────────────────────────────────────────
    print()
    print(f"{'─'*70}")
    print(f"  {'TABLE':<28} {'STATUS':<8} {'COLS':>5} {'TIME':>8}")
    print(f"{'─'*70}")
    for r in sorted(results, key=lambda x: x["table"]):
        tag = "DONE" if r["status"] == "done" \
              else "SKIP" if r["status"] == "skipped" \
              else "FAIL"
        print(f"  [{tag}] {r['table']:<26} {r['cols']:>5} cols  {r['secs']:>6}s")

    done    = sum(1 for r in results if r["status"] == "done")
    skipped = sum(1 for r in results if r["status"] == "skipped")
    failed  = [r for r in results if "FAILED" in r["status"]]

    print(f"{'='*70}")
    print(f"  Done: {done}  Skipped: {skipped}  Failed: {len(failed)}")
    print(f"\n  Results written to: {REPORT_TABLE}")
    print(f"\n  Preview:")
    print(f"    SELECT schema_name, table_name, column_name,")
    print(f"           total_count, null_records, fill_rate_pct")
    print(f"    FROM {REPORT_TABLE}")
    print(f"    ORDER BY table_name, fill_rate_pct ASC;")
    print(f"\n  Low fill-rate columns (< 50%):")
    print(f"    SELECT schema_name, table_name, column_name, fill_rate_pct")
    print(f"    FROM {REPORT_TABLE}")
    print(f"    WHERE fill_rate_pct < 50")
    print(f"    ORDER BY fill_rate_pct ASC;")
    print(f"\n  Cleanup SQL (when done):")
    print(f"    DROP TABLE IF EXISTS {REPORT_TABLE};")
    print(f"    DROP TABLE IF EXISTS {CHECKPOINT_TABLE};")
    print(f"{'='*70}\n")

    if failed:
        print("  Failed tables:")
        for r in failed:
            print(f"    {r['table']}: {r['status']}")
        sys.exit(1)


if __name__ == "__main__":
    main()
