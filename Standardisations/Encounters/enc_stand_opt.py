#!/usr/bin/env python3
"""
Optimized batched standardisation UPDATEs for: encounters

Change TARGET_TABLE at the top to run against any encounters table.

Six sequential passes — each with checkpoint/resume:

  Pass 1 — WHERE enc_type_std IS NULL AND enc_type IS NOT NULL:
    SET enc_type_std from REGEXP CASE on enc_type — no JOIN

  Pass 2 — WHERE enc_sub_type_std IS NULL AND enc_sub_type IS NOT NULL:
    SET enc_sub_type_std from REGEXP_REPLACE chain on enc_sub_type — no JOIN

  Pass 3 — WHERE enc_type_std IS NULL AND enc_sub_type_std IS NOT NULL:
    SET enc_type_std fallback from LIKE CASE on enc_sub_type_std — no JOIN
    PK staging rebuilt AFTER Pass 2 (enc_sub_type_std populated then)

  Pass 4 — WHERE enc_type_std IS NOT NULL AND enc_category_std IS NULL:
    SET enc_category_std from CASE on enc_type_std — no JOIN
    PK staging rebuilt AFTER Passes 1+3 (enc_type_std fully populated then)

  Pass 5 — All rows:
    SET enc_status_std via visitstscodes JOIN + CASE for known codes
    JOIN pre-materialized STAGING_VISITSTSCODES (UNION ALL of 6 ECW schemas)

  Pass 6 — WHERE enc_status_std IS NOT NULL:
    SET normalized_category from REGEXP CASE on enc_status_std — no JOIN
    PK staging rebuilt AFTER Pass 5 (enc_status_std populated then)

Dependency chain:
  enc_type      → Pass 1 → enc_type_std
  enc_sub_type  → Pass 2 → enc_sub_type_std → Pass 3 → enc_type_std (fallback)
                                               Pass 4 (after 1+3) → enc_category_std
  enc_status    → Pass 5 → enc_status_std   → Pass 6 → normalized_category

Pre-materialized lookup tables (computed ONCE, reused across runs):
  - staging.enc_std_visitstscodes  (6 ECW schemas' visitstscodes — shared across runs)

Std columns added to target table if not present (with metadata lock guard).

Optimizations applied:
- visitstscodes lookup pre-materialized once with composite index on (prefix, code)
- Per-pass PK staging tables (filtered to eligible rows only)
- Deferred PK staging for passes 3, 4, 6 (rebuilt after upstream passes complete)
- Server-side boundary sampling (avoids loading all PKs into memory)
- Commit after every batch (frees undo/log space)
- Checkpoint/resume per pass — re-run skips completed passes
- Disabled InnoDB checks per-session for bulk update speed
- Progress bar via tqdm

Usage:
    python enc_stand_opt.py
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

# ── Change this to run against a different encounters table ───────────
TARGET_TABLE = "kinsula_leq.encounters"

# ─────────────────────────────────────────────────────────────────────
_TABLE_SUFFIX = TARGET_TABLE.replace(".", "_").replace("-", "_")

STAGING_VISITSTSCODES = "staging.enc_std_visitstscodes1"       # shared across runs
STAGING_PK_PASS1      = f"staging.enc_std_pk1n_{_TABLE_SUFFIX}"  # enc_type (upfront)
STAGING_PK_PASS2      = f"staging.enc_std_pk2n_{_TABLE_SUFFIX}"  # enc_sub_type (upfront)
STAGING_PK_PASS3      = f"staging.enc_std_pk3n_{_TABLE_SUFFIX}"  # fallback type (deferred after pass 2)
STAGING_PK_PASS4      = f"staging.enc_std_pk4n_{_TABLE_SUFFIX}"  # enc_category (deferred after pass 1+3)
STAGING_PK_PASS5      = f"staging.enc_std_pk5n_{_TABLE_SUFFIX}"  # enc_status (upfront, all rows)
STAGING_PK_PASS6      = f"staging.enc_std_pk6n_{_TABLE_SUFFIX}"  # normalized_cat (deferred after pass 5)
CHECKPOINT_TABLE      = f"staging.etl_checkpoint_enc_stdn_{_TABLE_SUFFIX}"
CHECKPOINT_PASS1      = f"enc.std.pass1.enc_type_stdn.{_TABLE_SUFFIX}"
CHECKPOINT_PASS2      = f"enc.std.pass2.enc_sub_type_stdn.{_TABLE_SUFFIX}"
CHECKPOINT_PASS3      = f"enc.std.pass3.enc_type_std_fallbackn.{_TABLE_SUFFIX}"
CHECKPOINT_PASS4      = f"enc.std.pass4.enc_category_stdn.{_TABLE_SUFFIX}"
CHECKPOINT_PASS5      = f"enc.std.pass5.enc_status_stdn.{_TABLE_SUFFIX}"
CHECKPOINT_PASS6      = f"enc.std.pass6.normalized_categoryn.{_TABLE_SUFFIX}"

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
    """Pass 1: enc_type_std from REGEXP CASE on enc_type (Update 2 logic)."""
    return f"""
UPDATE {TARGET_TABLE}
SET enc_type_std = CASE

    WHEN LOWER(enc_type) REGEXP 'tele|virtual'
        THEN 'Telehealth Visit'

    WHEN LOWER(enc_type) REGEXP 'new'
        THEN 'New Patient Visit'

    WHEN LOWER(enc_type) REGEXP 'office visit|established|multi-visit|out patient'
        THEN 'Office Visit'

    WHEN LOWER(enc_type) REGEXP 'hospital|in patient'
        THEN 'Hospital Visit'

    WHEN LOWER(enc_type) REGEXP 'emergency'
        THEN 'Emergency Visit'

    WHEN LOWER(enc_type) REGEXP 'eeg'
        THEN 'EEG'

    WHEN LOWER(enc_type) REGEXP 'emg|electromyography'
        THEN 'EMG'

    WHEN LOWER(enc_type) REGEXP 'sleep|psg|mslt'
        THEN 'Sleep Study'

    WHEN LOWER(enc_type) REGEXP 'mri|mra|mrv'
        THEN 'MRI / Neuro Imaging'

    WHEN LOWER(enc_type) REGEXP 'xray|ultrasound'
        THEN 'Radiology'

    WHEN LOWER(enc_type) REGEXP 'nerve conduction'
        THEN 'Nerve Conduction Study'

    WHEN LOWER(enc_type) REGEXP 'procedure|lumbar puncture|biopsy'
        THEN 'Procedure'

    WHEN LOWER(enc_type) REGEXP 'injection|botox'
        THEN 'Injection / Botox'

    WHEN LOWER(enc_type) REGEXP 'infusion'
        THEN 'Infusion'

    WHEN LOWER(enc_type) REGEXP 'follow|f/u|fu'
        THEN 'Follow Up Visit'

    WHEN LOWER(enc_type) REGEXP 'ref'
        THEN 'Referral'

    WHEN LOWER(enc_type) REGEXP 'consult'
        THEN 'Consultation'

    WHEN LOWER(enc_type) REGEXP 'research'
        THEN 'Research'

    WHEN LOWER(enc_type) REGEXP 'testing|study'
        THEN 'Diagnostic Testing'

    WHEN LOWER(enc_type) REGEXP 'vng|videonystagmogram|balance'
        THEN 'Balance / Vestibular Testing'

    WHEN LOWER(enc_type) REGEXP 'hearing'
        THEN 'Hearing Evaluation'

    WHEN LOWER(enc_type) REGEXP 'admin|meeting|legal'
        THEN 'Administrative'

    WHEN LOWER(enc_type) REGEXP 'observation'
        THEN 'Observation'

    WHEN LOWER(enc_type) REGEXP 'no show'
        THEN 'No Show'

    WHEN LOWER(enc_type) REGEXP 'void'
        THEN 'Voided Visit'

    WHEN LOWER(enc_type) REGEXP 'work in'
        THEN 'Work In Visit'

    ELSE 'Other'

END
WHERE enc_type_std IS NULL
  AND enc_type IS NOT NULL
  AND TRIM(enc_type) != ''
  AND {BATCH_KEY} >= {pk_lo}
  AND {BATCH_KEY} <  {pk_hi}
"""


def build_pass2(pk_lo, pk_hi):
    """Pass 2: enc_sub_type_std from REGEXP_REPLACE chain on enc_sub_type (Update 3 logic)."""
    return f"""
UPDATE {TARGET_TABLE}
SET enc_sub_type_std = TRIM(
    REGEXP_REPLACE(
    REGEXP_REPLACE(
    REGEXP_REPLACE(
    REGEXP_REPLACE(
    REGEXP_REPLACE(
    REGEXP_REPLACE(
    REGEXP_REPLACE(
        enc_sub_type,
        '[0-9]+/[0-9]+', ''),
        '[0-9]+\\\\s*(?i)or\\\\s*[0-9]+\\\\s*(hr|hrs|hour|hours)', ''),
        '[0-9]+\\\\s*(?i)(hr|hrs|hour|hours|min|mins|minutes)', ''),
        '[0-9]+\\\\s*(?i)(day|days|week|weeks|month|months|year|years)', ''),
        '(^[0-9+]+)', ''),
        '-\\\\s*[0-9]+', ''),
        '[-/,]', ' ')
    )
WHERE enc_sub_type_std IS NULL
  AND enc_sub_type IS NOT NULL
  AND TRIM(enc_sub_type) != ''
  AND {BATCH_KEY} >= {pk_lo}
  AND {BATCH_KEY} <  {pk_hi}
"""


def build_pass3(pk_lo, pk_hi):
    """Pass 3: enc_type_std fallback from LIKE CASE on enc_sub_type_std (Update 5 logic)."""
    return f"""
UPDATE {TARGET_TABLE}
SET enc_type_std = CASE

    WHEN enc_sub_type_std LIKE '%CONSULT%'
         THEN 'Consultation'

    WHEN enc_sub_type_std LIKE '%OFFICE%'
      OR enc_sub_type_std LIKE '%OV%'
         THEN 'Office Visit'

    WHEN enc_sub_type_std LIKE '%NEW PATIENT%'
      OR enc_sub_type_std LIKE '%NP%'
         THEN 'New Patient Visit'

    WHEN enc_sub_type_std LIKE '%FOLLOW%'
      OR enc_sub_type_std LIKE '%FU%'
      OR enc_sub_type_std LIKE '%RECHECK%'
         THEN 'Follow Up Visit'

    WHEN enc_sub_type_std LIKE '%WORK IN%'
         THEN 'Work In Visit'

    WHEN enc_sub_type_std LIKE '%TELE%'
      OR enc_sub_type_std LIKE '%WEB%'
      OR enc_sub_type_std LIKE '%VIDEO%'
      OR enc_sub_type_std LIKE '%PHONE%'
         THEN 'Telehealth Visit'

    WHEN enc_sub_type_std LIKE '%VIRTUAL%'
         THEN 'Virtual / Telehealth'

    WHEN enc_sub_type_std LIKE '%PROCEDURE%'
      OR enc_sub_type_std LIKE '%BIOPSY%'
      OR enc_sub_type_std LIKE '%PUNCTURE%'
         THEN 'Procedure'

    WHEN enc_sub_type_std LIKE '%INJECTION%'
      OR enc_sub_type_std LIKE '%BOTOX%'
      OR enc_sub_type_std LIKE '%DYSPORT%'
      OR enc_sub_type_std LIKE '%DAXXIFY%'
         THEN 'Injection / Botox'

    WHEN enc_sub_type_std LIKE '%INFUSION%'
      OR enc_sub_type_std LIKE 'IV %'
         THEN 'Infusion'

    WHEN enc_sub_type_std LIKE '%SPECIALTY DRUG%'
         THEN 'Infusion / Specialty Drugs'

    WHEN enc_sub_type_std LIKE '%PSYCH%'
      OR enc_sub_type_std LIKE '%BEHAVIORAL%'
         THEN 'Psychology / Behavioral Health'

    WHEN enc_sub_type_std LIKE '%SLEEP%'
      OR enc_sub_type_std LIKE '%PSG%'
      OR enc_sub_type_std LIKE '%CPAP%'
         THEN 'Sleep Study'

    WHEN enc_sub_type_std LIKE '%EMG%'
     AND enc_sub_type_std NOT LIKE '%NCS%'
         THEN 'EMG'

    WHEN enc_sub_type_std LIKE '%NCS%'
      OR enc_sub_type_std LIKE '%NERVE CONDUCTION%'
         THEN 'Nerve Conduction Study'

    WHEN enc_sub_type_std LIKE '%EMG NCS%'
         THEN 'EMG / Nerve Conduction'

    WHEN enc_sub_type_std LIKE '%EEG%'
         THEN 'EEG'

    WHEN enc_sub_type_std LIKE '%MRI%'
      OR enc_sub_type_std LIKE '%MRA%'
      OR enc_sub_type_std LIKE '%MRV%'
         THEN 'MRI / Neuro Imaging'

    WHEN enc_sub_type_std LIKE '%XRAY%'
      OR enc_sub_type_std LIKE '%CT%'
      OR enc_sub_type_std LIKE '%ULTRASOUND%'
      OR enc_sub_type_std LIKE '%DOPPLER%'
         THEN 'Radiology'

    WHEN enc_sub_type_std LIKE '%LAB%'
      OR enc_sub_type_std LIKE '%BLOOD%'
      OR enc_sub_type_std LIKE '%URINE%'
      OR enc_sub_type_std LIKE '%PLATELET%'
         THEN 'Lab'

    WHEN enc_sub_type_std LIKE '%TEST%'
      OR enc_sub_type_std LIKE '%EVAL%'
         THEN 'Diagnostic Testing'

    WHEN enc_sub_type_std LIKE '%HEARING%'
      OR enc_sub_type_std LIKE '%AUDIO%'
         THEN 'Hearing Evaluation'

    WHEN enc_sub_type_std LIKE '%VNG%'
      OR enc_sub_type_std LIKE '%BALANCE%'
      OR enc_sub_type_std LIKE '%VESTIB%'
         THEN 'Balance / Vestibular Testing'

    WHEN enc_sub_type_std LIKE '%THERAPY%'
      OR enc_sub_type_std LIKE '%PT%'
      OR enc_sub_type_std LIKE '%OT%'
         THEN 'Physical / Occupational Therapy'

    WHEN enc_sub_type_std LIKE '%ADMIN%'
      OR enc_sub_type_std LIKE '%MEETING%'
      OR enc_sub_type_std LIKE '%RENTAL%'
         THEN 'Administrative'

    WHEN enc_sub_type_std LIKE '%RESEARCH%'
      OR enc_sub_type_std LIKE '%STUDY%'
         THEN 'Research'

    WHEN enc_sub_type_std LIKE '%NO SHOW%'
         THEN 'No Show'

    WHEN enc_sub_type_std LIKE '%OBSERVE%'
      OR enc_sub_type_std LIKE '%OBSERVATION%'
         THEN 'Observation'

    WHEN enc_sub_type_std LIKE '%ER%'
      OR enc_sub_type_std LIKE '%EMERGENCY%'
         THEN 'Emergency Visit'

    WHEN enc_sub_type_std LIKE '%HOSPITAL%'
      OR enc_sub_type_std LIKE '%INPATIENT%'
         THEN 'Hospital Visit'

    WHEN enc_sub_type_std LIKE '%REFERRAL%'
         THEN 'Referral'

    ELSE 'Other'

END
WHERE enc_type_std IS NULL
  AND enc_sub_type_std IS NOT NULL
  AND {BATCH_KEY} >= {pk_lo}
  AND {BATCH_KEY} <  {pk_hi}
"""


def build_pass4(pk_lo, pk_hi):
    """Pass 4: enc_category_std from CASE on enc_type_std (Update 4 logic)."""
    return f"""
UPDATE {TARGET_TABLE}
SET enc_category_std =
    CASE
        WHEN enc_type_std = 'Office Visit'                   THEN 'Office Visit'
        WHEN enc_type_std = 'New Patient Visit'              THEN 'Office Visit'
        WHEN enc_type_std = 'Follow Up Visit'                THEN 'Office Visit'
        WHEN enc_type_std = 'Work In Visit'                  THEN 'Office Visit'
        WHEN enc_type_std = 'Consultation'                   THEN 'Office Visit'
        WHEN enc_type_std = 'Referral'                       THEN 'Office Visit'
        WHEN enc_type_std = 'Psychology / Behavioral Health' THEN 'Office Visit'
        WHEN enc_type_std = 'Physical / Occupational Therapy' THEN 'Office Visit'
        WHEN enc_type_std = 'Telehealth Visit'               THEN 'Virtual Visit'
        WHEN enc_type_std = 'Virtual / Telehealth'           THEN 'Virtual Visit'
        WHEN enc_type_std = 'Infusion'                       THEN 'Infusion'
        WHEN enc_type_std = 'Infusion / Specialty Drugs'     THEN 'Infusion'
        WHEN enc_type_std IN ('EMG', 'EEG',
             'MRI / Neuro Imaging', 'Nerve Conduction Study',
             'EMG / Nerve Conduction', 'Balance / Vestibular Testing',
             'Hearing Evaluation', 'Radiology')              THEN 'Radiology'
        WHEN enc_type_std = 'Hospital Visit'                 THEN 'Hospital Visit'
        WHEN enc_type_std = 'Diagnostic Testing'             THEN 'Testing'
        WHEN enc_type_std = 'Sleep Study'                    THEN 'Testing'
        WHEN enc_type_std = 'Administrative'                 THEN 'Administrative / Non-Clinical'
        WHEN enc_type_std = 'Injection / Botox'              THEN 'Injection'
        WHEN enc_type_std = 'Lab'                            THEN 'Lab'
        WHEN enc_type_std = 'Procedure'                      THEN 'Procedures'
        WHEN enc_type_std = 'Voided Visit'                   THEN 'Other'
        WHEN enc_type_std = 'Observation'                    THEN 'Other'
        WHEN enc_type_std = 'No Show'                        THEN 'Other'
        WHEN enc_type_std = 'Emergency Visit'                THEN 'Other'
        WHEN enc_type_std = 'Research'                       THEN 'Other'
        ELSE 'Other'
    END
WHERE enc_type_std IS NOT NULL
  AND enc_category_std IS NULL
  AND {BATCH_KEY} >= {pk_lo}
  AND {BATCH_KEY} <  {pk_hi}
"""


def build_pass5(pk_lo, pk_hi):
    """Pass 5: enc_status_std via visitstscodes JOIN + CASE (Update 6a logic)."""
    return f"""
UPDATE {TARGET_TABLE} e
LEFT JOIN {STAGING_VISITSTSCODES} cc
    ON  LEFT(e.ndid, 8)     = cc.prefix
    AND TRIM(e.enc_status)  = TRIM(cc.code)
SET e.enc_status_std =
    CASE
        WHEN cc.status IS NOT NULL          THEN cc.status
        WHEN e.enc_status IS NULL
          OR e.enc_status = ''              THEN 'Unknown'
        WHEN e.enc_status IN ('0','1','2')  THEN 'Null'
        WHEN e.enc_status = '["CANCSMS"]'   THEN 'SMS Cancel'
        WHEN e.enc_status = '["CONFSMS"]'   THEN 'SMS Confirmed'
        WHEN e.enc_status = 'ACK'           THEN 'Acknowledged'
        WHEN e.enc_status = 'BUMP'          THEN 'BUMPED'
        WHEN e.enc_status = 'CAN'           THEN 'Cancelled'
        WHEN e.enc_status = 'CLOSED'        THEN 'Closed'
        WHEN e.enc_status = 'COM'           THEN 'Completed'
        WHEN e.enc_status = 'CONF'          THEN 'Confirmed'
        WHEN e.enc_status = 'DELETED'       THEN 'Deleted'
        WHEN e.enc_status = 'Kiosk'         THEN 'Kiosk'
        WHEN e.enc_status = 'N/S BILL'      THEN 'NO SHOW FEE'
        WHEN e.enc_status = 'NOS'           THEN 'No-Show'
        WHEN e.enc_status = 'NOSHOW'        THEN 'No-Show'
        WHEN e.enc_status = 'Open'          THEN 'Open'
        WHEN e.enc_status = 'PEND'          THEN 'Pending'
        WHEN e.enc_status = 'REQUIRESIGNATURE' THEN 'Signature Required'
        WHEN e.enc_status = 'REVIEW'        THEN 'Review Needed'
        WHEN e.enc_status = 'RSC'           THEN 'Rescheduled'
        WHEN e.enc_status = 'SCHED'         THEN 'Scheduled'
        WHEN e.enc_status = 'SCHEDULE'      THEN 'Scheduled'
        WHEN e.enc_status = 'TEMP'          THEN 'Temporary'
        WHEN e.enc_status = 'VMSGPEN'       THEN 'Voice Message'
        WHEN e.enc_status = 'WAIT'          THEN 'Waiting'
        ELSE e.enc_status
    END
WHERE e.{BATCH_KEY} >= {pk_lo}
  AND e.{BATCH_KEY} <  {pk_hi}
"""


def build_pass6(pk_lo, pk_hi):
    """Pass 6: normalized_category from REGEXP CASE on enc_status_std (Update 6b logic)."""
    return f"""
UPDATE {TARGET_TABLE}
SET normalized_category = CASE
    WHEN enc_status_std REGEXP 'Completed|Closed|Billed|Check Out|Check-Out|CHK|Transcription|READY FOR CHECK OUT'
         THEN 'Completed'
    WHEN enc_status_std REGEXP 'ROOMED|Exam|MA Ready|EEGReady|MRI Ready|Doctor|WITH MA|In-Progress'
         THEN 'In-Progress'
    WHEN enc_status_std REGEXP 'Arrived|Check-In|Check In|Check-in|Kiosk|QR|Reception|ARR|Acknowledged|WAIT'
         THEN 'Arrived'
    WHEN enc_status_std REGEXP 'LWOBS|Left without being seen|WalkOut|W/O Being Seen'
         THEN 'LWOBS'
    WHEN enc_status_std REGEXP 'No-Show|No Show|NOSHOW|NOS|NO SHOW FEE'
         THEN 'No-Show'
    WHEN enc_status_std REGEXP 'Cancel|Cx|Cxl|Deleted|Legal|DENT-no R/S'
         THEN 'Cancelled'
    WHEN enc_status_std REGEXP 'Confirm|Verification Complete|Talked to Pt'
         THEN 'Confirmed'
    WHEN enc_status_std REGEXP 'Scheduled|Rescheduled|Reschedule|RSC|R/S|Appt not needed|Open'
         THEN 'Scheduled'
    WHEN enc_status_std REGEXP 'Pending|Auth|Pen-|Precerted|Referral|Signature Required|Review Needed'
         THEN 'Pending'
    ELSE 'Admin / Other'
END
WHERE enc_status_std IS NOT NULL
  AND {BATCH_KEY} >= {pk_lo}
  AND {BATCH_KEY} <  {pk_hi}
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

def ensure_std_columns():
    std_cols = [
        ("enc_type_std",        "VARCHAR(100)"),
        ("enc_sub_type_std",    "VARCHAR(200)"),
        ("enc_category_std",    "VARCHAR(100)"),
        ("enc_status_std",      "VARCHAR(100)"),
        ("normalized_category", "VARCHAR(100)"),
    ]
    print(f"  Checking std columns on {TARGET_TABLE}...")
    ddl_conn  = get_connection()
    ddl_cur   = ddl_conn.cursor()
    ddl_cur.execute("SET lock_wait_timeout = 15")
    ddl_error = None
    added     = []
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

    _has_enc_type     = _col_exists(cur, TARGET_TABLE, "enc_type")
    _has_enc_sub_type = _col_exists(cur, TARGET_TABLE, "enc_sub_type")

    # ── 1. visitstscodes lookup (UNION ALL of 6 ECW schemas) ──────────
    print("  Materializing visitstscodes lookup (6 ECW schemas)...")
    if not _table_exists(cur, STAGING_VISITSTSCODES):
        cur.execute(f"""
            CREATE TABLE {STAGING_VISITSTSCODES} AS
            SELECT '10010001' AS prefix, code, status FROM dent.visitstscodes
            UNION ALL
            SELECT '10010003', code, status FROM texas.visitstscodes
            UNION ALL
            SELECT '10010004', code, status FROM northwest.visitstscodes
            UNION ALL
            SELECT '10010008', code, status FROM fcn_latest.visitstscodes
            UNION ALL
            SELECT '10010013', code, status FROM tncne.visitstscodes
            UNION ALL
            SELECT '10010014', code, status FROM arizona.visitstscodes
        """)
        cur.execute(
            f"ALTER TABLE {STAGING_VISITSTSCODES} "
            f"ADD INDEX idx_prefix_code (prefix, code)"
        )
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")
    cur.execute(f"SELECT COUNT(*) FROM {STAGING_VISITSTSCODES}")
    print(f"    {cur.fetchone()[0]:,} rows")

    # ── 2. PK staging — Pass 1: enc_type IS NOT NULL, enc_type_std NULL ──
    print("  Creating PK staging for Pass 1 (enc_type → enc_type_std)...")
    if not _has_enc_type:
        print("    enc_type column not found on target — Pass 1 will be skipped")
        ranges_p1, total_p1 = [], 0
    else:
        if not _table_exists(cur, STAGING_PK_PASS1):
            cur.execute(f"""
                CREATE TABLE {STAGING_PK_PASS1} AS
                SELECT {BATCH_KEY}
                FROM {TARGET_TABLE}
                WHERE enc_type_std IS NULL
                  AND enc_type IS NOT NULL
                  AND TRIM(enc_type) != ''
                  AND {BATCH_KEY} IS NOT NULL
            """)
            cur.execute(f"ALTER TABLE {STAGING_PK_PASS1} ADD INDEX idx_pk ({BATCH_KEY})")
            conn.commit()
            print("    created")
        else:
            print("    already exists, reusing")
        ranges_p1, total_p1 = _build_ranges(cur, STAGING_PK_PASS1)
        print(f"    {total_p1:,} rows → {len(ranges_p1)} batches")

    # ── 3. PK staging — Pass 2: enc_sub_type IS NOT NULL, enc_sub_type_std NULL ──
    print("  Creating PK staging for Pass 2 (enc_sub_type → enc_sub_type_std)...")
    if not _has_enc_sub_type:
        print("    enc_sub_type column not found on target — Pass 2 will be skipped")
        ranges_p2, total_p2 = [], 0
    else:
        if not _table_exists(cur, STAGING_PK_PASS2):
            cur.execute(f"""
                CREATE TABLE {STAGING_PK_PASS2} AS
                SELECT {BATCH_KEY}
                FROM {TARGET_TABLE}
                WHERE enc_sub_type_std IS NULL
                  AND enc_sub_type IS NOT NULL
                  AND TRIM(enc_sub_type) != ''
                  AND {BATCH_KEY} IS NOT NULL
            """)
            cur.execute(f"ALTER TABLE {STAGING_PK_PASS2} ADD INDEX idx_pk ({BATCH_KEY})")
            conn.commit()
            print("    created")
        else:
            print("    already exists, reusing")
        ranges_p2, total_p2 = _build_ranges(cur, STAGING_PK_PASS2)
        print(f"    {total_p2:,} rows → {len(ranges_p2)} batches")

    # ── 4. PK staging — Pass 5: all rows (enc_status_std) ────────────
    print("  Creating PK staging for Pass 5 (all rows — enc_status_std)...")
    if not _table_exists(cur, STAGING_PK_PASS5):
        cur.execute(f"""
            CREATE TABLE {STAGING_PK_PASS5} AS
            SELECT {BATCH_KEY}
            FROM {TARGET_TABLE}
            WHERE {BATCH_KEY} IS NOT NULL
        """)
        cur.execute(f"ALTER TABLE {STAGING_PK_PASS5} ADD INDEX idx_pk ({BATCH_KEY})")
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")
    ranges_p5, total_p5 = _build_ranges(cur, STAGING_PK_PASS5)
    print(f"    {total_p5:,} rows → {len(ranges_p5)} batches")

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

    # Passes 3, 4, 6 PK staging are deferred — built after upstream passes complete
    print("  Passes 3, 4, 6 PK staging deferred (rebuilt after upstream passes).")

    cur.close()
    conn.close()

    return {
        CHECKPOINT_PASS1: ranges_p1,
        CHECKPOINT_PASS2: ranges_p2,
        CHECKPOINT_PASS3: [],   # deferred after pass 2
        CHECKPOINT_PASS4: [],   # deferred after passes 1+3
        CHECKPOINT_PASS5: ranges_p5,
        CHECKPOINT_PASS6: [],   # deferred after pass 5
    }


# ── Deferred staging rebuilds ─────────────────────────────────────────

def rebuild_pass3_staging(all_ranges):
    """Build Pass 3 PK staging after Pass 2 has populated enc_sub_type_std."""
    print("\n  Rebuilding Pass 3 PK staging (enc_sub_type_std IS NOT NULL, enc_type_std IS NULL)...")
    conn = get_connection()
    cur  = conn.cursor()
    if not _table_exists(cur, STAGING_PK_PASS3):
        cur.execute(f"""
            CREATE TABLE {STAGING_PK_PASS3} AS
            SELECT {BATCH_KEY}
            FROM {TARGET_TABLE}
            WHERE enc_type_std IS NULL
              AND enc_sub_type_std IS NOT NULL
              AND {BATCH_KEY} IS NOT NULL
        """)
        cur.execute(f"ALTER TABLE {STAGING_PK_PASS3} ADD INDEX idx_pk ({BATCH_KEY})")
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")
    ranges_p3, total_p3 = _build_ranges(cur, STAGING_PK_PASS3)
    print(f"    {total_p3:,} rows → {len(ranges_p3)} batches")
    cur.close()
    conn.close()
    all_ranges[CHECKPOINT_PASS3] = ranges_p3
    return ranges_p3


def rebuild_pass4_staging(all_ranges):
    """Build Pass 4 PK staging after Passes 1+3 have populated enc_type_std."""
    print("\n  Rebuilding Pass 4 PK staging (enc_type_std IS NOT NULL, enc_category_std IS NULL)...")
    conn = get_connection()
    cur  = conn.cursor()
    if not _table_exists(cur, STAGING_PK_PASS4):
        cur.execute(f"""
            CREATE TABLE {STAGING_PK_PASS4} AS
            SELECT {BATCH_KEY}
            FROM {TARGET_TABLE}
            WHERE enc_type_std IS NOT NULL
              AND enc_category_std IS NULL
              AND {BATCH_KEY} IS NOT NULL
        """)
        cur.execute(f"ALTER TABLE {STAGING_PK_PASS4} ADD INDEX idx_pk ({BATCH_KEY})")
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")
    ranges_p4, total_p4 = _build_ranges(cur, STAGING_PK_PASS4)
    print(f"    {total_p4:,} rows → {len(ranges_p4)} batches")
    cur.close()
    conn.close()
    all_ranges[CHECKPOINT_PASS4] = ranges_p4
    return ranges_p4


def rebuild_pass6_staging(all_ranges):
    """Build Pass 6 PK staging after Pass 5 has populated enc_status_std."""
    print("\n  Rebuilding Pass 6 PK staging (enc_status_std IS NOT NULL)...")
    conn = get_connection()
    cur  = conn.cursor()
    if not _table_exists(cur, STAGING_PK_PASS6):
        cur.execute(f"""
            CREATE TABLE {STAGING_PK_PASS6} AS
            SELECT {BATCH_KEY}
            FROM {TARGET_TABLE}
            WHERE enc_status_std IS NOT NULL
              AND {BATCH_KEY} IS NOT NULL
        """)
        cur.execute(f"ALTER TABLE {STAGING_PK_PASS6} ADD INDEX idx_pk ({BATCH_KEY})")
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")
    ranges_p6, total_p6 = _build_ranges(cur, STAGING_PK_PASS6)
    print(f"    {total_p6:,} rows → {len(ranges_p6)} batches")
    cur.close()
    conn.close()
    all_ranges[CHECKPOINT_PASS6] = ranges_p6
    return ranges_p6


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
    print(f"  Encounters Standardisation UPDATE — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  target     : {TARGET_TABLE}")
    print(f"  batch_key  : {BATCH_KEY}")
    print(f"  batch_size : {BATCH_SIZE:,}")
    print(f"  passes     : 6  (enc_type_std | enc_sub_type_std | enc_type_std fallback |")
    print(f"                    enc_category_std | enc_status_std | normalized_category)")
    print(f"{'='*70}\n", flush=True)

    print("  Connecting to database and setting up tables...")
    sys.stdout.flush()
    all_ranges = setup_tables()

    results    = {}
    any_failed = False

    # Helper to run a pass with its own tqdm bar
    def _run(ck, label, build_fn, ranges):
        nonlocal any_failed
        if not ranges:
            conn_chk = get_connection()
            already  = is_done(conn_chk, ck)
            conn_chk.close()
            if already:
                print(f"\n  [SKIP] {label} — already done (checkpoint)")
                results[ck] = {"status": "skipped", "rows": 0, "secs": 0}
            else:
                print(f"\n  [SKIP] {label} — no eligible rows")
                results[ck] = {"status": "skipped", "rows": 0, "secs": 0}
            return True   # not a failure, keep going

        with tqdm(total=len(ranges), desc=label[:30], unit="batch") as pbar:
            print(f"\n  Starting {label} ({len(ranges)} batches)...")
            result = run_pass(ck, build_fn, ranges, pbar)
        results[ck] = result

        if result["status"].startswith("FAILED"):
            print(f"\n  FAILED at {label}: {result['status']}")
            print("  Aborting remaining passes.")
            any_failed = True
            return False
        return True

    # ── Pass 1: enc_type → enc_type_std ───────────────────────────────
    if not _run(CHECKPOINT_PASS1, "Pass 1 — enc_type_std from enc_type (REGEXP)",
                build_pass1, all_ranges[CHECKPOINT_PASS1]):
        pass
    else:
        # ── Pass 2: enc_sub_type → enc_sub_type_std ───────────────────
        if not _run(CHECKPOINT_PASS2, "Pass 2 — enc_sub_type_std from enc_sub_type (REGEXP_REPLACE)",
                    build_pass2, all_ranges[CHECKPOINT_PASS2]):
            pass
        else:
            # ── Pass 3: enc_sub_type_std → enc_type_std fallback ──────
            # Deferred: rebuild staging now that pass 2 has run
            conn_chk = get_connection()
            p3_done  = is_done(conn_chk, CHECKPOINT_PASS3)
            conn_chk.close()
            if not p3_done:
                rebuild_pass3_staging(all_ranges)
            _run(CHECKPOINT_PASS3, "Pass 3 — enc_type_std fallback from enc_sub_type_std (LIKE)",
                 build_pass3, all_ranges[CHECKPOINT_PASS3])

            if not any_failed:
                # ── Pass 4: enc_type_std → enc_category_std ───────────
                # Deferred: rebuild staging now that passes 1+3 have run
                conn_chk = get_connection()
                p4_done  = is_done(conn_chk, CHECKPOINT_PASS4)
                conn_chk.close()
                if not p4_done:
                    rebuild_pass4_staging(all_ranges)
                _run(CHECKPOINT_PASS4, "Pass 4 — enc_category_std from enc_type_std (CASE)",
                     build_pass4, all_ranges[CHECKPOINT_PASS4])

    if not any_failed:
        # ── Pass 5: enc_status → enc_status_std ───────────────────────
        _run(CHECKPOINT_PASS5, "Pass 5 — enc_status_std via visitstscodes JOIN",
             build_pass5, all_ranges[CHECKPOINT_PASS5])

        if not any_failed:
            # ── Pass 6: enc_status_std → normalized_category ──────────
            conn_chk = get_connection()
            p6_done  = is_done(conn_chk, CHECKPOINT_PASS6)
            conn_chk.close()
            if not p6_done:
                rebuild_pass6_staging(all_ranges)
            _run(CHECKPOINT_PASS6, "Pass 6 — normalized_category from enc_status_std (REGEXP)",
                 build_pass6, all_ranges[CHECKPOINT_PASS6])

    # ── Summary ───────────────────────────────────────────────────────
    all_pass_defs = [
        (CHECKPOINT_PASS1, "Pass 1 — enc_type_std from enc_type (REGEXP)"),
        (CHECKPOINT_PASS2, "Pass 2 — enc_sub_type_std from enc_sub_type (REGEXP_REPLACE)"),
        (CHECKPOINT_PASS3, "Pass 3 — enc_type_std fallback from enc_sub_type_std (LIKE)"),
        (CHECKPOINT_PASS4, "Pass 4 — enc_category_std from enc_type_std (CASE)"),
        (CHECKPOINT_PASS5, "Pass 5 — enc_status_std via visitstscodes JOIN"),
        (CHECKPOINT_PASS6, "Pass 6 — normalized_category from enc_status_std (REGEXP)"),
    ]
    print(f"\n{'='*70}")
    print(f"  Per-pass summary:")
    total_rows = 0
    for ck, label in all_pass_defs:
        res    = results.get(ck, {"status": "not run", "rows": 0, "secs": 0})
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
        print(f"  [{tag}] {label:<65}  {rows:>10,} rows  ({secs}s)")
        if status.startswith("FAILED"):
            print(f"         {status}")

    print(f"\n  Total rows updated: {total_rows:,}")
    print(f"{'='*70}")

    print(f"\n  Cleanup SQL (run after verifying data):")
    print(f"    -- Shared lookup (only drop when done with ALL encounters tables):")
    print(f"    -- DROP TABLE IF EXISTS {STAGING_VISITSTSCODES};")
    print(f"    -- Per-run tables:")
    print(f"    DROP TABLE IF EXISTS {STAGING_PK_PASS1};")
    print(f"    DROP TABLE IF EXISTS {STAGING_PK_PASS2};")
    print(f"    DROP TABLE IF EXISTS {STAGING_PK_PASS3};")
    print(f"    DROP TABLE IF EXISTS {STAGING_PK_PASS4};")
    print(f"    DROP TABLE IF EXISTS {STAGING_PK_PASS5};")
    print(f"    DROP TABLE IF EXISTS {STAGING_PK_PASS6};")
    print(f"    DROP TABLE IF EXISTS {CHECKPOINT_TABLE};")

    if any_failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
