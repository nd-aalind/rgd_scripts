#!/usr/bin/env python3
"""
enc_std.py — psid-partitioned standardisation UPDATEs for encounters  (v2 — batched)

Root cause of locking: the original ran one UPDATE per psid per pass, holding
InnoDB row locks for minutes.  This version batches every pass in BATCH_SIZE
chunks so each commit holds locks for ~1-3 seconds.

Changes from v1:
  - Each pass × psid is batched in BATCH_SIZE (20 000) PK chunks
  - PK+psid staging table built once, indexed on (psid, pk)
  - innodb_lock_wait_timeout + lock_wait_timeout = LOCK_TIMEOUT per session
  - Auto-retry (MAX_RETRIES) on deadlock (1213) / lock-wait timeout (1205)
  - Per-batch checkpoint key — re-run resumes at the first incomplete batch
  - CHECKPOINT_TABLE renamed to v2 so old partial-run entries don't interfere

Dependency chain (honoured within each psid):
  encounter_category + ehr_source_name + enc_sub_type → Pass 1 → enc_category_std (direct)
  enc_type                                             → Pass 2 → enc_type_std (direct)
  enc_sub_type → Pass 3 → enc_sub_type_std → Pass 4 → enc_type_std (imputed)
                                              Pass 5 (after 2+4) → enc_category_std (imputed)
"""

import logging
import os
import sys
import time
from datetime import datetime
import pymysql
from tqdm import tqdm
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())

# ── Configuration ──────────────────────────────────────────────────────────────
DB_CONFIG = {
    "host":            os.environ.get("DB_HOST"),
    "port":            3306,
    "user":            os.environ.get("DB_USER"),
    "password":        os.environ.get("DB_PASSWORD"),
    "database":        "udm_staging",
    "charset":         "utf8mb4",
    "connect_timeout": 30,
    "read_timeout":    86400,
    "write_timeout":   86400,
}

PSID_VALUES  = list(range(1, 15))
BATCH_SIZE   = 20_000
LOCK_TIMEOUT = 50        # seconds — innodb_lock_wait_timeout per session
MAX_RETRIES  = 3

TARGET_TABLE  = "rgd_udm_silver.encounters"
ENCOUNTERS_PK = "udm_inc_id"

_TABLE_SUFFIX    = TARGET_TABLE.replace(".", "_").replace("-", "_")
STAGING_CAT_MAP  = f"staging.enc_std_cat_map1_{_TABLE_SUFFIX}"
STG_TABLE        = f"staging.enc_std_pks_{_TABLE_SUFFIX}"
CHECKPOINT_TABLE = f"staging.etl_checkpoint_enc_std2_{_TABLE_SUFFIX}"   # v2

# ── Logging ────────────────────────────────────────────────────────────────────
_log_file = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    f"enc_std_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log",
)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(_log_file, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)
log    = logger.info


# ── Utilities ──────────────────────────────────────────────────────────────────

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


def _col_exists(cur, full_table_name, col_name):
    schema, table = full_table_name.split(".", 1)
    cur.execute(
        "SELECT COUNT(*) FROM information_schema.columns "
        "WHERE table_schema = %s AND table_name = %s AND column_name = %s",
        (schema, table, col_name),
    )
    return cur.fetchone()[0] > 0


def _index_exists(cur, schema, table, column):
    cur.execute(
        "SELECT COUNT(*) FROM information_schema.statistics "
        "WHERE table_schema = %s AND table_name = %s AND column_name = %s",
        (schema, table, column),
    )
    return cur.fetchone()[0] > 0


def _ensure_index(cur, conn, schema, table, column, prefix=None):
    if _index_exists(cur, schema, table, column):
        log(f"    exists: {schema}.{table} ({column})")
        return
    col_expr = f"{column}({prefix})" if prefix else column
    try:
        cur.execute(f"CREATE INDEX idx_{column} ON `{schema}`.`{table}` ({col_expr})")
        conn.commit()
        log(f"    created: {schema}.{table} ({col_expr})")
    except Exception as exc:
        try:
            conn.rollback()
        except Exception:
            pass
        log(f"    warning: {schema}.{table} ({col_expr}) — {exc}")


# ── Checkpoint ─────────────────────────────────────────────────────────────────

def _ck(pass_num, psid, lo):
    return f"enc.std.p{pass_num}.s{psid}.{_TABLE_SUFFIX}.{lo}"


def is_done(conn, key):
    cur = conn.cursor()
    try:
        cur.execute(
            f"SELECT status FROM {CHECKPOINT_TABLE} WHERE source_key = %s", (key,)
        )
        row = cur.fetchone()
        return row is not None and row[0] == "done"
    except pymysql.err.ProgrammingError:
        return False
    finally:
        cur.close()


def mark(conn, key, status, rows=0, error=None):
    cur = conn.cursor()
    cur.execute(
        f"""
        INSERT INTO {CHECKPOINT_TABLE}
            (source_key, status, rows_updated, started_at, completed_at, error_msg)
        VALUES (%s, %s, %s, NOW(), IF(%s='done', NOW(), NULL), %s)
        ON DUPLICATE KEY UPDATE
            status       = VALUES(status),
            rows_updated = VALUES(rows_updated),
            completed_at = IF(VALUES(status)='done', NOW(), NULL),
            error_msg    = VALUES(error_msg)
        """,
        (key, status, rows, status, error),
    )
    conn.commit()
    cur.close()


# ── PK staging ─────────────────────────────────────────────────────────────────

def setup_pk_staging(cur, conn):
    log(f"  Checking PK staging: {STG_TABLE} ...")
    if not _table_exists(cur, STG_TABLE):
        log("    building (SELECT pk + psid from target) ...")
        cur.execute(
            f"""
            CREATE TABLE {STG_TABLE} AS
            SELECT {ENCOUNTERS_PK} AS pk, psid
            FROM {TARGET_TABLE}
            """
        )
        conn.commit()
        cur.execute(f"CREATE INDEX idx_psid_pk ON {STG_TABLE} (psid, pk)")
        conn.commit()
        cur.execute(f"SELECT COUNT(*) FROM {STG_TABLE}")
        log(f"    staged {cur.fetchone()[0]:,} PKs.")
    else:
        cur.execute(f"SELECT COUNT(*) FROM {STG_TABLE}")
        log(f"    already exists — {cur.fetchone()[0]:,} rows.")


def get_batch_ranges(psid):
    conn = get_connection()
    cur  = conn.cursor()
    try:
        cur.execute(f"SELECT COUNT(*) FROM {STG_TABLE} WHERE psid = %s", (psid,))
        total = cur.fetchone()[0]
        if total == 0:
            return []
        cur.execute(
            f"""
            SELECT pk FROM (
                SELECT pk,
                       ROW_NUMBER() OVER (ORDER BY pk) AS rn
                FROM {STG_TABLE}
                WHERE psid = %s
            ) ranked
            WHERE MOD(rn - 1, %s) = 0
            ORDER BY pk
            """,
            (psid, BATCH_SIZE),
        )
        boundaries = [row[0] for row in cur.fetchall()]
        ranges = []
        for i, lo in enumerate(boundaries):
            hi = boundaries[i + 1] if i + 1 < len(boundaries) else None
            ranges.append((lo, hi))
        return ranges
    finally:
        cur.close()
        conn.close()


# ── Range filter helper ────────────────────────────────────────────────────────

def _rng(lo, hi):
    if hi is not None:
        return f"AND s.pk >= {lo} AND s.pk < {hi}"
    return f"AND s.pk >= {lo}"


# ── SQL builders ───────────────────────────────────────────────────────────────

def build_pass1(psid, lo, hi):
    return f"""
UPDATE {TARGET_TABLE} e
JOIN {STAGING_CAT_MAP} m
    ON  e.ehr_source_name    = m.ehr_source_name
    AND e.encounter_category = m.encounter_category
    AND COALESCE(e.enc_sub_type, '') = COALESCE(m.enc_sub_type, '')
JOIN {STG_TABLE} s ON e.{ENCOUNTERS_PK} = s.pk AND s.psid = {psid}
SET e.enc_category_std = m.enc_category_std
WHERE e.enc_category_std IS NULL
  AND e.encounter_category IS NOT NULL
  AND e.psid = {psid}
  {_rng(lo, hi)}
"""


def build_pass2(psid, lo, hi):
    return f"""
UPDATE {TARGET_TABLE} e
JOIN {STG_TABLE} s ON e.{ENCOUNTERS_PK} = s.pk AND s.psid = {psid}
SET e.enc_type_std = CASE
    WHEN e.enc_type IS NULL OR TRIM(e.enc_type) = '' THEN NULL
    WHEN LOWER(e.enc_type) REGEXP 'infusion'                                                                         THEN 'Infusion'
    WHEN LOWER(e.enc_type) REGEXP 'follow up|follow-up|follow|f/u|fu'                                               THEN 'Follow Up Visit'
    WHEN LOWER(e.enc_type) REGEXP 'eeg'                                                                              THEN 'EEG'
    WHEN LOWER(e.enc_type) REGEXP 'emg|electromyography'                                                             THEN 'EMG'
    WHEN LOWER(e.enc_type) REGEXP 'sleep|psg|mslt|cpap|mask fitting'                                                THEN 'Sleep Study'
    WHEN LOWER(e.enc_type) REGEXP 'mri|mra|mrv'                                                                      THEN 'MRI / Neuro Imaging'
    WHEN LOWER(e.enc_type) REGEXP 'xray|radiology|service coding'                                                    THEN 'Radiology'
    WHEN LOWER(e.enc_type) REGEXP 'nerve conduction'                                                                 THEN 'Nerve Conduction Study'
    WHEN LOWER(e.enc_type) REGEXP 'procedure|lumbar puncture|biopsy|spg|esi'                                        THEN 'Procedure'
    WHEN LOWER(e.enc_type) REGEXP 'injection|botox|dysport|xeomin|toxin'                                            THEN 'Injection / Botox'
    WHEN LOWER(e.enc_type) REGEXP 'testing|study|assessment|evaluation|actigraph|mda|special studies'               THEN 'Diagnostic Testing'
    WHEN LOWER(e.enc_type) REGEXP 'vng|eng|videonystagmogram|balance'                                                THEN 'Balance / Vestibular Testing'
    WHEN LOWER(e.enc_type) REGEXP 'hearing'                                                                          THEN 'Hearing Evaluation'
    WHEN LOWER(e.enc_type) REGEXP 'new'                                                                              THEN 'New Patient Visit'
    WHEN LOWER(e.enc_type) REGEXP 'tele|virtual'                                                                     THEN 'Telehealth Visit'
    WHEN LOWER(e.enc_type) REGEXP 'referral'                                                                         THEN 'Referral'
    WHEN LOWER(e.enc_type) REGEXP 'consult|neuro-ophthalmology'                                                      THEN 'Consultation'
    WHEN LOWER(e.enc_type) REGEXP 'research'                                                                         THEN 'Research'
    WHEN LOWER(e.enc_type) REGEXP 'office visit|established|multi-visit|out patient|problem|orv|physical therapy|end visit' THEN 'Office Visit'
    WHEN LOWER(e.enc_type) REGEXP 'hospital|in patient|nursing home'                                                 THEN 'Hospital Visit'
    WHEN LOWER(e.enc_type) REGEXP 'emergency'                                                                        THEN 'Emergency Visit'
    WHEN LOWER(e.enc_type) REGEXP 'admin|administration|meeting|legal|care management|primemobile|no charge|incomplete' THEN 'Administrative'
    WHEN LOWER(e.enc_type) REGEXP 'observation'                                                                      THEN 'Observation'
    WHEN LOWER(e.enc_type) REGEXP 'no show'                                                                          THEN 'No Show'
    WHEN LOWER(e.enc_type) REGEXP 'void'                                                                             THEN 'Voided Visit'
    WHEN LOWER(e.enc_type) REGEXP 'work in'                                                                          THEN 'Work In Visit'
    WHEN LOWER(e.enc_type) = 'other'                                                                                 THEN 'Other'
    ELSE 'NS'
END
WHERE e.enc_type_std IS NULL
  AND e.enc_type IS NOT NULL
  AND TRIM(e.enc_type) != ''
  AND e.psid = {psid}
  {_rng(lo, hi)}
"""


def build_pass3(psid, lo, hi):
    return f"""
UPDATE {TARGET_TABLE} e
JOIN {STG_TABLE} s ON e.{ENCOUNTERS_PK} = s.pk AND s.psid = {psid}
SET e.enc_sub_type_std = CASE
    WHEN e.enc_sub_type IS NULL OR TRIM(e.enc_sub_type) = ''         THEN NULL
    WHEN LOWER(TRIM(e.enc_sub_type)) = '48 or 72 hr'                 THEN 'Other'
    WHEN UPPER(TRIM(e.enc_sub_type)) = '(CANCELLATION/BILL)'         THEN 'CANCELLATION/BILL'
    WHEN UPPER(TRIM(e.enc_sub_type)) = '(PROCEDURE CANCELLATION)'    THEN 'PROCEDURE CANCELLATION'
    WHEN UPPER(TRIM(e.enc_sub_type)) = '(RESEARCH CANCELLATION)'     THEN 'RESEARCH CANCELLATION'
    WHEN TRIM(e.enc_sub_type) REGEXP '^[0-9]+(\\\\.[0-9]+)?$'        THEN 'Other'
    ELSE COALESCE(
        NULLIF(
            TRIM(
                REGEXP_REPLACE(
                REGEXP_REPLACE(
                REGEXP_REPLACE(
                REGEXP_REPLACE(
                REGEXP_REPLACE(
                REGEXP_REPLACE(
                REGEXP_REPLACE(
                REGEXP_REPLACE(
                REGEXP_REPLACE(
                REGEXP_REPLACE(
                REGEXP_REPLACE(
                REGEXP_REPLACE(
                REGEXP_REPLACE(
                REGEXP_REPLACE(
                    e.enc_sub_type,
                    '[0-9]+/[0-9]+',
                    ''),
                    '(^|[^A-Za-z])[0-9]+\\\\.?[0-9]*\\\\s*(hours|hour|hrs|hr|h)\\\\s*or\\\\s*[0-9]+\\\\.?[0-9]*\\\\s*(hours|hour|hrs|hr|h)([^A-Za-z]|$)',
                    ''),
                    '^[0-9]+\\\\s*(MINUTE|MINUTES|MIN|MINS|HOUR|HOURS|HR|HRS)\\\\s+',
                    ''),
                    '(^|[^A-Za-z])[0-9]+\\\\.?[0-9]*\\\\s*(hours|hour|mins|minutes|hrs|hr|min|h)([^A-Za-z]|$)',
                    ''),
                    '[0-9]+\\\\.?[0-9]*\\\\s*(days|day|weeks|week|months|month|years|year)',
                    ''),
                    '\\\\([^)]*\\\\)',
                    ''),
                    '\\\\s*[-/]\\\\s*[0-9]+[\\\\s-]*$',
                    ''),
                    '[_\\\\s]+[0-9]+\\\\s*$',
                    ''),
                    '^[0-9\\\\.]+\\\\s+(?!\\\\+)',
                    ''),
                    ',',
                    ' '),
                    '\\\\s*-+\\\\s*$',
                    ''),
                    '\\\\s+',
                    ' '),
                    '^\\\\*+\\\\s*',
                    ''),
                    '[*._]+\\\\s*$',
                    '')
            ),
            ''
        ),
        'Other'
    )
END
WHERE e.enc_sub_type_std IS NULL
  AND e.enc_sub_type IS NOT NULL
  AND TRIM(e.enc_sub_type) != ''
  AND e.psid = {psid}
  {_rng(lo, hi)}
"""


def build_pass4(psid, lo, hi):
    return f"""
UPDATE {TARGET_TABLE} e
JOIN {STG_TABLE} s ON e.{ENCOUNTERS_PK} = s.pk AND s.psid = {psid}
SET e.enc_type_std = CASE
    WHEN LOWER(e.enc_sub_type_std) REGEXP 'eeg|aeeg|veeg|ambulatory eeg|corticare-eeg|nexus-eeg|extended eeg|electroencephalography' THEN 'EEG'
    WHEN LOWER(e.enc_sub_type_std) REGEXP '(^emg$|emg[-/ ]|[-/ ]emg|same day emg|electromyography)'                                 THEN 'EMG'
    WHEN LOWER(e.enc_sub_type_std) REGEXP 'ncs|ncv|blink reflex|somatosensory|autonomic nervous system|nerve conduction'              THEN 'Nerve Conduction Study'
    WHEN LOWER(e.enc_sub_type_std) REGEXP '\\\\biv\\\\b|infusion|ocrevus|rituximab|hydration|spinraza|vyepti'                         THEN 'Infusion'
    WHEN LOWER(e.enc_sub_type_std) REGEXP 'mri|\\\\bmra\\\\b|mrv|brain|\\\\bct\\\\b|pet imaging'                                      THEN 'MRI / Neuro Imaging'
    WHEN LOWER(e.enc_sub_type_std) REGEXP 'xray|ultrasound'                                                                           THEN 'Radiology'
    WHEN LOWER(e.enc_sub_type_std) REGEXP 'mapping|nerve block|trigger|stim|proc|surgery|therapy|biopsy|dbs|lumbar puncture|treatment' THEN 'Procedure'
    WHEN LOWER(e.enc_sub_type_std) REGEXP 'injection|botox'                                                                           THEN 'Injection / Botox'
    WHEN LOWER(e.enc_sub_type_std) REGEXP 'testing|study|assessment|evaluation|exam|ekg|neuropsych'                                   THEN 'Diagnostic Testing'
    WHEN LOWER(e.enc_sub_type_std) REGEXP 'visit|clinic|office|medical|established|physical therapy'                                  THEN 'Office Visit'
    WHEN LOWER(e.enc_sub_type_std) REGEXP '^np|new patient|new pt'                                                                    THEN 'New Patient Visit'
    WHEN LOWER(e.enc_sub_type_std) REGEXP 'consult|conference|care planning'                                                          THEN 'Consultation'
    WHEN LOWER(e.enc_sub_type_std) REGEXP 'hearing|abr|assr|baer|audiolog|cochlear'                                                   THEN 'Hearing Evaluation'
    WHEN LOWER(e.enc_sub_type_std) REGEXP 'vestibular|balance|caloric|cvemp|vhit'                                                     THEN 'Balance / Vestibular Testing'
    WHEN LOWER(e.enc_sub_type_std) REGEXP 'form|billing|insurance|schedule|records|eprescription'                                     THEN 'Administrative'
    WHEN LOWER(e.enc_sub_type_std) REGEXP 'fit in|work in|wait list'                                                                  THEN 'Work In Visit'
    WHEN LOWER(e.enc_sub_type_std) REGEXP 'other|misc|unknown|trial'                                                                  THEN 'Other'
    ELSE 'Null'
END
WHERE (e.enc_type IS NULL OR TRIM(e.enc_type) = '')
  AND e.enc_sub_type_std IS NOT NULL
  AND TRIM(e.enc_sub_type_std) != ''
  AND e.psid = {psid}
  {_rng(lo, hi)}
"""


def build_pass5(psid, lo, hi):
    return f"""
UPDATE {TARGET_TABLE} e
JOIN {STG_TABLE} s ON e.{ENCOUNTERS_PK} = s.pk AND s.psid = {psid}
SET e.enc_category_std = CASE
    WHEN e.enc_type_std IN ('Infusion')                                                                                  THEN 'Infusion'
    WHEN e.enc_type_std IN ('Injection / Botox')                                                                         THEN 'Injection'
    WHEN e.enc_type_std IN ('MRI / Neuro Imaging','EEG','EMG','Sleep Study',
                             'Nerve Conduction Study','Balance / Vestibular Testing','Hearing Evaluation')               THEN 'Radiology'
    WHEN e.enc_type_std IN ('Diagnostic Testing')                                                                        THEN 'Testing'
    WHEN e.enc_type_std IN ('Procedure')                                                                                 THEN 'Procedures'
    WHEN e.enc_type_std IN ('Telehealth Visit')                                                                          THEN 'Virtual Visit'
    WHEN e.enc_type_std IN ('Referral')                                                                                  THEN 'Orderset'
    WHEN e.enc_type_std IN ('Work In Visit')                                                                             THEN 'Out of Office'
    WHEN e.enc_type_std IN ('Observation')                                                                               THEN 'Lab'
    WHEN e.enc_type_std IN ('Follow Up Visit')                                                                           THEN 'ePrescription Refills'
    WHEN e.enc_type_std IN ('Administrative','Research','No Show','Voided Visit')                                        THEN 'Administrative / Non-Clinical'
    WHEN e.enc_type_std IN ('Office Visit','New Patient Visit','Consultation')                                           THEN 'Office Visit'
    WHEN e.enc_type_std IN ('Hospital Visit','Emergency Visit')                                                          THEN 'Hospital Visit'
    WHEN e.enc_type_std IN ('Other')                                                                                     THEN 'Other'
    ELSE NULL
END
WHERE (e.encounter_category IS NULL OR TRIM(e.encounter_category) = '')
  AND e.enc_type_std IS NOT NULL
  AND TRIM(e.enc_type_std) != ''
  AND e.psid = {psid}
  {_rng(lo, hi)}
"""


# ── Batched executor ───────────────────────────────────────────────────────────

def _run_batched(pass_num, psid, ranges, build_fn, pbar, enabled=True):
    """Run one pass for one psid in small batches. Returns rows updated."""
    if not enabled:
        pbar.update(len(ranges))
        return 0

    conn = get_connection()
    try:
        # Session-scoped settings — fail fast, bulk-friendly
        with conn.cursor() as c:
            c.execute(f"SET innodb_lock_wait_timeout = {LOCK_TIMEOUT}")
            c.execute(f"SET lock_wait_timeout = {LOCK_TIMEOUT}")
            c.execute("SET unique_checks = 0")
            c.execute("SET foreign_key_checks = 0")

        total          = 0
        failed_batches = 0

        for lo, hi in ranges:
            ck = _ck(pass_num, psid, lo)
            if is_done(conn, ck):
                pbar.update(1)
                continue

            sql     = build_fn(psid, lo, hi)
            rows    = 0
            success = False

            for attempt in range(1, MAX_RETRIES + 1):
                try:
                    with conn.cursor() as c:
                        c.execute(sql)
                        conn.commit()
                        rows = c.rowcount
                    success = True
                    break
                except pymysql.err.OperationalError as exc:
                    try:
                        conn.rollback()
                    except Exception:
                        pass
                    if exc.args[0] in (1205, 1213) and attempt < MAX_RETRIES:
                        log(f"    [retry {attempt}/{MAX_RETRIES}] pass{pass_num} psid={psid} lo={lo}: {exc.args[1]}")
                        time.sleep(2 * attempt)
                    else:
                        log(f"    [FAILED] pass{pass_num} psid={psid} lo={lo}: {exc}")
                        failed_batches += 1
                        break

            if success:
                total += rows
                mark(conn, ck, "done", rows)

            pbar.update(1)

        if failed_batches:
            log(f"    WARNING: {failed_batches} batch(es) failed for pass{pass_num} psid={psid} — will retry on next run")

    finally:
        try:
            with conn.cursor() as c:
                c.execute("SET unique_checks = 1")
                c.execute("SET foreign_key_checks = 1")
        except Exception:
            pass
        conn.close()

    return total


# ── DDL: ensure std columns exist ─────────────────────────────────────────────

def ensure_std_columns():
    std_cols = [
        ("enc_category_std", "VARCHAR(100)"),
        ("enc_type_std",     "VARCHAR(100)"),
        ("enc_sub_type_std", "VARCHAR(200)"),
    ]
    log(f"  Checking std columns on {TARGET_TABLE} ...")
    conn = get_connection()
    cur  = conn.cursor()
    cur.execute("SET lock_wait_timeout = 15")   # surface metadata locks fast
    added = []
    try:
        for col_name, col_type in std_cols:
            if not _col_exists(cur, TARGET_TABLE, col_name):
                log(f"    adding {col_name} {col_type} ...")
                cur.execute(
                    f"ALTER TABLE {TARGET_TABLE} ADD COLUMN {col_name} {col_type} DEFAULT NULL"
                )
                conn.commit()
                added.append(col_name)
            else:
                log(f"    exists: {col_name}")
    except Exception as exc:
        try:
            conn.rollback()
        except Exception:
            pass
        log(f"\n  ERROR: ALTER TABLE blocked — likely a metadata lock on {TARGET_TABLE}.")
        log(f"  Find the blocker:")
        log(f"    SELECT id, user, state, info FROM information_schema.processlist")
        log(f"    WHERE state LIKE '%lock%' OR state LIKE '%wait%' ORDER BY time DESC;")
        log(f"  Then: KILL <id>;")
        log(f"\n  Original error: {exc}")
        sys.exit(1)
    finally:
        cur.close()
        conn.close()

    if added:
        log(f"    Added: {', '.join(added)}")
    else:
        log("    All std columns already present.")


# ── Setup ──────────────────────────────────────────────────────────────────────

def setup():
    ensure_std_columns()

    conn = get_connection()
    cur  = conn.cursor()
    try:
        _has_enc_type     = _col_exists(cur, TARGET_TABLE, "enc_type")
        _has_enc_sub_type = _col_exists(cur, TARGET_TABLE, "enc_sub_type")

        # ── Category lookup map ───────────────────────────────────────────────
        log(f"  Checking enc_category_std lookup: {STAGING_CAT_MAP} ...")
        if not _table_exists(cur, STAGING_CAT_MAP):
            log("    building ...")
            cur.execute(f"""
                CREATE TABLE {STAGING_CAT_MAP} AS
                WITH base AS (
                    SELECT DISTINCT
                        ehr_source_name,
                        encounter_category,
                        enc_sub_type,
                        CASE
                            WHEN ehr_source_name = 'eCW' AND encounter_category = '1'                         THEN 'Office Visit'
                            WHEN ehr_source_name = 'eCW' AND encounter_category = '2'                         THEN 'Telephone'
                            WHEN ehr_source_name = 'eCW' AND encounter_category = '3'                         THEN 'Out of Office'
                            WHEN ehr_source_name = 'eCW' AND encounter_category = '4'                         THEN 'Claim'
                            WHEN ehr_source_name = 'eCW' AND encounter_category = '5'                         THEN 'Lab'
                            WHEN ehr_source_name = 'eCW' AND encounter_category = '6'                         THEN 'Web Encounter'
                            WHEN ehr_source_name = 'eCW' AND encounter_category = '7'                         THEN 'ePrescription'
                            WHEN ehr_source_name = 'eCW' AND encounter_category = '8'                         THEN 'PTDASH'
                            WHEN ehr_source_name = 'eCW' AND encounter_category = '9'                         THEN 'Orderset'
                            WHEN ehr_source_name = 'eCW' AND encounter_category IN ('10','12','13','14','102') THEN 'Other'
                            ELSE encounter_category
                        END AS norm_cat
                    FROM {TARGET_TABLE}
                )
                SELECT DISTINCT
                    ehr_source_name,
                    encounter_category,
                    enc_sub_type,
                    CASE
                        WHEN norm_cat IS NULL OR TRIM(norm_cat) = '' THEN NULL
                        WHEN ehr_source_name = 'AthenaPractice' AND norm_cat LIKE 'zMDC Service Coding%'
                             AND LOWER(COALESCE(enc_sub_type,'')) REGEXP '(injection|epidural steroid)'
                            THEN 'Injection'
                        WHEN ehr_source_name = 'AthenaPractice' AND norm_cat LIKE 'zMDC Service Coding%'
                             AND LOWER(COALESCE(enc_sub_type,'')) REGEXP '(lumbar puncture|blood patch|facet|si joint|trochanteric|troch)'
                            THEN 'Procedures'
                        WHEN ehr_source_name = 'AthenaPractice' AND norm_cat LIKE 'zMDC Service Coding%'
                             AND LOWER(COALESCE(enc_sub_type,'')) REGEXP '(mri|mra|mrv|ct|angiogram|myelogram|xray|soft tissue|chest|abdomen|pelvis|head|cervical|thoracic|orbit|extremity|lumbar)'
                            THEN 'Radiology'
                        WHEN ehr_source_name = 'AthenaPractice' AND norm_cat LIKE 'zMDC Service Coding%'
                             AND COALESCE(enc_sub_type,'') REGEXP '(?i)(testing|visual fields)'
                            THEN 'Testing'
                        WHEN norm_cat REGEXP '(?i)(telemedicine|telehealth|telephone|virtual|web encounter|neuropsy telemedicine interview)' THEN 'Virtual Visit'
                        WHEN norm_cat REGEXP '(?i)(cpap|home sleep study|sleep read|actigraph watch|neuropsychological evaluation|cognitive assessment)' THEN 'Testing'
                        WHEN norm_cat REGEXP '(?i)^care management$'                                        THEN 'Administrative / Non-Clinical'
                        WHEN norm_cat REGEXP '(?i)(infusion administration|^infusion|zinfusion)'            THEN 'Infusion'
                        WHEN norm_cat REGEXP '(?i)(^injection|botox)'                                       THEN 'Injection'
                        WHEN norm_cat REGEXP '(?i)(radiology|mri|emg|eeg|xray|ct|ultrasound)'              THEN 'Radiology'
                        WHEN norm_cat REGEXP '(?i)(testing|assessment|evaluation)'                          THEN 'Testing'
                        WHEN norm_cat REGEXP '(?i)(procedure|surgery)'                                      THEN 'Procedures'
                        WHEN norm_cat REGEXP '(?i)(orderset|ordersonly)'                                    THEN 'Orderset'
                        WHEN norm_cat REGEXP '(?i)(out of office|field)'                                    THEN 'Out of Office'
                        WHEN norm_cat REGEXP '(?i)^lab'                                                     THEN 'Lab'
                        WHEN norm_cat REGEXP '(?i)eprescription'                                            THEN 'ePrescription Refills'
                        WHEN norm_cat REGEXP '(?i)(flowsheet|historical|ptdash|claim|admin)'                THEN 'Administrative / Non-Clinical'
                        WHEN norm_cat REGEXP '(?i)(hospital|in patient|inpatient|observation)'              THEN 'Hospital Visit'
                        WHEN norm_cat REGEXP '(?i)(office visit|visit|consult|new pt|follow up|ambulatory|nursing|physical therapy|orv)' THEN 'Office Visit'
                        WHEN norm_cat IN ('Other','Void','Research','Balance','Special Studies')             THEN 'Other'
                        ELSE 'NS'
                    END AS enc_category_std
                FROM base
            """)
            cur.execute(
                f"ALTER TABLE {STAGING_CAT_MAP} "
                f"ADD INDEX idx_cat (ehr_source_name(50), encounter_category(50))"
            )
            conn.commit()
            log("    created.")
        else:
            log("    already exists, reusing.")
        cur.execute(f"SELECT COUNT(*) FROM {STAGING_CAT_MAP}")
        log(f"    {cur.fetchone()[0]:,} distinct combos.")

        # ── Source column indexes ─────────────────────────────────────────────
        log("  Checking source column indexes ...")
        tgt_schema, tgt_table = TARGET_TABLE.split(".", 1)
        _ensure_index(cur, conn, tgt_schema, tgt_table, "psid")
        _ensure_index(cur, conn, tgt_schema, tgt_table, "ehr_source_name",    prefix=50)
        _ensure_index(cur, conn, tgt_schema, tgt_table, "encounter_category", prefix=50)
        if _has_enc_type:
            _ensure_index(cur, conn, tgt_schema, tgt_table, "enc_type")
        if _has_enc_sub_type:
            _ensure_index(cur, conn, tgt_schema, tgt_table, "enc_sub_type", prefix=100)

        # ── PK staging ────────────────────────────────────────────────────────
        setup_pk_staging(cur, conn)

        # ── Checkpoint table ──────────────────────────────────────────────────
        log(f"  Creating checkpoint table: {CHECKPOINT_TABLE} ...")
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {CHECKPOINT_TABLE} (
                source_key   VARCHAR(200) NOT NULL PRIMARY KEY,
                status       ENUM('running','done','failed') NOT NULL DEFAULT 'running',
                rows_updated BIGINT      DEFAULT 0,
                started_at   DATETIME    DEFAULT NULL,
                completed_at DATETIME    DEFAULT NULL,
                error_msg    TEXT        DEFAULT NULL
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """)
        conn.commit()
        log("    ready.")

    finally:
        cur.close()
        conn.close()

    return _has_enc_type, _has_enc_sub_type


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    log(f"\n{'='*70}")
    log(f"  Encounters Standardisation UPDATE (batched v2) — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log(f"  target     : {TARGET_TABLE}")
    log(f"  psid range : {PSID_VALUES[0]} … {PSID_VALUES[-1]}  ({len(PSID_VALUES)} partitions)")
    log(f"  batch size : {BATCH_SIZE:,}  lock timeout: {LOCK_TIMEOUT}s  retries: {MAX_RETRIES}")
    log(f"{'='*70}\n")

    has_enc_type, has_enc_sub_type = setup()

    # Precompute per-psid batch ranges (one DB round-trip per psid)
    log("\n  Computing batch ranges per psid ...")
    all_ranges = {}
    for psid in PSID_VALUES:
        all_ranges[psid] = get_batch_ranges(psid)
        log(f"    psid {psid:>2}: {len(all_ranges[psid]):,} batches")

    total_ticks = sum(len(r) for r in all_ranges.values()) * 5
    log(f"\n  Total batches × passes: {total_ticks:,}\n")

    grand_total = 0
    psid_summary = []

    with tqdm(total=total_ticks, desc="Overall", unit="batch") as pbar:
        for psid in PSID_VALUES:
            ranges = all_ranges[psid]
            if not ranges:
                log(f"\n  psid {psid}: no rows — skipping all passes.")
                pbar.update(5 * 0)   # zero batches, nothing to advance
                psid_summary.append((psid, 0))
                continue

            log(f"\n  ── psid {psid} ({len(ranges)} batches × 5 passes) ────────────────────────")

            r1 = _run_batched(1, psid, ranges, build_pass1,  pbar)
            r2 = _run_batched(2, psid, ranges, build_pass2,  pbar, has_enc_type)
            r3 = _run_batched(3, psid, ranges, build_pass3,  pbar, has_enc_sub_type)
            r4 = _run_batched(4, psid, ranges, build_pass4,  pbar, has_enc_sub_type)
            r5 = _run_batched(5, psid, ranges, build_pass5,  pbar)

            total_psid = r1 + r2 + r3 + r4 + r5
            grand_total += total_psid
            psid_summary.append((psid, total_psid))
            log(f"    psid {psid} done — {total_psid:,} rows updated  "
                f"(p1={r1:,} p2={r2:,} p3={r3:,} p4={r4:,} p5={r5:,})")

    log(f"\n{'='*70}")
    log(f"  Per-psid summary:")
    for psid, rows in psid_summary:
        log(f"    psid {psid:<3}  {rows:>12,} rows")
    log(f"\n  Grand total rows updated: {grand_total:,}")
    log(f"{'='*70}")

    log(f"\n  Cleanup SQL (run after verifying data):")
    log(f"    -- Keep STAGING_CAT_MAP if running this script again:")
    log(f"    -- DROP TABLE IF EXISTS {STAGING_CAT_MAP};")
    log(f"    DROP TABLE IF EXISTS {STG_TABLE};")
    log(f"    DROP TABLE IF EXISTS {CHECKPOINT_TABLE};")


if __name__ == "__main__":
    main()
