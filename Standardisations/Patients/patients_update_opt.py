#!/usr/bin/env python3
"""
Optimized batched standardisation UPDATEs for patients/patient_demographics tables.

Seven sequential passes — each with checkpoint/resume:

  Pass 1 — rgd_udm_silver.patients (all rows):
    SET gender_hl7_std, gender_CDISC_std, gender_OMOP_std, gender_OMOP_concept_id
    CASE on LOWER(TRIM(gender)) — no JOIN needed

  Pass 2 — rgd_udm_silver.patient_demographics (all rows):
    SET pat_race_code_std, pat_race_std
    JOIN semantics.race twice (by code and by name) — pre-materialized

  Pass 3 — rgd_udm_silver.patient_demographics (WHERE pat_race_code_std='NS' OR pat_race_std='NS'):
    SET pat_race_code_std, pat_race_std
    Fallback CASE IN lists for unmapped race names — no JOIN

  Pass 4 — rgd_udm_silver.patients (all rows):
    SET pat_ethnicity_code_std, pat_ethnicity_std
    JOIN semantics.ethnicity twice (by code and by name) — pre-materialized

  Pass 5 — rgd_udm_silver.patients (WHERE pat_ethnicity_code LIKE '%,%'):
    SET pat_ethnicity_code_std, pat_ethnicity_std
    Comma-separated ethnicity codes split via JSON_TABLE — pre-materialized mapping

  Pass 6 — rgd_udm_silver.patient_demographics (all rows):
    SET pat_deceased_status_std
    CASE on pat_deceased_status + deceased_date — no JOIN

  Pass 7 — rgd_udm_silver.patient_demographics (all rows):
    SET pat_marital_status_std
    CASE on UPPER(TRIM(pat_marital_status)) — no JOIN

Pre-materialized lookups (computed ONCE, reused across all batches):
  - staging.pat_std_race       (semantics.race — indexed on Code and race)
  - staging.pat_std_eth        (semantics.ethnicity — indexed on Code and ethnicity)
  - staging.pat_std_eth_mapping (JSON_TABLE pre-aggregated comma-separated codes)

Std columns added to target tables if not present (with metadata lock guard).

Optimizations applied:
- Lookup tables pre-materialized once (not re-scanned per batch)
- Per-pass PK staging tables (filtered to eligible rows only)
- Server-side boundary sampling (avoids loading all PKs into memory)
- Commit after every batch (frees undo/log space)
- Checkpoint/resume per pass — re-run skips completed passes
- Disabled InnoDB checks per-session for bulk update speed
- Progress bar via tqdm

Usage:
    python patients_update_opt.py
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

PATIENTS_TABLE = "rgd_udm_silver.patients"
SEMANTICS_DB   = "semantics"

# Batch key
BATCH_KEY_PAT  = "ndid"
BATCH_KEY_DEMO = "ndid"   # same table, same key

# ── Pre-materialized lookup tables ─────────────────────────────────────
STAGING_RACE        = "staging.pat_std_race_n"           # semantics.race
STAGING_ETH         = "staging.pat_std_eth_n"            # semantics.ethnicity
STAGING_ETH_MAPPING = "staging.pat_std_eth_mapping_n"    # comma-separated codes mapping

# ── Per-pass PK staging tables ─────────────────────────────────────────
# Shared between passes targeting the same table with same filter
STAGING_PK_PAT_ALL   = "staging.pat_std_pk_patients_all_n"      # patients, all rows
STAGING_PK_DEMO_ALL  = "staging.pat_std_pk_demo_all_n"          # patient_demographics, all rows
STAGING_PK_DEMO_NS   = "staging.pat_std_pk_demo_race__n"      # demographics WHERE race='NS'
STAGING_PK_PAT_ETH   = "staging.pat_std_pk_patients_eth_csv_n"  # patients with comma ethnicity

# ── Checkpoint ─────────────────────────────────────────────────────────
CHECKPOINT_TABLE = "staging.etl_checkpoint_patients_standardisation_n"
CHECKPOINT_PASS1 = "patients.std.pass1.gender"
CHECKPOINT_PASS2 = "patients.std.pass2.race_lookup"
CHECKPOINT_PASS3 = "patients.std.pass3.race_name_fallback"
CHECKPOINT_PASS4 = "patients.std.pass4.ethnicity_lookup"
CHECKPOINT_PASS5 = "patients.std.pass5.ethnicity_csv"
CHECKPOINT_PASS6 = "patients.std.pass6.deceased_status"
CHECKPOINT_PASS7 = "patients.std.pass7.marital_status"


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


def _build_ranges(cur, staging_pk, batch_key):
    """Compute batch boundary ranges from a PK staging table."""
    cur.execute(f"SELECT COUNT(*) FROM {staging_pk}")
    total = cur.fetchone()[0]
    if total == 0:
        return [], 0

    cur.execute(f"""
        SELECT {batch_key}
        FROM (
            SELECT {batch_key},
                   ROW_NUMBER() OVER (ORDER BY {batch_key}) AS rn
            FROM {staging_pk}
        ) t
        WHERE (rn - 1) % {BATCH_SIZE} = 0
        ORDER BY {batch_key}
    """)
    boundaries = [row[0] for row in cur.fetchall() if row[0] is not None]

    cur.execute(f"SELECT MAX({batch_key}) FROM {staging_pk}")
    max_pk = int(cur.fetchone()[0])

    ranges = []
    for i, lo in enumerate(boundaries):
        hi = boundaries[i + 1] if i + 1 < len(boundaries) else max_pk + 1
        ranges.append((lo, hi))

    return ranges, total


# ── Batch UPDATE builders ─────────────────────────────────────────────

def build_pass1(pk_lo, pk_hi):
    """Pass 1: gender standardisation on patients — no JOIN."""
    return f"""
UPDATE {PATIENTS_TABLE}
SET
    gender_hl7_std = CASE
        WHEN LOWER(TRIM(gender)) IN ('male', 'm') THEN 'Male'
        WHEN LOWER(TRIM(gender)) IN ('female', 'f') THEN 'Female'
        WHEN LOWER(TRIM(gender)) LIKE 'oth%'
          OR LOWER(TRIM(gender)) LIKE 'x%'
          OR LOWER(TRIM(gender)) LIKE 'ambi%' THEN 'Other'
        WHEN LOWER(TRIM(gender)) LIKE 'u%' THEN 'Unknown'
        WHEN gender IS NULL OR TRIM(gender) = '' THEN 'Unknown'
        ELSE 'NS'
    END,
    gender_CDISC_std = CASE
        WHEN LOWER(TRIM(gender)) IN ('male', 'm') THEN 'M'
        WHEN LOWER(TRIM(gender)) IN ('female', 'f') THEN 'F'
        WHEN LOWER(TRIM(gender)) LIKE 'oth%'
          OR LOWER(TRIM(gender)) LIKE 'x%' THEN 'Undifferentiated'
        WHEN LOWER(TRIM(gender)) LIKE 'u%' THEN NULL
        WHEN gender IS NULL OR TRIM(gender) = '' THEN NULL
        ELSE 'NS'
    END,
    gender_OMOP_std = CASE
        WHEN LOWER(TRIM(gender)) IN ('male', 'm') THEN 'MALE'
        WHEN LOWER(TRIM(gender)) IN ('female', 'f') THEN 'FEMALE'
        WHEN LOWER(TRIM(gender)) LIKE 'x%'
          OR LOWER(TRIM(gender)) LIKE 'ambi%' THEN 'AMBIGUOUS'
        WHEN LOWER(TRIM(gender)) LIKE 'oth%' THEN 'OTHER'
        WHEN LOWER(TRIM(gender)) LIKE 'u%' THEN 'UNKNOWN'
        WHEN gender IS NULL OR TRIM(gender) = '' THEN 'UNKNOWN'
        ELSE 'NS'
    END,
    gender_OMOP_concept_id = CASE
        WHEN LOWER(TRIM(gender)) IN ('male', 'm') THEN '8507'
        WHEN LOWER(TRIM(gender)) IN ('female', 'f') THEN '8532'
        WHEN LOWER(TRIM(gender)) LIKE 'oth%' THEN '8521'
        WHEN LOWER(TRIM(gender)) LIKE 'x%'
          OR LOWER(TRIM(gender)) LIKE 'ambi%' THEN '8570'
        WHEN LOWER(TRIM(gender)) LIKE 'u%' THEN '8551'
        WHEN gender IS NULL OR TRIM(gender) = '' THEN '8551'
        ELSE 'NS'
    END
WHERE {BATCH_KEY_PAT} >= {pk_lo}
  AND {BATCH_KEY_PAT} < {pk_hi}
"""


def build_pass2(pk_lo, pk_hi):
    """Pass 2: race lookup on patient_demographics — JOIN pre-materialized race table."""
    return f"""
UPDATE {PATIENTS_TABLE} a
LEFT JOIN {STAGING_RACE} b1 ON a.pat_race_code = b1.Code
LEFT JOIN {STAGING_RACE} b2 ON LOWER(a.pat_race) = b2.race_lower
SET
    a.pat_race_code_std = CASE
        WHEN b1.Code IS NOT NULL THEN b1.Code
        WHEN b2.Code IS NOT NULL THEN b2.Code
        WHEN (a.pat_race_code IS NULL OR TRIM(a.pat_race_code) = '')
             AND (a.pat_race IS NULL OR TRIM(a.pat_race) = '') THEN 'Unknown'
        ELSE 'NS'
    END,
    a.pat_race_std = CASE
        WHEN b1.Code IS NOT NULL THEN b1.race
        WHEN b2.Code IS NOT NULL THEN b2.race
        WHEN (a.pat_race_code IS NULL OR TRIM(a.pat_race_code) = '')
             AND (a.pat_race IS NULL OR TRIM(a.pat_race) = '') THEN 'Unknown'
        ELSE 'NS'
    END
WHERE a.{BATCH_KEY_DEMO} >= {pk_lo}
  AND a.{BATCH_KEY_DEMO} < {pk_hi}
"""


def build_pass3(pk_lo, pk_hi):
    """Pass 3: race name fallback for rows still 'NS' — no JOIN, CASE IN lists."""
    return f"""
UPDATE {PATIENTS_TABLE}
SET
    pat_race_code_std = CASE
        WHEN LOWER(pat_race) IN (
            'white','caucasian','caucasian/white','wwhite','whtie','wnite','whiet','whiite',
            'whitte','whtte','whjite','wjhite','wjote','wgite','whgite','whiteq','whte',
            'hungarian','moore','white/unsure','white,english','white,declined to specify',
            'white,english,declined to specify','english,declined to specify',
            'white,other race','european,english,cherokee,italian,polish'
        ) THEN '2106-3'
        WHEN LOWER(pat_race) IN (
            'black or african american','african american','african america',
            'afrcan american','african americian','african amercian',
            'african american/black','african american & caucasian','somalia',
            'black/white','black & hispanic','black/asian','black and sicilian',
            'black or african american (biracial)','black/white/indian',
            'black,declined to specify','black,other race',
            'black or african american,native hawaiian or other pacific islander',
            'black or african american,white','black or african american,white,black',
            'black or african american,declined to specify','african american,white',
            'african american,other race','african american,declined to specify',
            'african american,black'
        ) THEN '2054-5'
        WHEN LOWER(pat_race) IN (
            'american indian or alaska nati','american indian or alaskan native',
            'native american indian','native american','indian',
            'white/american indian','white/black/american indian','native/white',
            'white,american indian or alaska native','white/spanish american indian',
            'white,spanish american indian'
        ) THEN '1002-5'
        WHEN LOWER(pat_race) IN (
            'east indian','asian/indian','sikh','usikh',
            'white/asian','asian/white','white,asian',
            'white,american indian or alaska native,asian'
        ) THEN '2028-9'
        WHEN LOWER(pat_race) IN ('pacific islander') THEN '2076-8'
        WHEN LOWER(pat_race) IN (
            'arabic','arab-palestinan','white/arabic','middle eastern',
            'other race~arabic','other race/pakistani','other race/turkish','other race/hindu'
        ) THEN '2118-8'
        WHEN LOWER(pat_race) IN (
            'hispanic','hispanic-puerto rican','hispanic/white','white/hispanic',
            'white/puerto rican','white & puerto rican','latina',
            'puerto rican','white/black','other race,declined to specify'
        ) THEN '2131-1'
        WHEN LOWER(pat_race) IN (
            'unreported/refused to report','declined to specify','patient declined',
            'unspecified','state prohibited','unknown','unkown','uknown','unknow',
            'unkonown','unknownc','declined','none-other','n/a',
            'na@dent.com','donotemail@dent.com','20181227@dentinstitue.com',
            'kath_lean1@yahoo.com','mrschowski@aol.com','ekwilos@hotmail.com',
            'e','w','h','o','u','c','r','osco'
        ) THEN 'UNK'
        ELSE 'NS'
    END,
    pat_race_std = CASE
        WHEN LOWER(pat_race) IN (
            'white','caucasian','caucasian/white','wwhite','whtie','wnite','whiet','whiite',
            'whitte','whtte','whjite','wjhite','wjote','wgite','whgite','whiteq','whte',
            'hungarian','moore','white/unsure','white,english','white,declined to specify',
            'white,english,declined to specify','english,declined to specify',
            'white,other race','european,english,cherokee,italian,polish'
        ) THEN 'White'
        WHEN LOWER(pat_race) IN (
            'black or african american','african american','african america',
            'afrcan american','african americian','african amercian',
            'african american/black','african american & caucasian','somalia',
            'black/white','black & hispanic','black/asian','black and sicilian',
            'black or african american (biracial)','black/white/indian',
            'black,declined to specify','black,other race',
            'black or african american,native hawaiian or other pacific islander',
            'black or african american,white','black or african american,white,black',
            'black or african american,declined to specify','african american,white',
            'african american,other race','african american,declined to specify',
            'african american,black'
        ) THEN 'Black or African American'
        WHEN LOWER(pat_race) IN (
            'american indian or alaska nati','american indian or alaskan native',
            'native american indian','native american','indian',
            'white/american indian','white/black/american indian','native/white',
            'white,american indian or alaska native','white/spanish american indian',
            'white,spanish american indian'
        ) THEN 'American Indian or Alaska Native'
        WHEN LOWER(pat_race) IN (
            'east indian','asian/indian','sikh','usikh',
            'white/asian','asian/white','white,asian',
            'white,american indian or alaska native,asian'
        ) THEN 'Asian'
        WHEN LOWER(pat_race) IN ('pacific islander') THEN 'Native Hawaiian or Other Pacific Islander'
        WHEN LOWER(pat_race) IN (
            'arabic','arab-palestinan','white/arabic','middle eastern',
            'other race~arabic','other race/pakistani','other race/turkish','other race/hindu'
        ) THEN 'Middle Eastern or North African'
        WHEN LOWER(pat_race) IN (
            'hispanic','hispanic-puerto rican','hispanic/white','white/hispanic',
            'white/puerto rican','white & puerto rican','latina',
            'puerto rican','white/black','other race,declined to specify'
        ) THEN 'Other Race'
        WHEN LOWER(pat_race) IN (
            'unreported/refused to report','declined to specify','patient declined',
            'unspecified','state prohibited','unknown','unkown','uknown','unknow',
            'unkonown','unknownc','declined','none-other','n/a',
            'na@dent.com','donotemail@dent.com','20181227@dentinstitue.com',
            'kath_lean1@yahoo.com','mrschowski@aol.com','ekwilos@hotmail.com',
            'e','w','h','o','u','c','r','osco'
        ) THEN 'UNK'
        ELSE 'NS'
    END
WHERE (pat_race_code_std = 'NS' OR pat_race_std = 'NS')
  AND {BATCH_KEY_DEMO} >= {pk_lo}
  AND {BATCH_KEY_DEMO} < {pk_hi}
"""


def build_pass4(pk_lo, pk_hi):
    """Pass 4: ethnicity lookup on patients — JOIN pre-materialized ethnicity table."""
    return f"""
UPDATE {PATIENTS_TABLE} a
LEFT JOIN {STAGING_ETH} b1 ON a.pat_ethnicity_code = b1.Code
LEFT JOIN {STAGING_ETH} b2 ON LOWER(TRIM(a.pat_ethnicity)) = b2.ethnicity_lower
SET
    a.pat_ethnicity_code_std = CASE
        WHEN b1.Code IS NOT NULL THEN b1.Code
        WHEN b2.Code IS NOT NULL THEN b2.Code
        WHEN ((a.pat_ethnicity_code IS NULL OR TRIM(a.pat_ethnicity_code) = '')
              AND (a.pat_ethnicity IS NULL OR TRIM(a.pat_ethnicity) = ''))
          OR UPPER(TRIM(a.pat_ethnicity_code)) IN ('ASKU')
          OR LOWER(TRIM(a.pat_ethnicity_code)) LIKE '%declined%'
          OR LOWER(TRIM(a.pat_ethnicity)) LIKE '%declined%'
          OR LOWER(TRIM(a.pat_ethnicity)) LIKE '%refused%'
          OR LOWER(TRIM(a.pat_ethnicity)) LIKE '%unknown%' THEN 'Unknown'
        ELSE 'NS'
    END,
    a.pat_ethnicity_std = CASE
        WHEN b1.Code IS NOT NULL THEN b1.ethnicity
        WHEN b2.Code IS NOT NULL THEN b2.ethnicity
        WHEN ((a.pat_ethnicity_code IS NULL OR TRIM(a.pat_ethnicity_code) = '')
              AND (a.pat_ethnicity IS NULL OR TRIM(a.pat_ethnicity) = ''))
          OR UPPER(TRIM(a.pat_ethnicity_code)) IN ('ASKU')
          OR LOWER(TRIM(a.pat_ethnicity_code)) LIKE '%declined%'
          OR LOWER(TRIM(a.pat_ethnicity)) LIKE '%declined%'
          OR LOWER(TRIM(a.pat_ethnicity)) LIKE '%refused%'
          OR LOWER(TRIM(a.pat_ethnicity)) LIKE '%unknown%' THEN 'Unknown'
        ELSE 'NS'
    END
WHERE a.{BATCH_KEY_PAT} >= {pk_lo}
  AND a.{BATCH_KEY_PAT} < {pk_hi}
"""


def build_pass5(pk_lo, pk_hi):
    """Pass 5: comma-separated ethnicity codes — JOIN pre-materialized mapping."""
    return f"""
UPDATE {PATIENTS_TABLE} p
JOIN {STAGING_ETH_MAPPING} m ON p.pat_ethnicity_code = m.pat_ethnicity_code
SET
    p.pat_ethnicity_code_std = m.pat_ethnicity_code_std,
    p.pat_ethnicity_std      = m.pat_ethnicity_std
WHERE p.{BATCH_KEY_PAT} >= {pk_lo}
  AND p.{BATCH_KEY_PAT} < {pk_hi}
"""


def build_pass6(pk_lo, pk_hi):
    """Pass 6: deceased status on patient_demographics — no JOIN."""
    return f"""
UPDATE {PATIENTS_TABLE}
SET pat_deceased_status_std = CASE
    WHEN pat_deceased_status IN ('1', 'Y') OR deceased_date IS NOT NULL THEN 'Y'
    WHEN pat_deceased_status IN ('0', 'N') THEN 'N'
    WHEN pat_deceased_status IS NULL THEN 'N'
    ELSE 'NS'
END
WHERE {BATCH_KEY_DEMO} >= {pk_lo}
  AND {BATCH_KEY_DEMO} < {pk_hi}
"""


def build_pass7(pk_lo, pk_hi):
    """Pass 7: marital status on patient_demographics — no JOIN."""
    return f"""
UPDATE {PATIENTS_TABLE}
SET pat_marital_status_std = CASE
    WHEN UPPER(TRIM(pat_marital_status)) = 'SEPARATED'   THEN 'Separated'
    WHEN UPPER(TRIM(pat_marital_status)) = 'DIVORCED'    THEN 'Divorced'
    WHEN UPPER(TRIM(pat_marital_status)) = 'MARRIED'     THEN 'Married'
    WHEN UPPER(TRIM(pat_marital_status)) = 'SINGLE'      THEN 'Single'
    WHEN UPPER(TRIM(pat_marital_status)) = 'WIDOWED'     THEN 'Widowed'
    WHEN UPPER(TRIM(pat_marital_status)) = 'COMMON LAW'  THEN 'Common law'
    WHEN UPPER(TRIM(pat_marital_status)) = 'LIVING TOGETHER' THEN 'Living together'
    WHEN UPPER(TRIM(pat_marital_status)) IN ('DOMESTIC PARTNER', 'PARTNER') THEN 'Domestic partner'
    WHEN UPPER(TRIM(pat_marital_status)) = 'REGISTERED DOMESTIC PARTNER' THEN 'Registered domestic partner'
    WHEN UPPER(TRIM(pat_marital_status)) IN ('LEGALLY SEPARATED', 'LEGALLY SEPERATED') THEN 'Legally Separated'
    WHEN UPPER(TRIM(pat_marital_status)) = 'ANNULLED'      THEN 'Annulled'
    WHEN UPPER(TRIM(pat_marital_status)) = 'INTERLOCUTORY' THEN 'Interlocutory'
    WHEN UPPER(TRIM(pat_marital_status)) = 'UNMARRIED'     THEN 'Unmarried'
    WHEN UPPER(TRIM(pat_marital_status)) = 'UNKNOWN'       THEN 'Unknown'
    WHEN UPPER(TRIM(pat_marital_status)) = 'OTHER'         THEN 'Other'
    WHEN UPPER(TRIM(pat_marital_status)) = 'UNREPORTED'    THEN 'Unreported'
    WHEN TRIM(pat_marital_status) = '' OR pat_marital_status IS NULL THEN 'Unknown'
    ELSE 'NS'
END
WHERE {BATCH_KEY_DEMO} >= {pk_lo}
  AND {BATCH_KEY_DEMO} < {pk_hi}
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
    """Add std columns to target tables if not present. Fails fast on metadata lock."""
    std_cols = {
        PATIENTS_TABLE: [
            ("gender_hl7_std",           "VARCHAR(20)"),
            ("gender_CDISC_std",         "VARCHAR(20)"),
            ("gender_OMOP_std",          "VARCHAR(20)"),
            ("gender_OMOP_concept_id",   "VARCHAR(10)"),
            ("pat_ethnicity_code_std",   "VARCHAR(50)"),
            ("pat_ethnicity_std",        "VARCHAR(200)"),
        ],
        PATIENTS_TABLE: [
            ("pat_race_code_std",         "VARCHAR(20)"),
            ("pat_race_std",              "VARCHAR(100)"),
            ("pat_deceased_status_std",   "VARCHAR(5)"),
            ("pat_marital_status_std",        "VARCHAR(50)"),
        ],
    }

    for tbl, cols in std_cols.items():
        print(f"  Checking std columns on {tbl}...")
        ddl_conn = get_connection()
        ddl_cur  = ddl_conn.cursor()
        ddl_cur.execute("SET lock_wait_timeout = 15")
        ddl_error = None
        added = []
        try:
            for col_name, col_type in cols:
                if not _col_exists(ddl_cur, tbl, col_name):
                    print(f"    adding: {col_name} {col_type} ...")
                    ddl_cur.execute(
                        f"ALTER TABLE {tbl} ADD COLUMN {col_name} {col_type} DEFAULT NULL"
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
            print(f"\n  ERROR: Could not add column — metadata lock on {tbl}.")
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
    """
    1. Ensure std columns exist on both target tables.
    2. Pre-materialize race + ethnicity lookups.
    3. Pre-materialize comma-separated ethnicity mapping (JSON_TABLE).
    4. Create per-pass PK staging tables.
    5. Create checkpoint table.
    6. Compute batch ranges per pass.
    Returns dict: checkpoint_key → list of (lo, hi) ranges.
    """
    ensure_std_columns()

    conn = get_connection()
    cur  = conn.cursor()

    # ── 1. semantics.race lookup ───────────────────────────────────────
    # race_lower pre-computed so JOIN b2 ON LOWER(a.pat_race) = b2.race_lower
    # can use the index — avoids per-row LOWER() on the source table.
    print("  Materializing semantics.race lookup...")
    if not _table_exists(cur, STAGING_RACE):
        cur.execute(f"""
            CREATE TABLE {STAGING_RACE} AS
            SELECT Code, race, LOWER(race) AS race_lower
            FROM {SEMANTICS_DB}.race
        """)
        cur.execute(f"ALTER TABLE {STAGING_RACE} ADD INDEX idx_code (Code(30))")
        cur.execute(f"ALTER TABLE {STAGING_RACE} ADD INDEX idx_race_lower (race_lower(100))")
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")
    cur.execute(f"SELECT COUNT(*) FROM {STAGING_RACE}")
    print(f"    {cur.fetchone()[0]:,} rows")

    # ── 2. semantics.ethnicity lookup ──────────────────────────────────
    # ethnicity_lower pre-computed so JOIN b2 ON LOWER(TRIM(a.pat_ethnicity)) = b2.ethnicity_lower
    # can use the index.
    print("  Materializing semantics.ethnicity lookup...")
    if not _table_exists(cur, STAGING_ETH):
        cur.execute(f"""
            CREATE TABLE {STAGING_ETH} AS
            SELECT Code, ethnicity, LOWER(ethnicity) AS ethnicity_lower
            FROM {SEMANTICS_DB}.ethnicity
        """)
        cur.execute(f"ALTER TABLE {STAGING_ETH} ADD INDEX idx_code (Code(30))")
        cur.execute(f"ALTER TABLE {STAGING_ETH} ADD INDEX idx_eth_lower (ethnicity_lower(100))")
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")
    cur.execute(f"SELECT COUNT(*) FROM {STAGING_ETH}")
    print(f"    {cur.fetchone()[0]:,} rows")

    # ── 3. Comma-separated ethnicity mapping (JSON_TABLE pre-aggregation)
    print("  Materializing comma-separated ethnicity mapping...")
    if not _table_exists(cur, STAGING_ETH_MAPPING):
        cur.execute(f"""
            CREATE TABLE {STAGING_ETH_MAPPING} AS
            SELECT
                sc.pat_ethnicity_code,
                CASE
                    WHEN GROUP_CONCAT(DISTINCT e.Code ORDER BY e.Code SEPARATOR ', ') IS NULL
                         AND sc.pat_ethnicity_code LIKE '%ASKU%' THEN 'Unknown'
                    ELSE COALESCE(
                        GROUP_CONCAT(DISTINCT e.Code ORDER BY e.Code SEPARATOR ', '),
                        'NS')
                END AS pat_ethnicity_code_std,
                CASE
                    WHEN GROUP_CONCAT(DISTINCT e.ethnicity SEPARATOR ' / ') IS NULL
                         AND sc.pat_ethnicity_code LIKE '%ASKU%' THEN 'Unknown'
                    ELSE COALESCE(
                        GROUP_CONCAT(DISTINCT e.ethnicity SEPARATOR ' / '),
                        'NS')
                END AS pat_ethnicity_std
            FROM (
                SELECT
                    a.pat_ethnicity,
                    a.pat_ethnicity_code,
                    TRIM(j.code) AS ethnicity_code
                FROM {PATIENTS_TABLE} a,
                JSON_TABLE(
                    CONCAT('["', REPLACE(a.pat_ethnicity_code, ',', '","'), '"]'),
                    '$[*]' COLUMNS (code VARCHAR(50) PATH '$')
                ) j
                WHERE a.pat_ethnicity_code IS NOT NULL
                  AND a.pat_ethnicity_code <> ''
                  AND a.pat_ethnicity_code LIKE '%,%'
            ) sc
            LEFT JOIN {SEMANTICS_DB}.ethnicity e ON TRIM(sc.ethnicity_code) = e.Code
            GROUP BY sc.pat_ethnicity_code
        """)
        cur.execute(f"ALTER TABLE {STAGING_ETH_MAPPING} ADD INDEX idx_code (pat_ethnicity_code(100))")
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")
    cur.execute(f"SELECT COUNT(*) FROM {STAGING_ETH_MAPPING}")
    print(f"    {cur.fetchone()[0]:,} distinct comma-separated codes mapped")

    # ── 4. PK staging tables ──────────────────────────────────────────
    pk_stagings = [
        {
            "name":       STAGING_PK_PAT_ALL,
            "label":      "patients (all rows)",
            "table":      PATIENTS_TABLE,
            "batch_key":  BATCH_KEY_PAT,
            "filter":     f"{BATCH_KEY_PAT} IS NOT NULL",
        },
        {
            "name":       STAGING_PK_DEMO_ALL,
            "label":      "patient_demographics (all rows)",
            "table":      PATIENTS_TABLE,
            "batch_key":  BATCH_KEY_DEMO,
            "filter":     f"{BATCH_KEY_DEMO} IS NOT NULL",
        },
        {
            "name":       STAGING_PK_DEMO_NS,
            "label":      "patient_demographics (race = 'NS')",
            "table":      PATIENTS_TABLE,
            "batch_key":  BATCH_KEY_DEMO,
            "filter":     f"(pat_race_code_std = 'NS' OR pat_race_std = 'NS') AND {BATCH_KEY_DEMO} IS NOT NULL",
        },
        {
            "name":       STAGING_PK_PAT_ETH,
            "label":      "patients (comma ethnicity codes)",
            "table":      PATIENTS_TABLE,
            "batch_key":  BATCH_KEY_PAT,
            "filter":     f"pat_ethnicity_code IS NOT NULL AND pat_ethnicity_code <> '' AND pat_ethnicity_code LIKE '%,%' AND {BATCH_KEY_PAT} IS NOT NULL",
        },
    ]

    for ps in pk_stagings:
        print(f"  Creating PK staging for {ps['label']}...")
        if not _table_exists(cur, ps["name"]):
            cur.execute(f"""
                CREATE TABLE {ps['name']} AS
                SELECT {ps['batch_key']}
                FROM {ps['table']}
                WHERE {ps['filter']}
            """)
            cur.execute(f"ALTER TABLE {ps['name']} ADD INDEX idx_pk ({ps['batch_key']})")
            conn.commit()
            print("    created")
        else:
            print("    already exists, reusing")
        cur.execute(f"SELECT COUNT(*) FROM {ps['name']}")
        total = cur.fetchone()[0]
        print(f"    {total:,} rows")

    # ── 5. Checkpoint table ───────────────────────────────────────────
    print("  Creating checkpoint table...")
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {CHECKPOINT_TABLE} (
            source_key   VARCHAR(150) NOT NULL PRIMARY KEY,
            status       ENUM('running','done','failed') NOT NULL DEFAULT 'running',
            rows_updated BIGINT      DEFAULT 0,
            started_at   DATETIME    DEFAULT NULL,
            completed_at DATETIME    DEFAULT NULL,
            error_msg    TEXT        DEFAULT NULL
        )
    """)
    conn.commit()
    print("    ready")

    # ── 6. Compute batch ranges per pass ────────────────────��─────────
    # Pass 3 (race NS) staging may be 0 rows before Pass 2 runs — handled in main()
    all_ranges = {
        CHECKPOINT_PASS1: _build_ranges(cur, STAGING_PK_PAT_ALL,  BATCH_KEY_PAT)[0],
        CHECKPOINT_PASS2: _build_ranges(cur, STAGING_PK_DEMO_ALL, BATCH_KEY_DEMO)[0],
        CHECKPOINT_PASS3: _build_ranges(cur, STAGING_PK_DEMO_NS,  BATCH_KEY_DEMO)[0],
        CHECKPOINT_PASS4: _build_ranges(cur, STAGING_PK_PAT_ALL,  BATCH_KEY_PAT)[0],
        CHECKPOINT_PASS5: _build_ranges(cur, STAGING_PK_PAT_ETH,  BATCH_KEY_PAT)[0],
        CHECKPOINT_PASS6: _build_ranges(cur, STAGING_PK_DEMO_ALL, BATCH_KEY_DEMO)[0],
        CHECKPOINT_PASS7: _build_ranges(cur, STAGING_PK_DEMO_ALL, BATCH_KEY_DEMO)[0],
    }

    for ck, ranges in all_ranges.items():
        print(f"    {ck.split('.')[-1]}: {len(ranges)} batches")

    cur.close()
    conn.close()
    return all_ranges


# ── Pass 3 staging rebuild (after Pass 2 populates pat_race_code_std) ─

def rebuild_pass3_staging():
    """Rebuild Pass 3 PK staging after Pass 2 has populated pat_race_code_std/pat_race_std.
    Always drops and recreates so rows newly set to 'NS' by Pass 2 are included."""
    conn = get_connection()
    cur  = conn.cursor()
    print("  Rebuilding Pass 3 PK staging (after Pass 2 populated race std columns)...")
    cur.execute(f"DROP TABLE IF EXISTS {STAGING_PK_DEMO_NS}")
    cur.execute(f"""
        CREATE TABLE {STAGING_PK_DEMO_NS} AS
        SELECT {BATCH_KEY_DEMO}
        FROM {PATIENTS_TABLE}
        WHERE (pat_race_code_std = 'NS' OR pat_race_std = 'NS')
          AND {BATCH_KEY_DEMO} IS NOT NULL
    """)
    cur.execute(f"ALTER TABLE {STAGING_PK_DEMO_NS} ADD INDEX idx_pk ({BATCH_KEY_DEMO})")
    conn.commit()
    ranges, total = _build_ranges(cur, STAGING_PK_DEMO_NS, BATCH_KEY_DEMO)
    print(f"    {total:,} rows → {len(ranges)} batches")
    cur.close()
    conn.close()
    return ranges


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
    print(f"  Patients Standardisation UPDATE — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  targets    : {PATIENTS_TABLE}")
    print(f"             : {PATIENTS_TABLE}")
    print(f"  semantics  : {SEMANTICS_DB}.(race | ethnicity)")
    print(f"  batch_size : {BATCH_SIZE:,}")
    print(f"  passes     : 7")
    print(f"{'='*70}\n", flush=True)

    print("  Connecting to database...")
    sys.stdout.flush()
    all_ranges = setup_tables()

    passes = [
        (CHECKPOINT_PASS1, "Pass 1 — gender (patients, all rows)",                  build_pass1),
        (CHECKPOINT_PASS2, "Pass 2 — race lookup (demographics, all rows)",          build_pass2),
        (CHECKPOINT_PASS3, "Pass 3 — race name fallback (demographics, NS rows)",    build_pass3),
        (CHECKPOINT_PASS4, "Pass 4 — ethnicity lookup (patients, all rows)",         build_pass4),
        (CHECKPOINT_PASS5, "Pass 5 — ethnicity CSV split (patients, CSV rows)",      build_pass5),
        (CHECKPOINT_PASS6, "Pass 6 — deceased status (demographics, all rows)",      build_pass6),
        (CHECKPOINT_PASS7, "Pass 7 — marital status (demographics, all rows)",       build_pass7),
    ]

    results = {}
    any_failed = False

    total_batches = sum(len(all_ranges.get(ck, [])) for ck, _, _ in passes)
    with tqdm(total=total_batches, desc="Overall", unit="batch") as pbar:
        for ck, label, build_fn in passes:

            # After Pass 2 finishes, rebuild Pass 3 staging if it was empty
            if ck == CHECKPOINT_PASS3 and not is_done(get_connection(), CHECKPOINT_PASS3):
                ranges = rebuild_pass3_staging()
                all_ranges[CHECKPOINT_PASS3] = ranges
                pbar.total = sum(len(all_ranges.get(c, [])) for c, _, _ in passes)
                pbar.refresh()
            else:
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
        print(f"  [{tag}] {label:<55}  {rows:>10,} rows  ({secs}s)")
        if status.startswith("FAILED"):
            print(f"         {status}")

    print(f"\n  Total rows updated: {total_rows:,}")
    print(f"{'='*70}")

    print(f"\n  Cleanup SQL (run after verifying data):")
    for t in [STAGING_RACE, STAGING_ETH, STAGING_ETH_MAPPING,
              STAGING_PK_PAT_ALL, STAGING_PK_DEMO_ALL,
              STAGING_PK_DEMO_NS, STAGING_PK_PAT_ETH, CHECKPOINT_TABLE]:
        print(f"    DROP TABLE IF EXISTS {t};")

    if any_failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
