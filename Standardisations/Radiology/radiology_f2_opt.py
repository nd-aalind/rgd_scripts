#!/usr/bin/env python3
"""
Optimized batched standardisation UPDATE for: rgd_udm_silver.radiology
(radiology_f2 variant — extended code extraction + brain stem + thoracic spine fixes)

Change TARGET_TABLE at the top to run against any radiology table.

Three sequential passes — each with checkpoint/resume:

  Pass 1 — All rows:
    SET extracted_codes, proc_code_std (combined CPT+HCPCS), modality_std,
        strength_views_std, contrast_type_std
    LEFT JOIN pre-materialized staging.radf2_std_code_lookup (CTE result, indexed on study_name)

  Pass 2 — All rows:
    SET body_part_std, laterality_std, tracer_name_std
    Pure CASE WHEN on study_name — no JOIN needed

  Pass 3 — Unmapped rows only (proc_code_std IS NULL):
    SET probable_cpt_code, probable_cpt_match_count, matched_descriptions
    JOIN pre-materialized staging.radf2_combo_matches (regex CPT lookup keyed on std combo)
    Setup is lazy — called after Pass 2 completes, since combo table reads proc_code_std.

New columns added to target table (11 total):
  extracted_codes          TEXT
  proc_code_std            TEXT
  modality_std             VARCHAR(200)
  strength_views_std       VARCHAR(200)
  contrast_type_std        VARCHAR(50)
  body_part_std            VARCHAR(200)
  laterality_std           VARCHAR(20)
  tracer_name_std          VARCHAR(200)
  probable_cpt_code        TEXT
  probable_cpt_match_count INT
  matched_descriptions     TEXT

Key differences vs radiology_2_opt.py:
  - CTE normalization: 5 REGEXP_REPLACE steps (OR, /, +, -, &) instead of 2
  - CTE code extraction: up to 5 occurrences of 5-digit codes instead of 2
  - Body part: Brain Stem separated from Brain as its own entry
  - Body part: T-SPINE includes '\\bThoracic SPINE\\b', THEN = 'Thoracic Spine'

Pre-materialized lookup (computed ONCE, reused across runs):
  staging.radf2_std_code_lookup  — indexed on study_name(200)

Optimizations applied:
- Code lookup CTE pre-materialized once (not re-scanned per batch)
- Shared PK staging table for both passes
- Server-side boundary sampling (avoids loading all PKs into memory)
- Commit after every batch (frees undo/log space)
- Checkpoint/resume per pass — re-run skips completed passes
- Disabled InnoDB checks per-session for bulk update speed
- Progress bar via tqdm

Usage:
    python radiology_f2_opt.py
"""

import sys
import time
from datetime import datetime
import pymysql
from tqdm import tqdm

# ── Configuration ─────────────────────────────────────────────────────
DB_CONFIG = {
    "host":            "ndai-dev-rds-instance.cwp60ymu4ko0.us-east-1.rds.amazonaws.com",
    "port":            3306,
    "user":            "Aalind",
    "password":        "A@L1nd@123",
    "database":        'rgd_udm_silver',
    "charset":         "utf8mb4",
    "connect_timeout": 30,
    "read_timeout":    21600,
    "write_timeout":   21600,
}

BATCH_SIZE = 50_000

# ── Change this to run against a different radiology table ────────────
TARGET_TABLE = "rgd_udm_silver.radiology"

# ─────────────────────────────────────────────────────────────────────
_TABLE_SUFFIX = TARGET_TABLE.replace(".", "_").replace("-", "_")

STAGING_CODE_LOOKUP  = "staging.radf3_std_code_lookup_n_f2"               # shared across runs
# _uid suffix = tables keyed on udm_inc_id (unique per row); forces rebuild vs old ndid-keyed tables
STAGING_STUDY_KEYS   = f"staging.radf3_std_study_keys_uid_{_TABLE_SUFFIX}"
STAGING_FINAL        = f"staging.radf3_final_uid_{_TABLE_SUFFIX}"
STAGING_PK           = f"staging.radf3_std_pk_uid_{_TABLE_SUFFIX}"
CHECKPOINT_TABLE     = f"staging.etl_checkpoint_radf3_uid_{_TABLE_SUFFIX}"
CHECKPOINT_PASS1     = f"radiologyf3.std.pass1.uid.{_TABLE_SUFFIX}"
CHECKPOINT_PASS2     = f"radiologyf3.std.pass2.uid.{_TABLE_SUFFIX}"
CHECKPOINT_PASS3     = f"radiologyf3.std.pass3.uid.{_TABLE_SUFFIX}"

STAGING_CPT_CLEAN    = "staging.radf3_cpt_clean_n_f1"
STAGING_COMBO        = f"staging.radf3_combo_matches_uid_{_TABLE_SUFFIX}"
STAGING_PK_PASS3     = f"staging.radf2_std_pk4_uid_{_TABLE_SUFFIX}"

# udm_inc_id is the unique per-row key — ndid (patient ID) is NOT unique per radiology row
# and caused many-to-many JOINs when matching TARGET_TABLE rows to STAGING_FINAL
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
    """Add an index, handling TEXT (needs prefix) and short VARCHAR (1089 → no prefix)."""
    import pymysql
    for i, key_spec in enumerate((f"`{col}`({prefix})", f"`{col}`")):
        try:
            cur.execute(f"ALTER TABLE {table} ADD INDEX `{idx_name}` ({key_spec})")
            conn.commit()
            return
        except pymysql.err.OperationalError as e:
            code = e.args[0]
            if code == 1061:   # already exists
                return
            if code == 1089 and i == 0:  # prefix > column length → retry without
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
    """
    Pass 1: SET extracted_codes, proc_code_std, modality_std, strength_views_std, contrast_type_std.
    All values pre-computed in STAGING_FINAL — batch is a pure indexed equality join.
    """
    return f"""
UPDATE {TARGET_TABLE} r
JOIN {STAGING_FINAL} sf ON r.{BATCH_KEY} = sf.{BATCH_KEY}
SET
    r.extracted_codes    = sf.extracted_codes,
    r.proc_code_std      = sf.proc_code_std,
    r.modality_std       = sf.modality_std,
    r.strength_views_std = sf.strength_views_std,
    r.contrast_type_std  = sf.contrast_type_std
WHERE r.{BATCH_KEY} >= {pk_lo}
  AND r.{BATCH_KEY} <  {pk_hi}
"""


def _build_pass1_case_when():
    """Returns the full CASE WHEN SQL for Pass 1 columns — used in STAGING_FINAL creation."""
    return f"""
    l.extracted_codes,
    l.proc_code_std,

    CASE
        WHEN sk.study_name_upper REGEXP '\\\\bPET/CT\\\\b|\\\\bPET CT\\\\b'                                                                                                THEN 'Positron Emission Tomography (PET) / Computed Tomography'
        WHEN sk.study_name_upper REGEXP '\\\\bXR/RF\\\\b'                                                                                                                   THEN 'Digital Radiography / Radio Fluoroscopy'
        WHEN sk.study_name_upper REGEXP '\\\\bUS DOPPLER\\\\b|\\\\bUS DUPLEX\\\\b'                                                                                          THEN 'Ultrasound / Duplex Doppler'
        WHEN sk.study_name_upper REGEXP '\\\\bUS ECHOCARDIOGRAM\\\\b'                                                                                                       THEN 'Ultrasound / Echocardiography'
        WHEN sk.study_name_upper REGEXP '\\\\bXA US\\\\b'                                                                                                                   THEN 'X-Ray Angiography / Ultrasound'
        WHEN sk.study_name_upper REGEXP '\\\\bCT\\\\b|\\\\bCAT\\\\b|\\\\bNCT\\\\b|\\\\bLDCT\\\\b|\\\\bCTA\\\\b|\\\\bCTV\\\\b|\\\\bCTAC\\\\b|\\\\bCTC\\\\b|\\\\bCTP\\\\b'  THEN 'Computed Tomography'
        WHEN sk.study_name_upper REGEXP '\\\\bPET\\\\b'                                                                                                                     THEN 'Positron Emission Tomography (PET)'
        WHEN sk.study_name_upper REGEXP '\\\\bPT\\\\b'
             AND sk.study_name_upper NOT REGEXP '\\\\bPT/|/PT\\\\b|\\\\bPT\\\\s+(PANEL|COAGULATION)|\\\\bPROTIME\\\\b|\\\\bINR\\\\b|\\\\bPTT\\\\b|\\\\bESTAB\\\\s+PT\\\\b|\\\\bNEW\\\\s+PT\\\\b|\\\\bMED\\\\s+DECISION\\\\b'
                                                                                                                                                                              THEN 'Positron Emission Tomography (PET)'
        WHEN sk.study_name_upper REGEXP '\\\\bMRA\\\\b|\\\\bzzMRA\\\\b'                                                                                                     THEN 'Magnetic Resonance Angiography (MA - Retired) / Magnetic Resonance'
        WHEN sk.study_name_upper REGEXP '\\\\bMRI\\\\b|\\\\bMRV\\\\b|\\\\bMRCP\\\\b|\\\\b3TMRI\\\\b|\\\\bTMRI\\\\b|\\\\b3TMRA\\\\b|\\\\bMR\\\\b'                          THEN 'Magnetic Resonance'
        WHEN sk.study_name_upper REGEXP '\\\\bDEXA\\\\b|\\\\bDXA\\\\b|\\\\bDEXASCAN\\\\b'                                                                               THEN 'Bone Densitometry (X-Ray)'
        WHEN sk.study_name_upper REGEXP '\\\\bMAM\\\\b|\\\\bMAMM\\\\b|\\\\bMAMMO\\\\b|\\\\bMMAMMO\\\\b|\\\\bMAMMOGRAM\\\\b|\\\\bMAMMOGRAPHY\\\\b'                         THEN 'Mammography'
        WHEN sk.study_name_upper REGEXP '\\\\bMG\\\\b'
             AND sk.study_name_upper NOT REGEXP '\\\\bMYASTHENIA\\\\b|\\\\bGRAVIS\\\\b|\\\\bEVALUATION\\\\b'                                                               THEN 'Mammography'
        WHEN sk.study_name_upper REGEXP '\\\\bUS\\\\b|\\\\bULTRASOUND\\\\b|\\\\bUSV\\\\b|\\\\bBI US\\\\b|\\\\bOB US\\\\b'                                                  THEN 'Ultrasound'
        WHEN sk.study_name_upper REGEXP '\\\\bXA\\\\b'
             AND sk.study_name_upper NOT REGEXP '\\\\bANTI-XA\\\\b|\\\\bANTI XA\\\\b|\\\\bHEPARIN\\\\b'                                                                   THEN 'X-Ray Angiography'
        WHEN sk.study_name_upper REGEXP '\\\\bANG\\\\b|\\\\bANGIO\\\\b'                                                                                                    THEN 'X-Ray Angiography'
        WHEN sk.study_name_upper REGEXP '\\\\bCR\\\\b'                                                                                                                     THEN 'Computed Radiography'
        WHEN sk.study_name_upper REGEXP '\\\\bDX\\\\b|\\\\bXR\\\\b|\\\\bX-RAY\\\\b|\\\\bXRAY\\\\b|\\\\bXRY\\\\b'                                                          THEN 'Digital Radiography'
        WHEN sk.study_name_upper REGEXP '\\\\bDR\\\\b'
             AND sk.study_name_upper NOT REGEXP '\\\\bHLA\\\\b|\\\\bTYPING\\\\b|\\\\bDQ\\\\b|\\\\bDP\\\\b'                                                                 THEN 'Digital Radiography'
        WHEN sk.study_name_upper REGEXP '\\\\bRF\\\\b'
             AND sk.study_name_upper NOT REGEXP '\\\\bRHEUMATOID\\\\b|\\\\bFACTOR\\\\b|\\\\bANTI-CCP\\\\b|\\\\bANA\\\\b|\\\\bSERUM\\\\b|\\\\bTITER\\\\b'                  THEN 'Radio Fluoroscopy'
        WHEN sk.study_name_upper REGEXP '\\\\bECG[0-9]*\\\\b|\\\\bEKG[0-9]*\\\\b|\\\\bECG\\\\b|\\\\bEKG\\\\b|\\\\bELECTROCARDIOGRAM\\\\b|\\\\bELECTROCARDIOGRAPH\\\\b'   THEN 'Electrocardiography'
        WHEN sk.study_name_upper REGEXP '\\\\bEEG\\\\b|\\\\bELECTROCEPHANLOGRAM\\\\b|\\\\bELECTROENCEPHALOGRAM\\\\b'                                                     THEN 'Electroencephalography'
        WHEN sk.study_name_upper REGEXP '\\\\bFL\\\\b|\\\\bFLUORO\\\\b|\\\\bFLU\\\\b|\\\\bFLUOROSCOPY\\\\b|\\\\bFLUOROSCOPIC\\\\b'                                       THEN 'Radio Fluoroscopy'
        WHEN sk.study_name_upper REGEXP '\\\\bFS\\\\b'                                                                                                                     THEN 'Fundoscopy (FS - Retired) / Ophthalmic Photography'
        WHEN sk.study_name_upper REGEXP '\\\\bNM\\\\b'                                                                                                                     THEN 'Nuclear Medicine'
        WHEN sk.study_name_upper REGEXP '\\\\bECHO\\\\b|\\\\bECHOCARDIOGRAM\\\\b|\\\\bECHOCARDIOGRAPHY\\\\b'                                                             THEN 'Echocardiography (EC - Retired) / Ultrasound'
        WHEN sk.study_name_upper REGEXP '\\\\bENDOSCOPY\\\\b'                                                                                                              THEN 'Endoscopy'
        WHEN sk.study_name_upper REGEXP '\\\\bCD\\\\b'                                                                                                                     THEN 'Color Flow Doppler (CD - Retired) / Ultrasound'
        WHEN sk.study_name_upper REGEXP '\\\\bTCD\\\\b|\\\\bDUPLEX\\\\b|\\\\bDOPPLER\\\\b'                                                                                THEN 'Duplex Doppler (DD - Retired) / Ultrasound'
        WHEN sk.study_name_upper REGEXP '\\\\bAUDIO\\\\b|\\\\bAUDIOMETRY\\\\b|\\\\bAUDITORY\\\\b|\\\\bHEARING\\\\b|\\\\bAUDIOGRAM\\\\b|\\\\bACOUSTIC\\\\b'               THEN 'Audio'
        WHEN sk.study_name_upper REGEXP '\\\\bRP\\\\b'                                                                                                                     THEN 'Radiotherapy Plan'
        WHEN sk.study_name_upper REGEXP '\\\\bRT\\\\b'
             AND sk.study_name_upper NOT REGEXP '\\\\bCREATININE\\\\b|\\\\bCREAT\\\\b|\\\\bRENAL\\\\b|\\\\bKIDNEY\\\\b'                                                   THEN 'Radiographic Imaging (RG) / Interventional Radiology'
        WHEN sk.study_name_upper REGEXP '\\\\bRAD\\\\b|\\\\bIR\\\\b|\\\\bINTERVENTIONAL RADIOLOGY\\\\b'                                                                   THEN 'Radiographic Imaging (RG) / Interventional Radiology'
        WHEN sk.study_name_upper REGEXP '\\\\bSPECT\\\\b'                                                                                                                  THEN 'Single-Photon Emission Computed Tomography (ST - Retired) / Nuclear Medicine'
        WHEN sk.study_name_upper REGEXP '\\\\bBX\\\\b|\\\\bBIOPSY\\\\b|\\\\bVL\\\\b|\\\\bOHS\\\\b|\\\\bI-123\\\\b|\\\\b1-131\\\\b|\\\\bMPI\\\\b'                         THEN 'Other'
        ELSE NULL
    END AS modality_std,

    CASE
        WHEN sk.study_name_upper REGEXP '[0-9]+\\\\.?[0-9]*\\\\s*T\\\\b'
            AND sk.study_name_upper REGEXP 'W[/\\\\s]?WO|W\\\\s?&\\\\s?W/?O|W\\\\s?AND\\\\s?W/?O|WITHOUT/WITH|WITH/WITHOUT|WWO|WO/W|\\\\bWO\\\\b|\\\\bW/O\\\\b|\\\\bWITHOUT\\\\b|\\\\bWO CON\\\\b|\\\\bNCON\\\\b|\\\\bNO CON\\\\b|\\\\bWO C\\\\b|\\\\bWO CONTRAST\\\\b|\\\\bW CON\\\\b|\\\\bW CONTRAST\\\\b|\\\\bWITH CONTRAST\\\\b|\\\\bW C\\\\b|\\\\bW/\\\\b|\\\\bCON\\\\b'
            THEN CONCAT(REGEXP_SUBSTR(sk.study_name_upper, '[0-9]+\\\\.?[0-9]*(?=\\\\s*T\\\\b)'), 'T')
        WHEN sk.study_name_upper REGEXP '[0-9]+-[0-9]+\\\\s*V\\\\b'
            THEN CONCAT(REGEXP_SUBSTR(sk.study_name_upper, '[0-9]+-[0-9]+(?=\\\\s*V\\\\b)'), ' Views')
        WHEN sk.study_name_upper REGEXP '[0-9]+\\\\s*V\\\\b'
            THEN CONCAT(REGEXP_SUBSTR(sk.study_name_upper, '[0-9]+(?=\\\\s*V\\\\b)'), ' Views')
        WHEN sk.study_name_upper REGEXP '[0-9]+-[0-9]+\\\\s*VIEW.*(\\\\+|AND).*[0-9]+-[0-9]+\\\\s*VIEW|[0-9]+\\\\s*VIEWS?.*\\\\+.*[0-9]+\\\\s*VIEW'
            THEN CONCAT(REGEXP_SUBSTR(sk.study_name_upper, '[0-9]+-[0-9]+|[0-9]+(?=\\\\s*VIEWS?)'), ' Views + ', REGEXP_SUBSTR(sk.study_name_upper, '[0-9]+(?=\\\\s*VIEW)', 1, 2), ' View')
        WHEN sk.study_name_upper REGEXP '[><=]{{1,2}}\\\\s*[0-9]+\\\\s*VIEWS?'
            THEN CONCAT(REGEXP_SUBSTR(sk.study_name_upper, '[><=]{{1,2}}'), REGEXP_SUBSTR(sk.study_name_upper, '[0-9]+'), ' Views')
        WHEN sk.study_name_upper REGEXP '[0-9]+-[0-9]+\\\\s*VIEW'
            THEN CONCAT(REGEXP_SUBSTR(sk.study_name_upper, '[0-9]+-[0-9]+'), ' Views')
        WHEN sk.study_name_upper REGEXP '(MIN|MINIMUM)(\\\\s+OF)?\\\\s*(ONE|TWO|THREE|FOUR|FIVE|SIX|[0-9]+)\\\\s*(V\\\\b|VWS|VIEW|VIEWS)'
            THEN CONCAT('Min ', COALESCE(REGEXP_SUBSTR(sk.study_name_upper, '[0-9]+(?=\\\\s*(V\\\\b|VWS|VIEW|VIEWS))'), CASE WHEN sk.study_name_upper REGEXP 'ONE\\\\s*(V|VIEW|VIEWS)' THEN '1' WHEN sk.study_name_upper REGEXP 'TWO\\\\s*(V|VIEW|VIEWS)' THEN '2' WHEN sk.study_name_upper REGEXP 'THREE\\\\s*(V|VIEW|VIEWS)' THEN '3' WHEN sk.study_name_upper REGEXP 'FOUR\\\\s*(V|VIEW|VIEWS)' THEN '4' END), ' Views')
        WHEN sk.study_name_upper REGEXP '[0-9]+\\\\s*[\\\\+]\\\\s*VIEWS?|[0-9]+\\\\s*PLUS\\\\s*VIEWS?'
            THEN CONCAT(REGEXP_SUBSTR(sk.study_name_upper, '[0-9]+'), '+ Views')
        WHEN sk.study_name_upper REGEXP '[0-9]+\\\\s*OR\\\\s*MORE\\\\s*VIEWS?'
            THEN CONCAT(REGEXP_SUBSTR(sk.study_name_upper, '[0-9]+'), ' or More Views')
        WHEN sk.study_name_upper REGEXP '[0-9]+\\\\s*OR\\\\s*[0-9]+\\\\s*VIEWS?'
            THEN CONCAT(REGEXP_SUBSTR(sk.study_name_upper, '[0-9]+'), ' or ', REGEXP_SUBSTR(sk.study_name_upper, '[0-9]+', 1, 2), ' Views')
        WHEN sk.study_name_upper REGEXP '\\\\b(ONE|TWO|THREE|FOUR|FIVE|SIX)\\\\s*VIEWS?\\\\b'
            THEN CONCAT(CASE WHEN sk.study_name_upper REGEXP '\\\\bONE\\\\s*VIEWS?' THEN '1' WHEN sk.study_name_upper REGEXP '\\\\bTWO\\\\s*VIEWS?' THEN '2' WHEN sk.study_name_upper REGEXP '\\\\bTHREE\\\\s*VIEWS?' THEN '3' WHEN sk.study_name_upper REGEXP '\\\\bFOUR\\\\s*VIEWS?' THEN '4' WHEN sk.study_name_upper REGEXP '\\\\bFIVE\\\\s*VIEWS?' THEN '5' WHEN sk.study_name_upper REGEXP '\\\\bSIX\\\\s*VIEWS?' THEN '6' END, ' Views')
        WHEN sk.study_name_upper REGEXP '(LESS\\\\s*THAN|<)\\\\s*[0-9]+\\\\s*(V\\\\b|VIEW|VIEWS)'
            THEN CONCAT('Less Than ', REGEXP_SUBSTR(sk.study_name_upper, '[0-9]+(?=\\\\s*(V\\\\b|VIEW|VIEWS))'), ' Views')
        WHEN sk.study_name_upper REGEXP '[0-9]+\\\\+?\\\\s*VIEWS?'
            THEN CONCAT(REGEXP_SUBSTR(sk.study_name_upper, '[0-9]+'), ' Views')
        ELSE NULL
    END AS strength_views_std,

    CASE
        WHEN sk.study_name_upper REGEXP 'W[/\\\\s]?WO|W\\\\s?&\\\\s?W/?O|W\\\\s?AND\\\\s?W/?O|W\\\\s?OR\\\\s?W/?O|WITH\\\\s?AND\\\\s?W/?O|WO\\\\+W|W\\\\+W/?O|WITHOUT/WITH|WITH/WITHOUT|W\\\\s?AND\\\\s?WOW|WO,\\\\s?W|W,\\\\s?WO|WWO|W/W/O|WO/W|W/&W/O|W AND OR WO|W WO|W\\\\s?W/?O|WO\\\\s?W'
            THEN 'With and Without Contrast'
        WHEN sk.study_name_upper REGEXP '\\\\bWO\\\\b|\\\\bW/O\\\\b|\\\\bWITHOUT\\\\b|\\\\bWO CON\\\\b|\\\\bW/O CONTRAST\\\\b|\\\\bNCON\\\\b|\\\\bNO CON\\\\b|\\\\bWO C\\\\b|\\\\bWO CONTRAST\\\\b'
            THEN 'Without Contrast'
        WHEN sk.study_name_upper REGEXP '\\\\bW CON\\\\b|\\\\bW CONTRAST\\\\b|\\\\bWITH CONTRAST\\\\b|\\\\bW C\\\\b|\\\\bW/\\\\b|\\\\bCON\\\\b'
            THEN 'With Contrast'
        ELSE NULL
    END AS contrast_type_std"""


def _build_pass2_case_when():
    """Returns the full CASE WHEN SQL for Pass 2 columns — used in STAGING_FINAL creation."""
    return f"""
    CASE
        -- MULTI BODY PARTS FIRST
        WHEN sk.study_name_upper REGEXP 'CHEST.*(ABDOMEN|ABD).*(PELVIS|PELV)|CHEST/ABD/PELVIS|CHEST ABDOMEN PELVIS|CHEST\\\\+ABD.*PELVIS'                     THEN 'Chest, Abdomen, Pelvis'
        WHEN sk.study_name_upper REGEXP 'CHEST.*(ABDOMEN|ABD)|CHEST\\\\+ABD'                                                                                  THEN 'Chest, Abdomen'
        WHEN sk.study_name_upper REGEXP 'CHEST.*THORAX|CHEST/THORAX'                                                                                           THEN 'Chest, Thorax'
        WHEN sk.study_name_upper REGEXP '(ABDOMEN|ABD).*(PELVIS|PELV)|(PELVIS|PELV).*(ABDOMEN|ABD)'                                                            THEN 'Abdomen, Pelvis'
        WHEN sk.study_name_upper REGEXP 'HEAD.*NECK|NECK.*HEAD|NECK/HEAD'                                                                                      THEN 'Head, Neck'
        WHEN sk.study_name_upper REGEXP 'ORB.*FAC.*NCK|ORBIT.*FACE.*NECK|ORBIT/FACE/NK'                                                                       THEN 'Orbit, Face, Neck'
        WHEN sk.study_name_upper REGEXP 'ORBIT.*SELLA|ORBIT\\\\+SELLA|ORBIT SELLA POSS|ORBIT\\\\+SELLA\\\\+PF'                                               THEN 'Orbit, Sella'
        WHEN sk.study_name_upper REGEXP 'CERVICOTHORACOLUMBAR|CERVICO.*THORACO.*LUMBAR'                                                                        THEN 'Thoracic Spine, Lumbar Spine'
        WHEN sk.study_name_upper REGEXP 'THORACO.?LUMBAR|THORACOLUMBAR|THOR.*LUM[B]|THOR\\\\+LU[MB]'                                                          THEN 'Thoracic Spine, Lumbar Spine'
        WHEN sk.study_name_upper REGEXP 'LUMBOSACRAL PLEXUS'                                                                                                   THEN 'Lumbar Spine, Sacrum'
        WHEN sk.study_name_upper REGEXP 'LUMBO.?SACRAL|LUMB.?SACR|LUMBO SACRAL|LUMBOSACRAL'                                                                   THEN 'Lumbar Spine, Sacrum'
        WHEN sk.study_name_upper REGEXP 'SACRUM.*(AND|\\\\+|/).?COCCYX|SACRUM COCCYX|SACRUM/COCCYX'                                                           THEN 'Sacrum, Coccyx'
        WHEN sk.study_name_upper REGEXP 'FACIAL.*SINUS|SINUS.*FACIAL|FACIAL/SINUS|MAX.*FAC.*SIN|MAXILLOFACIAL|MAXIOFACIAL|MAXFACIAL|MAXILLA|SINUS FACIAL|MAX/FAC/SIN|MAXFACIAL BONES' THEN 'Facial Bones, Sinuses'
        WHEN sk.study_name_upper REGEXP 'TIB.?FIB|TIBIA.?FIBUL|TIBIA AND FIBULA|TIBIA/FIBULA|TIB & FIB|TIB\\\\+FIBULA|TIBIA\\\\+FIBULA|TM JOINTS|TIB\\\\s*\\\\\\\\T\\\\\\\\\\\\s*FIB|TIB\\\\s+T\\\\s+FIB' THEN 'Tibia, Fibula'
        WHEN sk.study_name_upper REGEXP 'FOREARM.*RADIUS|FOREARM.*ULNA|RADIUS.*ULNA|FOREARM/RADIUS'                                                            THEN 'Forearm, Radius, Ulna'
        WHEN sk.study_name_upper REGEXP 'CAROTID.*NECK|CAROTID/NECK|VASC CAROTID|CAROTIDS'                                                                    THEN 'Carotid, Neck'
        WHEN sk.study_name_upper REGEXP 'THYROID.*NECK|THY.*NECK|THYROID/NECK'                                                                                 THEN 'Thyroid, Neck'
        WHEN sk.study_name_upper REGEXP 'LIVER.*GALLBLADDER.*PANCREAS|LIVER GALLBLADDER PANCREAS'                                                              THEN 'Liver, Gallbladder, Pancreas'
        WHEN sk.study_name_upper REGEXP 'ILIUM.*STERNUM.*RIB|ILIUM STERNUM RIB'                                                                                THEN 'Ilium, Sternum, Rib'
        WHEN sk.study_name_upper REGEXP 'AC JOINTS|ACROMIOCLAVICULAR JOINTS|STERNOCLAVIC|STERNOCLAVICULAR'                                                     THEN 'Acromioclavicular Joints'
        WHEN sk.study_name_upper REGEXP '\\\\bTMJ\\\\b|TEMPOROMANDIBULAR|TEMPOROMANDIBULAR JOINT|TMJ BILATERAL'                                               THEN 'Temporomandibular Joint'
        WHEN sk.study_name_upper REGEXP 'VASC EXT LOWR|VASC EXTREMITY LOWER|LOWER EXTREMITY VENOUS|LOWER EXT VENOUS|LOWER EXTREMITY ARTERIES|LOWER EXT ARTERIAL|ARTERIAL LOW.*EXT|ARTERIAL LOWER EXT|ARTERIAL LOWER EXTREMITY|LOWER EXTREMITY ARTERI' THEN 'Lower Extremity (Vascular)'
        WHEN sk.study_name_upper REGEXP 'UPPER EXTREMITY VENOUS|UPPER EXT.*ARTERIAL|ARTERIAL UPPER EXT|ARTERIAL UPPER EXTREMITY|UPPER EXTREMITY ARTERIAL|LT UPPER VENOUS|UPPER OR LOWER EXT ARTERIAL' THEN 'Upper Extremity (Vascular)'
        WHEN sk.study_name_upper REGEXP 'VASC TRANSCRANIAL|TRANS CRANIAL|TRANSCRANIAL|VASC JUGULAR|JUGULAR.*SUBCLAVIAN'                                        THEN 'Transcranial (Vascular)'
        WHEN sk.study_name_upper REGEXP 'CEREBRAL ARTERIES|EXTRACRANIAL ARTERIES'                                                                              THEN 'Cerebral Arteries'
        WHEN sk.study_name_upper REGEXP '\\\\bLOWER EXTREMIT|\\\\bLOWER EXT\\\\b|\\\\bLOWER EXTR\\\\b|\\\\bLWR EXT\\\\b|\\\\bLE\\\\b|\\\\bLOW EXT\\\\b|\\\\bLEFT LOWER EXTREMITY\\\\b|\\\\bRIGHT LOWER EXTREMITY\\\\b|LOWER EXT.*NOT.*JNT|LOWER EXT.*NON|LWR EXT NOT JT|LOWER LEG|LOWER BACK|EXTREMITY LOWER|EXTREMITY.*LOWER' THEN 'Lower Extremity'
        WHEN sk.study_name_upper REGEXP '\\\\bUPPER EXTREMIT|\\\\bUPPER EXT\\\\b|\\\\bUPR.*EXT\\\\b|\\\\bUPR/LXTR\\\\b|UP EXT JT|LEFT EXTREMITY.*UPPER|UPPER EXT.*JOINT|UPPER EXT NON JOINT|UPPER EXTREMITY.*MUSCULO|EXTREMITY UPPER|EXTREMITY.*UPPER|LEFT EXTREMITY JOINT UPPER|RIGHT EXTREMITY JOINT LOWER' THEN 'Upper Extremity'
        WHEN sk.study_name_upper REGEXP 'BRACHIAL PLEXUS|RIGHT BRACHIAL PLEXUS|\\\\bBRACHPLEX\\\\b'                                                           THEN 'Brachial Plexus'
        WHEN sk.study_name_upper REGEXP 'SPINAL CANAL|SPINAL CORD|THORACIC SPINAL CORD|SPINAL CORD DORSAL'                                                     THEN 'Spinal Canal'
        WHEN sk.study_name_upper REGEXP 'SACROILIAC|SACROILIAC JOINT|SACROILIAC JNTS|SI JOINT'                                                                 THEN 'Sacroiliac Joint'
        -- SINGLE BODY PARTS
        WHEN sk.study_name_upper REGEXP '\\\\bABDOMEN\\\\b|\\\\bABD\\\\b|\\\\bABDOMINAL\\\\b'                                                                THEN 'Abdomen'
        WHEN sk.study_name_upper REGEXP '\\\\bANKLE\\\\b|\\\\bANK\\\\b'                                                                                       THEN 'Ankle'
        WHEN sk.study_name_upper REGEXP '\\\\bAORTA\\\\b|\\\\bTHORACIC AORTA\\\\b'                                                                            THEN 'Aorta'
        WHEN sk.study_name_upper REGEXP '\\\\bARTERY\\\\b|\\\\bARTERIAL\\\\b'                                                                                 THEN 'Arterial'
        WHEN sk.study_name_upper REGEXP 'AUDITORY CANAL'                                                                                                        THEN 'Auditory Canal'
        WHEN sk.study_name_upper REGEXP 'AXIAL SKELETON'                                                                                                        THEN 'Axial Skeleton'
        WHEN sk.study_name_upper REGEXP '\\\\bBONE\\\\b'                                                                                                       THEN 'Bone'
        -- Brain Stem before Brain (more specific first)
        WHEN sk.study_name_upper REGEXP '\\\\bBRAIN\\\\s*STEM\\\\b|\\\\bBRN\\\\s*STEM\\\\b|\\\\bBRAINSTEM\\\\b'                                              THEN 'Brain Stem'
        WHEN sk.study_name_upper REGEXP '\\\\bBRAIN\\\\b|\\\\bBRIAN\\\\b|\\\\bBRIN\\\\b'                                                                     THEN 'Brain'
        WHEN sk.study_name_upper REGEXP '\\\\bIAC\\\\b|\\\\bIACS\\\\b|INTERNAL AUDITORY|AUDITORY MEATUS|AUDITORY CANAL MRI'                                   THEN 'Internal Auditory Canal'
        -- CN V — Trigeminal nerve
        WHEN sk.study_name_upper REGEXP '\\\\bTRIGEMINAL\\\\b|\\\\b5TH.*NERVE\\\\b|\\\\bCN\\\\s*V\\\\b|\\\\bCRANIAL NERVE\\\\s*5\\\\b'                       THEN 'Trigeminal Nerve'
        -- CN VII — Facial nerve
        WHEN sk.study_name_upper REGEXP '\\\\b7TH.*NERVE\\\\b|\\\\bCN\\\\s*VII\\\\b|\\\\bCRANIAL NERVE\\\\s*7\\\\b|\\\\bFACIAL NERVE\\\\b'                   THEN 'Facial Nerve'
        -- CN VIII — Acoustic nerve
        WHEN sk.study_name_upper REGEXP '\\\\b8TH.*NERVE\\\\b|\\\\bCN\\\\s*VIII\\\\b|\\\\bCRANIAL NERVE\\\\s*8\\\\b|\\\\bACOUSTIC NEUROMA\\\\b|\\\\bVESTIBULOCOCHLEAR\\\\b' THEN 'Acoustic Nerve'
        WHEN sk.study_name_upper REGEXP '\\\\bCARDIAC\\\\b|\\\\bCARDIOVASC\\\\b'                                                                              THEN 'Heart'
        WHEN sk.study_name_upper REGEXP '\\\\bSACRAL PLEXUS\\\\b'                                                                                              THEN 'Sacrum'
        WHEN sk.study_name_upper REGEXP 'MUSCLE.*\\\\bUE\\\\b|MUSCLE.*UPPER|MUSCLE.*\\\\bUPR\\\\b'                                                            THEN 'Upper Extremity'
        WHEN sk.study_name_upper REGEXP 'MUSCLE.*\\\\bLE\\\\b|MUSCLE.*LOWER|MUSCLE.*\\\\bLWR\\\\b'                                                            THEN 'Lower Extremity'
        WHEN sk.study_name_upper REGEXP 'VISUAL EVOKED|EVOKED.*VISUAL|EVOKED.*POTENTIAL.*VIS'                                                                  THEN 'Eye / Orbit'
        -- L-SPINE, T-SPINE shorthand before generic SPINE
        WHEN sk.study_name_upper REGEXP '\\\\bL-SPINE\\\\b|\\\\bL SPINE\\\\b|\\\\bLS-SPINE\\\\b|\\\\bL-S SPINE\\\\b|\\\\bLS SPINE\\\\b'                      THEN 'Lumbar Spine'
        WHEN sk.study_name_upper REGEXP '\\\\bT-SPINE\\\\b|\\\\bT SPINE\\\\b|\\\\bTHORACIC SPINE\\\\b'                                                        THEN 'Thoracic Spine'
        WHEN sk.study_name_upper REGEXP '\\\\bBREAST\\\\b|\\\\bBREASTS\\\\b'                                                                                  THEN 'Breast'
        WHEN sk.study_name_upper REGEXP '\\\\bCALF\\\\b'                                                                                                       THEN 'Calf'
        WHEN sk.study_name_upper REGEXP '\\\\bCAROTID\\\\b|\\\\bCAROTIDS\\\\b'                                                                                THEN 'Carotid'
        WHEN sk.study_name_upper REGEXP '\\\\bCERVICAL SPINE\\\\b|\\\\bSPINE CERVICAL\\\\b|\\\\bC-SPINE\\\\b'                                                 THEN 'Cervical Spine'
        WHEN sk.study_name_upper REGEXP '\\\\bCERVICAL\\\\b'                                                                                                   THEN 'Cervical'
        WHEN sk.study_name_upper REGEXP '\\\\bCHEST\\\\b|\\\\bPA CHEST\\\\b|\\\\bCHEST PA\\\\b|\\\\bTHORAX\\\\b|\\\\bRIBS\\\\b|\\\\bPNEUMOTHORAX\\\\b|\\\\bTHORACENTESIS\\\\b' THEN 'Chest'
        WHEN sk.study_name_upper REGEXP '\\\\bCLAVICLE\\\\b'                                                                                                   THEN 'Clavicle'
        WHEN sk.study_name_upper REGEXP '\\\\bCOCCYX\\\\b'                                                                                                     THEN 'Coccyx'
        WHEN sk.study_name_upper REGEXP '\\\\bCOLON\\\\b|\\\\bLARGE INTESTINE\\\\b'                                                                           THEN 'Colon'
        WHEN sk.study_name_upper REGEXP 'CRANIAL NERVE'                                                                                                         THEN 'Cranial Nerve'
        WHEN sk.study_name_upper REGEXP '\\\\bEAR\\\\b'                                                                                                        THEN 'Ear'
        WHEN sk.study_name_upper REGEXP '\\\\bELBOW\\\\b|\\\\bELB\\\\b'                                                                                       THEN 'Elbow'
        WHEN sk.study_name_upper REGEXP '\\\\bESOPHAGUS\\\\b|\\\\bTRANSESOPHAGEAL\\\\b'                                                                       THEN 'Esophagus'
        WHEN sk.study_name_upper REGEXP '\\\\bEXTRACRANIAL\\\\b|\\\\bEXTRACRAN\\\\b'                                                                          THEN 'Extracranial'
        WHEN sk.study_name_upper REGEXP '\\\\bEYE\\\\b|\\\\bORBIT\\\\b|\\\\bORBITS\\\\b|\\\\bORB\\\\b|OPTIC NERVE'                                           THEN 'Eye / Orbit'
        WHEN sk.study_name_upper REGEXP '\\\\bFACE\\\\b|\\\\bFACIAL\\\\b|\\\\bFACIAL BONES\\\\b'                                                             THEN 'Face'
        WHEN sk.study_name_upper REGEXP '\\\\bFEMUR\\\\b|\\\\bFEM\\\\b|\\\\bLATERAL FEMORAL\\\\b'                                                            THEN 'Femur'
        WHEN sk.study_name_upper REGEXP '\\\\bFINGERS\\\\b|\\\\bFINGER\\\\b'                                                                                  THEN 'Fingers'
        WHEN sk.study_name_upper REGEXP '\\\\bFOOT\\\\b|\\\\bFEET\\\\b|\\\\bFT\\\\b|\\\\bHEEL\\\\b'                                                         THEN 'Foot'
        WHEN sk.study_name_upper REGEXP '\\\\bFOREARM\\\\b|\\\\bFORE\\\\b'                                                                                    THEN 'Forearm'
        WHEN sk.study_name_upper REGEXP '\\\\bGALLBLADDER\\\\b'                                                                                                THEN 'Gallbladder'
        WHEN sk.study_name_upper REGEXP '\\\\bGI\\\\b|\\\\bUGI\\\\b|\\\\bGASTRIC\\\\b|\\\\bGASTROINTESTINAL\\\\b|\\\\bSMALL INTESTINE\\\\b|\\\\bSTOMACH\\\\b|\\\\bBOWEL\\\\b' THEN 'Gastric / GI'
        WHEN sk.study_name_upper REGEXP 'GREATER OCCIPITAL|OCCIPITAL'                                                                                          THEN 'Occipital'
        WHEN sk.study_name_upper REGEXP '\\\\bGROIN\\\\b|\\\\bILIOINGUINAL\\\\b'                                                                              THEN 'Groin'
        WHEN sk.study_name_upper REGEXP '\\\\bHAND\\\\b|\\\\bHANDS\\\\b'                                                                                      THEN 'Hand'
        WHEN sk.study_name_upper REGEXP '\\\\bHEAD\\\\b|\\\\bORBITS\\\\b'                                                                                     THEN 'Head'
        WHEN sk.study_name_upper REGEXP '\\\\bHEART\\\\b|\\\\bTRANSTHORACIC\\\\b'                                                                             THEN 'Heart'
        WHEN sk.study_name_upper REGEXP '\\\\bHIP\\\\b|\\\\bHIPS\\\\b'                                                                                        THEN 'Hip'
        WHEN sk.study_name_upper REGEXP '\\\\bHUMERUS\\\\b|\\\\bHUM\\\\b|\\\\bUPPER ARM\\\\b'                                                                THEN 'Humerus / Upper Arm'
        WHEN sk.study_name_upper REGEXP '\\\\bINTRACRANIAL\\\\b|\\\\bINTRACRAN\\\\b'                                                                          THEN 'Intracranial'
        WHEN sk.study_name_upper REGEXP '\\\\bKIDNEY\\\\b|\\\\bKIDNEYS\\\\b|\\\\bRENAL\\\\b'                                                                 THEN 'Kidney'
        WHEN sk.study_name_upper REGEXP '\\\\bKNEE\\\\b|\\\\bKNEES\\\\b|\\\\bKN\\\\b'                                                                        THEN 'Knee'
        WHEN sk.study_name_upper REGEXP '\\\\bLEG\\\\b|\\\\bLOWER LEG\\\\b'                                                                                   THEN 'Leg'
        WHEN sk.study_name_upper REGEXP '\\\\bLIVER\\\\b'                                                                                                      THEN 'Liver'
        WHEN sk.study_name_upper REGEXP 'LUMBAR PLEXUS|LUMPLEX'                                                                                                THEN 'Lumbar Plexus'
        WHEN sk.study_name_upper REGEXP '\\\\bLUMBAR SPINE\\\\b|\\\\bSPINE LUMBAR\\\\b|\\\\bLUMBOSACRAL\\\\b|\\\\bLUMOSACRAL\\\\b'                          THEN 'Lumbar Spine'
        WHEN sk.study_name_upper REGEXP '\\\\bLUMBAR\\\\b'                                                                                                     THEN 'Lumbar'
        WHEN sk.study_name_upper REGEXP '\\\\bLUNG\\\\b'                                                                                                       THEN 'Lung'
        WHEN sk.study_name_upper REGEXP 'LYMPH NODE'                                                                                                            THEN 'Lymph Node'
        WHEN sk.study_name_upper REGEXP '\\\\bMANDIBLE\\\\b'                                                                                                   THEN 'Mandible'
        WHEN sk.study_name_upper REGEXP '\\\\bMASTOIDS\\\\b|\\\\bMASTOID\\\\b'                                                                                THEN 'Mastoids'
        WHEN sk.study_name_upper REGEXP '\\\\bNECK\\\\b|\\\\bNECK SOFT TISSUE\\\\b|\\\\bTHROAT\\\\b'                                                         THEN 'Neck'
        WHEN sk.study_name_upper REGEXP '\\\\bPANCREAS\\\\b'                                                                                                   THEN 'Pancreas'
        WHEN sk.study_name_upper REGEXP '\\\\bPARATHYROID\\\\b'                                                                                                THEN 'Parathyroid'
        WHEN sk.study_name_upper REGEXP '\\\\bPELVIS\\\\b|\\\\bPELVIC\\\\b'                                                                                   THEN 'Pelvis'
        WHEN sk.study_name_upper REGEXP '\\\\bPITUITARY\\\\b|\\\\bPITUITARY GLAND\\\\b|\\\\bSELLA TURCICA\\\\b|\\\\bSELLA\\\\b'                              THEN 'Pituitary'
        WHEN sk.study_name_upper REGEXP '\\\\bPROSTATE\\\\b|\\\\bRECTAL\\\\b'                                                                                 THEN 'Prostate / Rectal'
        WHEN sk.study_name_upper REGEXP '\\\\bRETROPERITONEUM\\\\b'                                                                                            THEN 'Retroperitoneum'
        WHEN sk.study_name_upper REGEXP '\\\\bSACRUM\\\\b'                                                                                                     THEN 'Sacrum'
        WHEN sk.study_name_upper REGEXP '\\\\bSCAPULA\\\\b|\\\\bSCAP\\\\b'                                                                                    THEN 'Scapula'
        WHEN sk.study_name_upper REGEXP '\\\\bSCOLIOSIS\\\\b'                                                                                                  THEN 'Scoliosis'
        WHEN sk.study_name_upper REGEXP '\\\\bSCROTAL\\\\b|\\\\bSCROTUM\\\\b|\\\\bTESTICULAR\\\\b|\\\\bTESTICLE\\\\b|\\\\bTESTES\\\\b'                      THEN 'Scrotal / Testicular'
        WHEN sk.study_name_upper REGEXP '\\\\bSHOULDER\\\\b|\\\\bSH\\\\b|UPPER EXT JOINT SHOULDER|\\\\bSHOULDERS\\\\b'                                       THEN 'Shoulder'
        WHEN sk.study_name_upper REGEXP '\\\\bSINUS\\\\b|\\\\bSINUSES\\\\b|\\\\bNASAL\\\\b|\\\\bSINUS/NASAL\\\\b'                                            THEN 'Sinuses'
        WHEN sk.study_name_upper REGEXP '\\\\bSKULL\\\\b'                                                                                                      THEN 'Skull'
        WHEN sk.study_name_upper REGEXP 'SPINAL CORD DORSAL'                                                                                                    THEN 'Spinal Cord'
        WHEN sk.study_name_upper REGEXP '\\\\bSPINE\\\\b|\\\\bSCPINE\\\\b'                                                                                    THEN 'Spine'
        WHEN sk.study_name_upper REGEXP '\\\\bSPLEEN\\\\b'                                                                                                     THEN 'Spleen'
        WHEN sk.study_name_upper REGEXP '\\\\bSTERNUM\\\\b'                                                                                                    THEN 'Sternum'
        WHEN sk.study_name_upper REGEXP '\\\\bTEETH\\\\b'                                                                                                      THEN 'Teeth'
        WHEN sk.study_name_upper REGEXP '\\\\bTHUMB\\\\b'                                                                                                      THEN 'Thumb'
        WHEN sk.study_name_upper REGEXP 'TEMPORAL BONE'                                                                                                         THEN 'Temporal Bone'
        WHEN sk.study_name_upper REGEXP '\\\\bTHIGH\\\\b|\\\\bTHIGHS\\\\b'                                                                                    THEN 'Thigh'
        WHEN sk.study_name_upper REGEXP '\\\\bTHORACIC\\\\b'                                                                                                   THEN 'Thoracic'
        WHEN sk.study_name_upper REGEXP '\\\\bTHYROID\\\\b'                                                                                                    THEN 'Thyroid'
        WHEN sk.study_name_upper REGEXP '\\\\bTOES\\\\b|\\\\bTOE\\\\b'                                                                                        THEN 'Toes'
        WHEN sk.study_name_upper REGEXP '\\\\bTORSO\\\\b|\\\\bPE TORSO\\\\b'                                                                                  THEN 'Torso'
        WHEN sk.study_name_upper REGEXP 'TRANSVAGINAL|TRANS-VAGINAL|TRANS VAGINAL'                                                                             THEN 'Transvaginal'
        WHEN sk.study_name_upper REGEXP '\\\\bUTERUS\\\\b'                                                                                                     THEN 'Uterus'
        WHEN sk.study_name_upper REGEXP 'VAGUS NERVE'                                                                                                           THEN 'Vagus Nerve'
        WHEN sk.study_name_upper REGEXP '\\\\bVEINS\\\\b|\\\\bVENOUS\\\\b'                                                                                    THEN 'Veins'
        WHEN sk.study_name_upper REGEXP 'WHOLE BODY'                                                                                                            THEN 'Whole Body'
        WHEN sk.study_name_upper REGEXP '\\\\bWRIST\\\\b|\\\\bWRISTS\\\\b|\\\\bWR\\\\b'                                                                      THEN 'Wrist'
        ELSE NULL
    END AS body_part_std,

    CASE
        WHEN sk.study_name_upper REGEXP '\\\\bBILATERAL\\\\b'      THEN 'Bilateral'
        WHEN sk.study_name_upper REGEXP '\\\\bUNILATERAL\\\\b'     THEN 'Unilateral'
        WHEN sk.study_name_upper REGEXP '\\\\bLEFT\\\\b|\\\\bLT\\\\b'  THEN 'Left'
        WHEN sk.study_name_upper REGEXP '\\\\bRIGHT\\\\b|\\\\bRT\\\\b' THEN 'Right'
        ELSE NULL
    END AS laterality_std,

    CASE
        WHEN sk.study_name_upper REGEXP 'FLORBETAPIR|AMYVID|A9591|F-?18\\\\s*FLORBETAPIR|18F-?FLORBETAPIR|\\\\[18F\\\\]\\\\s*FLORBETAPIR'     THEN 'Florbetapir F18 (Amyvid)'
        WHEN sk.study_name_upper REGEXP 'FLUTEMETAMOL|VIZAMYL|A9592|F-?18\\\\s*FLUTEMETAMOL|18F-?FLUTEMETAMOL|\\\\[18F\\\\]\\\\s*FLUTEMETAMOL' THEN 'Flutemetamol F18 (Vizamyl)'
        WHEN sk.study_name_upper REGEXP 'FLORBETABEN|NEURACEQ|A9593|F-?18\\\\s*FLORBETABEN|18F-?FLORBETABEN|\\\\[18F\\\\]\\\\s*FLORBETABEN'    THEN 'Florbetaben F18 (Neuraceq)'
        WHEN sk.study_name_upper REGEXP '\\\\bPIB\\\\b|PITTSBURGH\\\\s*COMPOUND|F-?18\\\\s*PIB|18F-?PIB'                                      THEN 'PiB F18 (Pittsburgh Compound-B)'
        WHEN sk.study_name_upper REGEXP 'PIFLUFOLASTAT|PYLARIFY|A9816|DCFPYL'                                                                  THEN 'Piflufolastat F18 (Pylarify)'
        WHEN sk.study_name_upper REGEXP 'FLOTUFOLASTAT|POSLUMA|A9815|PSMA-?1007'                                                               THEN 'Flotufolastat F18 (Posluma)'
        WHEN sk.study_name_upper REGEXP '\\\\bPSMA\\\\b' AND sk.study_name_upper REGEXP 'F-?18|\\\\bF18\\\\b|\\\\[18F\\\\]|18F-?'             THEN 'F18 - PSMA Tracer (Unspecified)'
        WHEN sk.study_name_upper REGEXP '\\\\bFDG\\\\b|FLUORODEOXYGLUCOSE|FLUDEOXYGLUCOSE|A9552|FDG-?PET'                                      THEN 'FDG F18 (Fluorodeoxyglucose)'
        WHEN sk.study_name_upper REGEXP 'SODIUM\\\\s*FLUORIDE|\\\\bNAF\\\\b|A9580|F-?18\\\\s*FLUORIDE|18F-?FLUORIDE|18F-?NAF'                 THEN 'Sodium Fluoride F18 (NaF)'
        WHEN sk.study_name_upper REGEXP '\\\\bFDOPA\\\\b|FLUORODOPA|FLUORO-?DOPA|A9600|F-?18\\\\s*FDOPA|18F-?FDOPA|18F-?DOPA'                 THEN 'Fluorodopa F18 (FDOPA)'
        WHEN sk.study_name_upper REGEXP 'FLUCICLOVINE|AXUMIN|A9584|\\\\bFACBC\\\\b'                                                            THEN 'Fluciclovine F18 (Axumin)'
        WHEN sk.study_name_upper REGEXP 'FLORTAUCIPIR|TAUVID|A9814|TAU\\\\s*PET'                                                               THEN 'Flortaucipir F18 (Tauvid)'
        WHEN sk.study_name_upper REGEXP '\\\\bF-?18\\\\b|\\\\[18F\\\\]|18F-?|\\\\bF18\\\\b'                                                   THEN 'F18 - Tracer Not Specified'
        WHEN sk.study_name_upper REGEXP 'GA-?68\\\\s*DOTATATE|NETSPOT|\\\\bDOTATATE\\\\b'                                                     THEN 'Ga68-DOTATATE (Netspot)'
        WHEN sk.study_name_upper REGEXP 'GA-?68\\\\s*DOTATOC|\\\\bDOTATOC\\\\b'                                                               THEN 'Ga68-DOTATOC'
        WHEN sk.study_name_upper REGEXP 'GA-?68\\\\s*PSMA|ILLUCCIX|LOCAMETZ|\\\\bPSMA-?11\\\\b'                                               THEN 'Ga68-PSMA-11 (Illuccix/Locametz)'
        WHEN sk.study_name_upper REGEXP '\\\\bGA-?68\\\\b|\\\\[68GA\\\\]|68GA-?|\\\\bGALLIUM\\\\s*68\\\\b|\\\\bGALLIUM-?68\\\\b'             THEN 'Ga68 - Tracer Not Specified'
        ELSE NULL
    END AS tracer_name_std"""


def build_pass2(pk_lo, pk_hi):
    """
    Pass 2: body_part_std, laterality_std, tracer_name_std.
    All values pre-computed in STAGING_FINAL — batch is a pure indexed equality join.
    """
    return f"""
UPDATE {TARGET_TABLE} r
JOIN {STAGING_FINAL} sf ON r.{BATCH_KEY} = sf.{BATCH_KEY}
SET
    r.body_part_std   = sf.body_part_std,
    r.laterality_std  = sf.laterality_std,
    r.tracer_name_std = sf.tracer_name_std
WHERE r.{BATCH_KEY} >= {pk_lo}
  AND r.{BATCH_KEY} <  {pk_hi}
"""


# ── Pass 3 batch UPDATE ───────────────────────────────────────────────

def build_pass3(pk_lo, pk_hi):
    """
    Pass 3: probable CPT code matching for rows where proc_code_std IS NULL.
    Joins to pre-materialized STAGING_COMBO keyed on (modality_std, body_part_std,
    contrast_type_std, strength_views_std).
    """
    return f"""
UPDATE {TARGET_TABLE} r
JOIN {STAGING_COMBO} m
    ON  r.modality_std      = m.modality_std
    AND r.body_part_std     = m.body_part_std
    AND r.contrast_type_std = m.contrast_type_std
    AND (r.strength_views_std = m.strength_views_std
         OR (r.strength_views_std IS NULL AND m.strength_views_std IS NULL))
SET
    r.probable_cpt_code        = m.probable_cpt_code,
    r.probable_cpt_match_count = m.probable_cpt_match_count,
    r.matched_descriptions     = m.matched_descriptions
WHERE r.{BATCH_KEY} >= {pk_lo}
  AND r.{BATCH_KEY} <  {pk_hi}
  AND r.proc_code_std IS NULL
"""


# ── Pass 3 lazy setup ─────────────────────────────────────────────────

def setup_pass3_tables():
    """
    Called after Pass 2 completes (lazy).
    1. staging.radf2_cpt_clean       — filtered imaging CPT codes (shared across runs)
    2. staging.radf2_combo_matches_* — regex-matched (modality, body_part, contrast, views)
                                       → probable_cpt_code per combo
    3. staging.radf2_std_pk3_*       — PKs of candidate unmapped rows only
    Returns batch ranges for Pass 3.
    """
    conn = get_connection()
    cur  = conn.cursor()

    # ── 1. CPT clean (imaging codes, shared) ─────────────────────────
    print("  [Pass 3] Materializing CPT clean lookup...")
    if not _table_exists(cur, STAGING_CPT_CLEAN):
        cur.execute(f"""
            CREATE TABLE {STAGING_CPT_CLEAN} AS
            SELECT DISTINCT PROCEDURECODE, COMMONDESCRIPTION,
                   UPPER(COMMONDESCRIPTION) AS DESC_U
            FROM tncpa.PROCEDURECODEREFERENCE
            WHERE (
                UPPER(COMMONDESCRIPTION) REGEXP '\\\\bMRI\\\\b|\\\\bMR\\\\b|FMRI|QMRCP|MAGNETIC RESONANCE'
             OR UPPER(COMMONDESCRIPTION) REGEXP '\\\\bCT\\\\b|\\\\bCAT SCAN\\\\b|COMPUTED TOMOGRAPHY'
             OR UPPER(COMMONDESCRIPTION) REGEXP '\\\\bPET\\\\b|POSITRON'
             OR UPPER(COMMONDESCRIPTION) REGEXP '\\\\bUS\\\\b|ULTRASOUND|ULTRASONOGRAPHY|SONOGRAM|SONOGRAPHY'
             OR UPPER(COMMONDESCRIPTION) REGEXP '\\\\bECHO\\\\b|ECHOCARDIOGRAPHY|ECHOCARDIOGRAM'
             OR UPPER(COMMONDESCRIPTION) REGEXP 'MAMMO|MAMMOGRAM|MAMMOGRAPHY'
             OR UPPER(COMMONDESCRIPTION) REGEXP '\\\\bDXA\\\\b|BONE DENSIT|BONE MINERAL'
             OR UPPER(COMMONDESCRIPTION) REGEXP '\\\\bXR\\\\b|X-?RAY|RADIOGRAPH|RADIOGRAM|RADIOLOGIC EXAM'
             OR UPPER(COMMONDESCRIPTION) REGEXP 'FLUORO|FLUOROSCOP'
             OR (UPPER(COMMONDESCRIPTION) REGEXP 'ANGIOGRAPH|ARTERIOGRAM|VENOGRAM|\\\\bANGIO\\\\b'
                 AND UPPER(COMMONDESCRIPTION) NOT REGEXP 'ANGIOPLAST|ANGIOPLST|ANGIOSCOP|ATHERECT|STENT|REVSC|REVASC|BALO ANGIO|BALLO ANGIO|ANGIO-SEAL|ANGIO-JET|ANGIOSTOMY|CATH/ANGIO')
             OR UPPER(COMMONDESCRIPTION) REGEXP '\\\\bSPECT\\\\b|SINGLE PHOTON'
             OR (UPPER(COMMONDESCRIPTION) REGEXP 'NUCLEAR MEDICINE|NUCLEAR MED|NUCLEAR IMAG|NUCLEAR EXAM|NUCLEAR LOCALIZ|NUCLEAR SCAN|NUCLEAR TX|NUCLEAR RX|NUCLEAR THERAPY|RADIONUCLIDE|SCINTIGRAPHY|SCINTIMAMMO|PLANAR IMAG|PLANAR W/|PLANAR SING|PLANAR MULT|GATED HEART PLANAR|TUMOR IMAGING|MYOCRD IMG'
                 AND UPPER(COMMONDESCRIPTION) NOT REGEXP 'ANTINUCLEAR|NUCLEAR ANTI|MONONUCLEAR|NUCLEAR MATRIX|NUCLEAR CELL|EPSTEIN.*NUCLEAR|PLANAR BACK|PLANAR SEAT|NONHEMATO NUCLEAR')
            )
            AND UPPER(COMMONDESCRIPTION) NOT REGEXP 'INJECTION[,\\\\s]|\\\\bINJ[,\\\\s]|\\\\bINJ\\\\.|\\\\bINJ$|ORAL[,\\\\s]|INFUSION'
            AND UPPER(COMMONDESCRIPTION) NOT REGEXP '\\\\bMG\\\\b|\\\\bML\\\\b|\\\\bIU\\\\b|MG/|MCG|\\\\bUNIT\\\\b'
            AND UPPER(COMMONDESCRIPTION) NOT REGEXP 'ANTIBOD|ANTIGEN|ASSAY|\\\\bANA\\\\b|EPSTEIN|MATRIX PROTEIN|MONONUCLEAR'
            AND UPPER(COMMONDESCRIPTION) NOT REGEXP 'KNEE-SHIN|KNEE DISART|MYOELECTRON|BRACHYTX|SWITCH CT|GREIFER|ULTRA-LIGHT'
            AND UPPER(COMMONDESCRIPTION) NOT REGEXP 'PT DOC|NO DOC|DOC RSN|CLIN DOC|NOT PERF|CARE DOC|PT NOT DOC|PT INELIG|CLIN NOT|DOC PT|PT RECEIV|PT REAS|PT MBHT|MED REAS|MEDRSN|SRCH FOR|NO SRCH|DOC SCR|NO SCR|PHODOC|PT W/DXA'
            AND UPPER(COMMONDESCRIPTION) NOT REGEXP 'DEXAMETHASONE|DEXAMETHA'
            AND UPPER(COMMONDESCRIPTION) NOT REGEXP '\\\\bFNA BX\\\\b|\\\\bBX BREAST\\\\b|\\\\bPERQ DEV\\\\b|\\\\bBX PRST8\\\\b|BRAIN BIOPSY|FLUOROGUIDE|\\\\bFLUORO LOC\\\\b|FLUORO EXAM OF|MR GUIDANCE|NEEDLE LOCALIZATION'
            AND UPPER(COMMONDESCRIPTION) NOT REGEXP 'MR SFTY|MR SAFETY|SET UP PORT|TRANSPORT PORT|MRI COMPATIBLE|HIGH DOSE CONTRAST MRI|MR CONTRAST|ECHOCARDIOGRAPHY CONTRAST'
            AND UPPER(COMMONDESCRIPTION) NOT REGEXP 'INTRVASC US|INTRAVASCULAR US|\\\\bIV US\\\\b|TRURL ABLT|LOW FREQUENCY NON-THERMAL|OSTEOGEN ULTRASOUND|DIATHERMY|PACHYMETRY|SERVICES OUTSIDE US'
            AND UPPER(COMMONDESCRIPTION) NOT REGEXP 'TBS DXA CAL|DXA ORDERED'
        """)
        cur.execute(f"ALTER TABLE {STAGING_CPT_CLEAN} ADD INDEX idx_proc (PROCEDURECODE(20))")
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")
    cur.execute(f"SELECT COUNT(*) FROM {STAGING_CPT_CLEAN}")
    print(f"    {cur.fetchone()[0]:,} rows")

    # ── 2. Combo matches lookup ────────────────────────────────────────
    print("  [Pass 3] Materializing combo matches (regex CPT lookup)...")
    if not _table_exists(cur, STAGING_COMBO):
        cur.execute(f"""
            CREATE TABLE {STAGING_COMBO} AS
            WITH rad_unmapped AS (
                SELECT DISTINCT
                    modality_std, body_part_std, contrast_type_std, strength_views_std
                FROM {TARGET_TABLE}
                WHERE proc_code_std     IS NULL
                  AND modality_std      IS NOT NULL
                  AND body_part_std     IS NOT NULL
                  AND contrast_type_std IS NOT NULL
            ),
            rad_patterns AS (
                SELECT
                    r.*,
                    CASE r.modality_std
                        WHEN 'Computed Tomography'                                                          THEN '\\\\bCT\\\\b|\\\\bCAT SCAN\\\\b'
                        WHEN 'Magnetic Resonance'                                                           THEN '\\\\bMRI\\\\b|\\\\bMR\\\\b|FMRI|QMRCP'
                        WHEN 'Magnetic Resonance Angiography (MA - Retired) / Magnetic Resonance'           THEN '\\\\bMRA\\\\b|MR ANGIO|MRI ANGIO|MR ANG'
                        WHEN 'Positron Emission Tomography (PET)'                                           THEN '\\\\bPET\\\\b'
                        WHEN 'Positron Emission Tomography (PET) / Computed Tomography'                     THEN 'PET.*CT|PET/CT'
                        WHEN 'Ultrasound'                                                                   THEN '\\\\bUS\\\\b|ULTRASOUND|SONOGRAM|\\\\bECHO EXAM\\\\b'
                        WHEN 'Ultrasound / Duplex Doppler'                                                  THEN 'DUPLEX|DOPPLER'
                        WHEN 'Ultrasound / Echocardiography'                                                THEN '\\\\bECHO\\\\b|\\\\bTTE\\\\b|\\\\bTEE\\\\b'
                        WHEN 'Echocardiography (EC - Retired) / Ultrasound'                                 THEN '\\\\bECHO\\\\b|\\\\bTTE\\\\b|\\\\bTEE\\\\b|ECHOCARDIOGRAPHY'
                        WHEN 'Mammography'                                                                  THEN 'MAMMO|MAMMOGRAM'
                        WHEN 'Bone Densitometry (X-Ray)'                                                    THEN '\\\\bDXA\\\\b|BONE DENSIT|BONE MINERAL'
                        WHEN 'Digital Radiography'                                                          THEN '\\\\bXR\\\\b|X-?RAY|RADIOGR'
                        WHEN 'Computed Radiography'                                                         THEN '\\\\bXR\\\\b|X-?RAY|RADIOGR'
                        WHEN 'Digital Radiography / Radio Fluoroscopy'                                      THEN 'FLUORO|\\\\bXR\\\\b'
                        WHEN 'Radio Fluoroscopy'                                                            THEN 'FLUORO'
                        WHEN 'Nuclear Medicine'                                                             THEN 'NUCLEAR|SCINT|RADIONUCLIDE|PLANAR IMAG|PLANAR W/|PLANAR SING|PLANAR MULT|GATED HEART PLANAR|TUMOR IMAGING'
                        WHEN 'X-Ray Angiography'                                                            THEN 'ANGIOGRAPH|ARTERIOGRAM|VENOGRAM'
                        WHEN 'Single-Photon Emission Computed Tomography (ST - Retired) / Nuclear Medicine' THEN '\\\\bSPECT\\\\b|SINGLE PHOTON'
                        WHEN 'Electrocardiography'                                                          THEN '\\\\bECG\\\\b|\\\\bEKG\\\\b|ELECTROCARDIO'
                        WHEN 'Electroencephalography'                                                       THEN '\\\\bEEG\\\\b|ELECTROENCEPH'
                        ELSE NULL
                    END AS modality_rx,
                    CASE r.body_part_std
                        WHEN 'Abdomen'                      THEN '\\\\bABD\\\\b|ABDOMEN|ABDOMINAL|\\\\bABDO\\\\b'
                        WHEN 'Abdomen, Pelvis'              THEN '(ABD|ABDOMEN).*(PELV|PELVIS)'
                        WHEN 'Chest'                        THEN '\\\\bCHEST\\\\b|\\\\bTHORAX\\\\b|\\\\bTHORAC\\\\b|\\\\bLUNG\\\\b'
                        WHEN 'Chest, Abdomen'               THEN 'CHEST.*ABD|THORAX.*ABD'
                        WHEN 'Chest, Abdomen, Pelvis'       THEN 'CHEST.*ABD.*PELV'
                        WHEN 'Pelvis'                       THEN '\\\\bPELV|PELVIS|PELVIC'
                        WHEN 'Head'                         THEN '\\\\bHEAD\\\\b|HEAD/BRAIN|\\\\bHD\\\\b'
                        WHEN 'Head, Neck'                   THEN '(HEAD|HD).*(NECK|NCK)|HEAD & NECK|\\\\bHEAD\\\\b|\\\\bNECK\\\\b|\\\\bNCK\\\\b'
                        WHEN 'Brain Stem'                   THEN 'BRAIN STEM|BRN STEM'
                        WHEN 'Brain'                        THEN '\\\\bBRAIN\\\\b|\\\\bBRN\\\\b|CEREBRAL|HEAD/BRAIN'
                        WHEN 'Neck'                         THEN '\\\\bNECK\\\\b|\\\\bNCK\\\\b'
                        WHEN 'Intracranial'                 THEN 'INTRACRAN|\\\\bICR\\\\b|BRAIN STEM|\\\\bBRAIN\\\\b|CEREBRAL|\\\\bHEAD\\\\b'
                        WHEN 'Extracranial'                 THEN 'EXTRACRAN|CAROTID|\\\\bNECK\\\\b|\\\\bNCK\\\\b|VERTEBRAL ART'
                        WHEN 'Cervical Spine'               THEN 'CERVICAL SPINE|\\\\bC-?SPINE\\\\b|NECK SPINE'
                        WHEN 'Thoracic Spine, Lumbar Spine' THEN 'THORACOLUMBAR|THORACO.?LUMBAR|THORACOLMB'
                        WHEN 'Lumbar Spine'                 THEN 'LUMBAR SPINE|\\\\bL-?SPINE\\\\b|L-S SPINE|LUMBOSACRAL'
                        WHEN 'Lumbar Spine, Sacrum'         THEN 'LUMBOSACRAL|LUMB.*SACR|L-S SPINE'
                        WHEN 'Spine'                        THEN '\\\\bSPINE\\\\b|SPINAL|VERTEBR|TRUNK SPINE|ENTIRE SPI'
                        WHEN 'Sacrum'                       THEN '\\\\bSACRUM\\\\b|SACRAL'
                        WHEN 'Sacrum, Coccyx'               THEN 'SACRUM TAILBONE|SACRUM.*COCCYX'
                        WHEN 'Coccyx'                       THEN 'COCCYX|TAILBONE'
                        WHEN 'Sacroiliac Joint'             THEN 'SACROILIAC|\\\\bSI JOINTS?\\\\b'
                        WHEN 'Shoulder'                     THEN 'SHOULDER'
                        WHEN 'Elbow'                        THEN 'ELBOW'
                        WHEN 'Wrist'                        THEN 'WRIST'
                        WHEN 'Hand'                         THEN '\\\\bHAND\\\\b'
                        WHEN 'Fingers'                      THEN 'FINGER'
                        WHEN 'Thumb'                        THEN 'THUMB'
                        WHEN 'Forearm'                      THEN 'FOREARM'
                        WHEN 'Humerus / Upper Arm'          THEN 'HUMERUS|UPPER ARM|\\\\bARM INFANT\\\\b'
                        WHEN 'Clavicle'                     THEN 'CLAVICLE|COLLAR BONE'
                        WHEN 'Scapula'                      THEN 'SCAPULA|SHOULDER BLADE'
                        WHEN 'Acromioclavicular Joints'     THEN 'ACROMIOCLAV|\\\\bAC JOINT|STRENOCLAVIC'
                        WHEN 'Hip'                          THEN '\\\\bHIPS?\\\\b'
                        WHEN 'Femur'                        THEN 'FEMUR|FEMORAL|\\\\bFEM\\\\b|THIGH'
                        WHEN 'Knee'                         THEN 'KNEES?'
                        WHEN 'Leg'                          THEN '\\\\bLEG\\\\b|LOWER LEG|LEG INFANT'
                        WHEN 'Calf'                         THEN '\\\\bCALF\\\\b'
                        WHEN 'Thigh'                        THEN 'THIGH'
                        WHEN 'Tibia, Fibula'                THEN 'TIBIA.*FIBULA|TIB.*FIB'
                        WHEN 'Ankle'                        THEN 'ANKLE'
                        WHEN 'Foot'                         THEN '\\\\bFOOT\\\\b|\\\\bFEET\\\\b|\\\\bHEEL\\\\b'
                        WHEN 'Toes'                         THEN '\\\\bTOE'
                        WHEN 'Upper Extremity'              THEN 'UPPER EXTREM|UPPR EXTREM|UPR EXTR|UPR EXTRM|UPPER EXT|JOINT UPR EXTR|JNT OF UPR|JOINT UPR'
                        WHEN 'Lower Extremity'              THEN 'LOWER EXTREM|LWR EXTREM|LWR EXTR|LOWER EXT|JOINT LWR EXTR|JNT OF LWR|JOINT LWR|LWR EXTRMTY'
                        WHEN 'Upper Extremity (Vascular)'   THEN '(UPPER EXTREM|UPR EXTR).*(ARTER|VEN|VASC|ANGIO)'
                        WHEN 'Lower Extremity (Vascular)'   THEN '(LOWER EXTREM|LWR EXTR).*(ARTER|VEN|VASC|ANGIO)'
                        WHEN 'Breast'                       THEN 'BREAST|BREASTS|\\\\bBRST\\\\b|MAMMARY|MAMMO'
                        WHEN 'Heart'                        THEN '\\\\bHEART\\\\b|\\\\bHRT\\\\b|CARDIAC|\\\\bCARD\\\\b|MYOCARD|HT MRI|CORONARY'
                        WHEN 'Aorta'                        THEN 'AORTA|AORTIC'
                        WHEN 'Carotid'                      THEN 'CAROTID'
                        WHEN 'Carotid, Neck'                THEN 'CAROTID'
                        WHEN 'Liver'                        THEN 'LIVER|HEPATIC'
                        WHEN 'Kidney'                       THEN 'KIDNEY|RENAL|\\\\bK TRANSPL\\\\b'
                        WHEN 'Gallbladder'                  THEN 'GALLBLADDER|BILIARY'
                        WHEN 'Pancreas'                     THEN 'PANCREA'
                        WHEN 'Spleen'                       THEN 'SPLEEN|SPLENIC'
                        WHEN 'Liver, Gallbladder, Pancreas' THEN 'HEPATOBILIARY|LIVER.*GALLBLADDER|BILE DUCTS?/PANCREAS'
                        WHEN 'Thyroid'                      THEN 'THYROID'
                        WHEN 'Thyroid, Neck'                THEN 'THYROID'
                        WHEN 'Parathyroid'                  THEN 'PARATHYRO|PARATHYRD'
                        WHEN 'Prostate / Rectal'            THEN 'PROSTATE|PRST8|RECTAL|RECTUM|TRANSRECTAL'
                        WHEN 'Uterus'                       THEN '\\\\bUTER|UTERINE'
                        WHEN 'Transvaginal'                 THEN 'TRANSVAG|VAGINAL'
                        WHEN 'Scrotal / Testicular'         THEN 'SCROT|TESTIC'
                        WHEN 'Eye / Orbit'                  THEN '\\\\bEYE\\\\b|ORBIT|\\\\bORBT\\\\b|OCULAR|EAR/FOSSA'
                        WHEN 'Face'                         THEN '\\\\bFACE\\\\b|FACIAL|\\\\bFAC\\\\b|MAXILLOFACIAL|MAXFAC'
                        WHEN 'Facial Bones, Sinuses'        THEN 'FACIAL BONES|MAXILLOFACIAL|(FACIAL|FAC).*SINUS'
                        WHEN 'Sinuses'                      THEN 'SINUS|NASAL'
                        WHEN 'Mandible'                     THEN 'MANDIBLE|\\\\bJAW\\\\b'
                        WHEN 'Temporomandibular Joint'      THEN '\\\\bTMJ\\\\b|TEMPOROMAND|JAW JOINT'
                        WHEN 'Mastoids'                     THEN 'MASTOID'
                        WHEN 'Temporal Bone'                THEN 'TEMPORAL BONE|TEMP BONE'
                        WHEN 'Skull'                        THEN '\\\\bSKULL\\\\b'
                        WHEN 'Internal Auditory Canal'      THEN 'INTERNAL AUDITORY|AUDITORY CANAL|\\\\bIAC\\\\b'
                        WHEN 'Pituitary'                    THEN 'PITUITARY|SELLA'
                        WHEN 'Sternum'                      THEN 'STERNUM|BREASTBONE'
                        WHEN 'Retroperitoneum'              THEN 'RETROPERITONE'
                        WHEN 'Spinal Canal'                 THEN 'SPINAL CANAL|SPINAL CORD'
                        WHEN 'Brachial Plexus'              THEN 'BRACHIAL PLEXUS|BRACHPLEX'
                        WHEN 'Whole Body'                   THEN 'WHOLE BODY|FULL BODY|WHOLBODY|WHOLEBOD'
                        WHEN 'Torso'                        THEN 'TORSO|TRUNK'
                        WHEN 'Orbit, Face, Neck'            THEN '(ORBT|ORBIT).*(FAC|FACE).*(NCK|NECK)'
                        ELSE NULL
                    END AS body_part_rx,
                    CASE r.contrast_type_std
                        WHEN 'With and Without Contrast' THEN
                            'W/O ?& ?W/DYE|W/O ?AND ?W/DYE|W/O ?& ?W/CNTR|W/O ?AND ?W/CONTRAST|WITHOUT AND WITH CONTRAST|WITH AND WITHOUT CONTRAST|W/O CNTR FLWD CNTR|WO CNTRST FLWD CNTRST|W/O CONT FLWD CNTR|\\\\bW OR W/O DYE\\\\b|\\\\bW OR W/O CNTR\\\\b|W/WO DYE|\\\\bWWO\\\\b'
                        WHEN 'Without Contrast' THEN
                            'W/O DYE|W/O CNTR|W/O CONT|\\\\bWO DYE\\\\b|\\\\bWO CNTR\\\\b|\\\\bWO\\\\b|WITHOUT CONTRAST|WITHOUT DYE|\\\\bNO CNTR\\\\b|\\\\bNO CONTRAST\\\\b|\\\\bC-\\\\b'
                        WHEN 'With Contrast' THEN
                            'W/DYE|\\\\bW DYE\\\\b|W/CNTR|W/CONTRAST|WITH CONTRAST|WITH DYE|W/CAD|\\\\bC\\\\+\\\\b'
                        ELSE NULL
                    END AS contrast_rx,
                    CASE r.contrast_type_std
                        WHEN 'Without Contrast' THEN
                            'W/O ?& ?W/DYE|W/O ?AND ?W/DYE|W/O ?& ?W/CNTR|WITHOUT AND WITH|WITH AND WITHOUT|W/O CNTR FLWD CNTR|WO CNTRST FLWD CNTRST|W/O CONT FLWD CNTR|W OR W/O|W/WO DYE|\\\\bWWO\\\\b|\\\\bC-/C\\\\+\\\\b|\\\\bC-\\\\+\\\\b'
                        WHEN 'With Contrast' THEN
                            'W/O ?& ?W/DYE|W/O ?AND ?W/DYE|W/O ?& ?W/CNTR|WITHOUT AND WITH|WITH AND WITHOUT|W/O CNTR FLWD CNTR|WO CNTRST FLWD CNTRST|W/O CONT FLWD CNTR|W OR W/O|W/WO DYE|\\\\bWWO\\\\b|\\\\bC-/C\\\\+\\\\b'
                        ELSE NULL
                    END AS contrast_exclude_rx,
                    CASE
                        WHEN r.modality_std NOT IN ('Digital Radiography','Computed Radiography',
                                                    'Digital Radiography / Radio Fluoroscopy',
                                                    'Bone Densitometry (X-Ray)')
                            THEN NULL
                        WHEN r.strength_views_std IS NULL
                            THEN NULL
                        WHEN r.strength_views_std = '2 Views'
                            THEN '\\\\b2 VIEWS?\\\\b|\\\\b2VWS\\\\b|\\\\b2 VW\\\\b|UNI 2 VIEW|BI 2 VIEW|RIBS UNI 2|2VW'
                        WHEN r.strength_views_std = '2 or 3 Views'
                            THEN '2-3 VIEWS?|2-3 VW|2/3 VWS|2/3 VW|UNI 2-3 VIEW|BI 2/3'
                        WHEN r.strength_views_std = '3 or 4 Views'
                            THEN '3-4 VIEWS?|3/4 VWS|3/4 VW|BI 3-4 VIEW'
                        ELSE NULL
                    END AS views_rx
                FROM rad_unmapped r
            )
            SELECT
                p.modality_std,
                p.body_part_std,
                p.contrast_type_std,
                p.strength_views_std,
                GROUP_CONCAT(DISTINCT SUBSTRING_INDEX(cpt.PROCEDURECODE, ',', 1)
                             ORDER BY SUBSTRING_INDEX(cpt.PROCEDURECODE, ',', 1)
                             SEPARATOR ',') AS probable_cpt_code,
                COUNT(DISTINCT SUBSTRING_INDEX(cpt.PROCEDURECODE, ',', 1)) AS probable_cpt_match_count,
                GROUP_CONCAT(DISTINCT cpt.COMMONDESCRIPTION
                             ORDER BY cpt.COMMONDESCRIPTION
                             SEPARATOR ' | ') AS matched_descriptions
            FROM rad_patterns p
            LEFT JOIN {STAGING_CPT_CLEAN} cpt
                   ON p.modality_rx  IS NOT NULL
                  AND p.body_part_rx IS NOT NULL
                  AND p.contrast_rx  IS NOT NULL
                  AND cpt.DESC_U REGEXP p.modality_rx
                  AND cpt.DESC_U REGEXP p.body_part_rx
                  AND cpt.DESC_U REGEXP p.contrast_rx
                  AND (p.contrast_exclude_rx IS NULL OR cpt.DESC_U NOT REGEXP p.contrast_exclude_rx)
                  AND (p.views_rx IS NULL OR cpt.DESC_U REGEXP p.views_rx)
                  AND NOT (
                      (p.body_part_std = 'Brain' AND cpt.DESC_U REGEXP 'BRAIN STEM|BRN STEM')
                   OR (p.body_part_std = 'Neck'  AND cpt.DESC_U REGEXP 'NECK SPINE|ORBT/FAC/NCK|ORBIT/FACE/NECK')
                  )
            GROUP BY p.modality_std, p.body_part_std, p.contrast_type_std, p.strength_views_std
        """)
        cur.execute(f"""
            ALTER TABLE {STAGING_COMBO}
            ADD INDEX idx_combo (modality_std(100), body_part_std(100), contrast_type_std(50))
        """)
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")
    cur.execute(f"SELECT COUNT(*) FROM {STAGING_COMBO}")
    print(f"    {cur.fetchone()[0]:,} combos")

    # ── 3. PK staging for Pass 3 (candidate unmapped rows only) ──────
    print("  [Pass 3] Creating PK staging (unmapped rows matching a combo)...")
    if not _table_exists(cur, STAGING_PK_PASS3):
        cur.execute(f"""
            CREATE TABLE {STAGING_PK_PASS3} AS
            SELECT r.{BATCH_KEY}
            FROM {TARGET_TABLE} r
            JOIN {STAGING_COMBO} m
                ON  r.modality_std      = m.modality_std
                AND r.body_part_std     = m.body_part_std
                AND r.contrast_type_std = m.contrast_type_std
                AND (r.strength_views_std = m.strength_views_std
                     OR (r.strength_views_std IS NULL AND m.strength_views_std IS NULL))
            WHERE r.proc_code_std IS NULL
              AND r.{BATCH_KEY} IS NOT NULL
        """)
        cur.execute(f"ALTER TABLE {STAGING_PK_PASS3} ADD INDEX idx_pk ({BATCH_KEY})")
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")

    ranges, total = _build_ranges(cur, STAGING_PK_PASS3)
    print(f"    {total:,} rows → {len(ranges)} batches")

    cur.close()
    conn.close()
    return ranges


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
        ("extracted_codes",          "TEXT"),
        ("proc_code_std",            "TEXT"),
        ("modality_std",             "VARCHAR(200)"),
        ("strength_views_std",       "VARCHAR(200)"),
        ("contrast_type_std",        "VARCHAR(50)"),
        ("body_part_std",            "VARCHAR(200)"),
        ("laterality_std",           "VARCHAR(20)"),
        ("tracer_name_std",          "VARCHAR(200)"),
        ("probable_cpt_code",        "TEXT"),
        ("probable_cpt_match_count", "INT"),
        ("matched_descriptions",     "TEXT"),
    ]
    print(f"  Checking std columns on {TARGET_TABLE}...")
    ddl_conn = get_connection()
    ddl_cur  = ddl_conn.cursor()
    ddl_cur.execute("SET lock_wait_timeout = 15")
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
    cur.execute("SET SESSION lock_wait_timeout = 3600")
    cur.execute("SET SESSION innodb_lock_wait_timeout = 3600")

    # ── 1. Code lookup staging (CTE materialized once) ────────────────
    # Extended vs radiology_2: 5 REGEXP_REPLACE normalization steps (OR, /, +, -, &)
    # and up to 5 occurrences of 5-digit code extraction.
    print("  Materializing code lookup CTE...")
    if not _table_exists(cur, STAGING_CODE_LOOKUP):
        cur.execute(f"""
            CREATE TABLE {STAGING_CODE_LOOKUP} AS
            WITH base AS (
                SELECT
                    study_name,
                    TRIM(BOTH ',' FROM CONCAT_WS(',',
                        IF(study_name REGEXP '\\\\bCPT\\\\s*[0-9]{{5}}\\\\b',
                            REGEXP_REPLACE(REGEXP_SUBSTR(study_name, 'CPT\\\\s*[0-9]{{5}}'), '[^0-9]', ''), NULL),
                        IF(study_name NOT REGEXP '\\\\bCPT\\\\s*[0-9]{{5}}\\\\b' AND study_name REGEXP '(^|[^0-9])[0-9]{{5}}([^0-9]|$)',
                            REGEXP_REPLACE(REGEXP_SUBSTR(study_name, '(^|[^0-9])[0-9]{{5}}([^0-9]|$)', 1, 1), '[^0-9]', ''), NULL),
                        IF(study_name NOT REGEXP '\\\\bCPT\\\\s*[0-9]{{5}}\\\\b',
                            NULLIF(REGEXP_REPLACE(REGEXP_SUBSTR(study_name, '(^|[^0-9])[0-9]{{5}}([^0-9]|$)', 1, 2), '[^0-9]', ''), ''), NULL),
                        IF(study_name NOT REGEXP '\\\\bCPT\\\\s*[0-9]{{5}}\\\\b',
                            NULLIF(REGEXP_REPLACE(REGEXP_SUBSTR(study_name, '(^|[^0-9])[0-9]{{5}}([^0-9]|$)', 1, 3), '[^0-9]', ''), ''), NULL),
                        IF(study_name NOT REGEXP '\\\\bCPT\\\\s*[0-9]{{5}}\\\\b',
                            NULLIF(REGEXP_REPLACE(REGEXP_SUBSTR(study_name, '(^|[^0-9])[0-9]{{5}}([^0-9]|$)', 1, 4), '[^0-9]', ''), ''), NULL),
                        IF(study_name NOT REGEXP '\\\\bCPT\\\\s*[0-9]{{5}}\\\\b',
                            NULLIF(REGEXP_REPLACE(REGEXP_SUBSTR(study_name, '(^|[^0-9])[0-9]{{5}}([^0-9]|$)', 1, 5), '[^0-9]', ''), ''), NULL),
                        IF(study_name NOT REGEXP '(^|[^0-9])[0-9]{{5}}([^0-9]|$)' AND study_name REGEXP '[A-Za-z]+[0-9]{{6,}}',
                            REGEXP_SUBSTR(study_name, '[A-Za-z]+[0-9]{{6,}}'), NULL),
                        NULLIF(REGEXP_SUBSTR(study_name, '\\\\b[A-Za-z][0-9]{{4}}\\\\b'), '')
                    )) AS extracted_codes
                FROM (
                    SELECT
                        REGEXP_REPLACE(
                            REGEXP_REPLACE(
                                REGEXP_REPLACE(
                                    REGEXP_REPLACE(
                                        REGEXP_REPLACE(study_name,
                                            '([0-9]{{5}})\\\\s+[Oo][Rr]\\\\s+([0-9]{{5}})', '\\\\1,\\\\2'),
                                        '([0-9]{{5}})\\\\s*/\\\\s*([0-9]{{5}})', '\\\\1,\\\\2'),
                                    '([0-9]{{5}})\\\\s*\\\\+\\\\s*([0-9]{{5}})', '\\\\1,\\\\2'),
                                '([0-9]{{5}})\\\\s*-\\\\s*([0-9]{{5}})', '\\\\1,\\\\2'),
                            '([0-9]{{5}})\\\\s*&\\\\s*([0-9]{{5}})', '\\\\1,\\\\2')
                        AS study_name
                    FROM (
                        SELECT DISTINCT study_name
                        FROM {TARGET_TABLE}
                        WHERE study_name IS NOT NULL
                    ) raw_names
                ) normalized
            ),
            code_lookup AS (
                SELECT
                    b.study_name,
                    b.extracted_codes,
                    NULLIF(TRIM(BOTH ',' FROM CONCAT_WS(',',
                        GROUP_CONCAT(DISTINCT cpt.PROCEDURECODE  ORDER BY cpt.PROCEDURECODE  SEPARATOR ','),
                        GROUP_CONCAT(DISTINCT hcpcs.HCPC         ORDER BY hcpcs.HCPC         SEPARATOR ',')
                    )), '') AS proc_code_std,
                    COUNT(DISTINCT cpt.PROCEDURECODE) + COUNT(DISTINCT hcpcs.HCPC) AS total_match_count
                FROM base b
                LEFT JOIN JSON_TABLE(
                    CONCAT('["', REPLACE(b.extracted_codes, ',', '","'), '"]'),
                    '$[*]' COLUMNS (code VARCHAR(20) PATH '$')
                ) codes ON TRUE
                LEFT JOIN tncpa.PROCEDURECODEREFERENCE cpt
                    ON codes.code = cpt.PROCEDURECODE AND codes.code REGEXP '^[0-9]{{5}}$'
                LEFT JOIN semantics.hcpcs hcpcs
                    ON codes.code = hcpcs.HCPC        AND codes.code REGEXP '^[A-Za-z][0-9]{{4}}$'
                GROUP BY b.study_name, b.extracted_codes
            )
            SELECT
                l.study_name,
                NULLIF(l.extracted_codes, '') AS extracted_codes,
                NULLIF(l.proc_code_std,   '') AS proc_code_std
            FROM code_lookup l
        """)
        cur.execute(f"ALTER TABLE {STAGING_CODE_LOOKUP} ADD INDEX idx_study (study_name(200))")
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")
    cur.execute(f"SELECT COUNT(*) FROM {STAGING_CODE_LOOKUP}")
    print(f"    {cur.fetchone()[0]:,} rows")

    # ── 2. Study keys (pre-computed REGEXP_REPLACE normalization + UPPER) ───
    # Extended: 5 REGEXP_REPLACE steps match the CTE normalization (OR, /, +, -, &)
    print("  Materializing study keys (pre-computed REGEXP_REPLACE + UPPER)...")
    if not _table_exists(cur, STAGING_STUDY_KEYS):
        cur.execute(f"""
            CREATE TABLE {STAGING_STUDY_KEYS} AS
            SELECT
                {BATCH_KEY},
                UPPER(study_name) AS study_name_upper,
                REGEXP_REPLACE(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(
                            REGEXP_REPLACE(
                                REGEXP_REPLACE(study_name,
                                    '([0-9]{{5}})\\\\s+[Oo][Rr]\\\\s+([0-9]{{5}})', '\\\\1,\\\\2'),
                                '([0-9]{{5}})\\\\s*/\\\\s*([0-9]{{5}})', '\\\\1,\\\\2'),
                            '([0-9]{{5}})\\\\s*\\\\+\\\\s*([0-9]{{5}})', '\\\\1,\\\\2'),
                        '([0-9]{{5}})\\\\s*-\\\\s*([0-9]{{5}})', '\\\\1,\\\\2'),
                    '([0-9]{{5}})\\\\s*&\\\\s*([0-9]{{5}})', '\\\\1,\\\\2'
                ) AS study_name_normalized
            FROM {TARGET_TABLE}
            WHERE {BATCH_KEY} IS NOT NULL
        """)
        cur.execute(f"ALTER TABLE {STAGING_STUDY_KEYS} ADD INDEX idx_pk ({BATCH_KEY})")
        _safe_add_index(cur, conn, STAGING_STUDY_KEYS, "study_name_normalized", "idx_study_norm")
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")
    cur.execute(f"SELECT COUNT(*) FROM {STAGING_STUDY_KEYS}")
    print(f"    {cur.fetchone()[0]:,} rows")

    # ── 2b. STAGING_FINAL (all CASE WHEN pre-computed once) ──────────
    print("  Materializing STAGING_FINAL (pre-computing all CASE WHEN once)...")
    if not _table_exists(cur, STAGING_FINAL):
        cur.execute(f"""
            CREATE TABLE {STAGING_FINAL} AS
            SELECT
                sk.{BATCH_KEY},
                {_build_pass1_case_when()},
                {_build_pass2_case_when()}
            FROM {STAGING_STUDY_KEYS} sk
            LEFT JOIN {STAGING_CODE_LOOKUP} l ON sk.study_name_normalized = l.study_name
        """)
        cur.execute(f"ALTER TABLE {STAGING_FINAL} ADD INDEX idx_pk ({BATCH_KEY})")
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")
    cur.execute(f"SELECT COUNT(*) FROM {STAGING_FINAL}")
    print(f"    {cur.fetchone()[0]:,} rows")

    # ── 3. Shared PK staging — all rows ──────────────────────────────
    print("  Creating PK staging (all rows)...")
    if not _table_exists(cur, STAGING_PK):
        cur.execute(f"""
            CREATE TABLE {STAGING_PK} AS
            SELECT {BATCH_KEY}
            FROM {TARGET_TABLE}
            WHERE {BATCH_KEY} IS NOT NULL
        """)
        cur.execute(f"ALTER TABLE {STAGING_PK} ADD INDEX idx_pk ({BATCH_KEY})")
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")
    ranges, total = _build_ranges(cur, STAGING_PK)
    print(f"    {total:,} rows → {len(ranges)} batches")

    # ── 4. Checkpoint table ────────────────────────────────────────────
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
        CHECKPOINT_PASS1: ranges,
        CHECKPOINT_PASS2: ranges,
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
    print(f"  Radiology Standardisation UPDATE (f2) — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  target     : {TARGET_TABLE}")
    print(f"  batch_key  : {BATCH_KEY}")
    print(f"  batch_size : {BATCH_SIZE:,}")
    print(f"  passes     : 3  (code lookup + modality | body part + tracer | CPT probable code)")
    print(f"{'='*70}\n", flush=True)

    print("  Connecting to database...")
    sys.stdout.flush()
    all_ranges = setup_tables()

    passes_12 = [
        (CHECKPOINT_PASS1, "Pass 1 — code lookup + modality + contrast (all rows)", build_pass1),
        (CHECKPOINT_PASS2, "Pass 2 — body part + laterality + tracer (all rows)",   build_pass2),
    ]

    results    = {}
    any_failed = False
    total_batches_12 = sum(len(all_ranges.get(ck, [])) for ck, _, _ in passes_12)

    with tqdm(total=total_batches_12, desc="Passes 1-2", unit="batch") as pbar:
        for ck, label, build_fn in passes_12:
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

    # ── Pass 3: lazy setup (reads proc_code_std set by Pass 1) ───────
    pass3_label  = "Pass 3 — CPT probable code (unmapped rows)"
    pass3_ranges = []
    if not any_failed:
        print(f"\n  Setting up {pass3_label}...")
        sys.stdout.flush()
        pass3_ranges = setup_pass3_tables()

        if pass3_ranges:
            with tqdm(total=len(pass3_ranges), desc="Pass 3", unit="batch") as pbar3:
                print(f"\n  Starting {pass3_label} ({len(pass3_ranges)} batches)...")
                result3 = run_pass(CHECKPOINT_PASS3, build_pass3, pass3_ranges, pbar3)
                results[CHECKPOINT_PASS3] = result3
                if result3["status"].startswith("FAILED"):
                    print(f"\n  FAILED at {pass3_label}: {result3['status']}")
                    any_failed = True
        else:
            print(f"  [SKIP] {pass3_label} — no unmapped rows with a matching combo")

    all_passes = passes_12 + [(CHECKPOINT_PASS3, pass3_label, build_pass3)]

    print(f"\n{'='*70}")
    print(f"  Per-pass summary:")
    total_rows = 0
    for ck, label, _ in all_passes:
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
        print(f"  [{tag}] {label:<60}  {rows:>10,} rows  ({secs}s)")
        if status.startswith("FAILED"):
            print(f"         {status}")

    print(f"\n  Total rows updated: {total_rows:,}")
    print(f"{'='*70}")

    print(f"\n  Cleanup SQL (run after verifying data):")
    print(f"    -- Shared lookups (only drop when done with ALL radiology tables):")
    print(f"    -- DROP TABLE IF EXISTS {STAGING_CODE_LOOKUP};")
    print(f"    -- DROP TABLE IF EXISTS {STAGING_CPT_CLEAN};")
    print(f"    -- Per-run tables (new udm_inc_id-keyed):")
    print(f"    DROP TABLE IF EXISTS {STAGING_STUDY_KEYS};")
    print(f"    DROP TABLE IF EXISTS {STAGING_FINAL};")
    print(f"    DROP TABLE IF EXISTS {STAGING_PK};")
    print(f"    DROP TABLE IF EXISTS {STAGING_COMBO};")
    print(f"    DROP TABLE IF EXISTS {STAGING_PK_PASS3};")
    print(f"    DROP TABLE IF EXISTS {CHECKPOINT_TABLE};")
    print(f"")
    print(f"  Old stale ndid-keyed tables (drop before re-running if they exist):")
    _sfx = _TABLE_SUFFIX
    print(f"    DROP TABLE IF EXISTS staging.radf3_std_study_keys2_{_sfx};")
    print(f"    DROP TABLE IF EXISTS staging.radf3_final_{_sfx};")
    print(f"    DROP TABLE IF EXISTS staging.radf3_std_pk_n_f2_{_sfx};")
    print(f"    DROP TABLE IF EXISTS staging.radf3_combo_matches_n_f1_{_sfx};")
    print(f"    DROP TABLE IF EXISTS staging.radf2_std_pk4_n_f1_{_sfx};")
    print(f"    DROP TABLE IF EXISTS staging.etl_checkpoint_radf3_std_n_2_f3_{_sfx};")
    print(f"")
    print(f"  Reset corrupted std columns before re-running (if passes 1/2 already ran):")
    print(f"    UPDATE {TARGET_TABLE}")
    print(f"    SET modality_std=NULL, strength_views_std=NULL, contrast_type_std=NULL,")
    print(f"        body_part_std=NULL, laterality_std=NULL, tracer_name_std=NULL,")
    print(f"        extracted_codes=NULL, proc_code_std=NULL,")
    print(f"        probable_cpt_code=NULL, probable_cpt_match_count=NULL, matched_descriptions=NULL;")

    if any_failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
