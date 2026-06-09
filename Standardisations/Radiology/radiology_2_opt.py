#!/usr/bin/env python3
"""
Optimized batched standardisation UPDATE for: rgd_udm_silver.radiology
(radiology_2 variant — simplified output columns)

Change TARGET_TABLE at the top to run against any radiology table.

Two sequential passes — each with checkpoint/resume:

  Pass 1 — All rows:
    SET extracted_codes, proc_code_std (combined CPT+HCPCS), modality_std, contrast_type_std
    LEFT JOIN pre-materialized staging.rad2_std_code_lookup (CTE result, indexed on study_name)

  Pass 2 — All rows:
    SET body_part_std, laterality_std, tracer_name_std
    Pure CASE WHEN on study_name — no JOIN needed

New columns added to target table (8 total):
  extracted_codes    TEXT
  proc_code_std      TEXT
  modality_std       VARCHAR(200)
  strength_views_std VARCHAR(200)
  contrast_type_std  VARCHAR(50)
  body_part_std      VARCHAR(200)
  laterality_std     VARCHAR(20)
  tracer_name_std    VARCHAR(200)

Pre-materialized lookup (computed ONCE, reused across runs):
  staging.rad2_std_code_lookup  — indexed on study_name(200)

Optimizations applied:
- Code lookup CTE pre-materialized once (not re-scanned per batch)
- Shared PK staging table for both passes
- Server-side boundary sampling (avoids loading all PKs into memory)
- Commit after every batch (frees undo/log space)
- Checkpoint/resume per pass — re-run skips completed passes
- Disabled InnoDB checks per-session for bulk update speed
- Progress bar via tqdm

Usage:
    python radiology_2_opt.py
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
    "database":        "kinsula_leq",
    "charset":         "utf8mb4",
    "connect_timeout": 30,
    "read_timeout":    21600,
    "write_timeout":   21600,
}

BATCH_SIZE = 50_000

# ── Change this to run against a different radiology table ────────────
TARGET_TABLE = "kinsula_leq.radiology"

# ─────────────────────────────────────────────────────────────────────
_TABLE_SUFFIX = TARGET_TABLE.replace(".", "_").replace("-", "_")

STAGING_CODE_LOOKUP  = "staging.rad2_std_code_lookup_n_2"           # shared across runs
STAGING_STUDY_KEYS   = f"staging.rad2_std_study_keys_{_TABLE_SUFFIX}" # pre-computed REGEXP_REPLACE + UPPER per row
STAGING_PK           = f"staging.rad2_std_pk_fn_{_TABLE_SUFFIX}"
CHECKPOINT_TABLE    = f"staging.etl_checkpoint_rad2_std_n_fn2_{_TABLE_SUFFIX}"
CHECKPOINT_PASS1    = f"radiology2.std.pass1.code_lookup_fn.{_TABLE_SUFFIX}"
CHECKPOINT_PASS2    = f"radiology2.std.pass2.body_tracer_fn.{_TABLE_SUFFIX}"

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
    Pass 1: code lookup + modality_std + strength_views_std + contrast_type_std.

    Uses pre-computed keys (no REGEXP_REPLACE/UPPER at query time):
      sk.study_name_normalized → direct equality join to STAGING_CODE_LOOKUP
      sk.study_name_upper      → used in all CASE WHEN conditions
    """
    return f"""
UPDATE {TARGET_TABLE} r
JOIN {STAGING_STUDY_KEYS} sk
    ON r.{BATCH_KEY} = sk.{BATCH_KEY}
LEFT JOIN {STAGING_CODE_LOOKUP} l
    ON sk.study_name_normalized = l.study_name
SET
    r.extracted_codes   = l.extracted_codes,
    r.proc_code_std     = l.proc_code_std,

    r.modality_std = CASE
        -- Combined modalities first
        WHEN sk.study_name_upper REGEXP '\\\\bPET/CT\\\\b|\\\\bPET CT\\\\b'                                                                                                THEN 'Positron Emission Tomography (PET) / Computed Tomography'
        WHEN sk.study_name_upper REGEXP '\\\\bXR/RF\\\\b'                                                                                                                   THEN 'Digital Radiography / Radio Fluoroscopy'
        WHEN sk.study_name_upper REGEXP '\\\\bUS DOPPLER\\\\b|\\\\bUS DUPLEX\\\\b'                                                                                          THEN 'Ultrasound / Duplex Doppler'
        WHEN sk.study_name_upper REGEXP '\\\\bUS ECHOCARDIOGRAM\\\\b'                                                                                                       THEN 'Ultrasound / Echocardiography'
        WHEN sk.study_name_upper REGEXP '\\\\bXA US\\\\b'                                                                                                                   THEN 'X-Ray Angiography / Ultrasound'
        -- Single modalities
        WHEN sk.study_name_upper REGEXP '\\\\bCT\\\\b|\\\\bCAT\\\\b|\\\\bNCT\\\\b|\\\\bLDCT\\\\b|\\\\bCTA\\\\b|\\\\bCTV\\\\b|\\\\bCTAC\\\\b|\\\\bCTC\\\\b|\\\\bCTP\\\\b'  THEN 'Computed Tomography'
        WHEN sk.study_name_upper REGEXP '\\\\bPET\\\\b'                                                                                                                     THEN 'Positron Emission Tomography (PET)'
        WHEN sk.study_name_upper REGEXP '\\\\bPT\\\\b'
             AND UPPER(r.study_name) NOT REGEXP '\\\\bPT/|/PT\\\\b|\\\\bPT\\\\s+(PANEL|COAGULATION)|\\\\bPROTIME\\\\b|\\\\bINR\\\\b|\\\\bPTT\\\\b|\\\\bESTAB\\\\s+PT\\\\b|\\\\bNEW\\\\s+PT\\\\b|\\\\bMED\\\\s+DECISION\\\\b'
                                                                                                                                                                              THEN 'Positron Emission Tomography (PET)'
        -- MRA before MRI/MR
        WHEN sk.study_name_upper REGEXP '\\\\bMRA\\\\b|\\\\bzzMRA\\\\b'                                                                                                     THEN 'Magnetic Resonance Angiography (MA - Retired) / Magnetic Resonance'
        WHEN sk.study_name_upper REGEXP '\\\\bMRI\\\\b|\\\\bMRV\\\\b|\\\\bMRCP\\\\b|\\\\b3TMRI\\\\b|\\\\bTMRI\\\\b|\\\\b3TMRA\\\\b|\\\\bMR\\\\b'                          THEN 'Magnetic Resonance'
        WHEN sk.study_name_upper REGEXP '\\\\bDEXA\\\\b|\\\\bDXA\\\\b'                                                                                                     THEN 'Bone Densitometry (X-Ray)'
        WHEN sk.study_name_upper REGEXP '\\\\bMAM\\\\b|\\\\bMAMM\\\\b|\\\\bMAMMO\\\\b|\\\\bMMAMMO\\\\b|\\\\bMAMMOGRAM\\\\b|\\\\bMAMMOGRAPHY\\\\b'                         THEN 'Mammography'
        WHEN sk.study_name_upper REGEXP '\\\\bMG\\\\b'
             AND UPPER(r.study_name) NOT REGEXP '\\\\bMYASTHENIA\\\\b|\\\\bGRAVIS\\\\b|\\\\bEVALUATION\\\\b'                                                               THEN 'Mammography'
        WHEN sk.study_name_upper REGEXP '\\\\bUS\\\\b|\\\\bULTRASOUND\\\\b|\\\\bUSV\\\\b|\\\\bBI US\\\\b|\\\\bOB US\\\\b'                                                  THEN 'Ultrasound'
        WHEN sk.study_name_upper REGEXP '\\\\bXA\\\\b'
             AND UPPER(r.study_name) NOT REGEXP '\\\\bANTI-XA\\\\b|\\\\bANTI XA\\\\b|\\\\bHEPARIN\\\\b'                                                                   THEN 'X-Ray Angiography'
        WHEN sk.study_name_upper REGEXP '\\\\bANG\\\\b|\\\\bANGIO\\\\b'                                                                                                    THEN 'X-Ray Angiography'
        WHEN sk.study_name_upper REGEXP '\\\\bCR\\\\b'                                                                                                                     THEN 'Computed Radiography'
        WHEN sk.study_name_upper REGEXP '\\\\bDX\\\\b|\\\\bXR\\\\b|\\\\bX-RAY\\\\b|\\\\bXRAY\\\\b|\\\\bXRY\\\\b'                                                          THEN 'Digital Radiography'
        WHEN sk.study_name_upper REGEXP '\\\\bDR\\\\b'
             AND UPPER(r.study_name) NOT REGEXP '\\\\bHLA\\\\b|\\\\bTYPING\\\\b|\\\\bDQ\\\\b|\\\\bDP\\\\b'                                                                 THEN 'Digital Radiography'
        WHEN sk.study_name_upper REGEXP '\\\\bRF\\\\b'
             AND UPPER(r.study_name) NOT REGEXP '\\\\bRHEUMATOID\\\\b|\\\\bFACTOR\\\\b|\\\\bANTI-CCP\\\\b|\\\\bANA\\\\b|\\\\bSERUM\\\\b|\\\\bTITER\\\\b'                  THEN 'Radio Fluoroscopy'
        WHEN sk.study_name_upper REGEXP '\\\\bFL\\\\b|\\\\bFLUORO\\\\b|\\\\bFLU\\\\b'                                                                                     THEN 'Radio Fluoroscopy'
        WHEN sk.study_name_upper REGEXP '\\\\bFS\\\\b'                                                                                                                     THEN 'Fundoscopy (FS - Retired) / Ophthalmic Photography'
        WHEN sk.study_name_upper REGEXP '\\\\bNM\\\\b'                                                                                                                     THEN 'Nuclear Medicine'
        WHEN sk.study_name_upper REGEXP '\\\\bECHO\\\\b|\\\\bECHOCARDIOGRAM\\\\b'                                                                                          THEN 'Echocardiography (EC - Retired) / Ultrasound'
        WHEN sk.study_name_upper REGEXP '\\\\bECG\\\\b|\\\\bEKG\\\\b'                                                                                                     THEN 'Electrocardiography'
        WHEN sk.study_name_upper REGEXP '\\\\bEEG\\\\b|\\\\bELECTROCEPHANLOGRAM\\\\b'                                                                                     THEN 'Electroencephalography'
        WHEN sk.study_name_upper REGEXP '\\\\bENDOSCOPY\\\\b'                                                                                                              THEN 'Endoscopy'
        WHEN sk.study_name_upper REGEXP '\\\\bCD\\\\b'                                                                                                                     THEN 'Color Flow Doppler (CD - Retired) / Ultrasound'
        WHEN sk.study_name_upper REGEXP '\\\\bTCD\\\\b|\\\\bDUPLEX\\\\b|\\\\bDOPPLER\\\\b'                                                                                THEN 'Duplex Doppler (DD - Retired) / Ultrasound'
        WHEN sk.study_name_upper REGEXP '\\\\bAUDIO\\\\b|\\\\bAUDIOMETRY\\\\b|\\\\bAUDITORY\\\\b|\\\\bHEARING\\\\b|\\\\bAUDIOGRAM\\\\b|\\\\bACOUSTIC\\\\b'               THEN 'Audio'
        WHEN sk.study_name_upper REGEXP '\\\\bRP\\\\b'                                                                                                                     THEN 'Radiotherapy Plan'
        WHEN sk.study_name_upper REGEXP '\\\\bRT\\\\b'
             AND UPPER(r.study_name) NOT REGEXP '\\\\bCREATININE\\\\b|\\\\bCREAT\\\\b|\\\\bRENAL\\\\b|\\\\bKIDNEY\\\\b'                                                   THEN 'Radiographic Imaging (RG) / Interventional Radiology'
        WHEN sk.study_name_upper REGEXP '\\\\bRAD\\\\b|\\\\bIR\\\\b|\\\\bINTERVENTIONAL RADIOLOGY\\\\b'                                                                   THEN 'Radiographic Imaging (RG) / Interventional Radiology'
        WHEN sk.study_name_upper REGEXP '\\\\bSPECT\\\\b'                                                                                                                  THEN 'Single-Photon Emission Computed Tomography (ST - Retired) / Nuclear Medicine'
        WHEN sk.study_name_upper REGEXP '\\\\bBX\\\\b|\\\\bBIOPSY\\\\b|\\\\bVL\\\\b|\\\\bOHS\\\\b|\\\\bI-123\\\\b|\\\\b1-131\\\\b|\\\\bMPI\\\\b'                         THEN 'Other'
        ELSE NULL
    END,

    r.strength_views_std = CASE
        WHEN sk.study_name_upper REGEXP '[0-9]+\\\\.?[0-9]*\\\\s*T\\\\b'
            AND sk.study_name_upper REGEXP 'W[/\\\\s]?WO|W\\\\s?&\\\\s?W/?O|W\\\\s?AND\\\\s?W/?O|WITHOUT/WITH|WITH/WITHOUT|WWO|WO/W|\\\\bWO\\\\b|\\\\bW/O\\\\b|\\\\bWITHOUT\\\\b|\\\\bWO CON\\\\b|\\\\bNCON\\\\b|\\\\bNO CON\\\\b|\\\\bWO C\\\\b|\\\\bWO CONTRAST\\\\b|\\\\bW CON\\\\b|\\\\bW CONTRAST\\\\b|\\\\bWITH CONTRAST\\\\b|\\\\bW C\\\\b|\\\\bW/\\\\b|\\\\bCON\\\\b'
            THEN CONCAT(REGEXP_SUBSTR(r.study_name, '[0-9]+\\\\.?[0-9]*(?=\\\\s*[Tt]\\\\b)'), 'T')
        WHEN sk.study_name_upper REGEXP '[0-9]+-[0-9]+\\\\s*V\\\\b'
            THEN CONCAT(REGEXP_SUBSTR(r.study_name, '[0-9]+-[0-9]+(?=\\\\s*[Vv]\\\\b)'), ' Views')
        WHEN sk.study_name_upper REGEXP '[0-9]+\\\\s*V\\\\b'
            THEN CONCAT(REGEXP_SUBSTR(r.study_name, '[0-9]+(?=\\\\s*[Vv]\\\\b)'), ' Views')
        WHEN sk.study_name_upper REGEXP '[0-9]+-[0-9]+\\\\s*VIEW.*(\\\\+|AND).*[0-9]+-[0-9]+\\\\s*VIEW|[0-9]+\\\\s*VIEWS?.*\\\\+.*[0-9]+\\\\s*VIEW'
            THEN CONCAT(REGEXP_SUBSTR(r.study_name, '[0-9]+-[0-9]+|[0-9]+(?=\\\\s*VIEWS?)'), ' Views + ', REGEXP_SUBSTR(r.study_name, '[0-9]+(?=\\\\s*VIEW)', 1, 2), ' View')
        WHEN sk.study_name_upper REGEXP '[><=]{{1,2}}\\\\s*[0-9]+\\\\s*VIEWS?'
            THEN CONCAT(REGEXP_SUBSTR(r.study_name, '[><=]{{1,2}}'), REGEXP_SUBSTR(r.study_name, '[0-9]+'), ' Views')
        WHEN sk.study_name_upper REGEXP '[0-9]+-[0-9]+\\\\s*VIEW'
            THEN CONCAT(REGEXP_SUBSTR(r.study_name, '[0-9]+-[0-9]+'), ' Views')
        WHEN sk.study_name_upper REGEXP '(MIN|MINIMUM)(\\\\s+OF)?\\\\s*(ONE|TWO|THREE|FOUR|FIVE|SIX|[0-9]+)\\\\s*(V\\\\b|VWS|VIEW|VIEWS)'
            THEN CONCAT('Min ', COALESCE(REGEXP_SUBSTR(r.study_name, '[0-9]+(?=\\\\s*(V\\\\b|VWS|VIEW|VIEWS))'), CASE WHEN sk.study_name_upper REGEXP 'ONE\\\\s*(V|VIEW|VIEWS)' THEN '1' WHEN sk.study_name_upper REGEXP 'TWO\\\\s*(V|VIEW|VIEWS)' THEN '2' WHEN sk.study_name_upper REGEXP 'THREE\\\\s*(V|VIEW|VIEWS)' THEN '3' WHEN sk.study_name_upper REGEXP 'FOUR\\\\s*(V|VIEW|VIEWS)' THEN '4' END), ' Views')
        WHEN sk.study_name_upper REGEXP '[0-9]+\\\\s*[\\\\+]\\\\s*VIEWS?|[0-9]+\\\\s*PLUS\\\\s*VIEWS?'
            THEN CONCAT(REGEXP_SUBSTR(r.study_name, '[0-9]+'), '+ Views')
        WHEN sk.study_name_upper REGEXP '[0-9]+\\\\s*OR\\\\s*MORE\\\\s*VIEWS?'
            THEN CONCAT(REGEXP_SUBSTR(r.study_name, '[0-9]+'), ' or More Views')
        WHEN sk.study_name_upper REGEXP '[0-9]+\\\\s*OR\\\\s*[0-9]+\\\\s*VIEWS?'
            THEN CONCAT(REGEXP_SUBSTR(r.study_name, '[0-9]+'), ' or ', REGEXP_SUBSTR(r.study_name, '[0-9]+', 1, 2), ' Views')
        WHEN sk.study_name_upper REGEXP '\\\\b(ONE|TWO|THREE|FOUR|FIVE|SIX)\\\\s*VIEWS?\\\\b'
            THEN CONCAT(CASE WHEN sk.study_name_upper REGEXP '\\\\bONE\\\\s*VIEWS?' THEN '1' WHEN sk.study_name_upper REGEXP '\\\\bTWO\\\\s*VIEWS?' THEN '2' WHEN sk.study_name_upper REGEXP '\\\\bTHREE\\\\s*VIEWS?' THEN '3' WHEN sk.study_name_upper REGEXP '\\\\bFOUR\\\\s*VIEWS?' THEN '4' WHEN sk.study_name_upper REGEXP '\\\\bFIVE\\\\s*VIEWS?' THEN '5' WHEN sk.study_name_upper REGEXP '\\\\bSIX\\\\s*VIEWS?' THEN '6' END, ' Views')
        WHEN sk.study_name_upper REGEXP '(LESS\\\\s*THAN|<)\\\\s*[0-9]+\\\\s*(V\\\\b|VIEW|VIEWS)'
            THEN CONCAT('Less Than ', REGEXP_SUBSTR(r.study_name, '[0-9]+(?=\\\\s*(V\\\\b|VIEW|VIEWS))'), ' Views')
        WHEN sk.study_name_upper REGEXP '[0-9]+\\\\+?\\\\s*VIEWS?'
            THEN CONCAT(REGEXP_SUBSTR(r.study_name, '[0-9]+'), ' Views')
        ELSE NULL
    END,

    r.contrast_type_std = CASE
        WHEN sk.study_name_upper REGEXP 'W[/\\\\s]?WO|W\\\\s?&\\\\s?W/?O|W\\\\s?AND\\\\s?W/?O|W\\\\s?OR\\\\s?W/?O|WITH\\\\s?AND\\\\s?W/?O|WO\\\\+W|W\\\\+W/?O|WITHOUT/WITH|WITH/WITHOUT|W\\\\s?AND\\\\s?WOW|WO,\\\\s?W|W,\\\\s?WO|WWO|W/W/O|WO/W|W/&W/O|W AND OR WO|W WO|W\\\\s?W/?O|WO\\\\s?W'
            THEN 'With and Without Contrast'
        WHEN sk.study_name_upper REGEXP '\\\\bWO\\\\b|\\\\bW/O\\\\b|\\\\bWITHOUT\\\\b|\\\\bWO CON\\\\b|\\\\bW/O CONTRAST\\\\b|\\\\bNCON\\\\b|\\\\bNO CON\\\\b|\\\\bWO C\\\\b|\\\\bWO CONTRAST\\\\b'
            THEN 'Without Contrast'
        WHEN sk.study_name_upper REGEXP '\\\\bW CON\\\\b|\\\\bW CONTRAST\\\\b|\\\\bWITH CONTRAST\\\\b|\\\\bW C\\\\b|\\\\bW/\\\\b|\\\\bCON\\\\b'
            THEN 'With Contrast'
        ELSE NULL
    END

WHERE r.{BATCH_KEY} >= {pk_lo}
  AND r.{BATCH_KEY} <  {pk_hi}
"""


def build_pass2(pk_lo, pk_hi):
    """
    Pass 2: body_part_std, laterality_std, tracer_name_std.
    Uses sk.study_name_upper from pre-built STAGING_STUDY_KEYS — no UPPER() at query time.
    """
    return f"""
UPDATE {TARGET_TABLE} r
JOIN {STAGING_STUDY_KEYS} sk
    ON r.{BATCH_KEY} = sk.{BATCH_KEY}
SET
    r.body_part_std = CASE
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
        WHEN sk.study_name_upper REGEXP '\\\\bBRAIN\\\\b|\\\\bBRAINSTEM\\\\b|\\\\bBRIAN\\\\b|\\\\bBRIN\\\\b'                                                 THEN 'Brain'
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
        WHEN sk.study_name_upper REGEXP '\\\\bT-SPINE\\\\b|\\\\bT SPINE\\\\b'                                                                                 THEN 'Spine'
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
    END,

    r.laterality_std = CASE
        WHEN sk.study_name_upper REGEXP '\\\\bBILATERAL\\\\b'      THEN 'Bilateral'
        WHEN sk.study_name_upper REGEXP '\\\\bUNILATERAL\\\\b'     THEN 'Unilateral'
        WHEN sk.study_name_upper REGEXP '\\\\bLEFT\\\\b|\\\\bLT\\\\b'  THEN 'Left'
        WHEN sk.study_name_upper REGEXP '\\\\bRIGHT\\\\b|\\\\bRT\\\\b' THEN 'Right'
        ELSE NULL
    END,

    r.tracer_name_std = CASE
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
    END

WHERE r.{BATCH_KEY} >= {pk_lo}
  AND r.{BATCH_KEY} <  {pk_hi}
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
        ("extracted_codes",   "TEXT"),
        ("proc_code_std",     "TEXT"),
        ("modality_std",        "VARCHAR(200)"),
        ("strength_views_std",  "VARCHAR(200)"),
        ("contrast_type_std",   "VARCHAR(50)"),
        ("body_part_std",     "VARCHAR(200)"),
        ("laterality_std",    "VARCHAR(20)"),
        ("tracer_name_std",   "VARCHAR(200)"),
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

    # ── 1. Code lookup staging (CTE materialized once) ────────────────
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
                        IF(study_name NOT REGEXP '(^|[^0-9])[0-9]{{5}}([^0-9]|$)' AND study_name REGEXP '[A-Za-z]+[0-9]{{6,}}',
                            REGEXP_SUBSTR(study_name, '[A-Za-z]+[0-9]{{6,}}'), NULL),
                        NULLIF(REGEXP_SUBSTR(study_name, '\\\\b[A-Za-z][0-9]{{4}}\\\\b'), '')
                    )) AS extracted_codes
                FROM (
                    SELECT REGEXP_REPLACE(
                            REGEXP_REPLACE(study_name,
                                '([0-9]{{5}})\\\\s+[Oo][Rr]\\\\s+([0-9]{{5}})', '\\\\1,\\\\2'),
                            '([0-9]{{5}})\\\\s*/\\\\s*([0-9]{{5}})', '\\\\1,\\\\2')
                           AS study_name
                    FROM {TARGET_TABLE}
                    WHERE study_name IS NOT NULL
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
    print("  Materializing study keys (pre-computed REGEXP_REPLACE + UPPER)...")
    if not _table_exists(cur, STAGING_STUDY_KEYS):
        cur.execute(f"""
            CREATE TABLE {STAGING_STUDY_KEYS} AS
            SELECT
                {BATCH_KEY},
                UPPER(study_name) AS study_name_upper,
                REGEXP_REPLACE(
                    REGEXP_REPLACE(study_name,
                        '([0-9]{{5}})\\\\s+[Oo][Rr]\\\\s+([0-9]{{5}})', '\\\\1,\\\\2'),
                    '([0-9]{{5}})\\\\s*/\\\\s*([0-9]{{5}})', '\\\\1,\\\\2'
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

    # ── 4. Shared PK staging — all rows ──────────────────────────────
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
    print(f"  Radiology Standardisation UPDATE (v2) — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  target     : {TARGET_TABLE}")
    print(f"  batch_key  : {BATCH_KEY}")
    print(f"  batch_size : {BATCH_SIZE:,}")
    print(f"  passes     : 2  (code lookup + modality + contrast | body part + laterality + tracer)")
    print(f"{'='*70}\n", flush=True)

    print("  Connecting to database...")
    sys.stdout.flush()
    all_ranges = setup_tables()

    passes = [
        (CHECKPOINT_PASS1, "Pass 1 — code lookup + modality + contrast (all rows)", build_pass1),
        (CHECKPOINT_PASS2, "Pass 2 — body part + laterality + tracer (all rows)",   build_pass2),
    ]

    results    = {}
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
        print(f"  [{tag}] {label:<55}  {rows:>10,} rows  ({secs}s)")
        if status.startswith("FAILED"):
            print(f"         {status}")

    print(f"\n  Total rows updated: {total_rows:,}")
    print(f"{'='*70}")

    print(f"\n  Cleanup SQL (run after verifying data):")
    print(f"    -- Shared lookup (only drop when done with ALL radiology tables):")
    print(f"    -- DROP TABLE IF EXISTS {STAGING_CODE_LOOKUP};")
    print(f"    -- Per-run tables:")
    print(f"    DROP TABLE IF EXISTS {STAGING_STUDY_KEYS};")
    print(f"    DROP TABLE IF EXISTS {STAGING_PK};")
    print(f"    DROP TABLE IF EXISTS {CHECKPOINT_TABLE};")

    if any_failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
