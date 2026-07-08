#!/usr/bin/env python3
"""
Optimized batched standardisation UPDATEs for: rgd_udm_silver.radiology

Change TARGET_TABLE at the top to run against a different radiology table.

Two sequential passes — each with checkpoint/resume:

  Pass 1 — All rows:
    SET extracted_codes, cpt_codes_std, hcpcs_codes_std, code_descriptions,
        cpt_count_flag, modality, modality_combined, contrast_type
    JOIN staging.rad_std_code_lookup (pre-materialized CTE)

  Pass 2 — All rows:
    SET body_part, laterality, pet_tracer_name, pet_hcpcs_code, is_pet_study
    Inline CASE — no JOIN

Pre-materialized lookup table (computed ONCE, reused across all batches):
  - staging.rad_std_code_lookup  (full CTE from radiology_stand.sql)

Std columns added to target table if not present (with metadata lock guard).

Optimizations applied:
- Code lookup pre-materialized once (not re-scanned per batch)
- Shared PK staging table (all rows where udm_inc_id IS NOT NULL)
- Server-side boundary sampling (avoids loading all PKs into memory)
- Commit after every batch (frees undo/log space)
- Checkpoint/resume per pass — re-run skips completed passes
- Disabled InnoDB checks per-session for bulk update speed
- Progress bar via tqdm

Usage:
    python radiology_stand_opt.py
"""

import sys
import time
from datetime import datetime
import pymysql
from tqdm import tqdm
import os
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())

# ── Configuration ─────────────────────────────────────────────────────
DB_CONFIG = {
    "host":            os.environ.get("DB_INTERNAL_HOST"),
    "port":            3306,
    "user":            os.environ.get("DB_INTERNAL_USER"),
    "password":        os.environ.get("DB_INTERNAL_PASSWORD"),
    "database":        "rgd_udm_silver",
    "charset":         "utf8mb4",
    "connect_timeout": 30,
    "read_timeout":    21600,
    "write_timeout":   21600,
}

BATCH_SIZE = 50_000

# ── Change this to run against a different radiology table ─────────────
TARGET_TABLE = "kinsula_leq.radiology"

# ─────────────────────────────────────────────────────────────────────
_TABLE_SUFFIX = TARGET_TABLE.replace(".", "_").replace("-", "_")

STAGING_CODE_LOOKUP = "staging.rad_std_code_lookup"       # shared CTE materialization
STAGING_PK          = f"staging.rad_std_pk_{_TABLE_SUFFIX}"
CHECKPOINT_TABLE    = f"staging.etl_checkpoint_rad_std_{_TABLE_SUFFIX}"
CHECKPOINT_PASS1    = f"radiology.std.pass1.codelookup.{_TABLE_SUFFIX}"
CHECKPOINT_PASS2    = f"radiology.std.pass2.bodypart_pet.{_TABLE_SUFFIX}"

BATCH_KEY = "udm_inc_id"   # integer PK on radiology tables


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
    """Pass 1: code lookup + modality + contrast_type via pre-materialized CTE."""
    return f"""
UPDATE {TARGET_TABLE} r
LEFT JOIN {STAGING_CODE_LOOKUP} l
    ON REGEXP_REPLACE(
           REGEXP_REPLACE(r.study_name,
               '([0-9]{{5}})\\\\s+[Oo][Rr]\\\\s+([0-9]{{5}})', '\\\\1,\\\\2'),
           '([0-9]{{5}})\\\\s*/\\\\s*([0-9]{{5}})', '\\\\1,\\\\2'
       ) = l.study_name
SET
    r.extracted_codes   = COALESCE(l.extracted_codes,   'NS'),
    r.cpt_codes_std     = COALESCE(l.cpt_codes_std,     'NS'),
    r.hcpcs_codes_std   = COALESCE(l.hcpcs_codes_std,   'NS'),
    r.code_descriptions = COALESCE(l.code_descriptions, 'NS'),
    r.cpt_count_flag    = COALESCE(l.cpt_count_flag,    'No CPT Code'),
    r.modality_std = CASE
        WHEN UPPER(r.study_name) REGEXP '\\\\bCT\\\\b|\\\\bCAT\\\\b|\\\\bNCT\\\\b|\\\\bLDCT\\\\b|\\\\bCTA\\\\b|\\\\bCTV\\\\b|\\\\bCTAC\\\\b|\\\\bCTC\\\\b|\\\\bCTP\\\\b' THEN 'Computed Tomography'
        WHEN UPPER(r.study_name) REGEXP '\\\\bPET\\\\b|\\\\bPT\\\\b' THEN 'Positron emission tomography (PET)'
        WHEN UPPER(r.study_name) REGEXP '\\\\bMRA\\\\b|\\\\bzzMRA\\\\b' THEN 'Magnetic resonance angiography'
        WHEN UPPER(r.study_name) REGEXP '\\\\bMRI\\\\b|\\\\bMRCP\\\\b|\\\\bMRV\\\\b|\\\\bTMRI\\\\b|\\\\b3TMRI\\\\b|\\\\bMR\\\\b' THEN 'Magnetic Resonance'
        WHEN UPPER(r.study_name) REGEXP '\\\\bMAM\\\\b|\\\\bMAMM\\\\b|\\\\bMAMMO\\\\b|\\\\bMMAMMO\\\\b|\\\\bMG\\\\b|\\\\bMAMMOGRAM\\\\b|\\\\bMAMMOGRAPHY\\\\b|\\\\bDEXA\\\\b|\\\\bDXA\\\\b' THEN 'Mammography'
        WHEN UPPER(r.study_name) REGEXP '\\\\bUS\\\\b|\\\\bULTRASOUND\\\\b|\\\\bUSV\\\\b|\\\\bBI US\\\\b|\\\\bOB US\\\\b' THEN 'Ultrasound'
        WHEN UPPER(r.study_name) REGEXP '\\\\bXA\\\\b|\\\\bANG\\\\b|\\\\bANGIO\\\\b' THEN 'X-Ray Angiography'
        WHEN UPPER(r.study_name) REGEXP '\\\\bCR\\\\b' THEN 'Computed Radiography'
        WHEN UPPER(r.study_name) REGEXP '\\\\bDX\\\\b|\\\\bDR\\\\b|\\\\bXR\\\\b|\\\\bX-RAY\\\\b|\\\\bXRAY\\\\b|\\\\bXRY\\\\b' THEN 'Digital Radiography'
        WHEN UPPER(r.study_name) REGEXP '\\\\bRF\\\\b|\\\\bFL\\\\b|\\\\bFLUORO\\\\b|\\\\bFLU\\\\b' THEN 'Radio Fluoroscopy'
        WHEN UPPER(r.study_name) REGEXP '\\\\bFS\\\\b' THEN 'Fundoscopy'
        WHEN UPPER(r.study_name) REGEXP '\\\\bNM\\\\b' THEN 'Nuclear Medicine'
        WHEN UPPER(r.study_name) REGEXP '\\\\bECHO\\\\b|\\\\bECHOCARDIOGRAM\\\\b' THEN 'Echocardiography'
        WHEN UPPER(r.study_name) REGEXP '\\\\bECG\\\\b|\\\\bEKG\\\\b' THEN 'Electrocardiography'
        WHEN UPPER(r.study_name) REGEXP '\\\\bEEG\\\\b|\\\\bELECTROCEPHANLOGRAM\\\\b' THEN 'Electroencephalography'
        WHEN UPPER(r.study_name) REGEXP '\\\\bENDOSCOPY\\\\b' THEN 'Endoscopy'
        WHEN UPPER(r.study_name) REGEXP '\\\\bCD\\\\b' THEN 'Color flow Doppler'
        WHEN UPPER(r.study_name) REGEXP '\\\\bTCD\\\\b|\\\\bDUPLEX\\\\b|\\\\bDOPPLER\\\\b' THEN 'Duplex Doppler'
        WHEN UPPER(r.study_name) REGEXP '\\\\bAUDIO\\\\b|\\\\bAUDIOMETRY\\\\b|\\\\bAUDITORY\\\\b|\\\\bHEARING\\\\b|\\\\bAUDIOGRAM\\\\b|\\\\bACOUSTIC\\\\b' THEN 'Audio'
        WHEN UPPER(r.study_name) REGEXP '\\\\bRP\\\\b' THEN 'Radiotherapy Plan'
        WHEN UPPER(r.study_name) REGEXP '\\\\bRT\\\\b|\\\\bRAD\\\\b|\\\\bIR\\\\b|\\\\bINTERVENTIONAL RADIOLOGY\\\\b' THEN 'Radiographic imaging'
        WHEN UPPER(r.study_name) REGEXP '\\\\bSPECT\\\\b' THEN 'Single-photon emission computed tomography (SPECT)'
        WHEN UPPER(r.study_name) REGEXP '\\\\bBX\\\\b|\\\\bBIOPSY\\\\b|\\\\bVL\\\\b|\\\\bOHS\\\\b|\\\\bI-123\\\\b|\\\\b1-131\\\\b|\\\\bMPI\\\\b' THEN 'Other'
        ELSE 'Other'
    END,
    r.modality_combined = CASE
        WHEN UPPER(r.study_name) REGEXP '\\\\bPET/CT\\\\b|\\\\bPET CT\\\\b' THEN 'Positron emission tomography (PET) / Computed Tomography'
        WHEN UPPER(r.study_name) REGEXP '\\\\bXR/RF\\\\b' THEN 'Digital Radiography / Radio Fluoroscopy'
        WHEN UPPER(r.study_name) REGEXP '\\\\bUS DOPPLER\\\\b|\\\\bUS DUPLEX\\\\b' THEN 'Ultrasound / Duplex Doppler'
        WHEN UPPER(r.study_name) REGEXP '\\\\bUS ECHOCARDIOGRAM\\\\b' THEN 'Ultrasound / Echocardiography'
        WHEN UPPER(r.study_name) REGEXP '\\\\bXA US\\\\b' THEN 'X-Ray Angiography / Ultrasound'
        WHEN UPPER(r.study_name) REGEXP '\\\\bCT\\\\b|\\\\bCAT\\\\b|\\\\bNCT\\\\b|\\\\bLDCT\\\\b|\\\\bCTA\\\\b|\\\\bCTV\\\\b|\\\\bCTAC\\\\b|\\\\bCTC\\\\b|\\\\bCTP\\\\b' THEN 'Computed Tomography'
        WHEN UPPER(r.study_name) REGEXP '\\\\bPET\\\\b|\\\\bPT\\\\b' THEN 'Positron emission tomography (PET)'
        WHEN UPPER(r.study_name) REGEXP '\\\\bMRA\\\\b|\\\\bzzMRA\\\\b' THEN 'Magnetic resonance angiography'
        WHEN UPPER(r.study_name) REGEXP '\\\\bMRI\\\\b|\\\\bMRCP\\\\b|\\\\bMRV\\\\b|\\\\bTMRI\\\\b|\\\\b3TMRI\\\\b|\\\\bMR\\\\b' THEN 'Magnetic Resonance'
        WHEN UPPER(r.study_name) REGEXP '\\\\bMAM\\\\b|\\\\bMAMM\\\\b|\\\\bMAMMO\\\\b|\\\\bMMAMMO\\\\b|\\\\bMG\\\\b|\\\\bMAMMOGRAM\\\\b|\\\\bMAMMOGRAPHY\\\\b|\\\\bDEXA\\\\b|\\\\bDXA\\\\b' THEN 'Mammography'
        WHEN UPPER(r.study_name) REGEXP '\\\\bUS\\\\b|\\\\bULTRASOUND\\\\b|\\\\bUSV\\\\b|\\\\bBI US\\\\b|\\\\bOB US\\\\b' THEN 'Ultrasound'
        WHEN UPPER(r.study_name) REGEXP '\\\\bXA\\\\b|\\\\bANG\\\\b|\\\\bANGIO\\\\b' THEN 'X-Ray Angiography'
        WHEN UPPER(r.study_name) REGEXP '\\\\bCR\\\\b' THEN 'Computed Radiography'
        WHEN UPPER(r.study_name) REGEXP '\\\\bDX\\\\b|\\\\bDR\\\\b|\\\\bXR\\\\b|\\\\bX-RAY\\\\b|\\\\bXRAY\\\\b|\\\\bXRY\\\\b' THEN 'Digital Radiography'
        WHEN UPPER(r.study_name) REGEXP '\\\\bRF\\\\b|\\\\bFL\\\\b|\\\\bFLUORO\\\\b|\\\\bFLU\\\\b' THEN 'Radio Fluoroscopy'
        WHEN UPPER(r.study_name) REGEXP '\\\\bFS\\\\b' THEN 'Fundoscopy'
        WHEN UPPER(r.study_name) REGEXP '\\\\bNM\\\\b' THEN 'Nuclear Medicine'
        WHEN UPPER(r.study_name) REGEXP '\\\\bECHO\\\\b|\\\\bECHOCARDIOGRAM\\\\b' THEN 'Echocardiography'
        WHEN UPPER(r.study_name) REGEXP '\\\\bECG\\\\b|\\\\bEKG\\\\b' THEN 'Electrocardiography'
        WHEN UPPER(r.study_name) REGEXP '\\\\bEEG\\\\b|\\\\bELECTROCEPHANLOGRAM\\\\b' THEN 'Electroencephalography'
        WHEN UPPER(r.study_name) REGEXP '\\\\bENDOSCOPY\\\\b' THEN 'Endoscopy'
        WHEN UPPER(r.study_name) REGEXP '\\\\bCD\\\\b' THEN 'Color flow Doppler'
        WHEN UPPER(r.study_name) REGEXP '\\\\bTCD\\\\b|\\\\bDUPLEX\\\\b|\\\\bDOPPLER\\\\b' THEN 'Duplex Doppler'
        WHEN UPPER(r.study_name) REGEXP '\\\\bAUDIO\\\\b|\\\\bAUDIOMETRY\\\\b|\\\\bAUDITORY\\\\b|\\\\bHEARING\\\\b|\\\\bAUDIOGRAM\\\\b|\\\\bACOUSTIC\\\\b' THEN 'Audio'
        WHEN UPPER(r.study_name) REGEXP '\\\\bRP\\\\b' THEN 'Radiotherapy Plan'
        WHEN UPPER(r.study_name) REGEXP '\\\\bRT\\\\b|\\\\bRAD\\\\b|\\\\bIR\\\\b|\\\\bINTERVENTIONAL RADIOLOGY\\\\b' THEN 'Radiographic imaging'
        WHEN UPPER(r.study_name) REGEXP '\\\\bSPECT\\\\b' THEN 'Single-photon emission computed tomography (SPECT)'
        WHEN UPPER(r.study_name) REGEXP '\\\\bBX\\\\b|\\\\bBIOPSY\\\\b|\\\\bVL\\\\b|\\\\bOHS\\\\b|\\\\bI-123\\\\b|\\\\b1-131\\\\b|\\\\bMPI\\\\b' THEN 'Other'
        ELSE 'Other'
    END,
    r.contrast_type = CASE
        WHEN UPPER(r.study_name) REGEXP 'W[/\\\\s]?WO|W\\\\s?&\\\\s?W/?O|W\\\\s?AND\\\\s?W/?O|W\\\\s?OR\\\\s?W/?O|WITH\\\\s?AND\\\\s?W/?O|WO\\\\+W|W\\\\+W/?O|WITHOUT/WITH|WITH/WITHOUT|W\\\\s?AND\\\\s?WOW|WO,\\\\s?W|W,\\\\s?WO|WWO|W/W/O|WO/W|W/&W/O|W AND OR WO|W WO|W\\\\s?W/?O|WO\\\\s?W'
            THEN 'With and Without Contrast'
        WHEN UPPER(r.study_name) REGEXP '\\\\bWO\\\\b|\\\\bW/O\\\\b|\\\\bWITHOUT\\\\b|\\\\bWO CON\\\\b|\\\\bW/O CONTRAST\\\\b|\\\\bNCON\\\\b|\\\\bNO CON\\\\b|\\\\bWO C\\\\b|\\\\bWO CONTRAST\\\\b'
            THEN 'Without Contrast'
        WHEN UPPER(r.study_name) REGEXP '\\\\bW CON\\\\b|\\\\bW CONTRAST\\\\b|\\\\bWITH CONTRAST\\\\b|\\\\bW C\\\\b|\\\\bW/\\\\b|\\\\bCON\\\\b'
            THEN 'With Contrast'
        ELSE 'No Contrast Info'
    END
WHERE r.{BATCH_KEY} >= {pk_lo}
  AND r.{BATCH_KEY} < {pk_hi}
"""


def build_pass2(pk_lo, pk_hi):
    """Pass 2: body_part, laterality, PET tracer columns — inline CASE only, no JOIN."""
    return f"""
UPDATE {TARGET_TABLE} r
SET
    r.body_part = CASE
        -- MULTI BODY PARTS FIRST

        -- Chest + Abdomen + Pelvis
        WHEN UPPER(r.study_name) REGEXP 'CHEST.*(ABDOMEN|ABD).*(PELVIS|PELV)|CHEST/ABD/PELVIS|CHEST ABDOMEN PELVIS|CHEST\\\\+ABD.*PELVIS'
            THEN 'Chest, Abdomen, Pelvis'

        -- Chest + Abdomen
        WHEN UPPER(r.study_name) REGEXP 'CHEST.*(ABDOMEN|ABD)|CHEST\\\\+ABD'
            THEN 'Chest, Abdomen'

        -- Chest + Thorax
        WHEN UPPER(r.study_name) REGEXP 'CHEST.*THORAX|CHEST/THORAX'
            THEN 'Chest, Thorax'

        -- Abdomen + Pelvis
        WHEN UPPER(r.study_name) REGEXP '(ABDOMEN|ABD).*(PELVIS|PELV)|(PELVIS|PELV).*(ABDOMEN|ABD)'
            THEN 'Abdomen, Pelvis'

        -- Head + Neck
        WHEN UPPER(r.study_name) REGEXP 'HEAD.*NECK|NECK.*HEAD|NECK/HEAD'
            THEN 'Head, Neck'

        -- Orbit + Face + Neck
        WHEN UPPER(r.study_name) REGEXP 'ORB.*FAC.*NCK|ORBIT.*FACE.*NECK|ORBIT/FACE/NK'
            THEN 'Orbit, Face, Neck'

        -- Orbit + Sella
        WHEN UPPER(r.study_name) REGEXP 'ORBIT.*SELLA|ORBIT\\\\+SELLA|ORBIT SELLA POSS|ORBIT\\\\+SELLA\\\\+PF'
            THEN 'Orbit, Sella'

        -- Thoracic + Lumbar Spine
        WHEN UPPER(r.study_name) REGEXP 'THORACO.?LUMBAR|THORACOLUMBAR|THOR.*LUM[B]|THOR\\\\+LU[MB]'
            THEN 'Thoracic Spine, Lumbar Spine'

        -- Lumbo-Sacral
        WHEN UPPER(r.study_name) REGEXP 'LUMBO.?SACRAL|LUMB.?SACR|LUMBO SACRAL|LUMBOSACRAL'
            THEN 'Lumbar Spine, Sacrum'

        -- Sacrum + Coccyx
        WHEN UPPER(r.study_name) REGEXP 'SACRUM.*(AND|\\\\+|/).?COCCYX|SACRUM COCCYX|SACRUM/COCCYX'
            THEN 'Sacrum, Coccyx'

        -- Facial / Sinus combined
        WHEN UPPER(r.study_name) REGEXP 'FACIAL.*SINUS|SINUS.*FACIAL|FACIAL/SINUS|MAX.*FAC.*SIN|MAXILLOFACIAL|MAXIOFACIAL|MAXFACIAL|MAXILLA|SINUS FACIAL|MAX/FAC/SIN|MAXFACIAL BONES'
            THEN 'Facial Bones, Sinuses'

        -- Tibia + Fibula
        WHEN UPPER(r.study_name) REGEXP 'TIB.?FIB|TIBIA.?FIBUL|TIBIA AND FIBULA|TIBIA/FIBULA|TIB & FIB|TIB\\\\+FIBULA|TIBIA\\\\+FIBULA|TM JOINTS'
            THEN 'Tibia, Fibula'

        -- Forearm / Radius / Ulna
        WHEN UPPER(r.study_name) REGEXP 'FOREARM.*RADIUS|FOREARM.*ULNA|RADIUS.*ULNA|FOREARM/RADIUS'
            THEN 'Forearm, Radius, Ulna'

        -- Carotid / Neck
        WHEN UPPER(r.study_name) REGEXP 'CAROTID.*NECK|CAROTID/NECK|VASC CAROTID|CAROTIDS'
            THEN 'Carotid, Neck'

        -- Thyroid / Neck
        WHEN UPPER(r.study_name) REGEXP 'THYROID.*NECK|THY.*NECK|THYROID/NECK'
            THEN 'Thyroid, Neck'

        -- Liver + Gallbladder + Pancreas
        WHEN UPPER(r.study_name) REGEXP 'LIVER.*GALLBLADDER.*PANCREAS|LIVER GALLBLADDER PANCREAS'
            THEN 'Liver, Gallbladder, Pancreas'

        -- Ilium + Sternum + Rib
        WHEN UPPER(r.study_name) REGEXP 'ILIUM.*STERNUM.*RIB|ILIUM STERNUM RIB'
            THEN 'Ilium, Sternum, Rib'

        -- AC Joints / Acromioclavicular
        WHEN UPPER(r.study_name) REGEXP 'AC JOINTS|ACROMIOCLAVICULAR JOINTS|STERNOCLAVIC|STERNOCLAVICULAR'
            THEN 'Acromioclavicular Joints'

        -- TMJ / Temporomandibular
        WHEN UPPER(r.study_name) REGEXP '\\\\bTMJ\\\\b|TEMPOROMANDIBULAR|TEMPOROMANDIBULAR JOINT|TMJ BILATERAL'
            THEN 'Temporomandibular Joint'

        -- Vascular Lower Extremity
        WHEN UPPER(r.study_name) REGEXP 'VASC EXT LOWR|VASC EXTREMITY LOWER|LOWER EXTREMITY VENOUS|LOWER EXT VENOUS|LOWER EXTREMITY ARTERIES|LOWER EXT ARTERIAL|ARTERIAL LOW.*EXT|ARTERIAL LOWER EXT|ARTERIAL LOWER EXTREMITY|LOWER EXTREMITY ARTERI'
            THEN 'Lower Extremity (Vascular)'

        -- Vascular Upper Extremity
        WHEN UPPER(r.study_name) REGEXP 'UPPER EXTREMITY VENOUS|UPPER EXT.*ARTERIAL|ARTERIAL UPPER EXT|ARTERIAL UPPER EXTREMITY|UPPER EXTREMITY ARTERIAL|LT UPPER VENOUS|UPPER OR LOWER EXT ARTERIAL'
            THEN 'Upper Extremity (Vascular)'

        -- Vascular Transcranial
        WHEN UPPER(r.study_name) REGEXP 'VASC TRANSCRANIAL|TRANS CRANIAL|TRANSCRANIAL|VASC JUGULAR|JUGULAR.*SUBCLAVIAN'
            THEN 'Transcranial (Vascular)'

        -- Cerebral Arteries
        WHEN UPPER(r.study_name) REGEXP 'CEREBRAL ARTERIES|EXTRACRANIAL ARTERIES'
            THEN 'Cerebral Arteries'

        -- Lower Extremity (general)
        WHEN UPPER(r.study_name) REGEXP '\\\\bLOWER EXTREMIT|\\\\bLOWER EXT\\\\b|\\\\bLOWER EXTR\\\\b|\\\\bLWR EXT\\\\b|\\\\bLE\\\\b|\\\\bLOW EXT\\\\b|\\\\bLEFT LOWER EXTREMITY\\\\b|\\\\bRIGHT LOWER EXTREMITY\\\\b|LOWER EXT.*NOT.*JNT|LOWER EXT.*NON|LWR EXT NOT JT|LOWER LEG|LOWER BACK|EXTREMITY LOWER|EXTREMITY.*LOWER'
            THEN 'Lower Extremity'

        -- Upper Extremity (general)
        WHEN UPPER(r.study_name) REGEXP '\\\\bUPPER EXTREMIT|\\\\bUPPER EXT\\\\b|\\\\bUPR.*EXT\\\\b|\\\\bUPR/LXTR\\\\b|UP EXT JT|LEFT EXTREMITY.*UPPER|UPPER EXT.*JOINT|UPPER EXT NON JOINT|UPPER EXTREMITY.*MUSCULO|EXTREMITY UPPER|EXTREMITY.*UPPER|LEFT EXTREMITY JOINT UPPER|RIGHT EXTREMITY JOINT LOWER'
            THEN 'Upper Extremity'

        -- Brachial Plexus
        WHEN UPPER(r.study_name) REGEXP 'BRACHIAL PLEXUS|RIGHT BRACHIAL PLEXUS'
            THEN 'Brachial Plexus'

        -- Spinal Canal / Cord
        WHEN UPPER(r.study_name) REGEXP 'SPINAL CANAL|SPINAL CORD|THORACIC SPINAL CORD|SPINAL CORD DORSAL'
            THEN 'Spinal Canal'

        -- Sacroiliac
        WHEN UPPER(r.study_name) REGEXP 'SACROILIAC|SACROILIAC JOINT|SACROILIAC JNTS|SI JOINT'
            THEN 'Sacroiliac Joint'

        -- SINGLE BODY PARTS (alphabetical)

        -- Abdomen
        WHEN UPPER(r.study_name) REGEXP '\\\\bABDOMEN\\\\b|\\\\bABD\\\\b|\\\\bABDOMINAL\\\\b'
            THEN 'Abdomen'

        -- Ankle
        WHEN UPPER(r.study_name) REGEXP '\\\\bANKLE\\\\b|\\\\bANK\\\\b'
            THEN 'Ankle'

        -- Aorta
        WHEN UPPER(r.study_name) REGEXP '\\\\bAORTA\\\\b|\\\\bTHORACIC AORTA\\\\b'
            THEN 'Aorta'

        -- Artery / Arterial
        WHEN UPPER(r.study_name) REGEXP '\\\\bARTERY\\\\b|\\\\bARTERIAL\\\\b'
            THEN 'Arterial'

        -- Auditory Canal
        WHEN UPPER(r.study_name) REGEXP 'AUDITORY CANAL'
            THEN 'Auditory Canal'

        -- Axial Skeleton
        WHEN UPPER(r.study_name) REGEXP 'AXIAL SKELETON'
            THEN 'Axial Skeleton'

        -- Bone
        WHEN UPPER(r.study_name) REGEXP '\\\\bBONE\\\\b'
            THEN 'Bone'

        -- Bowel
        WHEN UPPER(r.study_name) REGEXP '\\\\bBOWEL\\\\b'
            THEN 'Bowel'

        -- Brain
        WHEN UPPER(r.study_name) REGEXP '\\\\bBRAIN\\\\b|\\\\bBRAINSTEM\\\\b|\\\\bBRIAN\\\\b|\\\\bBRIN\\\\b'
            THEN 'Brain'

        -- Breast
        WHEN UPPER(r.study_name) REGEXP '\\\\bBREAST\\\\b|\\\\bBREASTS\\\\b'
            THEN 'Breast'

        -- Calf
        WHEN UPPER(r.study_name) REGEXP '\\\\bCALF\\\\b'
            THEN 'Calf'

        -- Carotid
        WHEN UPPER(r.study_name) REGEXP '\\\\bCAROTID\\\\b|\\\\bCAROTIDS\\\\b'
            THEN 'Carotid'

        -- Cervical Spine
        WHEN UPPER(r.study_name) REGEXP '\\\\bCERVICAL SPINE\\\\b|\\\\bSPINE CERVICAL\\\\b|\\\\bC-SPINE\\\\b'
            THEN 'Cervical Spine'

        -- Cervical
        WHEN UPPER(r.study_name) REGEXP '\\\\bCERVICAL\\\\b'
            THEN 'Cervical'

        -- Chest
        WHEN UPPER(r.study_name) REGEXP '\\\\bCHEST\\\\b|\\\\bPA CHEST\\\\b|\\\\bCHEST PA\\\\b|\\\\bTHORAX\\\\b|\\\\bRIBS\\\\b|\\\\bPNEUMOTHORAX\\\\b|\\\\bTHORACENTESIS\\\\b'
            THEN 'Chest'

        -- Clavicle
        WHEN UPPER(r.study_name) REGEXP '\\\\bCLAVICLE\\\\b'
            THEN 'Clavicle'

        -- Coccyx
        WHEN UPPER(r.study_name) REGEXP '\\\\bCOCCYX\\\\b'
            THEN 'Coccyx'

        -- Colon / Large Intestine
        WHEN UPPER(r.study_name) REGEXP '\\\\bCOLON\\\\b|\\\\bLARGE INTESTINE\\\\b'
            THEN 'Colon'

        -- Cranial Nerve
        WHEN UPPER(r.study_name) REGEXP 'CRANIAL NERVE'
            THEN 'Cranial Nerve'

        -- Ear
        WHEN UPPER(r.study_name) REGEXP '\\\\bEAR\\\\b'
            THEN 'Ear'

        -- Elbow
        WHEN UPPER(r.study_name) REGEXP '\\\\bELBOW\\\\b|\\\\bELB\\\\b'
            THEN 'Elbow'

        -- Esophagus
        WHEN UPPER(r.study_name) REGEXP '\\\\bESOPHAGUS\\\\b|\\\\bTRANSESOPHAGEAL\\\\b'
            THEN 'Esophagus'

        -- Extracranial
        WHEN UPPER(r.study_name) REGEXP '\\\\bEXTRACRANIAL\\\\b|\\\\bEXTRACRAN\\\\b'
            THEN 'Extracranial'

        -- Eye / Orbit
        WHEN UPPER(r.study_name) REGEXP '\\\\bEYE\\\\b|\\\\bORBIT\\\\b|\\\\bORBITS\\\\b|\\\\bORB\\\\b|OPTIC NERVE'
            THEN 'Eye / Orbit'

        -- Face
        WHEN UPPER(r.study_name) REGEXP '\\\\bFACE\\\\b|\\\\bFACIAL\\\\b|\\\\bFACIAL BONES\\\\b'
            THEN 'Face'

        -- Femur
        WHEN UPPER(r.study_name) REGEXP '\\\\bFEMUR\\\\b|\\\\bFEM\\\\b|\\\\bLATERAL FEMORAL\\\\b'
            THEN 'Femur'

        -- Fingers
        WHEN UPPER(r.study_name) REGEXP '\\\\bFINGERS\\\\b|\\\\bFINGER\\\\b'
            THEN 'Fingers'

        -- Foot / Feet
        WHEN UPPER(r.study_name) REGEXP '\\\\bFOOT\\\\b|\\\\bFEET\\\\b|\\\\bFT\\\\b|\\\\bHEEL\\\\b'
            THEN 'Foot'

        -- Forearm
        WHEN UPPER(r.study_name) REGEXP '\\\\bFOREARM\\\\b|\\\\bFORE\\\\b'
            THEN 'Forearm'

        -- Gallbladder
        WHEN UPPER(r.study_name) REGEXP '\\\\bGALLBLADDER\\\\b'
            THEN 'Gallbladder'

        -- Gastric / GI / UGI
        WHEN UPPER(r.study_name) REGEXP '\\\\bGI\\\\b|\\\\bUGI\\\\b|\\\\bGASTRIC\\\\b|\\\\bGASTROINTESTINAL\\\\b|\\\\bSMALL INTESTINE\\\\b|\\\\bSTOMACH\\\\b'
            THEN 'Gastric / GI'

        -- Greater Occipital
        WHEN UPPER(r.study_name) REGEXP 'GREATER OCCIPITAL|OCCIPITAL'
            THEN 'Occipital'

        -- Groin
        WHEN UPPER(r.study_name) REGEXP '\\\\bGROIN\\\\b|\\\\bILIOINGUINAL\\\\b'
            THEN 'Groin'

        -- Hand
        WHEN UPPER(r.study_name) REGEXP '\\\\bHAND\\\\b|\\\\bHANDS\\\\b'
            THEN 'Hand'

        -- Head
        WHEN UPPER(r.study_name) REGEXP '\\\\bHEAD\\\\b|\\\\bORBITS\\\\b'
            THEN 'Head'

        -- Heart
        WHEN UPPER(r.study_name) REGEXP '\\\\bHEART\\\\b|\\\\bTRANSTHORACIC\\\\b'
            THEN 'Heart'

        -- Hip
        WHEN UPPER(r.study_name) REGEXP '\\\\bHIP\\\\b|\\\\bHIPS\\\\b'
            THEN 'Hip'

        -- Humerus / Upper Arm
        WHEN UPPER(r.study_name) REGEXP '\\\\bHUMERUS\\\\b|\\\\bHUM\\\\b|\\\\bUPPER ARM\\\\b'
            THEN 'Humerus / Upper Arm'

        -- Intracranial
        WHEN UPPER(r.study_name) REGEXP '\\\\bINTRACRANIAL\\\\b|\\\\bINTRACRAN\\\\b'
            THEN 'Intracranial'

        -- Kidney
        WHEN UPPER(r.study_name) REGEXP '\\\\bKIDNEY\\\\b|\\\\bKIDNEYS\\\\b|\\\\bRENAL\\\\b'
            THEN 'Kidney'

        -- Knee
        WHEN UPPER(r.study_name) REGEXP '\\\\bKNEE\\\\b|\\\\bKNEES\\\\b|\\\\bKN\\\\b'
            THEN 'Knee'

        -- Leg
        WHEN UPPER(r.study_name) REGEXP '\\\\bLEG\\\\b|\\\\bLOWER LEG\\\\b'
            THEN 'Leg'

        -- Liver
        WHEN UPPER(r.study_name) REGEXP '\\\\bLIVER\\\\b'
            THEN 'Liver'

        -- Lumbar Plexus
        WHEN UPPER(r.study_name) REGEXP 'LUMBAR PLEXUS|LUMPLEX'
            THEN 'Lumbar Plexus'

        -- Lumbar Spine
        WHEN UPPER(r.study_name) REGEXP '\\\\bLUMBAR SPINE\\\\b|\\\\bSPINE LUMBAR\\\\b|\\\\bLUMBOSACRAL\\\\b|\\\\bLUMOSACRAL\\\\b'
            THEN 'Lumbar Spine'

        -- Lumbar
        WHEN UPPER(r.study_name) REGEXP '\\\\bLUMBAR\\\\b'
            THEN 'Lumbar'

        -- Lung
        WHEN UPPER(r.study_name) REGEXP '\\\\bLUNG\\\\b'
            THEN 'Lung'

        -- Lymph Node
        WHEN UPPER(r.study_name) REGEXP 'LYMPH NODE'
            THEN 'Lymph Node'

        -- Mandible
        WHEN UPPER(r.study_name) REGEXP '\\\\bMANDIBLE\\\\b'
            THEN 'Mandible'

        -- Mastoids
        WHEN UPPER(r.study_name) REGEXP '\\\\bMASTOIDS\\\\b|\\\\bMASTOID\\\\b'
            THEN 'Mastoids'

        -- Neck
        WHEN UPPER(r.study_name) REGEXP '\\\\bNECK\\\\b|\\\\bNECK SOFT TISSUE\\\\b|\\\\bTHROAT\\\\b'
            THEN 'Neck'

        -- Pancreas
        WHEN UPPER(r.study_name) REGEXP '\\\\bPANCREAS\\\\b'
            THEN 'Pancreas'

        -- Parathyroid
        WHEN UPPER(r.study_name) REGEXP '\\\\bPARATHYROID\\\\b'
            THEN 'Parathyroid'

        -- Pelvis
        WHEN UPPER(r.study_name) REGEXP '\\\\bPELVIS\\\\b|\\\\bPELVIC\\\\b'
            THEN 'Pelvis'

        -- Pituitary
        WHEN UPPER(r.study_name) REGEXP '\\\\bPITUITARY\\\\b|\\\\bPITUITARY GLAND\\\\b|\\\\bSELLA TURCICA\\\\b'
            THEN 'Pituitary'

        -- Prostate / Rectal
        WHEN UPPER(r.study_name) REGEXP '\\\\bPROSTATE\\\\b|\\\\bRECTAL\\\\b'
            THEN 'Prostate / Rectal'

        -- Retroperitoneum
        WHEN UPPER(r.study_name) REGEXP '\\\\bRETROPERITONEUM\\\\b'
            THEN 'Retroperitoneum'

        -- Sacrum
        WHEN UPPER(r.study_name) REGEXP '\\\\bSACRUM\\\\b'
            THEN 'Sacrum'

        -- Scapula
        WHEN UPPER(r.study_name) REGEXP '\\\\bSCAPULA\\\\b|\\\\bSCAP\\\\b'
            THEN 'Scapula'

        -- Scoliosis
        WHEN UPPER(r.study_name) REGEXP '\\\\bSCOLIOSIS\\\\b'
            THEN 'Scoliosis'

        -- Scrotal / Testicular
        WHEN UPPER(r.study_name) REGEXP '\\\\bSCROTAL\\\\b|\\\\bSCROTUM\\\\b|\\\\bTESTICULAR\\\\b|\\\\bTESTICLE\\\\b|\\\\bTESTES\\\\b'
            THEN 'Scrotal / Testicular'

        -- Shoulder
        WHEN UPPER(r.study_name) REGEXP '\\\\bSHOULDER\\\\b|\\\\bSH\\\\b|UPPER EXT JOINT SHOULDER'
            THEN 'Shoulder'

        -- Sinuses
        WHEN UPPER(r.study_name) REGEXP '\\\\bSINUS\\\\b|\\\\bSINUSES\\\\b|\\\\bNASAL\\\\b|\\\\bSINUS/NASAL\\\\b'
            THEN 'Sinuses'

        -- Skull
        WHEN UPPER(r.study_name) REGEXP '\\\\bSKULL\\\\b'
            THEN 'Skull'

        -- Spinal Cord Dorsal
        WHEN UPPER(r.study_name) REGEXP 'SPINAL CORD DORSAL'
            THEN 'Spinal Cord'

        -- Spine
        WHEN UPPER(r.study_name) REGEXP '\\\\bSPINE\\\\b|\\\\bSCPINE\\\\b'
            THEN 'Spine'

        -- Spleen
        WHEN UPPER(r.study_name) REGEXP '\\\\bSPLEEN\\\\b'
            THEN 'Spleen'

        -- Sternum
        WHEN UPPER(r.study_name) REGEXP '\\\\bSTERNUM\\\\b'
            THEN 'Sternum'

        -- Teeth / Thumb
        WHEN UPPER(r.study_name) REGEXP '\\\\bTEETH\\\\b|\\\\bTHUMB\\\\b'
            THEN 'Teeth / Thumb'

        -- Temporal Bone
        WHEN UPPER(r.study_name) REGEXP 'TEMPORAL BONE'
            THEN 'Temporal Bone'

        -- Thigh
        WHEN UPPER(r.study_name) REGEXP '\\\\bTHIGH\\\\b|\\\\bTHIGHS\\\\b'
            THEN 'Thigh'

        -- Thoracic
        WHEN UPPER(r.study_name) REGEXP '\\\\bTHORACIC\\\\b'
            THEN 'Thoracic'

        -- Thyroid
        WHEN UPPER(r.study_name) REGEXP '\\\\bTHYROID\\\\b'
            THEN 'Thyroid'

        -- Toes
        WHEN UPPER(r.study_name) REGEXP '\\\\bTOES\\\\b|\\\\bTOE\\\\b'
            THEN 'Toes'

        -- Torso / PE Torso
        WHEN UPPER(r.study_name) REGEXP '\\\\bTORSO\\\\b|\\\\bPE TORSO\\\\b'
            THEN 'Torso'

        -- Transvaginal
        WHEN UPPER(r.study_name) REGEXP 'TRANSVAGINAL|TRANS-VAGINAL|TRANS VAGINAL'
            THEN 'Transvaginal'

        -- Trigeminal
        WHEN UPPER(r.study_name) REGEXP 'TRIGEMINAL NERVE|TRIGEMINAL'
            THEN 'Trigeminal Nerve'

        -- Uterus
        WHEN UPPER(r.study_name) REGEXP '\\\\bUTERUS\\\\b'
            THEN 'Uterus'

        -- Vagus Nerve
        WHEN UPPER(r.study_name) REGEXP 'VAGUS NERVE'
            THEN 'Vagus Nerve'

        -- Veins
        WHEN UPPER(r.study_name) REGEXP '\\\\bVEINS\\\\b|\\\\bVENOUS\\\\b'
            THEN 'Veins'

        -- Whole Body
        WHEN UPPER(r.study_name) REGEXP 'WHOLE BODY'
            THEN 'Whole Body'

        -- Wrist
        WHEN UPPER(r.study_name) REGEXP '\\\\bWRIST\\\\b|\\\\bWRISTS\\\\b|\\\\bWR\\\\b'
            THEN 'Wrist'

        ELSE 'Other'
    END,
    r.laterality = CASE
        WHEN UPPER(r.study_name) REGEXP '\\\\bBILATERAL\\\\b' THEN 'Bilateral'
        WHEN UPPER(r.study_name) REGEXP '\\\\bUNILATERAL\\\\b' THEN 'Unilateral'
        WHEN UPPER(r.study_name) REGEXP '\\\\bLEFT\\\\b|\\\\bLT\\\\b' THEN 'Left'
        WHEN UPPER(r.study_name) REGEXP '\\\\bRIGHT\\\\b|\\\\bRT\\\\b' THEN 'Right'
        ELSE NULL
    END,
    r.pet_tracer_name = CASE
        -- F18 AMYLOID TRACERS
        WHEN UPPER(r.study_name) REGEXP 'FLORBETAPIR|AMYVID|A9591|F-?18\\\\s*FLORBETAPIR|18F-?FLORBETAPIR|\\\\[18F\\\\]\\\\s*FLORBETAPIR'
            THEN 'Florbetapir F18 (Amyvid)'
        WHEN UPPER(r.study_name) REGEXP 'FLUTEMETAMOL|VIZAMYL|A9592|F-?18\\\\s*FLUTEMETAMOL|18F-?FLUTEMETAMOL|\\\\[18F\\\\]\\\\s*FLUTEMETAMOL'
            THEN 'Flutemetamol F18 (Vizamyl)'
        WHEN UPPER(r.study_name) REGEXP 'FLORBETABEN|NEURACEQ|A9593|F-?18\\\\s*FLORBETABEN|18F-?FLORBETABEN|\\\\[18F\\\\]\\\\s*FLORBETABEN'
            THEN 'Florbetaben F18 (Neuraceq)'
        -- GENERIC AMYLOID PET (not matched to specific tracer)
        WHEN UPPER(r.study_name) REGEXP '\\\\bAMYLOID\\\\b'
            THEN 'F18 - Amyloid Tracer (Unspecified)'
        -- F18 PIB (research amyloid)
        WHEN UPPER(r.study_name) REGEXP '\\\\bPIB\\\\b|PITTSBURGH\\\\s*COMPOUND|F-?18\\\\s*PIB|18F-?PIB'
            THEN 'PiB F18 (Pittsburgh Compound-B)'
        -- F18 PSMA TRACERS
        WHEN UPPER(r.study_name) REGEXP 'PIFLUFOLASTAT|PYLARIFY|A9816|DCFPYL'
            THEN 'Piflufolastat F18 (Pylarify)'
        WHEN UPPER(r.study_name) REGEXP 'FLOTUFOLASTAT|POSLUMA|A9815|PSMA-?1007'
            THEN 'Flotufolastat F18 (Posluma)'
        -- GENERIC PSMA F-18 (brand not specified)
        WHEN UPPER(r.study_name) REGEXP '\\\\bPSMA\\\\b'
            AND UPPER(r.study_name) REGEXP 'F-?18|\\\\bF18\\\\b|\\\\[18F\\\\]|18F-?'
            THEN 'F18 - PSMA Tracer (Unspecified)'
        -- F18 FDG
        WHEN UPPER(r.study_name) REGEXP '\\\\bFDG\\\\b|FLUORODEOXYGLUCOSE|FLUDEOXYGLUCOSE|A9552|FDG-?PET'
            THEN 'FDG F18 (Fluorodeoxyglucose)'
        -- F18 SODIUM FLUORIDE (bone PET)
        WHEN UPPER(r.study_name) REGEXP 'SODIUM\\\\s*FLUORIDE|\\\\bNAF\\\\b|A9580|F-?18\\\\s*FLUORIDE|18F-?FLUORIDE|18F-?NAF'
            THEN 'Sodium Fluoride F18 (NaF)'
        -- F18 FDOPA / FLUORODOPA
        WHEN UPPER(r.study_name) REGEXP '\\\\bFDOPA\\\\b|FLUORODOPA|FLUORO-?DOPA|A9600|F-?18\\\\s*FDOPA|18F-?FDOPA|18F-?DOPA'
            THEN 'Fluorodopa F18 (FDOPA)'
        -- F18 FLUCICLOVINE (Axumin)
        WHEN UPPER(r.study_name) REGEXP 'FLUCICLOVINE|AXUMIN|A9584|\\\\bFACBC\\\\b'
            THEN 'Fluciclovine F18 (Axumin)'
        -- F18 FLORTAUCIPIR (Tauvid) — tau PET
        WHEN UPPER(r.study_name) REGEXP 'FLORTAUCIPIR|TAUVID|A9814|TAU\\\\s*PET'
            THEN 'Flortaucipir F18 (Tauvid)'
        -- GENERIC F18 (isotope present but no specific tracer matched)
        WHEN UPPER(r.study_name) REGEXP '\\\\bF-?18\\\\b|\\\\[18F\\\\]|18F-?|\\\\bF18\\\\b'
            THEN 'F18 - Tracer Not Specified'
        -- GA-68 TRACERS
        WHEN UPPER(r.study_name) REGEXP 'GA-?68\\\\s*DOTATATE|NETSPOT|\\\\bDOTATATE\\\\b'
            THEN 'Ga68-DOTATATE (Netspot)'
        WHEN UPPER(r.study_name) REGEXP 'GA-?68\\\\s*DOTATOC|\\\\bDOTATOC\\\\b'
            THEN 'Ga68-DOTATOC'
        WHEN UPPER(r.study_name) REGEXP 'GA-?68\\\\s*DOTANOC|\\\\bDOTANOC\\\\b'
            THEN 'Ga68-DOTANOC'
        WHEN UPPER(r.study_name) REGEXP 'GA-?68\\\\s*PSMA|ILLUCCIX|LOCAMETZ|\\\\bPSMA-?11\\\\b'
            THEN 'Ga68-PSMA-11 (Illuccix/Locametz)'
        -- GENERIC GA-68 (isotope present but no specific tracer matched)
        WHEN UPPER(r.study_name) REGEXP '\\\\bGA-?68\\\\b|\\\\[68GA\\\\]|68GA-?|\\\\bGALLIUM\\\\s*68\\\\b|\\\\bGALLIUM-?68\\\\b'
            THEN 'Ga68 - Tracer Not Specified'
        -- PET present but no isotope or tracer matched
        WHEN UPPER(r.study_name) REGEXP '\\\\bPET\\\\b|\\\\bPET/CT\\\\b|\\\\bPET/MR\\\\b|\\\\bPET-CT\\\\b|\\\\bPET-MR\\\\b'
            THEN 'PET - Tracer Not Specified'
        ELSE 'No PET Tracer'
    END,
    r.pet_hcpcs_code = CASE
        WHEN UPPER(r.study_name) REGEXP 'FLORBETAPIR|AMYVID|A9591|F-?18\\\\s*FLORBETAPIR|18F-?FLORBETAPIR|\\\\[18F\\\\]\\\\s*FLORBETAPIR'
            THEN 'A9591'
        WHEN UPPER(r.study_name) REGEXP 'FLUTEMETAMOL|VIZAMYL|A9592|F-?18\\\\s*FLUTEMETAMOL|18F-?FLUTEMETAMOL|\\\\[18F\\\\]\\\\s*FLUTEMETAMOL'
            THEN 'A9592'
        WHEN UPPER(r.study_name) REGEXP 'FLORBETABEN|NEURACEQ|A9593|F-?18\\\\s*FLORBETABEN|18F-?FLORBETABEN|\\\\[18F\\\\]\\\\s*FLORBETABEN'
            THEN 'A9593'
        WHEN UPPER(r.study_name) REGEXP '\\\\bAMYLOID\\\\b'
            THEN 'A9591/A9592/A9593 (Unspecified)'
        WHEN UPPER(r.study_name) REGEXP 'PIFLUFOLASTAT|PYLARIFY|A9816|DCFPYL'
            THEN 'A9816'
        WHEN UPPER(r.study_name) REGEXP 'FLOTUFOLASTAT|POSLUMA|A9815|PSMA-?1007'
            THEN 'A9815'
        WHEN UPPER(r.study_name) REGEXP '\\\\bPSMA\\\\b'
            AND UPPER(r.study_name) REGEXP 'F-?18|\\\\bF18\\\\b|\\\\[18F\\\\]|18F-?'
            THEN 'A9815/A9816 (Unspecified)'
        WHEN UPPER(r.study_name) REGEXP '\\\\bFDG\\\\b|FLUORODEOXYGLUCOSE|FLUDEOXYGLUCOSE|A9552|FDG-?PET'
            THEN 'A9552'
        WHEN UPPER(r.study_name) REGEXP 'SODIUM\\\\s*FLUORIDE|\\\\bNAF\\\\b|A9580'
            THEN 'A9580'
        WHEN UPPER(r.study_name) REGEXP '\\\\bFDOPA\\\\b|FLUORODOPA|FLUORO-?DOPA|A9600'
            THEN 'A9600'
        WHEN UPPER(r.study_name) REGEXP 'FLUCICLOVINE|AXUMIN|A9584|\\\\bFACBC\\\\b'
            THEN 'A9584'
        WHEN UPPER(r.study_name) REGEXP 'FLORTAUCIPIR|TAUVID|A9814|TAU\\\\s*PET'
            THEN 'A9814'
        WHEN UPPER(r.study_name) REGEXP '\\\\bPIB\\\\b|PITTSBURGH\\\\s*COMPOUND'
            THEN 'N/A (Research)'
        WHEN UPPER(r.study_name) REGEXP 'GA-?68\\\\s*DOTATATE|NETSPOT|\\\\bDOTATATE\\\\b'
            THEN 'A9800'
        WHEN UPPER(r.study_name) REGEXP 'GA-?68\\\\s*DOTATOC|\\\\bDOTATOC\\\\b'
            THEN 'A9801'
        WHEN UPPER(r.study_name) REGEXP 'GA-?68\\\\s*PSMA|ILLUCCIX|LOCAMETZ|\\\\bPSMA-?11\\\\b'
            THEN 'A9858'
        ELSE 'N/A'
    END,
    r.is_pet_study = CASE
        WHEN UPPER(r.study_name) REGEXP
            'FLORBETAPIR|AMYVID|A9591|FLUTEMETAMOL|VIZAMYL|A9592|FLORBETABEN|NEURACEQ|A9593|'
            'FLORTAUCIPIR|TAUVID|A9814|TAU\\\\s*PET|FLORZOLOTAU|PI-?2620|FLORPIRAMINE|MK-?6240|'
            'FLUCICLOVINE|AXUMIN|A9584|FACBC|FLOTUFOLASTAT|POSLUMA|A9815|PSMA-?1007|'
            'PIFLUFOLASTAT|PYLARIFY|A9816|DCFPYL|'
            'SODIUM\\\\s*FLUORIDE|\\\\bNAF\\\\b|A9580|'
            'FDG|FLUORODEOXYGLUCOSE|FLUDEOXYGLUCOSE|A9552|'
            'FMISO|FLUOROMISONIDAZOLE|'
            'FDOPA|FLUORODOPA|A9600|'
            '\\\\bDCFBC\\\\b|\\\\bPIB\\\\b|PITTSBURGH\\\\s*COMPOUND|\\\\bAMYLOID\\\\b|'
            '\\\\bF-?18\\\\b|\\\\[18F\\\\]|18F-?|\\\\bF18\\\\b|'
            '\\\\bGA-?68\\\\b|\\\\[68GA\\\\]|68GA-?|GALLIUM.?68|'
            '\\\\bPET\\\\b|\\\\bPET/CT\\\\b|\\\\bPET/MR\\\\b'
            THEN 'Yes'
        ELSE 'No'
    END
WHERE r.{BATCH_KEY} >= {pk_lo}
  AND r.{BATCH_KEY} < {pk_hi}
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
        ("extracted_codes",    "TEXT"),
        ("cpt_codes_std",      "TEXT"),
        ("hcpcs_codes_std",    "TEXT"),
        ("code_descriptions",  "TEXT"),
        ("cpt_count_flag",     "VARCHAR(50)"),
        ("modality_std",       "VARCHAR(100)"),
        ("modality_combined",  "VARCHAR(200)"),
        ("contrast_type",      "VARCHAR(50)"),
        ("body_part",          "VARCHAR(200)"),
        ("laterality",         "VARCHAR(20)"),
        ("pet_tracer_name",    "VARCHAR(200)"),
        ("pet_hcpcs_code",     "VARCHAR(50)"),
        ("is_pet_study",       "VARCHAR(5)"),
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

    # ── 1. Materialize CTE code lookup staging table ───────────────────
    print("  Materializing staging code lookup (CTE)...")
    if not _table_exists(cur, STAGING_CODE_LOOKUP):
        print("    creating staging.rad_std_code_lookup — this may take several minutes...")
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
                ) normalized
            ),
            code_lookup AS (
                SELECT
                    b.study_name,
                    b.extracted_codes,
                    GROUP_CONCAT(DISTINCT cpt.PROCEDURECODE  ORDER BY cpt.PROCEDURECODE  SEPARATOR ',') AS cpt_codes_std,
                    GROUP_CONCAT(DISTINCT hcpcs.HCPC         ORDER BY hcpcs.HCPC         SEPARATOR ',') AS hcpcs_codes_std,
                    GROUP_CONCAT(DISTINCT COALESCE(cpt.COMMONDESCRIPTION, cpt.DESCRIPTION, hcpcs.`SHORT DESCRIPTION`)
                        ORDER BY COALESCE(cpt.PROCEDURECODE, hcpcs.HCPC) SEPARATOR ' | ')               AS code_descriptions,
                    COUNT(DISTINCT cpt.PROCEDURECODE) + COUNT(DISTINCT hcpcs.HCPC)                      AS total_match_count
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
            ),
            cpt_std AS (
                SELECT
                    l.study_name,
                    l.extracted_codes,
                    COALESCE(l.cpt_codes_std,    'NS') AS cpt_codes_std,
                    COALESCE(l.hcpcs_codes_std,  'NS') AS hcpcs_codes_std,
                    COALESCE(l.code_descriptions,'NS') AS code_descriptions,
                    CASE
                        WHEN l.extracted_codes REGEXP '[A-Za-z]+[0-9]{{6,}}' AND l.total_match_count = 0 THEN 'Internal Identifier Only'
                        WHEN l.total_match_count >= 3  THEN 'Three Codes Present'
                        WHEN l.total_match_count = 2   THEN 'Two Codes Present'
                        WHEN l.total_match_count = 1   THEN 'Single Code'
                        WHEN l.extracted_codes IS NOT NULL AND l.extracted_codes != '' THEN 'Extracted But Not Matched'
                        ELSE 'No CPT Code'
                    END AS cpt_count_flag
                FROM code_lookup l
            )
            SELECT study_name, extracted_codes, cpt_codes_std, hcpcs_codes_std, code_descriptions, cpt_count_flag
            FROM cpt_std
        """)
        cur.execute(f"ALTER TABLE {STAGING_CODE_LOOKUP} ADD INDEX idx_study_name (study_name(200))")
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")
    cur.execute(f"SELECT COUNT(*) FROM {STAGING_CODE_LOOKUP}")
    print(f"    {cur.fetchone()[0]:,} rows")

    # ── 2. Shared PK staging — all rows where udm_inc_id IS NOT NULL ───
    print("  Creating shared PK staging (all rows)...")
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

    # ── 3. Checkpoint table ────────────────────────────────────────────
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
    print(f"  Radiology Standardisation UPDATE — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  target     : {TARGET_TABLE}")
    print(f"  batch_key  : {BATCH_KEY}")
    print(f"  batch_size : {BATCH_SIZE:,}")
    print(f"  passes     : 2  (code/modality/contrast lookup | body_part/laterality/PET)")
    print(f"{'='*70}\n", flush=True)

    print("  Connecting to database...")
    sys.stdout.flush()
    all_ranges = setup_tables()

    passes = [
        (CHECKPOINT_PASS1, "Pass 1 — Code lookup + modality + contrast (all rows)",   build_pass1),
        (CHECKPOINT_PASS2, "Pass 2 — Body part, laterality, PET tracers (all rows)",  build_pass2),
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
        print(f"  [{tag}] {label:<56}  {rows:>10,} rows  ({secs}s)")
        if status.startswith("FAILED"):
            print(f"         {status}")

    print(f"\n  Total rows updated: {total_rows:,}")
    print(f"{'='*70}")

    print(f"\n  Cleanup SQL (run after verifying data):")
    print(f"    -- Shared code lookup (only drop when done with all radiology tables):")
    print(f"    -- DROP TABLE IF EXISTS {STAGING_CODE_LOOKUP};")
    print(f"    -- Per-run tables:")
    print(f"    DROP TABLE IF EXISTS {STAGING_PK};")
    print(f"    DROP TABLE IF EXISTS {CHECKPOINT_TABLE};")

    if any_failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
