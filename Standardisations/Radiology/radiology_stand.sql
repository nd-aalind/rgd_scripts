WITH base AS (
    SELECT
        study_name,
        TRIM(BOTH ',' FROM CONCAT_WS(',',
            -- Case 1: Explicit CPT keyword
            IF(study_name REGEXP '\\bCPT\\s*[0-9]{5}\\b',
                REGEXP_REPLACE(REGEXP_SUBSTR(study_name, 'CPT\\s*[0-9]{5}'), '[^0-9]', ''), NULL),
            -- Case 2a-c: Standalone 5-digit codes (suppressed when CPT keyword present)
            IF(study_name NOT REGEXP '\\bCPT\\s*[0-9]{5}\\b' AND study_name REGEXP '(^|[^0-9])[0-9]{5}([^0-9]|$)',
                REGEXP_REPLACE(REGEXP_SUBSTR(study_name, '(^|[^0-9])[0-9]{5}([^0-9]|$)', 1, 1), '[^0-9]', ''), NULL),
            IF(study_name NOT REGEXP '\\bCPT\\s*[0-9]{5}\\b',
                NULLIF(REGEXP_REPLACE(REGEXP_SUBSTR(study_name, '(^|[^0-9])[0-9]{5}([^0-9]|$)', 1, 2), '[^0-9]', ''), ''), NULL),
            IF(study_name NOT REGEXP '\\bCPT\\s*[0-9]{5}\\b',
                NULLIF(REGEXP_REPLACE(REGEXP_SUBSTR(study_name, '(^|[^0-9])[0-9]{5}([^0-9]|$)', 1, 3), '[^0-9]', ''), ''), NULL),
            -- Case 3: Internal IDs (only when no 5-digit found)
            IF(study_name NOT REGEXP '(^|[^0-9])[0-9]{5}([^0-9]|$)' AND study_name REGEXP '[A-Za-z]+[0-9]{6,}',
                REGEXP_SUBSTR(study_name, '[A-Za-z]+[0-9]{6,}'), NULL),
            -- Case 4: HCPCS format (1 letter + 4 digits)
            NULLIF(REGEXP_SUBSTR(study_name, '\\b[A-Za-z][0-9]{4}\\b'), '')
        )) AS extracted_codes
    FROM (
        SELECT REGEXP_REPLACE(
                REGEXP_REPLACE(study_name,
                    '([0-9]{5})\\s+[Oo][Rr]\\s+([0-9]{5})', '\\1,\\2'),
                '([0-9]{5})\\s*/\\s*([0-9]{5})', '\\1,\\2')
               AS study_name
        FROM rgd_udm_silver.radiology
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
        ON codes.code = cpt.PROCEDURECODE AND codes.code REGEXP '^[0-9]{5}$'
    LEFT JOIN semantics.hcpcs hcpcs
        ON codes.code = hcpcs.HCPC        AND codes.code REGEXP '^[A-Za-z][0-9]{4}$'
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
            WHEN l.extracted_codes REGEXP '[A-Za-z]+[0-9]{6,}' AND l.total_match_count = 0 THEN 'Internal Identifier Only'
            WHEN l.total_match_count >= 3  THEN 'Three Codes Present'
            WHEN l.total_match_count = 2   THEN 'Two Codes Present'
            WHEN l.total_match_count = 1   THEN 'Single Code'
            WHEN l.extracted_codes IS NOT NULL AND l.extracted_codes != '' THEN 'Extracted But Not Matched'
            ELSE 'No CPT Code'
        END AS cpt_count_flag
    FROM code_lookup l
)

SELECT DISTINCT
    c.study_name,
    c.extracted_codes,
    c.cpt_codes_std,
    c.hcpcs_codes_std,
    c.code_descriptions,
    c.cpt_count_flag,

    -- PRIMARY MODALITY — derived from study_name only, no CPT description dependency
    CASE
        WHEN c.study_name REGEXP '\\bCT\\b|\\bCAT\\b|\\bNCT\\b|\\bLDCT\\b|\\bCTA\\b|\\bCTV\\b|\\bCTAC\\b|\\bCTC\\b|\\bCTP\\b' THEN 'Computed Tomography'
        WHEN c.study_name REGEXP '\\bPET\\b|\\bPT\\b'                                                                            THEN 'Positron emission tomography (PET)'
        WHEN c.study_name REGEXP '\\bMRA\\b|\\bzzMRA\\b'                                                                         THEN 'Magnetic resonance angiography'
        WHEN c.study_name REGEXP '\\bMRI\\b|\\bMRCP\\b|\\bMRV\\b|\\bTMRI\\b|\\b3TMRI\\b|\\bMR\\b'                               THEN 'Magnetic Resonance'
        WHEN c.study_name REGEXP '\\bMAM\\b|\\bMAMM\\b|\\bMAMMO\\b|\\bMMAMMO\\b|\\bMG\\b|\\bMAMMOGRAM\\b|\\bMAMMOGRAPHY\\b|\\bDEXA\\b|\\bDXA\\b' THEN 'Mammography'
        WHEN c.study_name REGEXP '\\bUS\\b|\\bULTRASOUND\\b|\\bUSV\\b|\\bBI US\\b|\\bOB US\\b'                                   THEN 'Ultrasound'
        WHEN c.study_name REGEXP '\\bXA\\b|\\bANG\\b|\\bANGIO\\b'                                                                THEN 'X-Ray Angiography'
        WHEN c.study_name REGEXP '\\bCR\\b'                                                                                       THEN 'Computed Radiography'
        WHEN c.study_name REGEXP '\\bDX\\b|\\bDR\\b|\\bXR\\b|\\bX-RAY\\b|\\bXRAY\\b|\\bXRY\\b'                                  THEN 'Digital Radiography'
        WHEN c.study_name REGEXP '\\bRF\\b|\\bFL\\b|\\bFLUORO\\b|\\bFLU\\b'                                                      THEN 'Radio Fluoroscopy'
        WHEN c.study_name REGEXP '\\bFS\\b'                                                                                       THEN 'Fundoscopy'
        WHEN c.study_name REGEXP '\\bNM\\b'                                                                                       THEN 'Nuclear Medicine'
        WHEN c.study_name REGEXP '\\bECHO\\b|\\bECHOCARDIOGRAM\\b'                                                               THEN 'Echocardiography'
        WHEN c.study_name REGEXP '\\bECG\\b|\\bEKG\\b'                                                                           THEN 'Electrocardiography'
        WHEN c.study_name REGEXP '\\bEEG\\b|\\bELECTROCEPHANLOGRAM\\b'                                                           THEN 'Electroencephalography'
        WHEN c.study_name REGEXP '\\bENDOSCOPY\\b'                                                                               THEN 'Endoscopy'
        WHEN c.study_name REGEXP '\\bCD\\b'                                                                                       THEN 'Color flow Doppler'
        WHEN c.study_name REGEXP '\\bTCD\\b|\\bDUPLEX\\b|\\bDOPPLER\\b'                                                          THEN 'Duplex Doppler'
        WHEN c.study_name REGEXP '\\bAUDIO\\b|\\bAUDIOMETRY\\b|\\bAUDITORY\\b|\\bHEARING\\b|\\bAUDIOGRAM\\b|\\bACOUSTIC\\b'     THEN 'Audio'
        WHEN c.study_name REGEXP '\\bRP\\b'                                                                                       THEN 'Radiotherapy Plan'
        WHEN c.study_name REGEXP '\\bRT\\b|\\bRAD\\b|\\bIR\\b|\\bINTERVENTIONAL RADIOLOGY\\b'                                    THEN 'Radiographic imaging'
        WHEN c.study_name REGEXP '\\bSPECT\\b'                                                                                    THEN 'Single-photon emission computed tomography (SPECT)'
        WHEN c.study_name REGEXP '\\bBX\\b|\\bBIOPSY\\b|\\bVL\\b|\\bOHS\\b|\\bI-123\\b|\\b1-131\\b|\\bMPI\\b'                   THEN 'Other'
        ELSE 'Other'
    END AS modality,

    -- COMBINED MODALITY — derived from study_name only, no CPT description dependency
    CASE
        WHEN c.study_name REGEXP '\\bPET/CT\\b|\\bPET CT\\b'                        THEN 'Positron emission tomography (PET) / Computed Tomography'
        WHEN c.study_name REGEXP '\\bXR/RF\\b'                                       THEN 'Digital Radiography / Radio Fluoroscopy'
        WHEN c.study_name REGEXP '\\bUS DOPPLER\\b|\\bUS DUPLEX\\b'                  THEN 'Ultrasound / Duplex Doppler'
        WHEN c.study_name REGEXP '\\bUS ECHOCARDIOGRAM\\b'                           THEN 'Ultrasound / Echocardiography'
        WHEN c.study_name REGEXP '\\bXA US\\b'                                       THEN 'X-Ray Angiography / Ultrasound'
        WHEN c.study_name REGEXP '\\bCT\\b|\\bCAT\\b|\\bNCT\\b|\\bLDCT\\b|\\bCTA\\b|\\bCTV\\b|\\bCTAC\\b|\\bCTC\\b|\\bCTP\\b' THEN 'Computed Tomography'
        WHEN c.study_name REGEXP '\\bPET\\b|\\bPT\\b'                                THEN 'Positron emission tomography (PET)'
        WHEN c.study_name REGEXP '\\bMRA\\b|\\bzzMRA\\b'                             THEN 'Magnetic resonance angiography'
        WHEN c.study_name REGEXP '\\bMRI\\b|\\bMRCP\\b|\\bMRV\\b|\\bTMRI\\b|\\b3TMRI\\b|\\bMR\\b' THEN 'Magnetic Resonance'
        WHEN c.study_name REGEXP '\\bMAM\\b|\\bMAMM\\b|\\bMAMMO\\b|\\bMMAMMO\\b|\\bMG\\b|\\bMAMMOGRAM\\b|\\bMAMMOGRAPHY\\b|\\bDEXA\\b|\\bDXA\\b' THEN 'Mammography'
        WHEN c.study_name REGEXP '\\bUS\\b|\\bULTRASOUND\\b|\\bUSV\\b|\\bBI US\\b|\\bOB US\\b'     THEN 'Ultrasound'
        WHEN c.study_name REGEXP '\\bXA\\b|\\bANG\\b|\\bANGIO\\b'                   THEN 'X-Ray Angiography'
        WHEN c.study_name REGEXP '\\bCR\\b'                                           THEN 'Computed Radiography'
        WHEN c.study_name REGEXP '\\bDX\\b|\\bDR\\b|\\bXR\\b|\\bX-RAY\\b|\\bXRAY\\b|\\bXRY\\b'    THEN 'Digital Radiography'
        WHEN c.study_name REGEXP '\\bRF\\b|\\bFL\\b|\\bFLUORO\\b|\\bFLU\\b'         THEN 'Radio Fluoroscopy'
        WHEN c.study_name REGEXP '\\bFS\\b'                                           THEN 'Fundoscopy'
        WHEN c.study_name REGEXP '\\bNM\\b'                                           THEN 'Nuclear Medicine'
        WHEN c.study_name REGEXP '\\bECHO\\b|\\bECHOCARDIOGRAM\\b'                   THEN 'Echocardiography'
        WHEN c.study_name REGEXP '\\bECG\\b|\\bEKG\\b'                               THEN 'Electrocardiography'
        WHEN c.study_name REGEXP '\\bEEG\\b|\\bELECTROCEPHANLOGRAM\\b'               THEN 'Electroencephalography'
        WHEN c.study_name REGEXP '\\bENDOSCOPY\\b'                                   THEN 'Endoscopy'
        WHEN c.study_name REGEXP '\\bCD\\b'                                           THEN 'Color flow Doppler'
        WHEN c.study_name REGEXP '\\bTCD\\b|\\bDUPLEX\\b|\\bDOPPLER\\b'              THEN 'Duplex Doppler'
        WHEN c.study_name REGEXP '\\bAUDIO\\b|\\bAUDIOMETRY\\b|\\bAUDITORY\\b|\\bHEARING\\b|\\bAUDIOGRAM\\b|\\bACOUSTIC\\b' THEN 'Audio'
        WHEN c.study_name REGEXP '\\bRP\\b'                                           THEN 'Radiotherapy Plan'
        WHEN c.study_name REGEXP '\\bRT\\b|\\bRAD\\b|\\bIR\\b|\\bINTERVENTIONAL RADIOLOGY\\b'      THEN 'Radiographic imaging'
        WHEN c.study_name REGEXP '\\bSPECT\\b'                                        THEN 'Single-photon emission computed tomography (SPECT)'
        WHEN c.study_name REGEXP '\\bBX\\b|\\bBIOPSY\\b|\\bVL\\b|\\bOHS\\b|\\bI-123\\b|\\b1-131\\b|\\bMPI\\b' THEN 'Other'
        ELSE 'Other'
    END AS modality_combined,

    -- CONTRAST TYPE — derived from study_name
    CASE
        WHEN UPPER(c.study_name) REGEXP 'W[/\\s]?WO|W\\s?&\\s?W/?O|W\\s?AND\\s?W/?O|W\\s?OR\\s?W/?O|WITH\\s?AND\\s?W/?O|WO\\+W|W\\+W/?O|WITHOUT/WITH|WITH/WITHOUT|W\\s?AND\\s?WOW|WO,\\s?W|W,\\s?WO|WWO|W/W/O|WO/W|W/&W/O|W AND OR WO|W WO|W\\s?W/?O|WO\\s?W'
            THEN 'With and Without Contrast'
        WHEN UPPER(c.study_name) REGEXP '\\bWO\\b|\\bW/O\\b|\\bWITHOUT\\b|\\bWO CON\\b|\\bW/O CONTRAST\\b|\\bNCON\\b|\\bNO CON\\b|\\bWO C\\b|\\bWO CONTRAST\\b'
            THEN 'Without Contrast'
        WHEN UPPER(c.study_name) REGEXP '\\bW CON\\b|\\bW CONTRAST\\b|\\bWITH CONTRAST\\b|\\bW C\\b|\\bW/\\b|\\bCON\\b'
            THEN 'With Contrast'
        ELSE 'No Contrast Info'
    END AS contrast_type

FROM cpt_std c;


===========Body parts==========================

SELECT DISTINCT
    study_name,

    -- BODY PART
    CASE
        -- MULTI BODY PARTS FIRST

        -- Chest + Abdomen + Pelvis
        WHEN UPPER(study_name) REGEXP 'CHEST.*(ABDOMEN|ABD).*(PELVIS|PELV)|CHEST/ABD/PELVIS|CHEST ABDOMEN PELVIS|CHEST\\+ABD.*PELVIS'
            THEN 'Chest, Abdomen, Pelvis'

        -- Chest + Abdomen
        WHEN UPPER(study_name) REGEXP 'CHEST.*(ABDOMEN|ABD)|CHEST\\+ABD'
            THEN 'Chest, Abdomen'

        -- Chest + Thorax
        WHEN UPPER(study_name) REGEXP 'CHEST.*THORAX|CHEST/THORAX'
            THEN 'Chest, Thorax'

        -- Abdomen + Pelvis
        WHEN UPPER(study_name) REGEXP '(ABDOMEN|ABD).*(PELVIS|PELV)|(PELVIS|PELV).*(ABDOMEN|ABD)'
            THEN 'Abdomen, Pelvis'

        -- Head + Neck
        WHEN UPPER(study_name) REGEXP 'HEAD.*NECK|NECK.*HEAD|NECK/HEAD'
            THEN 'Head, Neck'

        -- Orbit + Face + Neck
        WHEN UPPER(study_name) REGEXP 'ORB.*FAC.*NCK|ORBIT.*FACE.*NECK|ORBIT/FACE/NK'
            THEN 'Orbit, Face, Neck'

        -- Orbit + Sella
        WHEN UPPER(study_name) REGEXP 'ORBIT.*SELLA|ORBIT\\+SELLA|ORBIT SELLA POSS|ORBIT\\+SELLA\\+PF'
            THEN 'Orbit, Sella'

        -- Thoracic + Lumbar Spine
        WHEN UPPER(study_name) REGEXP 'THORACO.?LUMBAR|THORACOLUMBAR|THOR.*LUM[B]|THOR\\+LU[MB]'
            THEN 'Thoracic Spine, Lumbar Spine'

        -- Lumbo-Sacral
        WHEN UPPER(study_name) REGEXP 'LUMBO.?SACRAL|LUMB.?SACR|LUMBO SACRAL|LUMBOSACRAL'
            THEN 'Lumbar Spine, Sacrum'

        -- Sacrum + Coccyx
        WHEN UPPER(study_name) REGEXP 'SACRUM.*(AND|\\+|/).?COCCYX|SACRUM COCCYX|SACRUM/COCCYX'
            THEN 'Sacrum, Coccyx'

        -- Facial / Sinus combined
        WHEN UPPER(study_name) REGEXP 'FACIAL.*SINUS|SINUS.*FACIAL|FACIAL/SINUS|MAX.*FAC.*SIN|MAXILLOFACIAL|MAXIOFACIAL|MAXFACIAL|MAXILLA|SINUS FACIAL|MAX/FAC/SIN|MAXFACIAL BONES'
            THEN 'Facial Bones, Sinuses'

        -- Tibia + Fibula
        WHEN UPPER(study_name) REGEXP 'TIB.?FIB|TIBIA.?FIBUL|TIBIA AND FIBULA|TIBIA/FIBULA|TIB & FIB|TIB\\+FIBULA|TIBIA\\+FIBULA|TM JOINTS'
            THEN 'Tibia, Fibula'

        -- Forearm / Radius / Ulna
        WHEN UPPER(study_name) REGEXP 'FOREARM.*RADIUS|FOREARM.*ULNA|RADIUS.*ULNA|FOREARM/RADIUS'
            THEN 'Forearm, Radius, Ulna'

        -- Carotid / Neck
        WHEN UPPER(study_name) REGEXP 'CAROTID.*NECK|CAROTID/NECK|VASC CAROTID|CAROTIDS'
            THEN 'Carotid, Neck'

        -- Thyroid / Neck
        WHEN UPPER(study_name) REGEXP 'THYROID.*NECK|THY.*NECK|THYROID/NECK'
            THEN 'Thyroid, Neck'

        -- Liver + Gallbladder + Pancreas
        WHEN UPPER(study_name) REGEXP 'LIVER.*GALLBLADDER.*PANCREAS|LIVER GALLBLADDER PANCREAS'
            THEN 'Liver, Gallbladder, Pancreas'

        -- Ilium + Sternum + Rib
        WHEN UPPER(study_name) REGEXP 'ILIUM.*STERNUM.*RIB|ILIUM STERNUM RIB'
            THEN 'Ilium, Sternum, Rib'

        -- AC Joints / Acromioclavicular
        WHEN UPPER(study_name) REGEXP 'AC JOINTS|ACROMIOCLAVICULAR JOINTS|STERNOCLAVIC|STERNOCLAVICULAR'
            THEN 'Acromioclavicular Joints'

        -- TMJ / Temporomandibular
        WHEN UPPER(study_name) REGEXP '\\bTMJ\\b|TEMPOROMANDIBULAR|TEMPOROMANDIBULAR JOINT|TMJ BILATERAL'
            THEN 'Temporomandibular Joint'

        -- Vascular Lower Extremity
        WHEN UPPER(study_name) REGEXP 'VASC EXT LOWR|VASC EXTREMITY LOWER|LOWER EXTREMITY VENOUS|LOWER EXT VENOUS|LOWER EXTREMITY ARTERIES|LOWER EXT ARTERIAL|ARTERIAL LOW.*EXT|ARTERIAL LOWER EXT|ARTERIAL LOWER EXTREMITY|LOWER EXTREMITY ARTERI'
            THEN 'Lower Extremity (Vascular)'

        -- Vascular Upper Extremity
        WHEN UPPER(study_name) REGEXP 'UPPER EXTREMITY VENOUS|UPPER EXT.*ARTERIAL|ARTERIAL UPPER EXT|ARTERIAL UPPER EXTREMITY|UPPER EXTREMITY ARTERIAL|LT UPPER VENOUS|UPPER OR LOWER EXT ARTERIAL'
            THEN 'Upper Extremity (Vascular)'

        -- Vascular Transcranial
        WHEN UPPER(study_name) REGEXP 'VASC TRANSCRANIAL|TRANS CRANIAL|TRANSCRANIAL|VASC JUGULAR|JUGULAR.*SUBCLAVIAN'
            THEN 'Transcranial (Vascular)'

        -- Cerebral Arteries
        WHEN UPPER(study_name) REGEXP 'CEREBRAL ARTERIES|EXTRACRANIAL ARTERIES'
            THEN 'Cerebral Arteries'

        -- Lower Extremity (general — all variants)
        WHEN UPPER(study_name) REGEXP '\\bLOWER EXTREMIT|\\bLOWER EXT\\b|\\bLOWER EXTR\\b|\\bLWR EXT\\b|\\bLE\\b|\\bLOW EXT\\b|\\bLEFT LOWER EXTREMITY\\b|\\bRIGHT LOWER EXTREMITY\\b|LOWER EXT.*NOT.*JNT|LOWER EXT.*NON|LWR EXT NOT JT|LOWER LEG|LOWER BACK|EXTREMITY LOWER|EXTREMITY.*LOWER'
            THEN 'Lower Extremity'

        -- Upper Extremity (general — all variants)
        WHEN UPPER(study_name) REGEXP '\\bUPPER EXTREMIT|\\bUPPER EXT\\b|\\bUPR.*EXT\\b|\\bUPR/LXTR\\b|UP EXT JT|LEFT EXTREMITY.*UPPER|UPPER EXT.*JOINT|UPPER EXT NON JOINT|UPPER EXTREMITY.*MUSCULO|EXTREMITY UPPER|EXTREMITY.*UPPER|LEFT EXTREMITY JOINT UPPER|RIGHT EXTREMITY JOINT LOWER'
            THEN 'Upper Extremity'

        -- Brachial Plexus
        WHEN UPPER(study_name) REGEXP 'BRACHIAL PLEXUS|RIGHT BRACHIAL PLEXUS'
            THEN 'Brachial Plexus'

        -- Spinal Canal / Cord
        WHEN UPPER(study_name) REGEXP 'SPINAL CANAL|SPINAL CORD|THORACIC SPINAL CORD|SPINAL CORD DORSAL'
            THEN 'Spinal Canal'

        -- Sacroiliac
        WHEN UPPER(study_name) REGEXP 'SACROILIAC|SACROILIAC JOINT|SACROILIAC JNTS|SI JOINT'
            THEN 'Sacroiliac Joint'

        -- SINGLE BODY PARTS (alphabetical)

        -- Abdomen
        WHEN UPPER(study_name) REGEXP '\\bABDOMEN\\b|\\bABD\\b|\\bABDOMINAL\\b'
            THEN 'Abdomen'

        -- Ankle
        WHEN UPPER(study_name) REGEXP '\\bANKLE\\b|\\bANK\\b'
            THEN 'Ankle'

        -- Aorta
        WHEN UPPER(study_name) REGEXP '\\bAORTA\\b|\\bTHORACIC AORTA\\b'
            THEN 'Aorta'

        -- Artery / Arterial
        WHEN UPPER(study_name) REGEXP '\\bARTERY\\b|\\bARTERIAL\\b'
            THEN 'Arterial'

        -- Auditory Canal
        WHEN UPPER(study_name) REGEXP 'AUDITORY CANAL'
            THEN 'Auditory Canal'

        -- Axial Skeleton
        WHEN UPPER(study_name) REGEXP 'AXIAL SKELETON'
            THEN 'Axial Skeleton'

        -- Bone
        WHEN UPPER(study_name) REGEXP '\\bBONE\\b'
            THEN 'Bone'

        -- Bowel
        WHEN UPPER(study_name) REGEXP '\\bBOWEL\\b'
            THEN 'Bowel'

        -- Brain
        WHEN UPPER(study_name) REGEXP '\\bBRAIN\\b|\\bBRAINSTEM\\b|\\bBRIAN\\b|\\bBRIN\\b'
            THEN 'Brain'

        -- Breast
        WHEN UPPER(study_name) REGEXP '\\bBREAST\\b|\\bBREASTS\\b'
            THEN 'Breast'

        -- Calf
        WHEN UPPER(study_name) REGEXP '\\bCALF\\b'
            THEN 'Calf'

        -- Carotid
        WHEN UPPER(study_name) REGEXP '\\bCAROTID\\b|\\bCAROTIDS\\b'
            THEN 'Carotid'

        -- Cervical Spine
        WHEN UPPER(study_name) REGEXP '\\bCERVICAL SPINE\\b|\\bSPINE CERVICAL\\b|\\bC-SPINE\\b'
            THEN 'Cervical Spine'

        -- Cervical
        WHEN UPPER(study_name) REGEXP '\\bCERVICAL\\b'
            THEN 'Cervical'

        -- Chest
        WHEN UPPER(study_name) REGEXP '\\bCHEST\\b|\\bPA CHEST\\b|\\bCHEST PA\\b|\\bTHORAX\\b|\\bRIBS\\b|\\bPNEUMOTHORAX\\b|\\bTHORACENTESIS\\b'
            THEN 'Chest'

        -- Clavicle
        WHEN UPPER(study_name) REGEXP '\\bCLAVICLE\\b'
            THEN 'Clavicle'

        -- Coccyx
        WHEN UPPER(study_name) REGEXP '\\bCOCCYX\\b'
            THEN 'Coccyx'

        -- Colon / Large Intestine
        WHEN UPPER(study_name) REGEXP '\\bCOLON\\b|\\bLARGE INTESTINE\\b'
            THEN 'Colon'

        -- Cranial Nerve
        WHEN UPPER(study_name) REGEXP 'CRANIAL NERVE'
            THEN 'Cranial Nerve'

        -- Ear
        WHEN UPPER(study_name) REGEXP '\\bEAR\\b'
            THEN 'Ear'

        -- Elbow
        WHEN UPPER(study_name) REGEXP '\\bELBOW\\b|\\bELB\\b'
            THEN 'Elbow'

        -- Esophagus
        WHEN UPPER(study_name) REGEXP '\\bESOPHAGUS\\b|\\bTRANSESOPHAGEAL\\b'
            THEN 'Esophagus'

        -- Extracranial
        WHEN UPPER(study_name) REGEXP '\\bEXTRACRANIAL\\b|\\bEXTRACRAN\\b'
            THEN 'Extracranial'

        -- Eye / Orbit
        WHEN UPPER(study_name) REGEXP '\\bEYE\\b|\\bORBIT\\b|\\bORBITS\\b|\\bORB\\b|OPTIC NERVE'
            THEN 'Eye / Orbit'

        -- Face
        WHEN UPPER(study_name) REGEXP '\\bFACE\\b|\\bFACIAL\\b|\\bFACIAL BONES\\b'
            THEN 'Face'

        -- Femur
        WHEN UPPER(study_name) REGEXP '\\bFEMUR\\b|\\bFEM\\b|\\bLATERAL FEMORAL\\b'
            THEN 'Femur'

        -- Fingers
        WHEN UPPER(study_name) REGEXP '\\bFINGERS\\b|\\bFINGER\\b'
            THEN 'Fingers'

        -- Foot / Feet
        WHEN UPPER(study_name) REGEXP '\\bFOOT\\b|\\bFEET\\b|\\bFT\\b|\\bHEEL\\b'
            THEN 'Foot'

        -- Forearm
        WHEN UPPER(study_name) REGEXP '\\bFOREARM\\b|\\bFORE\\b'
            THEN 'Forearm'

        -- Gallbladder
        WHEN UPPER(study_name) REGEXP '\\bGALLBLADDER\\b'
            THEN 'Gallbladder'

        -- Gastric / GI / UGI
        WHEN UPPER(study_name) REGEXP '\\bGI\\b|\\bUGI\\b|\\bGASTRIC\\b|\\bGASTROINTESTINAL\\b|\\bSMALL INTESTINE\\b|\\bSTOMACH\\b'
            THEN 'Gastric / GI'

        -- Greater Occipital
        WHEN UPPER(study_name) REGEXP 'GREATER OCCIPITAL|OCCIPITAL'
            THEN 'Occipital'

        -- Groin
        WHEN UPPER(study_name) REGEXP '\\bGROIN\\b|\\bILIOINGUINAL\\b'
            THEN 'Groin'

        -- Hand
        WHEN UPPER(study_name) REGEXP '\\bHAND\\b|\\bHANDS\\b'
            THEN 'Hand'

        -- Head
        WHEN UPPER(study_name) REGEXP '\\bHEAD\\b|\\bORBITS\\b'
            THEN 'Head'

        -- Heart
        WHEN UPPER(study_name) REGEXP '\\bHEART\\b|\\bTRANSTHORACIC\\b'
            THEN 'Heart'

        -- Hip
        WHEN UPPER(study_name) REGEXP '\\bHIP\\b|\\bHIPS\\b'
            THEN 'Hip'

        -- Humerus / Upper Arm
        WHEN UPPER(study_name) REGEXP '\\bHUMERUS\\b|\\bHUM\\b|\\bUPPER ARM\\b'
            THEN 'Humerus / Upper Arm'

        -- Intracranial
        WHEN UPPER(study_name) REGEXP '\\bINTRACRANIAL\\b|\\bINTRACRAN\\b'
            THEN 'Intracranial'

        -- Kidney
        WHEN UPPER(study_name) REGEXP '\\bKIDNEY\\b|\\bKIDNEYS\\b|\\bRENAL\\b'
            THEN 'Kidney'

        -- Knee
        WHEN UPPER(study_name) REGEXP '\\bKNEE\\b|\\bKNEES\\b|\\bKN\\b'
            THEN 'Knee'

        -- Leg
        WHEN UPPER(study_name) REGEXP '\\bLEG\\b|\\bLOWER LEG\\b'
            THEN 'Leg'

        -- Liver
        WHEN UPPER(study_name) REGEXP '\\bLIVER\\b'
            THEN 'Liver'

        -- Lumbar Plexus
        WHEN UPPER(study_name) REGEXP 'LUMBAR PLEXUS|LUMPLEX'
            THEN 'Lumbar Plexus'

        -- Lumbar Spine
        WHEN UPPER(study_name) REGEXP '\\bLUMBAR SPINE\\b|\\bSPINE LUMBAR\\b|\\bLUMBOSACRAL\\b|\\bLUMOSACRAL\\b'
            THEN 'Lumbar Spine'

        -- Lumbar
        WHEN UPPER(study_name) REGEXP '\\bLUMBAR\\b'
            THEN 'Lumbar'

        -- Lung
        WHEN UPPER(study_name) REGEXP '\\bLUNG\\b'
            THEN 'Lung'

        -- Lymph Node
        WHEN UPPER(study_name) REGEXP 'LYMPH NODE'
            THEN 'Lymph Node'

        -- Mandible
        WHEN UPPER(study_name) REGEXP '\\bMANDIBLE\\b'
            THEN 'Mandible'

        -- Mastoids
        WHEN UPPER(study_name) REGEXP '\\bMASTOIDS\\b|\\bMASTOID\\b'
            THEN 'Mastoids'

        -- Neck
        WHEN UPPER(study_name) REGEXP '\\bNECK\\b|\\bNECK SOFT TISSUE\\b|\\bTHROAT\\b'
            THEN 'Neck'

        -- Pancreas
        WHEN UPPER(study_name) REGEXP '\\bPANCREAS\\b'
            THEN 'Pancreas'

        -- Parathyroid
        WHEN UPPER(study_name) REGEXP '\\bPARATHYROID\\b'
            THEN 'Parathyroid'

        -- Pelvis
        WHEN UPPER(study_name) REGEXP '\\bPELVIS\\b|\\bPELVIC\\b'
            THEN 'Pelvis'

        -- Pituitary
        WHEN UPPER(study_name) REGEXP '\\bPITUITARY\\b|\\bPITUITARY GLAND\\b|\\bSELLA TURCICA\\b'
            THEN 'Pituitary'

        -- Prostate / Rectal
        WHEN UPPER(study_name) REGEXP '\\bPROSTATE\\b|\\bRECTAL\\b'
            THEN 'Prostate / Rectal'

        -- Retroperitoneum
        WHEN UPPER(study_name) REGEXP '\\bRETROPERITONEUM\\b'
            THEN 'Retroperitoneum'

        -- Sacrum
        WHEN UPPER(study_name) REGEXP '\\bSACRUM\\b'
            THEN 'Sacrum'

        -- Scapula
        WHEN UPPER(study_name) REGEXP '\\bSCAPULA\\b|\\bSCAP\\b'
            THEN 'Scapula'

        -- Scoliosis
        WHEN UPPER(study_name) REGEXP '\\bSCOLIOSIS\\b'
            THEN 'Scoliosis'

        -- Scrotal / Testicular
        WHEN UPPER(study_name) REGEXP '\\bSCROTAL\\b|\\bSCROTUM\\b|\\bTESTICULAR\\b|\\bTESTICLE\\b|\\bTESTES\\b'
            THEN 'Scrotal / Testicular'

        -- Shoulder
        WHEN UPPER(study_name) REGEXP '\\bSHOULDER\\b|\\bSH\\b|UPPER EXT JOINT SHOULDER'
            THEN 'Shoulder'

        -- Sinuses
        WHEN UPPER(study_name) REGEXP '\\bSINUS\\b|\\bSINUSES\\b|\\bNASAL\\b|\\bSINUS/NASAL\\b'
            THEN 'Sinuses'

        -- Skull
        WHEN UPPER(study_name) REGEXP '\\bSKULL\\b'
            THEN 'Skull'

        -- Spinal Cord Dorsal
        WHEN UPPER(study_name) REGEXP 'SPINAL CORD DORSAL'
            THEN 'Spinal Cord'

        -- Spine
        WHEN UPPER(study_name) REGEXP '\\bSPINE\\b|\\bSCPINE\\b'
            THEN 'Spine'

        -- Spleen
        WHEN UPPER(study_name) REGEXP '\\bSPLEEN\\b'
            THEN 'Spleen'

        -- Sternum
        WHEN UPPER(study_name) REGEXP '\\bSTERNUM\\b'
            THEN 'Sternum'

        -- Teeth / Thumb
        WHEN UPPER(study_name) REGEXP '\\bTEETH\\b|\\bTHUMB\\b'
            THEN 'Teeth / Thumb'

        -- Temporal Bone
        WHEN UPPER(study_name) REGEXP 'TEMPORAL BONE'
            THEN 'Temporal Bone'

        -- Thigh
        WHEN UPPER(study_name) REGEXP '\\bTHIGH\\b|\\bTHIGHS\\b'
            THEN 'Thigh'

        -- Thoracic
        WHEN UPPER(study_name) REGEXP '\\bTHORACIC\\b'
            THEN 'Thoracic'

        -- Thyroid
        WHEN UPPER(study_name) REGEXP '\\bTHYROID\\b'
            THEN 'Thyroid'

        -- Toes
        WHEN UPPER(study_name) REGEXP '\\bTOES\\b|\\bTOE\\b'
            THEN 'Toes'

        -- Torso / PE Torso
        WHEN UPPER(study_name) REGEXP '\\bTORSO\\b|\\bPE TORSO\\b'
            THEN 'Torso'

        -- Transvaginal
        WHEN UPPER(study_name) REGEXP 'TRANSVAGINAL|TRANS-VAGINAL|TRANS VAGINAL'
            THEN 'Transvaginal'

        -- Trigeminal
        WHEN UPPER(study_name) REGEXP 'TRIGEMINAL NERVE|TRIGEMINAL'
            THEN 'Trigeminal Nerve'

        -- Uterus
        WHEN UPPER(study_name) REGEXP '\\bUTERUS\\b'
            THEN 'Uterus'

        -- Vagus Nerve
        WHEN UPPER(study_name) REGEXP 'VAGUS NERVE'
            THEN 'Vagus Nerve'

        -- Veins
        WHEN UPPER(study_name) REGEXP '\\bVEINS\\b|\\bVENOUS\\b'
            THEN 'Veins'

        -- Whole Body
        WHEN UPPER(study_name) REGEXP 'WHOLE BODY'
            THEN 'Whole Body'

        -- Wrist
        WHEN UPPER(study_name) REGEXP '\\bWRIST\\b|\\bWRISTS\\b|\\bWR\\b'
            THEN 'Wrist'

        ELSE 'Other'
    END AS body_part,

    -- LATERALITY
    CASE
        WHEN UPPER(study_name) REGEXP '\\bBILATERAL\\b'
            THEN 'Bilateral'
        WHEN UPPER(study_name) REGEXP '\\bUNILATERAL\\b'
            THEN 'Unilateral'
        WHEN UPPER(study_name) REGEXP '\\bLEFT\\b|\\bLT\\b'
            THEN 'Left'
        WHEN UPPER(study_name) REGEXP '\\bRIGHT\\b|\\bRT\\b'
            THEN 'Right'
        ELSE NULL
    END AS laterality

FROM rgd_udm_silver.radiology;


==========PET TRACER INFORMATION====================

SELECT DISTINCT
    study_name,

    -- COLUMN 1: Tracer Full Name
    CASE
        -- F18 AMYLOID TRACERS
        WHEN UPPER(study_name) REGEXP 'FLORBETAPIR|AMYVID|A9591|F-?18\\s*FLORBETAPIR|18F-?FLORBETAPIR|\\[18F\\]\\s*FLORBETAPIR'
            THEN 'Florbetapir F18 (Amyvid)'
        WHEN UPPER(study_name) REGEXP 'FLUTEMETAMOL|VIZAMYL|A9592|F-?18\\s*FLUTEMETAMOL|18F-?FLUTEMETAMOL|\\[18F\\]\\s*FLUTEMETAMOL'
            THEN 'Flutemetamol F18 (Vizamyl)'
        WHEN UPPER(study_name) REGEXP 'FLORBETABEN|NEURACEQ|A9593|F-?18\\s*FLORBETABEN|18F-?FLORBETABEN|\\[18F\\]\\s*FLORBETABEN'
            THEN 'Florbetaben F18 (Neuraceq)'
        -- GENERIC AMYLOID PET (not matched to specific tracer)
        WHEN UPPER(study_name) REGEXP '\\bAMYLOID\\b'
            THEN 'F18 - Amyloid Tracer (Unspecified)'

        -- F18 PIB (research amyloid)
        WHEN UPPER(study_name) REGEXP '\\bPIB\\b|PITTSBURGH\\s*COMPOUND|F-?18\\s*PIB|18F-?PIB'
            THEN 'PiB F18 (Pittsburgh Compound-B)'

        -- F18 PSMA TRACERS
        WHEN UPPER(study_name) REGEXP 'PIFLUFOLASTAT|PYLARIFY|A9816|DCFPYL'
            THEN 'Piflufolastat F18 (Pylarify)'
        WHEN UPPER(study_name) REGEXP 'FLOTUFOLASTAT|POSLUMA|A9815|PSMA-?1007'
            THEN 'Flotufolastat F18 (Posluma)'
        -- GENERIC PSMA F-18 (brand not specified)
        WHEN UPPER(study_name) REGEXP '\\bPSMA\\b'
            AND UPPER(study_name) REGEXP 'F-?18|\\bF18\\b|\\[18F\\]|18F-?'
            THEN 'F18 - PSMA Tracer (Unspecified)'

        -- F18 FDG
        WHEN UPPER(study_name) REGEXP '\\bFDG\\b|FLUORODEOXYGLUCOSE|FLUDEOXYGLUCOSE|A9552|FDG-?PET'
            THEN 'FDG F18 (Fluorodeoxyglucose)'

        -- F18 SODIUM FLUORIDE (bone PET)
        WHEN UPPER(study_name) REGEXP 'SODIUM\\s*FLUORIDE|\\bNAF\\b|A9580|F-?18\\s*FLUORIDE|18F-?FLUORIDE|18F-?NAF'
            THEN 'Sodium Fluoride F18 (NaF)'

        -- F18 FDOPA / FLUORODOPA
        WHEN UPPER(study_name) REGEXP '\\bFDOPA\\b|FLUORODOPA|FLUORO-?DOPA|A9600|F-?18\\s*FDOPA|18F-?FDOPA|18F-?DOPA'
            THEN 'Fluorodopa F18 (FDOPA)'

        -- F18 FLUCICLOVINE (Axumin) — prostate cancer
        WHEN UPPER(study_name) REGEXP 'FLUCICLOVINE|AXUMIN|A9584|\\bFACBC\\b'
            THEN 'Fluciclovine F18 (Axumin)'

        -- F18 FLORTAUCIPIR (Tauvid) — tau PET
        WHEN UPPER(study_name) REGEXP 'FLORTAUCIPIR|TAUVID|A9814|TAU\\s*PET'
            THEN 'Flortaucipir F18 (Tauvid)'

        -- GENERIC F18 (isotope present but no specific tracer matched)
        WHEN UPPER(study_name) REGEXP '\\bF-?18\\b|\\[18F\\]|18F-?|\\bF18\\b'
            THEN 'F18 - Tracer Not Specified'

        -- GA-68 TRACERS
        WHEN UPPER(study_name) REGEXP 'GA-?68\\s*DOTATATE|NETSPOT|\\bDOTATATE\\b'
            THEN 'Ga68-DOTATATE (Netspot)'
        WHEN UPPER(study_name) REGEXP 'GA-?68\\s*DOTATOC|\\bDOTATOC\\b'
            THEN 'Ga68-DOTATOC'
        WHEN UPPER(study_name) REGEXP 'GA-?68\\s*DOTANOC|\\bDOTANOC\\b'
            THEN 'Ga68-DOTANOC'
        WHEN UPPER(study_name) REGEXP 'GA-?68\\s*PSMA|ILLUCCIX|LOCAMETZ|\\bPSMA-?11\\b'
            THEN 'Ga68-PSMA-11 (Illuccix/Locametz)'
        -- GENERIC GA-68 (isotope present but no specific tracer matched)
        WHEN UPPER(study_name) REGEXP '\\bGA-?68\\b|\\[68GA\\]|68GA-?|\\bGALLIUM\\s*68\\b|\\bGALLIUM-?68\\b'
            THEN 'Ga68 - Tracer Not Specified'

        -- PET present but no isotope or tracer matched
        WHEN UPPER(study_name) REGEXP '\\bPET\\b|\\bPET/CT\\b|\\bPET/MR\\b|\\bPET-CT\\b|\\bPET-MR\\b'
            THEN 'PET - Tracer Not Specified'

        ELSE 'No PET Tracer'
    END AS pet_tracer_name,

    -- COLUMN 2: HCPCS Code
    CASE
        WHEN UPPER(study_name) REGEXP 'FLORBETAPIR|AMYVID|A9591|F-?18\\s*FLORBETAPIR|18F-?FLORBETAPIR|\\[18F\\]\\s*FLORBETAPIR'
            THEN 'A9591'
        WHEN UPPER(study_name) REGEXP 'FLUTEMETAMOL|VIZAMYL|A9592|F-?18\\s*FLUTEMETAMOL|18F-?FLUTEMETAMOL|\\[18F\\]\\s*FLUTEMETAMOL'
            THEN 'A9592'
        WHEN UPPER(study_name) REGEXP 'FLORBETABEN|NEURACEQ|A9593|F-?18\\s*FLORBETABEN|18F-?FLORBETABEN|\\[18F\\]\\s*FLORBETABEN'
            THEN 'A9593'
        WHEN UPPER(study_name) REGEXP '\\bAMYLOID\\b'
            THEN 'A9591/A9592/A9593 (Unspecified)'
        WHEN UPPER(study_name) REGEXP 'PIFLUFOLASTAT|PYLARIFY|A9816|DCFPYL'
            THEN 'A9816'
        WHEN UPPER(study_name) REGEXP 'FLOTUFOLASTAT|POSLUMA|A9815|PSMA-?1007'
            THEN 'A9815'
        WHEN UPPER(study_name) REGEXP '\\bPSMA\\b'
            AND UPPER(study_name) REGEXP 'F-?18|\\bF18\\b|\\[18F\\]|18F-?'
            THEN 'A9815/A9816 (Unspecified)'
        WHEN UPPER(study_name) REGEXP '\\bFDG\\b|FLUORODEOXYGLUCOSE|FLUDEOXYGLUCOSE|A9552|FDG-?PET'
            THEN 'A9552'
        WHEN UPPER(study_name) REGEXP 'SODIUM\\s*FLUORIDE|\\bNAF\\b|A9580'
            THEN 'A9580'
        WHEN UPPER(study_name) REGEXP '\\bFDOPA\\b|FLUORODOPA|FLUORO-?DOPA|A9600'
            THEN 'A9600'
        WHEN UPPER(study_name) REGEXP 'FLUCICLOVINE|AXUMIN|A9584|\\bFACBC\\b'
            THEN 'A9584'
        WHEN UPPER(study_name) REGEXP 'FLORTAUCIPIR|TAUVID|A9814|TAU\\s*PET'
            THEN 'A9814'
        WHEN UPPER(study_name) REGEXP '\\bPIB\\b|PITTSBURGH\\s*COMPOUND'
            THEN 'N/A (Research)'
        WHEN UPPER(study_name) REGEXP 'GA-?68\\s*DOTATATE|NETSPOT|\\bDOTATATE\\b'
            THEN 'A9800'
        WHEN UPPER(study_name) REGEXP 'GA-?68\\s*DOTATOC|\\bDOTATOC\\b'
            THEN 'A9801'
        WHEN UPPER(study_name) REGEXP 'GA-?68\\s*PSMA|ILLUCCIX|LOCAMETZ|\\bPSMA-?11\\b'
            THEN 'A9858'
        ELSE 'N/A'
    END AS pet_hcpcs_code,

    -- Is this a PET radiopharmaceutical study at all? (broad fallback)
    CASE
        WHEN UPPER(study_name) REGEXP
            'FLORBETAPIR|AMYVID|A9591|FLUTEMETAMOL|VIZAMYL|A9592|FLORBETABEN|NEURACEQ|A9593|'
            'FLORTAUCIPIR|TAUVID|A9814|TAU\\s*PET|FLORZOLOTAU|PI-?2620|FLORPIRAMINE|MK-?6240|'
            'FLUCICLOVINE|AXUMIN|A9584|FACBC|FLOTUFOLASTAT|POSLUMA|A9815|PSMA-?1007|'
            'PIFLUFOLASTAT|PYLARIFY|A9816|DCFPYL|'
            'SODIUM\\s*FLUORIDE|\\bNAF\\b|A9580|'
            'FDG|FLUORODEOXYGLUCOSE|FLUDEOXYGLUCOSE|A9552|'
            'FMISO|FLUOROMISONIDAZOLE|'
            'FDOPA|FLUORODOPA|A9600|'
            '\\bDCFBC\\b|\\bPIB\\b|PITTSBURGH\\s*COMPOUND|\\bAMYLOID\\b|'
            '\\bF-?18\\b|\\[18F\\]|18F-?|\\bF18\\b|'
            '\\bGA-?68\\b|\\[68GA\\]|68GA-?|GALLIUM.?68|'
            '\\bPET\\b|\\bPET/CT\\b|\\bPET/MR\\b'
            THEN 'Yes'
        ELSE 'No'
    END AS is_pet_study

FROM rgd_udm_silver.radiology;