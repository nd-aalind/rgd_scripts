WITH base AS (
    SELECT
        study_name,
        TRIM(BOTH ',' FROM CONCAT_WS(',',
            IF(study_name REGEXP '\\bCPT\\s*[0-9]{5}\\b',
                REGEXP_REPLACE(REGEXP_SUBSTR(study_name, 'CPT\\s*[0-9]{5}'), '[^0-9]', ''), NULL),
            IF(study_name NOT REGEXP '\\bCPT\\s*[0-9]{5}\\b' AND study_name REGEXP '(^|[^0-9])[0-9]{5}([^0-9]|$)',
                REGEXP_REPLACE(REGEXP_SUBSTR(study_name, '(^|[^0-9])[0-9]{5}([^0-9]|$)', 1, 1), '[^0-9]', ''), NULL),
            IF(study_name NOT REGEXP '\\bCPT\\s*[0-9]{5}\\b',
                NULLIF(REGEXP_REPLACE(REGEXP_SUBSTR(study_name, '(^|[^0-9])[0-9]{5}([^0-9]|$)', 1, 2), '[^0-9]', ''), ''), NULL),
            IF(study_name NOT REGEXP '\\bCPT\\s*[0-9]{5}\\b',
                NULLIF(REGEXP_REPLACE(REGEXP_SUBSTR(study_name, '(^|[^0-9])[0-9]{5}([^0-9]|$)', 1, 3), '[^0-9]', ''), ''), NULL),
            IF(study_name NOT REGEXP '(^|[^0-9])[0-9]{5}([^0-9]|$)' AND study_name REGEXP '[A-Za-z]+[0-9]{6,}',
                REGEXP_SUBSTR(study_name, '[A-Za-z]+[0-9]{6,}'), NULL),
            NULLIF(REGEXP_SUBSTR(study_name, '\\b[A-Za-z][0-9]{4}\\b'), '')
        )) AS extracted_codes
    FROM (
        SELECT REGEXP_REPLACE(
                REGEXP_REPLACE(study_name,
                    '([0-9]{5})\\s+[Oo][Rr]\\s+([0-9]{5})', '\\1,\\2'),
                '([0-9]{5})\\s*/\\s*([0-9]{5})', '\\1,\\2')
               AS study_name
        FROM kinsula_leq.radiology
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
        ON codes.code = cpt.PROCEDURECODE AND codes.code REGEXP '^[0-9]{5}$'
    LEFT JOIN semantics.hcpcs hcpcs
        ON codes.code = hcpcs.HCPC        AND codes.code REGEXP '^[A-Za-z][0-9]{4}$'
    GROUP BY b.study_name, b.extracted_codes
),

cpt_std AS (
    SELECT
        l.study_name,
        NULLIF(l.extracted_codes, '') AS extracted_codes,
        NULLIF(l.proc_code_std,   '') AS proc_code_std
    FROM code_lookup l
)

SELECT DISTINCT
    c.study_name,
    c.extracted_codes,
    c.proc_code_std,

    -- MODALITY STD
    CASE
        -- Combined modalities (must come first)
        WHEN c.study_name REGEXP '\\bPET/CT\\b|\\bPET CT\\b'                                                                                                THEN 'Positron Emission Tomography (PET) / Computed Tomography'
        WHEN c.study_name REGEXP '\\bXR/RF\\b'                                                                                                               THEN 'Digital Radiography / Radio Fluoroscopy'
        WHEN c.study_name REGEXP '\\bUS DOPPLER\\b|\\bUS DUPLEX\\b'                                                                                          THEN 'Ultrasound / Duplex Doppler'
        WHEN c.study_name REGEXP '\\bUS ECHOCARDIOGRAM\\b'                                                                                                   THEN 'Ultrasound / Echocardiography'
        WHEN c.study_name REGEXP '\\bXA US\\b'                                                                                                               THEN 'X-Ray Angiography / Ultrasound'
        -- Single modalities
        WHEN c.study_name REGEXP '\\bCT\\b|\\bCAT\\b|\\bNCT\\b|\\bLDCT\\b|\\bCTA\\b|\\bCTV\\b|\\bCTAC\\b|\\bCTC\\b|\\bCTP\\b'                              THEN 'Computed Tomography'
        WHEN c.study_name REGEXP '\\bPET\\b'                                                                                                                 THEN 'Positron Emission Tomography (PET)'
        -- \bPT\b with exclusion for lab coagulation tests and E&M billing codes
        WHEN c.study_name REGEXP '\\bPT\\b'
             AND c.study_name NOT REGEXP '\\bPT/|/PT\\b|\\bPT\\s+(PANEL|COAGULATION)|\\bPROTIME\\b|\\bINR\\b|\\bPTT\\b|\\bESTAB\\s+PT\\b|\\bNEW\\s+PT\\b|\\bMED\\s+DECISION\\b'
                                                                                                                                                              THEN 'Positron Emission Tomography (PET)'
        -- MRA before MRI/MR
        WHEN c.study_name REGEXP '\\bMRA\\b|\\bzzMRA\\b'                                                                                                     THEN 'Magnetic Resonance Angiography (MA - Retired) / Magnetic Resonance'
        WHEN c.study_name REGEXP '\\bMRI\\b|\\bMRV\\b|\\bMRCP\\b|\\b3TMRI\\b|\\bTMRI\\b|\\b3TMRA\\b|\\bMR\\b'                                             THEN 'Magnetic Resonance'
        WHEN c.study_name REGEXP '\\bDEXA\\b|\\bDXA\\b'                                                                                                     THEN 'Bone Densitometry (X-Ray)'
        WHEN c.study_name REGEXP '\\bMAM\\b|\\bMAMM\\b|\\bMAMMO\\b|\\bMMAMMO\\b|\\bMAMMOGRAM\\b|\\bMAMMOGRAPHY\\b'                                        THEN 'Mammography'
        -- \bMG\b with exclusion for Myasthenia Gravis
        WHEN c.study_name REGEXP '\\bMG\\b'
             AND c.study_name NOT REGEXP '\\bMYASTHENIA\\b|\\bGRAVIS\\b|\\bEVALUATION\\b'                                                                   THEN 'Mammography'
        WHEN c.study_name REGEXP '\\bUS\\b|\\bULTRASOUND\\b|\\bUSV\\b|\\bBI US\\b|\\bOB US\\b'                                                              THEN 'Ultrasound'
        -- \bXA\b with exclusion for anti-Xa lab context
        WHEN c.study_name REGEXP '\\bXA\\b'
             AND c.study_name NOT REGEXP '\\bANTI-XA\\b|\\bANTI XA\\b|\\bHEPARIN\\b'                                                                        THEN 'X-Ray Angiography'
        WHEN c.study_name REGEXP '\\bANG\\b|\\bANGIO\\b'                                                                                                    THEN 'X-Ray Angiography'
        WHEN c.study_name REGEXP '\\bCR\\b'                                                                                                                  THEN 'Computed Radiography'
        WHEN c.study_name REGEXP '\\bDX\\b|\\bXR\\b|\\bX-RAY\\b|\\bXRAY\\b|\\bXRY\\b'                                                                      THEN 'Digital Radiography'
        -- \bDR\b with exclusion for HLA typing context
        WHEN c.study_name REGEXP '\\bDR\\b'
             AND c.study_name NOT REGEXP '\\bHLA\\b|\\bTYPING\\b|\\bDQ\\b|\\bDP\\b'                                                                         THEN 'Digital Radiography'
        -- \bRF\b with exclusion for Rheumatoid Factor lab context
        WHEN c.study_name REGEXP '\\bRF\\b'
             AND c.study_name NOT REGEXP '\\bRHEUMATOID\\b|\\bFACTOR\\b|\\bANTI-CCP\\b|\\bANA\\b|\\bSERUM\\b|\\bTITER\\b'                                  THEN 'Radio Fluoroscopy'
        WHEN c.study_name REGEXP '\\bFL\\b|\\bFLUORO\\b|\\bFLU\\b'                                                                                         THEN 'Radio Fluoroscopy'
        WHEN c.study_name REGEXP '\\bFS\\b'                                                                                                                  THEN 'Fundoscopy (FS - Retired) / Ophthalmic Photography'
        WHEN c.study_name REGEXP '\\bNM\\b'                                                                                                                  THEN 'Nuclear Medicine'
        WHEN c.study_name REGEXP '\\bECHO\\b|\\bECHOCARDIOGRAM\\b'                                                                                          THEN 'Echocardiography (EC - Retired) / Ultrasound'
        WHEN c.study_name REGEXP '\\bECG\\b|\\bEKG\\b'                                                                                                      THEN 'Electrocardiography'
        WHEN c.study_name REGEXP '\\bEEG\\b|\\bELECTROCEPHANLOGRAM\\b'                                                                                     THEN 'Electroencephalography'
        WHEN c.study_name REGEXP '\\bENDOSCOPY\\b'                                                                                                          THEN 'Endoscopy'
        WHEN c.study_name REGEXP '\\bCD\\b'                                                                                                                  THEN 'Color Flow Doppler (CD - Retired) / Ultrasound'
        WHEN c.study_name REGEXP '\\bTCD\\b|\\bDUPLEX\\b|\\bDOPPLER\\b'                                                                                    THEN 'Duplex Doppler (DD - Retired) / Ultrasound'
        WHEN c.study_name REGEXP '\\bAUDIO\\b|\\bAUDIOMETRY\\b|\\bAUDITORY\\b|\\bHEARING\\b|\\bAUDIOGRAM\\b|\\bACOUSTIC\\b'                               THEN 'Audio'
        WHEN c.study_name REGEXP '\\bRP\\b'                                                                                                                  THEN 'Radiotherapy Plan'
        -- \bRT\b with exclusion for renal/lab context
        WHEN c.study_name REGEXP '\\bRT\\b'
             AND c.study_name NOT REGEXP '\\bCREATININE\\b|\\bCREAT\\b|\\bRENAL\\b|\\bKIDNEY\\b'                                                            THEN 'Radiographic Imaging (RG) / Interventional Radiology'
        WHEN c.study_name REGEXP '\\bRAD\\b|\\bIR\\b|\\bINTERVENTIONAL RADIOLOGY\\b'                                                                       THEN 'Radiographic Imaging (RG) / Interventional Radiology'
        WHEN c.study_name REGEXP '\\bSPECT\\b'                                                                                                              THEN 'Single-Photon Emission Computed Tomography (ST - Retired) / Nuclear Medicine'
        WHEN c.study_name REGEXP '\\bBX\\b|\\bBIOPSY\\b|\\bVL\\b|\\bOHS\\b|\\bI-123\\b|\\b1-131\\b|\\bMPI\\b'                                             THEN 'Other'
        ELSE NULL
    END AS modality_std,

    -- STRENGTH / VIEWS STD
    CASE
        WHEN UPPER(c.study_name) REGEXP '[0-9]+\\.?[0-9]*\\s*T\\b'
            AND UPPER(c.study_name) REGEXP 'W[/\\s]?WO|W\\s?&\\s?W/?O|W\\s?AND\\s?W/?O|WITHOUT/WITH|WITH/WITHOUT|WWO|WO/W|\\bWO\\b|\\bW/O\\b|\\bWITHOUT\\b|\\bWO CON\\b|\\bNCON\\b|\\bNO CON\\b|\\bWO C\\b|\\bWO CONTRAST\\b|\\bW CON\\b|\\bW CONTRAST\\b|\\bWITH CONTRAST\\b|\\bW C\\b|\\bW/\\b|\\bCON\\b'
            THEN CONCAT(REGEXP_SUBSTR(c.study_name, '[0-9]+\\.?[0-9]*(?=\\s*[Tt]\\b)'), 'T')
        WHEN UPPER(c.study_name) REGEXP '[0-9]+-[0-9]+\\s*V\\b'
            THEN CONCAT(REGEXP_SUBSTR(c.study_name, '[0-9]+-[0-9]+(?=\\s*[Vv]\\b)'), ' Views')
        WHEN UPPER(c.study_name) REGEXP '[0-9]+\\s*V\\b'
            THEN CONCAT(REGEXP_SUBSTR(c.study_name, '[0-9]+(?=\\s*[Vv]\\b)'), ' Views')
        WHEN UPPER(c.study_name) REGEXP '[0-9]+-[0-9]+\\s*VIEW.*(\\+|AND).*[0-9]+-[0-9]+\\s*VIEW|[0-9]+\\s*VIEWS?.*\\+.*[0-9]+\\s*VIEW'
            THEN CONCAT(REGEXP_SUBSTR(c.study_name, '[0-9]+-[0-9]+|[0-9]+(?=\\s*VIEWS?)'), ' Views + ', REGEXP_SUBSTR(c.study_name, '[0-9]+(?=\\s*VIEW)', 1, 2), ' View')
        WHEN UPPER(c.study_name) REGEXP '[><=]{1,2}\\s*[0-9]+\\s*VIEWS?'
            THEN CONCAT(REGEXP_SUBSTR(c.study_name, '[><=]{1,2}'), REGEXP_SUBSTR(c.study_name, '[0-9]+'), ' Views')
        WHEN UPPER(c.study_name) REGEXP '[0-9]+-[0-9]+\\s*VIEW'
            THEN CONCAT(REGEXP_SUBSTR(c.study_name, '[0-9]+-[0-9]+'), ' Views')
        WHEN UPPER(c.study_name) REGEXP '(MIN|MINIMUM)(\\s+OF)?\\s*(ONE|TWO|THREE|FOUR|FIVE|SIX|[0-9]+)\\s*(V\\b|VWS|VIEW|VIEWS)'
            THEN CONCAT('Min ', COALESCE(REGEXP_SUBSTR(c.study_name, '[0-9]+(?=\\s*(V\\b|VWS|VIEW|VIEWS))'),
                CASE
                    WHEN UPPER(c.study_name) REGEXP 'ONE\\s*(V|VIEW|VIEWS)'   THEN '1'
                    WHEN UPPER(c.study_name) REGEXP 'TWO\\s*(V|VIEW|VIEWS)'   THEN '2'
                    WHEN UPPER(c.study_name) REGEXP 'THREE\\s*(V|VIEW|VIEWS)' THEN '3'
                    WHEN UPPER(c.study_name) REGEXP 'FOUR\\s*(V|VIEW|VIEWS)'  THEN '4'
                END), ' Views')
        WHEN UPPER(c.study_name) REGEXP '[0-9]+\\s*[\\+]\\s*VIEWS?|[0-9]+\\s*PLUS\\s*VIEWS?'
            THEN CONCAT(REGEXP_SUBSTR(c.study_name, '[0-9]+'), '+ Views')
        WHEN UPPER(c.study_name) REGEXP '[0-9]+\\s*OR\\s*MORE\\s*VIEWS?'
            THEN CONCAT(REGEXP_SUBSTR(c.study_name, '[0-9]+'), ' or More Views')
        WHEN UPPER(c.study_name) REGEXP '[0-9]+\\s*OR\\s*[0-9]+\\s*VIEWS?'
            THEN CONCAT(REGEXP_SUBSTR(c.study_name, '[0-9]+'), ' or ', REGEXP_SUBSTR(c.study_name, '[0-9]+', 1, 2), ' Views')
        WHEN UPPER(c.study_name) REGEXP '\\b(ONE|TWO|THREE|FOUR|FIVE|SIX)\\s*VIEWS?\\b'
            THEN CONCAT(
                CASE
                    WHEN UPPER(c.study_name) REGEXP '\\bONE\\s*VIEWS?'   THEN '1'
                    WHEN UPPER(c.study_name) REGEXP '\\bTWO\\s*VIEWS?'   THEN '2'
                    WHEN UPPER(c.study_name) REGEXP '\\bTHREE\\s*VIEWS?' THEN '3'
                    WHEN UPPER(c.study_name) REGEXP '\\bFOUR\\s*VIEWS?'  THEN '4'
                    WHEN UPPER(c.study_name) REGEXP '\\bFIVE\\s*VIEWS?'  THEN '5'
                    WHEN UPPER(c.study_name) REGEXP '\\bSIX\\s*VIEWS?'   THEN '6'
                END, ' Views')
        WHEN UPPER(c.study_name) REGEXP '(LESS\\s*THAN|<)\\s*[0-9]+\\s*(V\\b|VIEW|VIEWS)'
            THEN CONCAT('Less Than ', REGEXP_SUBSTR(c.study_name, '[0-9]+(?=\\s*(V\\b|VIEW|VIEWS))'), ' Views')
        WHEN UPPER(c.study_name) REGEXP '[0-9]+\\+?\\s*VIEWS?'
            THEN CONCAT(REGEXP_SUBSTR(c.study_name, '[0-9]+'), ' Views')
        ELSE NULL
    END AS strength_views_std,

    -- CONTRAST TYPE STD
    CASE
        WHEN UPPER(c.study_name) REGEXP 'W[/\\s]?WO|W\\s?&\\s?W/?O|W\\s?AND\\s?W/?O|W\\s?OR\\s?W/?O|WITH\\s?AND\\s?W/?O|WO\\+W|W\\+W/?O|WITHOUT/WITH|WITH/WITHOUT|W\\s?AND\\s?WOW|WO,\\s?W|W,\\s?WO|WWO|W/W/O|WO/W|W/&W/O|W AND OR WO|W WO|W\\s?W/?O|WO\\s?W'
            THEN 'With and Without Contrast'
        WHEN UPPER(c.study_name) REGEXP '\\bWO\\b|\\bW/O\\b|\\bWITHOUT\\b|\\bWO CON\\b|\\bW/O CONTRAST\\b|\\bNCON\\b|\\bNO CON\\b|\\bWO C\\b|\\bWO CONTRAST\\b'
            THEN 'Without Contrast'
        WHEN UPPER(c.study_name) REGEXP '\\bW CON\\b|\\bW CONTRAST\\b|\\bWITH CONTRAST\\b|\\bW C\\b|\\bW/\\b|\\bCON\\b'
            THEN 'With Contrast'
        ELSE NULL
    END AS contrast_type_std,

    -- BODY PART STD
    CASE
        -- MULTI BODY PARTS FIRST
        WHEN UPPER(c.study_name) REGEXP 'CHEST.*(ABDOMEN|ABD).*(PELVIS|PELV)|CHEST/ABD/PELVIS|CHEST ABDOMEN PELVIS|CHEST\\+ABD.*PELVIS'                     THEN 'Chest, Abdomen, Pelvis'
        WHEN UPPER(c.study_name) REGEXP 'CHEST.*(ABDOMEN|ABD)|CHEST\\+ABD'                                                                                  THEN 'Chest, Abdomen'
        WHEN UPPER(c.study_name) REGEXP 'CHEST.*THORAX|CHEST/THORAX'                                                                                         THEN 'Chest, Thorax'
        WHEN UPPER(c.study_name) REGEXP '(ABDOMEN|ABD).*(PELVIS|PELV)|(PELVIS|PELV).*(ABDOMEN|ABD)'                                                          THEN 'Abdomen, Pelvis'
        WHEN UPPER(c.study_name) REGEXP 'HEAD.*NECK|NECK.*HEAD|NECK/HEAD'                                                                                    THEN 'Head, Neck'
        WHEN UPPER(c.study_name) REGEXP 'ORB.*FAC.*NCK|ORBIT.*FACE.*NECK|ORBIT/FACE/NK'                                                                     THEN 'Orbit, Face, Neck'
        WHEN UPPER(c.study_name) REGEXP 'ORBIT.*SELLA|ORBIT\\+SELLA|ORBIT SELLA POSS|ORBIT\\+SELLA\\+PF'                                                    THEN 'Orbit, Sella'
        WHEN UPPER(c.study_name) REGEXP 'CERVICOTHORACOLUMBAR|CERVICO.*THORACO.*LUMBAR'                                                                      THEN 'Thoracic Spine, Lumbar Spine'
        WHEN UPPER(c.study_name) REGEXP 'THORACO.?LUMBAR|THORACOLUMBAR|THOR.*LUM[B]|THOR\\+LU[MB]'                                                          THEN 'Thoracic Spine, Lumbar Spine'
        WHEN UPPER(c.study_name) REGEXP 'LUMBOSACRAL PLEXUS'                                                                                                 THEN 'Lumbar Spine, Sacrum'
        WHEN UPPER(c.study_name) REGEXP 'LUMBO.?SACRAL|LUMB.?SACR|LUMBO SACRAL|LUMBOSACRAL'                                                                 THEN 'Lumbar Spine, Sacrum'
        WHEN UPPER(c.study_name) REGEXP 'SACRUM.*(AND|\\+|/).?COCCYX|SACRUM COCCYX|SACRUM/COCCYX'                                                           THEN 'Sacrum, Coccyx'
        WHEN UPPER(c.study_name) REGEXP 'FACIAL.*SINUS|SINUS.*FACIAL|FACIAL/SINUS|MAX.*FAC.*SIN|MAXILLOFACIAL|MAXIOFACIAL|MAXFACIAL|MAXILLA|SINUS FACIAL|MAX/FAC/SIN|MAXFACIAL BONES' THEN 'Facial Bones, Sinuses'
        WHEN UPPER(c.study_name) REGEXP 'TIB.?FIB|TIBIA.?FIBUL|TIBIA AND FIBULA|TIBIA/FIBULA|TIB & FIB|TIB\\+FIBULA|TIBIA\\+FIBULA|TM JOINTS|TIB\\s*\\\\T\\\\\\s*FIB|TIB\\s+T\\s+FIB' THEN 'Tibia, Fibula'
        WHEN UPPER(c.study_name) REGEXP 'FOREARM.*RADIUS|FOREARM.*ULNA|RADIUS.*ULNA|FOREARM/RADIUS'                                                          THEN 'Forearm, Radius, Ulna'
        WHEN UPPER(c.study_name) REGEXP 'CAROTID.*NECK|CAROTID/NECK|VASC CAROTID|CAROTIDS'                                                                  THEN 'Carotid, Neck'
        WHEN UPPER(c.study_name) REGEXP 'THYROID.*NECK|THY.*NECK|THYROID/NECK'                                                                               THEN 'Thyroid, Neck'
        WHEN UPPER(c.study_name) REGEXP 'LIVER.*GALLBLADDER.*PANCREAS|LIVER GALLBLADDER PANCREAS'                                                            THEN 'Liver, Gallbladder, Pancreas'
        WHEN UPPER(c.study_name) REGEXP 'ILIUM.*STERNUM.*RIB|ILIUM STERNUM RIB'                                                                              THEN 'Ilium, Sternum, Rib'
        WHEN UPPER(c.study_name) REGEXP 'AC JOINTS|ACROMIOCLAVICULAR JOINTS|STERNOCLAVIC|STERNOCLAVICULAR'                                                   THEN 'Acromioclavicular Joints'
        WHEN UPPER(c.study_name) REGEXP '\\bTMJ\\b|TEMPOROMANDIBULAR|TEMPOROMANDIBULAR JOINT|TMJ BILATERAL'                                                 THEN 'Temporomandibular Joint'
        WHEN UPPER(c.study_name) REGEXP 'VASC EXT LOWR|VASC EXTREMITY LOWER|LOWER EXTREMITY VENOUS|LOWER EXT VENOUS|LOWER EXTREMITY ARTERIES|LOWER EXT ARTERIAL|ARTERIAL LOW.*EXT|ARTERIAL LOWER EXT|ARTERIAL LOWER EXTREMITY|LOWER EXTREMITY ARTERI' THEN 'Lower Extremity (Vascular)'
        WHEN UPPER(c.study_name) REGEXP 'UPPER EXTREMITY VENOUS|UPPER EXT.*ARTERIAL|ARTERIAL UPPER EXT|ARTERIAL UPPER EXTREMITY|UPPER EXTREMITY ARTERIAL|LT UPPER VENOUS|UPPER OR LOWER EXT ARTERIAL' THEN 'Upper Extremity (Vascular)'
        WHEN UPPER(c.study_name) REGEXP 'VASC TRANSCRANIAL|TRANS CRANIAL|TRANSCRANIAL|VASC JUGULAR|JUGULAR.*SUBCLAVIAN'                                      THEN 'Transcranial (Vascular)'
        WHEN UPPER(c.study_name) REGEXP 'CEREBRAL ARTERIES|EXTRACRANIAL ARTERIES'                                                                            THEN 'Cerebral Arteries'
        WHEN UPPER(c.study_name) REGEXP '\\bLOWER EXTREMIT|\\bLOWER EXT\\b|\\bLOWER EXTR\\b|\\bLWR EXT\\b|\\bLE\\b|\\bLOW EXT\\b|\\bLEFT LOWER EXTREMITY\\b|\\bRIGHT LOWER EXTREMITY\\b|LOWER EXT.*NOT.*JNT|LOWER EXT.*NON|LWR EXT NOT JT|LOWER LEG|LOWER BACK|EXTREMITY LOWER|EXTREMITY.*LOWER' THEN 'Lower Extremity'
        WHEN UPPER(c.study_name) REGEXP '\\bUPPER EXTREMIT|\\bUPPER EXT\\b|\\bUPR.*EXT\\b|\\bUPR/LXTR\\b|UP EXT JT|LEFT EXTREMITY.*UPPER|UPPER EXT.*JOINT|UPPER EXT NON JOINT|UPPER EXTREMITY.*MUSCULO|EXTREMITY UPPER|EXTREMITY.*UPPER|LEFT EXTREMITY JOINT UPPER|RIGHT EXTREMITY JOINT LOWER' THEN 'Upper Extremity'
        WHEN UPPER(c.study_name) REGEXP 'BRACHIAL PLEXUS|RIGHT BRACHIAL PLEXUS|\\bBRACHPLEX\\b'                                                             THEN 'Brachial Plexus'
        WHEN UPPER(c.study_name) REGEXP 'SPINAL CANAL|SPINAL CORD|THORACIC SPINAL CORD|SPINAL CORD DORSAL'                                                  THEN 'Spinal Canal'
        WHEN UPPER(c.study_name) REGEXP 'SACROILIAC|SACROILIAC JOINT|SACROILIAC JNTS|SI JOINT'                                                              THEN 'Sacroiliac Joint'
        -- SINGLE BODY PARTS
        WHEN UPPER(c.study_name) REGEXP '\\bABDOMEN\\b|\\bABD\\b|\\bABDOMINAL\\b'                                                                          THEN 'Abdomen'
        WHEN UPPER(c.study_name) REGEXP '\\bANKLE\\b|\\bANK\\b'                                                                                             THEN 'Ankle'
        WHEN UPPER(c.study_name) REGEXP '\\bAORTA\\b|\\bTHORACIC AORTA\\b'                                                                                  THEN 'Aorta'
        WHEN UPPER(c.study_name) REGEXP '\\bARTERY\\b|\\bARTERIAL\\b'                                                                                       THEN 'Arterial'
        WHEN UPPER(c.study_name) REGEXP 'AUDITORY CANAL'                                                                                                     THEN 'Auditory Canal'
        WHEN UPPER(c.study_name) REGEXP 'AXIAL SKELETON'                                                                                                     THEN 'Axial Skeleton'
        WHEN UPPER(c.study_name) REGEXP '\\bBONE\\b'                                                                                                         THEN 'Bone'
        WHEN UPPER(c.study_name) REGEXP '\\bBRAIN\\b|\\bBRAINSTEM\\b|\\bBRIAN\\b|\\bBRIN\\b'                                                               THEN 'Brain'
        WHEN UPPER(c.study_name) REGEXP '\\bIAC\\b|\\bIACS\\b|INTERNAL AUDITORY|AUDITORY MEATUS|AUDITORY CANAL MRI'                                         THEN 'Internal Auditory Canal'
        -- CN V — Trigeminal nerve
        WHEN UPPER(c.study_name) REGEXP '\\bTRIGEMINAL\\b|\\b5TH.*NERVE\\b|\\bCN\\s*V\\b|\\bCRANIAL NERVE\\s*5\\b'                                        THEN 'Trigeminal Nerve'
        -- CN VII — Facial nerve
        WHEN UPPER(c.study_name) REGEXP '\\b7TH.*NERVE\\b|\\bCN\\s*VII\\b|\\bCRANIAL NERVE\\s*7\\b|\\bFACIAL NERVE\\b'                                     THEN 'Facial Nerve'
        -- CN VIII — Acoustic nerve
        WHEN UPPER(c.study_name) REGEXP '\\b8TH.*NERVE\\b|\\bCN\\s*VIII\\b|\\bCRANIAL NERVE\\s*8\\b|\\bACOUSTIC NEUROMA\\b|\\bVESTIBULOCOCHLEAR\\b'       THEN 'Acoustic Nerve'
        WHEN UPPER(c.study_name) REGEXP '\\bCARDIAC\\b|\\bCARDIOVASC\\b'                                                                                    THEN 'Heart'
        WHEN UPPER(c.study_name) REGEXP '\\bSACRAL PLEXUS\\b'                                                                                                THEN 'Sacrum'
        WHEN UPPER(c.study_name) REGEXP 'MUSCLE.*\\bUE\\b|MUSCLE.*UPPER|MUSCLE.*\\bUPR\\b'                                                                  THEN 'Upper Extremity'
        WHEN UPPER(c.study_name) REGEXP 'MUSCLE.*\\bLE\\b|MUSCLE.*LOWER|MUSCLE.*\\bLWR\\b'                                                                  THEN 'Lower Extremity'
        WHEN UPPER(c.study_name) REGEXP 'VISUAL EVOKED|EVOKED.*VISUAL|EVOKED.*POTENTIAL.*VIS'                                                                THEN 'Eye / Orbit'
       
										
        -- L-SPINE, T-SPINE shorthand before generic SPINE
        WHEN UPPER(c.study_name) REGEXP '\\bL-SPINE\\b|\\bL SPINE\\b|\\bLS-SPINE\\b|\\bL-S SPINE\\b|\\bLS SPINE\\b'                                        THEN 'Lumbar Spine'
        WHEN UPPER(c.study_name) REGEXP '\\bT-SPINE\\b|\\bT SPINE\\b'                                                                                       THEN 'Spine'
        WHEN UPPER(c.study_name) REGEXP '\\bBREAST\\b|\\bBREASTS\\b'                                                                                        THEN 'Breast'
        WHEN UPPER(c.study_name) REGEXP '\\bCALF\\b'                                                                                                         THEN 'Calf'
        WHEN UPPER(c.study_name) REGEXP '\\bCAROTID\\b|\\bCAROTIDS\\b'                                                                                      THEN 'Carotid'
        WHEN UPPER(c.study_name) REGEXP '\\bCERVICAL SPINE\\b|\\bSPINE CERVICAL\\b|\\bC-SPINE\\b'                                                           THEN 'Cervical Spine'
        WHEN UPPER(c.study_name) REGEXP '\\bCERVICAL\\b'                                                                                                    THEN 'Cervical'
        WHEN UPPER(c.study_name) REGEXP '\\bCHEST\\b|\\bPA CHEST\\b|\\bCHEST PA\\b|\\bTHORAX\\b|\\bRIBS\\b|\\bPNEUMOTHORAX\\b|\\bTHORACENTESIS\\b'       THEN 'Chest'
        WHEN UPPER(c.study_name) REGEXP '\\bCLAVICLE\\b'                                                                                                    THEN 'Clavicle'
        WHEN UPPER(c.study_name) REGEXP '\\bCOCCYX\\b'                                                                                                      THEN 'Coccyx'
        WHEN UPPER(c.study_name) REGEXP '\\bCOLON\\b|\\bLARGE INTESTINE\\b'                                                                                 THEN 'Colon'
        WHEN UPPER(c.study_name) REGEXP 'CRANIAL NERVE'                                                                                                      THEN 'Cranial Nerve'
        WHEN UPPER(c.study_name) REGEXP '\\bEAR\\b'                                                                                                         THEN 'Ear'
        WHEN UPPER(c.study_name) REGEXP '\\bELBOW\\b|\\bELB\\b'                                                                                             THEN 'Elbow'
        WHEN UPPER(c.study_name) REGEXP '\\bESOPHAGUS\\b|\\bTRANSESOPHAGEAL\\b'                                                                            THEN 'Esophagus'
        WHEN UPPER(c.study_name) REGEXP '\\bEXTRACRANIAL\\b|\\bEXTRACRAN\\b'                                                                               THEN 'Extracranial'
        WHEN UPPER(c.study_name) REGEXP '\\bEYE\\b|\\bORBIT\\b|\\bORBITS\\b|\\bORB\\b|OPTIC NERVE'                                                         THEN 'Eye / Orbit'
        WHEN UPPER(c.study_name) REGEXP '\\bFACE\\b|\\bFACIAL\\b|\\bFACIAL BONES\\b'                                                                       THEN 'Face'
        WHEN UPPER(c.study_name) REGEXP '\\bFEMUR\\b|\\bFEM\\b|\\bLATERAL FEMORAL\\b'                                                                       THEN 'Femur'
        WHEN UPPER(c.study_name) REGEXP '\\bFINGERS\\b|\\bFINGER\\b'                                                                                        THEN 'Fingers'
        WHEN UPPER(c.study_name) REGEXP '\\bFOOT\\b|\\bFEET\\b|\\bFT\\b|\\bHEEL\\b'                                                                        THEN 'Foot'
        WHEN UPPER(c.study_name) REGEXP '\\bFOREARM\\b|\\bFORE\\b'                                                                                          THEN 'Forearm'
        WHEN UPPER(c.study_name) REGEXP '\\bGALLBLADDER\\b'                                                                                                 THEN 'Gallbladder'
        WHEN UPPER(c.study_name) REGEXP '\\bGI\\b|\\bUGI\\b|\\bGASTRIC\\b|\\bGASTROINTESTINAL\\b|\\bSMALL INTESTINE\\b|\\bSTOMACH\\b|\\bBOWEL\\b'         THEN 'Gastric / GI'
        WHEN UPPER(c.study_name) REGEXP 'GREATER OCCIPITAL|OCCIPITAL'                                                                                        THEN 'Occipital'
        WHEN UPPER(c.study_name) REGEXP '\\bGROIN\\b|\\bILIOINGUINAL\\b'                                                                                    THEN 'Groin'
        WHEN UPPER(c.study_name) REGEXP '\\bHAND\\b|\\bHANDS\\b'                                                                                            THEN 'Hand'
        WHEN UPPER(c.study_name) REGEXP '\\bHEAD\\b|\\bORBITS\\b'                                                                                           THEN 'Head'
        WHEN UPPER(c.study_name) REGEXP '\\bHEART\\b|\\bTRANSTHORACIC\\b'                                                                                  THEN 'Heart'
        WHEN UPPER(c.study_name) REGEXP '\\bHIP\\b|\\bHIPS\\b'                                                                                              THEN 'Hip'
        WHEN UPPER(c.study_name) REGEXP '\\bHUMERUS\\b|\\bHUM\\b|\\bUPPER ARM\\b'                                                                           THEN 'Humerus / Upper Arm'
        WHEN UPPER(c.study_name) REGEXP '\\bINTRACRANIAL\\b|\\bINTRACRAN\\b'                                                                               THEN 'Intracranial'
        WHEN UPPER(c.study_name) REGEXP '\\bKIDNEY\\b|\\bKIDNEYS\\b|\\bRENAL\\b'                                                                           THEN 'Kidney'
        WHEN UPPER(c.study_name) REGEXP '\\bKNEE\\b|\\bKNEES\\b|\\bKN\\b'                                                                                  THEN 'Knee'
        WHEN UPPER(c.study_name) REGEXP '\\bLEG\\b|\\bLOWER LEG\\b'                                                                                         THEN 'Leg'
        WHEN UPPER(c.study_name) REGEXP '\\bLIVER\\b'                                                                                                        THEN 'Liver'
        WHEN UPPER(c.study_name) REGEXP 'LUMBAR PLEXUS|LUMPLEX'                                                                                              THEN 'Lumbar Plexus'
        WHEN UPPER(c.study_name) REGEXP '\\bLUMBAR SPINE\\b|\\bSPINE LUMBAR\\b|\\bLUMBOSACRAL\\b|\\bLUMOSACRAL\\b'                                        THEN 'Lumbar Spine'
        WHEN UPPER(c.study_name) REGEXP '\\bLUMBAR\\b'                                                                                                       THEN 'Lumbar'
        WHEN UPPER(c.study_name) REGEXP '\\bLUNG\\b'                                                                                                         THEN 'Lung'
        WHEN UPPER(c.study_name) REGEXP 'LYMPH NODE'                                                                                                          THEN 'Lymph Node'
        WHEN UPPER(c.study_name) REGEXP '\\bMANDIBLE\\b'                                                                                                    THEN 'Mandible'
        WHEN UPPER(c.study_name) REGEXP '\\bMASTOIDS\\b|\\bMASTOID\\b'                                                                                      THEN 'Mastoids'
        WHEN UPPER(c.study_name) REGEXP '\\bNECK\\b|\\bNECK SOFT TISSUE\\b|\\bTHROAT\\b'                                                                   THEN 'Neck'
        WHEN UPPER(c.study_name) REGEXP '\\bPANCREAS\\b'                                                                                                    THEN 'Pancreas'
        WHEN UPPER(c.study_name) REGEXP '\\bPARATHYROID\\b'                                                                                                 THEN 'Parathyroid'
        WHEN UPPER(c.study_name) REGEXP '\\bPELVIS\\b|\\bPELVIC\\b'                                                                                         THEN 'Pelvis'
        WHEN UPPER(c.study_name) REGEXP '\\bPITUITARY\\b|\\bPITUITARY GLAND\\b|\\bSELLA TURCICA\\b|\\bSELLA\\b'                                            THEN 'Pituitary'
        WHEN UPPER(c.study_name) REGEXP '\\bPROSTATE\\b|\\bRECTAL\\b'                                                                                       THEN 'Prostate / Rectal'
        WHEN UPPER(c.study_name) REGEXP '\\bRETROPERITONEUM\\b'                                                                                             THEN 'Retroperitoneum'
        WHEN UPPER(c.study_name) REGEXP '\\bSACRUM\\b'                                                                                                       THEN 'Sacrum'
        WHEN UPPER(c.study_name) REGEXP '\\bSCAPULA\\b|\\bSCAP\\b'                                                                                          THEN 'Scapula'
        WHEN UPPER(c.study_name) REGEXP '\\bSCOLIOSIS\\b'                                                                                                   THEN 'Scoliosis'
        WHEN UPPER(c.study_name) REGEXP '\\bSCROTAL\\b|\\bSCROTUM\\b|\\bTESTICULAR\\b|\\bTESTICLE\\b|\\bTESTES\\b'                                        THEN 'Scrotal / Testicular'
        WHEN UPPER(c.study_name) REGEXP '\\bSHOULDER\\b|\\bSH\\b|UPPER EXT JOINT SHOULDER|\\bSHOULDERS\\b'                                                 THEN 'Shoulder'
        WHEN UPPER(c.study_name) REGEXP '\\bSINUS\\b|\\bSINUSES\\b|\\bNASAL\\b|\\bSINUS/NASAL\\b'                                                          THEN 'Sinuses'
        WHEN UPPER(c.study_name) REGEXP '\\bSKULL\\b'                                                                                                        THEN 'Skull'
        WHEN UPPER(c.study_name) REGEXP 'SPINAL CORD DORSAL'                                                                                                  THEN 'Spinal Cord'
        WHEN UPPER(c.study_name) REGEXP '\\bSPINE\\b|\\bSCPINE\\b'                                                                                          THEN 'Spine'
        WHEN UPPER(c.study_name) REGEXP '\\bSPLEEN\\b'                                                                                                       THEN 'Spleen'
        WHEN UPPER(c.study_name) REGEXP '\\bSTERNUM\\b'                                                                                                      THEN 'Sternum'
        WHEN UPPER(c.study_name) REGEXP '\\bTEETH\\b'                                                                                                     THEN 'Teeth'
        WHEN UPPER(c.study_name) REGEXP '\\bTHUMB\\b'                                                                                                     THEN 'Thumb'

        WHEN UPPER(c.study_name) REGEXP 'TEMPORAL BONE'                                                                                                       THEN 'Temporal Bone'
        WHEN UPPER(c.study_name) REGEXP '\\bTHIGH\\b|\\bTHIGHS\\b'                                                                                          THEN 'Thigh'
        WHEN UPPER(c.study_name) REGEXP '\\bTHORACIC\\b'                                                                                                     THEN 'Thoracic'
        WHEN UPPER(c.study_name) REGEXP '\\bTHYROID\\b'                                                                                                      THEN 'Thyroid'
        WHEN UPPER(c.study_name) REGEXP '\\bTOES\\b|\\bTOE\\b'                                                                                              THEN 'Toes'
        WHEN UPPER(c.study_name) REGEXP '\\bTORSO\\b|\\bPE TORSO\\b'                                                                                        THEN 'Torso'
        WHEN UPPER(c.study_name) REGEXP 'TRANSVAGINAL|TRANS-VAGINAL|TRANS VAGINAL'                                                                           THEN 'Transvaginal'
        WHEN UPPER(c.study_name) REGEXP '\\bUTERUS\\b'                                                                                                       THEN 'Uterus'
        WHEN UPPER(c.study_name) REGEXP 'VAGUS NERVE'                                                                                                         THEN 'Vagus Nerve'
        WHEN UPPER(c.study_name) REGEXP '\\bVEINS\\b|\\bVENOUS\\b'                                                                                          THEN 'Veins'
        WHEN UPPER(c.study_name) REGEXP 'WHOLE BODY'                                                                                                          THEN 'Whole Body'
        WHEN UPPER(c.study_name) REGEXP '\\bWRIST\\b|\\bWRISTS\\b|\\bWR\\b'                                                                                 THEN 'Wrist'
        ELSE NULL
    END AS body_part_std,

    -- LATERALITY STD
    CASE
        WHEN UPPER(c.study_name) REGEXP '\\bBILATERAL\\b'         THEN 'Bilateral'
        WHEN UPPER(c.study_name) REGEXP '\\bUNILATERAL\\b'        THEN 'Unilateral'
        WHEN UPPER(c.study_name) REGEXP '\\bLEFT\\b|\\bLT\\b'     THEN 'Left'
        WHEN UPPER(c.study_name) REGEXP '\\bRIGHT\\b|\\bRT\\b'    THEN 'Right'
        ELSE NULL
    END AS laterality_std,

    -- TRACER NAME STD
    CASE
        WHEN UPPER(c.study_name) REGEXP 'FLORBETAPIR|AMYVID|A9591|F-?18\\s*FLORBETAPIR|18F-?FLORBETAPIR|\\[18F\\]\\s*FLORBETAPIR'    THEN 'Florbetapir F18 (Amyvid)'
        WHEN UPPER(c.study_name) REGEXP 'FLUTEMETAMOL|VIZAMYL|A9592|F-?18\\s*FLUTEMETAMOL|18F-?FLUTEMETAMOL|\\[18F\\]\\s*FLUTEMETAMOL' THEN 'Flutemetamol F18 (Vizamyl)'
        WHEN UPPER(c.study_name) REGEXP 'FLORBETABEN|NEURACEQ|A9593|F-?18\\s*FLORBETABEN|18F-?FLORBETABEN|\\[18F\\]\\s*FLORBETABEN'    THEN 'Florbetaben F18 (Neuraceq)'
        WHEN UPPER(c.study_name) REGEXP '\\bPIB\\b|PITTSBURGH\\s*COMPOUND|F-?18\\s*PIB|18F-?PIB'                                       THEN 'PiB F18 (Pittsburgh Compound-B)'
        WHEN UPPER(c.study_name) REGEXP 'PIFLUFOLASTAT|PYLARIFY|A9816|DCFPYL'                                                          THEN 'Piflufolastat F18 (Pylarify)'
        WHEN UPPER(c.study_name) REGEXP 'FLOTUFOLASTAT|POSLUMA|A9815|PSMA-?1007'                                                       THEN 'Flotufolastat F18 (Posluma)'
        WHEN UPPER(c.study_name) REGEXP '\\bPSMA\\b' AND UPPER(c.study_name) REGEXP 'F-?18|\\bF18\\b|\\[18F\\]|18F-?'                 THEN 'F18 - PSMA Tracer (Unspecified)'
        WHEN UPPER(c.study_name) REGEXP '\\bFDG\\b|FLUORODEOXYGLUCOSE|FLUDEOXYGLUCOSE|A9552|FDG-?PET'                                  THEN 'FDG F18 (Fluorodeoxyglucose)'
        WHEN UPPER(c.study_name) REGEXP 'SODIUM\\s*FLUORIDE|\\bNAF\\b|A9580|F-?18\\s*FLUORIDE|18F-?FLUORIDE|18F-?NAF'                 THEN 'Sodium Fluoride F18 (NaF)'
        WHEN UPPER(c.study_name) REGEXP '\\bFDOPA\\b|FLUORODOPA|FLUORO-?DOPA|A9600|F-?18\\s*FDOPA|18F-?FDOPA|18F-?DOPA'               THEN 'Fluorodopa F18 (FDOPA)'
        WHEN UPPER(c.study_name) REGEXP 'FLUCICLOVINE|AXUMIN|A9584|\\bFACBC\\b'                                                        THEN 'Fluciclovine F18 (Axumin)'
        WHEN UPPER(c.study_name) REGEXP 'FLORTAUCIPIR|TAUVID|A9814|TAU\\s*PET'                                                         THEN 'Flortaucipir F18 (Tauvid)'
        WHEN UPPER(c.study_name) REGEXP '\\bF-?18\\b|\\[18F\\]|18F-?|\\bF18\\b'                                                       THEN 'F18 - Tracer Not Specified'
        WHEN UPPER(c.study_name) REGEXP 'GA-?68\\s*DOTATATE|NETSPOT|\\bDOTATATE\\b'                                                    THEN 'Ga68-DOTATATE (Netspot)'
        WHEN UPPER(c.study_name) REGEXP 'GA-?68\\s*DOTATOC|\\bDOTATOC\\b'                                                             THEN 'Ga68-DOTATOC'
        WHEN UPPER(c.study_name) REGEXP 'GA-?68\\s*PSMA|ILLUCCIX|LOCAMETZ|\\bPSMA-?11\\b'                                             THEN 'Ga68-PSMA-11 (Illuccix/Locametz)'
        WHEN UPPER(c.study_name) REGEXP '\\bGA-?68\\b|\\[68GA\\]|68GA-?|\\bGALLIUM\\s*68\\b|\\bGALLIUM-?68\\b'                       THEN 'Ga68 - Tracer Not Specified'
        ELSE NULL
    END AS tracer_name_std

FROM cpt_std c;