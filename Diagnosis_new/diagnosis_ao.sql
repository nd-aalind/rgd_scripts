select t.*,MD5((CONCAT_WS(
                ':',
                COALESCE(psid, ''), COALESCE(ndid, ''), COALESCE(eid, ''),
                COALESCE(enc_date, ''), COALESCE(diag_date, ''), COALESCE(diag_id, '')
                ,COALESCE(diag_code, '') ,COALESCE(diag_desc, ''),COALESCE(snomed_code, '') ,
                COALESCE(snomed_desc, ''),COALESCE(primary_diagnosis_flag, '')))) AS udm_unq_id,
                COALESCE(
    enc_date,
    diag_date
) as enc_date_proxy from (
SELECT
    dx.source_dx_id                         AS diag_id,
    dx.patient_id                           AS ndid,
    dx.clinical_encounter_id                AS eid,
    CASE
        WHEN dx.encounter_date IN ('None', '')                                          THEN NULL
        WHEN LENGTH(dx.encounter_date) = 10
             AND dx.encounter_date REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'              THEN STR_TO_DATE(dx.encounter_date, '%Y-%m-%d')
        WHEN LENGTH(dx.encounter_date) = 10
             AND dx.encounter_date REGEXP '^[0-9]{2}-[0-9]{2}-[0-9]{4}$'              THEN STR_TO_DATE(dx.encounter_date, '%m-%d-%Y')
        ELSE NULL
    END                                     AS encounter_date,
    CASE
        WHEN dx.dx_created_date IN ('None', '')                                         THEN NULL
        WHEN LENGTH(dx.dx_created_date) = 10
             AND dx.dx_created_date REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'             THEN STR_TO_DATE(dx.dx_created_date, '%Y-%m-%d')
        WHEN LENGTH(dx.dx_created_date) = 10
             AND dx.dx_created_date REGEXP '^[0-9]{2}-[0-9]{2}-[0-9]{4}$'             THEN STR_TO_DATE(dx.dx_created_date, '%m-%d-%Y')
        ELSE NULL
    END                                     AS diag_date,
    dx.icd_code                             AS diag_code,
    dx.icd_description                      AS diag_desc,
    dx.icd_codeset                          AS diag_coding_system,
    dx.icd_code                             AS diag_code_stripped,
    dx.dx_ordering                          AS primary_diagnosis_flag,
    NULL                                    AS parent_diag_code,
    NULL                                    AS parent_diag_desc,
    NULL                                    AS icd_codeset,
    NULL                                    AS icd_codeset_desc,
    NULL                                    AS icd_codeset_group,
    NULL                                    AS icd_codeset_system,
    dx.snomed_code                          AS snomed_code,
    dx.snomed_desc                          AS snomed_desc,
    ''                                      AS diag_severity,
    dx.dx_status                            AS diag_status,
    NULL                                    AS diag_end_date,
    NULL                                    AS provisional_diag_flag,
    NULL                                    AS differential_diag_flag,
    dx.dx_note               AS comments_notes,
    NULL                                    AS diag_category,
    NULL                                    AS diag_risk,
    NULL                                    AS work_up_status,
    CURRENT_TIMESTAMP()                     AS created_datetime,
    'ND'                                    AS created_by,
    CURRENT_TIMESTAMP()                     AS updated_datetime,
    'ND'                                    AS updated_by,
    'athenaone'                            AS ehr_source_name,
    'bronze_layer'                          AS source_path,
    'Structured'                            AS data_type,
    0010                                    AS psid,
    nd_extracted_date as nd_extracted_date
FROM (
    -- ── 1. CLINICAL_ENCOUNTER_DX_DIRECT ──────────────────────────────────────
    SELECT
        'CLINICAL_ENCOUNTER_DX_DIRECT'  AS dx_source,
        p.ENTERPRISEID                     AS patient_id,
        ce.CLINICALENCOUNTERID          AS clinical_encounter_id,
        ce.ENCOUNTERDATE                AS encounter_date,
        ced.CLINICALENCOUNTERDXID       AS source_dx_id,
        ced.DIAGNOSISCODE               AS icd_code,
        'DIRECT'                        AS icd_codeset,
        null     AS icd_description,
        ced.ORDERING                    AS dx_ordering,
        ced.STATUS                      AS dx_status,
        ced.NOTE                        AS dx_note,
        CAST(ced.SNOMEDCODE AS CHAR)    AS snomed_code,
        CAST(dc.DESCRIPTION as CHAR)   AS  snomed_desc,
        ced.CREATEDDATETIME             AS dx_created_date,
        ced.nd_extracted_date as nd_extracted_date
    FROM CLINICALENCOUNTERDIAGNOSIS ced
    JOIN CLINICALENCOUNTER          ce  ON ced.CLINICALENCOUNTERID = ce.CLINICALENCOUNTERID
                                       AND ced.CONTEXTID           = ce.CONTEXTID and ce.nd_active_flag = 'Y'
    JOIN PATIENT                    p   ON ce.CHARTID            = p.ENTERPRISEID
                                       AND ce.CONTEXTID            = p.CONTEXTID AND p.nd_active_flag = 'Y'
    LEFT JOIN SNOMED         dc  ON ced.SNOMEDCODE       = dc.SNOMEDCODE
                                       AND ced.CONTEXTID           = dc.CONTEXTID and dc.nd_active_flag = 'Y'
    WHERE ced.nd_active_flag = 'Y';

    UNION ALL

    -- ── 2. CLINICAL_ENCOUNTER_DX_ICD10 ───────────────────────────────────────
    SELECT
        'CLINICAL_ENCOUNTER_DX_ICD10',
        p.ENTERPRISEID,
        ce.CLINICALENCOUNTERID, ce.ENCOUNTERDATE,
        dxicd10.CLINICALENCOUNTERDXICD10ID,
        icd.DIAGNOSISCODE, 'ICD10',
        icd.DIAGNOSISCODEDESCRIPTION,
        dxicd10.ORDERING, ced.STATUS, ced.NOTE,
        CAST(ced.SNOMEDCODE AS CHAR),
        CAST(dc.DESCRIPTION as CHAR),
        dxicd10.CREATEDDATETIME,
        ced.nd_extracted_date as nd_extracted_date
    FROM CLINICALENCOUNTERDIAGNOSIS ced
    JOIN CLINICALENCOUNTERDXICD10   dxicd10 ON dxicd10.CLINICALENCOUNTERDXID = ced.CLINICALENCOUNTERDXID
                                            AND dxicd10.CONTEXTID              = ced.CONTEXTID
                                            AND dxicd10.DELETEDDATETIME       IS NULL and dxicd10.nd_active_flag = 'Y'
    JOIN CLINICALENCOUNTER          ce      ON ced.CLINICALENCOUNTERID         = ce.CLINICALENCOUNTERID
                                            AND ced.CONTEXTID                  = ce.CONTEXTID AND ce.nd_active_flag = 'Y'
    JOIN PATIENT                    p       ON ce.CHARTID                    = p.ENTERPRISEID 
                                            AND ce.CONTEXTID                   = p.CONTEXTID and p.nd_active_flag = 'Y'
    LEFT JOIN ICDCODEALL            icd     ON dxicd10.ICDCODEID               = icd.ICDCODEID
                                            AND dxicd10.CONTEXTID              = icd.CONTEXTID and icd.nd_active_flag = 'Y'
     LEFT JOIN SNOMED         dc  ON ced.SNOMEDCODE       = dc.SNOMEDCODE
                                       AND ced.CONTEXTID           = dc.CONTEXTID and dc.nd_active_flag = 'Y'
    WHERE ced.DELETEDDATETIME IS NULL and ced.nd_active_flag = 'Y'

    UNION ALL

    -- ── 3. CLINICAL_ENCOUNTER_DX_ICD9 ────────────────────────────────────────
    SELECT
        'CLINICAL_ENCOUNTER_DX_ICD10',
        p.ENTERPRISEID,
        ce.CLINICALENCOUNTERID, ce.ENCOUNTERDATE,
        dxicd10.CLINICALENCOUNTERDXICD10ID,
        icd.DIAGNOSISCODE, 'ICD10',
        icd.DIAGNOSISCODEDESCRIPTION,
        dxicd10.ORDERING, ced.STATUS, ced.NOTE,
        CAST(ced.SNOMEDCODE AS CHAR),
        CAST(dc.DESCRIPTION as CHAR),
        dxicd10.CREATEDDATETIME,
        ced.nd_extracted_date as nd_extracted_date
    FROM CLINICALENCOUNTERDIAGNOSIS ced
    JOIN CLINICALENCOUNTERDXICD10   dxicd10 ON dxicd10.CLINICALENCOUNTERDXID = ced.CLINICALENCOUNTERDXID
                                            AND dxicd10.CONTEXTID              = ced.CONTEXTID
                                            AND dxicd10.DELETEDDATETIME       IS NULL and dxicd10.nd_active_flag = 'Y'
    JOIN CLINICALENCOUNTER          ce      ON ced.CLINICALENCOUNTERID         = ce.CLINICALENCOUNTERID
                                            AND ced.CONTEXTID                  = ce.CONTEXTID AND ce.nd_active_flag = 'Y'
    JOIN PATIENT                    p       ON ce.CHARTID                    = p.ENTERPRISEID 
                                            AND ce.CONTEXTID                   = p.CONTEXTID and p.nd_active_flag = 'Y'
    LEFT JOIN ICDCODEALL            icd     ON dxicd10.ICDCODEID               = icd.ICDCODEID
                                            AND dxicd10.CONTEXTID              = icd.CONTEXTID and icd.nd_active_flag = 'Y'
     LEFT JOIN SNOMED         dc  ON ced.SNOMEDCODE       = dc.SNOMEDCODE
                                       AND ced.CONTEXTID           = dc.CONTEXTID and dc.nd_active_flag = 'Y'
    WHERE ced.DELETEDDATETIME IS NULL and ced.nd_active_flag = 'Y'

    UNION ALL

    -- ── 4. DOCUMENT_DX_ICD10 ─────────────────────────────────────────────────
    SELECT
        'DOCUMENT_DX_ICD10',
        p.ENTERPRISEID,
        doc.CLINICALENCOUNTERID, ce.ENCOUNTERDATE,
        ddi10.DOCUMENTDIAGNOSISICD10ID,
        icd.DIAGNOSISCODE, 'ICD10',
        icd.DIAGNOSISCODEDESCRIPTION,
        ddi10.ORDERING, NULL, NULL,
        CAST(dd.SNOMEDCODE AS CHAR),
        CAST(dc1.DESCRIPTION as CHAR),
        ddi10.CREATEDDATETIME
        ddi10.nd_extracted_date as nd_extracted_date
    FROM DOCUMENTDIAGNOSISICD10 ddi10
    JOIN DOCUMENTDIAGNOSIS      dd   ON ddi10.DOCUMENTDIAGNOSISID = dd.DOCUMENTDIAGNOSISID
                                     AND ddi10.CONTEXTID           = dd.CONTEXTID
                                     AND dd.DELETEDDATETIME       IS NULL and dd.nd_active_flag = 'Y'
    JOIN DOCUMENT               doc  ON dd.DOCUMENTID             = doc.DOCUMENTID
                                     AND dd.CONTEXTID              = doc.CONTEXTID and doc.nd_active_flag = 'Y'
    JOIN PATIENT                p    ON doc.CHARTID             = p.ENTERPRISEID
                                     AND doc.CONTEXTID             = p.CONTEXTID and p.nd_active_flag = 'Y'
    LEFT JOIN CLINICALENCOUNTER ce   ON doc.CLINICALENCOUNTERID   = ce.CLINICALENCOUNTERID
                                     AND doc.CONTEXTID             = ce.CONTEXTID and ce.nd_active_flag = 'Y'
    LEFT JOIN ICDCODEALL        icd  ON ddi10.ICDCODEID           = icd.ICDCODEID
                                     AND ddi10.CONTEXTID           = icd.CONTEXTID and icd.nd_active_flag = 'Y'                               
     LEFT JOIN SNOMED         dc1  ON dd.SNOMEDCODE       = dc1.SNOMEDCODE
                                       AND dd.CONTEXTID           = dc1.CONTEXTID and dc1.nd_active_flag = 'Y'
    WHERE ddi10.DELETEDDATETIME IS NULL AND ddi10.nd_active_flag = 'Y'

    UNION ALL

    -- ── 5. DOCUMENT_DX_ICD9 ──────────────────────────────────────────────────
    SELECT
        'DOCUMENT_DX_ICD9',
        p.ENTERPRISEID,
        doc.CLINICALENCOUNTERID, ce.ENCOUNTERDATE,
        ddi9.DOCUMENTDIAGNOSISICD9ID,
        ddi9.DIAGNOSISCODE, 'ICD9',
        dc.DIAGNOSISCODEDESCRIPTION,
        ddi9.ORDERING, NULL, NULL,
        CAST(dd.SNOMEDCODE AS CHAR),
        CAST(dc1.DESCRIPTION as CHAR),
        ddi9.CREATEDDATETIME,
        ddi9.nd_extracted_date as nd_extracted_date
    FROM DOCUMENTDIAGNOSISICD9  ddi9
    JOIN DOCUMENTDIAGNOSIS      dd   ON ddi9.DOCUMENTDIAGNOSISID = dd.DOCUMENTDIAGNOSISID
                                     AND ddi9.CONTEXTID           = dd.CONTEXTID
                                     AND dd.DELETEDDATETIME      IS NULL and dd.nd_active_flag = 'Y'
    JOIN DOCUMENT               doc  ON dd.DOCUMENTID            = doc.DOCUMENTID
                                     AND dd.CONTEXTID             = doc.CONTEXTID and doc.nd_active_flag = 'Y'
    JOIN PATIENT                p    ON doc.CHARTID            = p.ENTERPRISEID
                                     AND doc.CONTEXTID            = p.CONTEXTID and p.nd_active_flag = 'Y'
    LEFT JOIN CLINICALENCOUNTER ce   ON doc.CLINICALENCOUNTERID  = ce.CLINICALENCOUNTERID
                                     AND doc.CONTEXTID            = ce.CONTEXTID and ce.nd_active_flag = 'Y'
    LEFT JOIN DIAGNOSISCODE     dc   ON ddi9.DIAGNOSISCODE       = dc.DIAGNOSISCODE
                                     AND ddi9.CONTEXTID           = dc.CONTEXTID and dc.nd_active_flag = 'Y'
    LEFT JOIN SNOMED         dc1  ON dd.SNOMEDCODE       = dc1.SNOMEDCODE
                                       AND dd.CONTEXTID           = dc1.CONTEXTID and dc1.nd_active_flag = 'Y'
    WHERE ddi9.DELETEDDATETIME IS NULL and ddi9.nd_active_flag = 'Y'

    UNION ALL

    -- ── 6. CLINICAL_SERVICE_DX ───────────────────────────────────────────────
    SELECT
        'CLINICAL_SERVICE_DX',
        p.ENTERPRISEID,
        ce.CLINICALENCOUNTERID, ce.ENCOUNTERDATE,
        csd.CLINICALSERVICEDIAGNOSISID,
        csd.DIAGNOSISCODE, csd.DIAGNOSISCODESET,
        dc.DIAGNOSISCODEDESCRIPTION,
        csd.ORDERING, NULL, NULL,
        NULL,NULL,
        csd.CREATEDDATETIME,
        cs.nd_extracted_date as nd_extracted_date
    FROM CLINICALSERVICE              cs
    JOIN CLINICALENCOUNTER            ce   ON cs.CLINICALENCOUNTERID         = ce.CLINICALENCOUNTERID
                                          AND cs.CONTEXTID                   = ce.CONTEXTID and ce.nd_active_flag = 'Y'
    JOIN PATIENT                      p    ON ce.CHARTID                   = p.ENTERPRISEID
                                          AND ce.CONTEXTID                   = p.CONTEXTID AND p.nd_active_flag = 'Y'
    JOIN CLINICALSERVICEPROCEDURECODE cspc ON cspc.CLINICALSERVICEID         = cs.CLINICALSERVICEID
                                          AND cspc.CONTEXTID                 = cs.CONTEXTID AND cspc.nd_active_flag = 'Y'
    JOIN CLINICALSERVICEDIAGNOSIS     csd  ON csd.CLINICALSERVICEPROCCODEID  = cspc.CLINICALSERVICEPROCCODEID
                                          AND csd.CONTEXTID                  = cs.CONTEXTID
                                          AND csd.DELETEDDATETIME           IS NULL AND csd.nd_active_flag = 'Y'
    LEFT JOIN ICDCODEALL           dc   ON trim(csd.DIAGNOSISCODE)              = trim(dc.DIAGNOSISCODE)
                                          AND csd.CONTEXTID                  = dc.CONTEXTID AND dc.nd_active_flag = 'Y'
    WHERE cs.DELETEDDATETIME IS NULL and cs.nd_active_flag = 'Y'

    UNION ALL

    -- ── 7. REFERRAL_AUTH_DX ──────────────────────────────────────────────────
    SELECT
        'REFERRAL_AUTH_DX',
        p.ENTERPRISEID,
        NULL, NULL,
        rad.REFERRALAUTHDIAGNOSISID,
        rad.DIAGNOSISCODE, rad.DIAGNOSISCODESETNAME,
        icd.DIAGNOSISCODEDESCRIPTION,
        rad.ORDERING, NULL, NULL,
        NULL,NULL,
        rad.CREATEDDATETIME,
        rad.nd_extracted_date as nd_extracted_date
    FROM REFERRALAUTHDIAGNOSISCODE  rad
    JOIN REFERRALAUTHORIZATION      ra  ON rad.REFERRALAUTHID   = ra.REFERRALAUTHID
                                       AND rad.CONTEXTID         = ra.CONTEXTID and ra.nd_active_flag = 'Y'
    JOIN PATIENTINSURANCE           pi  ON ra.PATIENTINSURANCEID = pi.PATIENTINSURANCEID
                                       AND ra.CONTEXTID          = pi.CONTEXTID and pi.nd_active_flag = 'Y'
    JOIN PATIENT                    p   ON pi.PATIENTID          = p.ENTERPRISEID
                                       AND pi.CONTEXTID          = p.CONTEXTID AND p.nd_active_flag = 'Y'
    LEFT JOIN ICDCODEALL            icd ON rad.ICDCODEID         = icd.ICDCODEID
                                       AND rad.CONTEXTID         = icd.CONTEXTID AND icd.nd_active_flag = 'Y'
    WHERE rad.DELETEDDATETIME IS NULL
      AND ra.DELETEDDATETIME  IS NULL AND rad.nd_active_flag = 'Y'
) dx
ORDER BY
    dx.patient_id,
    dx.encounter_date DESC,
    dx.dx_source,
    dx.dx_ordering
)t;