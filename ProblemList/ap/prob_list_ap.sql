SELECT
    p.SPRID AS diag_id,
    p.PID AS ndid,
    NULL AS eid,
    NULL AS encounter_date,
    NULL AS problem_date,
    p.ONSETDATE AS problem_onset_date,
    p.STOPDATE AS problem_end_date,
    NULL AS resolved,
    COALESCE(p.ICD10MasterDiagnosisId,p.ICD9MasterDiagnosisId,p.SNOMEDMasterDiagnosisId) AS problem_id,
    COALESCE(md.Code,md9.Code,mdsnomed.Code,
    CASE WHEN p.CODE LIKE '%-%' THEN SUBSTRING_INDEX(p.CODE, '-', -1)ELSE p.CODE END) AS problem_code,
    COALESCE(md.LongDescription,md9.LongDescription,mdsnomed.LongDescription,md.ShortDescription,md9.ShortDescription,mdsnomed.ShortDescription,p.DESCRIPTION) AS problem_description,
    CASE
        WHEN p.ICD10MasterDiagnosisId IS NOT NULL THEN 'ICD-10'
        WHEN p.ICD9MasterDiagnosisId IS NOT NULL THEN 'ICD-9'
        WHEN p.SNOMEDMasterDiagnosisId IS NOT NULL THEN 'SNOMED'
        WHEN TRIM(UPPER(p.CODE)) LIKE 'CPT-%' THEN 'CPT'
        WHEN TRIM(UPPER(p.CODE)) LIKE 'SNO%' THEN 'SNOMED'
        WHEN TRIM(UPPER(p.CODE)) LIKE 'ICD9-%' THEN 'ICD-9'
        WHEN TRIM(UPPER(p.CODE)) LIKE 'ICD10-%' THEN 'ICD-10'
        WHEN TRIM(UPPER(p.CODE)) LIKE 'ICD-%' THEN 'ICD'
        WHEN p.CODE REGEXP '^[0-9]{6,18}$' THEN 'SNOMED'
        WHEN p.CODE REGEXP '^[0-9]{5}$' THEN 'CPT'
        WHEN p.CODE REGEXP '^[A-Z][0-9]{4}$' THEN 'HCPCS'
        WHEN p.CODE REGEXP '^[A-Z][0-9A-Z\\.]{2,}$' THEN 'ICD-10'
        WHEN p.CODE REGEXP '^[0-9]{3}(\\.[0-9]{1,2})?$' THEN 'ICD-9'
        ELSE null
    END AS coding_system,
    p.QUALIFIER AS problem_type,
    NULL AS status,
    NULL AS severity,
    NULL AS laterality,
    NULL AS problem_notes,
    current_date() as created_datetime,
    'ND' as created_by,
     current_date() as updated_datetime,
     'ND' as updated_by,
    'Athenaone' as ehr_source_name,
    'bronze_layer' as source_path,
    'Structured' as data_type,
    '7' as psid,
    DATE(p.nd_extracted_date) AS nd_extracted_date
FROM PROBLEM p
LEFT JOIN MasterDiagnosis md 
    ON md.MasterDiagnosisID = p.ICD10MasterDiagnosisId
LEFT JOIN MasterDiagnosis md9 
    ON md9.MasterDiagnosisID = p.ICD9MasterDiagnosisId
LEFT JOIN MasterDiagnosis mdsnomed
    ON mdsnomed.MasterDiagnosisID = p.SNOMEDMasterDiagnosisId;