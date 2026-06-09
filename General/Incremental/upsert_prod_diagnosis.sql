insert into rgd_udm_staging.diagnosis
(
    diag_id,
    ndid,
    eid,
    enc_date,
    encounter_end_date,
    diag_date,
    diag_code,
    diag_desc,
    diag_coding_system,
    diag_code_stripped,
    primary_diagnosis_flag,
    parent_diagnosis_code,
    parent_diagnosis_desc,
    icd_codeset,
    icd_codeset_desc,
    icd_codeset_group,
    icd_codeset_system,
    snomed_code,
    diag_severity,
    diag_status,
    diag_end_date,
    provisional_diag_flag,
    differential_diag_flag,
    comments_notes,
    diag_risk,
    specify,
    nd_extracted_date,
    created_datetime,
    created_by,
    ehr_source_name,
    source_path,
    data_type,
    psid,
    enc_date_proxy,
    icd10_desc_std,
    icd9_desc_std,
    diag_coding_system_std,
    primary_diagnosis_flag_std,
    udm_active_flag,
    udm_unq_id,
    updated_datetime,
    updated_by
)

SELECT
    diag_id,
    ndid,
    eid,
    enc_date,
    encounter_end_date,
    diag_date,
    diag_code,
    diag_desc,
    diag_coding_system,
    diag_code_stripped,
    primary_diagnosis_flag,
    parent_diagnosis_code,
    parent_diagnosis_desc,
    icd_codeset,
    icd_codeset_desc,
    icd_codeset_group,
    icd_codeset_system,
    snomed_code,
    diag_severity,
    diag_status,
    diag_end_date,
    provisional_diag_flag,
    differential_diag_flag,
    comments_notes,
    diag_risk,
    specify,
    nd_extracted_date,
    created_datetime,
    created_by,
    ehr_source_name,
    source_path,
    data_type,
    psid,
    enc_date_proxy,

    -- New columns in prod (set default/null or derive later)
    NULL AS icd10_desc_std,
    NULL AS icd9_desc_std,
    NULL AS diag_coding_system_std,
    NULL AS primary_diagnosis_flag_std,

    'Y' AS udm_active_flag,
    udm_unq_id,
    updated_datetime,
    updated_by

from udm_staging.diagnosis a

INNER JOIN (
    -- pick latest record per udm_unq_id
    SELECT
        udm_unq_id,
        nd_extracted_date,
        ROW_NUMBER() OVER (
            PARTITION BY udm_unq_id
            ORDER BY nd_extracted_date DESC, created_datetime DESC
        ) AS rn
    FROM udm_staging.diagnosis
    WHERE psid = 9
) ranked
    ON  a.udm_unq_id        = ranked.udm_unq_id
    AND a.nd_extracted_date = ranked.nd_extracted_date
    AND ranked.rn = 1

WHERE a.psid = 9;