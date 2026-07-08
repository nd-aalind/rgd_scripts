/* ==========================================================
   FAMILY HISTORY - ECW
   ========================================================== */

SELECT
    NULL AS family_hist_id,
    enc.patientID AS ndid,
    fhx.encounterid AS eid,
    enc.date AS enc_date,

    NULL AS onset_date,

    fhx.diagnosedAge AS onset_age,

    enc.date AS family_hist_date,

    'Family History' AS hist_category,
    fhx.name AS fam_hist_relation,

    CASE
        WHEN TRIM(UPPER(fhx.name))='MOTHER' THEN '32'
        WHEN TRIM(UPPER(fhx.name))='FATHER' THEN '33'
        WHEN TRIM(UPPER(fhx.name))='UNSPECIFIED' THEN '21'
        WHEN TRIM(UPPER(fhx.name)) IN
            ('MATERNALAUNT','SISTER','BROTHER',
             'MATERNALUNCLE','PATERNALAUNT','PATERNALUNCLE',
             'SIBLINGS','PATERNAL AUNT','MATERNAL AUNT')
            THEN 'G8'
        WHEN TRIM(UPPER(fhx.name)) IN ('SON','DAUGHTER')
            THEN '19'
        WHEN TRIM(UPPER(fhx.name)) IN
            ('PATERNALGRANDFATHER','PATERNALGRANDMOTHER',
             'GRANDPARENTS','MATERNALGRANDMOTHER',
             'MATERNALGRANDFATHER',
             'MATERNAL GRAND MOTHER',
             'PATERNAL GRAND MOTHER',
             'PATERNAL GRAND FATHER',
             'MATERNAL GRAND FATHER')
            THEN '4'
        WHEN TRIM(UPPER(fhx.name)) IN ('NONCONTRIBUTORY')
            THEN '21'
    END AS family_relationship_code,

    NULL AS family_hist_details,

    COALESCE(fhx.icdCode, fhx.snomedCode) AS family_hist_code,
    CASE
        WHEN fhx.icdCode IS NOT NULL THEN 'ICD'
        WHEN fhx.snomedCode IS NOT NULL THEN 'SNOMED'
        ELSE NULL
    END AS family_hist_coding_system,
    fhx.diagnosedYear AS family_hist_notes,
    fhx.icdDesc AS family_hist_value,
    it.itemname,
    it.itemdesc,
    CURRENT_TIMESTAMP AS created_datetime,
    'ND' AS created_by,
    CURRENT_TIMESTAMP AS updated_datetime,
    'ND' AS updated_by,
    'eCW' AS ehr_source_name,
    'bronze_layer' AS source_path,
    'Structured' AS data_type,
    8 AS psid,
    fhx.nd_extracted_date
FROM familyhxdetails fhx
LEFT JOIN enc
    ON fhx.encounterid = enc.encounterid
   AND fhx.nd_ActiveFlag = 'Y'
   AND enc.nd_ActiveFlag = 'Y'
LEFT JOIN items it
    ON it.itemID = fhx.itemid
   AND it.nd_ActiveFlag = 'Y';