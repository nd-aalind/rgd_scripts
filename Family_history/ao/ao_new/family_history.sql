/* ==========================================================
   FAMILY HISTORY - ATHENAONE
   ========================================================== */

SELECT
    pf.familyhistoryid AS family_hist_id,
    pf.chartid AS ndid,
    NULL AS eid,
    NULL AS enc_date,

    CASE
        WHEN CREATEDDATETIME IN ('None','') THEN NULL
        WHEN LEFT(CREATEDDATETIME,10) REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
            THEN STR_TO_DATE(LEFT(CREATEDDATETIME,10),'%Y-%m-%d')
        WHEN LEFT(CREATEDDATETIME,10) REGEXP '^[0-9]{2}-[0-9]{2}-[0-9]{4}$'
            THEN STR_TO_DATE(LEFT(CREATEDDATETIME,10),'%m-%d-%Y')
        ELSE NULL
    END AS onset_date,

    pf.onsetage AS onset_age,

    CASE
        WHEN CREATEDDATETIME IN ('None','') THEN NULL
        WHEN LEFT(CREATEDDATETIME,10) REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
            THEN STR_TO_DATE(LEFT(CREATEDDATETIME,10),'%Y-%m-%d')
        WHEN LEFT(CREATEDDATETIME,10) REGEXP '^[0-9]{2}-[0-9]{2}-[0-9]{4}$'
            THEN STR_TO_DATE(LEFT(CREATEDDATETIME,10),'%m-%d-%Y')
        ELSE NULL
    END AS family_hist_date,

    'Family History' AS hist_category,
    pf.relation AS fam_hist_relation,

    CASE
        WHEN TRIM(UPPER(relation))='MOTHER' THEN '32'
        WHEN TRIM(UPPER(relation))='FATHER' THEN '33'
        WHEN TRIM(UPPER(relation))='UNSPECIFIED' THEN '21'
        WHEN TRIM(UPPER(relation)) IN
            ('MATERNALAUNT','SISTER','BROTHER',
             'MATERNALUNCLE','PATERNALAUNT','PATERNALUNCLE')
            THEN 'G8'
        WHEN TRIM(UPPER(relation)) IN ('SON','DAUGHTER')
            THEN '19'
        WHEN TRIM(UPPER(relation)) IN
            ('PATERNALGRANDFATHER','PATERNALGRANDMOTHER',
             'MATERNALGRANDMOTHER','MATERNALGRANDFATHER')
            THEN '4'
        WHEN TRIM(UPPER(relation)) IN ('NONCONTRIBUTORY')
            THEN '21'
    END AS family_relationship_code,

    pf.familyhistoryproblem AS family_hist_details,
    pf.snomedcode AS family_hist_code,
    'SNOMED' AS family_hist_coding_system,

    NULL AS family_hist_notes,
    NULL AS family_hist_value,
    NULL AS itemname,
    NULL AS itemdesc,

    CURRENT_TIMESTAMP AS created_datetime,
    'ND' AS created_by,
    CURRENT_TIMESTAMP AS updated_datetime,
    'ND' AS updated_by,
    'AthenaOne' AS ehr_source_name,
    'bronze_layer' AS source_path,
    'Structured' AS data_type,
    10 AS psid,
    pf.nd_extracted_date
FROM patientfamilyhistory pf
WHERE pf.nd_active_flag = 'Y';