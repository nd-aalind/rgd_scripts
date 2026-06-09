 
    SELECT
    OBS.OBSID AS social_hist_id,
    OBS.PID AS ndid,
    NULL AS eid,
    NULL AS encounter_date,
    OBS.OBSDATE AS social_hist_date,
    'Social History' AS hist_category,
    OBSHEAD.NAME AS social_hist_category,
    OBSHEAD.DESCRIPTION AS social_hist_question,
    OBS.OBSVALUE AS social_hist_value,
    COALESCE(
        OBSHEAD.SNOMEDCODE,
        OBSHEAD.LOINCCODE,
        OBSHEAD.ICDCODE,
        OBSHEAD.CPTCODE,
        OBSHEAD.OTHERCODE, OBSHEAD.MLCODE) AS social_hist_code,
    CASE
        WHEN OBSHEAD.SNOMEDCODE IS NOT NULL THEN 'SNOMED'
        WHEN OBSHEAD.LOINCCODE IS NOT NULL THEN 'LOINC'
        WHEN OBSHEAD.ICDCODE IS NOT NULL THEN 'ICD'
        WHEN OBSHEAD.CPTCODE IS NOT NULL THEN 'CPT'
        WHEN OBSHEAD.OTHERCODE IS NOT NULL THEN 'CVX'
        ELSE NULL
    END AS social_hist_coding_system,
    OBS.DESCRIPTION social_hist_notes,
    current_date() as created_datetime,
    'ND' as created_by,
     current_date() as updated_datetime,
     'ND' as updated_by,
    'Athenaone' as ehr_source_name,
    'bronze_layer' as source_path,
    'Structured' as data_type,
    '' as psid,
    OBS.nd_extracte_date as nd_extracte_date
FROM OBS
LEFT JOIN OBSHEAD
    ON OBS.HDID = OBSHEAD.HDID and OBS.nd_Activeflag = 'Y' and OBSHEAD.nd_Activeflag = 'Y'
LEFT JOIN HIERGRPS
    ON OBSHEAD.GROUPID = HIERGRPS.GROUPID and HIERGRPS.nd_Activeflag = 'Y'
WHERE HIERGRPS.GROUPNAME IN (
    'SH',
    'Lifestyle/habits',
    'tobacco use',
    'Counseling'
);