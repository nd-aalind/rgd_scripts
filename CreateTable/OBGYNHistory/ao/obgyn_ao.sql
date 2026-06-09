SELECT
    g.PATIENTGPALHISTORYID AS obgyn_hist_id,
    g.CHARTID AS ndid,
    NULL AS eid,
    NULL AS encounter_date,
    NULL AS obgyn_episode_date,
    CREATEDDATETIME AS obgyn_hist_date,
    'OB/GYN History' AS obgyn_hist_category,
    'Pregnancy' AS obgyn_hist_subcategory,
    'Total Pregnancies' AS obgyn_hist_question,
    g.TOTAL AS obgyn_hist_value,
    NULL AS obgyn_hist_code,
    NULL AS obgyn_hist_coding_system,
    NULL AS obgyn_hist_notes,
    'AthenaOne' AS data_source,
    CURRENT_TIMESTAMP()                         AS created_datetime,
    'ND'                                        AS created_by,
    'ecw'                                 AS ehr_source_name,
    'bronze_layer'                              AS source_path,
    'Structured'                                AS data_type,
    6                                           AS psid,
    nd_extracted_date                        AS nd_extracted_date
FROM PATIENTGPALHISTORY g
WHERE g.TOTAL IS NOT NULL AND nd_active_flag = 'Y'
UNION ALL
SELECT
    g.PATIENTGPALHISTORYID,
    g.CHARTID,
    NULL,
    NULL,
    NULL,
    NULL,
    'OB/GYN History',
    'Pregnancy',
    'Full Term Births',
    g.FULLTERM,
    NULL,
    NULL,
    NULL,
    'AthenaOne',
    CURRENT_TIMESTAMP()                         AS created_datetime,
    'ND'                                        AS created_by,
    'ecw'                                 AS ehr_source_name,
    'bronze_layer'                              AS source_path,
    'Structured'                                AS data_type,
    6                                           AS psid,
    nd_extracted_date                        AS nd_extracted_date
FROM PATIENTGPALHISTORY g
WHERE g.FULLTERM IS NOT NULL AND nd_active_flag = 'Y'
UNION ALL
SELECT
    g.PATIENTGPALHISTORYID,
    g.CHARTID,
    NULL,
    NULL,
    NULL,
    NULL,
    'OB/GYN History',
    'Pregnancy',
    'Premature Births',
    g.PREMATURE,
    NULL,
    NULL,
    NULL,
    'AthenaOne',
    CURRENT_TIMESTAMP()                         AS created_datetime,
    'ND'                                        AS created_by,
    'ecw'                                 AS ehr_source_name,
    'bronze_layer'                              AS source_path,
    'Structured'                                AS data_type,
    6                                           AS psid,
    nd_extracted_date                        AS nd_extracted_date
FROM PATIENTGPALHISTORY g
WHERE g.PREMATURE IS NOT NULL AND nd_active_flag = 'Y'
UNION ALL
SELECT
    g.PATIENTGPALHISTORYID,
    g.CHARTID,
    NULL,
    NULL,
    NULL,
    NULL,
    'OB/GYN History',
    'Pregnancy',
    'Ectopic Pregnancies',
    g.ECTOPICS,
    NULL,
    NULL,
    NULL,
    'AthenaOne',
    CURRENT_TIMESTAMP()                         AS created_datetime,
    'ND'                                        AS created_by,
    'ecw'                                 AS ehr_source_name,
    'bronze_layer'                              AS source_path,
    'Structured'                                AS data_type,
    6                                           AS psid,
    nd_extracted_date                        AS nd_extracted_date
FROM PATIENTGPALHISTORY g
WHERE g.ECTOPICS IS NOT NULL AND nd_active_flag = 'Y'
UNION ALL
SELECT
    g.PATIENTGPALHISTORYID,
    g.CHARTID,
    NULL,
    NULL,
    NULL,
    NULL,
    'OB/GYN History',
    'Pregnancy',
    'Multiple Births',
    g.MULTIPLEBIRTHS,
    NULL,
    NULL,
    NULL,
    'AthenaOne',
    CURRENT_TIMESTAMP()                         AS created_datetime,
    'ND'                                        AS created_by,
    'ecw'                                 AS ehr_source_name,
    'bronze_layer'                              AS source_path,
    'Structured'                                AS data_type,
    6                                           AS psid,
    nd_extracted_date                        AS nd_extracted_date
FROM PATIENTGPALHISTORY g
WHERE g.MULTIPLEBIRTHS IS NOT NULL AND nd_active_flag = 'Y'
UNION ALL
SELECT
    g.PATIENTGPALHISTORYID,
    g.CHARTID,
    NULL,
    NULL,
    NULL,
    NULL,
    'OB/GYN History',
    'Pregnancy',
    'Spontaneous Abortions',
    g.SPONTANEOUSABORTION,
    NULL,
    NULL,
    NULL,
    'AthenaOne',
    CURRENT_TIMESTAMP()                         AS created_datetime,
    'ND'                                        AS created_by,
    'ecw'                                 AS ehr_source_name,
    'bronze_layer'                              AS source_path,
    'Structured'                                AS data_type,
    6                                           AS psid,
    nd_extracted_date                        AS nd_extracted_date
FROM PATIENTGPALHISTORY g
WHERE g.SPONTANEOUSABORTION IS NOT NULL AND nd_active_flag = 'Y'
UNION ALL
SELECT
    g.PATIENTGPALHISTORYID,
    g.CHARTID,
    NULL,
    NULL,
    NULL,
    NULL,
    'OB/GYN History',
    'Pregnancy',
    'Induced Abortions',
    g.INDUCEDABORTION,
    NULL,
    NULL,
    NULL,
    'AthenaOne',
    CURRENT_TIMESTAMP()                         AS created_datetime,
    'ND'                                        AS created_by,
    'ecw'                                 AS ehr_source_name,
    'bronze_layer'                              AS source_path,
    'Structured'                                AS data_type,
    6                                           AS psid,
    nd_extracted_date                        AS nd_extracted_date
FROM PATIENTGPALHISTORY g
WHERE g.INDUCEDABORTION IS NOT NULL AND nd_active_flag = 'Y';