SELECT
    p.Pregid AS obgyn_hist_id,
    p.Patientid AS ndid,
    NULL AS eid,
    NULL AS encounter_date,
    NULL AS obgyn_episode_date,
    p.Upd_Date AS obgyn_hist_date,
    'OB/GYN History' AS obgyn_hist_category,
    'Pregnancy' AS obgyn_hist_subcategory,
    'Pregnancy status' AS obgyn_hist_question,
    p.OBFStat AS obgyn_hist_value,
    p.AsmtValue AS obgyn_hist_code,
    p.Pregid AS obgyn_hist_coding_system,
    CONCAT_WS(' | ', p.notes, p.sticky_notes) AS obgyn_hist_notes,
    'eCW 1' AS data_source,
    CURRENT_TIMESTAMP()                         AS created_datetime,
    'ND'                                        AS created_by,
    'ecw'                                 AS ehr_source_name,
    'bronze_layer'                              AS source_path,
    'Structured'                                AS data_type,
    6                                           AS psid,
    nd_extracted_date                        AS nd_extracted_date
FROM obf_pregnancy p
WHERE p.OBFStat IS NOT NULL
UNION ALL
SELECT
    p.Pregid,
    p.Patientid,
    NULL,
    NULL,
    NULL,
    p.Upd_Date,
    'OB/GYN History',
    'Pregnancy',
    'Number of Babies',
    p.NumberOfBabies,
    p.AsmtValue,
    p.Pregid,
    CONCAT_WS(' | ', p.notes, p.sticky_notes),
    'eCW 1',
    CURRENT_TIMESTAMP()                         AS created_datetime,
    'ND'                                        AS created_by,
    'ecw'                                 AS ehr_source_name,
    'bronze_layer'                              AS source_path,
    'Structured'                                AS data_type,
    6                                           AS psid,
    nd_extracted_date                        AS nd_extracted_date
FROM obf_pregnancy p
WHERE p.NumberOfBabies IS NOT NULL
UNION ALL
SELECT
    p.Pregid,
    p.Patientid,
    NULL,
    NULL,
    NULL,
    p.Upd_Date,
    'OB/GYN History',
    'Pregnancy',
    'Discharge Date',
    p.Discharge_date,
    p.AsmtValue,
    p.Pregid,
    CONCAT_WS(' | ', p.notes, p.sticky_notes),
    'eCW 1',
    CURRENT_TIMESTAMP()                         AS created_datetime,
    'ND'                                        AS created_by,
    'ecw'                                 AS ehr_source_name,
    'bronze_layer'                              AS source_path,
    'Structured'                                AS data_type,
    6                                           AS psid,
    nd_extracted_date                        AS nd_extracted_date
FROM obf_pregnancy p
WHERE p.Discharge_date IS NOT NULL;