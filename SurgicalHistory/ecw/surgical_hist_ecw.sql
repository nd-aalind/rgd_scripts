SELECT
    NULL AS surgicalhistoryid,
    enc.patientID AS ndid,
    enc.encounterID AS eid,
    enc.date AS enc_date,

    CASE
        WHEN sh.date REGEXP '^[0-9]{4}$'
            THEN STR_TO_DATE(CONCAT(sh.date,'-01-01'),'%Y-%m-%d')

        WHEN sh.date REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
            THEN STR_TO_DATE(sh.date,'%Y-%m-%d')

        WHEN sh.date REGEXP '^[0-9]{4}/[0-9]{2}$'
            THEN STR_TO_DATE(CONCAT(sh.date,'/01'),'%Y/%m/%d')

        WHEN sh.date REGEXP '^[0-9]{2}/[0-9]{4}$'
            THEN STR_TO_DATE(CONCAT('01/',sh.date),'%d/%m/%Y')

        WHEN sh.date REGEXP '^[A-Za-z]+ [0-9]{4}$'
            THEN STR_TO_DATE(CONCAT('01 ',sh.date),'%d %M %Y')

        WHEN sh.date REGEXP '^[A-Za-z]+,[0-9]{1,2},[0-9]{4}$'
            THEN STR_TO_DATE(REPLACE(sh.date,',',' '),'%b %d %Y')

        ELSE NULL
    END AS surgery_date,

    'PATIENTSURGICALHISTORY' AS surg_hist_type,

    CASE
        WHEN sh.reason IN ('','null','none')
            THEN enc.reason
        ELSE sh.reason
    END AS surgery_name,

    CASE
        WHEN sh.cptcode IN ('','null')
            THEN NULL
        ELSE sh.cptcode
    END AS surgery_code,

    CASE
        WHEN sh.cptcode IN ('','null')
            THEN NULL
        ELSE 'CPT'
    END AS surgery_coding_system,

    sh.reason AS surgery_reason,

    CURRENT_TIMESTAMP() AS created_datetime,
    'ND' AS created_by,
    CURRENT_TIMESTAMP() AS updated_datetime,
    'ND' AS updated_by,

    'eCW' AS ehr_source_name,
    'bronze_layer' AS source_path,
    'Structured' AS data_type,
    3 AS psid,

    sh.nd_extracted_date

FROM surgicalhistory sh
INNER JOIN encounterdata ed
    ON sh.encounterID = ed.encounterID
   AND sh.nd_ActiveFlag = 'Y'
   AND ed.nd_ActiveFlag = 'Y'

INNER JOIN enc
    ON enc.encounterID = ed.encounterID
   AND enc.nd_ActiveFlag = 'Y';