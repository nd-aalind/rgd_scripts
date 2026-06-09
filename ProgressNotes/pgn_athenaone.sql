CREATE TABLE udm_staging.athenaone_progressnotes (
    chartid BIGINT,
    clinicalencounterid BIGINT,
    `key` VARCHAR(500),
    encounterdataclob LONGTEXT,
    psid INT,
    nd_extracted_date DATE
);

INSERT INTO udm_staging.athenaone_progressnotes
(
    chartid,
    clinicalencounterid,
    `key`,
    encounterdataclob,
    psid,
    nd_extracted_date
)
SELECT 
    b.CHARTID,
    a.CLINICALENCOUNTERID,
    CASE
        WHEN b.ENCOUNTERDATE IS NULL
             OR b.ENCOUNTERDATE IN ('', 'None')
            THEN NULL
        WHEN b.ENCOUNTERDATE REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}$'
            THEN DATE(b.ENCOUNTERDATE)
        WHEN b.ENCOUNTERDATE REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
            THEN STR_TO_DATE(b.ENCOUNTERDATE, '%Y-%m-%d')
        WHEN b.ENCOUNTERDATE REGEXP '^[0-9]{2}-[0-9]{2}-[0-9]{4}$'
            THEN STR_TO_DATE(b.ENCOUNTERDATE, '%m-%d-%Y')
        ELSE NULL
    END AS enc_date,
    a.`KEY`,
    a.ENCOUNTERDATACLOB,
    2 AS psid,
    a.nd_extracted_date
FROM tng_athena_one.CLINICALENCOUNTERDATA a
LEFT JOIN tng_athena_one.CLINICALENCOUNTER b 
    ON a.CLINICALENCOUNTERID = b.CLINICALENCOUNTERID
WHERE a.`KEY` LIKE '%FROZENSECTIONHTML_%'
  AND a.nd_active_flag = 'Y'
UNION ALL
SELECT 
    b.CHARTID,
    a.CLINICALENCOUNTERID,
    CASE
        WHEN b.ENCOUNTERDATE IS NULL
             OR b.ENCOUNTERDATE IN ('', 'None')
            THEN NULL
        WHEN b.ENCOUNTERDATE REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}$'
            THEN DATE(b.ENCOUNTERDATE)
        WHEN b.ENCOUNTERDATE REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
            THEN STR_TO_DATE(b.ENCOUNTERDATE, '%Y-%m-%d')
        WHEN b.ENCOUNTERDATE REGEXP '^[0-9]{2}-[0-9]{2}-[0-9]{4}$'
            THEN STR_TO_DATE(b.ENCOUNTERDATE, '%m-%d-%Y')
        ELSE NULL
    END AS enc_date,
    'FROZENSECTIONHTML_DiagnosisNote' AS `KEY`,
    a.NOTE AS encounterdataclob,
    2 AS psid,
    a.nd_extracted_date
FROM tng_athena_one.CLINICALENCOUNTERDIAGNOSIS a
JOIN tng_athena_one.CLINICALENCOUNTER b 
    ON a.CLINICALENCOUNTERID = b.CLINICALENCOUNTERID
WHERE a.nd_active_flag = 'Y';

