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
    a.`KEY`,
    a.ENCOUNTERDATACLOB,
    6 AS psid,
    a.nd_extracted_date
FROM tncpa.CLINICALENCOUNTERDATA a
LEFT JOIN tncpa.CLINICALENCOUNTER b 
    ON a.CLINICALENCOUNTERID = b.CLINICALENCOUNTERID
WHERE a.`KEY` LIKE '%FROZENSECTIONHTML_%'
  AND a.nd_active_flag = 'Y';
