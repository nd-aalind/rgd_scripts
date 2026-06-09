INSERT INTO rgd_udm_silver.examination(
    examid,
    ndid,
    eid,
    enc_start_date,
    exam_date,
    exam_category,
    exam_name,
    exam_findings,
    finding_type,
    exam_parameters,
    created_datetime,
    created_by,
    ehr_source_name,
    source_path,
    data_type,
    psid,
    nd_extracted_date
)
SELECT 
    CAST(ct.PATIENTTEMPLATEDATAID AS CHAR(30)) AS examid,
    CAST(ce.CHARTID AS SIGNED) AS ndid,
    CAST(ce.CLINICALENCOUNTERID AS SIGNED) AS eid,
    CASE 
        WHEN ce.ENCOUNTERDATE IN ('None', '') THEN NULL
        WHEN LEFT(ce.ENCOUNTERDATE,10) REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' 
            THEN STR_TO_DATE(LEFT(ce.ENCOUNTERDATE,10), '%Y-%m-%d')
        WHEN LEFT(ce.ENCOUNTERDATE,10) REGEXP '^[0-9]{2}-[0-9]{2}-[0-9]{4}$' 
            THEN STR_TO_DATE(LEFT(ce.ENCOUNTERDATE,10), '%m-%d-%Y')
        ELSE NULL
    END AS enc_start_date,
    CASE 
        WHEN ce.ENCOUNTERDATE IN ('None', '') THEN NULL
        WHEN LEFT(ce.ENCOUNTERDATE,10) REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' 
            THEN STR_TO_DATE(LEFT(ce.ENCOUNTERDATE,10), '%Y-%m-%d')
        WHEN LEFT(ce.ENCOUNTERDATE,10) REGEXP '^[0-9]{2}-[0-9]{2}-[0-9]{4}$' 
            THEN STR_TO_DATE(LEFT(ce.ENCOUNTERDATE,10), '%m-%d-%Y')
        ELSE NULL
    END AS exam_date,
    CAST(ct.CLINICALTEMPLATENAME AS CHAR(255)) AS exam_category,
    CAST(ct.CLINICALTEMPLATEPARAGRAPH AS CHAR(255)) AS exam_name,
    CONCAT(
        COALESCE(ct.CLINICALTEMPLATESENTENCE, ''),
        COALESCE(ct.CLINICALFINDING, '')
    ) AS exam_findings,
    CAST(ct.FINDINGTYPE AS CHAR(5000)) AS finding_type,
    CAST(ct.CLINICALTEMPLATESENTENCE AS CHAR(500)) AS exam_parameters,
    CURRENT_TIMESTAMP() AS created_datetime,
    'ND' AS created_by,
    'athenaone' AS ehr_source_name,
    'bronze_layer' AS source_path,
    'Structured' AS data_type,
    5 AS psid,
    ct.nd_extracted_date
FROM raleigh.CLINICALTEMPLATE ct
INNER JOIN (
    SELECT *
    FROM raleigh.CLINICALENCOUNTER
    WHERE nd_active_flag = 'Y'
) ce 
    ON ct.CLINICALENCOUNTERID = ce.CLINICALENCOUNTERID  
WHERE 
    ct.ENCOUNTERSECTION = 'PhysicalExam'
    AND ct.nd_active_flag = 'Y';