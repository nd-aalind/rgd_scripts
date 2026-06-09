WITH notes AS (
    SELECT 
        e.patientid, 
        e.chartid, 
        e.clinicalencounterid, 
        e.encounterdate, 
        ed.clinicalencounterdataid, 
        ed.key, 
        ed.value, 
        ed.encounterdataclob AS notes,
        'CLINICALENCOUNTERDATA' AS note_source,
        e.nd_extracted_date
    FROM (SELECT * FROM CLINICALENCOUNTER WHERE nd_active_flag = 'Y') e
    INNER JOIN (SELECT * FROM CLINICALENCOUNTERDATA WHERE nd_active_flag = 'Y') ed
        ON e.clinicalencounterid = ed.clinicalencounterid
       AND e.contextid = ed.contextid
       AND ed.encounterdataclob IS NOT NULL
       AND ed.encounterdataclob != ''
    UNION ALL
    SELECT 
        e.patientid, 
        e.chartid, 
        e.clinicalencounterid, 
        e.encounterdate, 
        cq.chartquestionnaireid, 
        cq.questionnairetemplatename, 
        cq.score,
        cqa.freetextanswer AS notes,
        'CHARTQUESTIONNAIREANSWER' AS note_source,
        e.nd_extracted_date
    FROM (SELECT * FROM CLINICALENCOUNTER WHERE nd_active_flag = 'Y') e
    LEFT JOIN (SELECT * FROM CHARTQUESTIONNAIRE WHERE nd_active_flag = 'Y') cq 
        ON e.chartid = cq.chartid 
       AND e.contextid = cq.contextid
    LEFT JOIN (SELECT * FROM CHARTQUESTIONNAIREANSWER WHERE nd_active_flag = 'Y') cqa 
        ON cq.chartquestionnaireid = cqa.chartquestionnaireid 
       AND cq.contextid = cqa.contextid
    UNION ALL
    SELECT 
        e.patientid, 
        e.chartid, 
        e.clinicalencounterid, 
        e.encounterdate, 
        d.clinicalencounterdxid, 
        d.status, 
        d.laterality, 
        d.note AS notes,
        'CLINICALENCOUNTERDIAGNOSIS' AS note_source,
        e.nd_extracted_date
    FROM (SELECT * FROM CLINICALENCOUNTER WHERE nd_active_flag = 'Y') e
    LEFT JOIN (SELECT * FROM CLINICALENCOUNTERDIAGNOSIS WHERE nd_active_flag = 'Y') d 
        ON e.clinicalencounterid = d.clinicalencounterid 
       AND e.contextid = d.contextid
    UNION ALL
    SELECT 
        e.patientid, 
        e.chartid, 
        e.clinicalencounterid, 
        e.encounterdate, 
        p.clinicalencounterprepnoteid, 
        NULL, 
        p.location, 
        p.prepnote AS notes,
        'CLINICALENCOUNTERPREPNOTE' AS note_source,
        e.nd_extracted_date
    FROM (SELECT * FROM CLINICALENCOUNTER WHERE nd_active_flag = 'Y') e
    LEFT JOIN (SELECT * FROM CLINICALENCOUNTERPREPNOTE WHERE nd_active_flag = 'Y') p 
        ON e.clinicalencounterid = p.clinicalencounterid 
       AND e.contextid = p.contextid
)
SELECT DISTINCT
    CAST(ce.chartid AS SIGNED) AS ndid,
    CAST(ce.clinicalencounterid AS SIGNED) AS eid,
    CASE
        WHEN ce.encounterdate IS NULL OR ce.encounterdate IN ('', 'None') THEN NULL
        WHEN ce.encounterdate REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}$'
            THEN DATE(ce.encounterdate)
        WHEN ce.encounterdate REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
            THEN STR_TO_DATE(ce.encounterdate, '%Y-%m-%d')
        WHEN ce.encounterdate REGEXP '^[0-9]{2}-[0-9]{2}-[0-9]{4}$'
            THEN STR_TO_DATE(ce.encounterdate, '%m-%d-%Y')
        ELSE NULL
    END AS enc_start_date,
    ce.notes       AS note,        -- LONGTEXT (no CAST needed)
    ce.key         AS note_type,   -- LONGTEXT (no CAST needed)
    ce.note_source AS note_source,
    CURRENT_TIMESTAMP() AS created_datetime,
    'ND'               AS created_by,
    'athenaone'        AS ehr_source_name,
    'bronze_layer'     AS source_path,
    'Structured'       AS data_type,
    10                  AS psid,
    ce.nd_extracted_date as nd_extracted_date
FROM notes ce;