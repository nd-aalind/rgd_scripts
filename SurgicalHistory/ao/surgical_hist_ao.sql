SELECT
    ps.PATIENTSURGERYID AS surgicalhistoryid,
    ps.CHARTID AS ndid,
    NULL AS eid,
    NULL AS enc_date,

    COALESCE(
        CASE
            WHEN SURGERYDATETIME IN ('None','') THEN NULL
            WHEN LEFT(SURGERYDATETIME,10) REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
                THEN STR_TO_DATE(LEFT(SURGERYDATETIME,10),'%Y-%m-%d')
            WHEN LEFT(SURGERYDATETIME,10) REGEXP '^[0-9]{2}-[0-9]{2}-[0-9]{4}$'
                THEN STR_TO_DATE(LEFT(SURGERYDATETIME,10),'%m-%d-%Y')
        END,
        CASE
            WHEN CREATEDDATETIME IN ('None','') THEN NULL
            WHEN LEFT(CREATEDDATETIME,10) REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
                THEN STR_TO_DATE(LEFT(CREATEDDATETIME,10),'%Y-%m-%d')
            WHEN LEFT(CREATEDDATETIME,10) REGEXP '^[0-9]{2}-[0-9]{2}-[0-9]{4}$'
                THEN STR_TO_DATE(LEFT(CREATEDDATETIME,10),'%m-%d-%Y')
        END
    ) AS surgery_date,

    ps.type AS surg_hist_type,
    ps.procedure AS surgery_name,

    COALESCE(ps.snomedcode, ps.procedurecode) AS surgery_code,

    CASE
        WHEN ps.snomedcode IS NOT NULL THEN 'SNOMED'
        WHEN ps.procedurecode REGEXP '^[0-9]{5}$' THEN 'CPT'
        WHEN ps.procedurecode REGEXP '^[A-Za-z][0-9]{4}$' THEN 'HCPCS'
    END AS surgery_coding_system,

    NULL AS surgery_reason,

    CURRENT_TIMESTAMP() AS created_datetime,
    'ND' AS created_by,
    CURRENT_TIMESTAMP() AS updated_datetime,
    'ND' AS updated_by,

    'AthenaOne' AS ehr_source_name,
    'bronze_layer' AS source_path,
    'Structured' AS data_type,
    10 AS psid,

    nd_extracted_date

FROM PATIENTSURGERY ps
WHERE ps.type <> 'REVIEWED.PATIENTSURGICALHISTORY'
  AND ps.deleteddatetime IS NULL
  AND ps.nd_active_flag = 'Y'

UNION ALL

SELECT
    psh.PATIENTSURGICALHISTORYID AS surgicalhistoryid,
    psh.CHARTID AS ndid,
    NULL AS eid,
    NULL AS enc_date,

    COALESCE(
        CASE
            WHEN SURGERYDATEDATETIME IN ('None','') THEN NULL
            WHEN LEFT(SURGERYDATEDATETIME,10) REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
                THEN STR_TO_DATE(LEFT(SURGERYDATEDATETIME,10),'%Y-%m-%d')
            WHEN LEFT(SURGERYDATEDATETIME,10) REGEXP '^[0-9]{2}-[0-9]{2}-[0-9]{4}$'
                THEN STR_TO_DATE(LEFT(SURGERYDATEDATETIME,10),'%m-%d-%Y')
        END,
        CASE
            WHEN psh.CREATEDDATETIME IN ('None','') THEN NULL
            WHEN LEFT(psh.CREATEDDATETIME,10) REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
                THEN STR_TO_DATE(LEFT(psh.CREATEDDATETIME,10),'%Y-%m-%d')
            WHEN LEFT(psh.CREATEDDATETIME,10) REGEXP '^[0-9]{2}-[0-9]{2}-[0-9]{4}$'
                THEN STR_TO_DATE(LEFT(psh.CREATEDDATETIME,10),'%m-%d-%Y')
        END
    ) AS surgery_date,

    'PATIENTSURGICALHISTORY' AS surg_hist_type,

    COALESCE(shp.NAME, s.DESCRIPTION) AS surgery_name,

    COALESCE(psh.snomedcode, psh.procedurecode) AS surgery_code,

    CASE
        WHEN psh.snomedcode IS NOT NULL THEN 'SNOMED'
        WHEN psh.procedurecode REGEXP '^[0-9]{5}$' THEN 'CPT'
        WHEN psh.procedurecode REGEXP '^[A-Za-z][0-9]{4}$' THEN 'HCPCS'
    END AS surgery_coding_system,

    psh.note AS surgery_reason,

    CURRENT_TIMESTAMP() AS created_datetime,
    'ND' AS created_by,
    CURRENT_TIMESTAMP() AS updated_datetime,
    'ND' AS updated_by,

    'AthenaOne' AS ehr_source_name,
    'bronze_layer' AS source_path,
    'Structured' AS data_type,
    10 AS psid,

    psh.nd_extracted_date

FROM PATIENTSURGICALHISTORY psh
LEFT JOIN SNOMED s
    ON psh.snomedcode = s.SNOMEDCODE
LEFT JOIN SURGICALHISTORYPROCEDURE shp
    ON psh.SURGICALHISTORYPROCEDUREID = shp.SURGICALHISTORYPROCEDUREID
   AND shp.nd_active_flag = 'Y'
WHERE psh.deleteddatetime IS NULL
  AND psh.nd_active_flag = 'Y';