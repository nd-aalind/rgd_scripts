SELECT 
    a.PatHistSurgicalID AS surghisid,
    a.PatientID,
    CAST(NULL AS UNSIGNED) AS eid,
    CAST(NULL AS DATE) AS enc_date,
    CASE 
        WHEN a.DateOfProcedure IS NULL
             OR TRIM(CAST(a.DateOfProcedure AS CHAR)) = ''
             OR STR_TO_DATE(a.DateOfProcedure, '%Y-%m-%d') IS NULL
        THEN DATE(a.CreateDate)

        ELSE DATE(a.DateOfProcedure)
    END AS Surg_hist_date,
    c.PatHistItemDescription AS Surg_Category,
    CAST(NULL AS CHAR) AS surg_Sub_category,
    CAST(NULL AS CHAR) AS Surg_value,
    c.AltSystemCode AS Surg_Code,
    c.AltSystem AS Surg_Coding_sys,
    a.PSHNote AS Surg_Notes,
    CURRENT_TIMESTAMP() AS created_datetime,
    'ND' AS created_by,
    'Greenway' AS ehr_source_name,
    'bronze_layer' AS source_path,
    'Structured' AS data_type,
    11 AS psid,
    a.nd_extracted_date AS nd_extracted_date
FROM PatHistSurgical a
JOIN PatHistCatPatHistItem b 
    ON a.PatHistCatPatHistItemID = b.PatHistCatPatHistItemID and a.nd_ActiveFlag = 'Y' and b.nd_ActiveFlag = 'Y'
JOIN PatHistItem c 
    ON b.PatHistItemID = c.PatHistItemID and c.nd_ActiveFlag = 'Y';