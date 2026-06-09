SELECT
    NULL AS social_hist_id,
    enc.patientID AS ndid,
    social.encounterID AS eid,
    enc.date AS encounter_date,
    enc.date AS social_hist_date,
    "Social History" AS social_hist_category,
    items.itemname AS social_hist_subcategory,
    i2.itemname AS social_hist_question,
    MAX(CASE 
            WHEN properties.name = 'Options' THEN social.value
        END) AS social_hist_value,
    MAX(CASE 
            WHEN properties.name = 'Notes' THEN social.value
        END) AS social_hist_code,
    NULL AS social_hist_coding_system,
    NULL AS social_hist_notes,
    "social" AS data_source,
    current_date() as created_datetime,
'ND' as created_by,
current_date() as updated_datetime,
'ND' as updated_by,
'athenaone' as ehr_source_name,
'bronze_layer' as source_path,
'Structured' as data_type,
'0010' as psid,
social.nd_extracted_date as nd_extracted_date
FROM social
LEFT JOIN enc 
    ON enc.encounterid = social.encounterID and social.nd_Activeflag = 'Y' 
    and enc.nd_Activeflag = 'Y'
LEFT JOIN items 
    ON social.catid = items.itemid and items.nd_Activeflag = 'Y'
LEFT JOIN items i2 
    ON social.itemid = i2.itemid and items.nd_Activeflag = 'Y'
LEFT JOIN properties 
    ON social.propid = properties.propid and properties.nd_Activeflag = 'Y'
GROUP BY
    enc.patientID,
    social.encounterID,
    enc.date,
    items.itemname
UNION ALL
SELECT 
    NULL AS social_hist_id,
    enc.patientid AS ndid,
    structsocialhistory.encounterid AS eid,
    enc.date AS encounter_date,
    enc.date AS social_hist_date,
    "Social History" AS social_hist_category,
    items.itemName AS social_hist_subcategory,
    structdatadetail.name AS social_hist_question,
    structsocialhistory.value AS social_hist_value,
    structsocialhistory.notes AS social_hist_code,
    NULL AS social_hist_coding_system,
    NULL AS social_hist_notes,
    "structsocialhistory" AS data_source,
    'ND' as created_by,
current_date() as updated_datetime,
'ND' as updated_by,
'athenaone' as ehr_source_name,
'bronze_layer' as source_path,
'Structured' as data_type,
'0010' as psid,
structsocialhistory.nd_extracted_date as nd_extracted_date
FROM structsocialhistory
LEFT JOIN enc 
    ON structsocialhistory.encounterid = enc.encounterid
    and structsocialhistory.nd_Activeflag = 'Y' and enc.nd_Activeflag = 'Y'
LEFT JOIN items 
    ON structsocialhistory.itemid = items.itemid and items.nd_Activeflag = 'Y'
LEFT JOIN structdatadetail 
    ON structsocialhistory.detailid = structdatadetail.id and structdatadetail = 'Y';