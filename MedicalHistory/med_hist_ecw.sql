SELECT
    NULL AS med_hist_id,
    enc.patientid AS ndid,
    enc.encounterid AS eid,
    enc.date AS encounter_date,
    NULL AS onset_date,
    enc.date AS med_hist_date,
    "Patient Medical History" AS med_hist_category,
    NULL AS med_hist_subcategory,
    NULL AS med_hist_question,
    encounterdata.pasthistory AS med_hist_value,
    NULL AS med_hist_code,
    NULL AS med_hist_coding_system,
    NULL AS med_hist_notes,
    current_date() as created_datetime,
'ND' as created_by,
current_date() as updated_datetime,
'ND' as updated_by,
'ECW' as ehr_source_name,
'bronze_layer' as source_path,
'Structured' as data_type,
9 as psid,
encounterdata.nd_extracted_date as nd_extracted_date
FROM encounterdata
LEFT JOIN enc 
    ON enc.encounterid = encounterdata.encounterid and encounterdata.nd_Activeflag = 'Y' 
    AND enc.nd_Activeflag = 'Y'
WHERE LENGTH(encounterdata.pasthistory)>1