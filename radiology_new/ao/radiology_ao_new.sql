select 
cr.CLINICALRESULTID as result_id, 
coalesce(d.CHARTID,ce.chartid) as ndid,
ce.CLINICALENCOUNTERID as eid, 
case 
  when ce.ENCOUNTERDATE in ('None', '') then null
  when LENGTH(ce.ENCOUNTERDATE) = 10 and ce.ENCOUNTERDATE REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' then STR_TO_DATE(ce.ENCOUNTERDATE, '%Y-%m-%d')
  when LENGTH(ce.ENCOUNTERDATE) = 10 and ce.ENCOUNTERDATE REGEXP '^[0-9]{2}-[0-9]{2}-[0-9]{4}$' then STR_TO_DATE(ce.ENCOUNTERDATE, '%m-%d-%Y')
  else null
end as enc_date,
CASE 
  WHEN cr.OBSERVATIONDATETIME IN ('None', '') THEN NULL
  -- YYYY-MM-DD
  WHEN cr.OBSERVATIONDATETIME REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
    THEN DATE(STR_TO_DATE(cr.OBSERVATIONDATETIME, '%Y-%m-%d'))
  -- YYYY-MM-DD HH:MM:SS
  WHEN cr.OBSERVATIONDATETIME REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}$'
    THEN DATE(STR_TO_DATE(cr.OBSERVATIONDATETIME, '%Y-%m-%d %H:%i:%s'))
  -- MM-DD-YYYY
  WHEN cr.OBSERVATIONDATETIME REGEXP '^[0-9]{2}-[0-9]{2}-[0-9]{4}$'
    THEN DATE(STR_TO_DATE(cr.OBSERVATIONDATETIME, '%m-%d-%Y'))
  -- MM-DD-YYYY HH:MM:SS
  WHEN cr.OBSERVATIONDATETIME REGEXP '^[0-9]{2}-[0-9]{2}-[0-9]{4} [0-9]{2}:[0-9]{2}:[0-9]{2}$'
    THEN DATE(STR_TO_DATE(cr.OBSERVATIONDATETIME, '%m-%d-%Y %H:%i:%s'))
  ELSE NULL
END AS img_date,
cr.CLINICALORDERTYPE as study_name, 
cr.CLINICALORDERGENUS as modality,
cro.RESULTSTATUS as img_status,
null as img_report_text,
group_concat(distinct coalesce(cro.result,d.documenttextdata) SEPARATOR '\n') img_finding,
d.DOCUMENTID AS report_id,
CASE 
  WHEN cr.OBSERVATIONDATETIME IN ('None', '') THEN NULL
  -- YYYY-MM-DD
  WHEN cr.OBSERVATIONDATETIME REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
    THEN DATE(STR_TO_DATE(cr.OBSERVATIONDATETIME, '%Y-%m-%d'))
  -- YYYY-MM-DD HH:MM:SS
  WHEN cr.OBSERVATIONDATETIME REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}$'
    THEN DATE(STR_TO_DATE(cr.OBSERVATIONDATETIME, '%Y-%m-%d %H:%i:%s'))
  -- MM-DD-YYYY
  WHEN cr.OBSERVATIONDATETIME REGEXP '^[0-9]{2}-[0-9]{2}-[0-9]{4}$'
    THEN DATE(STR_TO_DATE(cr.OBSERVATIONDATETIME, '%m-%d-%Y'))
  -- MM-DD-YYYY HH:MM:SS
  WHEN cr.OBSERVATIONDATETIME REGEXP '^[0-9]{2}-[0-9]{2}-[0-9]{4} [0-9]{2}:[0-9]{2}:[0-9]{2}$'
    THEN DATE(STR_TO_DATE(cr.OBSERVATIONDATETIME, '%m-%d-%Y %H:%i:%s'))
  ELSE NULL
END AS report_date,
d.STATUS as report_status,
null as img_reason,
case when (d.ORDERDATETIME) in ('','None','null') then null else DATE(STR_TO_DATE(d.ORDERDATETIME, '%m-%d-%Y %H:%i:%s')) end  as order_date,
cr.REPORTSTATUS as order_status,
cr.ORDERDOCUMENTID as order_prescription,
cr.CLINICALPROVIDERID as provider_id,
null as provider_name,
null as provider_npi,
CONCAT(coalesce(cro.observationnote,''),' - ',coalesce(d.providernote,'')) as internal_notes, -- cro.observationnote
cr.EXTERNALNOTE as note_to_patient,
d.DEPARTMENTID AS facility_id,
null as interpretation,
d.SOURCE as source,
null as result,
null as report_text,
current_timestamp() as created_datetime,
'ND' as created_by,
current_timestamp() as updated_datetime,
'ND' as updated_by,
'athenaone' as ehr_source_name,
'bronze_layer' as source_path,
'Structured' as data_type,
0005 as psid
FROM CLINICALRESULT cr
LEFT JOIN (SELECT * FROM CLINICALRESULTOBSERVATION WHERE nd_active_flag = 'Y') cro 
ON cr.CLINICALRESULTID=cro.CLINICALRESULTID
JOIN (SELECT * FROM DOCUMENT WHERE nd_active_flag = 'Y') d 
ON cr.DOCUMENTID=d.DOCUMENTID
LEFT JOIN (SELECT * FROM CLINICALENCOUNTER WHERE nd_active_flag = 'Y') ce 
on d.chartid = ce.chartid 
and ce.clinicalencounterid=d.clinicalencounterid
where cr.clinicalordertypegroup = 'IMAGING' AND cr.nd_active_flag = 'Y'
group by result_id, ndid, eid, enc_date, study_name,modality, img_date,
 img_status, order_status, report_date, internal_notes,
 created_datetime, created_by, updated_datetime, updated_by,
 ehr_source_name, source_path, data_type, psid,report_id,report_status,order_date,source,order_prescription,provider_id,note_to_patient;