SELECt a.PatHistMedicalId as PASTMEDICALHISTORYID ,
a.PatientId as ndid, 
null as eid, 
null as enc_date,
case
  when a.CreateDate in ('None', '') then null
  when left(a.CreateDate,10) REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' then STR_TO_DATE(left(a.CreateDate,10), '%Y-%m-%d')
  when left(a.CreateDate,10) REGEXP '^[0-9]{2}-[0-9]{2}-[0-9]{4}$' then STR_TO_DATE(left(a.CreateDate,10), '%m-%d-%Y')
  else null
end as med_hist_date,
'MedicalHistory' as hist_category,
d.PatHistCatDescription as med_hist_category,
null  as med_hist_question,
null  as med_hist_answer,
current_date() as created_datetime,
'ND' as created_by,
current_date() as updated_datetime,
'ND' as updated_by,
'Greenway' as ehr_source_name,
'bronze_layer' as source_path,
'Structured' as data_type,
9 as psid,
a.nd_extracted_date as nd_extracted_date
 from PatHistMedical a
Join PatHistCatPatHistItem b on a.PatHistCatPatHistItemID  = b.PatHistCatPatHistItemID 
and b.nd_ActiveFlag = 'Y'and a.nd_ActiveFlag = 'Y'
join PatHistCat d on d.PatHistCatID = b.PatHistCatID  and d.nd_ActiveFlag = 'Y';