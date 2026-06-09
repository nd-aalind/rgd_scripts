---PastMedical History----

select 
pm.PASTMEDICALHISTORYID as PASTMEDICALHISTORYID,
pm.CHARTID as ndid, 
null as eid, 
null as enc_date,
case
  when CREATEDDATETIME in ('None', '') then null
  when left(CREATEDDATETIME,10) REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' then STR_TO_DATE(left(CREATEDDATETIME,10), '%Y-%m-%d')
  when left(CREATEDDATETIME,10) REGEXP '^[0-9]{2}-[0-9]{2}-[0-9]{4}$' then STR_TO_DATE(left(CREATEDDATETIME,10), '%m-%d-%Y')
  else null
end as med_hist_date,
'MedicalHistory' as hist_category,
pm.pastmedicalhistorykey as med_hist_category,
pm.pastmedicalhistoryquestion as med_hist_question,
pm.pastmedicalhistoryanswer  as med_hist_answer,
current_date() as created_datetime,
'ND' as created_by,
current_date() as updated_datetime,
'ND' as updated_by,
'athenaone' as ehr_source_name,
'bronze_layer' as source_path,
'Structured' as data_type,
'0010' as psid,
pm.nd_extracted_date as nd_extracted_date
from PATIENTPASTMEDICALHISTORY pm
where pm.pastmedicalhistorykey <> 'REVIEWED.PASTMEDICALHISTORY'
and pm.nd_active_flag='Y'
and pm.deleteddatetime is null;