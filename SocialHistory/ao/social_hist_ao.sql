--Social Hx Athenaone --

select 
shr.socialhxformresponseid as socialhistoryid,
shr.CHARTID as ndid, 
shr.CLINICALENCOUNTERID as eid, 
case 
  when ce.ENCOUNTERDATE in ('None', '') then null
  when left(ce.ENCOUNTERDATE,10) REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' then STR_TO_DATE(left(ce.ENCOUNTERDATE,10), '%Y-%m-%d')
  when left(ce.ENCOUNTERDATE,10) REGEXP '^[0-9]{2}-[0-9]{2}-[0-9]{4}$' then STR_TO_DATE(left(ce.ENCOUNTERDATE,10), '%m-%d-%Y')
  else null
end as encounter_date,
case 
  when ce.ENCOUNTERDATE in ('None', '') then null
  when left(ce.ENCOUNTERDATE,10) REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' then STR_TO_DATE(left(ce.ENCOUNTERDATE,10), '%Y-%m-%d')
  when left(ce.ENCOUNTERDATE,10) REGEXP '^[0-9]{2}-[0-9]{2}-[0-9]{4}$' then STR_TO_DATE(left(ce.ENCOUNTERDATE,10), '%m-%d-%Y')
  else null
  end as social_hist_date,
  'SocialHistory' as hist_category,
shra.QUESTIONKEY as social_category,
shra.VALUE as social_option, 
shra.NOTE  as social_notes,
current_date() as created_datetime,
'ND' as created_by,
current_date() as updated_datetime,
'ND' as updated_by,
'athenaone' as ehr_source_name,
'bronze_layer' as source_path,
'Structured' as data_type,
'0010' as psid,
shra.nd_extracted_date as nd_extracted_date
from SOCIALHXFORMRESPONSEANSWER shra
inner join SOCIALHXFORMRESPONSE shr on shra.socialhxformresponseid = shr.socialhxformresponseid and shr.nd_active_flag='Y'
left join CLINICALENCOUNTER ce on shr.chartid = ce.chartid and shr.clinicalencounterid = ce.clinicalencounterid and ce.nd_active_flag='Y'
where shra.nd_active_flag='Y' and shra.DELETEDDATETIME is null
union
select 
ps.socialhistoryid as socialhistoryid,
ps.CHARTID as ndid, 
null as eid, 
null as encounter_date,
case
  when CREATEDDATETIME in ('None', '') then null
  when left(CREATEDDATETIME,10) REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' then STR_TO_DATE(left(CREATEDDATETIME,10), '%Y-%m-%d')
  when left(CREATEDDATETIME,10) REGEXP '^[0-9]{2}-[0-9]{2}-[0-9]{4}$' then STR_TO_DATE(left(CREATEDDATETIME,10), '%m-%d-%Y')
  else null
end as social_hist_date,
ps.socialhistorykey as hist_category,
ps.socialhistoryname as social_category,
ps.socialhistoryanswer as social_option, 
null  as social_notes,
current_date() as created_datetime,
'ND' as created_by,
current_date() as updated_datetime,
'ND' as updated_by,
'athenaone' as ehr_source_name,
'bronze_layer' as source_path,
'Structured' as data_type,
'0010' as psid,
ps.nd_extracted_date as nd_extracted_date
from PATIENTSOCIALHISTORY ps
where ps.socialhistorykey <> 'REVIEWED.SOCIALHISTORY'and ps.nd_active_flag='Y' and ps.deleteddatetime is null;