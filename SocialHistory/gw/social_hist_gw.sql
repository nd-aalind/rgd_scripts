SELECt a.PatHistSocialID,a.PatientID as ndid ,
null as eid, 
null as encounter_date,
case 
  when a.ScreeningDate in ('None', '') then null
  when left(a.ScreeningDate,10) REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' then STR_TO_DATE(left(a.ScreeningDate,10), '%Y-%m-%d')
  when left(a.ScreeningDate,10) REGEXP '^[0-9]{2}-[0-9]{2}-[0-9]{4}$' then STR_TO_DATE(left(a.ScreeningDate,10), '%m-%d-%Y')
  else null
  end as social_hist_date,
  'SocialHistory' as hist_category,
d.PatHistCatDescription as social_category,
null as social_option, 
a.PHSNote  as social_notes,
current_date() as created_datetime,
'ND' as created_by,
current_date() as updated_datetime,
'ND' as updated_by,
'Greenway' as ehr_source_name,
'bronze_layer' as source_path,
'Structured' as data_type,
9 as psid
-- SELECt b.PatHistCatID,a.* 
from PatHistSocial a
Join PatHistCatPatHistItem b on a.PatHistCatPatHistItemID  = b.PatHistCatPatHistItemID and b.nd_ActiveFlag = 'Y'
and a.nd_ActiveFlag = 'Y'
join PatHistCat d on d.PatHistCatID = b.PatHistCatID and d.nd_ActiveFlag = 'Y';