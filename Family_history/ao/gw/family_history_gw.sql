SELECt 
a.PatHistFamilyID as familyhistoryid,
a.PatientID as ndid, 
null as eid, 
null as encounter_date, 
case
  when a.CreateDate in ('None', '') then null
  when left(a.CreateDate,10) REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' then STR_TO_DATE(left(a.CreateDate,10), '%Y-%m-%d')
  when left(a.CreateDate,10) REGEXP '^[0-9]{2}-[0-9]{2}-[0-9]{4}$' then STR_TO_DATE(left(a.CreateDate,10), '%m-%d-%Y')
  else null
end as onsetdate,
null as age,
case
  when a.CreateDate in ('None', '') then null
  when left(a.CreateDate,10) REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' then STR_TO_DATE(left(a.CreateDate,10), '%Y-%m-%d')
  when left(a.CreateDate,10) REGEXP '^[0-9]{2}-[0-9]{2}-[0-9]{4}$' then STR_TO_DATE(left(a.CreateDate,10), '%m-%d-%Y')
  else null
end as fam_hist_date,
'FamilyHistory' as hist_category,
c.Relation  as fam_hist_relation,
null family_relationship_code,
a.PFHNote  as fam_hist_detail,
pi.AltSystemCode as family_hist_code,
pi.AltSystem as family_hist_coding_system,
current_date() as created_datetime,
'ND' as created_by,
current_date() as updated_datetime,
'ND' as updated_by,
'Greenway' as ehr_source_name,
'bronze_layer' as source_path,
'Structured' as data_type,
9 as psid,
a.nd_extracted_date as nd_extracted_date
from PatHistFamily a 
Join PatHistCatPatHistItem b on a.PatHistCatPatHistItemID  = b.PatHistCatPatHistItemID and b.nd_ActiveFlag = 'Y' -- and a.nd_ActiveFlag = 'Y'
left join PatHistCatMaster d on d.PatHistCatID = b.PatHistCatID  and d.nd_ActiveFlag = 'Y'
left join PatHistItem pi on pi.PatHistItemID = b.PatHistItemID and pi.nd_ActiveFlag = 'Y'
Left join PatHistFamilyRelation c on a.PatHistCatPatHistItemID = c.PatHistCatPatHistItemID and a.PatientID = c.PatientID  and c.nd_ActiveFlag = 'Y'