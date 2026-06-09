select 
a.vitalID as vital_id,
en.patientID as ndid,
en.encounterID as eid,
date(en.date) as enc_date,
NULL as enc_last_date,
b.itemName as vital_name,
a.propID as vital_code,
case when COALESCE(a.Value,a.value2) in ('','null') then null else 'LOINC' end as vital_coding_system,
date(en.date) as vital_date,
DATE_FORMAT(en.starttime, '%H:%i:%s') AS vital_time,
'' as vital_unit,
'' as vital_range,
COALESCE(a.Value,a.value2) as vital_result,
current_timestamp() as created_datetime,
'ND' as created_by,
current_timestamp() as updated_datetime,
'ND' as updated_by,
'athenaone' as ehr_source_name,
'bronze_layer' as source_path,
'Structured' as data_type,
0005 as psid,
a.nd_extracted_date as nd_extracted_date
from vitals a
left join items b
on a.vitalid=b.itemid and a.nd_ActiveFlag = 'Y' AND and b.nd_ActiveFlag = 'Y'
left join enc en 
on a.encounterid = en.encounterid and en.nd_ActiveFlag = 'Y'
union 
select
a.vitalID as vital_id,
en.patientID as ndid,
en.encounterID as eid,
date(en.date) as enc_date,
NULL as enc_last_date,
b.itemName as vital_name,
a.propID as vital_code,
case when COALESCE(a.Value,a.value2) in ('','null') then null else 'LOINC' end as vital_coding_system,
date(en.date) as vital_date,
DATE_FORMAT(en.starttime, '%H:%i:%s') AS vital_time,
'' as vital_unit,
'' as vital_range,
COALESCE(a.Value,a.value2) as vital_result,
current_timestamp() as created_datetime,
'ND' as created_by,
current_timestamp() as updated_datetime,
'ND' as updated_by,
'athenaone' as ehr_source_name,
'bronze_layer' as source_path,
'Structured' as data_type,
0005 as psid,
a.nd_extracted_date as nd_extracted_date
from vitalshistory a
left join items b
on a.vitalid=b.itemid and a.nd_ActiveFlag = 'Y' AND and b.nd_ActiveFlag = 'Y'
left join enc en and en.nd_ActiveFlag = 'Y'
on a.encounterid = en.encounterid;