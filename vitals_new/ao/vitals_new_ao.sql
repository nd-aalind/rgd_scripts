select 
var.VITALATTRIBUTEREADINGID as vital_id,
enc.chartid as ndid,
case
when lower(trim(vt.clinicalencounterid)) in ('null','','none') or vt.clinicalencounterid is null
then null else trim(vt.clinicalencounterid)
end as eid,
COALESCE(
        DATE(enc.ENCOUNTERDATE),
        STR_TO_DATE(LEFT(var.READINGDATEDATETIME, 10), '%Y-%m-%d')
    ) AS enc_date,
    COALESCE(
        DATE(enc.ENCOUNTERDATE),
        STR_TO_DATE(LEFT(var.READINGDATEDATETIME, 10), '%Y-%m-%d')
    ) AS enc_last_date,
    COALESCE(
        DATE(enc.ENCOUNTERDATE),
        STR_TO_DATE(LEFT(var.READINGDATEDATETIME, 10), '%Y-%m-%d')
    ) AS vital_date,
    COALESCE(
        STR_TO_DATE(SUBSTRING_INDEX(SUBSTRING_INDEX(var.READINGDATEDATETIME, ' ', -1), '+', 1), '%H:%i:%s'),
        STR_TO_DATE(SUBSTRING_INDEX(SUBSTRING_INDEX(var.CREATEDDATETIME, ' ', -1), '+', 1), '%H:%i:%s')
    ) AS vital_time,
    CASE when COALESCE(vt.keyid,0) <> 0 then vt.keyid else var.VITALATTRIBUTEID end as vital_code,
case
when vt.value is null or vt.key in ('null','','none','Null','None')
then null else 'LOINC'
end as vital_coding_system,
vt.key as vital_name,
vt.DBUNIT as vital_unit,
null as vital_range,
vt.value as vital_result,
current_timestamp() as created_datetime,
'ND' as created_by,
current_timestamp() as updated_datetime,
'ND' as updated_by,
'athenaone' as ehr_source_name,
'bronze_layer' as source_path,
'Structured' as data_type,
0005 as psid,
vt.nd_extracted_date as nd_extracted_date
from vitalsign vt
left join (select * from CLINICALENCOUNTER where nd_active_flag='Y') enc
on vt.clinicalencounterid=enc.clinicalencounterid 
and vt.nd_active_flag='Y'
left  join vitalattributereading var 
on var.CLINICALENCOUNTERDATAID = vt.ENCOUNTERDATAID 
and var.nd_active_flag = 'Y';