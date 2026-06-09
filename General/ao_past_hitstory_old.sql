use tncpa;

DROP TABLE IF EXISTS udm_tncpa.social_history;

create table udm_tncpa.social_history as
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
0006 as psid
from (select * from SOCIALHXFORMRESPONSEANSWER where nd_active_flag = 'Y') shra
inner join (select * from SOCIALHXFORMRESPONSE where nd_active_flag = 'Y') shr on shra.socialhxformresponseid = shr.socialhxformresponseid
inner join (select * from CLINICALENCOUNTER where nd_active_flag = 'Y') ce on shr.chartid = ce.chartid and shr.clinicalencounterid = ce.clinicalencounterid
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
0006 as psid
from (select * from PATIENTSOCIALHISTORY where nd_active_flag = 'Y') ps
where ps.socialhistorykey <> 'REVIEWED.SOCIALHISTORY'
;


DROP TABLE IF EXISTS udm_tncpa.family_history;

create table udm_tncpa.family_history as 
select 
pf.familyhistoryid as familyhistoryid,
pf.CHARTID as ndid, 
null as eid, 
null as encounter_date,
case
  when CREATEDDATETIME in ('None', '') then null
  when left(CREATEDDATETIME,10) REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' then STR_TO_DATE(left(CREATEDDATETIME,10), '%Y-%m-%d')
  when left(CREATEDDATETIME,10) REGEXP '^[0-9]{2}-[0-9]{2}-[0-9]{4}$' then STR_TO_DATE(left(CREATEDDATETIME,10), '%m-%d-%Y')
  else null
end as fam_hist_date,
'FamilyHistory' as hist_category,
pf.relation as fam_hist_relation,
pf.familyhistoryproblem  as fam_hist_detail,
current_date() as created_datetime,
'ND' as created_by,
current_date() as updated_datetime,
'ND' as updated_by,
'athenaone' as ehr_source_name,
'bronze_layer' as source_path,
'Structured' as data_type,
0006 as psid
from (select * from PATIENTFAMILYHISTORY where nd_active_flag = 'Y') pf;


DROP TABLE IF EXISTS udm_tncpa.surgical_history;

create table udm_tncpa.surgical_history as 
select 
ps.PATIENTSURGERYID as surgicalhistoryid,
ps.CHARTID as ndid, 
null as eid, 
null as encounter_date,
coalesce(
case
  when SURGERYDATETIME in ('None', '') then null
  when left(SURGERYDATETIME,10) REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' then STR_TO_DATE(left(SURGERYDATETIME,10), '%Y-%m-%d')
  when left(SURGERYDATETIME,10) REGEXP '^[0-9]{2}-[0-9]{2}-[0-9]{4}$' then STR_TO_DATE(left(SURGERYDATETIME,10), '%m-%d-%Y')
  else null
end,
case
  when CREATEDDATETIME in ('None', '') then null
  when left(CREATEDDATETIME,10) REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' then STR_TO_DATE(left(CREATEDDATETIME,10), '%Y-%m-%d')
  when left(CREATEDDATETIME,10) REGEXP '^[0-9]{2}-[0-9]{2}-[0-9]{4}$' then STR_TO_DATE(left(CREATEDDATETIME,10), '%m-%d-%Y')
  else null
end)
as surgery_date,
-- null as surgery_date,
ps.type as surg_hist_type, 
ps.procedure as surgery_name, 
coalesce(ps.snomedcode, ps.procedurecode) as surgery_code,
case when ps.snomedcode is not null then 'SNOMED'
    when ps.procedurecode is not null then 'CPT/HCPCS' end as surgery_coding_system,
null  as surgery_reason,
current_date() as created_datetime,
'ND' as created_by,
current_date() as updated_datetime,
'ND' as updated_by,
'athenaone' as ehr_source_name,
'bronze_layer' as source_path,
'Structured' as data_type,
0006 as psid
from (SELECT * from PATIENTSURGERY where nd_active_flag = 'Y') ps
where ps.type <> 'REVIEWED.PATIENTSURGICALHISTORY'
union
select 
psh.PATIENTSURGICALHISTORYid as surgicalhistoryid,
psh.CHARTID as ndid, 
null as eid, 
null as encounter_date,
coalesce(
case
  when SURGERYDATEDATETIME in ('None', '') then null
  when left(SURGERYDATEDATETIME,10) REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' then STR_TO_DATE(left(SURGERYDATEDATETIME,10), '%Y-%m-%d')
  when left(SURGERYDATEDATETIME,10) REGEXP '^[0-9]{2}-[0-9]{2}-[0-9]{4}$' then STR_TO_DATE(left(SURGERYDATEDATETIME,10), '%m-%d-%Y')
  else null
end,
case
  when psh.CREATEDDATETIME in ('None', '') then null
  when left(psh.CREATEDDATETIME,10) REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' then STR_TO_DATE(left(psh.CREATEDDATETIME,10), '%Y-%m-%d')
  when left(psh.CREATEDDATETIME,10) REGEXP '^[0-9]{2}-[0-9]{2}-[0-9]{4}$' then STR_TO_DATE(left(psh.CREATEDDATETIME,10), '%m-%d-%Y')
  else null
end)
as surgery_date,
'PATIENTSURGICALHISTORY' as surg_hist_type, 
COALESCE(shp.NAME,s.DESCRIPTION) as surgery_name, 
coalesce(psh.snomedcode, psh.procedurecode) as surgery_code,
case when psh.snomedcode is not null then 'SNOMED'
    when psh.procedurecode is not null then 'CPT/HCPCS' end as surgery_coding_system,
psh.note  as surgery_reason,
current_date() as created_datetime,
'ND' as created_by,
current_date() as updated_datetime,
'ND' as updated_by,
'athenaone' as ehr_source_name,
'bronze_layer' as source_path,
'Structured' as data_type,
0006 as psid
from (SELECT * from PATIENTSURGICALHISTORY where nd_active_flag = 'Y') psh
left join (SELECT * from SNOMED where nd_active_flag = 'Y') s on psh.snomedcode = s.SNOMEDCODE
left join (SELECT * from SURGICALHISTORYPROCEDURE where nd_active_flag = 'Y') shp on psh.SURGICALHISTORYPROCEDUREID = shp.SURGICALHISTORYPROCEDUREID ;

DROP TABLE IF EXISTS udm_tncpa.medical_history;

create table udm_tncpa.medical_history as 
select 
pm.PASTMEDICALHISTORYID as PASTMEDICALHISTORYID,
pm.CHARTID as ndid, 
null as eid, 
null as encounter_date,
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
0006 as psid
from (select * from PATIENTPASTMEDICALHISTORY where nd_active_flag = 'Y') pm
where pm.pastmedicalhistorykey <> 'REVIEWED.PASTMEDICALHISTORY';


