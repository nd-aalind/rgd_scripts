insert into udm_staging.diagnosis (diag_id, ndid, eid, enc_date, 
encounter_end_date, diag_date, diag_code, diag_desc, diag_coding_system, 
diag_code_stripped, primary_diagnosis_flag, parent_diagnosis_code, parent_diagnosis_desc, 
icd_codeset, icd_codeset_desc, icd_codeset_group, icd_codeset_system, snomed_code, 
diag_severity, diag_status, diag_end_date, provisional_diag_flag, differential_diag_flag, 
comments_notes, diag_risk, specify, nd_extracted_date, created_datetime, created_by,updated_datetime,updated_by,
ehr_source_name, source_path, data_type, psid, udm_inc_id,enc_date_proxy,udm_unq_id)
select t.*,
COALESCE(enc_date, diag_date) as enc_date_proxy,
CONCAT_WS(
    ':',
    COALESCE(psid, ''),COALESCE(ndid, ''),COALESCE(eid, ''),
    COALESCE(enc_date, ''),COALESCE(diag_date, ''),COALESCE(diag_code, ''),COALESCE(diag_desc, '')
) as udm_unq_id from 
(select distinct 
brvd.VisitDiagnosisID as diag_id,
v.PatientID as ndid, 
v.VisitID as eid, 
DATE(v.FromDateTime) AS enc_date,
CASE
  WHEN v.ThroughDateTime IS NULL OR v.ThroughDateTime IN ('', 'None') THEN NULL
  WHEN v.ThroughDateTime REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}'
    THEN DATE(v.ThroughDateTime)
  ELSE NULL
END AS encounter_end_date,
DATE(v.FromDateTime) AS diag_date,
TRIM(brvd.DiagnosisCode) as diag_code,
Case when brvd.CodingSystemID = 1020 then icd9.LONG_DESCRIPTION 
when brvd.CodingSystemID = 1016 then icd10.LONG_DESCRIPTION end as diag_desc,
case when brvd.CodingSystemID = 1020 then 'ICD-9'
when brvd.CodingSystemID = 1016 then 'ICD-10'
else null end as diag_coding_system,
trim((replace(brvd.DiagnosisCode,'.',''))) as diag_code_stripped,
case when brvd.Priority = 1 then "Y" else "N" end  as primary_diagnosis_flag,
'' as parent_diagnosis_code,
'' as parent_diagnosis_desc,
'' as icd_codeset,
'' as icd_codeset_desc,
'' as icd_codeset_group,
'' as icd_codeset_system,
'' as snomed_code,
null as diag_severity,
null as diag_status,
null as diag_end_date,
null as provisional_diag_flag,
null as differential_diag_flag,
null as comments_notes,
null as diag_risk,
null as specify,
date(brvd.nd_extracted_date) as nd_extracted_date,
current_date() as created_datetime,
'ND' as created_by,
current_date() as updated_datetime,
'ND' as updated_by,
'Greenway' AS ehr_source_name,
'bronze_table' as source_path,
'Structured' as data_type,
'9' as psid,
NULL AS udm_inc_id
from savannah.br_VisitDiagnosis brvd
inner join savannah.Visit v on brvd.VisitID = v.VisitID and brvd.nd_ActiveFlag = 'Y' and v.nd_ActiveFlag = 'Y'
-- inner join mind.br_ServiceDetailHistory dh on v.VisitID = dh.VisitID
left join semantics.icd9_fixed icd9 on trim((replace(brvd.DiagnosisCode,'.',''))) COLLATE utf8mb4_general_ci  = icd9.DIAGNOSIS_CODE COLLATE utf8mb4_general_ci
left join semantics.icd10_fixed icd10 on trim((replace(brvd.DiagnosisCode,'.',''))) COLLATE utf8mb4_general_ci  = icd10.CODE COLLATE utf8mb4_general_ci
)t
where DATE(t.nd_extracted_date) > '2026-01-26';