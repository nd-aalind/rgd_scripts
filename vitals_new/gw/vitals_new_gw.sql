select 
a.ClinicalVitalID as vital_id ,
PatientID as ndid,
b.VisitID as eid,
c.vital_date as enc_date,
null as enc_last_date,
c.vital_code,
c.vital_name,
c.vital_coding_system,
c.vital_date,
c.vital_time,
c.vital_unit,
null vital_range,
c.vital_result,
CURRENT_DATE() AS created_datetime,
'ND' AS created_by,
CURRENT_DATE() AS updated_datetime,
'ND' AS updated_by,
'Greenway' AS ehr_source_name,
'bronze_table' AS source_path,
'Structured' AS data_type,
'12' as psid,
a.nd_extracted_date as  nd_extracted_date/* Mind */
-- '9' as psid /* Savannaha */
-- '11' as psid /* JWM */
from ClinicalVital a
join ClinicalVitalGroup b on a.ClinicalVitalGroupID= b.ClinicalVitalGroupID and a.nd_ActiveFlag = 'Y' and b.nd_ActiveFlag = 'Y'
left join (SELECt omc.ClinicalVitalID,om.OBXConceptID as vital_code,
om.TestDescription vital_name,null vital_coding_system,
DATE_FORMAT(om.CollectionDate, '%Y-%m-%d') AS vital_date,
DATE_FORMAT(om.CollectionDate, '%H:%i:%s') AS vital_time,
om.ResultUnits vital_unit, om.ReferenceRange vital_range,
om.ResultValue vital_result
from OBXManual om   
join OBXManualClinicalVital omc on om.OBXManualId = omc.OBXManualId and om.nd_ActiveFlag = 'Y' and omc.nd_ActiveFlag = 'Y') as c on c.ClinicalVitalID = a.ClinicalVitalID;