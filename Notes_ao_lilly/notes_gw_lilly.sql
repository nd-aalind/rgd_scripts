SELECt DISTINCt * from 
(
select 
a.Patientid ndid,b.VisitId eid,DATE(b.FromDateTime) enc_start_date,a.Chief_Complaint as note,'ChiefComplaint' note_type,
'Clinicalbin' note_source,current_date() as created_datetime,'ND' created_by,'Greenway' ehr_source_name,
'bronze_table' source_path,'Structured' data_type,12 psid,DATE(b.nd_extracted_date)  nd_extracted_date 
from jwm.CLINICALBIN_CT_RWE_2_extracteddata a
join jwm.Visit b on a.VisitId = b.VisitId
INNER JOIN udm_staging.patientlist_lilly_all d on d.ndid = a.Patientid 
where a.Chief_Complaint Is NOT NULL
union all 
select 
a.Patientid ndid,b.VisitId eid,DATE(b.FromDateTime) enc_start_date,a.ROS as note,'ROS' note_type,
'Clinicalbin' note_source,current_date() as created_datetime,'ND' created_by,'Greenway' ehr_source_name,
'bronze_table' source_path,'Structured' data_type,12 psid,DATE(b.nd_extracted_date) nd_extracted_date 
from jwm.CLINICALBIN_CT_RWE_2_extracteddata a
join jwm.Visit b on a.VisitId = b.VisitId
INNER JOIN udm_staging.patientlist_lilly_all d on d.ndid = a.Patientid 
where a.ROS Is NOT NULL
union all 
select 
a.Patientid ndid,b.VisitId eid,DATE(b.FromDateTime) enc_start_date,a.HPI as note,'HPI Notes' note_type,
'Clinicalbin' note_source,current_date() as created_datetime,'ND' created_by,'Greenway' ehr_source_name,
'bronze_table' source_path,'Structured' data_type,12 psid,DATE(b.nd_extracted_date) nd_extracted_date 
from jwm.CLINICALBIN_CT_RWE_2_extracteddata a
join jwm.Visit b on a.VisitId = b.VisitId
INNER JOIN udm_staging.patientlist_lilly_all d on d.ndid = a.Patientid 
where a.HPI Is NOT NULL
union all 
select 
a.Patientid ndid,b.VisitId eid,DATE(b.FromDateTime) enc_start_date,a.Social_History as note,'Social_History' note_type,
'Clinicalbin' note_source,current_date() as created_datetime,'ND' created_by,'Greenway' ehr_source_name,
'bronze_table' source_path,'Structured' data_type,12 psid,DATE(b.nd_extracted_date) nd_extracted_date 
from jwm.CLINICALBIN_CT_RWE_2_extracteddata a
join jwm.Visit b on a.VisitId = b.VisitId
INNER JOIN udm_staging.patientlist_lilly_all d on d.ndid = a.Patientid 
where a.Social_History Is NOT NULL
union all 
select 
a.Patientid ndid,b.VisitId eid,DATE(b.FromDateTime) enc_start_date,a.Instructions as note,'Instructions' note_type,
'Clinicalbin' note_source,current_date() as created_datetime,'ND' created_by,'Greenway' ehr_source_name,
'bronze_table' source_path,'Structured' data_type,12 psid,DATE(b.nd_extracted_date) nd_extracted_date 
from jwm.CLINICALBIN_CT_RWE_2_extracteddata a
join jwm.Visit b on a.VisitId = b.VisitId
INNER JOIN udm_staging.patientlist_lilly_all d on d.ndid = a.Patientid 
where a.Instructions Is NOT NULL
union all 
select 
a.Patientid ndid,b.VisitId eid,DATE(b.FromDateTime) enc_start_date,a.Family_Medical_History as note,'Family_Medical_History' note_type,
'Clinicalbin' note_source,current_date() as created_datetime,'ND' created_by,'Greenway' ehr_source_name,
'bronze_table' source_path,'Structured' data_type,12 psid,DATE(b.nd_extracted_date) nd_extracted_date 
from jwm.CLINICALBIN_CT_RWE_2_extracteddata a
join jwm.Visit b on a.VisitId = b.VisitId
INNER JOIN udm_staging.patientlist_lilly_all d on d.ndid = a.Patientid 
where a.Family_Medical_History Is NOT NULL
union all
select 
a.Patientid ndid,b.VisitId eid,DATE(b.FromDateTime) enc_start_date,a.Assessment as note,'Assessment' note_type,
'Clinicalbin' note_source,current_date() as created_datetime,'ND' created_by,'Greenway' ehr_source_name,
'bronze_table' source_path,'Structured' data_type,9 psid,DATE(b.nd_extracted_date) nd_extracted_date 
from jwm.CLINICALBIN_CT_RWE_2_extracteddata a
join jwm.Visit b on a.VisitId = b.VisitId
INNER JOIN udm_staging.patientlist_lilly_all d on d.ndid = a.Patientid 
where a.Assessment Is NOT NULL
) a;