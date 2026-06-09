insert into udm_staging.procedures(proc_id, ndid, eid, encounter_date, proc_start_date, 
proc_last_date, proc_category, proc_code, proc_name, proc_coding_system, proc_units, 
proc_description, proc_notes, anesthesia_flag, anesthesia_detail_id, ordering_provider_id, 
ordering_provider_name, ordering_provider_npi, rendering_provider_id, rendering_provider_name, 
rendering_provider_npi, referring_provider_id, referring_provider_name, referring_provider_npi, 
place_of_service_Id, place_of_service_desc, order_date, Diagnosis_Indication, nd_extracted_date,
 created_datetime, created_by, updated_datetime, updated_by, ehr_source_name, source_path, data_type, 
 psid, udm_inc_id, udm_unq_id,enc_date_proxy)
SELECT
    t.*,
    CONCAT_WS(':',
        COALESCE(t.psid,           ''),
        COALESCE(t.ndid,           ''),
        COALESCE(t.eid,            ''),
        COALESCE(t.encounter_date, ''),
        COALESCE(t.proc_start_date,''),
        COALESCE(t.proc_last_date, ''),
        COALESCE(t.proc_code,      ''),
        COALESCE(t.proc_name,      '')
    ) AS udm_unq_id,
    COALESCE(t.encounter_date) AS enc_date_proxy
FROM 
(select 
FLOOR(TRIM(a.ServiceDetailID)) as proc_id,
TRIM(a.PatientID) as ndid,
TRIM(b.VisitID) as eid,
DATE_FORMAT(
  STR_TO_DATE(SUBSTRING_INDEX(b.fromdatetime, '.', 1), '%Y-%m-%d %H:%i:%s'),
  '%Y-%m-%d'
) AS encounter_date,
DATE_FORMAT(
  STR_TO_DATE(SUBSTRING_INDEX(a.FromDate, '.', 1), '%Y-%m-%d %H:%i:%s'),
  '%Y-%m-%d'
) AS proc_start_date,
DATE_FORMAT(
  STR_TO_DATE(SUBSTRING_INDEX(a.ToDate, '.', 1), '%Y-%m-%d %H:%i:%s'),
  '%Y-%m-%d'
) AS proc_last_date,
null AS proc_category,
TRIM(a.ProcedureCode) as proc_code,
TRIM(c.StandardDescription) as proc_name,
null as proc_coding_system,
a.NumberOfDaysOrUnits as proc_units,
TRIM(c.StandardDescription) as proc_description,
null as proc_notes,
null as anesthesia_flag,
null as anesthesia_detail_id,
TRIM(a.CareProviderID) as ordering_provider_id,
null as ordering_provider_name,
-- null as ordering_provider_npi,
 TRIM(d.NationalProviderID) ordering_provider_npi,
floor(TRIM(a.RenderingProviderID)) as rendering_provider_id,
null as rendering_provider_name,
-- CASE WHEN a.CareProviderID = a.RenderingProviderID THEN d.NationalProviderID END  as rendering_provider_npi,
null as rendering_provider_npi ,
floor(TRIM(a.ReferringProvID)) as referring_provider_id,
null as referring_provider_name,
CASE WHEN a.CareProviderID = a.ReferringProvID THEN d.NationalProviderID END as referring_provider_npi,
-- null as referring_provider_npi,
PlaceOfServiceID as place_of_service_Id,
ps.POSDesc as place_of_service_desc,
a.OrderDate as order_date,
null as Diagnosis_Indication,
a.nd_extracted_date as nd_extracted_date,
current_date() as created_datetime,
'ND' as created_by,
current_date() as updated_datetime,
'ND' as updated_by,
'Greenway' AS ehr_source_name,
'bronze_table' as source_path,
'Structured' as data_type,
'9' as psid
from savannah.br_ServiceDetail 	a
inner join savannah.Visit AS b ON a.VisitID=b.VisitID and a.nd_ActiveFlag = 'Y' and b.nd_ActiveFlag = 'Y'	
join savannah.ProcedureMasterInfo AS c ON a.ProcedureMasterID=c.ProcedureMasterID and c.nd_ActiveFlag = 'Y'
left join savannah.CareProvider d ON a.CareProviderID=d.CareProviderID and d.nd_ActiveFlag = 'Y'
left join savannah.PlaceOfService ps on a.PlaceOfServiceID = ps.POSCode and ps.nd_ActiveFlag = 'Y')t
where DATE(t.nd_extracted_date) > '2026-01-26';
;