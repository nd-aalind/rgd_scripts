SET @row_num := 0;

create table rgd_udm_silver.vitals_dedup as 
select  a.*,CONCAT_WS(':',
        COALESCE(a.psid,           ''),
        COALESCE(a.ndid,           ''),
        COALESCE(a.eid,            ''),
        COALESCE(a.vital_id,            ''),
        COALESCE(a.vital_date, ''),
        COALESCE(a.vital_time,''),
        COALESCE(a.vital_code, ''),
        COALESCE(a.vital_name,      '')
    )  from (
select distinct vital_id,ndid,eid,enc_date,enc_last_date,vital_date,vital_time,vital_code,vital_coding_system,
vital_name,vital_unit,vital_range,vital_result,created_datetime,created_by,updated_datetime,updated_by,
ehr_source_name,source_path,data_type,psid,nd_extracted_date,(@row_num := @row_num + 1) AS udm_inc_id 
from rgd_udm_staging.vitals_new order by created_datetime) a;