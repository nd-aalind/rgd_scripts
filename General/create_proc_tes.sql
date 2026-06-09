create table incremental_test.prod_procedures as
select *, CONCAT_WS(
    ':',
    COALESCE('18',''),COALESCE(ndid, ''),COALESCE(eid, ''),
    COALESCE(encounter_date, ''),COALESCE(proc_start_date, ''),COALESCE(proc_last_date, ''),
    COALESCE(proc_code, ''),COALESCE(proc_name, '')
) as udm_unq_id
from rgd_udm_silver.procedures p where psid = 5 ;