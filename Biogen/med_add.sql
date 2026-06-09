create table biogen_april.medication_additional as
select distinct
a.ndid,
a.med_id,
a.eid as encounter_id,
a.enc_date as enc_start_date,
a.med_start_date,
a.med_end_date,
a.med_code,
a.med_name,
a.med_coding_system,
a.med_status,
a.med_formulation,
a.med_strength,
a.med_pb_qty,
a.med_days_supply,
a.med_refills,
a.med_directions,
a.med_fill_type,
null as incremental_id
from rgd_udm_silver.medications_part1 a 
inner join biogen_april.patients_demo_additional b on a.ndid = b.ndid
WHERE  a.enc_date_proxy <= '2026-02-15'
UNION ALL 
select distinct
a.ndid,
a.med_id,
a.eid as encounter_id,
a.enc_date as enc_start_date,
a.med_start_date,
a.med_end_date,
a.med_code,
a.med_name,
a.med_coding_system,
a.med_status,
a.med_formulation,
a.med_strength,
a.med_pb_qty,
a.med_days_supply,
a.med_refills,
a.med_directions,
a.med_fill_type,
null as incremental_id
from rgd_udm_silver.medications_part2 a 
inner join biogen_april.patients_demo_additional b on a.ndid = b.ndid
WHERE  a.enc_date_proxy <= '2026-02-15';
