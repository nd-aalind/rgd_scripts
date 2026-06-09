update  rgd_udm_silver.notes_part1  a
join tng_athena_one.CLINICALENCOUNTER b
on a.eid = b.clinicalencounterid
set a.enc_start_date = b.ENCOUNTERDATE
where a.enc_start_date is null and a.psid = 5;