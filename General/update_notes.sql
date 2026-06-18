update rgd_udm_silver.notes_part1_lilly a
join (select CLINICALENCOUNTERID,CASE WHEN ENCOUNTERDATE IS NULL OR ENCOUNTERDATE IN ('', 'None') THEN NULL
                     WHEN ENCOUNTERDATE REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}$' THEN DATE(ENCOUNTERDATE)
                     WHEN ENCOUNTERDATE REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN STR_TO_DATE(ENCOUNTERDATE, '%Y-%m-%d')
                     WHEN ENCOUNTERDATE REGEXP '^[0-9]{2}/[0-9]{2}/[0-9]{4}$' THEN STR_TO_DATE(ENCOUNTERDATE, '%m/%d/%Y')
                     WHEN ENCOUNTERDATE REGEXP '^[0-9]{2}-[0-9]{2}-[0-9]{4}$' THEN STR_TO_DATE(ENCOUNTERDATE, '%m-%d-%Y')
                     ELSE NULL END as new from tng_athena_one.CLINICALENCOUNTER) b on a.eid = b.clinicalencounterid
                     set a.enc_start_date=new 
                     where a.enc_start_date is null and a.psid=2;