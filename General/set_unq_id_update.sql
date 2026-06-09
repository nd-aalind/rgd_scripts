update rgd_udm_staging.procedures set udm_unq_id = CONCAT_WS(':',
        COALESCE(t.psid,           ''),
        COALESCE(t.ndid,           ''),
        COALESCE(t.eid,            ''),
        COALESCE(t.proc_id,           ''),
        COALESCE(t.encounter_date, ''),
        COALESCE(t.proc_start_date,''),
        COALESCE(t.proc_last_date, ''),
        COALESCE(t.proc_code,      ''),
        COALESCE(t.proc_name,      '')
    ) where psid = '9';