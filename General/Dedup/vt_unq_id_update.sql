update rgd_udm_silver.vitals set udm_unq_id_raw = MD5(CONCAT_WS(':',
        COALESCE(psid,           ''),
        COALESCE(ndid,           ''),
        COALESCE(eid,            ''),
        COALESCE(vital_id,            ''),
        COALESCE(vital_date, ''),
        COALESCE(vital_time,''),
        COALESCE(vital_code, ''),
        COALESCE(vital_name,      '')
    ));