UPDATE rgd_udm_silver.diagnosis_dedup_v1 p
JOIN (
    SELECT 
        udm_inc_id,
        CASE 
            WHEN rn = 1 THEN 'Y'
            ELSE 'N'
        END AS new_flag
    FROM (
        SELECT 
            udm_inc_id,
            ROW_NUMBER() OVER (
                PARTITION BY udm_unq_id
                ORDER BY udm_inc_id DESC
            ) AS rn
        FROM rgd_udm_silver.diagnosis_dedup_v1
    ) x
) t
ON p.udm_inc_id = t.udm_inc_id
SET p.udm_active_flag = t.new_flag;