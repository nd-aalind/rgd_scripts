
UPDATE rgd_udm_staging.vitals p -- {{ params.prod_schema }}.{{ params.prod_table }} p
INNER JOIN udm_staging.vitals d
    ON  p.udm_unq_id     = d.udm_unq_id
    AND p.udm_active_flag = 'Y'
    and p.psid = d.psid 
    and p.psid = 9
SET
    p.udm_active_flag  = 'N',
    p.updated_datetime = NOW();