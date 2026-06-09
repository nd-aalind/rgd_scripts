insert into rgd_udm_staging.vitals
(
    vital_id,
    ndid,
    eid,
    vital_code,
    vital_name,
    vital_coding_system,
    vital_date,
    vital_time,
    vital_unit,
    vital_range,
    vital_result,
    created_datetime,
    created_by,
    updated_datetime,
    updated_by,
    ehr_source_name,
    source_path,
    data_type,
    psid,
    nd_extracted_date,
    enc_date_proxy,
    udm_unq_id,
    udm_active_flag
)

SELECT 
    vital_id,
    ndid,
    eid,
    vital_code,
    vital_name,
    vital_coding_system,
    vital_date,
    vital_time,
    vital_unit,
    vital_range,
    vital_result,
    created_datetime,
    created_by,
    updated_datetime,
    updated_by,
    ehr_source_name,
    source_path,
    data_type,
    psid,
    nd_extracted_date,
    enc_date_proxy,
    udm_unq_id,
    'Y' AS udm_active_flag

from udm_staging.vitals a

INNER JOIN (
    -- Pick the single latest staging row per udm_unq_id
    SELECT
        udm_unq_id,
        nd_extracted_date,
        ROW_NUMBER() OVER (
            PARTITION BY udm_unq_id
            ORDER BY nd_extracted_date DESC
        ) AS rn
    FROM udm_staging.vitals
    WHERE psid = 9
) ranked
    ON  a.udm_unq_id        = ranked.udm_unq_id
    AND a.nd_extracted_date = ranked.nd_extracted_date
    AND ranked.rn = 1;