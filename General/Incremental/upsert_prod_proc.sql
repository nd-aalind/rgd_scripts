insert into rgd_udm_staging.procedures
(
    proc_id,
    ndid,
    eid,
    encounter_date,
    proc_start_date,
    proc_last_date,
    proc_category,
    proc_code,
    proc_name,
    proc_coding_system,
    proc_units,
    proc_description,
    proc_notes,
    anesthesia_flag,
    anesthesia_detail_id,
    ordering_provider_id,
    ordering_provider_name,
    ordering_provider_npi,
    rendering_provider_id,
    rendering_provider_name,
    rendering_provider_npi,
    referring_provider_id,
    referring_provider_name,
    referring_provider_npi,
    place_of_service_Id,
    place_of_service_desc,
    order_date,
    Diagnosis_Indication,
    nd_extracted_date,
    created_datetime,
    created_by,
    ehr_source_name,
    source_path,
    data_type,
    psid,
    incremental_id,

    -- new prod columns
    proc_code_std,
    proc_coding_system_std,
    proc_name_std,
    proc_description_std,

    enc_date_proxy,
    udm_active_flag,
    udm_unq_id,
    updated_datetime,
    updated_by
)

SELECT
    proc_id,
    ndid,
    eid,
    encounter_date,
    proc_start_date,
    proc_last_date,
    proc_category,
    proc_code,
    proc_name,
    proc_coding_system,
    proc_units,
    proc_description,
    proc_notes,
    anesthesia_flag,
    anesthesia_detail_id,
    ordering_provider_id,
    ordering_provider_name,
    ordering_provider_npi,
    rendering_provider_id,
    rendering_provider_name,
    rendering_provider_npi,
    referring_provider_id,
    referring_provider_name,
    referring_provider_npi,
    place_of_service_Id,
    place_of_service_desc,
    order_date,
    Diagnosis_Indication,
    nd_extracted_date,
    created_datetime,
    created_by,
    ehr_source_name,
    source_path,
    data_type,
    psid,
    incremental_id,

    -- std columns (populate later via mapping)
    NULL AS proc_code_std,
    NULL AS proc_coding_system_std,
    NULL AS proc_name_std,
    NULL AS proc_description_std,

    enc_date_proxy,
    'Y' AS udm_active_flag,
    udm_unq_id,
    updated_datetime,
    updated_by

from udm_staging.procedures a

INNER JOIN (
    -- latest record per udm_unq_id
    SELECT
        udm_unq_id,
        nd_extracted_date,
        ROW_NUMBER() OVER (
            PARTITION BY udm_unq_id
            ORDER BY nd_extracted_date DESC, created_datetime DESC
        ) AS rn
    FROM udm_staging.procedures
    WHERE psid = 9
) ranked
    ON  a.udm_unq_id        = ranked.udm_unq_id
    AND a.nd_extracted_date = ranked.nd_extracted_date
    AND ranked.rn = 1

WHERE a.psid = 9;