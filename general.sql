INSERT INTO rgd_udm_silver.notes_part2 (
    ndid,
    eid,
    enc_start_date,
    note,
    note_type,
    note_source,
    created_datetime,
    created_by,
    ehr_source_name,
    source_path,
    data_type,
    psid,
    nd_extracted_date,
    enc_date_proxy
)
SELECT
    ndid,
    eid,
    enc_start_date,
    note,
    note_type,
    note_source,
    created_datetime,
    created_by,
    ehr_source_name,
    source_path,
    data_type,
    psid,
    nd_extracted_date,
    NULL   -- or NULL if you don’t want proxy
FROM udm_staging.notes_rgd_udm_texas;
