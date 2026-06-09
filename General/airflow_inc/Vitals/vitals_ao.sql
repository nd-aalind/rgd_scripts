-- =============================================================================
-- Category : vitals
-- EHR      : athenaone
-- Step     : 1 of 3 — Staging Insert
-- Purpose  : Read AthenaOne vitals records from the bronze layer and
--            INSERT them into the staging table with enc_date_proxy and
--            udm_unq_id computed columns.
--
-- Params used:
--   {{ params.main_schema }}         — primary bronze schema (e.g. tng_athena_one)
--   {{ params.incremental_schema }}  — incremental bronze schema (e.g. tng_inc)
--   {{ params.staging_schema }}      — target staging schema  (e.g. udm_staging)
--   {{ params.staging_table }}       — target staging table   (e.g. vitals)
--   {{ params.psid }}                — provider-site integer ID
--   {{ params.ehr_source_name }}     — EHR name string (e.g. athenaone)
-- =============================================================================

INSERT INTO {{ params.staging_schema }}.{{ params.staging_table }} (
    vital_id, ndid, eid, vital_code, vital_name, vital_coding_system,
    vital_date, vital_time, vital_unit, vital_range, vital_result,
    nd_extracted_date, created_datetime, created_by, updated_datetime, updated_by,
    ehr_source_name, source_path, data_type, psid,
    enc_date_proxy, udm_unq_id, udm_unq_id_raw
)
SELECT
    a.*,
    a.vital_date AS enc_date_proxy,
    MD5(CONCAT_WS(':',
        COALESCE(a.psid,       ''),
        COALESCE(a.ndid,       ''),
        COALESCE(a.eid,        ''),
        COALESCE(a.vital_date, ''),
        COALESCE(a.vital_time, ''),
        COALESCE(a.vital_code, ''),
        COALESCE(a.vital_name, '')
    )) AS udm_unq_id,
    CONCAT_WS(':',
        COALESCE(a.psid,       ''),
        COALESCE(a.ndid,       ''),
        COALESCE(a.eid,        ''),
        COALESCE(a.vital_date, ''),
        COALESCE(a.vital_time, ''),
        COALESCE(a.vital_code, ''),
        COALESCE(a.vital_name, '')
    ) AS udm_unq_id_raw
FROM (
    SELECT DISTINCT
        vt.ENCOUNTERDATAID                    AS vital_id,
        enc.CHARTID                           AS ndid,
        CASE
            WHEN LOWER(TRIM(vt.CLINICALENCOUNTERID)) IN ('null', '', 'none')
              OR vt.CLINICALENCOUNTERID IS NULL THEN NULL
            ELSE TRIM(vt.CLINICALENCOUNTERID)
        END                                   AS eid,
        vt.KEYID                              AS vital_code,
        vt.KEY                                AS vital_name,
        CASE
            WHEN vt.KEY IS NULL OR vt.KEY IN ('null', '', 'none', 'Null', 'None') THEN NULL
            ELSE 'LOINC'
        END                                   AS vital_coding_system,
        DATE(vt.CREATEDDATETIME)              AS vital_date,
        DATE_FORMAT(vt.CREATEDDATETIME, '%H:%i:%s') AS vital_time,
        vt.DBUNIT                             AS vital_unit,
        NULL                                  AS vital_range,
        vt.VALUE                              AS vital_result,
        vt.nd_extracted_date                  AS nd_extracted_date,
        CURRENT_TIMESTAMP()                   AS created_datetime,
        'ND'                                  AS created_by,
        CURRENT_TIMESTAMP()                   AS updated_datetime,
        'ND'                                  AS updated_by,
        '{{ params.ehr_source_name }}'        AS ehr_source_name,
        'bronze_layer'                        AS source_path,
        'Structured'                          AS data_type,
        {{ params.psid }}                     AS psid
    FROM {{ params.incremental_schema }}.VITALSIGN vt
    INNER JOIN (
        SELECT * FROM {{ params.main_schema }}.CLINICALENCOUNTER WHERE nd_active_flag = 'Y'
    ) enc ON vt.CLINICALENCOUNTERID = enc.CLINICALENCOUNTERID
    WHERE vt.nd_active_flag    = 'Y'
      AND vt.nd_extracted_date {{ params.nd_date_filter }}
) a
