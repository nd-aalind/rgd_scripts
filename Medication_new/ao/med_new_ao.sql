-- ── date_case macro (inlined below for each column) ──────────────────────────
-- Handles: NULL, '', 'None', YYYY-MM-DD HH:MM:SS, YYYY-MM-DD, MM/DD/YYYY, MM-DD-YYYY
-- CASE WHEN col IS NULL OR col IN ('','None') THEN NULL
--      WHEN col REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}$' THEN DATE(col)
--      WHEN col REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN STR_TO_DATE(col,'%Y-%m-%d')
--      WHEN col REGEXP '^[0-9]{2}/[0-9]{2}/[0-9]{4}$' THEN STR_TO_DATE(col,'%m/%d/%Y')
--      WHEN col REGEXP '^[0-9]{2}-[0-9]{2}-[0-9]{4}$' THEN STR_TO_DATE(col,'%m-%d-%Y')
--      ELSE NULL END

WITH medication_src AS (
    SELECT DISTINCT
        'clinicalprescription'                          AS source,
        cp.CLINICALPRESCRIPTIONID                       AS med_id,
        d.CHARTID                                       AS ndid,
        ch.ENTERPRISEID                                 AS patientid,
        ce.CLINICALENCOUNTERID                          AS eid,
        ce.ENCOUNTERDATE                                AS enc_date,
        cp.WRITTENDATEDATETIME                          AS written_date,
        cp.MEDICATIONADMINISTEREDDATETIME               AS med_administered_datetime,
        d.ORDERDATETIME                                 AS doc_orderdatetime,
        cp.STARTDATEDATETIME                            AS med_start_date,
        cp.STOPDATEDATETIME                             AS med_end_date,
        cp.CREATEDDATETIME                              AS med_createddatetime,
        d.CREATEDDATETIME                               AS doc_createddatetime,
        cp.LASTDISPENSEDDATEDATETIME                    AS last_dispensed_date,
        cp.SAMPLEEXPIRATIONDATEDATETIME                 AS sample_expiration_date,
        cp.ADMINISTEREXPIRATIONDATEDATETIME             AS administer_expiration_date,
        cp.EARLIESTFILLDATEDATETIME                     AS earliest_fill_date,
        cp.NDC                                          AS med_code,
        COALESCE(fndc.LN60, cp.labelname, fdb.MED_MEDID_DESC, d.CLINICALORDERTYPE) AS med_name,
        CASE WHEN cp.NDC IS NOT NULL THEN 'NDC' ELSE NULL END AS med_coding_system,
        NULL                                            AS med_status,
        cp.DOSAGEFORM                                   AS med_formulation,
        NULL                                            AS med_route,
        cp.AVGDAILYDOSEQUANTITY                         AS med_strength,
        cp.AVGDAILYDOSEUNIT                             AS med_strength_unit,
        cp.FREQUENCY                                    AS med_frequency,
        cp.DOSAGEQUANTITY                               AS med_presc_quantity,
        cp.DURATION                                     AS med_days_supply,
        cp.NUMBERREFILLSALLOWED                         AS med_refills,
        cp.SIG                                          AS med_directions,
        cp.LASTFILLDATEDATETIME                         AS med_fill_date,
        NULL                                            AS med_fill_type,
        CURRENT_TIMESTAMP()                             AS created_datetime,
        'ND'                                            AS created_by,
        CURRENT_TIMESTAMP()                             AS updated_datetime,
        'ND'                                            AS updated_by,
        'athenaone'                                     AS ehr_source_name,
        'bronze_layer'                                  AS source_path,
        'Structured'                                    AS data_type,
        5                                               AS psid,
        cp.nd_extracted_date
    FROM CLINICALPRESCRIPTION cp
    INNER JOIN DOCUMENT d
        ON  cp.documentid      = d.documentid
        AND d.nd_active_flag   = 'Y'
    INNER JOIN CHART ch
        ON  d.CHARTID          = ch.CHARTID
        AND ch.nd_active_flag  = 'Y'
    LEFT JOIN CLINICALENCOUNTER ce
        ON  d.clinicalencounterid  = ce.clinicalencounterid
        AND ce.nd_active_flag      = 'Y'
    LEFT JOIN FDB_RNDC14 fndc
        ON  cp.NDC             = fndc.NDC
        AND fndc.nd_active_flag = 'Y'
    LEFT JOIN (
        SELECT *
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY FDB_RMIID1ID
                       ORDER BY LASTUPDATED DESC
                   ) AS rn
            FROM athenaone.FDB_RMIID1
        ) x
        WHERE rn = 1
    ) fdb ON d.fbdmedid = fdb.medid
    WHERE cp.nd_active_flag = 'Y'
    UNION ALL
    SELECT DISTINCT
        'patientmedication'                             AS source,
        MED.PATIENTMEDICATIONID                         AS med_id,
        MED.CHARTID                                     AS ndid,
        ch.ENTERPRISEID                                 AS patientid,
        CE.CLINICALENCOUNTERID                          AS eid,
        CE.ENCOUNTERDATE                                AS enc_date,
        NULL                                            AS written_date,
        MED.MEDADMINISTEREDDATETIME                     AS med_administered_datetime,
        DOC.ORDERDATETIME                               AS doc_orderdatetime,
        MED.startdate                                   AS med_start_date,
        MED.stopdate                                    AS med_end_date,
        MED.CREATEDDATETIME                             AS med_createddatetime,
        DOC.CREATEDDATETIME                             AS doc_createddatetime,
        MED.DISPENSEDEXPIRATIONDATE                     AS last_dispensed_date,
        NULL                                            AS sample_expiration_date,
        MED.ADMINISTEREDEXPIRATIONDATE                  AS administer_expiration_date,
        NULL                                            AS earliest_fill_date,
        CASE
            WHEN LOWER(TRIM(MED1.NDC)) = 'none' THEN NULL
            ELSE TRIM(MED1.NDC)
        END                                             AS med_code,
        COALESCE(
            NULLIF(TRIM(MED.MEDICATIONNAME),  'none'),
            NULLIF(TRIM(MED1.MEDICATIONNAME), 'none'),
            DOC.CLINICALORDERTYPE
        )                                               AS med_name,
        CASE WHEN MED1.NDC IS NOT NULL THEN 'NDC' ELSE NULL END AS med_coding_system,
        CASE
            WHEN MED.DEACTIVATIONDATETIME IS NULL THEN 'Active'
            ELSE 'Inactive'
        END                                             AS med_status,
        TRIM(MED.DOSAGEFORM)                            AS med_formulation,
        TRIM(MED.DOSAGEROUTE)                           AS med_route,
        TRIM(MED.DOSAGESTRENGTH)                        AS med_strength,
        TRIM(MED.DOSAGESTRENGTHUNITS)                   AS med_strength_unit,
        TRIM(MED.FREQUENCY)                             AS med_frequency,
        MED.PRESCRIPTIONFILLQUANTITY                    AS med_presc_quantity,
        MED.LENGTHOFCOURSE                              AS med_days_supply,
        REPLACE(MED.NUMBEROFREFILLSPRESCRIBED, '.0', '') AS med_refills,
        MED.sig                                         AS med_directions,
        MED.FILLDATE                                    AS med_fill_date,
        NULL                                            AS med_fill_type,
        CURRENT_TIMESTAMP()                             AS created_datetime,
        'ND'                                            AS created_by,
        CURRENT_TIMESTAMP()                             AS updated_datetime,
        'ND'                                            AS updated_by,
        'athenaone'                                     AS ehr_source_name,
        'bronze_layer'                                  AS source_path,
        'Structured'                                    AS data_type,
        5                                               AS psid,
        MED.nd_extracted_date
    FROM PATIENTMEDICATION MED
    LEFT JOIN MEDICATION MED1
        ON  REPLACE(MED.medicationid,  '.0', '') = REPLACE(MED1.medicationid, '.0', '')
        AND MED1.nd_active_flag = 'Y'
    LEFT JOIN DOCUMENT DOC
        ON  MED.DOCUMENTID     = DOC.DOCUMENTID
        AND DOC.nd_active_flag = 'Y'
    INNER JOIN CHART ch
        ON  MED.CHARTID        = ch.CHARTID
        AND ch.nd_active_flag  = 'Y'
    LEFT JOIN CLINICALENCOUNTER CE
        ON  DOC.CLINICALENCOUNTERID = CE.CLINICALENCOUNTERID
        AND CE.nd_active_flag       = 'Y'
    WHERE MED.nd_active_flag = 'Y'
)
SELECT
    source,
    med_id,
    ndid,
    -- enc_date: fallback chain using date_case for each VARCHAR date column
    CASE
        WHEN psid IN (2, 5, 6, 10) AND source = 'clinicalprescription' THEN
            COALESCE(
                enc_date,
                CASE WHEN med_administered_datetime IS NULL OR med_administered_datetime IN ('', 'None') THEN NULL
                     WHEN med_administered_datetime REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}$' THEN DATE(med_administered_datetime)
                     WHEN med_administered_datetime REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN STR_TO_DATE(med_administered_datetime, '%Y-%m-%d')
                     WHEN med_administered_datetime REGEXP '^[0-9]{2}/[0-9]{2}/[0-9]{4}$' THEN STR_TO_DATE(med_administered_datetime, '%m/%d/%Y')
                     WHEN med_administered_datetime REGEXP '^[0-9]{2}-[0-9]{2}-[0-9]{4}$' THEN STR_TO_DATE(med_administered_datetime, '%m-%d-%Y')
                     ELSE NULL END,
                CASE WHEN med_fill_date IS NULL OR med_fill_date IN ('', 'None') THEN NULL
                     WHEN med_fill_date REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}$' THEN DATE(med_fill_date)
                     WHEN med_fill_date REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN STR_TO_DATE(med_fill_date, '%Y-%m-%d')
                     WHEN med_fill_date REGEXP '^[0-9]{2}/[0-9]{2}/[0-9]{4}$' THEN STR_TO_DATE(med_fill_date, '%m/%d/%Y')
                     WHEN med_fill_date REGEXP '^[0-9]{2}-[0-9]{2}-[0-9]{4}$' THEN STR_TO_DATE(med_fill_date, '%m-%d-%Y')
                     ELSE NULL END,
                CASE WHEN med_start_date IS NULL OR med_start_date IN ('', 'None') THEN NULL
                     WHEN med_start_date REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}$' THEN DATE(med_start_date)
                     WHEN med_start_date REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN STR_TO_DATE(med_start_date, '%Y-%m-%d')
                     WHEN med_start_date REGEXP '^[0-9]{2}/[0-9]{2}/[0-9]{4}$' THEN STR_TO_DATE(med_start_date, '%m/%d/%Y')
                     WHEN med_start_date REGEXP '^[0-9]{2}-[0-9]{2}-[0-9]{4}$' THEN STR_TO_DATE(med_start_date, '%m-%d-%Y')
                     ELSE NULL END,
                CASE WHEN written_date IS NULL OR written_date IN ('', 'None') THEN NULL
                     WHEN written_date REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}$' THEN DATE(written_date)
                     WHEN written_date REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN STR_TO_DATE(written_date, '%Y-%m-%d')
                     WHEN written_date REGEXP '^[0-9]{2}/[0-9]{2}/[0-9]{4}$' THEN STR_TO_DATE(written_date, '%m/%d/%Y')
                     WHEN written_date REGEXP '^[0-9]{2}-[0-9]{2}-[0-9]{4}$' THEN STR_TO_DATE(written_date, '%m-%d-%Y')
                     ELSE NULL END,
                med_createddatetime,
                doc_createddatetime
            )
        WHEN psid IN (2, 5, 6, 10) AND source = 'patientmedication' THEN
            COALESCE(
                enc_date,
                CASE WHEN med_start_date IS NULL OR med_start_date IN ('', 'None') THEN NULL
                     WHEN med_start_date REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}$' THEN DATE(med_start_date)
                     WHEN med_start_date REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN STR_TO_DATE(med_start_date, '%Y-%m-%d')
                     WHEN med_start_date REGEXP '^[0-9]{2}/[0-9]{2}/[0-9]{4}$' THEN STR_TO_DATE(med_start_date, '%m/%d/%Y')
                     WHEN med_start_date REGEXP '^[0-9]{2}-[0-9]{2}-[0-9]{4}$' THEN STR_TO_DATE(med_start_date, '%m-%d-%Y')
                     ELSE NULL END,
                CASE WHEN med_administered_datetime IS NULL OR med_administered_datetime IN ('', 'None') THEN NULL
                     WHEN med_administered_datetime REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}$' THEN DATE(med_administered_datetime)
                     WHEN med_administered_datetime REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN STR_TO_DATE(med_administered_datetime, '%Y-%m-%d')
                     WHEN med_administered_datetime REGEXP '^[0-9]{2}/[0-9]{2}/[0-9]{4}$' THEN STR_TO_DATE(med_administered_datetime, '%m/%d/%Y')
                     WHEN med_administered_datetime REGEXP '^[0-9]{2}-[0-9]{2}-[0-9]{4}$' THEN STR_TO_DATE(med_administered_datetime, '%m-%d-%Y')
                     ELSE NULL END,
                CASE WHEN med_fill_date IS NULL OR med_fill_date IN ('', 'None') THEN NULL
                     WHEN med_fill_date REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}$' THEN DATE(med_fill_date)
                     WHEN med_fill_date REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN STR_TO_DATE(med_fill_date, '%Y-%m-%d')
                     WHEN med_fill_date REGEXP '^[0-9]{2}/[0-9]{2}/[0-9]{4}$' THEN STR_TO_DATE(med_fill_date, '%m/%d/%Y')
                     WHEN med_fill_date REGEXP '^[0-9]{2}-[0-9]{2}-[0-9]{4}$' THEN STR_TO_DATE(med_fill_date, '%m-%d-%Y')
                     ELSE NULL END,
                med_createddatetime,
                doc_createddatetime
            )
        ELSE enc_date
    END                                                             AS enc_date,
    eid,
    CASE WHEN written_date IS NULL OR written_date IN ('', 'None') THEN NULL
         WHEN written_date REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}$' THEN DATE(written_date)
         WHEN written_date REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN STR_TO_DATE(written_date, '%Y-%m-%d')
         WHEN written_date REGEXP '^[0-9]{2}/[0-9]{2}/[0-9]{4}$' THEN STR_TO_DATE(written_date, '%m/%d/%Y')
         WHEN written_date REGEXP '^[0-9]{2}-[0-9]{2}-[0-9]{4}$' THEN STR_TO_DATE(written_date, '%m-%d-%Y')
         ELSE NULL END                                              AS written_date,
    CASE WHEN med_administered_datetime IS NULL OR med_administered_datetime IN ('', 'None') THEN NULL
         WHEN med_administered_datetime REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}$' THEN DATE(med_administered_datetime)
         WHEN med_administered_datetime REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN STR_TO_DATE(med_administered_datetime, '%Y-%m-%d')
         WHEN med_administered_datetime REGEXP '^[0-9]{2}/[0-9]{2}/[0-9]{4}$' THEN STR_TO_DATE(med_administered_datetime, '%m/%d/%Y')
         WHEN med_administered_datetime REGEXP '^[0-9]{2}-[0-9]{2}-[0-9]{4}$' THEN STR_TO_DATE(med_administered_datetime, '%m-%d-%Y')
         ELSE NULL END                                              AS med_administered_datetime,
    CASE WHEN doc_orderdatetime IS NULL OR doc_orderdatetime IN ('', 'None') THEN NULL
         WHEN doc_orderdatetime REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}$' THEN DATE(doc_orderdatetime)
         WHEN doc_orderdatetime REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN STR_TO_DATE(doc_orderdatetime, '%Y-%m-%d')
         WHEN doc_orderdatetime REGEXP '^[0-9]{2}/[0-9]{2}/[0-9]{4}$' THEN STR_TO_DATE(doc_orderdatetime, '%m/%d/%Y')
         WHEN doc_orderdatetime REGEXP '^[0-9]{2}-[0-9]{2}-[0-9]{4}$' THEN STR_TO_DATE(doc_orderdatetime, '%m-%d-%Y')
         ELSE NULL END                                              AS doc_orderdatetime,
    CASE WHEN med_start_date IS NULL OR med_start_date IN ('', 'None') THEN NULL
         WHEN med_start_date REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}$' THEN DATE(med_start_date)
         WHEN med_start_date REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN STR_TO_DATE(med_start_date, '%Y-%m-%d')
         WHEN med_start_date REGEXP '^[0-9]{2}/[0-9]{2}/[0-9]{4}$' THEN STR_TO_DATE(med_start_date, '%m/%d/%Y')
         WHEN med_start_date REGEXP '^[0-9]{2}-[0-9]{2}-[0-9]{4}$' THEN STR_TO_DATE(med_start_date, '%m-%d-%Y')
         ELSE NULL END                                              AS med_start_date,
    CASE WHEN med_end_date IS NULL OR med_end_date IN ('', 'None') THEN NULL
         WHEN med_end_date REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}$' THEN DATE(med_end_date)
         WHEN med_end_date REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN STR_TO_DATE(med_end_date, '%Y-%m-%d')
         WHEN med_end_date REGEXP '^[0-9]{2}/[0-9]{2}/[0-9]{4}$' THEN STR_TO_DATE(med_end_date, '%m/%d/%Y')
         WHEN med_end_date REGEXP '^[0-9]{2}-[0-9]{2}-[0-9]{4}$' THEN STR_TO_DATE(med_end_date, '%m-%d-%Y')
         ELSE NULL END                                              AS med_end_date,
    med_createddatetime,
    doc_createddatetime,
    CASE WHEN last_dispensed_date IS NULL OR last_dispensed_date IN ('', 'None') THEN NULL
         WHEN last_dispensed_date REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}$' THEN DATE(last_dispensed_date)
         WHEN last_dispensed_date REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN STR_TO_DATE(last_dispensed_date, '%Y-%m-%d')
         WHEN last_dispensed_date REGEXP '^[0-9]{2}/[0-9]{2}/[0-9]{4}$' THEN STR_TO_DATE(last_dispensed_date, '%m/%d/%Y')
         WHEN last_dispensed_date REGEXP '^[0-9]{2}-[0-9]{2}-[0-9]{4}$' THEN STR_TO_DATE(last_dispensed_date, '%m-%d-%Y')
         ELSE NULL END                                              AS last_dispensed_date,
    CASE WHEN sample_expiration_date IS NULL OR sample_expiration_date IN ('', 'None') THEN NULL
         WHEN sample_expiration_date REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}$' THEN DATE(sample_expiration_date)
         WHEN sample_expiration_date REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN STR_TO_DATE(sample_expiration_date, '%Y-%m-%d')
         WHEN sample_expiration_date REGEXP '^[0-9]{2}/[0-9]{2}/[0-9]{4}$' THEN STR_TO_DATE(sample_expiration_date, '%m/%d/%Y')
         WHEN sample_expiration_date REGEXP '^[0-9]{2}-[0-9]{2}-[0-9]{4}$' THEN STR_TO_DATE(sample_expiration_date, '%m-%d-%Y')
         ELSE NULL END                                              AS sample_expiration_date,
    CASE WHEN administer_expiration_date IS NULL OR administer_expiration_date IN ('', 'None') THEN NULL
         WHEN administer_expiration_date REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}$' THEN DATE(administer_expiration_date)
         WHEN administer_expiration_date REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN STR_TO_DATE(administer_expiration_date, '%Y-%m-%d')
         WHEN administer_expiration_date REGEXP '^[0-9]{2}/[0-9]{2}/[0-9]{4}$' THEN STR_TO_DATE(administer_expiration_date, '%m/%d/%Y')
         WHEN administer_expiration_date REGEXP '^[0-9]{2}-[0-9]{2}-[0-9]{4}$' THEN STR_TO_DATE(administer_expiration_date, '%m-%d-%Y')
         ELSE NULL END                                              AS administer_expiration_date,
    CASE WHEN earliest_fill_date IS NULL OR earliest_fill_date IN ('', 'None') THEN NULL
         WHEN earliest_fill_date REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}$' THEN DATE(earliest_fill_date)
         WHEN earliest_fill_date REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN STR_TO_DATE(earliest_fill_date, '%Y-%m-%d')
         WHEN earliest_fill_date REGEXP '^[0-9]{2}/[0-9]{2}/[0-9]{4}$' THEN STR_TO_DATE(earliest_fill_date, '%m/%d/%Y')
         WHEN earliest_fill_date REGEXP '^[0-9]{2}-[0-9]{2}-[0-9]{4}$' THEN STR_TO_DATE(earliest_fill_date, '%m-%d-%Y')
         ELSE NULL END                                              AS earliest_fill_date,
    med_code,
    med_name,
    med_coding_system,
    med_status,
    ''   AS med_status_flag,
    ''   AS med_indication,
    med_formulation,
    med_route,
    med_strength,
    med_strength_unit,
    med_frequency,
    med_presc_quantity,
    med_days_supply,
    med_refills,
    med_directions,
    CASE WHEN med_fill_date IS NULL OR med_fill_date IN ('', 'None') THEN NULL
         WHEN med_fill_date REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}$' THEN DATE(med_fill_date)
         WHEN med_fill_date REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN STR_TO_DATE(med_fill_date, '%Y-%m-%d')
         WHEN med_fill_date REGEXP '^[0-9]{2}/[0-9]{2}/[0-9]{4}$' THEN STR_TO_DATE(med_fill_date, '%m/%d/%Y')
         WHEN med_fill_date REGEXP '^[0-9]{2}-[0-9]{2}-[0-9]{4}$' THEN STR_TO_DATE(med_fill_date, '%m-%d-%Y')
         ELSE NULL END                                              AS med_fill_date,
    med_fill_type,
    NULL AS discont_date,
    ''   AS discont_reason,
    created_datetime,
    created_by,
    ehr_source_name,
    source_path,
    data_type,
    psid,
    updated_datetime,
    updated_by,
    nd_extracted_date,
    MD5(CONCAT_WS(':',
        COALESCE(psid, ''), COALESCE(ndid, ''), COALESCE(eid, ''),
        COALESCE(enc_date, ''), COALESCE(med_start_date, ''), COALESCE(med_end_date, ''),
        COALESCE(med_code, ''), COALESCE(med_name, ''), COALESCE(med_id, '')
    )) AS udm_unq_id,
    -- enc_date_proxy: same as enc_date
    CASE
        WHEN psid IN (2, 5, 6, 10) AND source = 'clinicalprescription' THEN
            COALESCE(
                enc_date,
                CASE WHEN med_administered_datetime IS NULL OR med_administered_datetime IN ('', 'None') THEN NULL
                     WHEN med_administered_datetime REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}$' THEN DATE(med_administered_datetime)
                     WHEN med_administered_datetime REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN STR_TO_DATE(med_administered_datetime, '%Y-%m-%d')
                     WHEN med_administered_datetime REGEXP '^[0-9]{2}/[0-9]{2}/[0-9]{4}$' THEN STR_TO_DATE(med_administered_datetime, '%m/%d/%Y')
                     WHEN med_administered_datetime REGEXP '^[0-9]{2}-[0-9]{2}-[0-9]{4}$' THEN STR_TO_DATE(med_administered_datetime, '%m-%d-%Y')
                     ELSE NULL END,
                CASE WHEN med_fill_date IS NULL OR med_fill_date IN ('', 'None') THEN NULL
                     WHEN med_fill_date REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}$' THEN DATE(med_fill_date)
                     WHEN med_fill_date REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN STR_TO_DATE(med_fill_date, '%Y-%m-%d')
                     WHEN med_fill_date REGEXP '^[0-9]{2}/[0-9]{2}/[0-9]{4}$' THEN STR_TO_DATE(med_fill_date, '%m/%d/%Y')
                     WHEN med_fill_date REGEXP '^[0-9]{2}-[0-9]{2}-[0-9]{4}$' THEN STR_TO_DATE(med_fill_date, '%m-%d-%Y')
                     ELSE NULL END,
                CASE WHEN med_start_date IS NULL OR med_start_date IN ('', 'None') THEN NULL
                     WHEN med_start_date REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}$' THEN DATE(med_start_date)
                     WHEN med_start_date REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN STR_TO_DATE(med_start_date, '%Y-%m-%d')
                     WHEN med_start_date REGEXP '^[0-9]{2}/[0-9]{2}/[0-9]{4}$' THEN STR_TO_DATE(med_start_date, '%m/%d/%Y')
                     WHEN med_start_date REGEXP '^[0-9]{2}-[0-9]{2}-[0-9]{4}$' THEN STR_TO_DATE(med_start_date, '%m-%d-%Y')
                     ELSE NULL END,
                CASE WHEN written_date IS NULL OR written_date IN ('', 'None') THEN NULL
                     WHEN written_date REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}$' THEN DATE(written_date)
                     WHEN written_date REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN STR_TO_DATE(written_date, '%Y-%m-%d')
                     WHEN written_date REGEXP '^[0-9]{2}/[0-9]{2}/[0-9]{4}$' THEN STR_TO_DATE(written_date, '%m/%d/%Y')
                     WHEN written_date REGEXP '^[0-9]{2}-[0-9]{2}-[0-9]{4}$' THEN STR_TO_DATE(written_date, '%m-%d-%Y')
                     ELSE NULL END,
                med_createddatetime,
                doc_createddatetime
            )
        WHEN psid IN (2, 5, 6, 10) AND source = 'patientmedication' THEN
            COALESCE(
                enc_date,
                CASE WHEN med_start_date IS NULL OR med_start_date IN ('', 'None') THEN NULL
                     WHEN med_start_date REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}$' THEN DATE(med_start_date)
                     WHEN med_start_date REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN STR_TO_DATE(med_start_date, '%Y-%m-%d')
                     WHEN med_start_date REGEXP '^[0-9]{2}/[0-9]{2}/[0-9]{4}$' THEN STR_TO_DATE(med_start_date, '%m/%d/%Y')
                     WHEN med_start_date REGEXP '^[0-9]{2}-[0-9]{2}-[0-9]{4}$' THEN STR_TO_DATE(med_start_date, '%m-%d-%Y')
                     ELSE NULL END,
                CASE WHEN med_administered_datetime IS NULL OR med_administered_datetime IN ('', 'None') THEN NULL
                     WHEN med_administered_datetime REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}$' THEN DATE(med_administered_datetime)
                     WHEN med_administered_datetime REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN STR_TO_DATE(med_administered_datetime, '%Y-%m-%d')
                     WHEN med_administered_datetime REGEXP '^[0-9]{2}/[0-9]{2}/[0-9]{4}$' THEN STR_TO_DATE(med_administered_datetime, '%m/%d/%Y')
                     WHEN med_administered_datetime REGEXP '^[0-9]{2}-[0-9]{2}-[0-9]{4}$' THEN STR_TO_DATE(med_administered_datetime, '%m-%d-%Y')
                     ELSE NULL END,
                CASE WHEN med_fill_date IS NULL OR med_fill_date IN ('', 'None') THEN NULL
                     WHEN med_fill_date REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}$' THEN DATE(med_fill_date)
                     WHEN med_fill_date REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN STR_TO_DATE(med_fill_date, '%Y-%m-%d')
                     WHEN med_fill_date REGEXP '^[0-9]{2}/[0-9]{2}/[0-9]{4}$' THEN STR_TO_DATE(med_fill_date, '%m/%d/%Y')
                     WHEN med_fill_date REGEXP '^[0-9]{2}-[0-9]{2}-[0-9]{4}$' THEN STR_TO_DATE(med_fill_date, '%m-%d-%Y')
                     ELSE NULL END,
                med_createddatetime,
                doc_createddatetime
            )
        ELSE enc_date
    END AS enc_date_proxy
FROM medication_src;
