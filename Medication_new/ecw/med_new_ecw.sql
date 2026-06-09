-- ── Phase 1: Materialize oldrxdetail pivot (GROUP BY oldrxid only) ───────────
-- Collapsed across dates to produce one row per oldrxid, preventing duplicate
-- JOIN rows in the main INSERT.
INSERT INTO fcn_latest.oldrxdetail_pivot
(
    oldrxid,
    med_formulation,
    med_frequency,
    med_strength,
    med_pb_qty,
    med_days_supply,
    med_refills,
    med_route,
    nd_extracted_date
)
SELECT
    ord.oldrxid,
    MAX(CASE WHEN prop.name = 'Formulation'       THEN ord.value END) AS med_formulation,
    MAX(CASE WHEN prop.name = 'Frequency'         THEN ord.value END) AS med_frequency,
    MAX(CASE WHEN prop.name = 'Size'              THEN ord.value END) AS med_strength,
    MAX(CASE WHEN prop.name IN ('Take','Amount')  THEN ord.value END) AS med_pb_qty,
    MAX(CASE WHEN prop.name = 'Duration'          THEN ord.value END) AS med_days_supply,
    SUM(CASE WHEN prop.name = 'Refills'           THEN ord.value END) AS med_refills,
    MAX(CASE WHEN prop.name = 'Route'             THEN ord.value END) AS med_route,
    MAX(DATE(ord.nd_extracted_date))                                   AS nd_extracted_date
FROM fcn_latest.oldrxdetail ord
JOIN fcn_latest.properties prop
    ON prop.propid = ord.prop
GROUP BY ord.oldrxid;


-- ── Phase 2: Main INSERT into destination table ───────────────────────────────
INSERT INTO suven.medication (
    source, med_id, ndid, enc_date, eid,
    written_date, med_administered_datetime, doc_orderdatetime,
    med_start_date, med_end_date,
    med_createddatetime, doc_createddatetime, last_dispensed_date,
    sample_expiration_date, administer_expiration_date, earliest_fill_date,
    med_code, med_name, med_coding_system,
    med_status, med_status_flag, med_indication,
    med_formulation, med_route, med_strength, med_strength_unit,
    med_frequency, med_presc_quantity, med_days_supply, med_refills,
    med_directions, med_fill_date, med_fill_type,
    discont_date, discont_reason,
    created_datetime, created_by, updated_datetime, updated_by,
    ehr_source_name, source_path, data_type, psid, nd_extracted_date,
    udm_unq_id, enc_date_proxy
)
SELECT
    'oldrxmain'                                                         AS source,
    b.oldrxid                                                           AS med_id,
    e.patientid                                                         AS ndid,
    DATE(e.date)                                                        AS enc_date,
    e.encounterid                                                       AS eid,
    NULL                                                                AS written_date,
    NULL                                                                AS med_administered_datetime,
    NULL                                                                AS doc_orderdatetime,
    CASE WHEN CAST(b.startdate AS CHAR) = 'None' THEN NULL
         WHEN YEAR(b.startdate) < 1991 THEN NULL
         ELSE DATE(LEFT(b.startdate, 10)) END                           AS med_start_date,
    CASE WHEN CAST(b.stopdate AS CHAR) = 'None' THEN NULL
         WHEN YEAR(b.stopdate) < 1991 THEN NULL
         ELSE DATE(LEFT(b.stopdate, 10)) END                            AS med_end_date,
    NULL                                                                AS med_createddatetime,
    NULL                                                                AS doc_createddatetime,
    NULL                                                                AS last_dispensed_date,
    NULL                                                                AS sample_expiration_date,
    NULL                                                                AS administer_expiration_date,
    NULL                                                                AS earliest_fill_date,
    b.ndc_code                                                          AS med_code,
    COALESCE(c.drugname, i.itemname)                                    AS med_name,
    CASE
        WHEN c.ndc     IS NOT NULL THEN 'NDC'
        WHEN i.keyname IS NOT NULL THEN i.keyname
        ELSE NULL
    END                                                                 AS med_coding_system,
    CASE
        WHEN d.rxcomment IN ('Taking','Takes','Start','Continue')
            THEN 'Taking'
        WHEN d.rxcomment IN ('Stop','Not-Taking','Discontinued','Discontinue',
                             'cancel','Cancelled','cancell','D/C by patient',
                             'D/C by another provider')
            THEN 'Not Taking'
        WHEN d.rxcomment IN ('Refill','Sample/Refill')
            THEN 'Refill'
        WHEN d.rxcomment IN ('Once')
            THEN 'Stat'
        WHEN d.rxcomment IN ('never started')
            THEN 'Never Started'
        WHEN d.rxcomment IN ('Ins Not Covered, Med chg')
            THEN 'Ins Not Covered, Med chg'
        WHEN d.rxcomment IN ('Error','Entered in error:')
            THEN 'Errors'
        WHEN d.rxcomment IN ('Awaiting ins. approval:')
            THEN 'Yet to start'
        ELSE NULL
    END                                                                 AS med_status,
    ''                                                                  AS med_status_flag,
    ''                                                                  AS med_indication,
    ord.med_formulation,
    ord.med_route,
    ord.med_strength,
    NULL                                                                AS med_strength_unit,
    ord.med_frequency,
    ord.med_pb_qty                                                      AS med_presc_quantity,
    ord.med_days_supply,
    ord.med_refills,
    COALESCE(NULLIF(TRIM(ora.AdditionalInstructions), ''), ora.rxnotes) AS med_directions,
    b.FillDate                                                          AS med_fill_date,
    NULL                                                                AS med_fill_type,
    NULL                                                                AS discont_date,
    ''                                                                  AS discont_reason,
    CURRENT_TIMESTAMP()                                                 AS created_datetime,
    'ND'                                                                AS created_by,
    CURRENT_TIMESTAMP()                                                 AS updated_datetime,
    'ND'                                                                AS updated_by,
    'eCW'                                                               AS ehr_source_name,
    'bronze_table'                                                      AS source_path,
    'Structured'                                                        AS data_type,
    8                                                                   AS psid,
    DATE(b.nd_extracted_date)                                           AS nd_extracted_date,
    MD5(CONCAT_WS(':',
        COALESCE(8,                                ''),
        COALESCE(e.patientid,                      ''),
        COALESCE(e.encounterid,                    ''),
        COALESCE(DATE(e.date),                     ''),
        COALESCE(CASE WHEN CAST(b.startdate AS CHAR) = 'None' THEN NULL WHEN YEAR(b.startdate) < 1991 THEN NULL ELSE DATE(LEFT(b.startdate,10)) END, ''),
        COALESCE(CASE WHEN CAST(b.stopdate  AS CHAR) = 'None' THEN NULL WHEN YEAR(b.stopdate)  < 1991 THEN NULL ELSE DATE(LEFT(b.stopdate, 10)) END, ''),
        COALESCE(b.ndc_code,                       ''),
        COALESCE(COALESCE(c.drugname, i.itemname), ''),
        COALESCE(b.oldrxid,                        '')
    ))                                                                  AS udm_unq_id,
    COALESCE(
        DATE(e.date),
        CASE WHEN CAST(b.startdate AS CHAR) = 'None' THEN NULL WHEN YEAR(b.startdate) < 1991 THEN NULL ELSE DATE(LEFT(b.startdate,10)) END,
        b.FillDate
    )                                                                   AS enc_date_proxy
FROM fcn_latest.enc e
INNER JOIN fcn_latest.oldrxmain b
    ON  e.encounterid                  = b.encounterid
    AND COALESCE(e.nd_ActiveFlag, 'Y') = 'Y'
    AND COALESCE(b.nd_ActiveFlag, 'Y') = 'Y'
JOIN oldrxdetail_pivot ord
    ON  ord.oldrxid = b.oldrxid
LEFT JOIN fcn_latest.oldrxmain_addlinfo ora
    ON  ora.oldrxid       = b.oldrxid
    AND ora.nd_ActiveFlag = 'Y'
LEFT JOIN fcn_latest.ndclookupenteries c
    ON  c.ndc                          = b.ndc_code
    AND COALESCE(c.nd_ActiveFlag, 'Y') = 'Y'
LEFT JOIN fcn_latest.items i
    ON  i.itemid                       = b.itemid
    AND COALESCE(i.nd_ActiveFlag, 'Y') = 'Y'
LEFT JOIN fcn_latest.rx_medication_alert d
    ON  d.encounterid                  = b.encounterid
    AND d.itemid                       = b.itemid
    AND COALESCE(d.nd_ActiveFlag, 'Y') = 'Y'
GROUP BY
    b.oldrxid,
    e.patientid, e.encounterid, e.date,
    b.startdate, b.stopdate, b.ndc_code, b.FillDate, b.nd_extracted_date,
    c.drugname, i.itemname, c.ndc, i.keyname,
    d.rxcomment,
    ora.AdditionalInstructions, ora.rxnotes,
    ord.med_formulation, ord.med_strength, ord.med_pb_qty,
    ord.med_days_supply, ord.med_refills, ord.med_route, ord.med_frequency;
