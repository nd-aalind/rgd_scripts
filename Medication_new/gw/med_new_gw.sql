INSERT INTO suven.medication (
    source, med_id, ndid, enc_date, eid,
    written_date, med_administered_datetime, doc_orderdatetime,
    med_start_date, med_end_date, med_createddatetime, doc_createddatetime,
    last_dispensed_date, sample_expiration_date, administer_expiration_date,
    earliest_fill_date, med_code, med_name, med_coding_system,
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
    'clinicalpatmed'                                                AS source,
    m.ClinicalPatMedID                                             AS med_id,
    m.PatientID                                                    AS ndid,
    DATE(v.FromDateTime)                                           AS enc_date,
    COALESCE(c.VisitID, m.VisitID)                                 AS eid,
    NULL                                                           AS written_date,
    NULL                                                           AS med_administered_datetime,
    NULL                                                           AS doc_orderdatetime,
    -- med_start_date: prefer m.StartDT, fall back to erx.StartDate with safe parsing
    CASE
        WHEN m.StartDT IS NOT NULL
            THEN DATE(m.StartDT)
        WHEN erx.StartDate IS NULL
            THEN NULL
        WHEN TRIM(erx.StartDate) IN ('', 'None', 'NONE', '0000-00-00 00:00:00')
            THEN NULL
        WHEN STR_TO_DATE(erx.StartDate, '%Y-%m-%d %H:%i:%s') IS NOT NULL
            THEN DATE(STR_TO_DATE(erx.StartDate, '%Y-%m-%d %H:%i:%s'))
        WHEN STR_TO_DATE(erx.StartDate, '%m-%d-%Y %H:%i:%s') IS NOT NULL
            THEN DATE(STR_TO_DATE(erx.StartDate, '%m-%d-%Y %H:%i:%s'))
        ELSE NULL
    END                                                            AS med_start_date,
    -- med_end_date: safe parsing of erx.StopDate
    CASE
        WHEN erx.StopDate IS NULL
            THEN NULL
        WHEN TRIM(erx.StopDate) IN ('', 'None', 'NONE', '0000-00-00 00:00:00')
            THEN NULL
        WHEN STR_TO_DATE(erx.StopDate, '%Y-%m-%d %H:%i:%s') IS NOT NULL
            THEN DATE(STR_TO_DATE(erx.StopDate, '%Y-%m-%d %H:%i:%s'))
        WHEN STR_TO_DATE(erx.StopDate, '%m-%d-%Y %H:%i:%s') IS NOT NULL
            THEN DATE(STR_TO_DATE(erx.StopDate, '%m-%d-%Y %H:%i:%s'))
        ELSE NULL
    END                                                            AS med_end_date,
    m.CreateDate                                                   AS med_createddatetime,
    NULL                                                           AS doc_createddatetime,
    NULL                                                           AS last_dispensed_date,
    NULL                                                           AS sample_expiration_date,
    NULL                                                           AS administer_expiration_date,
    NULL                                                           AS earliest_fill_date,
    b.RXNORM                                                       AS med_code,
    COALESCE(m.MedicationName, b.RXNORMDISPLAY)                    AS med_name,
    NULL                                                           AS med_coding_system,
    NULL                                                           AS med_status,
    NULL                                                           AS med_status_flag,
    COALESCE(ifdb.FDBMedicalConditionDesc, ia.ICDDesc, ipl.PatHistItemICDDesc)
                                                                   AS med_indication,
    NULL                                                           AS med_formulation,
    COALESCE(m.Route, erx.SigRoute)                                AS med_route,
    COALESCE(
        NULLIF(CASE WHEN TRIM(LOWER(m.MedicationStrength))  IN ('none', '') THEN '' ELSE TRIM(m.MedicationStrength)  END, ''),
        NULLIF(CASE WHEN TRIM(LOWER(erx.DrugStrength))      IN ('none', '') THEN '' ELSE TRIM(erx.DrugStrength)      END, '')
    )                                                              AS med_strength,
    CASE
        WHEN TRIM(LOWER(m.MedicationStrengthUnit)) = 'none'
             OR TRIM(m.MedicationStrengthUnit) = ''
        THEN NULL
        ELSE TRIM(m.MedicationStrengthUnit)
    END                                                            AS med_strength_unit,
    m.FrequencyCode                                                AS med_frequency,
    COALESCE(
        CONCAT(m.DispenseAmount, ' ', m.DispenseUnit),
        CONCAT(erx.SigQuantity,  ' ', erx.SigQuantityUnit)
    )                                                              AS med_presc_quantity,
    m.Duration                                                     AS med_days_supply,
    COALESCE(m.NumRefills, erx.Refills)                            AS med_refills,
    m.SIG COLLATE utf8mb4_general_ci                               AS med_directions,
    f.StatusDate                                                   AS med_fill_date,
    fs.StatusDescription                                           AS med_fill_type,
    md.DiscontinuedDate                                            AS discont_date,
    dcr.Description                                                AS discont_reason,
    CURRENT_TIMESTAMP()                                            AS created_datetime,
    'ND'                                                           AS created_by,
    CURRENT_TIMESTAMP()                                            AS updated_datetime,
    'ND'                                                           AS updated_by,
    'Greenway'                                                     AS ehr_source_name,
    'bronze_table'                                                 AS source_path,
    'Structured'                                                   AS data_type,
    9                                                              AS psid,  -- 9=Mind, 11=JWM, etc.
    m.nd_extracted_date,
    MD5(CONCAT_WS(':',
        COALESCE(9,                                                ''),
        COALESCE(m.PatientID,                                      ''),
        COALESCE(COALESCE(c.VisitID, m.VisitID),                   ''),
        COALESCE(DATE(v.FromDateTime),                             ''),
        COALESCE(
            CASE
                WHEN m.StartDT IS NOT NULL THEN DATE(m.StartDT)
                WHEN erx.StartDate IS NULL THEN NULL
                WHEN TRIM(erx.StartDate) IN ('', 'None', 'NONE', '0000-00-00 00:00:00') THEN NULL
                WHEN STR_TO_DATE(erx.StartDate, '%Y-%m-%d %H:%i:%s') IS NOT NULL THEN DATE(STR_TO_DATE(erx.StartDate, '%Y-%m-%d %H:%i:%s'))
                WHEN STR_TO_DATE(erx.StartDate, '%m-%d-%Y %H:%i:%s') IS NOT NULL THEN DATE(STR_TO_DATE(erx.StartDate, '%m-%d-%Y %H:%i:%s'))
                ELSE NULL
            END, ''),
        COALESCE(
            CASE
                WHEN erx.StopDate IS NULL THEN NULL
                WHEN TRIM(erx.StopDate) IN ('', 'None', 'NONE', '0000-00-00 00:00:00') THEN NULL
                WHEN STR_TO_DATE(erx.StopDate, '%Y-%m-%d %H:%i:%s') IS NOT NULL THEN DATE(STR_TO_DATE(erx.StopDate, '%Y-%m-%d %H:%i:%s'))
                WHEN STR_TO_DATE(erx.StopDate, '%m-%d-%Y %H:%i:%s') IS NOT NULL THEN DATE(STR_TO_DATE(erx.StopDate, '%m-%d-%Y %H:%i:%s'))
                ELSE NULL
            END, ''),
        COALESCE(b.RXNORM,                                         ''),
        COALESCE(COALESCE(m.MedicationName, b.RXNORMDISPLAY),      '')
    ))                                                             AS udm_unq_id,
    COALESCE(
        DATE(v.FromDateTime),
        CASE
            WHEN m.StartDT IS NOT NULL THEN DATE(m.StartDT)
            WHEN erx.StartDate IS NULL THEN NULL
            WHEN TRIM(erx.StartDate) IN ('', 'None', 'NONE', '0000-00-00 00:00:00') THEN NULL
            WHEN STR_TO_DATE(erx.StartDate, '%Y-%m-%d %H:%i:%s') IS NOT NULL THEN DATE(STR_TO_DATE(erx.StartDate, '%Y-%m-%d %H:%i:%s'))
            WHEN STR_TO_DATE(erx.StartDate, '%m-%d-%Y %H:%i:%s') IS NOT NULL THEN DATE(STR_TO_DATE(erx.StartDate, '%m-%d-%Y %H:%i:%s'))
            ELSE NULL
        END,
        f.StatusDate,
        m.CreateDate
    )                                                              AS enc_date_proxy
FROM ClinicalPatMeds m
LEFT JOIN ClinicalDocuments c
    ON  c.DocumentID  = m.OrderingDocumentID
    AND c.PatientID   = m.PatientID
    AND c.nd_activeflag = 'Y'
LEFT JOIN Visit v
    ON  v.VisitID     = COALESCE(c.VisitID, m.VisitID)
    AND v.nd_activeflag = 'Y'
LEFT JOIN (
    SELECT Medid, MIN(RxNorm) AS RXNORM, MIN(RxNormDisplay) AS RXNORMDISPLAY
    FROM MedicationRxNormFact
    WHERE nd_activeflag = 'Y'
    GROUP BY Medid
) b ON b.Medid = m.MedicationID
LEFT JOIN ERXPatMedList erx
    ON  erx.ClinicalPatMedID = m.ClinicalPatMedID
    AND erx.nd_activeflag    = 'Y'
LEFT JOIN ClinicalPatMedsDiscontinued md
    ON  md.ClinicalPatMedID  = m.ClinicalPatMedID
    AND md.nd_activeflag     = 'Y'
LEFT JOIN ClinicalDiscontinuedReason dcr
    ON  dcr.DiscontinuedReasonid = md.DiscontinuedReasonID
    AND dcr.nd_activeflag        = 'Y'
LEFT JOIN ClinicalPatMedsFillStatus f
    ON  f.ClinicalPatMedID   = m.ClinicalPatMedID
    AND f.nd_activeflag      = 'Y'
LEFT JOIN RXFillStatus fs
    ON  fs.RXFillStatusID    = f.RxFillStatusID
    AND fs.nd_activeflag     = 'Y'
LEFT JOIN (
    SELECT DISTINCT ClinicalPatMedID, FDBMedicalConditionDesc
    FROM MedicationIndicationFDB
    WHERE FDBMedicalConditionDesc IS NOT NULL
      AND nd_activeflag = 'Y'
) ifdb ON ifdb.ClinicalPatMedID = m.ClinicalPatMedID
LEFT JOIN (
    SELECT DISTINCT ClinicalPatMedID, ICDDesc
    FROM MedicationIndicationAssessment
    WHERE ICDDesc IS NOT NULL
      AND nd_activeflag = 'Y'
) ia ON ia.ClinicalPatMedID = m.ClinicalPatMedID
LEFT JOIN (
    SELECT DISTINCT ClinicalPatMedID, PatHistItemICDDesc
    FROM MedicationIndicationProblemList
    WHERE PatHistItemICDDesc IS NOT NULL
      AND nd_activeflag = 'Y'
) ipl ON ipl.ClinicalPatMedID = m.ClinicalPatMedID
WHERE m.nd_activeflag = 'Y';
