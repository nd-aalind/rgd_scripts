INSERT INTO rgd_udm_silver.medication (
source,med_id,ndid,eid,enc_date,written_date,med_administered_datetime,doc_orderdatetime,med_start_date,med_end_date,
med_createddatetime,doc_createddatetime,last_dispensed_date,sample_expiration_date,administer_expiration_date,
earliest_fill_date,med_code,med_name,med_coding_system,med_status,med_status_flag,med_indication,
med_formulation,med_route,med_strength,med_strength_unit,med_frequency,
med_pb_qty,med_days_supply,med_refills,med_directions,fill_date,med_fill_type,discont_date,discont_reason,
created_datetime,created_by,ehr_source_name,source_path,data_type,psid,updated_datetime,updated_by,
nd_extracted_date
)
 WITH medication_src AS (
    SELECT
        'clinicalprescription' AS source,
        cp.CLINICALPRESCRIPTIONID AS med_id,
        d.CHARTID AS ndid,
        ce.CLINICALENCOUNTERID AS eid,
        ce.ENCOUNTERDATE AS enc_date,
        cp.WRITTENDATEDATETIME AS written_date,
        cp.MEDICATIONADMINISTEREDDATETIME AS med_administered_datetime,
        d.ORDERDATETIME AS doc_orderdatetime,
        cp.STARTDATEDATETIME AS med_start_date,
        cp.STOPDATEDATETIME AS med_end_date,
        cp.CREATEDDATETIME AS med_createddatetime,
        d.CREATEDDATETIME AS doc_createddatetime,
        cp.LASTDISPENSEDDATEDATETIME AS last_dispensed_date,
        cp.SAMPLEEXPIRATIONDATEDATETIME AS sample_expiration_date,
        cp.ADMINISTEREXPIRATIONDATEDATETIME AS administer_expiration_date,
        cp.EARLIESTFILLDATEDATETIME AS earliest_fill_date,
        cp.NDC AS med_code,
        COALESCE(fndc.LN60, labelname, fdb.MED_MEDID_DESC, d.CLINICALORDERTYPE) AS med_name,
        CASE WHEN cp.NDC IS NOT NULL THEN 'NDC' ELSE NULL END AS med_coding_system,
        NULL AS med_status,
        cp.DOSAGEFORM AS med_formulation,
        NULL AS med_route,
        cp.AVGDAILYDOSEQUANTITY AS med_strength,
        cp.AVGDAILYDOSEUNIT AS med_strength_unit,
        cp.FREQUENCY AS med_frequency,
        cp.DOSAGEQUANTITY AS med_presc_quantity,
        cp.DURATION AS med_days_supply,
        cp.NUMBERREFILLSALLOWED AS med_refills,
        cp.SIG AS med_directions,
        cp.LASTFILLDATEDATETIME AS med_fill_date,
        NULL AS med_fill_type,
        CURRENT_TIMESTAMP() AS created_datetime,
        'ND' AS created_by,
        CURRENT_TIMESTAMP() AS updated_datetime,
        'ND' AS updated_by,
        'athenaone' AS ehr_source_name,
        'bronze_layer' AS source_path,
        'Structured' AS data_type,
        5 AS psid
        ,cp.nd_extracted_date
    FROM CLINICALPRESCRIPTION cp
    LEFT JOIN DOCUMENT d
        ON cp.documentid = d.documentid AND d.nd_active_flag = 'Y'
    LEFT JOIN CLINICALENCOUNTER ce
        ON d.clinicalencounterid = ce.clinicalencounterid AND ce.nd_active_flag = 'Y'
    LEFT JOIN FDB_RNDC14 fndc
        ON cp.NDC = fndc.NDC AND fndc.nd_active_flag = 'Y'
    LEFT JOIN (
        SELECT *
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY FDB_RMIID1ID ORDER BY LASTUPDATED DESC) rn
            FROM raleigh.FDB_RMIID1
        ) x
        WHERE rn = 1
    ) fdb ON d.fbdmedid = fdb.medid
    WHERE cp.nd_active_flag = 'Y'
    UNION ALL
    SELECT DISTINCT
        'patientmedication' AS source,
        MED.PATIENTMEDICATIONID AS med_id,
        MED.CHARTID AS ndid,
        CE.CLINICALENCOUNTERID AS eid,
        CE.ENCOUNTERDATE AS enc_date,
        NULL AS written_date,
        MED.MEDADMINISTEREDDATETIME AS med_administered_datetime,
        DOC.ORDERDATETIME AS doc_orderdatetime,
        MED.startdate AS med_start_date,
        MED.stopdate AS med_end_date,
        MED.CREATEDDATETIME AS med_createddatetime,
        DOC.CREATEDDATETIME AS doc_createddatetime,
        MED.DISPENSEDEXPIRATIONDATE AS last_dispensed_date,
        NULL AS sample_expiration_date,
        MED.ADMINISTEREDEXPIRATIONDATE AS administer_expiration_date,
        NULL AS earliest_fill_date,
        CASE WHEN LOWER(TRIM(MED1.NDC)) = 'none' THEN NULL ELSE TRIM(MED1.NDC) END AS med_code,
        COALESCE(
            NULLIF(TRIM(MED.MEDICATIONNAME), 'none'),
            NULLIF(TRIM(MED1.MEDICATIONNAME), 'none'),
            DOC.CLINICALORDERTYPE
        ) AS med_name,
        CASE WHEN MED1.NDC IS NOT NULL THEN 'NDC' ELSE NULL END AS med_coding_system,
        CASE WHEN MED.DEACTIVATIONDATETIME IS NULL THEN 'Active' ELSE 'Inactive' END AS med_status,
        TRIM(MED.DOSAGEFORM) AS med_formulation,
        TRIM(MED.DOSAGEROUTE) AS med_route,
        TRIM(MED.DOSAGESTRENGTH) AS med_strength,
        TRIM(MED.DOSAGESTRENGTHUNITS) AS med_strength_unit,
        TRIM(MED.FREQUENCY) AS med_frequency,
        MED.PRESCRIPTIONFILLQUANTITY AS med_presc_quantity,
        MED.LENGTHOFCOURSE AS med_days_supply,
        REPLACE(MED.NUMBEROFREFILLSPRESCRIBED, '.0', '') AS med_refills,
        MED.sig AS med_directions,
        MED.FILLDATE AS med_fill_date,
        NULL AS med_fill_type,
        CURRENT_TIMESTAMP() AS created_datetime,
        'ND' AS created_by,
        CURRENT_TIMESTAMP() AS updated_datetime,
        'ND' AS updated_by,
        'athenaone' AS ehr_source_name,
        'bronze_layer' AS source_path,
        'Structured' AS data_type,
        5 AS psid
        ,MED.nd_extracted_date
    FROM PATIENTMEDICATION MED
    LEFT JOIN MEDICATION MED1
        ON REPLACE(MED.medicationid, '.0', '') = REPLACE(MED1.medicationid, '.0', '')
       AND MED1.nd_active_flag = 'Y'
    LEFT JOIN DOCUMENT DOC
        ON MED.DOCUMENTID = DOC.DOCUMENTID AND DOC.nd_active_flag = 'Y'
    LEFT JOIN CLINICALENCOUNTER CE
        ON DOC.CLINICALENCOUNTERID = CE.CLINICALENCOUNTERID AND CE.nd_active_flag = 'Y'
    WHERE MED.nd_active_flag = 'Y'
 )
SELECT
    source, med_id, ndid, eid, enc_date,
    STR_TO_DATE(written_date,'%Y-%m-%d %H:%i:%s'),
    STR_TO_DATE(med_administered_datetime,'%Y-%m-%d %H:%i:%s'),
    STR_TO_DATE(doc_orderdatetime,'%Y-%m-%d %H:%i:%s'),
    STR_TO_DATE(med_start_date,'%Y-%m-%d %H:%i:%s'),
    STR_TO_DATE(med_end_date,'%Y-%m-%d %H:%i:%s'),
    med_createddatetime, doc_createddatetime,
    STR_TO_DATE(last_dispensed_date,'%Y-%m-%d %H:%i:%s'),
    STR_TO_DATE(sample_expiration_date,'%Y-%m-%d %H:%i:%s'),
    STR_TO_DATE(administer_expiration_date,'%Y-%m-%d %H:%i:%s'),
    STR_TO_DATE(earliest_fill_date,'%Y-%m-%d %H:%i:%s'),
    med_code, med_name, med_coding_system, med_status,
    '' AS med_status_flag, '' AS med_indication,
    med_formulation, med_route,
    med_strength, med_strength_unit, med_frequency,
    med_presc_quantity, med_days_supply, med_refills, med_directions,
    STR_TO_DATE(med_fill_date,'%Y-%m-%d %H:%i:%s'),
    med_fill_type, NULL as discont_date, '' as discont_reason,
    created_datetime, created_by, 
    ehr_source_name, source_path, data_type, psid,updated_datetime, updated_by,nd_extracted_date
FROM medication_src ;