-- Appointment eCW (extended canonical structure)

SELECT
    enc.encounterID AS appointment_id,
    enc.patientID AS ndid,
    enc.encounterID AS encounter_id,
    enc.date AS encounter_date,

    /* appointment_created_date */
    NULL AS appointment_created_date,

    /* appointment_date */
    DATE(enc.date) AS appointment_date,

    /* appointment_start_time */
    TIME(enc.startTime) AS appointment_start_time,

    /* retained operational field */
    TIME(enc.endTime) AS appointment_end_time,

    /* appointment_duration */
    TIMESTAMPDIFF(
        MINUTE,
        enc.startTime,
        enc.endTime
    ) AS appointment_duration,

    /* appointment_status */
    enc.STATUS AS appointment_status,

    /* appointment_type */
    CASE 
        WHEN enc.encType = 1 THEN 'Office Visit'
        WHEN enc.encType = 2 THEN 'Tele/Virtual Visit'
        WHEN enc.encType = 3 THEN 'Out of Office'
        WHEN enc.encType = 4 THEN 'Claim'
        WHEN enc.encType = 5 THEN 'Lab'
        WHEN enc.encType = 6 THEN 'Web Encounter'
        WHEN enc.encType = 7 THEN 'ePrescription Refills'
        WHEN enc.encType = 8 THEN 'PTDASH'
        WHEN enc.encType = 9 THEN 'Orderset'
        ELSE NULL
    END AS appointment_type,

    /* appointment_name */
    COALESCE(
        vc.Description,
        enc.visitType
    ) AS appointment_name,

    /* provider */
    enc.doctorID AS provider_id,
    NULL AS provider_name,

    /* speciality */
    d.speciality AS doc_speciality,

    /* department */
    enc.deptid AS department_id,

    /* retained operational fields */
    enc.timeIn AS check_in_time,
    enc.timeOut AS check_out_time,

    COALESCE(
        enc.reason,
        ed.chiefComplaint
    ) AS appointment_reason,

    /* appointment_notes */
    enc.generalNotes AS appointment_notes,

    /* cancellation_flag */
    CASE 
        WHEN enc.deleteFlag = 1
          OR UPPER(enc.STATUS) IN (
                'CANCELLED',
                'CANCELED',
                'CX',
                'CAN'
             )
        THEN 1
        ELSE 0
    END AS cancellation_flag,

    /* cancellation_reason */
    ed.cancellationReason AS cancellation_reason,

    /* no_show_flag */
    CASE
        WHEN UPPER(enc.STATUS) IN (
                'N/S',
                'NOS',
                'NOSHOW',
                'NO SHOW',
                'N/S FEE',
                'NO-SHOW'
             )
        THEN 1
        ELSE 0
    END AS no_show_flag,

    /* reschedule_flag */
    CASE
        WHEN UPPER(enc.STATUS) IN (
                'R/S',
                'RES',
                'RESCHEDULED',
                'R/S NWN',
                'RESCH'
             )
        THEN 1
        ELSE 0
    END AS reschedule_flag,

    /* rescheduled_appt_id */
    NULL AS rescheduled_appt_id,

    /* retained operational field */
    NULL AS confirmation_status,

    /* insurance enrichment */
    NULL AS pat_insurance,
    NULL AS pat_insurance_type,

    /* referral enrichment */
    CASE
        WHEN r.referralid IS NOT NULL THEN 1
        ELSE 0
    END AS referral_flag,

    r.referralid AS referral_id,

    /* prior auth */
    r.authNo AS appointment_prior_auth_id,

    /* insurance */
    r.insid AS insurance_id,

    /* copay */
    enc.VisitCopay AS copay_amount,
    ed.copay AS copay_collected,

    /* telehealth */
    CASE
        WHEN enc.encType = 2 THEN 1
        ELSE 0
    END AS telehealth_flag,

    /* audit columns */
    CURRENT_TIMESTAMP() AS created_datetime,
    'ND' AS created_by,

    CURRENT_TIMESTAMP() AS updated_time,
    'ND' AS updated_by,

    /* source metadata */
    'eCW' AS ehr_source_name,
    'bronze_layer' AS source_path,
    'Structured' AS data_type,

    /* psid */
    1 AS psid

FROM enc enc

LEFT JOIN doctors d
    ON enc.doctorID = d.doctorID
   AND enc.nd_activeflag = 'Y'
   AND d.nd_activeflag = 'Y'

LEFT JOIN encounterdata ed
    ON enc.encounterID = ed.encounterID
   AND ed.nd_activeflag = 'Y'

LEFT JOIN visitcodes vc
    ON enc.visitType = vc.Name
   AND vc.nd_activeflag = 'Y'

LEFT JOIN referraldetail rd
    ON enc.encounterID = rd.encounterid
   AND rd.nd_activeflag = 'Y'

LEFT JOIN referral r
    ON rd.referralid = r.referralid
   AND r.deleteFlag = 0
   AND r.nd_activeflag = 'Y';