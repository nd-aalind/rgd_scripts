---- AthenaHealth Practice Appointment ---

        SELECT
    a.AppointmentsId AS appointment_id,
    pv.PatientProfileId AS ndid,
    a.PatientVisitId AS encounter_id,
    pv.Visit AS encounter_date,
    a.Created AS appointment_created_date,
    DATE(a.ApptStart) AS appointment_date,
    a.ApptStart AS appointment_start_time,
    a.ApptStop AS appointment_end_time,
    a.Duration AS appointment_duration,
    a.Status AS appointment_status,
    a.Type as appointment_type,
    apt.Name AS appointment_name,
    a.DoctorId AS provider_id,
    null AS provider_name,
    a.Notes AS appointment_notes,
    /* Cancellation_flag */
    a.Canceled as Cancellation_flag,
    null as cancellation_reason,
    /* no_show_flag */
    CASE 
        WHEN LOWER(a.Status) = 'no show' THEN 1 
        ELSE 0 
    END AS no_show_flag,
    null as reschedule_flag,
    /* prior auth */
    null as Rescheduled_appt_id,
    null as pat_insurance,
    null as pat_insurance_type,
    /* referral_flag */
    CASE 
        WHEN pv.ReferringDoctorId IS NOT NULL THEN 1
        ELSE 0
    END AS referral_flag,
    /* ===== Authorization  ===== */
    a.PriorAuthorizationNumber AS appointment_prior_auth_id,
    pv.PrimaryInsuranceCarriersId AS insurance_id,
    /* copay */
    null AS copay_amount,
    null AS copay_collected,
    /* telehealth_flag */
    CASE 
        WHEN a.VovMeetingId IS NOT NULL THEN 1
        ELSE 0
    END AS telehealth_flag,
    CURRENT_TIMESTAMP() AS created_datetime,
    'ND' AS created_by,
    'Athena Practice' AS ehr_source_name,
    'bronze_layer' AS source_path,
    'Structured' AS data_type,
    '7' as psid
FROM Appointments a
LEFT JOIN PatientVisit pv 
    ON a.PatientVisitId = pv.PatientVisitId and a.nd_activeflag = 'Y' and pv.nd_activeflag = 'Y'
LEFT JOIN ApptType apt 
    ON a.ApptTypeId = apt.ApptTypeId and apt.nd_activeflag = 'Y'
LEFT JOIN DoctorFacility df
    ON a.DoctorId = df.PVId and df.nd_activeflag = 'Y';