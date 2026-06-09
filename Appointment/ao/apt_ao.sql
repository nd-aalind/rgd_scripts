    -- Appointment Athenaone --


SELECT
    a.APPOINTMENT_ID AS appointment_id,
    a.PATIENT_ID AS ndid,
    ce.CLINICALENCOUNTERID AS encounter_id,
    ce.ENCOUNTERDATE AS encounter_date,
    /* created date */
    a.SCHEDULED_TIMESTAMP AS appointment_created_date,
    a.APPOINTMENT_DATE AS appointment_date,
    a.APPOINTMENT_START_TIME AS appointment_start_time,
    a.BOOKED_APPOINTMENT_DURATION AS appointment_duration,
    /* appointment_status */
    CASE 
        WHEN a.STATUS_TYPE = 'f' THEN 'Scheduled'
        WHEN a.STATUS_TYPE = '2' THEN 'Checked In'
        WHEN a.STATUS_TYPE IN ('3','4') THEN 'Completed'
        WHEN a.STATUS_TYPE = 'x' THEN 'Cancelled'
        WHEN a.STATUS_TYPE = 'o' THEN 'Open'
        ELSE NULL
    END AS appointment_status,
    /* appointment_type */
    COALESCE(apt.APPOINTMENTTYPECLASS, ce.CLINICALENCOUNTERTYPE) AS appointment_type,
    /* appointment_name */
    COALESCE(
        a.BOOKED_APPOINTMENT_NAME,
        apt.APPOINTMENTTYPENAME
    ) AS appointment_name,
    a.RENDERING_PROVIDER_ID AS provider_id,
    null AS provider_name,
    a.RENDERING_PROVIDER_SPECIALTY_TYPE as doc_speciality,
    a.DEPARTMENT_ID AS department_id,
    a.APPOINTMENT_NOTE AS appointment_notes,
    /* cancellation flag */
    CASE WHEN a.STATUS_TYPE = 'x' THEN 1 ELSE 0 END AS Cancellation_flag,
    /* cancellation_reason */
    COALESCE(a.CANCELLED_REASON_LOCAL_NAME, acr.NAME) AS cancellation_reason,
    /* no_show_flag */
    CASE 
        WHEN a.FULL_NO_SHOW_COUNT_INDICATOR = 1 
          OR a.NON_RESCHED_NO_SHOW_COUNT_INDICATOR = 1 THEN 1
        WHEN acr.NOSHOWYN = 'Y' THEN 1
        ELSE 0
    END AS no_show_flag,
    /* reschedule_flag */
    CASE WHEN a.RESCHEDULED_COUNT_INDICATOR = 1 THEN 1
         WHEN acr.PATIENTRESCHEDULEDYN = 'Y' THEN 1
         ELSE 0 END AS reschedule_flag,
    a.RESCHEDULED_APPOINTMENT_ID AS Rescheduled_appt_id,
    /* Insurance_details */
    a.patient_insurance_category_type as pat_insurance,
    a.patient_insurance_grouping_type as pat_isurance_type,
    /* ===== Referral  ===== */
    CASE WHEN ral.REFERRALID IS NOT NULL THEN 1 ELSE 0 END AS referral_flag,
    ral.REFERRALID AS referral_id,
    /* ===== Authorization  ===== */
    ra.REFERRALAUTHNUMBER AS appointment_prior_auth_id,
    ra.PATIENTINSURANCEID AS insurance_id,
    /* copay */
    aei.COPAYAMOUNT AS copay_amount,
    aei.COPAYAMOUNTCOLLECTED AS copay_collected,
    /* telehealth */
    a.TELEHEALTH_APPOINTMENT_INDICATOR AS telehealth_flag,
        CURRENT_TIMESTAMP() AS created_datetime,
        'ND' AS created_by,
        CURRENT_TIMESTAMP() AS updated_time,
        'ND' AS updated_by,
        -- '{{ params.ehr_source_name }}' AS ehr_source_name,
        'Athenone' AS ehr_source_name,
        'bronze_layer' AS source_path,
        'Structured' AS data_type,
        '10' AS psid
        -- nsp.provider_specialty,
        -- nsp.provider_subspecialty
       -- {{ params.psid }} AS psid
FROM APPOINTMENT a
join PATIENT p
    on a.patient_id=p.ENTERPRISEID
    and p.nd_active_flag='Y'
LEFT JOIN CLINICALENCOUNTER ce
    ON a.APPOINTMENT_ID = ce.APPOINTMENTID
    and ce.nd_active_flag='Y'
LEFT JOIN APPOINTMENTTYPE apt
    ON a.BOOKED_APPOINTMENT_TYPE_ID = apt.APPOINTMENTTYPEID
LEFT JOIN APPOINTMENTCANCELREASON acr
    ON a.CANCELLED_REASON_LOCAL_NAME = acr.NAME
LEFT JOIN APPOINTMENTELIGIBILITYINFO aei
    ON a.APPOINTMENT_ID = aei.APPOINTMENTID
    and aei.nd_active_flag='Y'
/* referral link */
LEFT JOIN REFERRALAPPOINTMENTLINK ral
    ON a.APPOINTMENT_ID = ral.APPOINTMENTID
    AND ral.DELETEDDATETIME IS NULL
/* referral authorization */
LEFT JOIN REFERRALAUTHORIZATION ra
    ON ral.REFERRALID = ra.REFERRALAUTHID
    AND ra.DELETEDDATETIME IS NULL
left join PROVIDER pr 
on pr.PROVIDERID = a.RENDERING_PROVIDER_ID 
and pr.nd_active_flag = 'Y'
-- left join semantics.npi_specialty_mapping nsp 
-- on nsp.NPI = a.RENDERING_PROVIDER_NPI_ID
where a.nd_active_flag='Y';