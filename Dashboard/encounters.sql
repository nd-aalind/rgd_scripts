


CREATE TABLE patient_encounters AS
WITH latest_appt AS (
  SELECT 
    d.PATIENT_ID,
    d.APPOINTMENT_ID,
    d.BOOKED_APPOINTMENT_NAME,
    d.APPOINTMENT_DATE,
    d.APPOINTMENT_START_TIME,
    CASE 
        WHEN d.STATUS_TYPE = 'f' THEN 'Scheduled'
        WHEN d.STATUS_TYPE = '2' THEN 'Checked In'
        WHEN d.STATUS_TYPE IN ('3','4') THEN 'Completed'
        WHEN d.STATUS_TYPE = 'x' THEN 'Cancelled'
        WHEN d.STATUS_TYPE = 'o' THEN 'Open'
        ELSE NULL
    END AS appointment_status,
    CASE WHEN d.STATUS_TYPE = 'x' THEN 1 ELSE 0 END AS Cancellation_flag,
    COALESCE(d.CANCELLED_REASON_LOCAL_NAME, acr.NAME) AS cancellation_reason,
    d.lastupdated,
    ROW_NUMBER() OVER (
      PARTITION BY d.APPOINTMENT_ID
      ORDER BY d.lastupdated DESC
    ) AS rn
  FROM APPOINTMENT d
  LEFT JOIN APPOINTMENTCANCELREASON acr
    ON d.CANCELLED_REASON_LOCAL_NAME = acr.NAME
    and acr.nd_active_flag='Y'
  WHERE d.nd_active_flag = 'Y'
)
SELECT 
a.CHARTID AS ndid,
  a.CLINICALENCOUNTERID AS encounter_id,
  la.APPOINTMENT_ID as appointment_id,
  la.APPOINTMENT_DATE AS appointment_date,
  la.APPOINTMENT_START_TIME as appointment_start_time,
  la.BOOKED_APPOINTMENT_NAME AS visit_type,
  la.appointment_status as appointment_status,
  -- CASE 
--         WHEN ap2.appointmentstatus = 'f - Filled' THEN 'Scheduled'
--         WHEN ap2.appointmentstatus = '2 - Checked In' THEN 'Checked In'
--         WHEN ap2.appointmentstatus IN ('3 - Checked Out','4 - Charge Entered') THEN 'Completed'
--         WHEN ap2.appointmentstatus = 'x - Cancelled' THEN 'Cancelled'
--         WHEN ap2.appointmentstatus = 'o - Open Slot' THEN 'Open'
--         ELSE NULL
--     END AS status,
  la.Cancellation_flag as cancellation_flag,
  la.cancellation_reason as cancellation_reason,
  la.patient_id AS patient_id,
  a.ENCOUNTERDATE AS encounter_date,
  c.PATIENTFACINGNAME AS rendering_provider_name,
  ap2.SCHEDULINGPROVIDER AS scheduling_provider,
  vm.visit_type_grouped
  from latest_appt la 
left join CLINICALENCOUNTER a
ON a.APPOINTMENTID = la.APPOINTMENT_ID
and a.nd_active_flag = 'Y'
AND la.rn = 1 
LEFT JOIN CHART b 
  ON a.CHARTID = b.CHARTID
  AND b.nd_active_flag = 'Y'
LEFT JOIN PROVIDER c 
  ON a.PROVIDERID = c.PROVIDERID
  AND c.nd_active_flag = 'Y'  
LEFT JOIN visittype_mapping vm
on lower(trim(vm.visit_type))=lower(trim(la.BOOKED_APPOINTMENT_NAME))
LEFT JOIN appointment_2 ap2
ON ap2.APPOINTMENTID = la.APPOINTMENT_ID
  AND ap2.nd_active_flag = 'Y'
LEFT JOIN PROVIDER p
ON ap2.SCHEDULINGPROVIDERID = p.PROVIDERID
  AND p.nd_active_flag = 'Y'; 
  
