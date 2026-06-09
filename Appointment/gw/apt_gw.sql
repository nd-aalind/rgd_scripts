-- Appointment Greenway --

SELECT
sa.ApptID AS appointment_id,
sa.PatientID AS ndid,
v.VisitID AS encounter_id,
v.FromDateTime AS encounter_date,
sa.DateMade AS appointment_created_date,
DATE(sa.StartDate) AS appointment_date,
sa.StartDate AS appointment_start_time,
COALESCE(sar.EndTime,pl.AppointmentEndTime) AS appointment_end_time,
COALESCE(sar.DurationMinutes,TIMESTAMPDIFF(MINUTE,sa.StartDate,sar.EndTime)) AS appointment_duration,
CASE WHEN sa.ChangeId=2 THEN 'No Show' 
WHEN sa.ChangeId IN(1,3,8) OR sa.Disable=1 THEN 'Cancelled' 
WHEN sa.ChangeId IN(4,5) THEN 'Rescheduled' ELSE NULL 
END AS appointment_status,
COALESCE(ecl.EncounterValueName,vt.EncounterClassID) AS appointment_type,
satl.ApptTypeName AS appointment_name,
COALESCE(plrp.ProviderID,v.CareProviderID) AS provider_id,
null AS provider_name,
pl.TimeIn AS check_in_time,
pl.TimeOut AS check_out_time,
COALESCE(sa.PatientComplaint,pl.ChiefComplaint,v.PrimaryComplaint) AS appointment_reason,
-- COALESCE(pl.ChiefComplaint,v.PrimaryComplaint) AS chief_complaint,
sa.ApptDescription AS appointment_notes,
CASE WHEN sa.ChangeId IN(1,3,8) OR sa.Disable=1 THEN 1 ELSE 0 END AS Cancellation_flag,
sacl.ChangeReason AS cancellation_reason,
CASE WHEN sa.ChangeId=2 THEN 1 ELSE 0 END AS no_show_flag,
CASE WHEN sa.ChangeId IN(4,5) THEN 1 ELSE 0 END AS reschedule_flag,
srl.InsPreCertId AS appointment_prior_auth_id,
ipc.AuthorizationStatus AS prior_auth_status,
CASE WHEN ipc.InsPreCertID IS NOT NULL AND ipc.Active=1 THEN 1 ELSE 0 END AS referral_flag,
ipc.InsID AS insurance_id,
ipc.CoPayAmount AS copay_amount,
COALESCE(sa.IsTelehealthAppt,pl.IsTelehealthAppt) AS telehealth_flag,
CURRENT_TIMESTAMP() AS created_datetime,
        'ND' AS created_by,
        'Greenway' AS ehr_source_name,
        'bronze_layer' AS source_path,
        'Structured' AS data_type
FROM ScheduleAppointment sa
LEFT JOIN PatientList pl 
ON pl.ApptId=sa.ApptID and pl.nd_ActiveFlag = 'Y' and sa.nd_ActiveFlag = 'Y'
LEFT JOIN Visit v 
ON pl.VisitId=v.VisitID and v.nd_ActiveFlag = 'Y'
LEFT JOIN ScheduleAppointmentResources sar  
ON sa.ApptID=sar.ApptID and sar.nd_ActiveFlag = 'Y'
LEFT JOIN ScheduleApptTypeList satl 
ON sa.ApptTypeID=satl.ApptTypeID and satl.nd_ActiveFlag = 'Y'
LEFT JOIN VisitTypes vt 
ON v.VisitTypeID=vt.VisitTypeID and vt.nd_ActiveFlag = 'Y'
LEFT JOIN EncounterClassLookUp ecl 
ON vt.EncounterClassID=ecl.EncounterClassID and ecl.nd_ActiveFlag = 'Y'
LEFT JOIN PatientListResourceProviders plrp 
ON pl.PatientListID=plrp.PatientListID and plrp.nd_ActiveFlag = 'Y'
LEFT JOIN ScheduleApptChangeLookup sacl 
ON sacl.ChangeID=sa.ChangeId and sacl.nd_ActiveFlag = 'Y'
LEFT JOIN ScheduleReferralLink srl 
ON srl.ApptId=sa.ApptID and srl.nd_ActiveFlag = 'Y'
LEFT JOIN InsurancePreCert ipc 
ON srl.InsPreCertId=ipc.InsPreCertID and ipc.nd_ActiveFlag = 'Y';