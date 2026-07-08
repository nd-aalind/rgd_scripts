
create table {{DEST_SCHEMA}}.registration as
    WITH LatestInsurance AS (
    SELECT i.contextid, i.patientid, ip.name AS Insurance,
           ROW_NUMBER() OVER (PARTITION BY i.patientid ORDER BY i.lastupdated DESC) AS rn
    FROM {{SOURCE_SCHEMA}}.patientinsurance i
    LEFT JOIN {{SOURCE_SCHEMA}}.insurancepackage ip
      ON ip.insurancepackageid = i.insurancepackageid
      and ip.nd_active_flag='Y'
      where i.nd_active_flag='Y'
),
First_Encounter AS (
    SELECT ce.contextid, ce.patientid, ce.encounterdate AS first_enc_date, 
           ce.departmentid, ce.providerid AS encounter_providerid, ce.appointmentid,
           ROW_NUMBER() OVER (PARTITION BY ce.patientid ORDER BY ce.encounterdate ASC) AS rn
    FROM {{SOURCE_SCHEMA}}.clinicalencounter ce
    WHERE ce.deleteddatetime IS NULL and ce.nd_active_flag='Y'
),
CareTeam AS (
    SELECT
        pp.patientid,
        pp.patientproviderid,
        pp.role                 AS care_team_role,
        pp.recipienttype        AS care_team_provider_type,
        pp.recipientid          AS care_team_provider_id,
        CASE pp.RECIPIENTTYPE WHEN 'CLINICALPROVIDERID'  THEN TRIM(CONCAT_WS(' ', cp.FIRSTNAME, cp.LASTNAME))
        WHEN 'REFERRINGPROVIDERID' THEN TRIM(CONCAT_WS(' ', rp.FIRSTNAME, rp.LASTNAME)) ELSE NULL
        END AS care_team_provider_name,
        CASE pp.RECIPIENTTYPE WHEN 'CLINICALPROVIDERID'  THEN cp.NPI
        WHEN 'REFERRINGPROVIDERID' THEN rp.NPINUMBER ELSE NULL END AS care_team_provider_npi,
        CASE pp.RECIPIENTTYPE WHEN 'REFERRINGPROVIDERID' THEN rp.SPECIALTY ELSE NULL
        END AS care_team_provider_specialty
    FROM PATIENTPROVIDER pp
    LEFT JOIN CLINICALPROVIDER cp
        ON  cp.CLINICALPROVIDERID = pp.RECIPIENTID
        AND pp.RECIPIENTTYPE = 'CLINICALPROVIDERID'
        AND cp.ISDELETED = 0
    LEFT JOIN REFERRINGPROVIDER rp
        ON  rp.REFERRINGPROVIDERID = pp.RECIPIENTID
        AND pp.RECIPIENTTYPE = 'REFERRINGPROVIDERID'
        AND rp.DELETEDDATETIME  IS NULL
    WHERE pp.DELETEDDATETIME    IS NULL
      AND pp.TYPE = 'PATIENTPROVIDER'
),
CareTeamAggregated AS (
    SELECT
        patientid,
        COUNT(patientproviderid) AS care_team_total_count,
        GROUP_CONCAT(DISTINCT care_team_provider_name
            ORDER BY care_team_provider_name
            SEPARATOR ' | ') AS care_team_provider_names,
        GROUP_CONCAT(DISTINCT care_team_role
            ORDER BY care_team_role
            SEPARATOR ' | ') AS care_team_roles,
        GROUP_CONCAT(DISTINCT care_team_provider_type
            ORDER BY care_team_provider_type
            SEPARATOR ' | ') AS care_team_provider_types,
        GROUP_CONCAT(DISTINCT care_team_provider_npi
            ORDER BY care_team_provider_npi
            SEPARATOR ' | ') AS care_team_provider_npis,
        GROUP_CONCAT(DISTINCT care_team_provider_specialty
            ORDER BY care_team_provider_specialty
            SEPARATOR ' | ') AS care_team_provider_specialties,
        GROUP_CONCAT(
            CONCAT(
                COALESCE(care_team_role, 'Unknown Role'),
                ': ',
                COALESCE(care_team_provider_name, 'Unknown Provider')
            )
            ORDER BY care_team_role
            SEPARATOR ' | ') AS care_team_role_provider_map
    FROM CareTeam
    GROUP BY patientid
)
SELECT DISTINCT
    p.contextid,
    p.PATIENTID AS patient_id,
    p.REGISTRATIONDEPARTMENTID AS registration_clinic_id,
    CASE WHEN TRIM(p.REGISTRATIONDATE) IN ('None','Null','') THEN NULL ELSE CAST(p.REGISTRATIONDATE AS DATE) 
    END AS registration_date,
    NULL AS registration_time,
    CASE WHEN TRIM(p.DOB) IN ('None','Null','') THEN NULL ELSE CAST(p.DOB AS DATE) 
    END AS date_of_birth,
    p.SEX AS gender,
    p.FIRSTNAME AS first_name,
    p.LASTNAME AS last_name,
    p.EMAIL AS email,
    p.MOBILEPHONE AS mobile_number,
    p.PATIENTHOMEPHONE AS phone_number,
    p.ADDRESS AS address_line_1,
    p.ADDRESS2 AS address_line_2,
    p.CITY AS city,
    p.STATE AS state,
    p.ZIP AS zip_code,
    NULL AS county,
    NULL AS region,
    p.LANGUAGE AS primary_language,
    p.CDCRACECODE AS race_code,
    p.RACE AS race,
    rc.race_grouped,
    p.CDCETHNICITYCODE AS ethnicity_code,
    p.ETHNICITY AS ethnicity,
    p.MARITALSTATUS AS marital_status,
    CASE WHEN p.DECEASEDDATE IS NOT NULL AND TRIM(p.DECEASEDDATE) NOT IN ('None','Null','') THEN 'Y' ELSE NULL 
    END AS deceased_status,
    CASE WHEN TRIM(p.DECEASEDDATE) IN ('None','Null','') THEN NULL ELSE CAST(p.DECEASEDDATE AS DATE) END AS date_of_death,
    NULL AS reason_of_death,
    trim(p.PRIMARYPROVIDERID) AS primary_provider_id,  
    trim(CONCAT_WS(' ',prov.PROVIDERFIRSTNAME,prov.PROVIDERLASTNAME)) AS provider,
    prov.patientfacingname as rendering_provider_name,
    CASE WHEN p.REFERRALSOURCE IS NOT NULL AND TRIM(p.REFERRALSOURCE) NOT IN ('','None','Null') THEN 'Y' ELSE 'N' 
    END AS referral_flag,
    p.REFERRALSOURCE AS referral_from,
    p.CONTEXTNAME AS facility_name,
    d_reg.DEPARTMENTCITY AS facility_city,
    d_reg.DEPARTMENTNAME AS patient_facility,
    d_reg.PLACEOFSERVICETYPE AS Place_visit_type,
    enc_prov.SPECIALTY AS specialty,
    apt.schedulingproviderid  as scheduling_provider_id,
    pr.SCHEDULINGNAME as scheduling_provider,
    CASE WHEN p.PATIENTSTATUS IN ('a','') OR p.PATIENTSTATUS IS NULL THEN 'Y' ELSE 'N' 
    END AS patient_activeflag,
    le.first_enc_date,
    li.Insurance,
    ap.BOOKED_APPOINTMENT_NAME,
    vm.visit_type_grouped
    -- ,cta.care_team_total_count,
    -- cta.care_team_provider_names,
    -- cta.care_team_roles,
    -- cta.care_team_provider_types,
    -- cta.care_team_provider_npis,
    -- cta.care_team_provider_specialties,
    -- cta.care_team_role_provider_map
FROM PATIENT p
LEFT JOIN PROVIDER prov 
    ON trim(p.PRIMARYPROVIDERID) = trim(prov.PROVIDERID) 
    and prov.nd_active_flag='Y'
    AND p.contextid = prov.contextid
LEFT JOIN DEPARTMENT d_reg 
    ON p.REGISTRATIONDEPARTMENTID = d_reg.DEPARTMENTID 
    and d_reg.nd_active_flag='Y'
    AND p.contextid = d_reg.contextid
LEFT JOIN LatestInsurance li 
    ON li.patientid = p.patientid 
    AND p.contextid = li.contextid 
    AND li.rn = 1
LEFT JOIN First_Encounter le 
    ON le.patientid = p.patientid 
    AND le.contextid = p.contextid 
    AND le.rn = 1
LEFT JOIN PROVIDER enc_prov 
    ON trim(le.encounter_providerid) = trim(enc_prov.PROVIDERID) 
    and enc_prov.nd_active_flag='Y'
LEFT JOIN APPOINTMENT ap
    ON le.appointmentid = ap.appointment_id
    and ap.nd_active_flag='Y'
    AND le.contextid = ap.contextid
LEFT JOIN appointment_2 apt
    ON le.appointmentid = apt.appointmentid
    and apt.nd_active_flag='Y'
    AND le.contextid = apt.contextid   
LEFT JOIN PROVIDER pr 
    ON trim(apt.schedulingproviderid) = trim(pr.PROVIDERID) 
    and pr.nd_active_flag='Y'
    AND apt.contextid = pr.contextid   
LEFT JOIN {{DEST_SCHEMA}}.visittype_mapping vm
ON lower(trim(vm.visit_type))=lower(trim(ap.BOOKED_APPOINTMENT_NAME)) 
LEFT JOIN {{DEST_SCHEMA}}.ref_race_grouping rc
    ON lower(trim(p.race))=lower(trim(rc.race))
-- LEFT JOIN CareTeamAggregated cta ON cta.patientid = p.PATIENTID
WHERE p.nd_active_flag='Y' 
-- AND REGISTRATIONDATE >='2022-08-18';    
