
  CREATE TABLE {{DEST_SCHEMA}}.patient_information as
SELECT 
    registrationdate,
    patientid,
    chartid AS ndid,
    gender,
    ufname,
    ulname,
    dob,
    race,
    race_grouped,
    uemail,
    mobile,
    upaddress,
    upaddress2,
    upcity,
    upstate,
    upPhone,
    zipcode,
    country_name,         
    patientstatus,
    primaryprovider,
    deceased_status,
    deceasedDate,
    active_flag_6m,
    active_flag_12m,
    active_flag_18m,
    active_flag_24m
FROM (
    SELECT
        p.registrationdate,
        p.ENTERPRISEID AS patientid,
        ch.chartid,
        CASE
            WHEN LOWER(p.sex) = 'm' THEN 'Male'
            WHEN LOWER(p.sex) = 'f' THEN 'Female'
            ELSE 'Unknown'
        END AS gender,
        p.FIRSTNAME AS ufname,
        p.LASTNAME AS ulname,
        p.DOB AS dob,
        p.RACE AS race,
        rc.race_grouped,
        p.EMAIL AS uemail,
        p.MOBILEPHONE AS mobile,
        p.ADDRESS AS upaddress,
        p.ADDRESS2 AS upaddress2,
        p.CITY AS upcity,
        p.STATE AS upstate,
        p.PATIENTHOMEPHONE AS upPhone,
        p.ZIP AS zipcode,
        COALESCE(cntry.NAME, pi.COUNTRY) AS country_name,
        CASE p.PATIENTSTATUS 
            WHEN 'i' THEN 'Inactive' 
            WHEN 'd' THEN 'Deleted' 
            WHEN 'a' THEN 'Active' 
            ELSE 'Pending' 
        END AS patientstatus,
        pro.BILLEDNAME AS primaryprovider,
        CASE
            WHEN p.DECEASEDDATE IS NOT NULL THEN 1
            ELSE 0
        END AS deceased_status,
        p.DECEASEDDATE AS deceasedDate,
        CASE 
            WHEN p.DECEASEDDATE IS NOT NULL THEN 0
            WHEN pe.last_enc > DATE_SUB('2025-07-01', INTERVAL 6 MONTH) THEN 1 
            ELSE 0 
        END AS active_flag_6m,
        CASE 
            WHEN p.DECEASEDDATE IS NOT NULL THEN 0
            WHEN pe.last_enc > DATE_SUB('2025-07-01', INTERVAL 12 MONTH) THEN 1 
            ELSE 0 
        END AS active_flag_12m,
        CASE 
            WHEN p.DECEASEDDATE IS NOT NULL THEN 0
            WHEN pe.last_enc > DATE_SUB('2025-07-01', INTERVAL 18 MONTH) THEN 1 
            ELSE 0 
        END AS active_flag_18m,
        CASE 
            WHEN p.DECEASEDDATE IS NOT NULL THEN 0
            WHEN pe.last_enc > DATE_SUB('2025-07-01', INTERVAL 24 MONTH) THEN 1 
            ELSE 0 
        END AS active_flag_24m,
        ROW_NUMBER() OVER (
            PARTITION BY p.ENTERPRISEID 
            ORDER BY pe.last_enc DESC
        ) AS rn
    FROM PATIENT p
    LEFT JOIN ref_race_grouping rc
        ON LOWER(TRIM(p.race)) = LOWER(TRIM(rc.race))
    LEFT JOIN (
        SELECT patient_id, MAX(encounter_date) AS last_enc
        FROM patient_encounters
        GROUP BY patient_id
    ) pe ON p.ENTERPRISEID = pe.patient_id
    LEFT JOIN provider pro
        ON p.PRIMARYPROVIDERID = pro.PROVIDERID
    LEFT JOIN chart ch
        ON ch.enterpriseid = p.patientid
    LEFT JOIN (
        SELECT 
            PATIENTID,
            INSUREDCOUNTRYID,
            COUNTRY,
            ROW_NUMBER() OVER (
                PARTITION BY PATIENTID 
                ORDER BY SEQUENCENUMBER ASC
            ) AS ins_rn
        FROM PATIENTINSURANCE
        WHERE DELETEDDATETIME IS NULL
    ) pi ON pi.PATIENTID = p.PATIENTID AND pi.ins_rn = 1
    LEFT JOIN COUNTRY cntry 
        ON pi.INSUREDCOUNTRYID = cntry.COUNTRYID
    WHERE p.nd_active_flag = 'Y'
) t
WHERE rn = 1;
