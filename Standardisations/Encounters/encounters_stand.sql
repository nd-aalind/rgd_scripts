-- Update 1: 

/* =========================================================
   STEP 1 — ECW (numeric enctype → category)
   ========================================================= */

WITH ecw_raw AS (
    SELECT distinct enctype FROM fcn_latest.enc
    UNION ALL SELECT distinct enctype FROM arizona_staging.enc
    UNION ALL SELECT distinct enctype FROM texas.enc
    UNION ALL SELECT distinct enctype FROM northwest.enc
    UNION ALL SELECT distinct enctype FROM tncne.enc
    UNION ALL SELECT distinct enctype FROM dent.enc
),

ecw_category AS (
    SELECT
        CASE
            WHEN enctype = 1 THEN 'Office Visit'
            WHEN enctype = 2 THEN 'Telephone / Virtual Visit'
            WHEN enctype = 3 THEN 'Out of Office'
            WHEN enctype = 4 THEN 'Claim'
            WHEN enctype = 5 THEN 'Lab'
            WHEN enctype = 6 THEN 'Web Encounter'
            WHEN enctype = 7 THEN 'ePrescription Refills'
            WHEN enctype = 8 THEN 'PTDASH'
            WHEN enctype = 9 THEN 'Orderset'
            ELSE 'Other'
        END AS enc_category
    FROM ecw_raw
),

/* =========================================================
   STEP 2 — ATHENAONE
   ========================================================= */

athena_category AS (

    SELECT distinct
        COALESCE(at.APPOINTMENTTYPECLASS,
                 ce.CLINICALENCOUNTERTYPE) AS enc_category
    FROM raleigh.CLINICALENCOUNTER ce
    LEFT JOIN raleigh.APPOINTMENT ap
        ON ce.APPOINTMENTID = ap.APPOINTMENT_ID
    LEFT JOIN raleigh.APPOINTMENTTYPE at
        ON ap.BOOKED_APPOINTMENT_TYPE_ID = at.APPOINTMENTTYPEID

    UNION ALL

    SELECT distinct
        COALESCE(at.APPOINTMENTTYPECLASS,
                 ce.CLINICALENCOUNTERTYPE) AS enc_category
    FROM tng_athena_one.CLINICALENCOUNTER ce
    LEFT JOIN tng_athena_one.APPOINTMENT ap
        ON ce.APPOINTMENTID = ap.APPOINTMENT_ID
    LEFT JOIN tng_athena_one.APPOINTMENTTYPE at
        ON ap.BOOKED_APPOINTMENT_TYPE_ID = at.APPOINTMENTTYPEID

    UNION ALL

    SELECT distinct
        COALESCE(at.APPOINTMENTTYPECLASS,
                 ce.CLINICALENCOUNTERTYPE) AS enc_category
    FROM dcnd.CLINICALENCOUNTER ce
    LEFT JOIN dcnd.APPOINTMENT ap
        ON ce.APPOINTMENTID = ap.APPOINTMENT_ID
    LEFT JOIN dcnd.APPOINTMENTTYPE at
        ON ap.BOOKED_APPOINTMENT_TYPE_ID = at.APPOINTMENTTYPEID

    UNION ALL

    SELECT distinct
        COALESCE(at.APPOINTMENTTYPECLASS,
                 ce.CLINICALENCOUNTERTYPE) AS enc_category
    FROM tncpa.CLINICALENCOUNTER ce
    LEFT JOIN tncpa.APPOINTMENT ap
        ON ce.APPOINTMENTID = ap.APPOINTMENT_ID
    LEFT JOIN tncpa.APPOINTMENTTYPE at
        ON ap.BOOKED_APPOINTMENT_TYPE_ID = at.APPOINTMENTTYPEID
),

/* =========================================================
   STEP 3 — GREENWAY
   ========================================================= */

greenway_category AS (

   SELECT distinct
        COALESCE(ecl.EncounterValueName,
                 vt.EncounterClassID) AS enc_category
    FROM savannah.Visit v
    LEFT JOIN savannah.VisitTypes vt
        ON v.VisitTypeID = vt.VisitTypeID
    LEFT JOIN savannah.EncounterClassLookUp ecl
        ON vt.EncounterClassID = ecl.EncounterClassID

    UNION ALL

    SELECT distinct
        COALESCE(ecl.EncounterValueName,
                 vt.EncounterClassID) AS enc_category
    FROM mind.Visit v
    LEFT JOIN mind.VisitTypes vt
        ON v.VisitTypeID = vt.VisitTypeID
    LEFT JOIN mind.EncounterClassLookUp ecl
        ON vt.EncounterClassID = ecl.EncounterClassID

    UNION ALL

    SELECT distinct
        COALESCE(ecl.EncounterValueName,
                 vt.EncounterClassID) AS enc_category
    FROM jwm.Visit v
    LEFT JOIN jwm.VisitTypes vt
        ON v.VisitTypeID = vt.VisitTypeID
    LEFT JOIN jwm.EncounterClassLookUp ecl
        ON vt.EncounterClassID = ecl.EncounterClassID
),

/* =========================================================
   STEP 4 — UNION ALL SOURCES
   ========================================================= */

all_categories AS (
    SELECT enc_category FROM ecw_category
    UNION ALL
    SELECT enc_category FROM athena_category
    UNION ALL
    SELECT enc_category FROM greenway_category
)

/* =========================================================
   STEP 5 — FINAL OUTPUT
   ========================================================= */

/* =========================================================
   STEP 5 — FINAL OUTPUT
   ========================================================= */

/* =========================================================
   STEP 5 — FINAL OUTPUT (Regex based normalization)
   ========================================================= */

SELECT DISTINCT
    enc_category,

    CASE

        /* Administrative / Non-Clinical */
        WHEN enc_category REGEXP '^(FLOWSHEET|HISTORICAL|PTDASH|Claim)$'
            THEN 'Administrative / Non-Clinical'

        /* ePrescription */
        WHEN enc_category REGEXP '(?i)eprescription'
            THEN 'ePrescription Refills'

        /* Infusion */
        WHEN enc_category REGEXP '(?i)^infusion'
            THEN 'Infusion'

        /* Injection */
        WHEN enc_category REGEXP '(?i)^injection'
            THEN 'Injection'

        /* Lab */
        WHEN enc_category REGEXP '(?i)^lab'
            THEN 'Lab'

        /* Office Visit */
        WHEN enc_category REGEXP '(?i)(office visit|visit|consult|ambulatory|nursing)'
            THEN 'Office Visit'

        /* Orderset */
        WHEN enc_category REGEXP '(?i)orderset|ordersonly'
            THEN 'Orderset'

        /* Out of Office */
        WHEN enc_category REGEXP '(?i)(out of office|field)'
            THEN 'Out of Office'

        /* Procedures */
        WHEN enc_category REGEXP '(?i)(procedures|surgery)'
            THEN 'Procedures'

        /* Radiology */
        WHEN enc_category REGEXP '(?i)radiology'
            THEN 'Radiology'

        /* Virtual Visit */
        WHEN enc_category REGEXP '(?i)(telehealth|telephone|virtual|web encounter)'
            THEN 'Virtual Visit'

        /* Testing */
        WHEN enc_category REGEXP '(?i)testing'
            THEN 'Testing'

        /* Miscellaneous */
        WHEN enc_category REGEXP '(?i)misc'
            THEN 'Other'

        ELSE 'Other'

    END AS enc_category_std

FROM all_categories
ORDER BY enc_category_std, enc_category;


-- Update 2:

WITH ecw_data AS (

    -- dent
    SELECT DISTINCT
        NULL AS enc_type
    FROM dent.enc enc
    LEFT JOIN dent.visitcodes vc
        ON enc.visittype = vc.Name

    UNION ALL

    -- texas
    SELECT DISTINCT
        NULL
    FROM texas.enc enc
    LEFT JOIN texas.visitcodes vc
        ON enc.visittype = vc.Name

    UNION ALL

    -- northwest
    SELECT DISTINCT
        NULL
    FROM northwest.enc enc
    LEFT JOIN northwest.visitcodes vc
        ON enc.visittype = vc.Name

    UNION ALL

    -- fcn
    SELECT DISTINCT
        NULL
    FROM fcn_latest.enc enc
    LEFT JOIN fcn_latest.visitcodes vc
        ON enc.visittype = vc.Name

    UNION ALL

    -- tncne
    SELECT DISTINCT
        NULL
    FROM tncne.enc enc
    LEFT JOIN tncne.visitcodes vc
        ON enc.visittype = vc.Name

    UNION ALL

    -- arizona
    SELECT DISTINCT
        NULL
    FROM arizona_staging.enc enc
    LEFT JOIN arizona_staging.visitcodes vc
        ON enc.visittype = vc.Name
),

athena_data AS (

    -- tng
    SELECT DISTINCT
        at.COMMUNICATORDISPLAYNAME AS enc_type
    FROM tng_athena_one.CLINICALENCOUNTER ce
    LEFT JOIN tng_athena_one.APPOINTMENT ap
        ON ce.APPOINTMENTID = ap.APPOINTMENT_ID
    LEFT JOIN tng_athena_one.APPOINTMENTTYPE at
        ON ap.BOOKED_APPOINTMENT_TYPE_ID = at.APPOINTMENTTYPEID

    UNION ALL

    -- raleigh
    SELECT DISTINCT
        at.COMMUNICATORDISPLAYNAME
    FROM raleigh.CLINICALENCOUNTER ce
    LEFT JOIN raleigh.APPOINTMENT ap
        ON ce.APPOINTMENTID = ap.APPOINTMENT_ID
    LEFT JOIN raleigh.APPOINTMENTTYPE at
        ON ap.BOOKED_APPOINTMENT_TYPE_ID = at.APPOINTMENTTYPEID

    UNION ALL

    -- tncpa
    SELECT DISTINCT
        at.COMMUNICATORDISPLAYNAME
    FROM tncpa.CLINICALENCOUNTER ce
    LEFT JOIN tncpa.APPOINTMENT ap
        ON ce.APPOINTMENTID = ap.APPOINTMENT_ID
    LEFT JOIN tncpa.APPOINTMENTTYPE at
        ON ap.BOOKED_APPOINTMENT_TYPE_ID = at.APPOINTMENTTYPEID

    UNION ALL

    -- dcnd
    SELECT DISTINCT
        at.COMMUNICATORDISPLAYNAME
    FROM dcnd.CLINICALENCOUNTER ce
    LEFT JOIN dcnd.APPOINTMENT ap
        ON ce.APPOINTMENTID = ap.APPOINTMENT_ID
    LEFT JOIN dcnd.APPOINTMENTTYPE at
        ON ap.BOOKED_APPOINTMENT_TYPE_ID = at.APPOINTMENTTYPEID
),

greenway_data AS (

    -- mind
    SELECT DISTINCT
        vt.StandardName AS enc_type
    FROM mind.Visit v
    LEFT JOIN mind.VisitTypes vt
        ON v.VisitTypeID = vt.VisitTypeID
    
    UNION ALL

    -- jwm
    SELECT DISTINCT
        vt.StandardName
    FROM jwm.Visit v
    LEFT JOIN jwm.VisitTypes vt
        ON v.VisitTypeID = vt.VisitTypeID
        
        UNION ALL
        
  -- savannah
  SELECT DISTINCT
        vt.StandardName
    FROM savannah.Visit v
    LEFT JOIN savannah.VisitTypes vt
        ON v.VisitTypeID = vt.VisitTypeID
    
  
)

SELECT
    enc_type,

    CASE

        /* TELEMED / VIRTUAL */
        WHEN LOWER(enc_type) REGEXP 'tele|virtual'
            THEN 'Telehealth Visit'

        /* NEW PATIENT */
        WHEN LOWER(enc_type) REGEXP 'new'
            THEN 'New Patient Visit'


        /* OFFICE / GENERAL VISIT */
        WHEN LOWER(enc_type) REGEXP 'office visit|established|multi-visit|out patient'
            THEN 'Office Visit'

        /* HOSPITAL */
        WHEN LOWER(enc_type) REGEXP 'hospital|in patient'
            THEN 'Hospital Visit'

        /* EMERGENCY */
        WHEN LOWER(enc_type) REGEXP 'emergency'
            THEN 'Emergency Visit'

        /* EEG */
        WHEN LOWER(enc_type) REGEXP 'eeg'
            THEN 'EEG'

        /* EMG */
        WHEN LOWER(enc_type) REGEXP 'emg|electromyography'
            THEN 'EMG'

        /* SLEEP STUDY */
        WHEN LOWER(enc_type) REGEXP 'sleep|psg|mslt'
            THEN 'Sleep Study'

        /* MRI / IMAGING */
        WHEN LOWER(enc_type) REGEXP 'mri|mra|mrv'
            THEN 'MRI / Neuro Imaging'

        /* XRAY / RADIOLOGY */
        WHEN LOWER(enc_type) REGEXP 'xray|ultrasound'
            THEN 'Radiology'

        /* NERVE TEST */
        WHEN LOWER(enc_type) REGEXP 'nerve conduction'
            THEN 'Nerve Conduction Study'

        /* PROCEDURES */
        WHEN LOWER(enc_type) REGEXP 'procedure|lumbar puncture|biopsy'
            THEN 'Procedure'

        /* INJECTION */
        WHEN LOWER(enc_type) REGEXP 'injection|botox'
            THEN 'Injection / Botox'

        /* INFUSION */
        WHEN LOWER(enc_type) REGEXP 'infusion'
            THEN 'Infusion'
            
                 /* FOLLOW UP */
        WHEN LOWER(enc_type) REGEXP 'follow|f/u|fu'
            THEN 'Follow Up Visit'

        /* REFERRAL */
        WHEN LOWER(enc_type) REGEXP 'ref'
            THEN 'Referral'

        /* CONSULT */
        WHEN LOWER(enc_type) REGEXP 'consult'
            THEN 'Consultation'

        /* RESEARCH */
        WHEN LOWER(enc_type) REGEXP 'research'
            THEN 'Research'

        /* TESTING */
        WHEN LOWER(enc_type) REGEXP 'testing|study'
            THEN 'Diagnostic Testing'

        /* BALANCE / VNG */
        WHEN LOWER(enc_type) REGEXP 'vng|videonystagmogram|balance'
            THEN 'Balance / Vestibular Testing'

        /* HEARING */
        WHEN LOWER(enc_type) REGEXP 'hearing'
            THEN 'Hearing Evaluation'

        /* ADMIN */
        WHEN LOWER(enc_type) REGEXP 'admin|meeting|legal'
            THEN 'Administrative'

        /* OBSERVATION */
        WHEN LOWER(enc_type) REGEXP 'observation'
            THEN 'Observation'

        /* NO SHOW */
        WHEN LOWER(enc_type) REGEXP 'no show'
            THEN 'No Show'

        /* VOIDED */
        WHEN LOWER(enc_type) REGEXP 'void'
            THEN 'Voided Visit'

        /* WORK IN */
        WHEN LOWER(enc_type) REGEXP 'work in'
            THEN 'Work In Visit'

        /* OTHER */
        ELSE 'Other'

    END AS enc_type_std

FROM (
    SELECT enc_type FROM ecw_data
    UNION ALL
    SELECT enc_type FROM athena_data
    UNION ALL
    SELECT enc_type FROM greenway_data
) final_data
ORDER BY enc_type;


--  Update 3:

WITH ecw_data AS (

    -- dent
    SELECT DISTINCT
        COALESCE(vc.Description, enc.visittype) AS enc_sub_type
    FROM dent.enc enc
    LEFT JOIN dent.visitcodes vc
        ON enc.visittype = vc.Name

    UNION ALL

    -- texas
    SELECT DISTINCT
        COALESCE(vc.Description, enc.visittype)
    FROM texas.enc enc
    LEFT JOIN texas.visitcodes vc
        ON enc.visittype = vc.Name

    UNION ALL

    -- northwest
    SELECT DISTINCT
        COALESCE(vc.Description, enc.visittype)
    FROM northwest.enc enc
    LEFT JOIN northwest.visitcodes vc
        ON enc.visittype = vc.Name

    UNION ALL

    -- fcn
    SELECT DISTINCT
        COALESCE(vc.Description, enc.visittype)
    FROM fcn_latest.enc enc
    LEFT JOIN fcn_latest.visitcodes vc
        ON enc.visittype = vc.Name

    UNION ALL

    -- tncne
    SELECT DISTINCT
        COALESCE(vc.Description, enc.visittype)
    FROM tncne.enc enc
    LEFT JOIN tncne.visitcodes vc
        ON enc.visittype = vc.Name

    UNION ALL

    -- arizona
    SELECT DISTINCT
        COALESCE(vc.Description, enc.visittype)
    FROM arizona_staging.enc enc
    LEFT JOIN arizona_staging.visitcodes vc
        ON enc.visittype = vc.Name
),

athena_data AS (

    -- tng
    SELECT DISTINCT
        at.APPOINTMENTTYPENAME AS enc_sub_type
    FROM tng_athena_one.CLINICALENCOUNTER ce
    LEFT JOIN tng_athena_one.APPOINTMENT ap
        ON ce.APPOINTMENTID = ap.APPOINTMENT_ID
    LEFT JOIN tng_athena_one.APPOINTMENTTYPE at
        ON ap.BOOKED_APPOINTMENT_TYPE_ID = at.APPOINTMENTTYPEID

    UNION ALL

    -- raleigh
    SELECT DISTINCT
        at.APPOINTMENTTYPENAME
    FROM raleigh.CLINICALENCOUNTER ce
    LEFT JOIN raleigh.APPOINTMENT ap
        ON ce.APPOINTMENTID = ap.APPOINTMENT_ID
    LEFT JOIN raleigh.APPOINTMENTTYPE at
        ON ap.BOOKED_APPOINTMENT_TYPE_ID = at.APPOINTMENTTYPEID

    UNION ALL

    -- tncpa
    SELECT DISTINCT
        at.APPOINTMENTTYPENAME
    FROM tncpa.CLINICALENCOUNTER ce
    LEFT JOIN tncpa.APPOINTMENT ap
        ON ce.APPOINTMENTID = ap.APPOINTMENT_ID
    LEFT JOIN tncpa.APPOINTMENTTYPE at
        ON ap.BOOKED_APPOINTMENT_TYPE_ID = at.APPOINTMENTTYPEID

    UNION ALL

    -- dcnd
    SELECT DISTINCT
        at.APPOINTMENTTYPENAME
    FROM dcnd.CLINICALENCOUNTER ce
    LEFT JOIN dcnd.APPOINTMENT ap
        ON ce.APPOINTMENTID = ap.APPOINTMENT_ID
    LEFT JOIN dcnd.APPOINTMENTTYPE at
        ON ap.BOOKED_APPOINTMENT_TYPE_ID = at.APPOINTMENTTYPEID
),

greenway_data AS (

    -- mind
    SELECT DISTINCT
        vt.StandardName AS enc_sub_type
    FROM mind.Visit v
    LEFT JOIN mind.VisitTypes vt
        ON v.VisitTypeID = vt.VisitTypeID
    

    UNION ALL

    -- jwm
    SELECT DISTINCT
        vt.StandardName
    FROM jwm.Visit v
    LEFT JOIN jwm.VisitTypes vt
        ON v.VisitTypeID = vt.VisitTypeID
    
        
        UNION ALL

    -- savannah
    SELECT DISTINCT
        vt.StandardName
    FROM savannah.Visit v
    LEFT JOIN savannah.VisitTypes vt
        ON v.VisitTypeID = vt.VisitTypeID
   
)

SELECT
    enc_sub_type,

/*Clean up*/
TRIM(
REGEXP_REPLACE(
REGEXP_REPLACE(
REGEXP_REPLACE(
REGEXP_REPLACE(
REGEXP_REPLACE(
REGEXP_REPLACE(
REGEXP_REPLACE(
enc_sub_type,

/* remove patterns like 24/48 */
'[0-9]+/[0-9]+',''),

/* remove patterns like 48 or 72 hr */
'[0-9]+\\s*(?i)or\\s*[0-9]+\\s*(hr|hrs|hour|hours)',''),

/* remove durations like 8 HR */
'[0-9]+\\s*(?i)(hr|hrs|hour|hours|min|mins|minutes)',''),

/* remove month/year durations */
'[0-9]+\\s*(?i)(day|days|week|weeks|month|months|year|years)',''),

/* remove numeric prefixes */
'(^[0-9+]+)',''),

/* remove trailing numbers */
'-\\s*[0-9]+',''),

/* replace separators */
'[-/,]',' ')

) AS enc_sub_type_std

FROM (
    SELECT enc_sub_type FROM ecw_data
    UNION ALL
    SELECT enc_sub_type FROM athena_data
    UNION ALL
    SELECT enc_sub_type FROM greenway_data
) final_data
ORDER BY enc_sub_type;


-- Update 4:

UPDATE table_name
SET enc_category_std =
    CASE
        WHEN enc_type_std = 'Office Visit' THEN 'Office Visit'
        WHEN enc_type_std = 'Infusion' THEN 'Infusion'
        WHEN enc_type_std IN ('EMG','EEG') THEN 'Radiology'
        WHEN enc_type_std = 'Hospital Visit' THEN 'Hospital Visit'
        WHEN enc_type_std = 'Diagnostic Testing' THEN 'Testing'
        WHEN enc_type_std = 'Administrative' THEN 'Administrative / Non-Clinical'
        WHEN enc_type_std = 'Injection / Botox' THEN 'Injection'
        WHEN enc_type_std = 'Balance / Vestibular Testing' THEN 'Other'
        WHEN enc_type_std = 'Voided Visit' THEN 'Other'
        WHEN enc_type_std = 'Observation' THEN 'Other'
        WHEN enc_type_std = 'No Show' THEN 'Other'
        WHEN enc_type_std = 'Emergency Visit' THEN 'Other'
        WHEN enc_type_std = 'Research' THEN 'Other'
        ELSE 'Other'
    END
WHERE enc_type_std IS NOT NULL
AND enc_category_std IS NULL;

-- Update 5 : 

UPDATE table_name
SET enc_type_std =
CASE

    -- Consultation
    WHEN enc_sub_type_std LIKE '%CONSULT%' 
         THEN 'Consultation'

    -- Office Visit
    WHEN enc_sub_type_std LIKE '%OFFICE%' 
         OR enc_sub_type_std LIKE '%OV%' 
         THEN 'Office Visit'

    -- New Patient Visit
    WHEN enc_sub_type_std LIKE '%NEW PATIENT%' 
         OR enc_sub_type_std LIKE '%NP%' 
         THEN 'New Patient Visit'

    -- Follow Up Visit
    WHEN enc_sub_type_std LIKE '%FOLLOW%' 
         OR enc_sub_type_std LIKE '%FU%' 
         OR enc_sub_type_std LIKE '%RECHECK%' 
         THEN 'Follow Up Visit'

    -- Work In Visit
    WHEN enc_sub_type_std LIKE '%WORK IN%' 
         THEN 'Work In Visit'

    -- Telehealth Visit
    WHEN enc_sub_type_std LIKE '%TELE%' 
         OR enc_sub_type_std LIKE '%WEB%' 
         OR enc_sub_type_std LIKE '%VIDEO%' 
         OR enc_sub_type_std LIKE '%PHONE%' 
         THEN 'Telehealth Visit'

    -- Virtual / Telehealth
    WHEN enc_sub_type_std LIKE '%VIRTUAL%' 
         THEN 'Virtual / Telehealth'

    -- Procedure
    WHEN enc_sub_type_std LIKE '%PROCEDURE%' 
         OR enc_sub_type_std LIKE '%BIOPSY%' 
         OR enc_sub_type_std LIKE '%PUNCTURE%' 
         THEN 'Procedure'

    -- Injection / Botox
    WHEN enc_sub_type_std LIKE '%INJECTION%' 
         OR enc_sub_type_std LIKE '%BOTOX%' 
         OR enc_sub_type_std LIKE '%DYSPORT%' 
         OR enc_sub_type_std LIKE '%DAXXIFY%' 
         THEN 'Injection / Botox'

    -- Infusion
    WHEN enc_sub_type_std LIKE '%INFUSION%' 
         OR enc_sub_type_std LIKE 'IV %' 
         THEN 'Infusion'

    -- Infusion / Specialty Drugs
    WHEN enc_sub_type_std LIKE '%SPECIALTY DRUG%' 
         THEN 'Infusion / Specialty Drugs'

    -- Psychology / Behavioral Health
    WHEN enc_sub_type_std LIKE '%PSYCH%' 
         OR enc_sub_type_std LIKE '%BEHAVIORAL%' 
         THEN 'Psychology / Behavioral Health'

    -- Sleep Study
    WHEN enc_sub_type_std LIKE '%SLEEP%' 
         OR enc_sub_type_std LIKE '%PSG%' 
         OR enc_sub_type_std LIKE '%CPAP%' 
         THEN 'Sleep Study'

    -- EMG
    WHEN enc_sub_type_std LIKE '%EMG%' 
         AND enc_sub_type_std NOT LIKE '%NCS%' 
         THEN 'EMG'

    -- Nerve Conduction Study
    WHEN enc_sub_type_std LIKE '%NCS%' 
         OR enc_sub_type_std LIKE '%NERVE CONDUCTION%' 
         THEN 'Nerve Conduction Study'

    -- EMG / Nerve Conduction
    WHEN enc_sub_type_std LIKE '%EMG NCS%' 
         THEN 'EMG / Nerve Conduction'

    -- EEG
    WHEN enc_sub_type_std LIKE '%EEG%' 
         THEN 'EEG'

    -- MRI / Neuro Imaging
    WHEN enc_sub_type_std LIKE '%MRI%' 
         OR enc_sub_type_std LIKE '%MRA%' 
         OR enc_sub_type_std LIKE '%MRV%' 
         THEN 'MRI / Neuro Imaging'

    -- Radiology
    WHEN enc_sub_type_std LIKE '%XRAY%' 
         OR enc_sub_type_std LIKE '%CT%' 
         OR enc_sub_type_std LIKE '%ULTRASOUND%' 
         OR enc_sub_type_std LIKE '%DOPPLER%' 
         THEN 'Radiology'

    -- Lab
    WHEN enc_sub_type_std LIKE '%LAB%' 
         OR enc_sub_type_std LIKE '%BLOOD%' 
         OR enc_sub_type_std LIKE '%URINE%' 
         OR enc_sub_type_std LIKE '%PLATELET%' 
         THEN 'Lab'

    -- Diagnostic Testing
    WHEN enc_sub_type_std LIKE '%TEST%' 
         OR enc_sub_type_std LIKE '%EVAL%' 
         THEN 'Diagnostic Testing'

    -- Hearing Evaluation
    WHEN enc_sub_type_std LIKE '%HEARING%' 
         OR enc_sub_type_std LIKE '%AUDIO%' 
         THEN 'Hearing Evaluation'

    -- Balance / Vestibular Testing
    WHEN enc_sub_type_std LIKE '%VNG%' 
         OR enc_sub_type_std LIKE '%BALANCE%' 
         OR enc_sub_type_std LIKE '%VESTIB%' 
         THEN 'Balance / Vestibular Testing'

    -- Physical / Occupational Therapy
    WHEN enc_sub_type_std LIKE '%THERAPY%' 
         OR enc_sub_type_std LIKE '%PT%' 
         OR enc_sub_type_std LIKE '%OT%' 
         THEN 'Physical / Occupational Therapy'

    -- Administrative
    WHEN enc_sub_type_std LIKE '%ADMIN%' 
         OR enc_sub_type_std LIKE '%MEETING%' 
         OR enc_sub_type_std LIKE '%RENTAL%' 
         THEN 'Administrative'

    -- Research
    WHEN enc_sub_type_std LIKE '%RESEARCH%' 
         OR enc_sub_type_std LIKE '%STUDY%' 
         THEN 'Research'

    -- No Show
    WHEN enc_sub_type_std LIKE '%NO SHOW%' 
         THEN 'No Show'

    -- Observation
    WHEN enc_sub_type_std LIKE '%OBSERVE%' 
         OR enc_sub_type_std LIKE '%OBSERVATION%' 
         THEN 'Observation'

    -- Emergency Visit
    WHEN enc_sub_type_std LIKE '%ER%' 
         OR enc_sub_type_std LIKE '%EMERGENCY%' 
         THEN 'Emergency Visit'

    -- Hospital Visit
    WHEN enc_sub_type_std LIKE '%HOSPITAL%' 
         OR enc_sub_type_std LIKE '%INPATIENT%' 
         THEN 'Hospital Visit'

    -- Referral
    WHEN enc_sub_type_std LIKE '%REFERRAL%' 
         THEN 'Referral'

    -- Default
    ELSE 'Other'

END
WHERE enc_type_std IS NULL
AND enc_sub_type_std IS NOT NULL;


-- Update 6:

WITH client_codes AS (
    SELECT '10010001' AS prefix, code, status FROM dent.visitstscodes UNION ALL
    SELECT '10010003' AS prefix, code, status FROM texas.visitstscodes UNION ALL
    SELECT '10010004' AS prefix, code, status FROM northwest.visitstscodes UNION ALL
    SELECT '10010008' AS prefix, code, status FROM fcn_latest.visitstscodes UNION ALL
    SELECT '10010013' AS prefix, code, status FROM tncne.visitstscodes UNION ALL
    SELECT '10010014' AS prefix, code, status FROM arizona_staging.visitstscodes
),
mapped_data AS (
    SELECT DISTINCT 
        e.enc_status,
        CASE 
            WHEN cc.status IS NOT NULL THEN cc.status
            WHEN e.enc_status IS NULL OR e.enc_status = '' THEN 'Unknown'
            WHEN e.enc_status IN ('0', '1', '2') THEN 'Null'
            WHEN e.enc_status = '["CANCSMS"]' THEN 'SMS Cancel'
            WHEN e.enc_status = '["CONFSMS"]' THEN 'SMS Confirmed'
            WHEN e.enc_status = 'ACK' THEN 'Acknowledged'
            WHEN e.enc_status = 'BUMP' THEN 'BUMPED'
            WHEN e.enc_status = 'CAN' THEN 'Cancelled'
            WHEN e.enc_status = 'CLOSED' THEN 'Closed'
            WHEN e.enc_status = 'COM' THEN 'Completed'
            WHEN e.enc_status = 'CONF' THEN 'Confirmed'
            WHEN e.enc_status = 'DELETED' THEN 'Deleted'
            WHEN e.enc_status = 'Kiosk' THEN 'Kiosk'
            WHEN e.enc_status = 'N/S BILL' THEN 'NO SHOW FEE'
            WHEN e.enc_status = 'NOS' THEN 'No-Show'
            WHEN e.enc_status = 'NOSHOW' THEN 'No-Show'
            WHEN e.enc_status = 'Open' THEN 'Open'
            WHEN e.enc_status = 'PEND' THEN 'Pending'
            WHEN e.enc_status = 'REQUIRESIGNATURE' THEN 'Signature Required'
            WHEN e.enc_status = 'REVIEW' THEN 'Review Needed'
            WHEN e.enc_status = 'RSC' THEN 'Rescheduled'
            WHEN e.enc_status = 'SCHED' THEN 'Scheduled'
            WHEN e.enc_status = 'SCHEDULE' THEN 'Scheduled'
            WHEN e.enc_status = 'TEMP' THEN 'Temporary'
            WHEN e.enc_status = 'VMSGPEN' THEN 'Voice Message'
            WHEN e.enc_status = 'WAIT' THEN 'Waiting'
            ELSE e.enc_status 
        END AS enc_status_std
    FROM rgd_udm_silver.encounters_01282026 e
    LEFT JOIN client_codes cc 
        ON LEFT(e.ndid, 8) = cc.prefix 
        AND trim(e.enc_status) = trim(cc.code)
)
SELECT 
    enc_status,
    enc_status_std,
    CASE 
        -- 1. COMPLETED
        WHEN enc_status_std REGEXP 'Completed|Closed|Billed|Check Out|Check-Out|CHK|Transcription|READY FOR CHECK OUT' THEN 'Completed'
        
        -- 2. IN-PROGRESS
        WHEN enc_status_std REGEXP 'ROOMED|Exam|MA Ready|EEGReady|MRI Ready|Doctor|WITH MA|In-Progress' THEN 'In-Progress'
        
        -- 3. ARRIVED
        WHEN enc_status_std REGEXP 'Arrived|Check-In|Check In|Check-in|Kiosk|QR|Reception|ARR|Acknowledged|WAIT' THEN 'Arrived'
        
        -- 4. LWOBS
        WHEN enc_status_std REGEXP 'LWOBS|Left without being seen|WalkOut|W/O Being Seen' THEN 'LWOBS'
        
        -- 5. NO-SHOW
        WHEN enc_status_std REGEXP 'No-Show|No Show|NOSHOW|NOS|NO SHOW FEE' THEN 'No-Show'
        
        -- 6. CANCELLED
        WHEN enc_status_std REGEXP 'Cancel|Cx|Cxl|Deleted|Legal|DENT-no R/S' THEN 'Cancelled'
        
        -- 7. CONFIRMED
        WHEN enc_status_std REGEXP 'Confirm|Verification Complete|Talked to Pt' THEN 'Confirmed'
        
        -- 8. SCHEDULED
        WHEN enc_status_std REGEXP 'Scheduled|Rescheduled|Reschedule|RSC|R/S|Appt not needed|Open' THEN 'Scheduled'
        
        -- 9. PENDING / AUTH
        WHEN enc_status_std REGEXP 'Pending|Auth|Pen-|Precerted|Referral|Signature Required|Review Needed' THEN 'Pending'
        
        -- 10. ADMIN / OTHER
        ELSE 'Admin / Other'
    END AS normalized_category
FROM mapped_data;

