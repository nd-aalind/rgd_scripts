/* STEP 1 */

/* ENC_CATEGORY NORMALISATION */

WITH base AS (
    SELECT DISTINCT
        ehr_source_name,
        encounter_category,
        enc_sub_type,

        CASE
            WHEN ehr_source_name = 'eCW' AND encounter_category = '1' THEN 'Office Visit'
            WHEN ehr_source_name = 'eCW' AND encounter_category = '2' THEN 'Telephone'
            WHEN ehr_source_name = 'eCW' AND encounter_category = '3' THEN 'Out of Office'
            WHEN ehr_source_name = 'eCW' AND encounter_category = '4' THEN 'Claim'
            WHEN ehr_source_name = 'eCW' AND encounter_category = '5' THEN 'Lab'
            WHEN ehr_source_name = 'eCW' AND encounter_category = '6' THEN 'Web Encounter'
            WHEN ehr_source_name = 'eCW' AND encounter_category = '7' THEN 'ePrescription'
            WHEN ehr_source_name = 'eCW' AND encounter_category = '8' THEN 'PTDASH'
            WHEN ehr_source_name = 'eCW' AND encounter_category = '9' THEN 'Orderset'
            WHEN ehr_source_name = 'eCW'
                 AND encounter_category IN ('10','12','13','14','102')
                THEN 'Other'
            ELSE encounter_category
        END AS norm_cat

    FROM rgd_udm_silver.encounters
)

SELECT DISTINCT
    ehr_source_name,
    encounter_category,

    CASE
        WHEN norm_cat IS NULL OR TRIM(norm_cat) = '' THEN NULL

        /* ==================================================
           AthenaPractice - zMDC Service Coding
           ================================================== */

        /* Injection */

WHEN ehr_source_name = 'AthenaPractice'
     AND norm_cat LIKE 'zMDC Service Coding%'
     AND LOWER(COALESCE(enc_sub_type,'')) REGEXP
     '(injection|epidural steroid)'
THEN 'Injection'

/* Procedures */

WHEN ehr_source_name = 'AthenaPractice'
     AND norm_cat LIKE 'zMDC Service Coding%'
     AND LOWER(COALESCE(enc_sub_type,'')) REGEXP
     '(lumbar puncture|blood patch|facet|si joint|trochanteric|troch)'
THEN 'Procedures'

/* Radiology */

WHEN ehr_source_name = 'AthenaPractice'
     AND norm_cat LIKE 'zMDC Service Coding%'
     AND LOWER(COALESCE(enc_sub_type,'')) REGEXP
     '(mri|mra|mrv|ct|angiogram|myelogram|xray|soft tissue|chest|abdomen|pelvis|head|cervical|thoracic|orbit|extremity|lumbar)'
THEN 'Radiology'

        WHEN ehr_source_name = 'AthenaPractice'
             AND norm_cat LIKE 'zMDC Service Coding%'
             AND COALESCE(enc_sub_type,'') REGEXP
                 '(?i)(testing|visual fields)'
        THEN 'Testing'

       

        /* ==================================================
           Virtual Visits
           ================================================== */

        WHEN norm_cat REGEXP
             '(?i)(telemedicine|telehealth|telephone|virtual|web encounter|neuropsy telemedicine interview)'
        THEN 'Virtual Visit'

        /* ==================================================
           Testing
           ================================================== */

        WHEN norm_cat REGEXP
             '(?i)(cpap|home sleep study|sleep read|actigraph watch|neuropsychological evaluation|cognitive assessment)'
        THEN 'Testing'

        /* ==================================================
           Care Management
           ================================================== */

        WHEN norm_cat REGEXP '(?i)^care management$'
        THEN 'Administrative / Non-Clinical'

        /* ==================================================
           Infusion
           ================================================== */

        WHEN norm_cat REGEXP '(?i)(infusion administration|^infusion|zinfusion)'
        THEN 'Infusion'

        /* ==================================================
           Injection
           ================================================== */

        WHEN norm_cat REGEXP '(?i)(^injection|botox)'
        THEN 'Injection'

        /* ==================================================
           Radiology
           ================================================== */

        WHEN norm_cat REGEXP '(?i)(radiology|mri|emg|eeg|xray|ct|ultrasound)'
        THEN 'Radiology'

        /* ==================================================
           Testing
           ================================================== */

        WHEN norm_cat REGEXP '(?i)(testing|assessment|evaluation)'
        THEN 'Testing'

        /* ==================================================
           Procedures
           ================================================== */

        WHEN norm_cat REGEXP '(?i)(procedure|surgery)'
        THEN 'Procedures'

        /* ==================================================
           Orderset
           ================================================== */

        WHEN norm_cat REGEXP '(?i)(orderset|ordersonly)'
        THEN 'Orderset'

        /* ==================================================
           Out of Office
           ================================================== */

        WHEN norm_cat REGEXP '(?i)(out of office|field)'
        THEN 'Out of Office'

        /* ==================================================
           Lab
           ================================================== */

        WHEN norm_cat REGEXP '(?i)^lab'
        THEN 'Lab'

        /* ==================================================
           ePrescription
           ================================================== */

        WHEN norm_cat REGEXP '(?i)eprescription'
        THEN 'ePrescription Refills'

        /* ==================================================
           Administrative
           ================================================== */

        WHEN norm_cat REGEXP '(?i)(flowsheet|historical|ptdash|claim|admin)'
        THEN 'Administrative / Non-Clinical'

        /* ==================================================
           Hospital Visit
           ================================================== */

        WHEN norm_cat REGEXP '(?i)(hospital|in patient|inpatient|observation)'
        THEN 'Hospital Visit'

        /* ==================================================
           Office Visit
           ================================================== */

        WHEN norm_cat REGEXP
             '(?i)(office visit|visit|consult|new pt|follow up|ambulatory|nursing|physical therapy|orv)'
        THEN 'Office Visit'

        /* ==================================================
           Other
           ================================================== */

        WHEN norm_cat IN ('Other','Void','Research','Balance','Special Studies')
        THEN 'Other'

        ELSE 'NS'
    END AS enc_category_std

FROM base;



/* STEP 2 */

/* ENC_TYPE NORMALISATION */

SELECT DISTINCT
    ehr_source_name,
    enc_type,

    CASE

        WHEN enc_type IS NULL OR TRIM(enc_type) = '' THEN NULL
        
        /* INFUSION */
        WHEN LOWER(enc_type) REGEXP 'infusion'
            THEN 'Infusion'
        
                 /* FOLLOW UP */
        WHEN LOWER(enc_type) REGEXP 'follow up|follow-up|follow|f/u|fu'
            THEN 'Follow Up Visit'

        /* EEG */
        WHEN LOWER(enc_type) REGEXP 'eeg'
            THEN 'EEG'

        /* EMG */
        WHEN LOWER(enc_type) REGEXP 'emg|electromyography'
            THEN 'EMG'

        /* SLEEP STUDY */
        WHEN LOWER(enc_type) REGEXP 'sleep|psg|mslt|cpap|mask fitting'
            THEN 'Sleep Study'

        /* MRI / IMAGING */
        WHEN LOWER(enc_type) REGEXP 'mri|mra|mrv'
            THEN 'MRI / Neuro Imaging'

        /* RADIOLOGY */
        WHEN LOWER(enc_type) REGEXP 'xray|radiology|service coding'
            THEN 'Radiology'

        /* NERVE CONDUCTION */
        WHEN LOWER(enc_type) REGEXP 'nerve conduction'
            THEN 'Nerve Conduction Study'

        /* PROCEDURES */
        WHEN LOWER(enc_type) REGEXP 'procedure|lumbar puncture|biopsy|spg|esi'
            THEN 'Procedure'

        /* INJECTION / BOTOX */
        WHEN LOWER(enc_type) REGEXP 'injection|botox|dysport|xeomin|toxin'
            THEN 'Injection / Botox'
            
        /* DIAGNOSTIC TESTING */
        WHEN LOWER(enc_type) REGEXP 'testing|study|assessment|evaluation|actigraph|mda|special studies'
            THEN 'Diagnostic Testing'

        /* BALANCE / VESTIBULAR */
        WHEN LOWER(enc_type) REGEXP 'vng|eng|videonystagmogram|balance'
            THEN 'Balance / Vestibular Testing'

        /* HEARING */
        WHEN LOWER(enc_type) REGEXP 'hearing'
            THEN 'Hearing Evaluation'
            
                /* NEW PATIENT */
        WHEN LOWER(enc_type) REGEXP 'new'
            THEN 'New Patient Visit'
            
                 /* TELEMED / VIRTUAL */
        WHEN LOWER(enc_type) REGEXP 'tele|virtual'
            THEN 'Telehealth Visit'
            
        /* REFERRAL */
        WHEN LOWER(enc_type) REGEXP 'referral'
            THEN 'Referral'

        /* CONSULTATION */
        WHEN LOWER(enc_type) REGEXP 'consult|neuro-ophthalmology'
            THEN 'Consultation'

        /* RESEARCH */
        WHEN LOWER(enc_type) REGEXP 'research'
            THEN 'Research'
            
        /* OFFICE / GENERAL VISIT */
        WHEN LOWER(enc_type) REGEXP 'office visit|established|multi-visit|out patient|problem|orv|physical therapy|end visit'
            THEN 'Office Visit'

        /* HOSPITAL */
        WHEN LOWER(enc_type) REGEXP 'hospital|in patient|nursing home'
            THEN 'Hospital Visit'

        /* EMERGENCY */
        WHEN LOWER(enc_type) REGEXP 'emergency'
            THEN 'Emergency Visit'

        /* ADMINISTRATIVE */
        WHEN LOWER(enc_type) REGEXP 'admin|administration|meeting|legal|care management|primemobile|no charge|incomplete'
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
        WHEN LOWER(enc_type) = 'other'
            THEN 'Other'

        ELSE 'NS'

    END AS enc_type_std

FROM rgd_udm_silver.encounters

ORDER BY enc_type;


* STEP 3 */

/* ENC_SUB_TYPE CLEANING */

SELECT DISTINCT
    enc_sub_type,

    CASE
        WHEN enc_sub_type IS NULL OR TRIM(enc_sub_type) = '' THEN NULL
        
        
    /* Explicit mappings */
    WHEN LOWER(TRIM(enc_sub_type)) = '48 or 72 hr'
        THEN 'Other'

    WHEN UPPER(TRIM(enc_sub_type)) = '(CANCELLATION/BILL)'
        THEN 'CANCELLATION/BILL'

    WHEN UPPER(TRIM(enc_sub_type)) = '(PROCEDURE CANCELLATION)'
        THEN 'PROCEDURE CANCELLATION'

    WHEN UPPER(TRIM(enc_sub_type)) = '(RESEARCH CANCELLATION)'
        THEN 'RESEARCH CANCELLATION'

        -- Numeric only → Other
        WHEN TRIM(enc_sub_type) REGEXP '^[0-9]+(\\.[0-9]+)?$'
            THEN 'Other'
                

        ELSE
            COALESCE(
                NULLIF(
                    TRIM(
                        REGEXP_REPLACE(
                            REGEXP_REPLACE(
                                REGEXP_REPLACE(
                                    REGEXP_REPLACE(
                                        REGEXP_REPLACE(
                                            REGEXP_REPLACE(
                                                REGEXP_REPLACE(
                                                    REGEXP_REPLACE(
                                                        REGEXP_REPLACE(
                                                            REGEXP_REPLACE(
                                                                REGEXP_REPLACE(
                                                                    REGEXP_REPLACE(
                                                                        REGEXP_REPLACE(
                                                                            REGEXP_REPLACE(
                                                                                

                                                                                    -- remove fractions (1/2)
                                                                                    enc_sub_type,
                                                                                    '[0-9]+/[0-9]+',
                                                                                    ''
                                                                                ),

                                                                                -- remove ""1.5 hr or 2 hrs"" (preserve H2H)
                                                                                '(^|[^A-Za-z])[0-9]+\\.?[0-9]*\\s*(hours|hour|hrs|hr|h)\\s*or\\s*[0-9]+\\.?[0-9]*\\s*(hours|hour|hrs|hr|h)([^A-Za-z]|$)',
                                                                                ''
                                                                            ),

                                                                            -- remove leading durations
                                                                            '^[0-9]+\\s*(MINUTE|MINUTES|MIN|MINS|HOUR|HOURS|HR|HRS)\\s+',
                                                                            ''
                                                                        ),

                                                                        -- remove embedded durations (preserve H2H)
                                                                        '(^|[^A-Za-z])[0-9]+\\.?[0-9]*\\s*(hours|hour|mins|minutes|hrs|hr|min|h)([^A-Za-z]|$)',
                                                                        ''
                                                                    ),

                                                                    -- remove day/week/month/year durations
                                                                    '[0-9]+\\.?[0-9]*\\s*(days|day|weeks|week|months|month|years|year)',
                                                                    ''
                                                                ),

                                                                -- remove parentheses
                                                                '\\([^)]*\\)',
                                                                ''
                                                            ),

                                                            -- remove trailing ""-180""
                                                            '\\s*[-/]\\s*[0-9]+[\\s-]*$',
                                                            ''
                                                        ),

                                                        -- remove trailing ""_60""
                                                        '[_\\s]+[0-9]+\\s*$',
                                                        ''
                                                    ),

                                                    -- preserve 70+ patterns
                                                    '^[0-9\\.]+\\s+(?!\\+)',
                                                    ''
                                                ),

                        

                                            -- replace commas
                                            ',',
                                            ' '
                                        ),

                                        -- remove trailing hyphens
                                        '\\s*-+\\s*$',
                                        ''
                                    ),

                                    -- collapse spaces
                                    '\\s+',
                                    ' '
                                ),

                                -- strip leading asterisks
                                '^\\*+\\s*',
                                ''
                            ),

                            -- strip trailing * . _
                            '[*._]+\\s*$',
                            ''
                        )
                    ),
                    ''
                ),
                'Other'
            )

    END AS enc_sub_type_std

FROM rgd_udm_silver.encounters;



/* STEP 4 */

/* ENC_TYPE IMPUTED  */ /* Impute only when enc_type missing and enc_sub_type_std available */

select distinct enc_type,enc_sub_type_std,

        CASE

        /* EEG */
        WHEN LOWER(enc_sub_type_std) REGEXP
        'eeg|aeeg|veeg|ambulatory eeg|corticare-eeg|nexus-eeg|extended eeg|electroencephalography'
        THEN 'EEG'

        /* EMG */
        WHEN LOWER(enc_sub_type_std) REGEXP
        '(^emg$|emg[-/ ]|[-/ ]emg|same day emg|electromyography)'
        THEN 'EMG'

        /* Nerve Conduction */
        WHEN LOWER(enc_sub_type_std) REGEXP
        'ncs|ncv|blink reflex|somatosensory|autonomic nervous system|nerve conduction'
        THEN 'Nerve Conduction Study'

        /* Infusion */
        WHEN LOWER(enc_sub_type_std) REGEXP
        '\\biv\\b|infusion|ocrevus|rituximab|hydration|spinraza|vyepti'
        THEN 'Infusion'

        /* MRI / Imaging */
        WHEN LOWER(enc_sub_type_std) REGEXP
        'mri|\\bmra\\b|mrv|brain|\\bct\\b|pet imaging'
        THEN 'MRI / Neuro Imaging'

        /* Radiology */
        WHEN LOWER(enc_sub_type_std) REGEXP
        'xray|ultrasound'
        THEN 'Radiology'

        /* Procedure */
        WHEN LOWER(enc_sub_type_std) REGEXP
        'mapping|nerve block|trigger|stim|proc|surgery|therapy|biopsy|dbs|lumbar puncture|treatment'
        THEN 'Procedure'

        /* Injection */
        WHEN LOWER(enc_sub_type_std) REGEXP
        'injection|botox'
        THEN 'Injection / Botox'

        /* Diagnostic Testing */
        WHEN LOWER(enc_sub_type_std) REGEXP
        'testing|study|assessment|evaluation|exam|ekg|neuropsych'
        THEN 'Diagnostic Testing'

        /* Office Visit */
        WHEN LOWER(enc_sub_type_std) REGEXP
        'visit|clinic|office|medical|established|physical therapy'
        THEN 'Office Visit'

        /* New Patient */
        WHEN LOWER(enc_sub_type_std) REGEXP
        '^np|new patient|new pt'
        THEN 'New Patient Visit'

        /* Consultation */
        WHEN LOWER(enc_sub_type_std) REGEXP
        'consult|conference|care planning'
        THEN 'Consultation'

        /* Hearing */
        WHEN LOWER(enc_sub_type_std) REGEXP
        'hearing|abr|assr|baer|audiolog|cochlear'
        THEN 'Hearing Evaluation'

        /* Balance */
        WHEN LOWER(enc_sub_type_std) REGEXP
        'vestibular|balance|caloric|cvemp|vhit'
        THEN 'Balance / Vestibular Testing'

        /* Administrative */
        WHEN LOWER(enc_sub_type_std) REGEXP
        'form|billing|insurance|schedule|records|eprescription'
        THEN 'Administrative'

        /* Work In */
        WHEN LOWER(enc_sub_type_std) REGEXP
        'fit in|work in|wait list'
        THEN 'Work In Visit'

        /* Other */
        WHEN LOWER(enc_sub_type_std) REGEXP
        'other|misc|unknown|trial'
        THEN 'Other'

        ELSE 'Null'

END AS enc_type_std

from rgd_udm_silver.encounters

/* Impute only when enc_type missing and enc_sub_type_std available */
  Where (enc_type IS NULL OR TRIM(enc_type)='')
         AND enc_sub_type_std IS NOT NULL
         AND TRIM(enc_sub_type_std)<>'';




/* STEP 5 */

/* ENC_CATEGORY IMPUTED  */ /* Impute only when enc_category missing and enc_type_std available */


select 

distinct enc_category,enc_type_std,

        CASE

        /* INFUSION */
        WHEN enc_type_std IN (
            'Infusion'
        )
        THEN 'Infusion'


        /* INJECTION */
        WHEN enc_type_std IN (
            'Injection / Botox'
        )
        THEN 'Injection'


        /* RADIOLOGY */
        WHEN enc_type_std IN (
            'MRI / Neuro Imaging',
            'EEG',
            'EMG',
            'Sleep Study',
            'Nerve Conduction Study',
            'Balance / Vestibular Testing',
            'Hearing Evaluation'
        )
        THEN 'Radiology'


        /* TESTING */
        WHEN enc_type_std IN (
            'Diagnostic Testing'
        )
        THEN 'Testing'


        /* PROCEDURES */
        WHEN enc_type_std IN (
            'Procedure'
        )
        THEN 'Procedures'


        /* VIRTUAL VISIT */
        WHEN enc_type_std IN (
            'Telehealth Visit'
        )
        THEN 'Virtual Visit'


        /* ORDERSET */
        WHEN enc_type_std IN (
            'Referral'
        )
        THEN 'Orderset'


        /* OUT OF OFFICE */
        WHEN enc_type_std IN (
            'Work In Visit'
        )
        THEN 'Out of Office'


        /* LAB */
        WHEN enc_type_std IN (
            'Observation'
        )
        THEN 'Lab'


        /* EPRESCRIPTION */
        WHEN enc_type_std IN (
            'Follow Up Visit'
        )
        THEN 'ePrescription Refills'


        /* ADMINISTRATIVE / NON-CLINICAL */
        WHEN enc_type_std IN (
            'Administrative',
            'Research',
            'No Show',
            'Voided Visit'
        )
        THEN 'Administrative / Non-Clinical'


        /* OFFICE VISIT */
        WHEN enc_type_std IN (
            'Office Visit',
            'New Patient Visit',
            'Consultation'
        )
        THEN 'Office Visit'


        /* HOSPITAL VISIT */
        WHEN enc_type_std IN (
            'Hospital Visit',
            'Emergency Visit'
        )
        THEN 'Hospital Visit'


        /* OTHER */
        WHEN enc_type_std IN (
            'Other'
        )
        THEN 'Other'


        ELSE Null

END AS enc_category_std

From rgd_udm_silver.encounters

 /* Impute only when enc_category missing and enc_type_std available */
    WHERE (enc_category IS NULL OR TRIM(enc_category)='')
         AND enc_type_std IS NOT NULL
         AND TRIM(enc_type_std)<>'';