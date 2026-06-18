create table billing_chatbot.diagnosis_claims
    WITH claim_dedup AS (
    SELECT * FROM (
        SELECT
            c.*,
            ROW_NUMBER() OVER (
                PARTITION BY c.CLAIMAPPOINTMENTID
                ORDER BY c.CLAIMCREATEDDATETIME DESC
            ) rn
        FROM CLAIM c
        WHERE c.nd_active_flag          = 'Y'
          AND c.contextid               = '25810'
    ) t
    WHERE rn = 1
),
encounter_dedup AS (
    SELECT * FROM (
        SELECT
            ce.*,
            ROW_NUMBER() OVER (
                PARTITION BY ce.APPOINTMENTID
                ORDER BY ce.CLINICALENCOUNTERID DESC
            ) rn
        FROM CLINICALENCOUNTER ce
        WHERE ce.DELETEDDATETIME        IS NULL
          AND ce.contextid              = '25810'
          AND ce.ENCOUNTERSTATUS        NOT IN ('DELETED','TEMP')
    ) t
    WHERE rn = 1
),
claim_diagnosis_dedup AS (
    SELECT * FROM (
        SELECT
            cd.*,
            ROW_NUMBER() OVER (
                PARTITION BY cd.CLAIMID, cd.SEQUENCENUMBER
                ORDER BY cd.CREATEDDATETIME DESC
            ) rn
        FROM CLAIMDIAGNOSIS cd
        WHERE cd.DELETEDDATETIME        IS NULL
          AND cd.contextid              = '25810'
    ) t
    WHERE rn = 1
),
encounter_diagnosis_dedup AS (
    SELECT * FROM (
        SELECT
            ced.*,
            ROW_NUMBER() OVER (
                PARTITION BY ced.CLINICALENCOUNTERID, ced.DIAGNOSISCODE
                ORDER BY ced.CREATEDDATETIME DESC
            ) rn
        FROM CLINICALENCOUNTERDIAGNOSIS ced
        WHERE ced.DELETEDDATETIME       IS NULL
          AND ced.contextid             = '25810'
    ) t
    WHERE rn = 1
),
icd_dedup AS (
    SELECT
        DIAGNOSISCODE,
        DIAGNOSISCODESET,
        MAX(DIAGNOSISCODEDESCRIPTION)   AS diagnosiscodedescription,
        MAX(EFFECTIVEDATE)              AS effectivedate,
        MAX(EXPIRATIONDATE)             AS expirationdate
    FROM ICDCODEALL
    WHERE (ISDELETED = 0 OR ISDELETED IS NULL)
      AND (EXPIRATIONDATE IS NULL) -- OR EXPIRATIONDATE >= '2022-08-18')
    GROUP BY DIAGNOSISCODE, DIAGNOSISCODESET
),
diagnosiscode_dedup AS (
    SELECT
        DIAGNOSISCODE,
        contextid,
        MAX(DIAGNOSISCODEDESCRIPTION)   AS diagnosiscodedescription
    FROM DIAGNOSISCODE
    WHERE ISDELETED                     = 0
       OR ISDELETED                     IS NULL
    GROUP BY DIAGNOSISCODE, contextid
)
SELECT * FROM (
    SELECT
        c.contextid,
        c.CLAIMID                                                           AS claim_id,
        c.PATIENTID                                                         AS patient_id,
        c.CLAIMSERVICEDATE                                                  AS service_date,
        c.CLAIMCREATEDDATETIME                                              AS claim_created_datetime,
        c.CLAIMAPPOINTMENTID                                                AS appointment_id,
        c.PRIMARYCLAIMSTATUS                                                AS primary_claim_status,
        c.SECONDARYCLAIMSTATUS                                              AS secondary_claim_status,
        c.PATIENTCLAIMSTATUS                                                AS patient_claim_status,
        rend_prov.PROVIDERID                                                AS rendering_provider_id,
        CONCAT(
            COALESCE(rend_prov.PROVIDERFIRSTNAME, ''), ' ',
            COALESCE(rend_prov.PROVIDERLASTNAME, '')
        )                                                                   AS rendering_provider_name,
        rend_prov.SPECIALTY                                                 AS rendering_provider_specialty,
        sup_prov.PROVIDERID                                                 AS supervising_provider_id,
        CONCAT(
            COALESCE(sup_prov.PROVIDERFIRSTNAME, ''), ' ',
            COALESCE(sup_prov.PROVIDERLASTNAME, '')
        )                                                                   AS supervising_provider_name,
        sup_prov.SPECIALTY                                                  AS supervising_provider_specialty,
        dep.DEPARTMENTNAME                                                  AS service_department_name,
        dep.DEPARTMENTCITY                                                  AS service_department_city,
        ce.CLINICALENCOUNTERID                                              AS encounter_id,
        ce.ENCOUNTERDATE                                                    AS encounter_date,
        cd.CLAIMDIAGNOSISID                                                 AS claim_diagnosis_id,
        cd.SEQUENCENUMBER                                                   AS diagnosis_sequence,
        cd.DIAGNOSISCODE                                                    AS claim_diagnosis_code,
        cd.DIAGNOSISCODESETNAME                                             AS diagnosis_codeset,
        icd.DIAGNOSISCODEDESCRIPTION                                        AS claim_diagnosis_description,
        CASE cd.SEQUENCENUMBER
            WHEN 1 THEN 'Primary'
            WHEN 2 THEN 'Secondary'
            WHEN 3 THEN 'Tertiary'
            ELSE CONCAT('Diagnosis ', cd.SEQUENCENUMBER)
        END                                                                 AS diagnosis_priority,
        ced.CLINICALENCOUNTERDXID                                           AS encounter_diagnosis_id,
        ced.DIAGNOSISCODE                                                   AS encounter_diagnosis_code,
        dc.diagnosiscodedescription                                         AS encounter_diagnosis_description,
        ced.STATUS                                                          AS encounter_diagnosis_status,
        ced.LATERALITY                                                      AS diagnosis_laterality,
        ced.NOTE                                                            AS diagnosis_note,
        ced.CREATEDBY                                                       AS diagnosis_recorded_by,
        ced.CREATEDDATETIME                                                 AS diagnosis_recorded_datetime,
        CASE ced.STATUS
            WHEN 'NEWPROBLEMWORKUP'         THEN 'New Problem - Workup'
            WHEN 'ESTABLISHEDSTABLE'        THEN 'Established - Stable'
            WHEN 'ESTABLISHEDIMPROVING'     THEN 'Established - Improving'
            WHEN 'ESTABLISHEDWORSENING'     THEN 'Established - Worsening'
            WHEN 'ESTABLISHEDANDSTABLE'     THEN 'Established and Stable'
            WHEN 'DIFFERENTIALDX'           THEN 'Differential Diagnosis'
            WHEN 'NEXTVISITWORKUP'          THEN 'Next Visit Workup'
            WHEN 'NEWPROVLMENOWORKUP'       THEN 'New Problem - No Workup'
            WHEN 'UNCONTROLLED'             THEN 'Uncontrolled'
            WHEN 'MINOR'                    THEN 'Minor'
            ELSE COALESCE(ced.STATUS, 'Not Specified')
        END                                                                 AS diagnosis_status_label,
        CASE
            WHEN cd.DIAGNOSISCODE = ced.DIAGNOSISCODE                      THEN 'Matched'
            WHEN cd.DIAGNOSISCODE IS NOT NULL AND ced.DIAGNOSISCODE IS NULL THEN 'Claim Only'
            WHEN cd.DIAGNOSISCODE IS NULL AND ced.DIAGNOSISCODE IS NOT NULL THEN 'Encounter Only'
            ELSE 'Unmatched'
        END                                                                 AS diagnosis_source_match,
        cd.CREATEDDATETIME                                                  AS claim_diagnosis_created_datetime,
        cd.CREATEDBY                                                        AS claim_diagnosis_created_by,
        ROW_NUMBER() OVER (
            PARTITION BY c.CLAIMID, cd.SEQUENCENUMBER
            ORDER BY
                ce.CLINICALENCOUNTERID  DESC,
                ced.CREATEDDATETIME     DESC
        )                                                                   AS rn
    FROM claim_dedup c
    INNER JOIN PATIENT p
        ON  p.PATIENTID                 = c.PATIENTID
        AND p.contextid                 = c.contextid
        AND p.nd_active_flag            = 'Y'
    LEFT JOIN PROVIDER rend_prov
        ON  rend_prov.PROVIDERID        = c.RENDERINGPROVIDERID
        AND rend_prov.contextid         = c.contextid
        AND rend_prov.nd_active_flag    = 'Y'
    LEFT JOIN PROVIDER sup_prov
        ON  sup_prov.PROVIDERID         = c.SUPERVISINGPROVIDERID
        AND sup_prov.contextid          = c.contextid
        AND sup_prov.nd_active_flag     = 'Y'
    LEFT JOIN DEPARTMENT dep
        ON  dep.DEPARTMENTID            = c.SERVICEDEPARTMENTID
        AND dep.contextid               = c.contextid
        AND dep.nd_active_flag          = 'Y'
    LEFT JOIN encounter_dedup ce
        ON  ce.APPOINTMENTID            = c.CLAIMAPPOINTMENTID
        AND ce.contextid                = c.contextid
    LEFT JOIN claim_diagnosis_dedup cd
        ON  cd.CLAIMID                  = c.CLAIMID
        AND cd.contextid                = c.contextid
    LEFT JOIN icd_dedup icd
        ON  icd.DIAGNOSISCODE           = cd.DIAGNOSISCODE
        AND icd.DIAGNOSISCODESET        = cd.DIAGNOSISCODESETNAME
    LEFT JOIN encounter_diagnosis_dedup ced
        ON  ced.CLINICALENCOUNTERID     = ce.CLINICALENCOUNTERID
        AND ced.DIAGNOSISCODE           = cd.DIAGNOSISCODE
    LEFT JOIN diagnosiscode_dedup dc
        ON  dc.DIAGNOSISCODE            = ced.DIAGNOSISCODE
        AND dc.contextid                = ced.contextid
    -- WHERE c.CLAIMSERVICEDATE            >= '2022-08-18'
) final
WHERE rn = 1
ORDER BY
    service_date                        DESC,
    patient_id,
    diagnosis_sequence                  ASC;