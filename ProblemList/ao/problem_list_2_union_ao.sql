WITH problemlist AS (
    SELECT
        PATIENTPROBLEM.PATIENTPROBLEMID AS diag_id,
        PATIENTPROBLEM.CHARTID AS ndid,
        NULL AS eid,
        NULL AS encounter_date,
        PATIENTPROBLEM.CREATEDDATETIME AS problem_date,
        PATIENTPROBLEM.ONSETDATE AS problem_onset_date,
        PATIENTPROBLEM.DEACTIVATEDDATETIME AS problem_end_date,
        NULL AS resolved,
        SNOMED.DESCRIPTION AS problem_desc,
        PATIENTPROBLEM.SNOMEDCODE AS snomed_code,
        PATIENTPROBLEM.DIAGNOSISCODE AS icd_code,
        PATIENTPROBLEM.TYPE AS problem_type,
        PATIENTPROBLEM.STATUS AS status,
        NULL AS severity,
        PATIENTPROBLEM.LATERALITY AS laterality,
        PATIENTPROBLEM.NOTE AS problem_notes,
        'PATIENTPROBLEM' AS data_source,
        CURRENT_TIMESTAMP() AS created_datetime,
        'ND' AS created_by,
        -- '{{ params.ehr_source_name }}' AS ehr_source_name,
        'Athenone' AS ehr_source_name,
        'bronze_layer' AS source_path,
        'Structured' AS data_type,
        '10' AS psid,
        PATIENTPROBLEM.nd_extracted_date as nd_extracted_date
       -- {{ params.psid }} AS psid,
    FROM PATIENTPROBLEM
    LEFT JOIN SNOMED
        ON SNOMED.SNOMEDCODE = PATIENTPROBLEM.SNOMEDCODE
    WHERE PATIENTPROBLEM.nd_active_flag = 'Y'
    UNION ALL
    SELECT
        PATIENTSNOMEDPROBLEM.PATIENTSNOMEDPROBLEMID AS diag_id,
        PATIENTSNOMEDPROBLEM.CHARTID AS ndid,
        NULL AS eid,
        NULL AS encounter_date,
        PATIENTSNOMEDPROBLEM.ENTEREDDATETIME AS problem_date,
        PATIENTSNOMEDPROBLEM.STARTDATEDATETIME AS problem_onset_date,
        PATIENTSNOMEDPROBLEM.ENDDATEDATETIME AS problem_end_date,
        NULL AS resolved,
        SNOMED.DESCRIPTION AS problem_desc,
        PATIENTSNOMEDPROBLEM.SNOMEDCODE AS snomed_code,
        NULL AS icd_code,
        NULL AS problem_type,
        NULL AS status,
        PATIENTSNOMEDPROBLEM.SEVERITY AS severity,
        PATIENTSNOMEDPROBLEM.LATERALITY AS laterality,
        PATIENTSNOMEDPROBLEM.PROBLEMNOTE AS problem_notes,
        'PATIENTSNOMEDPROBLEM' AS data_source,
        CURRENT_TIMESTAMP() AS created_datetime,
        'ND' AS created_by,
        -- '{{ params.ehr_source_name }}' AS ehr_source_name,
        'Athenone' AS ehr_source_name,
        'bronze_layer' AS source_path,
        'Structured' AS data_type,
        '10' AS psid,
        PATIENTSNOMEDPROBLEM.nd_extracted_date as nd_extracted_date
       -- {{ params.psid }} AS psid,
    FROM PATIENTSNOMEDPROBLEM
    LEFT JOIN SNOMED
        ON SNOMED.SNOMEDCODE = PATIENTSNOMEDPROBLEM.SNOMEDCODE
    WHERE PATIENTSNOMEDPROBLEM.nd_active_flag = 'Y'
),
dedup AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY 
                   diag_id,
                   ndid,
                   problem_date,
                   COALESCE(icd_code, snomed_code),
                   problem_onset_date,
                   problem_end_date
               ORDER BY problem_date DESC
           ) AS rn
    FROM problemlist
)
SELECT *
FROM dedup 
WHERE rn = 1;