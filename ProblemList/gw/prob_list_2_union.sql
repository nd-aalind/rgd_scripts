INSERT INTO udm_staging.problemlist (
    diag_id,
    ndid,
    eid,
    encounter_date,
    problem_date,
    problem_onset_date,
    problem_end_date,
    resolved,
    problem_desc,
    snomed_code,
    icd_code,
    problem_type,
    status,
    severity,
    laterality,
    problem_notes,
    data_source,
    psid,
    nd_extracted_date
)
SELECT
    CAST(p.PatHistProblemListID AS SIGNED) AS diag_id,
    CAST(p.PatientID AS SIGNED) AS ndid,
    NULL AS eid,
    NULL AS encounter_date,
    DATE(p.LastChanged) AS problem_date,
    DATE(p.ProblemStartDate) AS problem_onset_date,
    DATE(p.DateResolved) AS problem_end_date,
    CAST(p.IsCurrent AS SIGNED) as resolved,
    PatHistItem.PatHistItemDescription AS problem_desc,
    CASE WHEN AltSystem LIKE '%SNO%' THEN PatHistItem.AltSystemCode ELSE NULL END AS snomed_code,
        CASE WHEN AltSystem LIKE '%ICD%' THEN PatHistItem.AltSystemCode ELSE NULL END AS icd_code,
    NULL AS problem_type,
    ProblemListStatus.ProblemListStatusDesc AS status,
    NULL AS severity,
    NULL AS laterality,
    PatHistProblemList.ProblemNote AS problem_notes,
    "Greenway" AS data_source,
    12 as psid,
    null as nd_extracted_date
FROM PatHistProblemList
LEFT JOIN PatHistCatPatHistItem
    ON PatHistCatPatHistItem.PatHistCatPatHistItemID = PatHistProblemList.PatHistCatPatHistItemID
LEFT JOIN PatHistItem
    ON PatHistItem.PatHistItemID = PatHistCatPatHistItem.PatHistItemID
LEFT JOIN ProblemListStatus
    ON ProblemListStatus.ProblemListStatusID = PatHistProblemList.ProblemListStatusID
    UNION ALL 
    SELECT
    CAST(PatHistProblemListHistory.PatHistProblemListID AS SIGNED)AS diag_id,
    CAST(PatHistProblemListHistory.PatientID AS SIGNED) AS ndid,
    NULL AS eid,
    NULL AS encounter_date,
    DATE(PatHistProblemListHistory.LastChanged) AS problem_date,
    DATE(PatHistProblemListHistory.ProblemStartDate) AS problem_onset_date,
    DATE(PatHistProblemListHistory.DateResolved) AS problem_end_date,
    NULL AS resolved,
    PatHistItem.PatHistItemDescription AS problem_desc,
    CASE WHEN AltSystem LIKE '%SNO%' THEN PatHistItem.AltSystemCode ELSE NULL END AS snomed_code,
        CASE WHEN AltSystem LIKE '%ICD%' THEN PatHistItem.AltSystemCode ELSE NULL END AS icd_code,
    NULL AS problem_type,
    ProblemListStatus.ProblemListStatusDesc AS status,
    NULL AS severity,
    NULL AS laterality,
    PatHistProblemListHistory.ProblemNote AS problem_notes,
    "Greenway" AS data_source,
    12 as psid,
    null as nd_extracted_date
FROM PatHistProblemListHistory
LEFT JOIN PatHistCatPatHistItem
    ON PatHistCatPatHistItem.PatHistCatPatHistItemID = PatHistProblemListHistory.PatHistCatPatHistItemID
LEFT JOIN PatHistItem
    ON PatHistItem.PatHistItemID = PatHistCatPatHistItem.PatHistItemID
LEFT JOIN ProblemListStatus
    ON ProblemListStatus.ProblemListStatusID = PatHistProblemListHistory.ProblemListStatusID;