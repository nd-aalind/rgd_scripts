SELECT
    PatHistProblemList.PatHistProblemListID AS diag_id,
    PatHistProblemList.PatientID AS ndid,
    NULL AS eid,
    NULL AS encounter_date,
    PatHistProblemList.LastChanged AS problem_date,
    PatHistProblemList.ProblemStartDate AS problem_onset_date,
    PatHistProblemList.DateResolved AS problem_end_date,
    PatHistProblemList.IsCurrent AS resolved,
    PatHistItem.PatHistItemDescription AS problem_desc,
    CASE WHEN AltSystem LIKE '%SNO%' THEN PatHistItem.AltSystemCode ELSE NULL END AS snomed_code,
        CASE WHEN AltSystem LIKE '%ICD%' THEN PatHistItem.AltSystemCode ELSE NULL END AS icd_code,
    NULL AS problem_type,
    ProblemListStatus.ProblemListStatusDesc AS status,
    NULL AS severity,
    NULL AS laterality,
    PatHistProblemList.ProblemNote AS problem_notes,
    "Greenway" AS data_source
FROM PatHistProblemList
LEFT JOIN PatHistCatPatHistItem
    ON PatHistCatPatHistItem.PatHistCatPatHistItemID = PatHistProblemList.PatHistCatPatHistItemID
LEFT JOIN PatHistItem
    ON PatHistItem.PatHistItemID = PatHistCatPatHistItem.PatHistItemID
LEFT JOIN ProblemListStatus
    ON ProblemListStatus.ProblemListStatusID = PatHistProblemList.ProblemListStatusID
UNION ALL
SELECT
    PatHistProblemListHistory.PatHistProblemListID AS diag_id,
    PatHistProblemListHistory.PatientID AS ndid,
    NULL AS eid,
    NULL AS encounter_date,
    PatHistProblemListHistory.LastChanged AS problem_date,
    PatHistProblemListHistory.ProblemStartDate AS problem_onset_date,
    PatHistProblemListHistory.DateResolved AS problem_end_date,
    NULL AS resolved,
    PatHistItem.PatHistItemDescription AS problem_desc,
    CASE WHEN AltSystem LIKE '%SNO%' THEN PatHistItem.AltSystemCode ELSE NULL END AS snomed_code,
        CASE WHEN AltSystem LIKE '%ICD%' THEN PatHistItem.AltSystemCode ELSE NULL END AS icd_code,
    NULL AS problem_type,
    ProblemListStatus.ProblemListStatusDesc AS status,
    NULL AS severity,
    NULL AS laterality,
    PatHistProblemListHistory.ProblemNote AS problem_notes,
    "Greenway" AS data_source
FROM PatHistProblemListHistory
LEFT JOIN PatHistCatPatHistItem
    ON PatHistCatPatHistItem.PatHistCatPatHistItemID = PatHistProblemListHistory.PatHistCatPatHistItemID
LEFT JOIN PatHistItem
    ON PatHistItem.PatHistItemID = PatHistCatPatHistItem.PatHistItemID
LEFT JOIN ProblemListStatus
    ON ProblemListStatus.ProblemListStatusID = PatHistProblemListHistory.ProblemListStatusID
UNION ALL
SELECT
    ClinicalProblemList.problemid AS diag_id,
    ClinicalProblemListPatient.PatientID AS ndid,
    NULL AS eid,
    NULL AS encounter_date,
    ClinicalProblemList.dateadded AS problem_date,
    NULL AS problem_onset_date,
    NULL AS problem_end_date,
    ClinicalProblemList.enabled AS resolved,
    ClinicalProblemList.problem AS problem_desc,
    ClinicalProblemList.snomed AS snomed_code,
    COALESCE(ClinicalProblemList.icd10, ClinicalProblemList.icd9) AS icd_code,
    NULL AS problem_type,
    NULL AS status,
    NULL AS severity,
    NULL AS laterality,
    ClinicalProblemList.notes AS problem_notes,
    "Greenway 3" AS data_source
FROM ClinicalProblemList
LEFT JOIN ClinicalProblemListPatient
    ON ClinicalProblemListPatient.problemid = ClinicalProblemList.problemid;