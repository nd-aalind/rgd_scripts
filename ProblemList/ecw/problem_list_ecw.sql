
INSERT INTO target_schema.problemlist (
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
    CAST(problemlist.SlNo AS SIGNED) AS diag_id,
    CAST(problemlist.patientID AS SIGNED) AS ndid,
    CAST(problemlist.encounterID AS SIGNED) AS eid,
    date(enc.date) AS encounter_date,
    date(problemlist.AddedDate) AS problem_date,
    date(problemlist.onsetdate) AS problem_onset_date,
    date(problemlist.resolvedon) AS problem_end_date,
    CAST(problemlist.Resolved AS SIGNED) AS resolved,
    CAST(problemlist.SNOMEDDesc AS CHAR) AS problem_desc,
    CAST(problemlist.SNOMED AS CHAR(50)) AS snomed_code,
    CAST(icd_synonyms.ICD9_SNOMEDCT_IMO AS CHAR(50)) AS icd_code,
    CAST(problemlist.problemtype AS CHAR(100)) AS problem_type,
    CAST(problemlist.WUStatus AS CHAR(100)) AS status,
    CAST(problemlist.Risk AS CHAR(100)) AS severity,
    NULL AS laterality,
    CAST(problemlist.notes AS CHAR) AS problem_notes,
    'eCW' AS data_source,
    3 AS psid,
    NULL AS nd_extracted_date
FROM problemlist
LEFT JOIN enc 
    ON enc.encounterid = problemlist.encounterID;
LEFT JOIN icd_synonyms 
    ON problemlist.synonymid = icd_synonyms.id;