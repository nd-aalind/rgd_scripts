SELECT
    pl.SlNo AS problem_id,
    pl.patientID AS ndid,
    -- case when pl.encounterID in ('','none','Null') then null else trim(pl.encounterID) end AS eid,
    pl.encounterid as eid,
    enc.date AS encounter_date,
    pl.AddedDate AS problem_date,
    pl.onsetdate AS problem_onset_date,
    pl.resolvedon AS problem_end_date,
    pl.Resolved AS resolved,
    plg.item AS icd_code,
    plg.itemName as problem_desc,
    pl.problemtype AS problem_type,
    pl.WUStatus AS status,
    pl.Risk AS severity,
    NULL AS laterality,
    pl.notes AS problem_notes,
    'PATIENTPROBLEM' AS data_source,
    CURRENT_TIMESTAMP() AS created_datetime,
    'ND' AS created_by,
    'eCW' AS ehr_source_name,
    'bronze_layer' AS source_path,
    'Structured' AS data_type,
    '' AS psid
FROM problemlist pl
left join problemlistlog plg
on plg.patientid=pl.patientid
and plg.id=pl.SlNo
and plg.encounterid=pl.encounterId and plg.nd_ActiveFlag='Y' 
inner JOIN enc 
ON enc.encounterid = pl.encounterId and enc.nd_ActiveFlag='Y'
where pl.nd_ActiveFlag='Y';