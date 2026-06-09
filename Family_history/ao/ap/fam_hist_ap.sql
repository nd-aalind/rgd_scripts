SELECT
    FamilyHealthHistory.FamilyHealthHistoryID AS family_hist_id,
    FamilyHealthHistory.PID AS ndid,
    NULL AS eid,
    NULL AS encounter_date,
    NULL AS onset_date,
    null as onset_age,
    SignedDate AS family_hist_date,
    'Family History' AS hist_category,
    FHxRelationship.Relation AS fam_hist_relation,
    FHxRelationship.Code as family_relationship_code,
    FamilyHealthHistory.Description AS family_hist_details,
    MasterDiagnosis.Code AS family_hist_code,
    'SNOMED' AS family_hist_coding_system,
    FamilyHealthHistory.FHxComments AS family_hist_notes,
    current_date() as created_datetime,
    'ND' as created_by,
     current_date() as updated_datetime,
     'ND' as updated_by,
    'Athenaone' as ehr_source_name,
    'bronze_layer' as source_path,
    'Structured' as data_type,
    '' as psid,
    FamilyHealthHistory.nd_extracted_date as nd_extracted_date
FROM 
FamilyHealthHistory
LEFT JOIN FHxRelationship
    ON FamilyHealthHistory.FHxRelationshipID = FHxRelationship.FHxRelationshipID
    and FamilyHealthHistory.nd_Activeflag = 'Y' and FHxRelationship.nd_Activeflag = 'Y'
LEFT JOIN MasterDiagnosis
    ON FamilyHealthHistory.SnomedMasterDiagnosisID = MasterDiagnosis.MasterDiagnosisID
    and MasterDiagnosis.nd_Activeflag = 'Y'
WHERE
    FamilyHealthHistory.Inactive = 'N'
    AND FamilyHealthHistory.FiledInError = 'N';