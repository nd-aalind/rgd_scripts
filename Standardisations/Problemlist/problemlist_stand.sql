-- update 1:

SELECT DISTINCT
	p.icd_code,
	CASE WHEN COALESCE(icd9.LONG_DESCRIPTION, icd9f.LONG_DESCRIPTION) IS NOT NULL 
			AND COALESCE(icd10.LONG_DESCRIPTION, icd10f.LONG_DESCRIPTION) IS NOT NULL THEN 'Matching both ICD-9 and ICD-10'
		ELSE COALESCE(icd10.LONG_DESCRIPTION, icd10f.LONG_DESCRIPTION, icd9.LONG_DESCRIPTION, icd9f.LONG_DESCRIPTION, 'NS') 
        END AS problem_desc_std -- added column, set update
FROM udm_staging.problemlist p
LEFT JOIN semantics.icd10cm_with_parent icd10
	ON UPPER(p.icd_code) = REPLACE(icd10.diagnosis_code, '.', '')
LEFT JOIN semantics.icd10_fixed icd10f
	ON UPPER(p.icd_code) = REPLACE(icd10f.code, '.', '')
LEFT JOIN semantics.icd9cm_lookup icd9
	ON UPPER(p.icd_code) = REPLACE(icd9.diagnosis_code, '.', '')
LEFT JOIN semantics.icd9_fixed icd9f
	ON UPPER(p.icd_code) = REPLACE(icd9f.diagnosis_code, '.', '')
WHERE (p.icd_code IS NOT NULL OR TRIM(p.icd_code) <> ""); -- only where icd code is present

-- Update 2: 

WITH snomed_max AS (
    SELECT 
        conceptId,
        MAX(id) AS latest_snomed_id
	FROM semantics.snomed
    WHERE active = 1
    GROUP BY conceptId
)

SELECT DISTINCT p.snomed_code, p.problem_desc,
	COALESCE(s.term, 'NS') AS problem_desc_std -- update column
FROM udm_staging.problemlist p
LEFT JOIN snomed_max sm 
	ON p.snomed_code = sm.conceptId
LEFT JOIN semantics.snomed s 
	ON p.snomed_code = s.conceptId AND s.Id = sm.latest_snomed_id
WHERE (p.snomed_code IS NOT NULL OR TRIM(p.snomed_code) <> "");


-- Update 3:

-- change to update query to add mapped_icd_code_std and mapped_icd_desc columns
SELECT DISTINCT pl.problem_desc, pl.snomed_code, -- original columns
		REPLACE(map.maptarget,"?","") AS mapped_icd_code, -- added column
        map.maptargetname AS mapped_icd_desc -- added column
FROM udm_staging.problemlist pl
LEFT JOIN semantics.snomed_icd10_map map 
	ON pl.snomed_code = map.referencedcomponentid 
		AND map.mapcategoryname = 'MAP SOURCE CONCEPT IS PROPERLY CLASSIFIED';


-- Update 4:

SELECT pl.*, 
  ci.charlson_comorbidity, ei.elixhauser_comorbidity -- added columns
FROM pl
LEFT JOIN semantics.charlson_icd_map ci 
  ON REPLACE(COALESCE(pl.icd_code, pl.mapped_icd_code), ".", "") LIKE REPLACE(ci.icd_pattern, ".","")
LEFT JOIN semantics.elixhauser_icd_map ei 
  ON REPLACE(COALESCE(pl.icd_code, pl.mapped_icd_code), ".", "") LIKE REPLACE(ei.icd_pattern, ".","");



