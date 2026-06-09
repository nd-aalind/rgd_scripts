-- UPDATE 1

SELECT DISTINCT
	d.diag_code,
	d.diag_desc, 
    d.diag_coding_system,
	CASE WHEN d.diag_code IS NULL OR d.diag_code='' THEN NULL -- added new
		WHEN COALESCE(icd9.LONG_DESCRIPTION, icd9f.LONG_DESCRIPTION) IS NOT NULL 
			AND COALESCE(icd10.LONG_DESCRIPTION, icd10f.LONG_DESCRIPTION) IS NOT NULL THEN 'Matching both ICD-9 and ICD-10'
		ELSE COALESCE(icd10.LONG_DESCRIPTION, icd10f.LONG_DESCRIPTION, icd9.LONG_DESCRIPTION, icd9f.LONG_DESCRIPTION, 'NS') 
        END AS diag_desc_std,
	CASE WHEN diag_code IS NULL OR TRIM(diag_code) = '' THEN NULL -- no diag code, no coding system
		WHEN (icd10.LONG_DESCRIPTION IS NOT NULL OR icd10f.LONG_DESCRIPTION IS NOT NULL)
			 AND (icd9.LONG_DESCRIPTION IS NULL AND icd9f.LONG_DESCRIPTION IS NULL) THEN 'ICD-10'
		WHEN (icd9.LONG_DESCRIPTION IS NOT NULL OR icd9f.LONG_DESCRIPTION IS NOT NULL)
			 AND (icd10.LONG_DESCRIPTION IS NULL AND icd10f.LONG_DESCRIPTION IS NULL) THEN 'ICD-9'
		WHEN (icd9.LONG_DESCRIPTION IS NOT NULL OR icd9f.LONG_DESCRIPTION IS NOT NULL)
			AND (icd10.LONG_DESCRIPTION IS NOT NULL OR icd10f.LONG_DESCRIPTION IS NOT NULL) THEN 'Matching both ICD-9 and ICD-10'
		ELSE 'NS'
	END AS diag_coding_system_std
FROM kinsula_leq.diagnosis d
LEFT JOIN semantics.icd10cm_with_parent icd10
	ON UPPER(REPLACE(d.diag_code, '.', '')) = REPLACE(icd10.diagnosis_code, '.', '')
LEFT JOIN semantics.icd10_fixed icd10f
	ON UPPER(REPLACE(d.diag_code, '.', '')) = REPLACE(icd10f.code, '.', '')
LEFT JOIN semantics.icd9cm_lookup icd9
	ON UPPER(REPLACE(d.diag_code, '.', '')) = REPLACE(icd9.diagnosis_code, '.', '')
LEFT JOIN semantics.icd9_fixed icd9f
	ON UPPER(REPLACE(d.diag_code, '.', '')) = REPLACE(icd9f.diagnosis_code, '.', '');


-- UPDATE 2

-- flagged cases
SELECT DISTINCT
	d.diag_code,
	d.diag_desc, 
    d.diag_coding_system,
	/* Routed ICD-10 description */
	CASE
		/* ICD-10 V codes (long) */
		WHEN UPPER(REPLACE(d.diag_code,'.','')) LIKE 'V%'
			 AND LENGTH(REPLACE(d.diag_code,'.','')) > 5
		THEN COALESCE(icd10.LONG_DESCRIPTION, icd10f.LONG_DESCRIPTION)
		/* ICD-10 E codes */
		WHEN UPPER(d.diag_code) RLIKE '^E[0-9]{2}(\\.[0-9]{1,4})?$'
		THEN COALESCE(icd10.LONG_DESCRIPTION, icd10f.LONG_DESCRIPTION)
		
		/* ICD-9 V codes (short) */
		WHEN UPPER(REPLACE(d.diag_code,'.','')) LIKE 'V%'
			 AND LENGTH(REPLACE(d.diag_code,'.','')) <= 5
		THEN COALESCE(icd9.LONG_DESCRIPTION, icd9f.LONG_DESCRIPTION)
		/* ICD-9 E codes */
		WHEN UPPER(d.diag_code) RLIKE '^E[0-9]{3}(\\.[0-9]+)?$'
		THEN COALESCE(icd9.LONG_DESCRIPTION, icd9f.LONG_DESCRIPTION)
        
        ELSE 'NS'
	END AS diag_desc_std,
	/* Routed coding system */
	CASE
		WHEN UPPER(REPLACE(d.diag_code,'.','')) LIKE 'V%'
			 AND LENGTH(REPLACE(d.diag_code,'.','')) > 5
		THEN 'ICD-10'
		WHEN UPPER(REPLACE(d.diag_code,'.','')) LIKE 'V%'
			 AND LENGTH(REPLACE(d.diag_code,'.','')) <= 5
		THEN 'ICD-9'
		WHEN UPPER(d.diag_code) RLIKE '^E[0-9]{3}(\\.[0-9]+)?$'
		THEN 'ICD-9'
		WHEN UPPER(d.diag_code) RLIKE '^E[0-9]{2}(\\.[0-9]{1,4})?$'
		THEN 'ICD-10'
		ELSE 'NS'
	END AS diag_coding_system_std
FROM kinsula_leq.diagnosis d
LEFT JOIN semantics.icd10cm_with_parent icd10
	ON UPPER(REPLACE(d.diag_code, '.', '')) = REPLACE(icd10.diagnosis_code, '.', '')
LEFT JOIN semantics.icd10_fixed icd10f
	ON UPPER(REPLACE(d.diag_code, '.', '')) = REPLACE(icd10f.code, '.', '')
LEFT JOIN semantics.icd9cm_lookup icd9
	ON UPPER(REPLACE(d.diag_code, '.', '')) = REPLACE(icd9.diagnosis_code, '.', '')
LEFT JOIN semantics.icd9_fixed icd9f
	ON UPPER(REPLACE(d.diag_code, '.', '')) = REPLACE(icd9f.diagnosis_code, '.', '')
WHERE d.diag_coding_system_std = 'Matching both ICD-9 and ICD-10';


-- UPDATE 3

UPDATE rgd_udm_silver.diagnosis a
SET 
    a.primary_diagnosis_flag_std = CASE 
		-- PrimaryCode	tinyint	A tiny integer indicating whether the diagnosis code is the primary code (1) or not (0). Example: 0, 1.
        WHEN LOWER(a.ehr_source_name) = 'ecw' AND a.primary_diagnosis_flag IN (1,'1') THEN 'Y'
        WHEN LOWER(a.ehr_source_name) = 'ecw' AND a.primary_diagnosis_flag IN (0,'0') THEN 'N'
        -- Ordering	number	The ordering of the diagnosis code in the encounter, starts from 0
        WHEN LOWER(a.ehr_source_name) = 'athenaone' AND a.primary_diagnosis_flag IN (0,'0') THEN 'Y' 
        WHEN LOWER(a.ehr_source_name) = 'athenaone' AND CAST(a.primary_diagnosis_flag AS UNSIGNED) > 0 THEN 'N'
        -- GW: Priority	tinyint	NO	This column store priority of Diagnosis, starts from 1
        -- AP: ListOrder smallint The order in which diagnoses should be displayed, starts from 1
		WHEN LOWER(a.ehr_source_name) IN ('greenway', 'athenapractice') AND a.primary_diagnosis_flag IN (1,'1') THEN 'Y' 
        WHEN LOWER(a.ehr_source_name) IN ('greenway', 'athenapractice') AND CAST(a.primary_diagnosis_flag AS UNSIGNED) > 1 THEN 'N'
        -- general
        WHEN a.primary_diagnosis_flag IN ('y','Y') THEN 'Y'
        WHEN a.primary_diagnosis_flag IN ('n','N') THEN 'N'
        WHEN a.primary_diagnosis_flag IS NULL THEN NULL
        ELSE 'NS'
    END;