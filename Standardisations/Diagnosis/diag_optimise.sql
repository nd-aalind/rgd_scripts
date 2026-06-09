/* =========================================================
   STEP 0 — CLEANUP (optional if rerunning)
   ========================================================= */
DROP TEMPORARY TABLE IF EXISTS tmp_diag_base;
DROP TEMPORARY TABLE IF EXISTS tmp_icd10;
DROP TEMPORARY TABLE IF EXISTS tmp_icd10f;
DROP TEMPORARY TABLE IF EXISTS tmp_icd9;
DROP TEMPORARY TABLE IF EXISTS tmp_icd9f;
DROP TEMPORARY TABLE IF EXISTS tmp_diag_mapped;


/* =========================================================
   STEP 1 — BASE TABLE (normalize diag_code once)
   ========================================================= */
CREATE TEMPORARY TABLE tmp_diag_base AS
SELECT 
    d.udm_inc_id,
    d.diag_code,
    d.diag_desc,
    d.diag_coding_system,
    UPPER(REPLACE(d.diag_code, '.', '')) AS diag_code_clean
FROM kinsula_leq.diagnosis d;

CREATE INDEX idx_tmp_diag_code ON tmp_diag_base(diag_code_clean);


/* =========================================================
   STEP 2 — NORMALIZE LOOKUP TABLES
   ========================================================= */

CREATE TEMPORARY TABLE tmp_icd10 AS
SELECT 
    REPLACE(diagnosis_code, '.', '') AS code,
    LONG_DESCRIPTION
FROM semantics.icd10cm_with_parent;

CREATE INDEX idx_tmp_icd10 ON tmp_icd10(code);


CREATE TEMPORARY TABLE tmp_icd10f AS
SELECT 
    REPLACE(code, '.', '') AS code,
    LONG_DESCRIPTION
FROM semantics.icd10_fixed;

CREATE INDEX idx_tmp_icd10f ON tmp_icd10f(code);


CREATE TEMPORARY TABLE tmp_icd9 AS
SELECT 
    REPLACE(diagnosis_code, '.', '') AS code,
    LONG_DESCRIPTION
FROM semantics.icd9cm_lookup;

CREATE INDEX idx_tmp_icd9 ON tmp_icd9(code);


CREATE TEMPORARY TABLE tmp_icd9f AS
SELECT 
    REPLACE(diagnosis_code, '.', '') AS code,
    LONG_DESCRIPTION
FROM semantics.icd9_fixed;

CREATE INDEX idx_tmp_icd9f ON tmp_icd9f(code);


/* =========================================================
   STEP 3 — JOIN ONCE (central mapping)
   ========================================================= */
CREATE TEMPORARY TABLE tmp_diag_mapped AS
SELECT
    d.*,
    COALESCE(icd10.LONG_DESCRIPTION, icd10f.LONG_DESCRIPTION) AS icd10_desc,
    COALESCE(icd9.LONG_DESCRIPTION, icd9f.LONG_DESCRIPTION) AS icd9_desc
FROM tmp_diag_base d
LEFT JOIN tmp_icd10 icd10 ON d.diag_code_clean = icd10.code
LEFT JOIN tmp_icd10f icd10f ON d.diag_code_clean = icd10f.code
LEFT JOIN tmp_icd9 icd9 ON d.diag_code_clean = icd9.code
LEFT JOIN tmp_icd9f icd9f ON d.diag_code_clean = icd9f.code;

CREATE INDEX idx_tmp_diag_map_id ON tmp_diag_mapped(udm_inc_id);


/* =========================================================
   UPDATE 1 — STANDARD DESCRIPTION + CODING SYSTEM
   ========================================================= */
UPDATE kinsula_leq.diagnosis d
JOIN tmp_diag_mapped m 
  ON d.udm_inc_id = m.udm_inc_id

SET
    d.diag_desc_std = CASE 
        WHEN d.diag_code IS NULL OR d.diag_code='' THEN NULL
        WHEN m.icd9_desc IS NOT NULL AND m.icd10_desc IS NOT NULL 
            THEN 'Matching both ICD-9 and ICD-10'
        ELSE COALESCE(m.icd10_desc, m.icd9_desc, 'NS')
    END,

    d.diag_coding_system_std = CASE 
        WHEN d.diag_code IS NULL OR d.diag_code='' THEN NULL
        WHEN m.icd10_desc IS NOT NULL AND m.icd9_desc IS NULL THEN 'ICD-10'
        WHEN m.icd9_desc IS NOT NULL AND m.icd10_desc IS NULL THEN 'ICD-9'
        WHEN m.icd9_desc IS NOT NULL AND m.icd10_desc IS NOT NULL 
            THEN 'Matching both ICD-9 and ICD-10'
        ELSE 'NS'
    END;


/* =========================================================
   UPDATE 2 — FLAGGED CASES (V/E logic override)
   ========================================================= */
UPDATE kinsula_leq.diagnosis d
JOIN tmp_diag_mapped m 
  ON d.udm_inc_id = m.udm_inc_id

SET
    d.diag_desc_std = CASE
        WHEN m.diag_code_clean LIKE 'V%' AND LENGTH(m.diag_code_clean) > 5
            THEN m.icd10_desc
        WHEN m.diag_code_clean RLIKE '^E[0-9]{2}(\\.[0-9]{1,4})?$'
            THEN m.icd10_desc
        WHEN m.diag_code_clean LIKE 'V%' AND LENGTH(m.diag_code_clean) <= 5
            THEN m.icd9_desc
        WHEN m.diag_code_clean RLIKE '^E[0-9]{3}(\\.[0-9]+)?$'
            THEN m.icd9_desc
        ELSE d.diag_desc_std
    END,

    d.diag_coding_system_std = CASE
        WHEN m.diag_code_clean LIKE 'V%' AND LENGTH(m.diag_code_clean) > 5 THEN 'ICD-10'
        WHEN m.diag_code_clean LIKE 'V%' AND LENGTH(m.diag_code_clean) <= 5 THEN 'ICD-9'
        WHEN m.diag_code_clean RLIKE '^E[0-9]{3}(\\.[0-9]+)?$' THEN 'ICD-9'
        WHEN m.diag_code_clean RLIKE '^E[0-9]{2}(\\.[0-9]{1,4})?$' THEN 'ICD-10'
        ELSE d.diag_coding_system_std
    END

WHERE d.diag_coding_system_std = 'Matching both ICD-9 and ICD-10';


/* =========================================================
   UPDATE 3 — PRIMARY DIAGNOSIS FLAG (SAFE VERSION)
   ========================================================= */
UPDATE rgd_udm_silver.diagnosis a
SET 
    a.primary_diagnosis_flag_std = CASE 
        WHEN LOWER(a.ehr_source_name) = 'ecw' AND a.primary_diagnosis_flag = '1' THEN 'Y'
        WHEN LOWER(a.ehr_source_name) = 'ecw' AND a.primary_diagnosis_flag = '0' THEN 'N'

        WHEN LOWER(a.ehr_source_name) = 'athenaone' AND a.primary_diagnosis_flag = '0' THEN 'Y' 
        WHEN LOWER(a.ehr_source_name) = 'athenaone' AND a.primary_diagnosis_flag IN ('1','9') THEN 'N'

        WHEN LOWER(a.ehr_source_name) IN ('greenway','athenapractice') AND a.primary_diagnosis_flag = '1' THEN 'Y' 
        WHEN LOWER(a.ehr_source_name) IN ('greenway','athenapractice') AND a.primary_diagnosis_flag IN ('0','9') THEN 'N'

        WHEN LOWER(a.primary_diagnosis_flag) = 'y' THEN 'Y'
        WHEN LOWER(a.primary_diagnosis_flag) = 'n' THEN 'N'
        WHEN a.primary_diagnosis_flag IS NULL THEN NULL
        ELSE 'NS'
    END;