UPDATE rgd_udm_silver.patients
SET
    gender_hl7_std = CASE
        WHEN LOWER(TRIM(gender)) IN ('male', 'm') THEN 'Male' -- added trim
        WHEN LOWER(TRIM(gender)) IN ('female', 'f') THEN 'Female'
        WHEN LOWER(TRIM(gender)) LIKE 'oth%' OR LOWER(TRIM(gender)) LIKE 'x%' 
                OR LOWER(TRIM(gender)) LIKE 'ambi%' THEN 'Other'
        WHEN LOWER(TRIM(gender)) LIKE 'u%' THEN 'Unknown'
        WHEN gender IS NULL OR TRIM(gender) = '' THEN 'Unknown'
        ELSE 'NS' -- flag for non-standardized values
    END,

    gender_CDISC_std = CASE
        WHEN LOWER(TRIM(gender)) IN ('male', 'm') THEN 'M'
        WHEN LOWER(TRIM(gender)) IN ('female', 'f') THEN 'F'
        WHEN LOWER(TRIM(gender)) LIKE 'oth%' OR LOWER(TRIM(gender)) LIKE 'x%' THEN 'Undifferentiated'
        WHEN LOWER(TRIM(gender)) LIKE 'u%' THEN NULL
        WHEN gender IS NULL OR TRIM(gender) = '' THEN NULL
        ELSE 'NS' -- flag for un-standardized values
    END,

    gender_OMOP_std = CASE
        WHEN LOWER(TRIM(gender)) IN ('male', 'm') THEN 'MALE'
        WHEN LOWER(TRIM(gender)) IN ('female', 'f') THEN 'FEMALE'
        WHEN LOWER(TRIM(gender)) LIKE 'x%' OR LOWER(TRIM(gender)) LIKE 'ambi%' THEN 'AMBIGUOUS'
        WHEN LOWER(TRIM(gender)) LIKE 'oth%' THEN 'OTHER'
        WHEN LOWER(TRIM(gender)) LIKE 'u%' THEN 'UNKNOWN'
        WHEN gender IS NULL OR TRIM(gender) = '' THEN 'UNKNOWN'
        ELSE 'NS' -- flag for un-standardized values
    END,
    
    gender_OMOP_concept_id = CASE
        WHEN LOWER(TRIM(gender)) IN ('male', 'm') THEN 8507
        WHEN LOWER(TRIM(gender)) IN ('female', 'f') THEN 8532
        WHEN LOWER(TRIM(gender)) LIKE 'oth%' THEN 8521
        WHEN LOWER(TRIM(gender)) LIKE 'x%' OR LOWER(TRIM(gender)) LIKE 'ambi%' THEN 8570
        WHEN LOWER(TRIM(gender)) LIKE 'u%' THEN 8551
        WHEN gender IS NULL OR TRIM(gender) = '' THEN 8551
        ELSE 'NS' -- flag for un-standardized values
    END;