UPDATE udm_staging.patient_demographics
SET
    gender_hl7_std = CASE
        WHEN LOWER(gender) IN ('male', 'm') THEN 'Male'
        WHEN LOWER(gender) IN ('female', 'f') THEN 'Female'
        WHEN LOWER(gender) LIKE 'oth%' OR LOWER(gender) LIKE 'x%' 
                OR LOWER(gender) LIKE 'ambi%' THEN 'Other'
        WHEN LOWER(gender) LIKE 'unk%' THEN 'Unknown'
        WHEN gender IS NULL OR TRIM(gender) = '' THEN 'Unknown'
        ELSE 'Flagged' -- flag for un-standrdized values
    END,
    gender_CDISC_std = CASE
        WHEN LOWER(gender) IN ('male', 'm') THEN 'M'
        WHEN LOWER(gender) IN ('female', 'f') THEN 'F'
        WHEN LOWER(gender) LIKE 'oth%' OR LOWER(gender) LIKE 'x%' THEN 'Undifferentiated'
        WHEN LOWER(gender) LIKE 'unk%' THEN NULL
        WHEN gender IS NULL OR TRIM(gender) = '' THEN NULL
        ELSE 'Flagged' -- flag for un-standrdized values
    END,
    gender_OMOP_std = CASE
        WHEN LOWER(gender) IN ('male', 'm') THEN 'MALE'
        WHEN LOWER(gender) IN ('female', 'f') THEN 'FEMALE'
        WHEN LOWER(gender) LIKE 'x%' OR LOWER(gender) LIKE 'ambi%' THEN 'AMBIGUOUS'
        WHEN LOWER(gender) LIKE 'oth%' THEN 'OTHER'
        WHEN LOWER(gender) LIKE 'unk%' THEN 'UNKNOWN'
        WHEN gender IS NULL OR TRIM(gender) = '' THEN 'UNKNOWN'
        ELSE 'Flagged' -- flag for un-standrdized values
    END,
    gender_OMOP_concept_id = CASE
        WHEN LOWER(gender) IN ('male', 'm') THEN 8507
        WHEN LOWER(gender) IN ('female', 'f') THEN 8532
        WHEN LOWER(gender) LIKE 'oth%' THEN 8521
        WHEN LOWER(gender) LIKE 'x%' OR LOWER(gender) LIKE 'ambi%' THEN 8570
        WHEN LOWER(gender) LIKE 'unk%' THEN 8551
        WHEN gender IS NULL OR TRIM(gender) = '' THEN 8551
        ELSE 'Flagged' -- flag for un-standrdized values
    END;