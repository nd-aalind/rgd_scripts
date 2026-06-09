UPDATE rgd_udm_silver.patients a
LEFT JOIN semantics.ethnicity b1 
    ON a.pat_ethnicity_code = b1.Code
LEFT JOIN semantics.ethnicity b2 
    ON LOWER(a.pat_ethnicity) = LOWER(b2.ethnicity)
SET
    a.pat_ethnicity_code_std = CASE 
        WHEN b1.Code IS NOT NULL THEN b1.Code
        WHEN b2.Code IS NOT NULL THEN b2.Code
        ELSE 'Flagged'
    END,
    a.pat_ethnicity_std = CASE 
        WHEN b1.Code IS NOT NULL THEN b1.ethnicity
        WHEN b2.Code IS NOT NULL THEN b2.ethnicity
        ELSE 'Flagged'
    END;
