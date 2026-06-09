UPDATE rgd_udm_silver.patients a
LEFT JOIN semantics.race b1 ON a.pat_race_code = b1.Code
LEFT JOIN semantics.race b2 ON LOWER(a.pat_race) = LOWER(b2.race)
SET 
    a.pat_race_code_std = CASE 
        WHEN b1.Code IS NOT NULL THEN b1.Code
        WHEN b2.Code IS NOT NULL THEN b2.Code
        ELSE 'Flagged'
    END,
    a.pat_race_std = CASE 
        WHEN b1.Code IS NOT NULL THEN b1.race
        WHEN b2.Code IS NOT NULL THEN b2.race
        ELSE 'Flagged'
    END;

