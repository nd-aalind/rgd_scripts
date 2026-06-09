-- Update 1:

UPDATE rgd_udm_silver.medications m
SET 
    med_code_std = n.NDC,
    med_name_std_LN = n.LN60,
    med_name_std_BN = n.BN,
    mapping_type = 'Exact match (FDB-NDC)',
    conf = 1.0
FROM rgd_udm_silver.medications m
JOIN FDB.RNDC14_NDC_MSTR n ON REPLACE(m.med_code, '-', '') = REPLACE(n.NDC, '-', '');

-- Update 2:

-- Update medication names with exact matches from FDB using LN60
UPDATE rgd_udm_silver.medications m
SET 
    med_code_std = n.NDC,
    med_name_std_LN = n.LN60,
    med_name_std_BN = n.BN,
    mapping_type = 'Exact match (FDB-LN60)',
    conf = 1.0
FROM rgd_udm_silver.medications m
JOIN FDB.RNDC14_NDC_MSTR n ON UPPER(TRIM(m.med_name)) = UPPER(TRIM(n.LN60))
WHERE med_name_std_LN IS NULL AND mapping_type IS NULL;

-- Update medication names with exact matches from FDB using LN
UPDATE rgd_udm_silver.medications m
SET 
    med_code_std = n.NDC,
    med_name_std_LN = n.LN60,
    med_name_std_BN = n.BN,
    mapping_type = 'Exact match (FDB-LN)',
    conf = 1.0
FROM rgd_udm_silver.medications m
JOIN FDB.RNDC14_NDC_MSTR n ON UPPER(TRIM(m.med_name)) = UPPER(TRIM(n.LN))
WHERE med_name_std_LN IS NULL AND mapping_type IS NULL;

-- Update 3:

UPDATE rgd_udm_silver.medications m
SET 
    med_code_std   = n.NDC,
    med_name_std_LN = n.LN60,
    med_name_std_BN = n.BN,
    mapping_type   = 'Exact match (FDB-BN)',
    conf           = 1.0
FROM FDB.RNDC14_NDC_MSTR n
WHERE 
    UPPER(TRIM(m.med_name)) = UPPER(TRIM(n.BN))
    AND m.med_name_std_LN IS NULL
    AND m.mapping_type IS NULL

    /* --- Extract numeric strength from med_strength --- */
    AND TRY_TO_NUMBER(
        REGEXP_SUBSTR(LOWER(m.med_strength), '\\d+(\\.\\d+)?')
    )
    =
    /* --- Extract numeric strength from LN60 --- */
    TRY_TO_NUMBER(
        REGEXP_SUBSTR(LOWER(n.LN60), '\\d+(\\.\\d+)?')
    );

