SELECT Distinct 
    pat_marital_status,

    CASE 
        WHEN UPPER(TRIM(pat_marital_status)) = 'SEPARATED' THEN 'Separated'
        WHEN UPPER(TRIM(pat_marital_status)) = 'DIVORCED' THEN 'Divorced'
        WHEN UPPER(TRIM(pat_marital_status)) = 'MARRIED' THEN 'Married'
        WHEN UPPER(TRIM(pat_marital_status)) = 'SINGLE' THEN 'Single'
        WHEN UPPER(TRIM(pat_marital_status)) = 'WIDOWED' THEN 'Widowed'
        WHEN UPPER(TRIM(pat_marital_status)) = 'COMMON LAW' THEN 'Common law'
        WHEN UPPER(TRIM(pat_marital_status)) = 'LIVING TOGETHER' THEN 'Living together'
        WHEN UPPER(TRIM(pat_marital_status)) IN ('DOMESTIC PARTNER','PARTNER') THEN 'Domestic partner'
        WHEN UPPER(TRIM(pat_marital_status)) = 'REGISTERED DOMESTIC PARTNER' THEN 'Registered domestic partner'
        WHEN UPPER(TRIM(pat_marital_status)) IN ('LEGALLY SEPARATED','LEGALLY SEPERATED') THEN 'Legally Separated'
        WHEN UPPER(TRIM(pat_marital_status)) = 'ANNULLED' THEN 'Annulled'
        WHEN UPPER(TRIM(pat_marital_status)) = 'INTERLOCUTORY' THEN 'Interlocutory'
        WHEN UPPER(TRIM(pat_marital_status)) = 'UNMARRIED' THEN 'Unmarried'
        WHEN UPPER(TRIM(pat_marital_status)) = 'UNKNOWN' THEN 'Unknown'
        WHEN UPPER(TRIM(pat_marital_status)) = 'OTHER' THEN 'Other'
        WHEN UPPER(TRIM(pat_marital_status)) = 'UNREPORTED' THEN 'Unreported'
        WHEN TRIM(pat_marital_status) = '' OR pat_marital_status IS NULL THEN 'Unknown' -- handling null cases
    ELSE 'NS'
    END AS pat_marital_status_std
    FROM rgd_udm_silver.patient_demographics;