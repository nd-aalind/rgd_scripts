-- Upadate 1

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


-- Upadate 2


-- CASE 1: where pat_race_code is not null, use the code as source of truth
-- CASE 2: where pat_race_code is null, fallback the race name as source of truth
UPDATE rgd_udm_silver.patient_demographics a
LEFT JOIN semantics.race b1 ON a.pat_race_code = b1.Code
LEFT JOIN semantics.race b2 ON LOWER(a.pat_race) = LOWER(b2.race)
SET 
    a.pat_race_code_std = CASE 
        WHEN b1.Code IS NOT NULL THEN b1.Code
        WHEN b2.Code IS NOT NULL THEN b2.Code
  -- when no race info is present in original data itself, convert Nulls to 'Unknown'
        WHEN (a.pat_race_code IS NULL OR TRIM(a.pat_race_code) = '') 
             AND (a.pat_race IS NULL OR TRIM(a.pat_race) = '' )
              THEN 'Unknown'
        ELSE 'NS'
    END,
    
    a.pat_race_std = CASE 
        WHEN b1.Code IS NOT NULL THEN b1.race
        WHEN b2.Code IS NOT NULL THEN b2.race
  -- when no race info is present in original data itself, convert Nulls to 'Unknown'
        WHEN (a.pat_race_code IS NULL OR TRIM(a.pat_race_code) = '') 
             AND (a.pat_race IS NULL OR TRIM(a.pat_race) = '' )
              THEN 'Unknown'
        ELSE 'NS'
    END;


-- Upadate 3

-- Cleaning up invalid codes/names
-- Flagging codes that do not match standards CPT and HCPCS
SELECT DISTINCT
    pat_race_code,
	pat_race,
    -- race code std
    CASE
        -- 2106-3 WHITE
        WHEN LOWER(pat_race) IN (
            'white','caucasian','caucasian/white','wwhite','whtie','wnite','whiet','whiite',
            'whitte','whtte','whjite','wjhite','wjote','wgite','whgite','whiteq','whte',
            'hungarian','moore','white/unsure','white,english','white,declined to specify',
            'white,english,declined to specify','english,declined to specify',
            'white,other race','european,english,cherokee,italian,polish'
        ) THEN '2106-3'

        -- 2054-5 BLACK OR AFRICAN AMERICAN
        WHEN LOWER(pat_race) IN (
            'black or african american','african american','african america',
            'afrcan american','african americian','african amercian',
            'african american/black','african american & caucasian','somalia',
            'black/white','black & hispanic','black/asian','black and sicilian',
            'black or african american (biracial)','black/white/indian',
            'black,declined to specify','black,other race',
            'black or african american,native hawaiian or other pacific islander',
            'black or african american,white','black or african american,white,black',
            'black or african american,declined to specify','african american,white',
            'african american,other race','african american,declined to specify',
            'african american,black'
        ) THEN '2054-5'

        -- 1002-5 AMERICAN INDIAN OR ALASKA NATIVE
        WHEN LOWER(pat_race) IN (
            'american indian or alaska nati','american indian or alaskan native',
            'native american indian','native american','indian',
            'white/american indian','white/black/american indian','native/white',
            'white,american indian or alaska native','white/spanish american indian',
            'white,spanish american indian'
        ) THEN '1002-5'

        -- 2028-9 ASIAN
        WHEN LOWER(pat_race) IN (
            'east indian','asian/indian','sikh','usikh',
            'white/asian','asian/white','white,asian',
            'white,american indian or alaska native,asian'
        ) THEN '2028-9'

        -- 2076-8 PACIFIC ISLANDER
        WHEN LOWER(pat_race) IN (
            'pacific islander'
        ) THEN '2076-8'

        -- 2118-8 MIDDLE EASTERN OR NORTH AFRICAN
        WHEN LOWER(pat_race) IN (
            'arabic','arab-palestinan','white/arabic','middle eastern',
            'other race~arabic','other race/pakistani',
            'other race/turkish','other race/hindu'
        ) THEN '2118-8'

        -- 2131-1 OTHER RACE
        WHEN LOWER(pat_race) IN (
            'hispanic','hispanic-puerto rican','hispanic/white','white/hispanic',
            'white/puerto rican','white & puerto rican','latina',
            'puerto rican','white/black','other race,declined to specify'
        ) THEN '2131-1'

        -- UNKNOWN / REFUSED / BAD DATA
        WHEN LOWER(pat_race) IN (
            'unreported/refused to report','declined to specify','patient declined',
            'unspecified','state prohibited','unknown','unkown','uknown','unknow',
            'unkonown','unknownc','declined','none-other','n/a',
            'na@dent.com','donotemail@dent.com','20181227@dentinstitue.com',
            'kath_lean1@yahoo.com','mrschowski@aol.com','ekwilos@hotmail.com',
            'e','w','h','o','u','c','r','osco'
        ) THEN 'UNK'

        -- MIXED / MULTIRACIAL
        WHEN LOWER(pat_race) IN (
            'bi-racial','biracial','multi racial','multiracial',
            'mixed-black/white','mixed'
        ) THEN 'NS'

        ELSE 'NS'
    END AS pat_race_code_std,
    
    -- race name std
	CASE    
        -- 2106-3 WHITE
        WHEN LOWER(pat_race) IN (
            'white','caucasian','caucasian/white','wwhite','whtie','wnite','whiet','whiite',
            'whitte','whtte','whjite','wjhite','wjote','wgite','whgite','whiteq','whte',
            'hungarian','moore','white/unsure','white,english','white,declined to specify',
            'white,english,declined to specify','english,declined to specify',
            'white,other race','european,english,cherokee,italian,polish'
        ) THEN 'White'

        -- 2054-5 BLACK OR AFRICAN AMERICAN
        WHEN LOWER(pat_race) IN (
            'black or african american','african american','african america',
            'afrcan american','african americian','african amercian',
            'african american/black','african american & caucasian','somalia',
            'black/white','black & hispanic','black/asian','black and sicilian',
            'black or african american (biracial)','black/white/indian',
            'black,declined to specify','black,other race',
            'black or african american,native hawaiian or other pacific islander',
            'black or african american,white','black or african american,white,black',
            'black or african american,declined to specify','african american,white',
            'african american,other race','african american,declined to specify',
            'african american,black'
        ) THEN 'Black or African American'

        -- 1002-5 AMERICAN INDIAN OR ALASKA NATIVE
        WHEN LOWER(pat_race) IN (
            'american indian or alaska nati','american indian or alaskan native',
            'native american indian','native american','indian',
            'white/american indian','white/black/american indian','native/white',
            'white,american indian or alaska native','white/spanish american indian',
            'white,spanish american indian'
        ) THEN 'American Indian or Alaska Native'

        -- 2028-9 ASIAN
        WHEN LOWER(pat_race) IN (
            'east indian','asian/indian','sikh','usikh',
            'white/asian','asian/white','white,asian',
            'white,american indian or alaska native,asian'
        ) THEN 'Asian'

        -- 2076-8 PACIFIC ISLANDER
        WHEN LOWER(pat_race) IN (
            'pacific islander'
        ) THEN 'Native Hawaiian or Other Pacific Islander'

        -- 2118-8 MIDDLE EASTERN OR NORTH AFRICAN
        WHEN LOWER(pat_race) IN (
            'arabic','arab-palestinan','white/arabic','middle eastern',
            'other race~arabic','other race/pakistani',
            'other race/turkish','other race/hindu'
        ) THEN 'Middle Eastern or North African'

        -- 2131-1 OTHER RACE
        WHEN LOWER(pat_race) IN (
            'hispanic','hispanic-puerto rican','hispanic/white','white/hispanic',
            'white/puerto rican','white & puerto rican','latina',
            'puerto rican','white/black','other race,declined to specify'
        ) THEN 'Other Race'

        -- UNKNOWN / REFUSED / BAD DATA
        WHEN LOWER(pat_race) IN (
            'unreported/refused to report','declined to specify','patient declined',
            'unspecified','state prohibited','unknown','unkown','uknown','unknow',
            'unkonown','unknownc','declined','none-other','n/a',
            'na@dent.com','donotemail@dent.com','20181227@dentinstitue.com',
            'kath_lean1@yahoo.com','mrschowski@aol.com','ekwilos@hotmail.com',
            'e','w','h','o','u','c','r','osco'
        ) THEN 'UNK'

        -- MIXED / MULTIRACIAL
        WHEN LOWER(pat_race) IN (
            'bi-racial','biracial','multi racial','multiracial',
            'mixed-black/white','mixed'
        ) THEN 'NS'

        ELSE 'NS'
    END AS pat_race_std

FROM rgd_udm_silver.patient_demographics
WHERE pat_race_code_std = 'NS'
   OR pat_race_std = 'NS';

-- Update 4 

-- CASE 1: where pat_ethnicty_code is not null, use the code as source of truth
-- CASE 2: where pat_ethnicty_code is null, fallback the race name as source of truth
SELECT DISTINCT a.pat_ethnicity_code, 
        CASE WHEN b1.Code IS NOT NULL THEN b1.Code
            WHEN b2.Code IS NOT NULL THEN b2.Code
    -- when no ethnicity info is present in original data itself, convert Nulls to 'Unknown'
        WHEN ((a.pat_ethnicity_code IS NULL OR TRIM(a.pat_ethnicity_code) = '') 
             AND (a.pat_ethnicity IS NULL OR TRIM(a.pat_ethnicity) = '' ))
             OR UPPER(TRIM(a.pat_ethnicity_code)) IN  ('ASKU') -- added code for "asked but unknown"
             OR LOWER(TRIM(a.pat_ethnicity_code)) LIKE '%declined%' 
             OR LOWER(TRIM(a.pat_ethnicity)) LIKE '%declined%' OR LOWER(TRIM(a.pat_ethnicity)) LIKE '%refused%' 
             OR LOWER(TRIM(a.pat_ethnicity)) LIKE '%unknown%' -- added unknown
              THEN 'Unknown'
            ELSE 'NS' END AS pat_ethnicity_code_std,
        a.pat_ethnicity, 
        CASE WHEN b1.Code IS NOT NULL THEN b1.ethnicity
            WHEN b2.Code IS NOT NULL THEN b2.ethnicity
      -- when no ethnicity info is present in original data itself, convert Nulls to 'Unknown'
        WHEN ((a.pat_ethnicity_code IS NULL OR TRIM(a.pat_ethnicity_code) = '') 
             AND (a.pat_ethnicity IS NULL OR TRIM(a.pat_ethnicity) = '' ))
             OR UPPER(TRIM(a.pat_ethnicity_code)) IN  ('ASKU')
             OR LOWER(TRIM(a.pat_ethnicity_code)) LIKE '%declined%' 
             OR LOWER(TRIM(a.pat_ethnicity)) LIKE '%declined%' OR LOWER(TRIM(a.pat_ethnicity)) LIKE '%refused%' 
             OR LOWER(TRIM(a.pat_ethnicity)) LIKE '%unknown%'
              THEN 'Unknown'
            ELSE 'NS' END AS pat_ethnicity_std        
FROM rgd_udm_silver.patients a
LEFT JOIN semantics.ethnicity b1 ON a.pat_ethnicity_code=b1.Code
LEFT JOIN semantics.ethnicity b2 
  ON LOWER(TRIM(a.pat_ethnicity))=LOWER(b2.ethnicity); -- added trim

-- Update 5

-- Split comma-separated Ethnicity codes
WITH split_codes AS (
    SELECT
        a.pat_ethnicity,
        a.pat_ethnicity_code,
        TRIM(j.code) AS ethnicity_code
    FROM rgd_udm_silver.patients a,
    JSON_TABLE(
        CONCAT('["', REPLACE(a.pat_ethnicity_code, ',', '","'), '"]'),
        "$[*]" COLUMNS (
            code VARCHAR(50) PATH "$"
        )
    ) j
    WHERE a.pat_ethnicity_code IS NOT NULL
      AND a.pat_ethnicity_code <> ""
      AND a.pat_ethnicity_code LIKE "%,%"
),

ethnicity_mapping AS (SELECT
    sc.pat_ethnicity,
    CASE WHEN GROUP_CONCAT(DISTINCT e.ethnicity SEPARATOR ' / ')  IS NULL 
		AND pat_ethnicity_code LIKE '%ASKU%' THEN 'Unknown'  -- ADDED UNKNOWN
        ELSE COALESCE(GROUP_CONCAT(DISTINCT e.ethnicity SEPARATOR ' / '), 'NS')
        END AS pat_ethnicity_std,
    sc.pat_ethnicity_code,
        CASE WHEN GROUP_CONCAT(DISTINCT e.Code ORDER BY e.Code SEPARATOR ', ')  IS NULL 
		AND pat_ethnicity_code LIKE '%ASKU%' THEN 'Unknown' 
        ELSE COALESCE(GROUP_CONCAT(DISTINCT e.Code ORDER BY e.Code SEPARATOR ', ') , 'NS')
        END AS pat_ethnicity_code_std
FROM split_codes sc
LEFT JOIN semantics.ethnicity e 
    ON TRIM(sc.ethnicity_code) = e.Code
GROUP BY
    sc.pat_ethnicity,
    sc.pat_ethnicity_code
)

SELECT DISTINCT p.pat_ethnicity_code, p.pat_ethnicity, -- CHANGE THIS TO UPDATE STATEMENT
	e.pat_ethnicity_code_std AS pat_ethnicity_code_std,
	e.pat_ethnicity_std AS pat_ethnicity_std
FROM rgd_udm_silver.patients p
JOIN ethnicity_mapping e ON p.pat_ethnicity_code = e.pat_ethnicity_code;

-- Update 6

UPDATE rgd_udm_silver.patient_demographics
  SET pat_deceased_status_std =
  CASE 
      WHEN pat_deceased_status IN ('1','Y') or deceased_date is not NULL THEN 'Y'
      WHEN pat_deceased_status = ('0','N') THEN 'N'
      WHEN pat_deceased_status IS NULL THEN 'N'
      ELSE 'NS'
END;


-- Update 7

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

    

