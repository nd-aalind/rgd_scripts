-- Update 1:

WITH snomed_max AS (
    SELECT 
        conceptId,
        MAX(id) AS latest_snomed_id
	FROM semantics.snomed
    WHERE active = 1
    GROUP BY conceptId
),
rx_max AS (
    SELECT 
        EVD_RXN_RXCUI,
        MAX(EVD_RXN_CONCEPT_SOURCE_KEY) AS latest_rx_id
	FROM FDB.REVDCS0_RXN_CONCEPT_SOURCE
    GROUP BY EVD_RXN_RXCUI
)
SELECT DISTINCT
    a.allergen_name,
    COALESCE(rxdesc.EVD_RXN_STR, ndc.BN, s.term, 'NS')  AS allergen_name_std,
    a.allergen_code,
    COALESCE(rx.EVD_RXN_RXCUI, ndc.NDC, s.conceptId, 'NS') as allergen_code_std,
    a.allergen_coding_system, 
    
    CASE WHEN rx.EVD_RXN_RXCUI IS NOT NULL AND ndc.NDC IS NULL AND s.conceptId IS NULL THEN 'RXNORM' 
        WHEN ndc.NDC IS NOT NULL AND rx.EVD_RXN_RXCUI IS NULL AND s.conceptId IS NULL THEN 'NDC'
        WHEN s.conceptId IS NOT NULL AND rx.EVD_RXN_RXCUI IS NULL AND ndc.NDC IS NULL THEN 'SNOMED' 
        ELSE 'NS' END AS allergen_coding_system_std
FROM rgd_udm_silver.allergies a
LEFT JOIN snomed_max sm ON a.allergen_code = sm.conceptId -- one snomed record per code
LEFT JOIN semantics.snomed s ON sm.conceptId = s.conceptId AND s.Id = sm.latest_snomed_id -- snomed master table
LEFT JOIN FDB.RNDC14_NDC_MSTR ndc ON REPLACE(TRIM(a.allergen_code),'_','') = ndc.NDC -- ndc master table
LEFT JOIN rx_max rxm ON rxm.EVD_RXN_RXCUI=TRIM(a.allergen_code) -- rxnorm master one record per code
LEFT JOIN FDB.REVDCS0_RXN_CONCEPT_SOURCE rx ON rx.EVD_RXN_RXCUI = TRIM(a.allergen_code) -- rxnorm master table
								AND rx.EVD_RXN_CONCEPT_SOURCE_KEY = rxm.latest_rx_id 
LEFT JOIN FDB.REVDCD0_RXN_CONCEPT_DESC rxdesc ON rx.EVD_RXN_CONCEPT_SOURCE_KEY=rxdesc.EVD_RXN_CONCEPT_SOURCE_KEY -- rxnorm description
WHERE a.allergen_code IS NOT NULL AND a.allergen_code <> "";





-- Update 2:

WITH snomed_max AS (
    SELECT 
        conceptId,
        MAX(id) AS latest_snomed_id
	FROM semantics.snomed
    WHERE active = 1
    GROUP BY conceptId
)

SELECT DISTINCT
    a.allergy_reaction_name,
    s.term AS allergy_reaction_name_std,
    a.allergy_reaction_code,
    s.conceptId as allergy_reaction_code_std,
    a.allergy_reaction_coding_system, 
    CASE WHEN s.conceptId IS NOT NULL THEN 'SNOMED' 
      ELSE NULL 
    END AS allergy_reaction_coding_system_std
FROM rgd_udm_silver.allergies_inc a
LEFT JOIN snomed_max sm ON a.allergy_reaction_code = sm.conceptId
LEFT JOIN semantics.snomed s ON a.allergy_reaction_code = s.conceptId AND s.Id = sm.latest_snomed_id
WHERE a.allergy_reaction_code IS NOT NULL AND a.allergy_reaction_code <> ""; -- only where code is present

-- Update 3:

SELECT DISTINCT
    a.allergy_reaction_name,
    COALESCE(s.term, 'NS') AS allergy_reaction_name_std,
    a.allergy_reaction_code,
    COALESCE(s.conceptId, 'NS') as allergy_reaction_code_std,
    a.allergy_reaction_coding_system, 
    CASE WHEN s.conceptId IS NOT NULL THEN 'SNOMED' 
      ELSE 'NS' 
    END AS allergy_reaction_coding_system_std
FROM rgd_udm_silver.allergies_inc a
LEFT JOIN semantics.snomed s ON LOWER(s.term) LIKE LOWER(a.allergy_reaction_name)
WHERE (allergy_reaction_name_std IS NULL OR allergy_reaction_name_std = "NS") -- only non standardized records
AND a.allergy_reaction_name IS NOT NULL AND a.allergy_reaction_name <> "" ; -- only where name is present


-- Update 4 : 

WITH snomed_max AS (
    SELECT 
        conceptId,
        MAX(id) AS latest_snomed_id
    FROM semantics.snomed
    WHERE active = 1
    GROUP BY conceptId
),

split_codes AS (
    SELECT
        a.allergy_reaction_name,
        a.allergy_reaction_code AS original_code,
        a.allergy_reaction_coding_system,
        TRIM(j.code) AS split_code
    FROM kinsula_leq.allergies a,
    JSON_TABLE(
        CONCAT('["', REPLACE(a.allergy_reaction_code, ',', '","'), '"]'),
        "$[*]" COLUMNS (
            code VARCHAR(50) PATH "$"
        )
    ) j
    WHERE a.allergy_reaction_code IS NOT NULL
      AND a.allergy_reaction_code <> ""
      AND a.allergy_reaction_code LIKE "%,%"
),

snomed_mapped AS (SELECT DISTINCT
    sc.original_code,
    sc.split_code,
    COALESCE(s.conceptId, 'NS') AS allergy_reaction_code_std,
    sc.allergy_reaction_name,
    COALESCE(s.term, 'NS') AS allergy_reaction_name_std,
    sc.allergy_reaction_coding_system,
    CASE 
        WHEN s.conceptId IS NOT NULL THEN 'SNOMED'
        ELSE 'NS'
    END AS allergy_reaction_coding_system_std

FROM split_codes sc
LEFT JOIN snomed_max sm 
    ON TRIM(sc.split_code) = sm.conceptId
LEFT JOIN semantics.snomed s 
    ON sc.split_code = s.conceptId
    AND s.id = sm.latest_snomed_id
)

SELECT DISTINCT a.allergy_reaction_name, a.allergy_reaction_code, a.allergy_reaction_coding_system,
sm.allergy_reaction_name_std, sm.allergy_reaction_code_std, sm.allergy_reaction_coding_system_std
FROM kinsula_leq.allergies a
JOIN snomed_mapped sm ON a.allergy_reaction_code=sm.original_code;