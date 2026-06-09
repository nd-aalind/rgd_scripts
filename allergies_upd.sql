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
UPDATE kinsula_leq.allergies a
LEFT JOIN snomed_max sm 
    ON a.allergen_code = sm.conceptId
LEFT JOIN semantics.snomed s 
    ON sm.conceptId = s.conceptId 
   AND s.Id = sm.latest_snomed_id
LEFT JOIN FDB.RNDC14_NDC_MSTR ndc 
    ON REPLACE(TRIM(a.allergen_code),'_','') = ndc.NDC
LEFT JOIN rx_max rxm 
    ON rxm.EVD_RXN_RXCUI = TRIM(a.allergen_code)
LEFT JOIN FDB.REVDCS0_RXN_CONCEPT_SOURCE rx 
    ON rx.EVD_RXN_RXCUI = TRIM(a.allergen_code)
   AND rx.EVD_RXN_CONCEPT_SOURCE_KEY = rxm.latest_rx_id
LEFT JOIN FDB.REVDCD0_RXN_CONCEPT_DESC rxdesc 
    ON rx.EVD_RXN_CONCEPT_SOURCE_KEY = rxdesc.EVD_RXN_CONCEPT_SOURCE_KEY
SET
    a.allergen_name_std = COALESCE(rxdesc.EVD_RXN_STR, ndc.BN, s.term, 'NS'),
    a.allergen_code_std = COALESCE(rx.EVD_RXN_RXCUI, ndc.NDC, s.conceptId, 'NS'),
    a.allergen_coding_system_std = CASE 
        WHEN rx.EVD_RXN_RXCUI IS NOT NULL 
             AND ndc.NDC IS NULL 
             AND s.conceptId IS NULL THEN 'RXNORM' 
        WHEN ndc.NDC IS NOT NULL 
             AND rx.EVD_RXN_RXCUI IS NULL 
             AND s.conceptId IS NULL THEN 'NDC'
        WHEN s.conceptId IS NOT NULL 
             AND rx.EVD_RXN_RXCUI IS NULL 
             AND ndc.NDC IS NULL THEN 'SNOMED' 
        ELSE 'NS' 
    END
WHERE a.allergen_code IS NOT NULL 
  AND a.allergen_code <> '';