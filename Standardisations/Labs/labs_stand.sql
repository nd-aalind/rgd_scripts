-- Update 1:

UPDATE rgd_udm_silver.labs l
JOIN semantics.loinc loinc 
    ON l.result_code = loinc.LOINC_NUM
SET
    l.panel_code_std        = NULL,
    l.result_panel_std      = NULL,
    l.panel_match_type      = NULL,
    l.component_code_std    = loinc.LOINC_NUM,
    l.result_component_std  = loinc.COMPONENT,
    l.result_desc_std       = loinc.LONG_COMMON_NAME,
    l.component_match_type  = 'Exact, conf 1'
    l.specimen_source_std   = loinc.SYSTEM
WHERE l.result_code IS NOT NULL;

-- Update 2:

SELECT DISTINCT
    l.specimen_source,
    CASE 
        -- BLOOD & COMPONENTS (LOINC: Ser, Plas, Bld, Ser/Plas)
        WHEN l.specimen_source REGEXP 'Ser/Plas|S/P' THEN 'Ser/Plas'
        WHEN l.specimen_source REGEXP 'Serum' THEN 'Ser'
        WHEN l.specimen_source REGEXP 'Plasma' THEN 'Plas'
		WHEN l.specimen_source REGEXP 'Venous' THEN 'BldV'
		WHEN l.specimen_source REGEXP 'Arterial' THEN 'BldA'
		WHEN l.specimen_source REGEXP 'Capillary' THEN 'BldC'
		WHEN l.specimen_source REGEXP 'cord blood|blood cord' THEN 'BldCo'
		WHEN l.specimen_source REGEXP 'Blood|BLD|^BL$' THEN 'Bld'

        -- CEREBROSPINAL FLUID (LOINC: Csf)
        WHEN l.specimen_source REGEXP 'CSF|Cerebrospinal|Spinal' THEN 'CSF'

        -- STOOL (LOINC: Stool)
        WHEN l.specimen_source REGEXP 'Stool|Fecal|Faeces' THEN 'Stool'

        -- RESPIRATORY (LOINC: Resp, Nasopharyngeal, Throat, Sputum)
        WHEN l.specimen_source REGEXP 'Throat' THEN 'Thrt'
        WHEN l.specimen_source REGEXP 'Sputum' THEN 'Spt'
        WHEN l.specimen_source REGEXP 'BAL|Bronchial|Alveolar' THEN 'BAL'
        WHEN l.specimen_source REGEXP 'Respiratory' THEN 'Resp'
        WHEN l.specimen_source REGEXP 'Swab' THEN 'XXX.swab'

        -- GENITAL/OBSTETRIC (LOINC: Vagina, Cervix, Placenta)
        WHEN l.specimen_source REGEXP 'Vaginal/Rectal|Vagina/rect|vag/rec'  THEN 'Vag+Rectum'
        WHEN l.specimen_source REGEXP 'Vaginal|VAG|Vulva' THEN 'Vag'
        WHEN l.specimen_source REGEXP 'Cervix|Cervical|Cvx' THEN 'Cvx'
        WHEN l.specimen_source REGEXP 'Endomet' THEN 'Endomet'
        WHEN l.specimen_source REGEXP 'Placenta' THEN 'Placenta'
        WHEN l.specimen_source REGEXP 'Rectal|rectum' THEN 'Rectum'

        -- BODY FLUIDS (LOINC: Plr fld, Perit fld, Syn fld)
        WHEN l.specimen_source REGEXP 'Pleural|PLFL|BPLEU' THEN 'Plr fld'
        WHEN l.specimen_source REGEXP 'Peritoneal|Ascites|Perit' THEN 'Perit fld'
        WHEN l.specimen_source REGEXP 'Synovial|syn fl' THEN 'Syn fld'

        -- TISSUE & BIOPSY (LOINC: Tiss)
        WHEN l.specimen_source REGEXP 'skin' THEN 'Skin'
        WHEN l.specimen_source REGEXP 'Tissue|TISS' THEN 'Tiss'
        
        -- URINE (LOINC: Urine)
        WHEN l.specimen_source REGEXP 'Urine|URIN|^UR$|^U$|URNE|URN|UCC' THEN 'Urine'

        -- MISC LOINC SYSTEMS
        WHEN l.specimen_source REGEXP 'Abscess|ABS' THEN 'Abscess'
        WHEN l.specimen_source REGEXP 'Wound|WND' THEN 'Wound'
        WHEN l.specimen_source REGEXP 'Saliva' THEN 'Saliva'
        WHEN l.specimen_source REGEXP 'Sweat' THEN 'Sweat'
        WHEN l.specimen_source REGEXP 'Calculus|Stone|Calculi' THEN 'Calculus'
        
        ELSE NULL
    END AS specimen_source_std
FROM rgd_udm_silver.labs l
WHERE l.specimen_source_std IS NULL;

-- Update 3:

SELECT DISTINCT
    l.result_name,
    l.result_parameter,
    CASE -- update specimen_source_std
        -- 1. CEREBROSPINAL FLUID (CSF)
        WHEN l.result_name REGEXP 'CSF|spinal' 
          OR l.result_parameter REGEXP 'CSF|spinal'
          THEN 'CSF'

        -- 2. SERUM & PLASMA (Check combo first)
        WHEN l.result_name REGEXP 'serum/plasma|serum or plasma|serum / plasma|serum/plas|S/P'
          OR l.result_parameter REGEXP 'serum/plasma|serum or plasma|serum / plasma|serum/plas|S/P'
          THEN 'Ser/Plas'

        -- 3. SERUM (Stand-alone or shorthand)
        -- The [[:punct:]] or [[:space:]] handles cases like ", s" or "(s)"
        WHEN l.result_name REGEXP 'serum|,[[:space:]]*s|\\(s\\)'
          OR l.result_parameter REGEXP 'serum|,[[:space:]]*s|\\(s\\)'
          THEN 'Ser'

        -- 4. PLASMA (Stand-alone or shorthand)
        WHEN l.result_name REGEXP 'plasma|,[[:space:]]*p|\\(p\\)'
          OR l.result_parameter REGEXP 'plasma|,[[:space:]]*p|\\(p\\)'
          THEN 'Plas'

        -- 5. WHOLE BLOOD / BLOOD
        -- Matches blood, bld, or venous. 
        WHEN l.result_name REGEXP 'blood|bld|,[[:space:]]*b|whole blood|venous'
          OR l.result_parameter REGEXP 'blood|bld|,[[:space:]]*b|whole blood|venous'
          THEN 'Bld'

        -- 6. URINE
        WHEN l.result_name REGEXP 'urin| ur|urate|\\(u\\)'
          OR l.result_parameter REGEXP 'urin| ur|urate|\\(u\\)'
          THEN 'Urine'

        -- 7. STOOL / FECAL
        WHEN l.result_name REGEXP 'stool|faeces|fecal|feces'
          OR l.result_parameter REGEXP 'stool|faeces|fecal|feces'
          THEN 'Stool'

        -- 8. SWABS & OTHER (For exhaustiveness)
        WHEN l.result_name REGEXP 'swab|throat|nasal|wound|eye|ear'
          THEN 'Swab'

        -- 9. BODY FLUIDS
        WHEN l.result_name REGEXP 'pleural|peritoneal|ascites|fluid|synovial|dialysate'
          THEN 'Body Fld'
          
		-- 10. HEMATOLOGY PANELS (Inferred Whole Blood)
        WHEN l.result_name REGEXP 'CBC|Hemogram|Hgb/Hct|Platelet|Complete Blood|A1c|Glycated|Glycohemoglobin'
          OR l.result_parameter REGEXP 'CBC|Hemogram|Hgb/Hct|Diff|A1c|Glycated|Glycohemoglobin'
          THEN 'Bld'

        -- 11. METABOLIC PANELS (Inferred Serum/Plasma)
        WHEN l.result_name REGEXP 'CMP|BMP|Metabolic|Basic met|Comp met|Chem[[:space:]]*[[:digit:]]+|SMA|Lipid|Cholest|Triglyceride|HDL|LDL|TSH|Thyroid|T3|T4|Vitamin|Folate|B12|Ferritin'
          OR l.result_parameter REGEXP 'CMP|BMP|Metabolic|Lipid|Cholest|Triglyceride|HDL|LDL|TSH|Thyroid|T3|T4|Vitamin|Folate|B12|Ferritin'
          THEN 'Ser/Plas'

        ELSE NULL
    END AS specimen_source_std
FROM kinsula_leq.labs l
WHERE specimen_source_std IS NULL ;