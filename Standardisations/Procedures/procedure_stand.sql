-- Update 1: 

SELECT DISTINCT
    p.proc_code,
    CASE WHEN (p.proc_code IS NULL OR TRIM(p.proc_code) = '') 
          AND (p.proc_name IS NULL OR TRIM(p.proc_name) = '')
        THEN NULL -- added for nulls
		ELSE LEFT(COALESCE(h.HCPC,t.PROCEDURECODE, 'NS'), 5) END AS proc_code_std,
    CASE WHEN (p.proc_code IS NULL OR TRIM(p.proc_code) = '') 
          AND (p.proc_name IS NULL OR TRIM(p.proc_name) = '')
        THEN NULL -- added for nulls
		WHEN p.proc_code LIKE '%,%' THEN TRIM(SUBSTRING(p.proc_code, INSTR(p.proc_code, ',') + 1))
        WHEN p.proc_code NOT LIKE '%,%' THEN NULL
		ELSE 'NS'
	END AS proc_modifier_std,
    p.proc_coding_system,
    CASE WHEN (p.proc_code IS NULL OR TRIM(p.proc_code) = '') 
          AND (p.proc_name IS NULL OR TRIM(p.proc_name) = '')
        THEN NULL -- added for nulls
        WHEN h.HCPC IS NOT NULL THEN 'HCPCS'
		WHEN LEFT(t.PROCEDURECODE, 5) REGEXP '^[0-9]+$' THEN 'CPT'
		WHEN LEFT(t.PROCEDURECODE, 5) REGEXP '^(?=.*[A-Za-z])(?=.*[0-9])[A-Za-z0-9]+$' THEN 'HCPCS'
        ELSE 'NS'
    END AS proc_coding_system_std,
    p.proc_name,
    CASE WHEN (p.proc_code IS NULL OR TRIM(p.proc_code) = '') 
          AND (p.proc_name IS NULL OR TRIM(p.proc_name) = '')
        THEN NULL -- added for nulls
        ELSE COALESCE(h.`SHORT DESCRIPTION`, t.COMMONDESCRIPTION,'NS')
    END AS proc_name_std,
    p.proc_description,
    CASE WHEN (p.proc_code IS NULL OR TRIM(p.proc_code) = '') 
          AND (p.proc_name IS NULL OR TRIM(p.proc_name) = '')
        THEN NULL -- added for nulls
        ELSE COALESCE(h.`LONG DESCRIPTION`, t.DESCRIPTION, 'NS')
    END AS proc_description_std
FROM kinsula_leq.procedures p
LEFT JOIN semantics.hcpcs h 
    ON LEFT(TRIM(p.proc_code), 5) = h.HCPC
LEFT JOIN tncpa.PROCEDURECODEREFERENCE t 
    ON LEFT(TRIM(p.proc_code), 5) = t.PROCEDURECODE;

-- Update 2 : 

SELECT distinct 
    proc_name,
    -- Standard Code
    CASE
        WHEN LOWER(proc_name) LIKE '%occipital nerve block%' THEN '64405'
        WHEN LOWER(proc_name) LIKE '%trigger point%' THEN '20552'
        WHEN LOWER(proc_name) LIKE '%consultation%' 
             AND LOWER(proc_name) LIKE '%telemedicine%' THEN '99242'
        WHEN LOWER(proc_name) LIKE '%testosterone%' THEN 'J1071'
        WHEN LOWER(proc_name) LIKE '%b12%' THEN 'J3420'
        WHEN LOWER(proc_name) LIKE '%ocrelizumab%' THEN 'J2350'
        ELSE 'NS'
    END AS proc_code_std,
    NULL AS proc_modifier_std,
    -- Standard Name / Common Description
    CASE
        WHEN LOWER(proc_name) LIKE '%occipital nerve block%' THEN 'Injection, anesthetic agent; greater occipital nerve'
        WHEN LOWER(proc_name) LIKE '%trigger point%' THEN 'Injection(s), trigger point(s)'
        WHEN LOWER(proc_name) LIKE '%consultation%' 
             AND LOWER(proc_name) LIKE '%telemedicine%' THEN 'Office consultation'
        WHEN LOWER(proc_name) LIKE '%testosterone%' THEN 'Injection, testosterone cypionate'
        WHEN LOWER(proc_name) LIKE '%b12%' THEN 'Injection, vitamin B-12 cyanocobalamin'
        WHEN LOWER(proc_name) LIKE '%ocrelizumab%' THEN 'Injection, ocrelizumab, 1 mg'
        ELSE 'NS'
    END AS short_description,
    -- Long Description
    CASE
        WHEN LOWER(proc_name) LIKE '%occipital nerve block%' THEN 'Injection of anesthetic agent for greater occipital nerve block'
        WHEN LOWER(proc_name) LIKE '%trigger point%' THEN 'Injection of one or more trigger points in muscle'
        WHEN LOWER(proc_name) LIKE '%consultation%' 
             AND LOWER(proc_name) LIKE '%telemedicine%' THEN 'Office consultation for a new or established patient via telemedicine'
        WHEN LOWER(proc_name) LIKE '%testosterone%' THEN 'Injection of testosterone preparation'
        WHEN LOWER(proc_name) LIKE '%b12%' THEN 'Injection of vitamin B12 (cyanocobalamin)'
        WHEN LOWER(proc_name) LIKE '%ocrelizumab%' THEN 'Injection of ocrelizumab, per 1 mg'
        ELSE 'NS'
    END AS long_description
FROM rgd_udm_silver.procedures
WHERE proc_code_std = 'NS' AND proc_name IS NOT NULL;




