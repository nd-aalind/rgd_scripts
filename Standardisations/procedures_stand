UPDATE rgd_udm_silver.procedures p
LEFT JOIN semantics.hcpcs h 
    ON p.proc_code = h.HCPC
LEFT JOIN tncpa.PROCEDURECODEREFERENCE t 
    ON p.proc_code = t.PROCEDURECODE
SET
    p.proc_code_std = CASE 
        WHEN h.HCPC IS NOT NULL THEN h.HCPC
        WHEN t.PROCEDURECODE IS NOT NULL THEN t.PROCEDURECODE
        ELSE 'Flagged' 
    END,
    p.proc_coding_system_std = CASE
        WHEN h.HCPC IS NOT NULL THEN 'HCPCS'
        WHEN t.PROCEDURECODE IS NOT NULL 
             AND t.PROCEDURECODE REGEXP '^[0-9]{5}$' THEN 'CPT'
        WHEN t.PROCEDURECODE IS NOT NULL 
             AND t.PROCEDURECODE REGEXP '^[A-Z][0-9]{4}$' THEN 'HCPCS'
        ELSE 'Flagged'
    END,
    p.proc_name_std = CASE 
        WHEN h.HCPC IS NOT NULL THEN h.`SHORT DESCRIPTION`
        WHEN t.PROCEDURECODE IS NOT NULL THEN t.COMMONDESCRIPTION
        ELSE 'Flagged' 
    END,
    p.proc_description_std = CASE 
        WHEN h.HCPC IS NOT NULL THEN h.`LONG DESCRIPTION`
        WHEN t.PROCEDURECODE IS NOT NULL THEN t.DESCRIPTION
        ELSE 'Flagged'
    END
WHERE p.proc_code IS NOT NULL
  AND p.proc_code <> '';