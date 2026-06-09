UPDATE rgd_udm_silver.procedures
SET
    proc_code_std = CASE
        WHEN LOWER(proc_name) LIKE '%occipital nerve block%' THEN '64405'
        WHEN LOWER(proc_name) LIKE '%trigger point%' THEN '20552'
        WHEN LOWER(proc_name) LIKE '%eeg%' THEN '95816'
        WHEN LOWER(proc_name) LIKE '%consultation%' 
             AND LOWER(proc_name) LIKE '%telemedicine%' THEN '99242'
        WHEN LOWER(proc_name) LIKE '%testosterone%' THEN 'J1071'
        WHEN LOWER(proc_name) LIKE '%b12%' THEN 'J3420'
        WHEN LOWER(proc_name) LIKE '%ocrelizumab%' THEN 'J2350'
        ELSE 'Flagged'
    END,
    proc_name_std = CASE
        WHEN LOWER(proc_name) LIKE '%occipital nerve block%' THEN 'Injection, anesthetic agent; greater occipital nerve'
        WHEN LOWER(proc_name) LIKE '%trigger point%' THEN 'Injection(s), trigger point(s)'
        WHEN LOWER(proc_name) LIKE '%eeg%' THEN 'Electroencephalogram (EEG)'
        WHEN LOWER(proc_name) LIKE '%consultation%' 
             AND LOWER(proc_name) LIKE '%telemedicine%' THEN 'Office consultation'
        WHEN LOWER(proc_name) LIKE '%testosterone%' THEN 'Injection, testosterone cypionate'
        WHEN LOWER(proc_name) LIKE '%b12%' THEN 'Injection, vitamin B-12 cyanocobalamin'
        WHEN LOWER(proc_name) LIKE '%ocrelizumab%' THEN 'Injection, ocrelizumab, 1 mg'
        ELSE 'Flagged'
    END,
    proc_description_std = CASE
        WHEN LOWER(proc_name) LIKE '%occipital nerve block%' THEN 'Injection of anesthetic agent for greater occipital nerve block'
        WHEN LOWER(proc_name) LIKE '%trigger point%' THEN 'Injection of one or more trigger points in muscle'
        WHEN LOWER(proc_name) LIKE '%eeg%' THEN 'Electroencephalogram recording and interpretation'
        WHEN LOWER(proc_name) LIKE '%consultation%' 
             AND LOWER(proc_name) LIKE '%telemedicine%' THEN 'Office consultation for a new or established patient via telemedicine'
        WHEN LOWER(proc_name) LIKE '%testosterone%' THEN 'Injection of testosterone preparation'
        WHEN LOWER(proc_name) LIKE '%b12%' THEN 'Injection of vitamin B12 (cyanocobalamin)'
        WHEN LOWER(proc_name) LIKE '%ocrelizumab%' THEN 'Injection of ocrelizumab, per 1 mg'
        ELSE 'Flagged'
    END
WHERE proc_code IS NULL
  AND proc_name IS NOT NULL;
