UPDATE rgd_udm_silver.problemlist
SET ckd_comorbidity = 'Chronic kidney disease (CKD)'
WHERE LOWER(problem_desc) LIKE '%chronic kidney%'
   OR LOWER(problem_desc_std) LIKE '%chronic kidney%'
   OR icd_code LIKE 'N18%'
   OR mapped_icd_code LIKE 'N18%';