UPDATE rgd_udm_silver.patients
SET pat_deceased_status_std =
  CASE 
      WHEN pat_deceased_status IN ('1','Y') or deceased_date is not NULL THEN 'Deceased'
      WHEN pat_deceased_status = '0' THEN 'Null'
      WHEN pat_deceased_status IS NULL THEN 'Null'
      ELSE 'Flagged'
END;