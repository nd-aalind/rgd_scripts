UPDATE rgd_udm_silver.medications_part1
SET enc_date_proxy = CASE
    WHEN source = 'clinicalprescription' THEN
        COALESCE(
            enc_date,
            med_start_date,
            med_administered_datetime,
            fill_date,
            written_date,
            med_createddatetime,
            doc_createddatetime
        )
    WHEN source = 'patientmedication' THEN
        COALESCE(
            enc_date,
            med_start_date,
            med_administered_datetime,
            fill_date,
            med_createddatetime,
            doc_createddatetime
        )
END
WHERE source IN ('clinicalprescription', 'patientmedication')
  AND psid IN (2, 5, 6, 10);