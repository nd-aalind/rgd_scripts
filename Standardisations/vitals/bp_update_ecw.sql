UPDATE rgd_udm_silver.vitals_dedup
SET
    vital_result_std = 'NS',
    vital_unit_std   = 'NS'
WHERE vital_name IN (
    'Blood pressure (BP)',
    'Blood pressure, sitting',
    'Blood pressure, standing',
    'Blood pressure, supine',
    'BP',
    'BP - Lying',
    'BP - Sitting',
    'BP - Standing',
    'BP sitting',
    'BP standing',
    'BP supine',
    'BP-Treatment',
    'BP:',
    'Diastolic BP:',
    'Repeat blood pressure',
    'Repeat BP',
    'Systolic BP:'
) and psid in (1,3,4,8,13,14);