UPDATE vitals
SET
    vital_result_std = ROUND(vital_result / 2.54, 2),
    vital_unit_std = 'in'
WHERE vital_name IN (
    'VITALS.HEADCIRCUMFERENCE',
    'VITALS.WAISTCIRCUMFERENCE'
)
AND vital_unit = 'cm';