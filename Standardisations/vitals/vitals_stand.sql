
-- Update 1:

SELECT DISTINCT
    v.vital_name,
    v.vital_code,
    CASE 
    WHEN v.vital_name IS NULL OR TRIM(v.vital_name) = '' THEN NULL
    WHEN l.vital_name_std LIKE 'Blood pressure (Split Required)' AND 
     REGEXP_REPLACE(
                REGEXP_REPLACE(vital_result, '<[^>]*>', ''),
                '[^0-9/]',
                ''
             ) NOT REGEXP '^[0-9]+/[0-9]+$'
    THEN 'NS'
    WHEN l.vital_name_std IS NOT NULL THEN l.vital_name_std  
        ELSE 'NS' END AS vital_name_std,
    CASE 
    WHEN v.vital_name IS NULL OR TRIM(v.vital_name) = '' THEN NULL
    WHEN l.vital_code_std IS NOT NULL THEN l.vital_code_std 
        ELSE 'NS' END AS vital_code_std,
    CASE 
    WHEN v.vital_name IS NULL OR TRIM(v.vital_name) = '' THEN NULL
    WHEN l.vital_coding_system_std IS NOT NULL THEN l.vital_coding_system_std 
        ELSE 'NS' END AS vital_coding_system_std
FROM kinsula_leq.vitals v
LEFT JOIN semantics.vitals_loinc l ON LOWER(REPLACE(TRIM(v.vital_name),':','')) = LOWER(TRIM(l.vital_name));


-- Update 2:

WITH BP_clean AS (SELECT
	*,
	REGEXP_REPLACE(
		REGEXP_REPLACE(vital_result, '<[^>]*>', ''),  -- remove HTML
		'[^0-9/]', ''
	) AS vital_clean
FROM kinsula_leq.vitals
WHERE vital_name_std = 'Blood pressure (Split Required)' 
AND vital_result LIKE '%/%'
),
BP_valid AS (
    SELECT *
    FROM BP_clean
    WHERE vital_clean REGEXP '^[0-9]+/[0-9]+$'
)
SELECT DISTINCT
    vital_name,
    'Systolic blood pressure' AS vital_name_std,
    vital_result,
    TRIM(SUBSTRING_INDEX(vital_clean,'/',1)) AS vital_result_std,
    'mmHg' AS vital_unit_std
FROM BP_valid

UNION ALL

SELECT DISTINCT
    vital_name,
    'Diastolic blood pressure' AS vital_name_std,
    vital_result,
    TRIM(SUBSTRING_INDEX(vital_clean,'/',-1)) AS vital_result_std,
    'mmHg' AS vital_unit_std
FROM BP_valid;


-- update 3: 


WITH cleaned_vitals AS (

    /* =========================
       STEP 1: CLEAN RAW VALUES
       ========================= */

    SELECT
        DISTINCT
        vital_name,
        vital_name_std,
        vital_result,
        vital_unit,

        /* Handle XML separately */
        TRIM(
            LOWER(
                REGEXP_REPLACE(
                    REPLACE(
                        REPLACE(vital_result, '&apos;', "'"),
                        '&quot;', '"'
                    ),
                    '<[^>]*>',
                    ''
                )
            )
        ) AS cleaned_result

    FROM kinsula_leq.vitals
),

parsed_vitals AS (

    /* =========================
       STEP 2: PREP / PARSE
       ========================= */

    SELECT

        vital_name,
        vital_name_std,
        vital_result,
        vital_unit,
        cleaned_result,

        /* Normalize feet/inches formats */
       REGEXP_REPLACE(
    REPLACE(
        REGEXP_REPLACE(
            REGEXP_REPLACE(
                cleaned_result,
                'feet|foot|ft', "'"
            ),
            'inches|inch|in', '"'
        ),
        "''",
        '"'
    ),
    "\\s+",
    ""
) AS normalized_height,

        /* Extract numeric from cleaned value */
        CASE

            WHEN vital_name_std = 'Blood pressure (Split Required)'
                THEN 'NS'

            ELSE
                CAST(
                    NULLIF(
                        REGEXP_SUBSTR(
                            REGEXP_REPLACE(
                                LOWER(
                                    CASE
                                        WHEN vital_result REGEXP '<[^>]+>'
                                            THEN REGEXP_REPLACE(
                                                vital_result,
                                                '<[^>]*>',
                                                ''
                                            )
                                        ELSE vital_result
                                    END
                                ),
                                '[^0-9.]',
                                ''
                            ),
                            '[0-9]+(\\.[0-9]+)?'
                        ),
                        ''
                    ) AS DOUBLE
                )

        END AS val

    FROM cleaned_vitals
)

SELECT

    vital_name,
    vital_name_std,
    vital_result,
    vital_unit,

    /* =========================
       RESULT STANDARDIZATION
       ========================= */

    CASE

        -- -- -- --
        /* 1. ft + in (normal + decimal) */

        WHEN vital_name_std = 'Body height'
            AND normalized_height REGEXP "^[0-9]+'[0-9]+(\\.[0-9]+)?\"?$"
        THEN
            CAST(
                REGEXP_SUBSTR(
                    normalized_height,
                    '^[0-9]+'
                ) AS DOUBLE
            ) * 12
            +
            CAST(
                REGEXP_SUBSTR(
                    normalized_height,
                    "(?<=')[0-9]+(\\.[0-9]+)?"
                ) AS DOUBLE
            )

        /* 2. ft + fraction (4'9 1/2") */

        WHEN vital_name_std = 'Body height'
            AND normalized_height REGEXP "^[0-9]+'[0-9]+[0-9]*/[0-9]+"
        THEN
            CAST(
                REGEXP_SUBSTR(
                    normalized_height,
                    '^[0-9]+'
                ) AS DOUBLE
            ) * 12
            +
            CAST(
                REGEXP_SUBSTR(
                    normalized_height,
                    "(?<=')[0-9]+"
                ) AS DOUBLE
            )
            +
            (
                CAST(
                    REGEXP_SUBSTR(
                        normalized_height,
                        "[0-9]+(?=/)"
                    ) AS DOUBLE
                )
                /
                CAST(
                    REGEXP_SUBSTR(
                        normalized_height,
                        "(?<=/)[0-9]+"
                    ) AS DOUBLE
                )
            )

        /* 3. only feet (5') */

        WHEN vital_name_std = 'Body height'
            AND normalized_height REGEXP "^[0-9]+'$"
        THEN
            CAST(
                REGEXP_SUBSTR(
                    normalized_height,
                    '[0-9]+'
                ) AS DOUBLE
            ) * 12

        /* 4. inches only (66", 74") */

        WHEN vital_name_std = 'Body height'
            AND normalized_height REGEXP '^[0-9]+(\\.[0-9]+)?\"$'
        THEN
            CAST(
                REGEXP_SUBSTR(
                    normalized_height,
                    '[0-9]+(\\.[0-9]+)?'
                ) AS DOUBLE
            )

        /* 5. fallback cm */

        WHEN vital_name_std = 'Body height'
            AND normalized_height REGEXP '^[0-9]+(\\.[0-9]+)?$'
            AND CAST(normalized_height AS DOUBLE) BETWEEN 100 AND 250
        THEN ROUND(
            CAST(normalized_height AS DOUBLE) / 2.54,
            2
        )

        -- -- -- -- -- --

        /* HEIGHT cm → in */

        WHEN vital_name_std = 'Body height'
            AND (
                LOWER(TRIM(vital_unit)) = 'cm'
                OR LOWER(vital_name) LIKE '%cm%'
            )
        THEN ROUND(val / 2.54, 2)

        /* HEIGHT no unit assume cm */

        WHEN vital_name_std = 'Body height'
            AND (
                TRIM(vital_unit) = ''
                OR vital_unit IS NULL
            )
            AND val BETWEEN 100 AND 250
        THEN ROUND(val / 2.54, 2)

        /* WEIGHT g → lb */

        WHEN vital_name_std = 'Body weight'
            AND (
                LOWER(vital_unit) = 'g'
                OR val > 1000
            )
        THEN ROUND(val / 453.59237, 2)

        /* WEIGHT kg → lb */

        WHEN vital_name_std = 'Body weight'
            AND (
                LOWER(vital_unit) = 'kg'
                OR LOWER(vital_result) LIKE '%kg%'
                OR val < 80
            )
        THEN ROUND(val * 2.20462, 2)

        ELSE
            CASE
                WHEN vital_name_std = 'Blood pressure (Split Required)'
                    THEN 'NS'
                ELSE val
            END

    END AS vital_result_std,

    /* =========================
       UNIT STANDARDIZATION
       ========================= */

    CASE

        -- weight

        WHEN vital_name_std = 'Body weight'
            AND (
                vital_unit IN ('g', 'kg', '[lb_av]')
                OR val > 1000
                OR LOWER(vital_result) LIKE '%kg%'
                OR val < 80
            )
        THEN 'lb'

        WHEN vital_name_std = 'Body weight'
            AND (
                TRIM(vital_unit) = ''
                OR vital_unit IS NULL
            )
            AND val BETWEEN 150 AND 500
        THEN 'lb'

        WHEN vital_name_std = 'Weight-for-length Per age and sex'
            AND vital_unit = '{percentile}'
        THEN 'percentile'

        -- height

        WHEN vital_name_std = 'Body height'
            AND (
                vital_unit IN ('[in_i]', 'cm')
                OR LOWER(vital_name) LIKE '%cm%'
            )
        THEN 'in'

        -- -- -- --

        /* feet/inch formats → inches */

        WHEN vital_name_std = 'Body height'
            AND cleaned_result REGEXP "('|ft|in|\")"
        THEN 'in'
        
        WHEN vital_name_std = 'Body height'
    AND (
        TRIM(vital_unit) = ''
        OR vital_unit IS NULL
    )
    AND val BETWEEN 100 AND 250
    THEN 'in'

        -- -- -- --

        -- BP

        WHEN vital_name_std = 'Diastolic blood pressure'
        THEN 'mmHg'

        WHEN vital_name_std = 'Systolic blood pressure'
        THEN 'mmHg'

        -- others

        WHEN vital_name_std = 'Body temperature'
            AND vital_unit IN ('[degF]', 'F')
        THEN '°F'

        WHEN vital_name_std = 'Heart rate'
        THEN 'bpm'

        WHEN vital_name_std = 'Respiratory rate'
        THEN 'breaths/min'

        WHEN vital_name_std = 'Body surface area'
            AND vital_unit = 'm2'
        THEN 'm²'

        WHEN vital_name_std = 'Inhaled oxygen flow rate'
        THEN 'L/min'

        WHEN vital_name_std = 'Inhaled oxygen concentration'
            AND vital_unit = '%'
        THEN '%'

        WHEN vital_name_std = 'Oxygen saturation in Arterial blood by Pulse oximetry'
            AND vital_unit = '%'
        THEN '%'

        ELSE vital_unit

    END AS vital_unit_std

FROM parsed_vitals;