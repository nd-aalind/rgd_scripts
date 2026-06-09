UPDATE rgd_udm_silver.patients a
JOIN (
    SELECT
        sc.pat_ethnicity,
        sc.pat_ethnicity_code,

        GROUP_CONCAT(DISTINCT e.ethnicity SEPARATOR ' / ') 
            AS pat_ethnicity_std,

        GROUP_CONCAT(DISTINCT e.Code ORDER BY e.Code SEPARATOR ', ') 
            AS pat_ethnicity_code_std

    FROM (
        SELECT
            a.pat_ethnicity,
            a.pat_ethnicity_code,
            TRIM(j.code) AS ethnicity_code
        FROM rgd_udm_silver.patients a,
        JSON_TABLE(
            CONCAT('["', REPLACE(a.pat_ethnicity_code, ',', '","'), '"]'),
            "$[*]" COLUMNS (
                code VARCHAR(50) PATH "$"
            )
        ) j
        WHERE a.pat_ethnicity_code IS NOT NULL
          AND a.pat_ethnicity_code <> ''
          AND a.pat_ethnicity_code LIKE '%,%'
    ) sc
    LEFT JOIN semantics.ethnicity e 
        ON sc.ethnicity_code = e.Code

    GROUP BY
        sc.pat_ethnicity,
        sc.pat_ethnicity_code
) x
ON a.pat_ethnicity = x.pat_ethnicity
AND a.pat_ethnicity_code = x.pat_ethnicity_code
SET
    a.pat_ethnicity_std = x.pat_ethnicity_std,
    a.pat_ethnicity_code_std = x.pat_ethnicity_code_std;
