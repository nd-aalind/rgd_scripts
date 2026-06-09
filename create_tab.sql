 create table deidentified_merged.dedup_patientinsurance  SELECT *
                FROM (
                    SELECT t.*,
                           ROW_NUMBER() OVER (
                               PARTITION BY nd_auto_increment_id
                               ORDER BY nd_deidentification_datetime DESC
                           ) AS rn,
                           COUNT(*) OVER (
                               PARTITION BY nd_auto_increment_id
                           ) AS cnt
                    FROM deidentified_merged.PATIENTINSURANCE t
                ) x
                WHERE cnt > 1
                  AND rn = 1;