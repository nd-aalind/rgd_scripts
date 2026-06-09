INSERT INTO udm_staging.vitals (vital_id, ndid, eid, vital_code, vital_name, 
vital_coding_system, vital_date, vital_time, vital_unit, vital_range, vital_result, 
created_datetime, created_by, updated_datetime, updated_by, ehr_source_name, 
source_path, data_type, psid, nd_extracted_date, udm_unq_id, enc_date_proxy
)
select t.*,
CONCAT_WS(':',
        COALESCE(t.psid,           ''),
        COALESCE(t.ndid,           ''),
        COALESCE(t.eid,            ''),
        COALESCE(t.vital_date, ''),
        COALESCE(t.vital_time,''),
        COALESCE(t.vital_code, ''),
        COALESCE(t.vital_name,      '')
    ) AS udm_unq_id,
    CASE
        WHEN t.psid IN (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14) THEN 
            COALESCE(t.vital_date) END AS enc_date_proxy 
from
(
SELECT
  CAST(a.ClinicalVitalID AS DECIMAL(18,0))                 AS vital_id,
  CAST(b.PatientID AS SIGNED)                              AS ndid,
  CAST(b.VisitID AS SIGNED)                                AS eid,
  CAST(c.vital_code AS SIGNED)                             AS vital_code,
  CAST(c.vital_name AS CHAR(100))                          AS vital_name,
  CAST(c.vital_coding_system AS CHAR(10))                  AS vital_coding_system,
  CAST(c.vital_date AS CHAR(10))                           AS vital_date,
  CAST(c.vital_time AS CHAR(13))                           AS vital_time,
  CAST(c.vital_unit AS CHAR(60))                           AS vital_unit,
  CAST(NULL AS BINARY(0))                                  AS vital_range,
  CAST(c.vital_result AS CHAR(500))                        AS vital_result,
  CAST(CURRENT_TIMESTAMP() AS DATETIME)                    AS created_datetime,
  CAST('ND' AS CHAR(2))                                    AS created_by,
  CAST(CURRENT_TIMESTAMP() AS DATETIME)                    AS updated_datetime,
  CAST('ND' AS CHAR(2))                                    AS updated_by,
  CAST('Greenway' AS CHAR(255))                            AS ehr_source_name,
  CAST('bronze_table' AS CHAR(12))                         AS source_path,
  CAST('Structured' AS CHAR(10))                           AS data_type,
  CAST('11' AS CHAR(4))                                    AS psid,      -- Mind
  CAST(a.nd_extracted_date AS DATE)                                       AS nd_extracted_date
FROM savannah.ClinicalVital a
JOIN savannah.ClinicalVitalGroup b
  ON a.ClinicalVitalGroupID = b.ClinicalVitalGroupID and a.nd_ActiveFlag = 'Y' AND b.nd_ActiveFlag = 'Y'
LEFT JOIN (
    SELECT
      omc.ClinicalVitalID,
      CAST(om.OBXConceptID AS SIGNED)                       AS vital_code,
      CAST(om.TestDescription AS CHAR(100))                 AS vital_name,
      CAST(NULL AS CHAR(10))                                AS vital_coding_system,
      DATE_FORMAT(om.CollectionDate, '%Y-%m-%d')            AS vital_date,
      DATE_FORMAT(om.CollectionDate, '%H:%i:%s')            AS vital_time,
      CAST(om.ResultUnits AS CHAR(60))                      AS vital_unit,
      CAST(om.ReferenceRange AS CHAR(255))                  AS vital_range,
      CAST(om.ResultValue AS CHAR(500))                     AS vital_result
    FROM savannah.OBXManual om
    JOIN savannah.OBXManualClinicalVital omc
      ON om.OBXManualId = omc.OBXManualId and om.nd_ActiveFlag = 'Y' and omc.nd_ActiveFlag = 'Y'
) c
  ON c.ClinicalVitalID = a.ClinicalVitalID)t
where DATE(t.nd_extracted_date) > '2026-01-26'
;