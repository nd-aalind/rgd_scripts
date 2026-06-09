-- =============================================================================
-- Biogen Subset Extract — biogen_april schema
-- Patient cohort : biogen.patient_list  (COALESCE(ndid_v1, ndid))
--                  biogen.patient_list_athenaone  (ndid)
-- Date range     : enc_date >= '2025-10-01' AND enc_date <= '2026-02-15'
-- incremental_id : always NULL (placeholder column)
-- =============================================================================

-- Patient union reused as an inline subquery in every statement below.
-- Format:
--   SELECT COALESCE(ndid_v1, ndid) AS pat_id FROM biogen.patient_list
--   UNION ALL
--   SELECT ndid AS pat_id FROM biogen.patient_list_athenaone


-- -----------------------------------------------------------------------------
-- 1. encounters
--    enc_date filter applied directly on the primary date column
-- -----------------------------------------------------------------------------
CREATE TABLE biogen_april.encounters AS
SELECT
    a.ndid,
    a.eid                AS encounter_id,
    a.enc_date           AS encounter_date,
    a.enc_reason         AS encounter_reason,
    a.provider_name      AS at_phy_name,
    NULL                 AS incremental_id
FROM rgd_udm_silver.encounters a
INNER JOIN (
    SELECT COALESCE(ndid_v1, ndid) AS pat_id FROM biogen.patient_list
    UNION ALL
    SELECT ndid AS pat_id FROM biogen.patient_list_athenaone
) b ON a.ndid = b.pat_id
WHERE a.enc_date_proxy >= '2025-10-01'
  AND a.enc_date_proxy <= '2026-02-15';


-- -----------------------------------------------------------------------------
-- 2. diagnosis
-- -----------------------------------------------------------------------------
CREATE TABLE biogen_april.diagnosis AS
SELECT
    a.ndid,
    a.eid                AS encounter_id,
    a.enc_date           AS encounter_date,
    a.diag_date          AS diagnosis_recorded_date,
    a.diag_code,
    a.diag_desc,
    a.diag_coding_system,
    NULL                 AS incremental_id
FROM rgd_udm_silver.diagnosis a
INNER JOIN (
    SELECT COALESCE(ndid_v1, ndid) AS pat_id FROM biogen.patient_list
    UNION ALL
    SELECT ndid AS pat_id FROM biogen.patient_list_athenaone
) b ON a.ndid = b.pat_id
WHERE a.enc_date_proxy >= '2025-10-01'
  AND a.enc_date_proxy <= '2026-02-15';


-- -----------------------------------------------------------------------------
-- 3. procedures
--    source date column is `encounter_date` (not enc_date)
-- -----------------------------------------------------------------------------
CREATE TABLE biogen_april.procedures AS
SELECT
    a.ndid,
    a.eid                AS encounter_id,
    a.encounter_date,
    a.proc_start_date    AS procedure_date,
    a.proc_code,
    a.proc_name,
    a.proc_coding_system,
    NULL                 AS incremental_id
FROM rgd_udm_silver.procedures a
INNER JOIN (
    SELECT COALESCE(ndid_v1, ndid) AS pat_id FROM biogen.patient_list
    UNION ALL
    SELECT ndid AS pat_id FROM biogen.patient_list_athenaone
) b ON a.ndid = b.pat_id
WHERE a.enc_date_proxy >= '2025-10-01'
  AND a.enc_date_proxy <= '2026-02-15';


-- -----------------------------------------------------------------------------
-- 4. allergies
--    No direct enc_date column — using enc_date_proxy for date filter
--    allergy_reaction_name → allergy_reaction_type (closest available column)
-- -----------------------------------------------------------------------------
CREATE TABLE biogen_april.allergies AS
SELECT
    a.ndid,
    a.eid                        AS encounter_id,
    a.enc_date_proxy             AS encounter_date,
    a.allergen_code,
    a.allergen_coding_system,
    a.allergy_type,
    a.allergen_name,
    a.allergy_reaction_name      AS allergy_reaction_type,
    a.allergy_status,
    NULL                         AS incremental_id
FROM rgd_udm_silver.allergies a
INNER JOIN (
    SELECT COALESCE(ndid_v1, ndid) AS pat_id FROM biogen.patient_list
    UNION ALL
    SELECT ndid AS pat_id FROM biogen.patient_list_athenaone
) b ON a.ndid = b.pat_id
WHERE a.enc_date_proxy >= '2025-10-01'
  AND a.enc_date_proxy <= '2026-02-15';


-- -----------------------------------------------------------------------------
-- 5. vitals
--    vital_date is VARCHAR — using enc_date_proxy for the date filter
-- -----------------------------------------------------------------------------
CREATE TABLE biogen_april.vitals AS
SELECT
    a.ndid,
    a.eid                AS encounter_id,
    a.vital_date,
    a.vital_coding_system,
    a.vital_id,
    a.vital_name,
    a.vital_result,
    a.vital_unit,
    NULL                 AS incremental_id
FROM rgd_udm_silver.vitals a
INNER JOIN (
    SELECT COALESCE(ndid_v1, ndid) AS pat_id FROM biogen.patient_list
    UNION ALL
    SELECT ndid AS pat_id FROM biogen.patient_list_athenaone
) b ON a.ndid = b.pat_id
WHERE a.enc_date_proxy >= '2025-10-01'
  AND a.enc_date_proxy <= '2026-02-15';


-- -----------------------------------------------------------------------------
-- 6. labs
-- -----------------------------------------------------------------------------
CREATE TABLE biogen_april.labs AS
SELECT
    a.ndid,
    a.eid                        AS encounter_id,
    a.sample_collection_date,
    a.result_date,
    a.result_name,
    a.result_id,
    a.result_code,
    a.result_coding_system,
    a.result_value,
    a.result_unit,
    a.result_range,
    a.ordering_provider_name,
    NULL                         AS incremental_id
FROM rgd_udm_silver.labs a
INNER JOIN (
    SELECT COALESCE(ndid_v1, ndid) AS pat_id FROM biogen.patient_list
    UNION ALL
    SELECT ndid AS pat_id FROM biogen.patient_list_athenaone
) b ON a.ndid = b.pat_id
WHERE a.enc_date_proxy >= '2025-10-01'
  AND a.enc_date_proxy <= '2026-02-15';


-- -----------------------------------------------------------------------------
-- 7. radiology
--    enc_date is DATETIME — cast to DATE for range comparison
--    study_name   → test_name
--    img_finding  → test_parameter
--    img_status   → resultstatus
--    img_date     → resultdate  (VARCHAR in source)
--    img_report_text → value
--    internal_notes  → note
-- -----------------------------------------------------------------------------
CREATE TABLE biogen_april.radiology AS
SELECT
    a.ndid,
    a.eid                    AS encounter_id,
    DATE(a.enc_date)         AS encounter_date,
    a.report_id,
    a.study_name             AS test_name,
    a.img_finding            AS test_parameter,
    a.img_status             AS resultstatus,
    a.img_date               AS resultdate,
    a.img_report_text        AS value,
    a.internal_notes         AS note,
    NULL                     AS incremental_id
FROM rgd_udm_silver.radiology a
INNER JOIN (
    SELECT COALESCE(ndid_v1, ndid) AS pat_id FROM biogen.patient_list
    UNION ALL
    SELECT ndid AS pat_id FROM biogen.patient_list_athenaone
) b ON a.ndid = b.pat_id
WHERE a.enc_date_proxy >= '2025-10-01'
  AND a.enc_date_proxy <= '2026-02-15';


-- -----------------------------------------------------------------------------
-- 8. ros
--    ros_name  → system_name
--    ros_option → Present
--    ros_notes  → note
-- -----------------------------------------------------------------------------
CREATE TABLE biogen_april.ros AS
SELECT
    a.ndid,
    a.eid                AS encounter_id,
    a.enc_start_date     AS encounter_date,
    a.ros_category,
    a.ros_name           AS system_name,
    a.ros_option         AS Present,
    a.ros_notes          AS note,
    NULL                 AS incremental_id
FROM rgd_udm_silver.ros a
INNER JOIN (
    SELECT COALESCE(ndid_v1, ndid) AS pat_id FROM biogen.patient_list
    UNION ALL
    SELECT ndid AS pat_id FROM biogen.patient_list_athenaone
) b ON a.ndid = b.pat_id
WHERE a.enc_date_proxy >= '2025-10-01'
  AND a.enc_date_proxy <= '2026-02-15';


-- -----------------------------------------------------------------------------
-- 9. examination
-- -----------------------------------------------------------------------------
CREATE TABLE biogen_april.examinations AS
SELECT
    a.ndid,
    a.eid                AS encounter_id,
    a.enc_start_date     AS encounter_date,
    a.exam_date,
    a.examid             AS exam_id,
    a.exam_category,
    a.exam_name,
    a.exam_findings,
    NULL                 AS incremental_id
FROM rgd_udm_silver.examination a
INNER JOIN (
    SELECT COALESCE(ndid_v1, ndid) AS pat_id FROM biogen.patient_list
    UNION ALL
    SELECT ndid AS pat_id FROM biogen.patient_list_athenaone
) b ON a.ndid = b.pat_id
WHERE a.enc_date_proxy >= '2025-10-01'
  AND a.enc_date_proxy <= '2026-02-15';


-- -----------------------------------------------------------------------------
-- 10. note  (UNION ALL of notes_part1 + notes_part2)
-- -----------------------------------------------------------------------------
CREATE TABLE biogen_april.note AS
SELECT
    a.ndid,
    a.eid                AS encounter_id,
    a.enc_start_date     AS encounter_date,
    a.note_type,
    a.note_source,
    a.note,
    NULL                 AS incremental_id
FROM rgd_udm_silver.notes_part1 a
INNER JOIN (
    SELECT COALESCE(ndid_v1, ndid) AS pat_id FROM biogen.patient_list
    UNION ALL
    SELECT ndid AS pat_id FROM biogen.patient_list_athenaone
) b ON a.ndid = b.pat_id
WHERE a.enc_start_date >= '2025-10-01'
  AND a.enc_start_date <= '2026-02-15'

UNION ALL

SELECT
    a.ndid,
    a.eid                AS encounter_id,
    a.enc_start_date     AS encounter_date,
    a.note_type,
    a.note_source,
    a.note,
    NULL                 AS incremental_id
FROM rgd_udm_silver.notes_part2 a
INNER JOIN (
    SELECT COALESCE(ndid_v1, ndid) AS pat_id FROM biogen.patient_list
    UNION ALL
    SELECT ndid AS pat_id FROM biogen.patient_list_athenaone
) b ON a.ndid = b.pat_id
WHERE a.enc_start_date >= '2025-10-01'
  AND a.enc_start_date <= '2026-02-15';



