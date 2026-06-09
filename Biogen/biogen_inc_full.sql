-- =============================================================================
-- Biogen Incremental (May) — Full SQL Extract
-- Cohort  : biogen_april.patients_demo_inc_combined  (JOIN on ndid)
-- Date    : enc_date_proxy >= '2026-02-16' AND enc_date_proxy <= '2026-03-31'
-- Targets : biogen_april.*_inc_may_v2
-- Run each statement independently; skip any whose source table does not exist.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- 1. encounters
-- -----------------------------------------------------------------------------
CREATE TABLE biogen_april.encounters_inc_may_v2 AS
SELECT
    a.ndid,
    a.eid                AS encounter_id,
    a.enc_date           AS encounter_date,
    a.enc_reason         AS encounter_reason,
    a.provider_name      AS at_phy_name,
    CAST(NULL AS SIGNED) AS incremental_id,
    a.udm_active_flag,
    a.udm_unq_id
FROM incremental_test.encounters a
INNER JOIN biogen_april.patients_demo_inc_combined c ON a.ndid = c.ndid
WHERE a.enc_date_proxy >= '2026-02-16'
  AND a.enc_date_proxy <= '2026-03-31';


-- -----------------------------------------------------------------------------
-- 2. diagnosis
-- -----------------------------------------------------------------------------
CREATE TABLE biogen_april.diagnosis_inc_may_v2 AS
SELECT
    a.ndid,
    a.eid                AS encounter_id,
    a.enc_date           AS encounter_date,
    a.diag_date          AS diagnosis_recorded_date,
    a.diag_code,
    a.diag_desc,
    a.diag_coding_system,
    CAST(NULL AS SIGNED) AS incremental_id,
    a.udm_active_flag,
    a.udm_unq_id
FROM incremental_test.diagnosis a
INNER JOIN biogen_april.patients_demo_inc_combined c ON a.ndid = c.ndid
WHERE a.enc_date_proxy >= '2026-02-16'
  AND a.enc_date_proxy <= '2026-03-31';


-- -----------------------------------------------------------------------------
-- 3. procedures
-- -----------------------------------------------------------------------------
CREATE TABLE biogen_april.procedures_inc_may_v2 AS
SELECT
    a.ndid,
    a.eid                AS encounter_id,
    a.encounter_date,
    a.proc_start_date    AS procedure_date,
    a.proc_code,
    a.proc_name,
    a.proc_coding_system,
    CAST(NULL AS SIGNED) AS incremental_id,
    a.udm_active_flag,
    a.udm_unq_id
FROM incremental_test.procedures a
INNER JOIN biogen_april.patients_demo_inc_combined c ON a.ndid = c.ndid
WHERE a.enc_date_proxy >= '2026-02-16'
  AND a.enc_date_proxy <= '2026-03-31';


-- -----------------------------------------------------------------------------
-- 4. allergies
-- -----------------------------------------------------------------------------
CREATE TABLE biogen_april.allergies_inc_may_v2 AS
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
    CAST(NULL AS SIGNED)         AS incremental_id,
    a.udm_active_flag,
    a.udm_unq_id
FROM incremental_test.allergies a
INNER JOIN biogen_april.patients_demo_inc_combined c ON a.ndid = c.ndid
WHERE a.enc_date_proxy >= '2026-02-16'
  AND a.enc_date_proxy <= '2026-03-31';


-- -----------------------------------------------------------------------------
-- 5. vitals
-- -----------------------------------------------------------------------------
CREATE TABLE biogen_april.vitals_inc_may_v2 AS
SELECT
    a.ndid,
    a.eid                AS encounter_id,
    a.vital_date,
    a.vital_coding_system,
    a.vital_id,
    a.vital_name,
    a.vital_result,
    a.vital_unit,
    CAST(NULL AS SIGNED) AS incremental_id,
    a.udm_active_flag,
    a.udm_unq_id
FROM incremental_test.vitals a
INNER JOIN biogen_april.patients_demo_inc_combined c ON a.ndid = c.ndid
WHERE a.enc_date_proxy >= '2026-02-16'
  AND a.enc_date_proxy <= '2026-03-31';


-- -----------------------------------------------------------------------------
-- 6. labs
-- -----------------------------------------------------------------------------
CREATE TABLE biogen_april.labs_inc_may_v2 AS
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
    CAST(NULL AS SIGNED)         AS incremental_id,
    a.udm_active_flag,
    a.udm_unq_id
FROM incremental_test.labs a
INNER JOIN biogen_april.patients_demo_inc_combined c ON a.ndid = c.ndid
WHERE a.enc_date_proxy >= '2026-02-16'
  AND a.enc_date_proxy <= '2026-03-31';


-- -----------------------------------------------------------------------------
-- 7. radiology  (no udm_inc_id in source — no batching needed)
-- -----------------------------------------------------------------------------
CREATE TABLE biogen_april.radiology_inc_may_v2 AS
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
    CAST(NULL AS SIGNED)     AS incremental_id,
    a.udm_active_flag,
    a.udm_unq_id
FROM incremental_test.radiology a
INNER JOIN biogen_april.patients_demo_inc_combined c ON a.ndid = c.ndid
WHERE a.enc_date_proxy >= '2026-02-16'
  AND a.enc_date_proxy <= '2026-03-31';


-- -----------------------------------------------------------------------------
-- 8. ros
-- -----------------------------------------------------------------------------
CREATE TABLE biogen_april.ros_inc_may_v2 AS
SELECT
    a.ndid,
    a.eid                AS encounter_id,
    a.enc_start_date     AS encounter_date,
    a.ros_category,
    a.ros_name           AS system_name,
    a.ros_option         AS Present,
    a.ros_notes          AS note,
    CAST(NULL AS SIGNED) AS incremental_id,
    a.udm_active_flag,
    a.udm_unq_id
FROM incremental_test.ros a
INNER JOIN biogen_april.patients_demo_inc_combined c ON a.ndid = c.ndid
WHERE a.enc_date_proxy >= '2026-02-16'
  AND a.enc_date_proxy <= '2026-03-31';


-- -----------------------------------------------------------------------------
-- 9. examination
-- -----------------------------------------------------------------------------
CREATE TABLE biogen_april.examination_inc_may_v2 AS
SELECT
    a.ndid,
    a.eid                AS encounter_id,
    a.enc_start_date     AS encounter_date,
    a.exam_date,
    a.examid             AS exam_id,
    a.exam_category,
    a.exam_name,
    a.exam_findings,
    CAST(NULL AS SIGNED) AS incremental_id,
    a.udm_active_flag,
    a.udm_unq_id
FROM incremental_test.examination a
INNER JOIN biogen_april.patients_demo_inc_combined c ON a.ndid = c.ndid
WHERE a.enc_date_proxy >= '2026-02-16'
  AND a.enc_date_proxy <= '2026-03-31';


-- -----------------------------------------------------------------------------
-- 10. note  (UNION ALL of notes_part1 + notes_part2 — skip if sources missing)
-- -----------------------------------------------------------------------------
CREATE TABLE biogen_april.note_inc_may_v2 AS
SELECT
    a.ndid,
    a.eid                AS encounter_id,
    a.enc_start_date     AS encounter_date,
    a.note_type,
    a.note_source,
    a.note,
    CAST(NULL AS SIGNED) AS incremental_id,
    a.udm_active_flag,
    a.udm_unq_id
FROM incremental_test.notes_part1 a
INNER JOIN biogen_april.patients_demo_inc_combined c ON a.ndid = c.ndid
WHERE a.enc_start_date >= '2026-02-16'
  AND a.enc_start_date <= '2026-03-31'

UNION ALL

SELECT
    a.ndid,
    a.eid                AS encounter_id,
    a.enc_start_date     AS encounter_date,
    a.note_type,
    a.note_source,
    a.note,
    CAST(NULL AS SIGNED) AS incremental_id,
    a.udm_active_flag,
    a.udm_unq_id
FROM incremental_test.notes_part2 a
INNER JOIN biogen_april.patients_demo_inc_combined c ON a.ndid = c.ndid
WHERE a.enc_start_date >= '2026-02-16'
  AND a.enc_start_date <= '2026-03-31';


-- -----------------------------------------------------------------------------
-- 11. medications  (optional — skip if source missing)
-- -----------------------------------------------------------------------------
CREATE TABLE biogen_april.medications_inc_may_v2 AS
SELECT
    a.ndid,
    a.med_id,
    a.eid                AS encounter_id,
    a.enc_start_date,
    a.med_start_date,
    a.med_end_date,
    a.med_code,
    a.med_name,
    a.med_coding_system,
    a.med_status,
    a.med_formulation,
    a.med_strength,
    a.med_pb_qty,
    a.med_days_supply,
    a.med_refills,
    a.med_directions,
    a.med_fill_type,
    CAST(NULL AS SIGNED) AS incremental_id,
    a.udm_active_flag,
    a.udm_unq_id
FROM incremental_test.medications a
INNER JOIN biogen_april.patients_demo_inc_combined c ON a.ndid = c.ndid
WHERE a.enc_start_date >= '2026-02-16'
  AND a.enc_start_date <= '2026-03-31';


-- -----------------------------------------------------------------------------
-- 12. past_history  (optional — skip if source missing)
-- -----------------------------------------------------------------------------
CREATE TABLE biogen_april.past_history_inc_may_v2 AS
SELECT
    a.ndid,
    a.eid                      AS encounter_id,
    a.visit_date,
    a.medical_history,
    a.past_surgical_history,
    a.family_history_note,
    a.social_history_full,
    CAST(NULL AS SIGNED)       AS incremental_id,
    a.udm_active_flag,
    a.udm_unq_id
FROM incremental_test.past_history a
INNER JOIN biogen_april.patients_demo_inc_combined c ON a.ndid = c.ndid
WHERE a.visit_date >= '2026-02-16'
  AND a.visit_date <= '2026-03-31';


-- -----------------------------------------------------------------------------
-- 13. patient_demographics  (optional — no date filter — skip if source missing)
-- -----------------------------------------------------------------------------
CREATE TABLE biogen_april.patient_demographics_inc_may_v2 AS
SELECT
    a.ndid,
    a.year_of_birth,
    a.gender,
    a.pat_lan,
    a.pat_country,
    a.pat_zip,
    a.pat_race,
    a.pat_ms,
    a.pat_ds,
    a.deceasedDate,
    CAST(NULL AS SIGNED) AS incremental_id,
    a.udm_active_flag,
    a.udm_unq_id
FROM incremental_test.patient_demographics a
INNER JOIN biogen_april.patients_demo_inc_combined c ON a.ndid = c.ndid;


-- =============================================================================
-- Cleanup (run after verifying all targets are correct)
-- =============================================================================
-- DROP TABLE IF EXISTS biogen_april.encounters_inc_may_v2;
-- DROP TABLE IF EXISTS biogen_april.diagnosis_inc_may_v2;
-- DROP TABLE IF EXISTS biogen_april.procedures_inc_may_v2;
-- DROP TABLE IF EXISTS biogen_april.allergies_inc_may_v2;
-- DROP TABLE IF EXISTS biogen_april.vitals_inc_may_v2;
-- DROP TABLE IF EXISTS biogen_april.labs_inc_may_v2;
-- DROP TABLE IF EXISTS biogen_april.radiology_inc_may_v2;
-- DROP TABLE IF EXISTS biogen_april.ros_inc_may_v2;
-- DROP TABLE IF EXISTS biogen_april.examination_inc_may_v2;
-- DROP TABLE IF EXISTS biogen_april.note_inc_may_v2;
-- DROP TABLE IF EXISTS biogen_april.medications_inc_may_v2;
-- DROP TABLE IF EXISTS biogen_april.past_history_inc_may_v2;
-- DROP TABLE IF EXISTS biogen_april.patient_demographics_inc_may_v2;
