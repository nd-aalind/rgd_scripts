-- =========================
-- DIAGNOSIS
-- =========================
CREATE TABLE rgd_udm_staging.diagnosis AS
SELECT d.*, 'Y' AS udm_active_flag,
CONCAT_WS(':',
    COALESCE(psid,''), COALESCE(ndid,''), COALESCE(eid,''),
    COALESCE(enc_date,''), COALESCE(diag_date,''),
    COALESCE(diag_code,''), COALESCE(diag_desc,'')
) AS udm_unq_id
FROM rgd_udm_silver.diagnosis d;

-- =========================
-- PROCEDURES
-- =========================
CREATE TABLE rgd_udm_staging.procedures AS
SELECT d.*, 'Y' AS udm_active_flag,
CONCAT_WS(':',
    COALESCE(psid,''), COALESCE(ndid,''), COALESCE(eid,''),
    COALESCE(encounter_date,''), COALESCE(proc_start_date,''), COALESCE(proc_last_date,''),
    COALESCE(proc_code,''), COALESCE(proc_name,'')
) AS udm_unq_id
FROM rgd_udm_silver.procedures d;

-- =========================
-- MEDICATIONS
-- =========================
CREATE TABLE rgd_udm_staging.medications_part1 AS
SELECT d.*, 'Y' AS udm_active_flag,
CONCAT_WS(':',
    COALESCE(psid,''), COALESCE(ndid,''), COALESCE(eid,''),
    COALESCE(enc_date,''), COALESCE(med_start_date,''), COALESCE(med_end_date,''),
    COALESCE(med_code,''), COALESCE(med_name,'')
) AS udm_unq_id
FROM rgd_udm_silver.medications_part1 d;

CREATE TABLE rgd_udm_staging.medications_part2 AS
SELECT d.*, 'Y' AS udm_active_flag,
CONCAT_WS(':',
    COALESCE(psid,''), COALESCE(ndid,''), COALESCE(eid,''),
    COALESCE(enc_date,''), COALESCE(med_start_date,''), COALESCE(med_end_date,''),
    COALESCE(med_code,''), COALESCE(med_name,'')
) AS udm_unq_id
FROM rgd_udm_silver.medications_part2 d;

-- =========================
-- ALLERGIES
-- =========================
CREATE TABLE rgd_udm_staging.allergies AS
SELECT d.*, 'Y' AS udm_active_flag,
CONCAT_WS(':',
    COALESCE(psid,''), COALESCE(ndid,''), COALESCE(enc_id,''),
    COALESCE(enc_date,''), COALESCE(allergy_start_date,''),
    COALESCE(allergen_code,''), COALESCE(allergen_name,''), COALESCE(allergy_type,'')
) AS udm_unq_id
FROM rgd_udm_silver.allergies d;

-- =========================
-- LABS
-- =========================
CREATE TABLE rgd_udm_staging.labs AS
SELECT d.*, 'Y' AS udm_active_flag,
CONCAT_WS(':',
    COALESCE(psid,''), COALESCE(ndid,''), COALESCE(eid,''),
    COALESCE(enc_start_date,''), COALESCE(result_date,''),
    COALESCE(result_code,''), COALESCE(result_name,'')
) AS udm_unq_id
FROM rgd_udm_silver.labs d;

-- =========================
-- ENCOUNTERS
-- =========================
CREATE TABLE rgd_udm_staging.encounters AS
SELECT d.*, 'Y' AS udm_active_flag,
CONCAT_WS(':', COALESCE(psid,''), COALESCE(ndid,''), COALESCE(eid,'')) AS udm_unq_id
FROM rgd_udm_silver.encounters d;

-- =========================
-- EXAMINATION
-- =========================
CREATE TABLE rgd_udm_staging.examination AS
SELECT d.*, 'Y' AS udm_active_flag,
CONCAT_WS(':', COALESCE(psid,''), COALESCE(ndid,''), COALESCE(eid,'')) AS udm_unq_id
FROM rgd_udm_silver.examination d;

-- =========================
-- INSURANCE
-- =========================
CREATE TABLE rgd_udm_staging.insurance AS
SELECT d.*, 'Y' AS udm_active_flag
FROM rgd_udm_silver.insurance d;

-- =========================
-- NOTES
-- =========================
CREATE TABLE rgd_udm_staging.notes_part1 AS
SELECT d.*, 'Y' AS udm_active_flag
FROM rgd_udm_silver.notes_part1 d;

CREATE TABLE rgd_udm_staging.notes_part2 AS
SELECT d.*, 'Y' AS udm_active_flag
FROM rgd_udm_silver.notes_part2 d;

CREATE TABLE rgd_udm_staging.notes_part3 AS
SELECT d.*, 'Y' AS udm_active_flag
FROM rgd_udm_silver.notes_part3 d;

-- =========================
-- PATIENTS
-- =========================
CREATE TABLE rgd_udm_staging.patients AS
SELECT d.*, 'Y' AS udm_active_flag
FROM rgd_udm_silver.patients d;

-- =========================
-- PROGRESS NOTES
-- =========================
CREATE TABLE rgd_udm_staging.progressnotes_part1 AS
SELECT d.*, 'Y' AS udm_active_flag
FROM rgd_udm_silver.progressnotes_part1 d;

CREATE TABLE rgd_udm_staging.progressnotes_part2 AS
SELECT d.*, 'Y' AS udm_active_flag
FROM rgd_udm_silver.progressnotes_part2 d;

-- =========================
-- RADIOLOGY
-- =========================
CREATE TABLE rgd_udm_staging.radiology AS
SELECT d.*, 'Y' AS udm_active_flag
FROM rgd_udm_silver.radiology d;

-- =========================
-- ROS
-- =========================
CREATE TABLE rgd_udm_staging.ros AS
SELECT d.*, 'Y' AS udm_active_flag
FROM rgd_udm_silver.ros d;

-- =========================
-- VITALS
-- =========================
CREATE TABLE rgd_udm_staging.vitals AS
SELECT d.*, 'Y' AS udm_active_flag
FROM rgd_udm_silver.vitals d;