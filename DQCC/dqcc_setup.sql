-- ============================================================
-- DQCC Setup — run once before first execution of dqcc_runner.py
-- Creates dq_rule table and seeds all COMP-001 to COMP-020 rules
-- ============================================================

USE dqqc_rgd;

-- ─────────────────────────────────────────────────────────────
-- 1. Rule definition table
-- ─────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS dqqc_rgd.dq_rule (
    rule_id         VARCHAR(50)     NOT NULL PRIMARY KEY,
    rule_name       VARCHAR(200)    NOT NULL,
    rule_type       VARCHAR(50)     NOT NULL,
    rule_expression VARCHAR(500)    NOT NULL,
    threshold       DECIMAL(5,2)    NOT NULL COMMENT 'Minimum pass% required',
    severity        VARCHAR(20)     NOT NULL COMMENT 'Critical / Major / Minor',
    is_active       CHAR(1)         NOT NULL DEFAULT 'Y',
    created_by      VARCHAR(50)     NOT NULL,
    created_at      DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at      DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP
                                    ON UPDATE CURRENT_TIMESTAMP
);

-- ─────────────────────────────────────────────────────────────
-- 2. Result table (matches existing dq_run_result; recreate
--    only if starting fresh — comment out if table already exists)
-- ─────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS dqqc_rgd.dq_run_result (
    result_id        BIGINT AUTO_INCREMENT PRIMARY KEY,
    run_id           VARCHAR(50),
    run_date         DATETIME,
    triggered_by     VARCHAR(100),
    run_status       VARCHAR(20),
    rule_id          VARCHAR(50),
    schema_name      VARCHAR(100),
    table_name       VARCHAR(100),
    column_name      VARCHAR(100),
    total_rows       INT,
    failed_rows      INT,
    pass_percentage  DECIMAL(5,2),
    rule_status      VARCHAR(20),
    created_at       DATETIME,
    updated_at       DATETIME
);

-- Useful index for dashboarding / querying latest run
CREATE INDEX IF NOT EXISTS idx_run_rule ON dqqc_rgd.dq_run_result (run_id, rule_id);

-- ─────────────────────────────────────────────────────────────
-- 3. Seed dq_rule
--    threshold = minimum pass_percentage for a PASS verdict
-- ─────────────────────────────────────────────────────────────

INSERT INTO dqqc_rgd.dq_rule
    (rule_id, rule_name, rule_type, rule_expression, threshold, severity, is_active, created_by)
VALUES
-- COMP-001  ndid across all 11 tables
('COMP-001', 'Patient ID completeness',
 'Completeness', 'Every record has a valid ndid across all clinical tables',
 100.00, 'Critical', 'Y', 'Data Engineering'),

-- COMP-002  encounter_id across encounter-level tables
('COMP-002', 'Encounter ID completeness',
 'Completeness', 'All encounter-level records have a valid eid / enc_id',
 100.00, 'Critical', 'Y', 'Data Engineering'),

-- COMP-003  date fields across all clinical tables
('COMP-003', 'Date field completeness',
 'Completeness', 'All clinical event records have a populated date field',
 98.00, 'Critical', 'Y', 'Data Engineering'),

-- COMP-004  diag_code in diagnosis
('COMP-004', 'AD/MCI diagnosis code',
 'Completeness', 'Diagnosis records have a populated diag_code',
 95.00, 'Critical', 'Y', 'Data Engineering'),

-- COMP-005  med_code in medication
('COMP-005', 'Medication identifier',
 'Completeness', 'Medication records have a populated med_code (NDC or RxNorm)',
 95.00, 'Critical', 'Y', 'Data Engineering'),

-- COMP-006  baseline cognitive score (NLP / examinations — manual)
('COMP-006', 'Baseline cognitive score',
 'Completeness', 'AD/MCI patients have baseline MMSE/MoCA/CDR score',
 90.00, 'Critical', 'N', 'Data Science'),

-- COMP-007  follow-up cognitive score (NLP / examinations — manual)
('COMP-007', 'Follow-up cognitive score',
 'Completeness', 'AD/MCI patients have >=1 follow-up cognitive assessment',
 70.00, 'Major', 'N', 'Data Science'),

-- COMP-008  lab core fields
('COMP-008', 'Lab core fields',
 'Completeness', 'Lab records have lab_name, lab_value, lab_unit, result_date populated',
 95.00, 'Major', 'Y', 'Data Engineering'),

-- COMP-009  AD medication details
('COMP-009', 'AD medication details',
 'Completeness', 'AD medication records have med_name, med_strength, med_start_date',
 90.00, 'Major', 'Y', 'Data Engineering'),

-- COMP-010  pat_deceased_status
('COMP-010', 'Death status',
 'Completeness', 'All patients have pat_deceased_status populated',
 100.00, 'Critical', 'Y', 'Data Engineering'),

-- COMP-011  gender
('COMP-011', 'Gender',
 'Completeness', 'All patients have gender populated',
 100.00, 'Critical', 'Y', 'Data Engineering'),

-- COMP-012  dob
('COMP-012', 'Year of birth',
 'Completeness', 'All patients have dob populated',
 100.00, 'Critical', 'Y', 'Data Engineering'),

-- COMP-013  enc_date
('COMP-013', 'Encounter date',
 'Completeness', 'All encounters have enc_date populated',
 100.00, 'Critical', 'Y', 'Data Engineering'),

-- COMP-014  diag_date
('COMP-014', 'Diagnosis date',
 'Completeness', 'All diagnoses have diag_date populated',
 95.00, 'Major', 'Y', 'Data Engineering'),

-- COMP-015  med_start_date
('COMP-015', 'Medication start date',
 'Completeness', 'All active medications have med_start_date populated',
 90.00, 'Major', 'Y', 'Data Engineering'),

-- COMP-016  result_date (labs)
('COMP-016', 'Lab result date',
 'Completeness', 'All lab results have result_date populated',
 95.00, 'Major', 'Y', 'Data Engineering'),

-- COMP-017  proc_start_date
('COMP-017', 'Procedure date',
 'Completeness', 'All procedures have proc_start_date populated',
 95.00, 'Major', 'Y', 'Data Engineering'),

-- COMP-018  site-level completeness (derived / analytics — manual)
('COMP-018', 'Site-level completeness',
 'Completeness', 'Completeness stratified by data source site; flag if >10% diff between sites',
 90.00, 'Major', 'N', 'RWE Analytics'),

-- COMP-019  longitudinal follow-up (derived cohort — manual)
('COMP-019', 'Longitudinal follow-up',
 'Coverage', 'AD/MCI cohort has >= 6 months of follow-up',
 70.00, 'Major', 'N', 'RWE Analytics'),

-- COMP-020  incremental_id across all tables
('COMP-020', 'Incremental ID present',
 'Completeness', 'All records across all 11 tables have incremental_id populated',
 100.00, 'Critical', 'Y', 'Data Engineering')

ON DUPLICATE KEY UPDATE
    threshold  = VALUES(threshold),
    is_active  = VALUES(is_active),
    updated_at = CURRENT_TIMESTAMP;


-- ─────────────────────────────────────────────────────────────
-- 4. Quick verify
-- ─────────────────────────────────────────────────────────────

SELECT rule_id, rule_name, threshold, severity, is_active
FROM dqqc_rgd.dq_rule
ORDER BY rule_id;
