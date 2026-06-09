create schema dqqc_rgd;

use dqqc_rgd;


CREATE TABLE dqqc_rgd.dq_run_result (
    result_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    run_id VARCHAR(50),
    run_date DATETIME,
    triggered_by VARCHAR(100),
    run_status VARCHAR(20),
    rule_id VARCHAR(50),
    schema_name VARCHAR(100),
    table_name VARCHAR(100),
    column_name VARCHAR(100),
    total_rows INT,
    failed_rows INT,
    pass_percentage DECIMAL(5,2),
    rule_status VARCHAR(20),
    created_at DATETIME,
    updated_at DATETIME
);

select * from dq_run_result;


## MED_CODE

INSERT INTO dq_run_result (
    run_id,
    run_date,
    triggered_by,
    run_status,
    rule_id,
    schema_name,
    table_name,
    column_name,
    total_rows,
    failed_rows,
    pass_percentage,
    rule_status,
    created_at,
    updated_at
)
SELECT
    'RUN_20250212',
    NOW(),
    'DQ_JOB',
    'COMPLETED',
    'COMP-004',
    'rgd_udm_silver',
    'medication',
    'med_code',
    total_rows,
    failed_rows,
    pass_percentage,
    CASE
        WHEN pass_percentage >= 95 THEN 'PASS'
        WHEN pass_percentage >= 90 THEN 'WARN'
        ELSE 'FAIL'
    END,
    NOW(),
    NOW()
FROM (
    SELECT 
        COUNT(*) AS total_rows,
        SUM(med_code IS NULL OR med_code = '') AS failed_rows,
        ROUND(
            100 * (1 - SUM(med_code IS NULL OR med_code = '') / COUNT(*)), 
            2
        ) AS pass_percentage
    FROM rgd_udm_silver.medication
) t;


## DIAG_CODE

SELECT * FROM rgd_udm_silver.diagnosis_inc limit 1;

show index from rgd_udm_silver.diagnosis_inc;

create index idx_dg_cd on rgd_udm_silver.diagnosis_inc(diag_code);

INSERT INTO dq_run_result (
    run_id,
    run_date,
    triggered_by,
    run_status,
    rule_id,
    schema_name,
    table_name,
    column_name,
    total_rows,
    failed_rows,
    pass_percentage,
    rule_status,
    created_at,
    updated_at
)
SELECT
    'RUN_20250212',
    NOW(),
    'DQ_JOB',
    'COMPLETED',
    'COMP-003',
    'rgd_udm_silver',
    'diagnosis_inc',
    'diag_code',
    total_rows,
    failed_rows,
    pass_percentage,
    CASE
        WHEN pass_percentage >= 95 THEN 'PASS'
        WHEN pass_percentage >= 90 THEN 'WARN'
        ELSE 'FAIL'
    END,
    NOW(),
    NOW()
FROM (
    SELECT 
        COUNT(*) AS total_rows,
        SUM(diag_code IS NULL OR diag_code = '') AS failed_rows,
        ROUND(
            100 * (1 - SUM(diag_code IS NULL OR diag_code = '') / COUNT(*)), 
            2
        ) AS pass_percentage
    FROM rgd_udm_silver.diagnosis_inc
) t;



### ndid 

INSERT INTO dq_run_result (
    run_id,
    run_date,
    triggered_by,
    run_status,
    rule_id,
    schema_name,
    table_name,
    column_name,
    total_rows,
    failed_rows,
    pass_percentage,
    rule_status,
    created_at,
    updated_at
)
SELECT
    'RUN_20250212',
    NOW(),
    'DQ_JOB',
    'COMPLETED',
    'COMP-001',
    'rgd_udm_silver',
    table_name,
    'ndid',
    total_rows,
    failed_rows,
    ROUND(100 * (1 - failed_rows / total_rows),2) AS pass_percentage,
    CASE
        WHEN failed_rows = 0 THEN 'PASS'
        ELSE 'FAIL'
    END,
    NOW(),
    NOW()
FROM (

    SELECT 'patient_demographics' table_name,
           COUNT(*) total_rows,
           SUM(ndid IS NULL OR ndid='') failed_rows
    FROM rgd_udm_silver.patient_demographics

    UNION ALL

    SELECT 'encounters',
           COUNT(*),
           SUM(ndid IS NULL OR ndid='')
    FROM rgd_udm_silver.encounters

    UNION ALL

    SELECT 'diagnosis_inc',
           COUNT(*),
           SUM(ndid IS NULL OR ndid='')
    FROM rgd_udm_silver.diagnosis_inc

    UNION ALL

    SELECT 'procedures_inc',
           COUNT(*),
           SUM(ndid IS NULL OR ndid='')
    FROM rgd_udm_silver.procedures_inc

    UNION ALL

    SELECT 'medication',
           COUNT(*),
           SUM(ndid IS NULL OR ndid='')
    FROM rgd_udm_silver.medication

    UNION ALL

    SELECT 'labs',
           COUNT(*),
           SUM(ndid IS NULL OR ndid='')
    FROM rgd_udm_silver.labs

    UNION ALL

    SELECT 'radiology_inc',
           COUNT(*),
           SUM(ndid IS NULL OR ndid='')
    FROM rgd_udm_silver.radiology_inc

    UNION ALL

    SELECT 'vitals_inc',
           COUNT(*),
           SUM(ndid IS NULL OR ndid='')
    FROM rgd_udm_silver.vitals_inc

    UNION ALL

    SELECT 'allergies_inc',
           COUNT(*),
           SUM(ndid IS NULL OR ndid='')
    FROM rgd_udm_silver.allergies_inc

    UNION ALL

    SELECT 'examination_inc',
           COUNT(*),
           SUM(ndid IS NULL OR ndid='')
    FROM rgd_udm_silver.examination_inc

    UNION ALL

    SELECT 'ros_inc',
           COUNT(*),
           SUM(ndid IS NULL OR ndid='')
    FROM rgd_udm_silver.ros_inc

) t;

show index from rgd_udm_silver.patient_demographics;

create index idx_ndid on rgd_udm_silver.patient_demographics(ndid);


### eid 

select * from dq_run_result;

INSERT INTO dq_run_result (
    run_id,
    run_date,
    triggered_by,
    run_status,
    rule_id,
    schema_name,
    table_name,
    column_name,
    total_rows,
    failed_rows,
    pass_percentage,
    rule_status,
    created_at,
    updated_at
)
SELECT
    'RUN_20250212',
    NOW(),
    'DQ_JOB',
    'COMPLETED',
    'COMP-002',
    'rgd_udm_silver',
    table_name,
    'eid',
    total_rows,
    failed_rows,
    ROUND(100 * (1 - failed_rows / total_rows),2) AS pass_percentage,
    CASE
        WHEN failed_rows = 0 THEN 'PASS'
        ELSE 'FAIL'
    END,
    NOW(),
    NOW()
FROM (

    SELECT 'patient_demographics' table_name,
           COUNT(*) total_rows,
           SUM(eid IS NULL OR eid='') failed_rows
    FROM rgd_udm_silver.encounters

    UNION ALL

    SELECT 'diagnosis_inc',
           COUNT(*),
           SUM(eid IS NULL OR eid='')
    FROM rgd_udm_silver.diagnosis_inc

    UNION ALL

    SELECT 'procedures_inc',
           COUNT(*),
           SUM(eid IS NULL OR eid='')
    FROM rgd_udm_silver.procedures_inc

    UNION ALL

    SELECT 'medication',
           COUNT(*),
           SUM(eid IS NULL OR eid='')
    FROM rgd_udm_silver.medication

    UNION ALL

    SELECT 'labs',
           COUNT(*),
           SUM(eid IS NULL OR eid='')
    FROM rgd_udm_silver.labs

    UNION ALL

    SELECT 'radiology_inc',
           COUNT(*),
           SUM(eid IS NULL OR eid='')
    FROM rgd_udm_silver.radiology_inc

    UNION ALL

    SELECT 'vitals_inc',
           COUNT(*),
           SUM(eid IS NULL OR eid='')
    FROM rgd_udm_silver.vitals_inc

    UNION ALL

    SELECT 'allergies_inc',
           COUNT(*),
           SUM(eid IS NULL OR eid='')
    FROM rgd_udm_silver.allergies_inc

    UNION ALL

    SELECT 'examination_inc',
           COUNT(*),
           SUM(enc_id  IS NULL OR enc_id='')
    FROM rgd_udm_silver.examination_inc

    UNION ALL

    SELECT 'ros_inc',
           COUNT(*),
           SUM(eid IS NULL OR eid='')
    FROM rgd_udm_silver.ros_inc

) t;


### desease status

INSERT INTO dq_run_result (
    run_id,
    run_date,
    triggered_by,
    run_status,
    rule_id,
    schema_name,
    table_name,
    column_name,
    total_rows,
    failed_rows,
    pass_percentage,
    rule_status,
    created_at,
    updated_at
)
SELECT
    'RUN_20250212',
    NOW(),
    'DQ_JOB',
    'COMPLETED',
    'COMP-010',
    'rgd_udm_silver',
    'patient_demographics',
    'pat_deceased_status',
    total_rows,
    failed_rows,
    pass_percentage,
     CASE
        WHEN failed_rows = 0 THEN 'PASS'
        ELSE 'FAIL'
    END,
    NOW(),
    NOW()
FROM (
    SELECT 
        COUNT(*) AS total_rows,
        SUM(pat_deceased_status IS NULL OR pat_deceased_status = '') AS failed_rows,
        ROUND(
            100 * (1 - SUM(pat_deceased_status IS NULL OR pat_deceased_status = '') / COUNT(*)), 
            2
        ) AS pass_percentage
    FROM rgd_udm_silver.patient_demographics
) t;

select *  from rgd_udm_silver.patient_demographics limit 10;

create index idx_dob on rgd_udm_silver.patient_demographics(dob);


## gender

INSERT INTO dq_run_result (
    run_id,
    run_date,
    triggered_by,
    run_status,
    rule_id,
    schema_name,
    table_name,
    column_name,
    total_rows,
    failed_rows,
    pass_percentage,
    rule_status,
    created_at,
    updated_at
)
SELECT
    'RUN_20250212',
    NOW(),
    'DQ_JOB',
    'COMPLETED',
    'COMP-011',
    'rgd_udm_silver',
    'patient_demographics',
    'gender',
    total_rows,
    failed_rows,
    pass_percentage,
     CASE
        WHEN failed_rows = 0 THEN 'PASS'
        ELSE 'FAIL'
    END,
    NOW(),
    NOW()
FROM (
    SELECT 
        COUNT(*) AS total_rows,
        SUM(gender IS NULL OR gender = '') AS failed_rows,
        ROUND(
            100 * (1 - SUM(gender IS NULL OR gender = '') / COUNT(*)), 
            2
        ) AS pass_percentage
    FROM rgd_udm_silver.patient_demographics
) t;


## DOB

INSERT INTO dq_run_result (
    run_id,
    run_date,
    triggered_by,
    run_status,
    rule_id,
    schema_name,
    table_name,
    column_name,
    total_rows,
    failed_rows,
    pass_percentage,
    rule_status,
    created_at,
    updated_at
)
SELECT
    'RUN_20250212',
    NOW(),
    'DQ_JOB',
    'COMPLETED',
    'COMP-012',
    'rgd_udm_silver',
    'patient_demographics',
    'dob',
    total_rows,
    failed_rows,
    pass_percentage,
     CASE
        WHEN failed_rows = 0 THEN 'PASS'
        ELSE 'FAIL'
    END,
    NOW(),
    NOW()
FROM (
    SELECT 
        COUNT(*) AS total_rows,
        SUM(dob IS NULL OR dob = '') AS failed_rows,
        ROUND(
            100 * (1 - SUM(dob IS NULL OR dob = '') / COUNT(*)), 
            2
        ) AS pass_percentage
    FROM rgd_udm_silver.patient_demographics
) t;

## encounter date

select * from dq_run_result;

DELETE from dq_run_result where column_name = 'enc_date';

INSERT INTO dq_run_result (
    run_id,
    run_date,
    triggered_by,
    run_status,
    rule_id,
    schema_name,
    table_name,
    column_name,
    total_rows,
    failed_rows,
    pass_percentage,
    rule_status,
    created_at,
    updated_at
)
SELECT
    'RUN_20250212',
    NOW(),
    'DQ_JOB',
    'COMPLETED',
    'COMP-013',
    'rgd_udm_silver',
    'encounters',
    'enc_date',
    total_rows,
    failed_rows,
    ROUND(100 * (1 - failed_rows / total_rows),2) AS pass_percentage,
    CASE
        WHEN failed_rows = 0 THEN 'PASS'
        ELSE 'FAIL'
    END,
    NOW(),
    NOW()
FROM (
    SELECT
        COUNT(*) AS total_rows,
        SUM(enc_date IS NULL or CAST(enc_date AS CHAR) = '') AS failed_rows
    FROM rgd_udm_silver.encounters
) t;

select * from rgd_udm_silver.encounters limit 1;

show index from rgd_udm_silver.encounters;

create index idx_enc_dt on rgd_udm_silver.encounters(enc_date);


### Diagnosis Date

INSERT INTO dq_run_result (
    run_id,
    run_date,
    triggered_by,
    run_status,
    rule_id,
    schema_name,
    table_name,
    column_name,
    total_rows,
    failed_rows,
    pass_percentage,
    rule_status,
    created_at,
    updated_at
)
SELECT
    'RUN_20250212',
    NOW(),
    'DQ_JOB',
    'COMPLETED',
    'COMP-014',
    'rgd_udm_silver',
    'diagnosis_inc',
    'diag_date',
    total_rows,
    failed_rows,
    pass_percentage,
    CASE
        WHEN pass_percentage >= 95 THEN 'PASS'
        ELSE 'FAIL'
    END,
    NOW(),
    NOW()
FROM (
    SELECT
        COUNT(*) AS total_rows,
        SUM(diag_date IS NULL OR CAST(diag_date AS CHAR) = '') AS failed_rows,
        ROUND(
            100 * (1 - SUM(diag_date IS NULL OR CAST(diag_date AS CHAR) = '') / COUNT(*)),
            2
        ) AS pass_percentage
    FROM rgd_udm_silver.diagnosis_inc
) t;

select * from rgd_udm_silver.diagnosis_inc limit 1;

show index from rgd_udm_silver.diagnosis_inc;

create index idx_enc_dt on rgd_udm_silver.diagnosis_inc(diag_date);


### Medication date


INSERT INTO dq_run_result (
    run_id,
    run_date,
    triggered_by,
    run_status,
    rule_id,
    schema_name,
    table_name,
    column_name,
    total_rows,
    failed_rows,
    pass_percentage,
    rule_status,
    created_at,
    updated_at
)
SELECT
    'RUN_20250212',
    NOW(),
    'DQ_JOB',
    'COMPLETED',
    'COMP-015',
    'rgd_udm_silver',
    'medication',
    'med_start_date',
    total_rows,
    failed_rows,
    pass_percentage,
    CASE
        WHEN pass_percentage >= 90 THEN 'PASS'
        ELSE 'FAIL'
    END,
    NOW(),
    NOW()
FROM (
    SELECT
        COUNT(*) AS total_rows,
        SUM(med_start_date IS NULL OR CAST(med_start_date AS CHAR) = '') AS failed_rows,
        ROUND(
            100 * (1 - SUM(med_start_date IS NULL OR CAST(med_start_date AS CHAR) = '') / COUNT(*)),
            2
        ) AS pass_percentage
    FROM rgd_udm_silver.medication
) t;


select * from rgd_udm_silver.medication limit 1;

show index from rgd_udm_silver.medication;


### LABs date

select * from dq_run_result;


INSERT INTO dq_run_result (
    run_id,
    run_date,
    triggered_by,
    run_status,
    rule_id,
    schema_name,
    table_name,
    column_name,
    total_rows,
    failed_rows,
    pass_percentage,
    rule_status,
    created_at,
    updated_at
)
SELECT
    'RUN_20250212',
    NOW(),
    'DQ_JOB',
    'COMPLETED',
    'COMP-015',
    'rgd_udm_silver',
    'labs',
    'result_date',
    total_rows,
    failed_rows,
    pass_percentage,
    CASE
        WHEN pass_percentage >= 95 THEN 'PASS'
        ELSE 'FAIL'
    END,
    NOW(),
    NOW()
FROM (
    SELECT
        COUNT(*) AS total_rows,
        SUM(result_date IS NULL OR CAST(result_date AS CHAR) = '') AS failed_rows,
        ROUND(
            100 * (1 - SUM(result_date IS NULL OR CAST(result_date AS CHAR) = '') / COUNT(*)),
            2
        ) AS pass_percentage
    FROM rgd_udm_silver.labs
) t;

select * from rgd_udm_silver.labs limit 1;

show index from rgd_udm_silver.labs;

create index idx_resdt on rgd_udm_silver.labs(result_date);






### Procedure date

select * from rgd_udm_silver.vitals_inc vi;

select * from dqqc_rgd.dq_run_result;


INSERT INTO dq_run_result (
    run_id,
    run_date,
    triggered_by,
    run_status,
    rule_id,
    schema_name,
    table_name,
    column_name,
    total_rows,
    failed_rows,
    pass_percentage,
    rule_status,
    created_at,
    updated_at
)
SELECT
    'RUN_20250212',
    NOW(),
    'DQ_JOB',
    'COMPLETED',
    'COMP-017',
    'rgd_udm_silver',
    'procedures_inc',
    'proc_start_date',
    total_rows,
    failed_rows,
    pass_percentage,
    CASE
        WHEN pass_percentage >= 95 THEN 'PASS'
        ELSE 'FAIL'
    END,
    NOW(),
    NOW()
FROM (
    SELECT
        COUNT(*) AS total_rows,
        SUM(proc_start_date IS NULL OR CAST(proc_start_date AS CHAR) = '') AS failed_rows,
        ROUND(
            100 * (1 - SUM(proc_start_date IS NULL OR CAST(proc_start_date AS CHAR) = '') / COUNT(*)),
            2
        ) AS pass_percentage
    FROM rgd_udm_silver.procedures_inc
) t;


select * from rgd_udm_silver.procedures_inc limit 1;

show index from rgd_udm_silver.procedures_inc;

create index idx_resdt on rgd_udm_silver.procedures_inc(result_date);


SELECT COUNT(*),COUNT(DISTINCT CHARTID),COUNT() FROM tng_athena_one.PATIENT;

SELECT * FROM raleigh.APPOINTMENTELIGIBILITYINFO;