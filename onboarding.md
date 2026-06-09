# SQL Optimizer — Onboarding Reference

> Edit this file whenever credentials, table names, or conventions change.
> Paste the relevant sections into each new query / script request.

---

## 1. Database Connection

```python
DB_CONFIG = {
    "host":            "ndai-dev-rds-instance.cwp60ymu4ko0.us-east-1.rds.amazonaws.com",
    "port":            3306,
    "user":            "Aalind",
    "password":        "A@L1nd@123",
    "database":        "rgd_udm_silver",
    "charset":         "utf8mb4",
    "connect_timeout": 30,
    "read_timeout":    21600,
    "write_timeout":   21600,
}
```

---

## 2. Schema Map

| Schema | Purpose |
|--------|---------|
| `rgd_udm_silver` | Production silver-layer tables (write target for ETL) |
| `rgd_udm_staging` | Staging / test tables (write target during dev) |
| `staging` | Temp tables: PK ranges, checkpoints, materialized CTEs |
| `semantics` | Lookup tables (LOINC, ICD mappings, standardization refs) |
| `tncpa` / `raleigh` | AthenaOne source databases |
| `noran` | AthenaPlus / Noran source database |
| `kinsula_leq` | eClinicalWorks source database |
| `greenway` | Greenway source database |
| `dqqc_rgd` | DQCC rule and result tables |

---

## 3. EHR Systems

| EHR | Short | Source DB | Active Flag | Notes |
|-----|-------|-----------|-------------|-------|
| AthenaOne | `ao` | `tncpa` / `raleigh` | `nd_active_flag = 'Y'` | SNOMED hierarchy joins |
| AthenaPlus / Noran | `ap` | `noran` | `nd_active_flag = 'Y'` | OBS/OBSHEAD/HIERGRPS |
| eClinicalWorks | `ecw` | `kinsula_leq` | `activeflag = 1` | ICD-9/10, DATE strings |
| Greenway | `gw` | `greenway` | varies (Y/N flags) | Different field naming |

---

## 4. Silver-Layer Table Registry

> Update when tables are renamed or new ones are added.

| Domain | Silver Table | Primary Key |
|--------|-------------|-------------|
| Patients | `rgd_udm_silver.patients` | `ndid` |
| Encounters | `rgd_udm_silver.encounters` | `ndid`, `eid` |
| Diagnosis | `rgd_udm_silver.diagnosis` | `ndid`, `eid` |
| Procedures | `rgd_udm_silver.procedures` | `ndid`, `eid` |
| Medication | `rgd_udm_silver.medications_part1` / `medications_part2` | `ndid`, `eid` |
| Labs | `rgd_udm_silver.labs` | `ndid`, `eid` |
| Vitals | `rgd_udm_silver.vitals` | `ndid`, `eid` |
| Radiology | `rgd_udm_silver.radiology` | `ndid`, `eid` |
| Allergies | `rgd_udm_silver.allergies` | `ndid`, `eid` |
| Examination | `rgd_udm_silver.examination` | `ndid`, `enc_id` |
| ROS | `rgd_udm_silver.ros` | `ndid`, `eid` |
| Notes | `rgd_udm_silver.note` | `ndid`, `eid` |
| Appointments | `rgd_udm_silver.appointments` | `ndid`, `eid` |

---

## 5. Script Patterns

### 5a. ETL Insert Script (SQL → Python)
Used for: moving data from EHR source tables into silver layer.

- File pair: `<domain>/<ehr>/<name>.sql` + `<name>_opt.py`
- Batch key: sparse PK from a pre-materialized staging table
- One `ThreadPoolExecutor` thread per UNION ALL branch
- Checkpoint table: `staging.etl_checkpoint_<scriptname>_v1`
- InnoDB tuning: `SET unique_checks=0; foreign_key_checks=0;` (session-scoped)

### 5b. Standardisation Script (UPDATE in place)
Used for: normalising column values on existing silver tables (codes, units, etc.).

- File pair: `Standardisations/<domain>/<name>_stand.sql` + `<name>_opt_stand.py`
- Multi-pass: Pass 1 sets std codes via lookup JOIN, Pass 2 handles special cases (e.g. BP split INSERT), Pass 3 sets result/unit std values
- PK staging rebuilt per-pass (each pass filters eligible rows differently)
- Checkpoint per pass, not per batch

### 5c. DQCC Runner
Used for: automated data quality checks on silver tables.

- Rules defined in `dqqc_rgd.dq_rule` (threshold, severity, is_active)
- Results written to `dqqc_rgd.dq_run_result`
- Runner: `DQCC/dqcc_runner.py` — trigger with `python dqcc_runner.py`
- Rule IDs: COMP-001 to COMP-020

---

## 6. Naming Conventions

| Object | Pattern | Example |
|--------|---------|---------|
| Staging CTE table | `staging.<name>_v1` | `staging.diag_cte_v1` |
| Checkpoint table | `staging.etl_checkpoint_<script>_v1` | `staging.etl_checkpoint_vitals_ao_v1` |
| PK staging table | `staging.tmp_<source>_staging` | `staging.tmp_encounters_staging` |
| Std script suffix | `fn<N>` (increment when breaking change) | `fn3` |
| Index | `idx_<column>` | `idx_ndid`, `idx_vital_name` |

---

## 7. Standard Batch Config

```python
BATCH_SIZE  = 50_000
MAX_WORKERS = 6        # one per UNION ALL branch; don't exceed DB max_connections
```

---

## 8. Column Conventions (Silver Layer)

| Column | Meaning |
|--------|---------|
| `ndid` | NeuroDiscovery patient ID (universal join key) |
| `eid` | Encounter ID (EHR-specific) |
| `enc_id` | Alternate encounter ID (some tables, e.g. examination) |
| `psid` | Practice/site ID |
| `udm_inc_id` | Auto-increment row ID used as batch key on production tables |
| `nd_active_flag` | AthenaOne/Plus active record flag (`'Y'` / `'N'`) |
| `activeflag` | eClinicalWorks active flag (`1` / `0`) |
| `incremental_id` | Planned future row ID (not yet populated in all tables) |

---

## 9. Lookup Tables (semantics schema)

| Table | Used for |
|-------|---------|
| `semantics.vitals_loinc` | Vitals name → LOINC code mapping |
| `semantics.icd_lookup` | ICD-9 / ICD-10 mappings |
| `semantics.rxnorm_lookup` | Medication code normalization |

---

## 10. Active Standardisation Scripts

> Update as scripts are run / completed.

| Domain | Script | Status |
|--------|--------|--------|
| Vitals | `Standardisations/vitals/vitals_opt_stand.py` | In progress (ecw) |
| Diagnosis | `Standardisations/Diagnosis/diag_optimise_new.py` | — |
| Labs | `Standardisations/Labs/labs_opt_stand.py` | — |
| Radiology | `Standardisations/Radiology/radiology_f2_opt.py` | — |
| Procedures | `Standardisations/Procedures/procedure_stand_opt.py` | — |
| Allergies | `Standardisations/Allergies/allergies_stand_opt.py` | — |
| Encounters | `Standardisations/Encounters/enc_stand_opt.py` | — |
| Problem List | `Standardisations/Problemlist/problemlist_stang_opt.py` | — |
