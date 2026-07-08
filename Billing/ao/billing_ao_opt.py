#!/usr/bin/env python3
"""
Optimized ETL for: billing_chatbot.billing_details
Source: AthenaOne billing tables

Pre-materialized staging tables (ROW_NUMBER dedup computed ONCE):
  - staging.billing_ao_era_v1_{schema}       (ERADETAIL latest per CHARGEID)
  - staging.billing_ao_vc_v1_{schema}        (VISITCHARGE latest per VISITCHARGEID)
  - staging.billing_ao_cd_v1_{schema}        (CHARGEDETAIL latest per CHARGEID)
  - staging.billing_ao_pc_v1_{schema}        (PROCEDURECODE latest per PROCEDURECODE)
  - staging.billing_ao_cn_claim_v1_{schema}  (CLAIMNOTE latest per CLAIMID)
  - staging.billing_ao_cn_charge_v1_{schema} (CLAIMNOTE latest per CHARGEID)

Optimizations:
- All 5 CTE ROW_NUMBER window functions materialized once (not re-run per batch)
- Batching by actual TRANSACTIONID values (sparse-ID safe)
- Indexes ensured on all join key columns before processing
- Checkpoint/resume — re-run skips completed batches
- Commit after every batch
- InnoDB checks disabled per-session for bulk speed
- tqdm progress bar

Usage:
    python billing_ao_opt.py
"""

import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import pymysql
from tqdm import tqdm
import os
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())

# ── Configuration ─────────────────────────────────────────────────────────────
DB_CONFIG = {
    "host":            os.environ.get("DB_HOST"),
    "port":            3306,
    "user":            os.environ.get("DB_USER"),
    "password":        os.environ.get("DB_PASSWORD"),
    "database":        "tng_athena_one",
    "charset":         "utf8mb4",
    "connect_timeout": 30,
    "read_timeout":    21600,
    "write_timeout":   21600,
}

BATCH_SIZE  = 25_000   # billing rows are wide; smaller batches reduce lock time
MAX_WORKERS = 1        # single source table

# ── Change this to run for a different schema ─────────────────────────────────
SOURCE_SCHEMA = "raleigh"

DEST_TABLE          = "billing_chatbot.billing_details_raleigh"
STAGING_ERA         = f"staging.billing_ao_era_v1_{SOURCE_SCHEMA}"
STAGING_VC          = f"staging.billing_ao_vc_v1_{SOURCE_SCHEMA}"
STAGING_CD          = f"staging.billing_ao_cd_v1_{SOURCE_SCHEMA}"
STAGING_PC          = f"staging.billing_ao_pc_v1_{SOURCE_SCHEMA}"
STAGING_CN_CLAIM    = f"staging.billing_ao_cn_claim_v1_{SOURCE_SCHEMA}"
STAGING_CN_CHARGE   = f"staging.billing_ao_cn_charge_v1_{SOURCE_SCHEMA}"
STAGING_PK          = f"staging.billing_ao_pk_v1_{SOURCE_SCHEMA}"
CHECKPOINT_TABLE    = f"staging.etl_checkpoint_billing_ao_v1_{SOURCE_SCHEMA}"
CHECKPOINT_KEY      = f"billing_ao_{SOURCE_SCHEMA}"
BATCH_KEY           = "TRANSACTIONID"


# ── Helpers ───────────────────────────────────────────────────────────────────

def get_connection():
    return pymysql.connect(**DB_CONFIG)


def _table_exists(cur, full_table_name):
    schema, table = full_table_name.split(".", 1)
    cur.execute(
        "SELECT COUNT(*) FROM information_schema.tables "
        "WHERE table_schema = %s AND table_name = %s",
        (schema, table),
    )
    return cur.fetchone()[0] > 0


def _ensure_index(cur, conn, schema, table, index_name, columns):
    cur.execute(
        "SELECT COUNT(*) FROM information_schema.statistics "
        "WHERE table_schema = %s AND table_name = %s AND index_name = %s",
        (schema, table, index_name),
    )
    if cur.fetchone()[0] > 0:
        print(f"    {table}.{columns[0]} — index exists")
        return
    col_list = ", ".join(columns)
    print(f"    Creating index {index_name} on {schema}.{table}({col_list})...")
    cur.execute(f"CREATE INDEX {index_name} ON `{schema}`.`{table}` ({col_list})")
    conn.commit()
    print(f"    done")


# ── Checkpoint ────────────────────────────────────────────────────────────────

def is_done(conn):
    cur = conn.cursor()
    cur.execute(
        f"SELECT status FROM {CHECKPOINT_TABLE} WHERE source_key = %s",
        (CHECKPOINT_KEY,),
    )
    row = cur.fetchone()
    cur.close()
    return row is not None and row[0] == "done"


def mark(conn, status, rows=0, error=None):
    cur = conn.cursor()
    cur.execute(f"""
        INSERT INTO {CHECKPOINT_TABLE}
            (source_key, status, rows_inserted, started_at, completed_at, error_msg)
        VALUES (%s, %s, %s, NOW(), IF(%s = 'done', NOW(), NULL), %s)
        ON DUPLICATE KEY UPDATE
            status        = VALUES(status),
            rows_inserted = VALUES(rows_inserted),
            completed_at  = IF(VALUES(status) = 'done', NOW(), NULL),
            error_msg     = VALUES(error_msg)
    """, (CHECKPOINT_KEY, status, rows, status, error))
    conn.commit()
    cur.close()


# ── Batch INSERT builder ──────────────────────────────────────────────────────

def build_batch_insert(pk_lo, pk_hi):
    return f"""
INSERT INTO {DEST_TABLE}
    (practice_name, charge_id, claim_id, patient_id, visit_id, visit_bill_id,
     TRANSACTIONTYPE, TRANSACTIONREASON, TRANSACTIONTRANSFERTYPE, TRANSACTIONTRANSFERINTENT,
     TRANSACTIONMETHOD, tr_reversal_flag,
     TRANSACTIONCREATEDDATETIME, POSTDATE, VOIDEDDATE, FIRSTBILLEDDATETIME, LASTBILLEDDATETIME,
     procedure_code, procedure_description, procedure_code_group, fee_schedule_rvu,
     modifiers, place_of_service,
     charge_type, SOFTCODEYN, emergency_flag, EPSDTYN,
     NDC, DRUGNAME, DRUGDOSAGE, DRUGUNITPRICE, DRUGUNITQUALIFIER, LINENOTE, CLAIMCREATEDYN,
     service_date_from, service_date_to, CLAIMSERVICEDATE, CHARGEDATEDATETIME,
     UNITS, charge_units, HCPCSUNITS,
     fee_schedule_amount, transaction_amount, visit_charge_amount,
     WORKRVU, PRACTICEEXPENSERVU, MALPRACTICERVU, TOTALRVU,
     expected_amount, outstanding_amount, posted_payments, posted_transfers, EXPECTEDALLOWEDAMOUNT,
     ERARECORDID, ERABATCHID, era_payer_name, era_transfer_type, era_reversal_flag,
     era_allowable, era_payment, era_copay, era_coinsurance, era_contractual, era_deductible,
     era_withhold, era_patient_transfer,
     era_other_adjustment, era_other_adjustment_reason,
     era_other_transfer, era_other_transfer_reason,
     era_incentive_payment, era_interest,
     era_claim_allowable, era_claim_payment, era_claim_contractual,
     era_claim_coinsurance, era_claim_copay, era_claim_deductible,
     PRIMARYCLAIMSTATUS, SECONDARYCLAIMSTATUS, PATIENTCLAIMSTATUS,
     PRIMARYOUTSTANDING, SECONDARYOUTSTANDING, PATIENTOUTSTANDING,
     PRIMARYCLAIMTYPE, SECONDARYCLAIMTYPE, CLAIMTYPE, CLAIMCREATEDDATETIME,
     claim_last_action, claim_last_action_date, LASTBILLEDDATE1, LASTBILLEDDATEP, claim_facility_name,
     rendering_provider_id, rendering_provider_name, rendering_provider_type,
     rendering_provider_type_name, rendering_provider_type_category,
     rendering_provider_specialty, rendering_provider_npi,
     billing_provider_id, billing_provider_name, billing_provider_type,
     billing_provider_type_name, billing_provider_specialty, billing_provider_npi,
     supervising_provider_id, supervising_provider_name, supervising_provider_npi,
     supervising_provider_specialty, supervising_provider_type,
     primary_ins_id, primary_insurance_name, primary_insurance_package_id,
     primary_package_name, primary_package_type, primary_reporting_category,
     primary_irc_group, primary_global_allowable_category, primary_local_allowable_category,
     primary_ins_sequence, primary_policy_id, primary_group_number, primary_patient_relationship,
     primary_policyholder_name,
     primary_ins_effective_date, primary_ins_expiry_date, primary_ins_cancelled_date,
     primary_eligibility_status, primary_eligibility_message, primary_eligibility_service_date,
     primary_copay_amount, primary_coinsurance_pct,
     secondary_ins_id, secondary_insurance_name, secondary_insurance_package_id,
     secondary_package_name, secondary_package_type, secondary_reporting_category,
     secondary_irc_group, secondary_ins_sequence, secondary_policy_id,
     secondary_group_number, secondary_patient_relationship, secondary_policyholder_name,
     secondary_ins_effective_date, secondary_ins_expiry_date,
     secondary_eligibility_status, secondary_copay_amount, secondary_coinsurance_pct,
     CHARGEDETAILID, chargedetail_quantity, LEVELOFCAREID,
     chargedetail_service_from, chargedetail_service_to,
     claimnote_id, claimnote_createddatetime, claimnote_createdby,
     claimnote_action, claimnote_transfertype, claimnote_claimstatus, claimnote_pendingflag,
     claimnote_patientinsuranceid, claimnote_billingbatchid, claimnote_scrubtype,
     claimnote_scrubaction, claimnote_denialstatusentry, claimnote_transactionposted,
     claimnote_athenakickreasonid, claimnote_kickreasonid, claimnote_kickdate, claimnote_kickdays,
     claimnote_athena_kickcode, claimnote_remarkcode, claimnote_remarkcode_description,
     claimnote_text, claimnote_actionresponse, claimnote_claimreferencenumber,
     claimnote_paymentbatchid, claimnote_nextscrubdatedatetime,
     claimnote_lastupdated, claimnote_lastmodifieddatetime, claimnote_lastmodifiedby,
     charge_claimnote_id, charge_claimnote_createddatetime, charge_claimnote_createdby,
     charge_claimnote_action, charge_claimnote_transfertype, charge_claimnote_claimstatus,
     charge_claimnote_pendingflag, charge_claimnote_patientinsuranceid,
     charge_claimnote_scrubtype, charge_claimnote_scrubaction,
     charge_claimnote_denialstatusentry, charge_claimnote_transactionposted,
     charge_claimnote_remarkcode, charge_claimnote_remarkcode_description,
     charge_claimnote_text, charge_claimnote_outstanding, charge_claimnote_lastupdated)
SELECT
    tr.CONTEXTNAME,
    tr.TRANSACTIONID,
    tr.CLAIMID,
    tr.PATIENTID,
    vc.VISITID,
    vc.VISITBILLID,
    tr.TRANSACTIONTYPE, tr.TRANSACTIONREASON, tr.TRANSACTIONTRANSFERTYPE,
    tr.TRANSACTIONTRANSFERINTENT, tr.TRANSACTIONMETHOD, tr.REVERSALFLAG,
    tr.TRANSACTIONCREATEDDATETIME, tr.POSTDATE, tr.VOIDEDDATE,
    tr.FIRSTBILLEDDATETIME, tr.LASTBILLEDDATETIME,
    COALESCE(vc.PROCEDURECODE,  tr.PROCEDURECODE),
    pc.PROCEDURECODEDESCRIPTION,
    pc.PROCEDURECODEGROUP,
    pc.pc_rvu,
    COALESCE(vc.MODIFIERS,      tr.OTHERMODIFIER),
    COALESCE(vc.PLACEOFSERVICE, tr.PLACEOFSERVICE),
    vc.charge_type, vc.SOFTCODEYN, vc.EMG, vc.EPSDTYN,
    vc.NDC, vc.DRUGNAME, vc.DRUGDOSAGE, vc.DRUGUNITPRICE, vc.DRUGUNITQUALIFIER,
    vc.LINENOTE, vc.CLAIMCREATEDYN,
    COALESCE(DATE(vc.FROMDATEDATETIME), tr.CHARGEFROMDATE),
    COALESCE(DATE(vc.TODATEDATETIME),   tr.CHARGETODATE),
    c.CLAIMSERVICEDATE,
    vc.CHARGEDATEDATETIME,
    tr.UNITS,
    COALESCE(vc.CHARGECODEUNITS, tr.UNITS),
    vc.HCPCSUNITS,
    vc.ORIGINALUNITAMOUNT,
    tr.AMOUNT,
    vc.vc_amount,
    tr.WORKRVU, tr.PRACTICEEXPENSERVU, tr.MALPRACTICERVU, tr.TOTALRVU,
    tr.EXPECTED, tr.OUTSTANDING, tr.PAYMENTS, tr.TRANSFERS, tr.EXPECTEDALLOWEDAMOUNT,
    er.ERARECORDID, er.ERABATCHID, er.PAYERNAME, er.TRANSFERTYPE, er.REVERSALFLAG,
    er.ALLOWABLE, er.PAYMENT, er.COPAY, er.COINSURANCE, er.CONTRACTUAL, er.DEDUCTIBLE,
    er.WITHHOLD, er.PATIENTTRANSFER, er.OTHERADJUSTMENT, er.OTHERADJUSTMENTREASON,
    er.OTHERTRANSFER, er.OTHERTRANSFERREASON, er.INCENTIVEPAYMENT, er.INTEREST,
    er.CLAIMALLOWABLE, er.CLAIMPAYMENT, er.CLAIMCONTRACTUAL,
    er.CLAIMCOINSURANCE, er.CLAIMCOPAY, er.CLAIMDEDUCTIBLE,
    c.PRIMARYCLAIMSTATUS, c.SECONDARYCLAIMSTATUS, c.PATIENTCLAIMSTATUS,
    c.PRIMARYOUTSTANDING, c.SECONDARYOUTSTANDING, c.PATIENTOUTSTANDING,
    c.PRIMARYCLAIMTYPE, c.SECONDARYCLAIMTYPE, c.CLAIMTYPE, c.CLAIMCREATEDDATETIME,
    c.LASTACTION, c.LASTACTIONDATE, c.LASTBILLEDDATE1, c.LASTBILLEDDATEP, c.CONTEXTNAME,
    c.RENDERINGPROVIDERID,
    TRIM(CONCAT_WS(' ', rp.PROVIDERFIRSTNAME, rp.PROVIDERLASTNAME)),
    rp.PROVIDERTYPE, rp.PROVIDERTYPENAME, rp.PROVIDERTYPECATEGORY,
    rp.SPECIALTY, rp.PROVIDERNPINUMBER,
    c.PRIMARYBILLINGPROVIDERID,
    TRIM(CONCAT_WS(' ', bp.PROVIDERFIRSTNAME, bp.PROVIDERLASTNAME)),
    bp.PROVIDERTYPE, bp.PROVIDERTYPENAME, bp.SPECIALTY, bp.PROVIDERNPINUMBER,
    c.SUPERVISINGPROVIDERID,
    TRIM(CONCAT_WS(' ', sp.PROVIDERFIRSTNAME, sp.PROVIDERLASTNAME)),
    sp.PROVIDERNPINUMBER, sp.SPECIALTY, sp.PROVIDERTYPE,
    COALESCE(c.CLAIMPRIMARYPATIENTINSID, vc.PRIMARYPATIENTINSURANCEID),
    pi1.INSURANCENAME, pi1.INSURANCEPACKAGEID,
    py1.INSURANCEPACKAGENAME, py1.INSURANCEPACKAGETYPE, py1.INSURANCEREPORTINGCATEGORY,
    py1.IRCGROUP, py1.GLOBALALLOWABLECATEGORY, py1.LOCALALLOWABLECATEGORY,
    pi1.SEQUENCENUMBER, pi1.POLICYIDNUMBER, pi1.POLICYGROUPNUMBER, pi1.PATIENTRELATIONSHIP,
    TRIM(CONCAT_WS(' ', pi1.FIRSTNAME, pi1.LASTNAME)),
    pi1.ISSUEDATE, pi1.EXPIRATIONDATE, pi1.CANCELLATIONDATE,
    pi1.ELIGIBILITYSTATUS, pi1.ELIGIBILITYMESSAGE, pi1.ELIGIBILITYSERVICEDATE,
    pi1.COPAY, pi1.COINSURANCEPERCENT,
    c.CLAIMSECONDARYPATIENTINSID,
    pi2.INSURANCENAME, pi2.INSURANCEPACKAGEID,
    py2.INSURANCEPACKAGENAME, py2.INSURANCEPACKAGETYPE, py2.INSURANCEREPORTINGCATEGORY,
    py2.IRCGROUP,
    pi2.SEQUENCENUMBER, pi2.POLICYIDNUMBER, pi2.POLICYGROUPNUMBER, pi2.PATIENTRELATIONSHIP,
    TRIM(CONCAT_WS(' ', pi2.FIRSTNAME, pi2.LASTNAME)),
    pi2.ISSUEDATE, pi2.EXPIRATIONDATE,
    pi2.ELIGIBILITYSTATUS, pi2.COPAY, pi2.COINSURANCEPERCENT,
    cd.CHARGEDETAILID, cd.QUANTITY, cd.LEVELOFCAREID, cd.cd_fromdate, cd.cd_todate,
    cn_c.CLAIMNOTEID, cn_c.CREATEDDATETIME, cn_c.CREATEDBY,
    cn_c.ACTION, cn_c.TRANSFERTYPE, cn_c.CLAIMSTATUS, cn_c.PENDINGFLAG,
    cn_c.PATIENTINSURANCEID, cn_c.BILLINGBATCHID, cn_c.SCRUBTYPE, cn_c.SCRUBACTION,
    cn_c.DENIALSTATUSENTRY, cn_c.TRANSACTIONPOSTED,
    cn_c.ATHENAKICKREASONID, cn_c.KICKREASONID, cn_c.KICKDATE, cn_c.KICKDAYS,
    cn_c.ATHENA_KICKCODE, cn_c.REMARKCODE, cn_c.REMARKCODE_DESCRIPTION,
    cn_c.NOTE, cn_c.ACTIONRESPONSE, cn_c.CLAIMREFERENCENUMBER,
    cn_c.PAYMENTBATCHID, cn_c.NEXTSCRUBDATEDATETIME,
    cn_c.LASTUPDATED, cn_c.LASTMODIFIEDDATETIME, cn_c.LASTMODIFIEDBY,
    cn_t.CLAIMNOTEID, cn_t.CREATEDDATETIME, cn_t.CREATEDBY,
    cn_t.ACTION, cn_t.TRANSFERTYPE, cn_t.CLAIMSTATUS, cn_t.PENDINGFLAG,
    cn_t.PATIENTINSURANCEID, cn_t.SCRUBTYPE, cn_t.SCRUBACTION,
    cn_t.DENIALSTATUSENTRY, cn_t.TRANSACTIONPOSTED,
    cn_t.REMARKCODE, cn_t.REMARKCODE_DESCRIPTION,
    cn_t.NOTE, cn_t.OUTSTANDING, cn_t.LASTUPDATED
FROM `{SOURCE_SCHEMA}`.`TRANSACTION` tr
INNER JOIN `{SOURCE_SCHEMA}`.CLAIM c
    ON  c.CLAIMID        = tr.CLAIMID
    AND c.nd_active_flag = 'Y'
LEFT JOIN {STAGING_ERA} er
    ON  er.CHARGEID = tr.TRANSACTIONID
LEFT JOIN {STAGING_CD} cd
    ON  cd.CHARGEID = tr.TRANSACTIONID
LEFT JOIN {STAGING_VC} vc
    ON  vc.VISITCHARGEID = cd.VISITCHARGEID
LEFT JOIN {STAGING_PC} pc
    ON  pc.PROCEDURECODE = COALESCE(vc.PROCEDURECODE, tr.PROCEDURECODE)
LEFT JOIN `{SOURCE_SCHEMA}`.PROVIDER rp
    ON  rp.PROVIDERID     = c.RENDERINGPROVIDERID
    AND rp.nd_active_flag = 'Y'
LEFT JOIN `{SOURCE_SCHEMA}`.PROVIDER bp
    ON  bp.PROVIDERID     = c.PRIMARYBILLINGPROVIDERID
    AND bp.nd_active_flag = 'Y'
LEFT JOIN `{SOURCE_SCHEMA}`.PROVIDER sp
    ON  sp.PROVIDERID     = c.SUPERVISINGPROVIDERID
    AND sp.nd_active_flag = 'Y'
LEFT JOIN `{SOURCE_SCHEMA}`.PATIENTINSURANCE pi1
    ON  pi1.PATIENTINSURANCEID = COALESCE(c.CLAIMPRIMARYPATIENTINSID, vc.PRIMARYPATIENTINSURANCEID)
    AND pi1.nd_active_flag     = 'Y'
LEFT JOIN `{SOURCE_SCHEMA}`.PAYER py1
    ON  py1.INSURANCEPACKAGEID = pi1.INSURANCEPACKAGEID
    AND py1.nd_active_flag     = 'Y'
LEFT JOIN `{SOURCE_SCHEMA}`.PATIENTINSURANCE pi2
    ON  pi2.PATIENTINSURANCEID = c.CLAIMSECONDARYPATIENTINSID
    AND pi2.nd_active_flag     = 'Y'
LEFT JOIN `{SOURCE_SCHEMA}`.PAYER py2
    ON  py2.INSURANCEPACKAGEID = pi2.INSURANCEPACKAGEID
    AND py2.nd_active_flag     = 'Y'
LEFT JOIN {STAGING_CN_CLAIM}  cn_c ON cn_c.CLAIMID   = c.CLAIMID
LEFT JOIN {STAGING_CN_CHARGE} cn_t ON cn_t.CHARGEID  = tr.TRANSACTIONID
WHERE tr.nd_active_flag = 'Y'
  AND tr.{BATCH_KEY} >= {pk_lo}
  AND tr.{BATCH_KEY} <  {pk_hi}
"""


# ── Setup ─────────────────────────────────────────────────────────────────────

def setup_tables():
    conn = get_connection()
    cur  = conn.cursor()

    # ── 1. Source table indexes ───────────────────────────────────────
    print("  Ensuring indexes on source tables...")
    for tbl, idx, cols in [
        ("TRANSACTION",      "idx_claimid",          ["CLAIMID"]),
        ("TRANSACTION",      "idx_active_flag",       ["nd_active_flag"]),
        ("CLAIM",            "idx_claimid",           ["CLAIMID"]),
        ("CLAIM",            "idx_active_flag",       ["nd_active_flag"]),
        ("ERADETAIL",        "idx_chargeid",          ["CHARGEID"]),
        ("ERADETAIL",        "idx_active_flag",       ["nd_active_flag"]),
        ("VISITCHARGE",      "idx_visitchargeid",     ["VISITCHARGEID"]),
        ("VISITCHARGE",      "idx_active_flag",       ["nd_active_flag"]),
        ("CHARGEDETAIL",     "idx_chargeid",          ["CHARGEID"]),
        ("CHARGEDETAIL",     "idx_active_flag",       ["nd_active_flag"]),
        ("PROCEDURECODE",    "idx_procedurecode",     ["PROCEDURECODE"]),
        ("PROCEDURECODE",    "idx_active_flag",       ["nd_active_flag"]),
        ("CLAIMNOTE",        "idx_claimid",           ["CLAIMID"]),
        ("CLAIMNOTE",        "idx_chargeid",          ["CHARGEID"]),
        ("CLAIMNOTE",        "idx_active_flag",       ["nd_active_flag"]),
        ("PROVIDER",         "idx_providerid",        ["PROVIDERID"]),
        ("PATIENTINSURANCE", "idx_patientinsuranceid",["PATIENTINSURANCEID"]),
        ("PAYER",            "idx_insurancepackageid",["INSURANCEPACKAGEID"]),
    ]:
        _ensure_index(cur, conn, SOURCE_SCHEMA, tbl, idx, cols)

    # ── 2. Materialize era_deduped (latest ERADETAIL per CHARGEID) ────
    print(f"  Materializing ERA dedup ({STAGING_ERA})...")
    if not _table_exists(cur, STAGING_ERA):
        cur.execute(f"""
            CREATE TABLE {STAGING_ERA} AS
            SELECT
                ERARECORDID, CHARGEID, CLAIMID, PAYERNAME,
                ALLOWABLE, COPAY, COINSURANCE, CONTRACTUAL,
                CLAIMCONTRACTUAL, CLAIMCOINSURANCE, CLAIMALLOWABLE, CLAIMPAYMENT,
                CLAIMDEDUCTIBLE, CLAIMCOPAY, DEDUCTIBLE, PAYMENT,
                PATIENTTRANSFER, OTHERADJUSTMENT, OTHERADJUSTMENTREASON,
                OTHERTRANSFER, OTHERTRANSFERREASON, WITHHOLD,
                INCENTIVEPAYMENT, INTEREST, TRANSFERTYPE, REVERSALFLAG, ERABATCHID
            FROM (
                SELECT *,
                       ROW_NUMBER() OVER (PARTITION BY CHARGEID ORDER BY ERARECORDID DESC) AS rn
                FROM `{SOURCE_SCHEMA}`.ERADETAIL
                WHERE nd_active_flag = 'Y'
            ) x WHERE rn = 1
        """)
        cur.execute(f"ALTER TABLE {STAGING_ERA} ADD INDEX idx_chargeid (CHARGEID)")
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")
    cur.execute(f"SELECT COUNT(*) FROM {STAGING_ERA}")
    print(f"    {cur.fetchone()[0]:,} ERA rows")

    # ── 3. Materialize vc_deduped (latest VISITCHARGE per VISITCHARGEID) ──
    print(f"  Materializing VisitCharge dedup ({STAGING_VC})...")
    if not _table_exists(cur, STAGING_VC):
        cur.execute(f"""
            CREATE TABLE {STAGING_VC} AS
            SELECT
                VISITCHARGEID, VISITID, VISITBILLID, PROVIDERID,
                PROCEDURECODE, MODIFIERS, PLACEOFSERVICE, REVENUECODE,
                AMOUNT AS vc_amount, ORIGINALUNITAMOUNT, CHARGECODEUNITS, HCPCSUNITS,
                STATUS AS vc_status, CHARGEDEPARTMENTID, SERVICEDEPARTMENTID, VISITDEPARTMENTID,
                SUPERVISINGPROVIDERID, PRIMARYPATIENTINSURANCEID,
                CHARGEDATEDATETIME, FROMDATEDATETIME, TODATEDATETIME, POSTDATEDATETIME,
                CLAIMCREATEDYN, SOURCE AS charge_source, TYPE AS charge_type,
                SOFTCODEYN, EMG, EPSDTYN, FAMILYPLAN,
                NDC, DRUGNAME, DRUGDOSAGE, DRUGUNITPRICE, DRUGUNITQUALIFIER, LINENOTE
            FROM (
                SELECT *,
                       ROW_NUMBER() OVER (PARTITION BY VISITCHARGEID ORDER BY LASTUPDATED DESC) AS rn
                FROM `{SOURCE_SCHEMA}`.VISITCHARGE
                WHERE nd_active_flag = 'Y'
            ) x WHERE rn = 1
        """)
        cur.execute(f"ALTER TABLE {STAGING_VC} ADD INDEX idx_visitchargeid (VISITCHARGEID)")
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")
    cur.execute(f"SELECT COUNT(*) FROM {STAGING_VC}")
    print(f"    {cur.fetchone()[0]:,} VisitCharge rows")

    # ── 4. Materialize cd_deduped (latest CHARGEDETAIL per CHARGEID) ──
    print(f"  Materializing ChargeDetail dedup ({STAGING_CD})...")
    if not _table_exists(cur, STAGING_CD):
        cur.execute(f"""
            CREATE TABLE {STAGING_CD} AS
            SELECT
                CHARGEDETAILID, CHARGEID, VISITCHARGEID,
                AMOUNT AS cd_amount, DESCRIPTION AS cd_description, QUANTITY,
                PROCEDURECODE AS cd_procedurecode,
                RENDERINGPROVIDERID AS cd_renderingproviderid, RENDERINGDEPARTMENTID,
                LEVELOFCAREID,
                FROMDATEDATETIME AS cd_fromdate, TODATEDATETIME AS cd_todate
            FROM (
                SELECT *,
                       ROW_NUMBER() OVER (PARTITION BY CHARGEID ORDER BY CHARGEDETAILID DESC) AS rn
                FROM `{SOURCE_SCHEMA}`.CHARGEDETAIL
                WHERE nd_active_flag = 'Y'
            ) x WHERE rn = 1
        """)
        cur.execute(f"ALTER TABLE {STAGING_CD} ADD INDEX idx_chargeid (CHARGEID)")
        cur.execute(f"ALTER TABLE {STAGING_CD} ADD INDEX idx_visitchargeid (VISITCHARGEID)")
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")
    cur.execute(f"SELECT COUNT(*) FROM {STAGING_CD}")
    print(f"    {cur.fetchone()[0]:,} ChargeDetail rows")

    # ── 5. Materialize pc_deduped (latest PROCEDURECODE per code) ─────
    print(f"  Materializing ProcedureCode dedup ({STAGING_PC})...")
    if not _table_exists(cur, STAGING_PC):
        cur.execute(f"""
            CREATE TABLE {STAGING_PC} AS
            SELECT
                PROCEDURECODE, PROCEDURECODEDESCRIPTION, PROCEDURECODEGROUP,
                PROCEDURECODEGROUPID, RVU AS pc_rvu, EFFECTIVEDATE, EXPIRATIONDATE, UDSYN
            FROM (
                SELECT *,
                       ROW_NUMBER() OVER (PARTITION BY PROCEDURECODE ORDER BY EFFECTIVEDATE DESC) AS rn
                FROM `{SOURCE_SCHEMA}`.`PROCEDURECODE`
                WHERE nd_active_flag  = 'Y'
                  AND DELETEDDATETIME IS NULL
            ) x WHERE rn = 1
        """)
        cur.execute(f"ALTER TABLE {STAGING_PC} ADD INDEX idx_proccode (PROCEDURECODE(50))")
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")
    cur.execute(f"SELECT COUNT(*) FROM {STAGING_PC}")
    print(f"    {cur.fetchone()[0]:,} ProcedureCode rows")

    # ── 6. Materialize cn_claim (latest CLAIMNOTE per CLAIMID) ────────
    print(f"  Materializing ClaimNote (claim-level) dedup ({STAGING_CN_CLAIM})...")
    if not _table_exists(cur, STAGING_CN_CLAIM):
        cur.execute(f"""
            CREATE TABLE {STAGING_CN_CLAIM} AS
            SELECT
                CLAIMNOTEID, CLAIMID, CHARGEID, CREATEDDATETIME, CREATEDBY, ACTION,
                TRANSFERTYPE, CLAIMSTATUS, PENDINGFLAG, PATIENTINSURANCEID,
                BILLINGPROVIDERID, BILLINGINSURANCEPACKAGEID, BILLINGBATCHID,
                SCRUBTYPE, RULECLASS, CLAIMRULEID, CLAIMRULENAME, SCRUBACTION,
                FRONTENDACTION, BACKENDACTION, SCRUBFIXTEXT, FIXTEXT,
                DENIALSTATUSENTRY, TRANSACTIONPOSTED, ATHENAKICKREASONID, KICKREASONID,
                KICKDATE, KICKDAYS, ATHENA_KICKCODE, REMARKCODE, REMARKCODE_DESCRIPTION,
                NOTE, ACTIONRESPONSE, CONTACTNAME, CONTACTNUMBER, CALLREFERENCENUMBER,
                CLAIMREFERENCENUMBER, PAYMENTMETHOD, PAYMENTAMOUNT, PAYMENTDATEDATETIME,
                CHECKAMOUNT, CHECKIDENTIFIER, CASHEDDATEDATETIME, PAYMENTBATCHID,
                APPEALLETTERID, REQUIRESMANUALCLOSEYN, OVERRIDEABLEYN, OUTSTANDING,
                NEXTSCRUBDATEDATETIME, LASTUPDATED, LASTMODIFIEDDATETIME, LASTMODIFIEDBY
            FROM (
                SELECT *,
                       ROW_NUMBER() OVER (PARTITION BY CLAIMID ORDER BY CLAIMNOTEID DESC) AS rn
                FROM `{SOURCE_SCHEMA}`.CLAIMNOTE
                WHERE nd_active_flag  = 'Y'
                  AND DELETEDDATETIME IS NULL
            ) x WHERE rn = 1
        """)
        cur.execute(f"ALTER TABLE {STAGING_CN_CLAIM} ADD INDEX idx_claimid (CLAIMID)")
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")
    cur.execute(f"SELECT COUNT(*) FROM {STAGING_CN_CLAIM}")
    print(f"    {cur.fetchone()[0]:,} ClaimNote (claim-level) rows")

    # ── 7. Materialize cn_charge (latest CLAIMNOTE per CHARGEID) ──────
    print(f"  Materializing ClaimNote (charge-level) dedup ({STAGING_CN_CHARGE})...")
    if not _table_exists(cur, STAGING_CN_CHARGE):
        cur.execute(f"""
            CREATE TABLE {STAGING_CN_CHARGE} AS
            SELECT
                CLAIMNOTEID, CLAIMID, CHARGEID, CREATEDDATETIME, CREATEDBY, ACTION,
                TRANSFERTYPE, CLAIMSTATUS, PENDINGFLAG, PATIENTINSURANCEID,
                SCRUBTYPE, SCRUBACTION, DENIALSTATUSENTRY, TRANSACTIONPOSTED,
                REMARKCODE, REMARKCODE_DESCRIPTION, NOTE, OUTSTANDING, LASTUPDATED
            FROM (
                SELECT *,
                       ROW_NUMBER() OVER (PARTITION BY CHARGEID ORDER BY CLAIMNOTEID DESC) AS rn
                FROM `{SOURCE_SCHEMA}`.CLAIMNOTE
                WHERE nd_active_flag  = 'Y'
                  AND DELETEDDATETIME IS NULL
            ) x WHERE rn = 1
        """)
        cur.execute(f"ALTER TABLE {STAGING_CN_CHARGE} ADD INDEX idx_chargeid (CHARGEID)")
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")
    cur.execute(f"SELECT COUNT(*) FROM {STAGING_CN_CHARGE}")
    print(f"    {cur.fetchone()[0]:,} ClaimNote (charge-level) rows")

    # ── 8. Destination table ──────────────────────────────────────────
    print("  Creating destination table...")
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {DEST_TABLE} (
            practice_name                       VARCHAR(255)  DEFAULT NULL,
            charge_id                           BIGINT        DEFAULT NULL,
            claim_id                            BIGINT        DEFAULT NULL,
            patient_id                          BIGINT        DEFAULT NULL,
            visit_id                            BIGINT        DEFAULT NULL,
            visit_bill_id                       BIGINT        DEFAULT NULL,
            TRANSACTIONTYPE                     VARCHAR(100)  DEFAULT NULL,
            TRANSACTIONREASON                   VARCHAR(255)  DEFAULT NULL,
            TRANSACTIONTRANSFERTYPE             VARCHAR(100)  DEFAULT NULL,
            TRANSACTIONTRANSFERINTENT           VARCHAR(100)  DEFAULT NULL,
            TRANSACTIONMETHOD                   VARCHAR(100)  DEFAULT NULL,
            tr_reversal_flag                    VARCHAR(5)    DEFAULT NULL,
            TRANSACTIONCREATEDDATETIME          DATETIME      DEFAULT NULL,
            POSTDATE                            DATE          DEFAULT NULL,
            VOIDEDDATE                          DATE          DEFAULT NULL,
            FIRSTBILLEDDATETIME                 DATETIME      DEFAULT NULL,
            LASTBILLEDDATETIME                  DATETIME      DEFAULT NULL,
            procedure_code                      VARCHAR(50)   DEFAULT NULL,
            procedure_description               VARCHAR(500)  DEFAULT NULL,
            procedure_code_group                VARCHAR(255)  DEFAULT NULL,
            fee_schedule_rvu                    DECIMAL(15,4) DEFAULT NULL,
            modifiers                           VARCHAR(100)  DEFAULT NULL,
            place_of_service                    VARCHAR(50)   DEFAULT NULL,
            charge_type                         VARCHAR(100)  DEFAULT NULL,
            SOFTCODEYN                          VARCHAR(5)    DEFAULT NULL,
            emergency_flag                      VARCHAR(5)    DEFAULT NULL,
            EPSDTYN                             VARCHAR(5)    DEFAULT NULL,
            NDC                                 VARCHAR(50)   DEFAULT NULL,
            DRUGNAME                            VARCHAR(255)  DEFAULT NULL,
            DRUGDOSAGE                          VARCHAR(100)  DEFAULT NULL,
            DRUGUNITPRICE                       DECIMAL(15,4) DEFAULT NULL,
            DRUGUNITQUALIFIER                   VARCHAR(50)   DEFAULT NULL,
            LINENOTE                            TEXT,
            CLAIMCREATEDYN                      VARCHAR(5)    DEFAULT NULL,
            service_date_from                   DATE          DEFAULT NULL,
            service_date_to                     DATE          DEFAULT NULL,
            CLAIMSERVICEDATE                    DATE          DEFAULT NULL,
            CHARGEDATEDATETIME                  DATETIME      DEFAULT NULL,
            UNITS                               DECIMAL(15,4) DEFAULT NULL,
            charge_units                        DECIMAL(15,4) DEFAULT NULL,
            HCPCSUNITS                          DECIMAL(15,4) DEFAULT NULL,
            fee_schedule_amount                 DECIMAL(15,4) DEFAULT NULL,
            transaction_amount                  DECIMAL(15,4) DEFAULT NULL,
            visit_charge_amount                 DECIMAL(15,4) DEFAULT NULL,
            WORKRVU                             DECIMAL(15,4) DEFAULT NULL,
            PRACTICEEXPENSERVU                  DECIMAL(15,4) DEFAULT NULL,
            MALPRACTICERVU                      DECIMAL(15,4) DEFAULT NULL,
            TOTALRVU                            DECIMAL(15,4) DEFAULT NULL,
            expected_amount                     DECIMAL(15,4) DEFAULT NULL,
            outstanding_amount                  DECIMAL(15,4) DEFAULT NULL,
            posted_payments                     DECIMAL(15,4) DEFAULT NULL,
            posted_transfers                    DECIMAL(15,4) DEFAULT NULL,
            EXPECTEDALLOWEDAMOUNT               DECIMAL(15,4) DEFAULT NULL,
            ERARECORDID                         BIGINT        DEFAULT NULL,
            ERABATCHID                          BIGINT        DEFAULT NULL,
            era_payer_name                      VARCHAR(255)  DEFAULT NULL,
            era_transfer_type                   VARCHAR(100)  DEFAULT NULL,
            era_reversal_flag                   VARCHAR(5)    DEFAULT NULL,
            era_allowable                       DECIMAL(15,4) DEFAULT NULL,
            era_payment                         DECIMAL(15,4) DEFAULT NULL,
            era_copay                           DECIMAL(15,4) DEFAULT NULL,
            era_coinsurance                     DECIMAL(15,4) DEFAULT NULL,
            era_contractual                     DECIMAL(15,4) DEFAULT NULL,
            era_deductible                      DECIMAL(15,4) DEFAULT NULL,
            era_withhold                        DECIMAL(15,4) DEFAULT NULL,
            era_patient_transfer                DECIMAL(15,4) DEFAULT NULL,
            era_other_adjustment                DECIMAL(15,4) DEFAULT NULL,
            era_other_adjustment_reason         VARCHAR(255)  DEFAULT NULL,
            era_other_transfer                  DECIMAL(15,4) DEFAULT NULL,
            era_other_transfer_reason           VARCHAR(255)  DEFAULT NULL,
            era_incentive_payment               DECIMAL(15,4) DEFAULT NULL,
            era_interest                        DECIMAL(15,4) DEFAULT NULL,
            era_claim_allowable                 DECIMAL(15,4) DEFAULT NULL,
            era_claim_payment                   DECIMAL(15,4) DEFAULT NULL,
            era_claim_contractual               DECIMAL(15,4) DEFAULT NULL,
            era_claim_coinsurance               DECIMAL(15,4) DEFAULT NULL,
            era_claim_copay                     DECIMAL(15,4) DEFAULT NULL,
            era_claim_deductible                DECIMAL(15,4) DEFAULT NULL,
            PRIMARYCLAIMSTATUS                  VARCHAR(100)  DEFAULT NULL,
            SECONDARYCLAIMSTATUS                VARCHAR(100)  DEFAULT NULL,
            PATIENTCLAIMSTATUS                  VARCHAR(100)  DEFAULT NULL,
            PRIMARYOUTSTANDING                  DECIMAL(15,4) DEFAULT NULL,
            SECONDARYOUTSTANDING                DECIMAL(15,4) DEFAULT NULL,
            PATIENTOUTSTANDING                  DECIMAL(15,4) DEFAULT NULL,
            PRIMARYCLAIMTYPE                    VARCHAR(100)  DEFAULT NULL,
            SECONDARYCLAIMTYPE                  VARCHAR(100)  DEFAULT NULL,
            CLAIMTYPE                           VARCHAR(100)  DEFAULT NULL,
            CLAIMCREATEDDATETIME                DATETIME      DEFAULT NULL,
            claim_last_action                   VARCHAR(255)  DEFAULT NULL,
            claim_last_action_date              DATE          DEFAULT NULL,
            LASTBILLEDDATE1                     DATE          DEFAULT NULL,
            LASTBILLEDDATEP                     DATE          DEFAULT NULL,
            claim_facility_name                 VARCHAR(255)  DEFAULT NULL,
            rendering_provider_id               BIGINT        DEFAULT NULL,
            rendering_provider_name             VARCHAR(255)  DEFAULT NULL,
            rendering_provider_type             VARCHAR(100)  DEFAULT NULL,
            rendering_provider_type_name        VARCHAR(255)  DEFAULT NULL,
            rendering_provider_type_category    VARCHAR(100)  DEFAULT NULL,
            rendering_provider_specialty        VARCHAR(255)  DEFAULT NULL,
            rendering_provider_npi              VARCHAR(20)   DEFAULT NULL,
            billing_provider_id                 BIGINT        DEFAULT NULL,
            billing_provider_name               VARCHAR(255)  DEFAULT NULL,
            billing_provider_type               VARCHAR(100)  DEFAULT NULL,
            billing_provider_type_name          VARCHAR(255)  DEFAULT NULL,
            billing_provider_specialty          VARCHAR(255)  DEFAULT NULL,
            billing_provider_npi                VARCHAR(20)   DEFAULT NULL,
            supervising_provider_id             BIGINT        DEFAULT NULL,
            supervising_provider_name           VARCHAR(255)  DEFAULT NULL,
            supervising_provider_npi            VARCHAR(20)   DEFAULT NULL,
            supervising_provider_specialty      VARCHAR(255)  DEFAULT NULL,
            supervising_provider_type           VARCHAR(100)  DEFAULT NULL,
            primary_ins_id                      BIGINT        DEFAULT NULL,
            primary_insurance_name              VARCHAR(255)  DEFAULT NULL,
            primary_insurance_package_id        BIGINT        DEFAULT NULL,
            primary_package_name                VARCHAR(255)  DEFAULT NULL,
            primary_package_type                VARCHAR(100)  DEFAULT NULL,
            primary_reporting_category          VARCHAR(100)  DEFAULT NULL,
            primary_irc_group                   VARCHAR(100)  DEFAULT NULL,
            primary_global_allowable_category   VARCHAR(100)  DEFAULT NULL,
            primary_local_allowable_category    VARCHAR(100)  DEFAULT NULL,
            primary_ins_sequence                INT           DEFAULT NULL,
            primary_policy_id                   VARCHAR(100)  DEFAULT NULL,
            primary_group_number                VARCHAR(100)  DEFAULT NULL,
            primary_patient_relationship        VARCHAR(100)  DEFAULT NULL,
            primary_policyholder_name           VARCHAR(255)  DEFAULT NULL,
            primary_ins_effective_date          DATE          DEFAULT NULL,
            primary_ins_expiry_date             DATE          DEFAULT NULL,
            primary_ins_cancelled_date          DATE          DEFAULT NULL,
            primary_eligibility_status          VARCHAR(100)  DEFAULT NULL,
            primary_eligibility_message         TEXT,
            primary_eligibility_service_date    DATE          DEFAULT NULL,
            primary_copay_amount                DECIMAL(15,4) DEFAULT NULL,
            primary_coinsurance_pct             DECIMAL(10,4) DEFAULT NULL,
            secondary_ins_id                    BIGINT        DEFAULT NULL,
            secondary_insurance_name            VARCHAR(255)  DEFAULT NULL,
            secondary_insurance_package_id      BIGINT        DEFAULT NULL,
            secondary_package_name              VARCHAR(255)  DEFAULT NULL,
            secondary_package_type              VARCHAR(100)  DEFAULT NULL,
            secondary_reporting_category        VARCHAR(100)  DEFAULT NULL,
            secondary_irc_group                 VARCHAR(100)  DEFAULT NULL,
            secondary_ins_sequence              INT           DEFAULT NULL,
            secondary_policy_id                 VARCHAR(100)  DEFAULT NULL,
            secondary_group_number              VARCHAR(100)  DEFAULT NULL,
            secondary_patient_relationship      VARCHAR(100)  DEFAULT NULL,
            secondary_policyholder_name         VARCHAR(255)  DEFAULT NULL,
            secondary_ins_effective_date        DATE          DEFAULT NULL,
            secondary_ins_expiry_date           DATE          DEFAULT NULL,
            secondary_eligibility_status        VARCHAR(100)  DEFAULT NULL,
            secondary_copay_amount              DECIMAL(15,4) DEFAULT NULL,
            secondary_coinsurance_pct           DECIMAL(10,4) DEFAULT NULL,
            CHARGEDETAILID                      BIGINT        DEFAULT NULL,
            chargedetail_quantity               DECIMAL(15,4) DEFAULT NULL,
            LEVELOFCAREID                       BIGINT        DEFAULT NULL,
            chargedetail_service_from           DATETIME      DEFAULT NULL,
            chargedetail_service_to             DATETIME      DEFAULT NULL,
            claimnote_id                        BIGINT        DEFAULT NULL,
            claimnote_createddatetime           DATETIME      DEFAULT NULL,
            claimnote_createdby                 VARCHAR(100)  DEFAULT NULL,
            claimnote_action                    VARCHAR(100)  DEFAULT NULL,
            claimnote_transfertype              VARCHAR(100)  DEFAULT NULL,
            claimnote_claimstatus               VARCHAR(100)  DEFAULT NULL,
            claimnote_pendingflag               VARCHAR(5)    DEFAULT NULL,
            claimnote_patientinsuranceid        BIGINT        DEFAULT NULL,
            claimnote_billingbatchid            BIGINT        DEFAULT NULL,
            claimnote_scrubtype                 VARCHAR(100)  DEFAULT NULL,
            claimnote_scrubaction               VARCHAR(100)  DEFAULT NULL,
            claimnote_denialstatusentry         VARCHAR(255)  DEFAULT NULL,
            claimnote_transactionposted         VARCHAR(5)    DEFAULT NULL,
            claimnote_athenakickreasonid        BIGINT        DEFAULT NULL,
            claimnote_kickreasonid              BIGINT        DEFAULT NULL,
            claimnote_kickdate                  DATE          DEFAULT NULL,
            claimnote_kickdays                  INT           DEFAULT NULL,
            claimnote_athena_kickcode           VARCHAR(50)   DEFAULT NULL,
            claimnote_remarkcode                VARCHAR(50)   DEFAULT NULL,
            claimnote_remarkcode_description    VARCHAR(500)  DEFAULT NULL,
            claimnote_text                      TEXT,
            claimnote_actionresponse            TEXT,
            claimnote_claimreferencenumber      VARCHAR(100)  DEFAULT NULL,
            claimnote_paymentbatchid            BIGINT        DEFAULT NULL,
            claimnote_nextscrubdatedatetime     DATETIME      DEFAULT NULL,
            claimnote_lastupdated               DATETIME      DEFAULT NULL,
            claimnote_lastmodifieddatetime      DATETIME      DEFAULT NULL,
            claimnote_lastmodifiedby            VARCHAR(100)  DEFAULT NULL,
            charge_claimnote_id                 BIGINT        DEFAULT NULL,
            charge_claimnote_createddatetime    DATETIME      DEFAULT NULL,
            charge_claimnote_createdby          VARCHAR(100)  DEFAULT NULL,
            charge_claimnote_action             VARCHAR(100)  DEFAULT NULL,
            charge_claimnote_transfertype       VARCHAR(100)  DEFAULT NULL,
            charge_claimnote_claimstatus        VARCHAR(100)  DEFAULT NULL,
            charge_claimnote_pendingflag        VARCHAR(5)    DEFAULT NULL,
            charge_claimnote_patientinsuranceid BIGINT        DEFAULT NULL,
            charge_claimnote_scrubtype          VARCHAR(100)  DEFAULT NULL,
            charge_claimnote_scrubaction        VARCHAR(100)  DEFAULT NULL,
            charge_claimnote_denialstatusentry  VARCHAR(255)  DEFAULT NULL,
            charge_claimnote_transactionposted  VARCHAR(5)    DEFAULT NULL,
            charge_claimnote_remarkcode         VARCHAR(50)   DEFAULT NULL,
            charge_claimnote_remarkcode_description VARCHAR(500) DEFAULT NULL,
            charge_claimnote_text               TEXT,
            charge_claimnote_outstanding        DECIMAL(15,4) DEFAULT NULL,
            charge_claimnote_lastupdated        DATETIME      DEFAULT NULL
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
    """)
    conn.commit()
    print("    ready")

    # ── 9. Checkpoint table ───────────────────────────────────────────
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {CHECKPOINT_TABLE} (
            source_key    VARCHAR(150) NOT NULL PRIMARY KEY,
            status        ENUM('running','done','failed') NOT NULL DEFAULT 'running',
            rows_inserted BIGINT      DEFAULT 0,
            started_at    DATETIME    DEFAULT NULL,
            completed_at  DATETIME    DEFAULT NULL,
            error_msg     TEXT        DEFAULT NULL
        )
    """)
    conn.commit()

    # ── 10. PK staging ────────────────────────────────────────────────
    print(f"  Building PK staging ({STAGING_PK})...")
    if not _table_exists(cur, STAGING_PK):
        cur.execute(f"""
            CREATE TABLE {STAGING_PK} AS
            SELECT {BATCH_KEY}
            FROM `{SOURCE_SCHEMA}`.`TRANSACTION`
            WHERE {BATCH_KEY} IS NOT NULL
              AND nd_active_flag = 'Y'
        """)
        cur.execute(f"ALTER TABLE {STAGING_PK} ADD INDEX idx_pk ({BATCH_KEY})")
        conn.commit()
        print("    created")
    else:
        print("    already exists, reusing")

    cur.execute(f"SELECT COUNT(*) FROM {STAGING_PK}")
    total = cur.fetchone()[0]

    if total == 0:
        cur.close()
        conn.close()
        return []

    cur.execute(f"""
        SELECT {BATCH_KEY}
        FROM (
            SELECT {BATCH_KEY},
                   ROW_NUMBER() OVER (ORDER BY {BATCH_KEY}) AS rn
            FROM {STAGING_PK}
        ) t
        WHERE (rn - 1) % {BATCH_SIZE} = 0
        ORDER BY {BATCH_KEY}
    """)
    boundaries = [row[0] for row in cur.fetchall() if row[0] is not None]
    cur.execute(f"SELECT MAX({BATCH_KEY}) FROM {STAGING_PK}")
    max_pk = int(cur.fetchone()[0])

    ranges = []
    for i, lo in enumerate(boundaries):
        hi = boundaries[i + 1] if i + 1 < len(boundaries) else max_pk + 1
        ranges.append((lo, hi))

    print(f"    {total:,} transactions → {len(ranges)} batches of ~{BATCH_SIZE:,}")

    cur.close()
    conn.close()
    return ranges


# ── Runner ────────────────────────────────────────────────────────────────────

def run_insert(ranges, pbar):
    conn = get_connection()

    if is_done(conn):
        conn.close()
        pbar.update(len(ranges))
        return {"status": "skipped", "rows": 0, "secs": 0}

    mark(conn, "running")
    t0         = time.time()
    total_rows = 0

    try:
        cur = conn.cursor()
        cur.execute("SET SESSION innodb_lock_wait_timeout = 3600")
        cur.execute("SET unique_checks = 0")
        cur.execute("SET foreign_key_checks = 0")

        for pk_lo, pk_hi in ranges:
            cur.execute(build_batch_insert(pk_lo, pk_hi))
            conn.commit()
            total_rows += cur.rowcount
            pbar.update(1)

        cur.execute("SET unique_checks = 1")
        cur.execute("SET foreign_key_checks = 1")
        cur.close()

        elapsed = round(time.time() - t0, 1)
        mark(conn, "done", total_rows)
        conn.close()
        return {"status": "done", "rows": total_rows, "secs": elapsed}

    except Exception as exc:
        elapsed = round(time.time() - t0, 1)
        mark(conn, "failed", total_rows, str(exc))
        try:
            conn.close()
        except Exception:
            pass
        return {"status": f"FAILED: {exc}", "rows": total_rows, "secs": elapsed}


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print(f"\n{'='*70}", flush=True)
    print(f"  AthenaOne Billing ETL — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  source     : {SOURCE_SCHEMA}.TRANSACTION")
    print(f"  dest       : {DEST_TABLE}")
    print(f"  checkpoint : {CHECKPOINT_TABLE}")
    print(f"  batch_size : {BATCH_SIZE:,}  |  workers : {MAX_WORKERS}")
    print(f"{'='*70}\n", flush=True)

    print("Setup:")
    sys.stdout.flush()
    ranges = setup_tables()
    print()

    if not ranges:
        print("No rows to process. Exiting.")
        return

    result = None
    with tqdm(total=len(ranges), desc="billing_ao", unit="batch") as pbar:
        result = run_insert(ranges, pbar)

    print()
    tag = "DONE" if result["status"] == "done" \
          else "SKIP" if result["status"] == "skipped" \
          else "FAIL"
    print(f"\n{'='*70}")
    print(f"  [{tag}]  {result['rows']:>12,} rows  ({result['secs']}s)")
    if result["status"].startswith("FAILED"):
        print(f"  ERROR: {result['status']}")
    print(f"{'='*70}")

    print(f"\n  Cleanup SQL (run after verifying data):")
    print(f"    DROP TABLE IF EXISTS {STAGING_ERA};")
    print(f"    DROP TABLE IF EXISTS {STAGING_VC};")
    print(f"    DROP TABLE IF EXISTS {STAGING_CD};")
    print(f"    DROP TABLE IF EXISTS {STAGING_PC};")
    print(f"    DROP TABLE IF EXISTS {STAGING_CN_CLAIM};")
    print(f"    DROP TABLE IF EXISTS {STAGING_CN_CHARGE};")
    print(f"    DROP TABLE IF EXISTS {STAGING_PK};")
    print(f"    DROP TABLE IF EXISTS {CHECKPOINT_TABLE};")

    if result["status"].startswith("FAILED"):
        sys.exit(1)


if __name__ == "__main__":
    main()
