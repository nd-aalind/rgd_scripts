create table billing_chatbot.billing_details as 
WITH
era_deduped AS (
    SELECT
        ERARECORDID,
        CHARGEID,
        CLAIMID,
        PAYERNAME,
        ALLOWABLE,
        COPAY,
        COINSURANCE,
        CONTRACTUAL,
        CLAIMCONTRACTUAL,
        CLAIMCOINSURANCE,
        CLAIMALLOWABLE,
        CLAIMPAYMENT,
        CLAIMDEDUCTIBLE,
        CLAIMCOPAY,
        DEDUCTIBLE,
        PAYMENT,
        PATIENTTRANSFER,
        OTHERADJUSTMENT,
        OTHERADJUSTMENTREASON,
        OTHERTRANSFER,
        OTHERTRANSFERREASON,
        WITHHOLD,
        INCENTIVEPAYMENT,
        INTEREST,
        TRANSFERTYPE,
        REVERSALFLAG,
        ERABATCHID,
        ROW_NUMBER() OVER (
            PARTITION BY CHARGEID
            ORDER BY ERARECORDID DESC
        ) AS rn
    FROM ERADETAIL
    WHERE nd_active_flag = 'Y'
),
vc_deduped AS (
    SELECT
        VISITCHARGEID,
        VISITID,
        VISITBILLID,
        PROVIDERID,
        PROCEDURECODE,
        MODIFIERS,
        PLACEOFSERVICE,
        REVENUECODE,
        AMOUNT              AS vc_amount,
        ORIGINALUNITAMOUNT,
        CHARGECODEUNITS,
        HCPCSUNITS,
        STATUS              AS vc_status,
        CHARGEDEPARTMENTID,
        SERVICEDEPARTMENTID,
        VISITDEPARTMENTID,
        SUPERVISINGPROVIDERID,
        PRIMARYPATIENTINSURANCEID,
        CHARGEDATEDATETIME,
        FROMDATEDATETIME,
        TODATEDATETIME,
        POSTDATEDATETIME,
        CLAIMCREATEDYN,
        SOURCE              AS charge_source,
        TYPE                AS charge_type,
        SOFTCODEYN,
        EMG,
        EPSDTYN,
        FAMILYPLAN,
        NDC,
        DRUGNAME,
        DRUGDOSAGE,
        DRUGUNITPRICE,
        DRUGUNITQUALIFIER,
        LINENOTE,
        ROW_NUMBER() OVER (
            PARTITION BY VISITCHARGEID
            ORDER BY LASTUPDATED DESC
        ) AS rn
    FROM VISITCHARGE
    WHERE nd_active_flag = 'Y'
),
cd_deduped AS (
    SELECT
        CHARGEDETAILID,
        CHARGEID,
        VISITCHARGEID,
        AMOUNT              AS cd_amount,
        DESCRIPTION         AS cd_description,
        QUANTITY,
        PROCEDURECODE       AS cd_procedurecode,
        RENDERINGPROVIDERID AS cd_renderingproviderid,
        RENDERINGDEPARTMENTID,
        LEVELOFCAREID,
        FROMDATEDATETIME    AS cd_fromdate,
        TODATEDATETIME      AS cd_todate,
        ROW_NUMBER() OVER (
            PARTITION BY CHARGEID
            ORDER BY CHARGEDETAILID DESC
        ) AS rn
    FROM CHARGEDETAIL
    WHERE nd_active_flag = 'Y'
),
pc_deduped AS (
    SELECT
        PROCEDURECODE,
        PROCEDURECODEDESCRIPTION,
        PROCEDURECODEGROUP,
        PROCEDURECODEGROUPID,
        RVU                 AS pc_rvu,
        EFFECTIVEDATE,
        EXPIRATIONDATE,
        UDSYN,
        ROW_NUMBER() OVER (
            PARTITION BY PROCEDURECODE
            ORDER BY EFFECTIVEDATE DESC
        ) AS rn
    FROM `PROCEDURECODE`                          -- reserved-word table name escaped
    WHERE nd_active_flag  = 'Y'
      AND DELETEDDATETIME IS NULL
),
cn_deduped AS (
    SELECT
        CLAIMNOTEID,
        CLAIMID,
        CHARGEID,
        CREATEDDATETIME,
        CREATEDBY,
        ACTION,
        TRANSFERTYPE,
        CLAIMSTATUS,
        PENDINGFLAG,
        PATIENTINSURANCEID,
        BILLINGPROVIDERID,
        BILLINGINSURANCEPACKAGEID,
        BILLINGBATCHID,
        SCRUBTYPE,
        RULECLASS,
        CLAIMRULEID,
        CLAIMRULENAME,
        LOCALCLAIMRULEID,
        NETWORKCLAIMRULEID,
        CUSTOMRULEID,
        SCRUBACTION,
        FRONTENDACTION,
        BACKENDACTION,
        SCRUBFIXTEXT,
        FIXTEXT,
        DENIALSTATUSENTRY,
        TRANSACTIONPOSTED,
        ATHENAKICKREASONID,
        KICKREASONID,
        KICKDATE,
        KICKDAYS,
        ATHENA_KICKCODE,
        REMARKCODE,
        REMARKCODE_DESCRIPTION,
        NOTE,
        ACTIONRESPONSE,
        CONTACTNAME,
        CONTACTNUMBER,
        CALLREFERENCENUMBER,
        CLAIMREFERENCENUMBER,
        PAYMENTMETHOD,
        PAYMENTAMOUNT,
        PAYMENTDATEDATETIME,
        PAYMENTAMOUNTNAFLAG,
        PAYMENTDATENAFLAG,
        CHECKCCNUMBER,
        CHECKAMOUNT,
        CHECKIDENTIFIER,
        CASHEDDATEDATETIME,
        PAYMENTBATCHID,
        APPEALLETTERID,
        CUSTOMAPPEALLETTERID,
        REQUIRESMANUALCLOSEYN,
        RESOLUTIONTEMPLATEYN,
        OVERRIDEABLEYN,
        OUTSTANDING,
        POSTBILLKICKREASONID,
        POSTBILLKICKSETDATETIME,
        POSTBILLKICKSETBY,
        NEXTSCRUBDATEDATETIME,
        LASTUPDATED,
        LASTMODIFIEDDATETIME,
        LASTMODIFIEDBY,
        DELETEDDATETIME,
        DELETEDBY,
        BULKACTIONSIZE,
        ROW_NUMBER() OVER (
            PARTITION BY CLAIMID
            ORDER BY CLAIMNOTEID DESC
        ) AS rn_claim,
        ROW_NUMBER() OVER (
            PARTITION BY CHARGEID
            ORDER BY CLAIMNOTEID DESC
        ) AS rn_charge
    FROM CLAIMNOTE
    WHERE nd_active_flag  = 'Y'
      AND DELETEDDATETIME IS NULL
)
SELECT
    -- ── Identity ─────────────────────────────────────────────────
    tr.CONTEXTNAME                                                    AS practice_name,
    tr.TRANSACTIONID                                                  AS charge_id,
    tr.CLAIMID                                                        AS claim_id,
    tr.PATIENTID                                                      AS patient_id,
    vc.VISITID                                                        AS visit_id,
    vc.VISITBILLID                                                    AS visit_bill_id,
    -- ── Transaction core ─────────────────────────────────────────
    tr.TRANSACTIONTYPE,
    tr.TRANSACTIONREASON,
    tr.TRANSACTIONTRANSFERTYPE,
    tr.TRANSACTIONTRANSFERINTENT,
    tr.TRANSACTIONMETHOD,
    tr.REVERSALFLAG                                                   AS tr_reversal_flag,
    tr.TRANSACTIONCREATEDDATETIME,
    tr.POSTDATE,
    tr.VOIDEDDATE,
    tr.FIRSTBILLEDDATETIME,
    tr.LASTBILLEDDATETIME,
    -- ── Procedure / charge line ───────────────────────────────────
    COALESCE(vc.PROCEDURECODE,  tr.PROCEDURECODE)                     AS procedure_code,
    pc.PROCEDURECODEDESCRIPTION                                       AS procedure_description,
    pc.PROCEDURECODEGROUP                                             AS procedure_code_group,
    pc.pc_rvu                                                         AS fee_schedule_rvu,
    COALESCE(vc.MODIFIERS,      tr.OTHERMODIFIER)                     AS modifiers,
    COALESCE(vc.PLACEOFSERVICE, tr.PLACEOFSERVICE)                    AS place_of_service,
    -- COALESCE(vc.REVENUECODE,    tr.REVENUECODE)                    AS revenue_code,
    -- vc.charge_source,
    vc.charge_type,
    vc.SOFTCODEYN,
    vc.EMG                                                            AS emergency_flag,
    vc.EPSDTYN,
    vc.NDC,
    vc.DRUGNAME,
    vc.DRUGDOSAGE,
    vc.DRUGUNITPRICE,
    vc.DRUGUNITQUALIFIER,
    vc.LINENOTE,
    vc.CLAIMCREATEDYN,
    -- ── Dates ────────────────────────────────────────────────────
    COALESCE(DATE(vc.FROMDATEDATETIME), tr.CHARGEFROMDATE)            AS service_date_from,
    COALESCE(DATE(vc.TODATEDATETIME),   tr.CHARGETODATE)              AS service_date_to,
    c.CLAIMSERVICEDATE,
    vc.CHARGEDATEDATETIME,
    -- ── Units & amounts ──────────────────────────────────────────
    tr.UNITS,
    COALESCE(vc.CHARGECODEUNITS, tr.UNITS)                            AS charge_units,
    vc.HCPCSUNITS,
    vc.ORIGINALUNITAMOUNT                                             AS fee_schedule_amount,
    tr.AMOUNT                                                         AS transaction_amount,
    vc.vc_amount                                                      AS visit_charge_amount,
    -- ── RVUs ─────────────────────────────────────────────────────
    tr.WORKRVU,
    tr.PRACTICEEXPENSERVU,
    tr.MALPRACTICERVU,
    tr.TOTALRVU,
    -- ── AR buckets ───────────────────────────────────────────────
    tr.EXPECTED                                                       AS expected_amount,
    tr.OUTSTANDING                                                    AS outstanding_amount,
    tr.PAYMENTS                                                       AS posted_payments,
    tr.TRANSFERS                                                      AS posted_transfers,
    tr.EXPECTEDALLOWEDAMOUNT,
    -- ── ERA ──────────────────────────────────────────────────────
    er.ERARECORDID,
    er.ERABATCHID,
    er.PAYERNAME                                                      AS era_payer_name,
    er.TRANSFERTYPE                                                   AS era_transfer_type,
    er.REVERSALFLAG                                                   AS era_reversal_flag,
    er.ALLOWABLE                                                      AS era_allowable,
    er.PAYMENT                                                        AS era_payment,
    er.COPAY                                                          AS era_copay,
    er.COINSURANCE                                                    AS era_coinsurance,
    er.CONTRACTUAL                                                    AS era_contractual,
    er.DEDUCTIBLE                                                     AS era_deductible,
    er.WITHHOLD                                                       AS era_withhold,
    er.PATIENTTRANSFER                                                AS era_patient_transfer,
    er.OTHERADJUSTMENT                                                AS era_other_adjustment,
    er.OTHERADJUSTMENTREASON                                          AS era_other_adjustment_reason,
    er.OTHERTRANSFER                                                  AS era_other_transfer,
    er.OTHERTRANSFERREASON                                            AS era_other_transfer_reason,
    er.INCENTIVEPAYMENT                                               AS era_incentive_payment,
    er.INTEREST                                                       AS era_interest,
    er.CLAIMALLOWABLE                                                 AS era_claim_allowable,
    er.CLAIMPAYMENT                                                   AS era_claim_payment,
    er.CLAIMCONTRACTUAL                                               AS era_claim_contractual,
    er.CLAIMCOINSURANCE                                               AS era_claim_coinsurance,
    er.CLAIMCOPAY                                                     AS era_claim_copay,
    er.CLAIMDEDUCTIBLE                                                AS era_claim_deductible,
    -- ── Claim status ─────────────────────────────────────────────
    c.PRIMARYCLAIMSTATUS,
    c.SECONDARYCLAIMSTATUS,
    c.PATIENTCLAIMSTATUS,
    c.PRIMARYOUTSTANDING,
    c.SECONDARYOUTSTANDING,
    c.PATIENTOUTSTANDING,
    c.PRIMARYCLAIMTYPE,
    c.SECONDARYCLAIMTYPE,
    c.CLAIMTYPE,
    c.CLAIMCREATEDDATETIME,
    c.LASTACTION                                                      AS claim_last_action,
    c.LASTACTIONDATE                                                  AS claim_last_action_date,
    c.LASTBILLEDDATE1,
    c.LASTBILLEDDATEP,
    c.CONTEXTNAME                                                     AS claim_facility_name,
    -- ── Rendering provider ────────────────────────────────────────
    c.RENDERINGPROVIDERID                                             AS rendering_provider_id,
    TRIM(CONCAT_WS(' ', rp.PROVIDERFIRSTNAME, rp.PROVIDERLASTNAME))   AS rendering_provider_name,
    -- rp.BILLEDNAME                                                  AS rendering_provider_billed_name,
    rp.PROVIDERTYPE                                                   AS rendering_provider_type,
    rp.PROVIDERTYPENAME                                               AS rendering_provider_type_name,
    rp.PROVIDERTYPECATEGORY                                           AS rendering_provider_type_category,
    rp.SPECIALTY                                                      AS rendering_provider_specialty,
    -- rp.TAXONOMY                                                    AS rendering_provider_taxonomy,
    rp.PROVIDERNPINUMBER                                              AS rendering_provider_npi,
    -- ── Billing provider ──────────────────────────────────────────
    c.PRIMARYBILLINGPROVIDERID                                        AS billing_provider_id,
    TRIM(CONCAT_WS(' ', bp.PROVIDERFIRSTNAME, bp.PROVIDERLASTNAME))   AS billing_provider_name,
    -- bp.BILLEDNAME                                                  AS billing_provider_billed_name,
    bp.PROVIDERTYPE                                                   AS billing_provider_type,
    bp.PROVIDERTYPENAME                                               AS billing_provider_type_name,
    bp.SPECIALTY                                                      AS billing_provider_specialty,
    bp.PROVIDERNPINUMBER                                              AS billing_provider_npi,
    -- bp.FEDERALIDNUMBER                                             AS billing_provider_tax_id,
    -- bp.FEDERALIDNUMBERTYPE                                         AS billing_provider_tax_id_type,
    -- ── Supervising provider ──────────────────────────────────────
    c.SUPERVISINGPROVIDERID                                           AS supervising_provider_id,
    TRIM(CONCAT_WS(' ', sp.PROVIDERFIRSTNAME, sp.PROVIDERLASTNAME))   AS supervising_provider_name,
    sp.PROVIDERNPINUMBER                                              AS supervising_provider_npi,
    sp.SPECIALTY                                                      AS supervising_provider_specialty,
    sp.PROVIDERTYPE                                                   AS supervising_provider_type,
    -- ════════════════════════════════════════════════════════════
    -- PRIMARY INSURANCE
    -- ════════════════════════════════════════════════════════════
    COALESCE(c.CLAIMPRIMARYPATIENTINSID, vc.PRIMARYPATIENTINSURANCEID) AS primary_ins_id,
    pi1.INSURANCENAME                                                  AS primary_insurance_name,
    pi1.INSURANCEPACKAGEID                                             AS primary_insurance_package_id,
    py1.INSURANCEPACKAGENAME                                           AS primary_package_name,
    py1.INSURANCEPACKAGETYPE                                           AS primary_package_type,
    py1.INSURANCEREPORTINGCATEGORY                                     AS primary_reporting_category,
    py1.IRCGROUP                                                       AS primary_irc_group,
    py1.GLOBALALLOWABLECATEGORY                                        AS primary_global_allowable_category,
    py1.LOCALALLOWABLECATEGORY                                         AS primary_local_allowable_category,
    pi1.SEQUENCENUMBER                                                 AS primary_ins_sequence,
    pi1.POLICYIDNUMBER                                                 AS primary_policy_id,
    pi1.POLICYGROUPNUMBER                                              AS primary_group_number,
    pi1.PATIENTRELATIONSHIP                                            AS primary_patient_relationship,
    TRIM(CONCAT_WS(' ', pi1.FIRSTNAME, pi1.LASTNAME))                  AS primary_policyholder_name,
    pi1.ISSUEDATE                                                      AS primary_ins_effective_date,
    pi1.EXPIRATIONDATE                                                 AS primary_ins_expiry_date,
    pi1.CANCELLATIONDATE                                               AS primary_ins_cancelled_date,
    pi1.ELIGIBILITYSTATUS                                              AS primary_eligibility_status,
    pi1.ELIGIBILITYMESSAGE                                             AS primary_eligibility_message,
    pi1.ELIGIBILITYSERVICEDATE                                         AS primary_eligibility_service_date,
    pi1.COPAY                                                          AS primary_copay_amount,
    pi1.COINSURANCEPERCENT                                             AS primary_coinsurance_pct,
    -- pi1.INSURANCEPRODUCTCODE                                        AS primary_insurance_product_code,
    -- ipt1.NAME                                                       AS primary_insurance_product_type,
    -- ════════════════════════════════════════════════════════════
    -- SECONDARY INSURANCE
    -- ════════════════════════════════════════════════════════════
    c.CLAIMSECONDARYPATIENTINSID                                       AS secondary_ins_id,
    pi2.INSURANCENAME                                                  AS secondary_insurance_name,
    pi2.INSURANCEPACKAGEID                                             AS secondary_insurance_package_id,
    py2.INSURANCEPACKAGENAME                                           AS secondary_package_name,
    py2.INSURANCEPACKAGETYPE                                           AS secondary_package_type,
    py2.INSURANCEREPORTINGCATEGORY                                     AS secondary_reporting_category,
    py2.IRCGROUP                                                       AS secondary_irc_group,
    pi2.SEQUENCENUMBER                                                 AS secondary_ins_sequence,
    pi2.POLICYIDNUMBER                                                 AS secondary_policy_id,
    pi2.POLICYGROUPNUMBER                                              AS secondary_group_number,
    pi2.PATIENTRELATIONSHIP                                            AS secondary_patient_relationship,
    TRIM(CONCAT_WS(' ', pi2.FIRSTNAME, pi2.LASTNAME))                  AS secondary_policyholder_name,
    pi2.ISSUEDATE                                                      AS secondary_ins_effective_date,
    pi2.EXPIRATIONDATE                                                 AS secondary_ins_expiry_date,
    pi2.ELIGIBILITYSTATUS                                              AS secondary_eligibility_status,
    pi2.COPAY                                                          AS secondary_copay_amount,
    pi2.COINSURANCEPERCENT                                             AS secondary_coinsurance_pct,
    -- pi2.INSURANCEPRODUCTCODE                                        AS secondary_insurance_product_code,
    -- ipt2.NAME                                                       AS secondary_insurance_product_type,
    -- ── Charge detail ─────────────────────────────────────────────
    cd.CHARGEDETAILID,
    -- cd.cd_amount                                                    AS chargedetail_amount,
    cd.QUANTITY                                                        AS chargedetail_quantity,
    -- cd.cd_renderingproviderid                                       AS chargedetail_rendering_provider_id,
    -- cd.RENDERINGDEPARTMENTID                                        AS chargedetail_rendering_dept_id,
    cd.LEVELOFCAREID,
    cd.cd_fromdate                                                     AS chargedetail_service_from,
    cd.cd_todate                                                       AS chargedetail_service_to,
    -- ════════════════════════════════════════════════════════════
    -- CLAIM NOTE — claim-level (latest note for the whole claim)
    -- ════════════════════════════════════════════════════════════
    cn_c.CLAIMNOTEID                                                   AS claimnote_id,
    cn_c.CREATEDDATETIME                                               AS claimnote_createddatetime,
    cn_c.CREATEDBY                                                     AS claimnote_createdby,
    cn_c.ACTION                                                        AS claimnote_action,
    cn_c.TRANSFERTYPE                                                  AS claimnote_transfertype,
    cn_c.CLAIMSTATUS                                                   AS claimnote_claimstatus,
    cn_c.PENDINGFLAG                                                   AS claimnote_pendingflag,
    cn_c.PATIENTINSURANCEID                                            AS claimnote_patientinsuranceid,
    -- cn_c.BILLINGPROVIDERID                                          AS claimnote_billingproviderid,
    -- cn_c.BILLINGINSURANCEPACKAGEID                                  AS claimnote_billinginsurancepackageid,
    cn_c.BILLINGBATCHID                                                AS claimnote_billingbatchid,
    cn_c.SCRUBTYPE                                                     AS claimnote_scrubtype,
    -- cn_c.RULECLASS                                                  AS claimnote_ruleclass,
    -- cn_c.CLAIMRULEID                                                AS claimnote_claimruleid,
    -- cn_c.CLAIMRULENAME                                              AS claimnote_claimrulename,
    -- cn_c.LOCALCLAIMRULEID                                           AS claimnote_localclaimruleid,
    -- cn_c.NETWORKCLAIMRULEID                                         AS claimnote_networkclaimruleid,
    -- cn_c.CUSTOMRULEID                                               AS claimnote_customruleid,
    cn_c.SCRUBACTION                                                   AS claimnote_scrubaction,
    -- cn_c.FRONTENDACTION                                             AS claimnote_frontendaction,
    -- cn_c.BACKENDACTION                                              AS claimnote_backendaction,
    -- cn_c.SCRUBFIXTEXT                                               AS claimnote_scrubfixtext,
    -- cn_c.FIXTEXT                                                    AS claimnote_fixtext,
    cn_c.DENIALSTATUSENTRY                                             AS claimnote_denialstatusentry,
    cn_c.TRANSACTIONPOSTED                                             AS claimnote_transactionposted,
    cn_c.ATHENAKICKREASONID                                            AS claimnote_athenakickreasonid,
    cn_c.KICKREASONID                                                  AS claimnote_kickreasonid,
    cn_c.KICKDATE                                                      AS claimnote_kickdate,
    cn_c.KICKDAYS                                                      AS claimnote_kickdays,
    cn_c.ATHENA_KICKCODE                                               AS claimnote_athena_kickcode,
    cn_c.REMARKCODE                                                    AS claimnote_remarkcode,
    cn_c.REMARKCODE_DESCRIPTION                                        AS claimnote_remarkcode_description,
    cn_c.NOTE                                                          AS claimnote_text,
    cn_c.ACTIONRESPONSE                                                AS claimnote_actionresponse,
    -- cn_c.CONTACTNAME                                                AS claimnote_contactname,
    -- cn_c.CONTACTNUMBER                                              AS claimnote_contactnumber,
    -- cn_c.CALLREFERENCENUMBER                                        AS claimnote_callreferencenumber,
    cn_c.CLAIMREFERENCENUMBER                                          AS claimnote_claimreferencenumber,
    -- cn_c.PAYMENTMETHOD                                              AS claimnote_paymentmethod,
    -- cn_c.PAYMENTAMOUNT                                              AS claimnote_paymentamount,
    -- cn_c.PAYMENTDATEDATETIME                                        AS claimnote_paymentdatedatetime,
    -- cn_c.CHECKAMOUNT                                                AS claimnote_checkamount,
    -- cn_c.CHECKIDENTIFIER                                            AS claimnote_checkidentifier,
    -- cn_c.CASHEDDATEDATETIME                                         AS claimnote_casheddatedatetime,
    cn_c.PAYMENTBATCHID                                                AS claimnote_paymentbatchid,
    -- cn_c.APPEALLETTERID                                             AS claimnote_appealletterid,
    -- cn_c.REQUIRESMANUALCLOSEYN                                      AS claimnote_requiresmanualcloseyn,
    -- cn_c.OVERRIDEABLEYN                                             AS claimnote_overrideableyn,
    -- cn_c.OUTSTANDING                                                AS claimnote_outstanding,
    -- cn_c.POSTBILLKICKREASONID                                       AS claimnote_postbillkickreasonid,
    -- cn_c.POSTBILLKICKSETDATETIME                                    AS claimnote_postbillkicksetdatetime,
    -- cn_c.POSTBILLKICKSETBY                                          AS claimnote_postbillkicksetby,
    cn_c.NEXTSCRUBDATEDATETIME                                         AS claimnote_nextscrubdatedatetime,
    cn_c.LASTUPDATED                                                   AS claimnote_lastupdated,
    cn_c.LASTMODIFIEDDATETIME                                          AS claimnote_lastmodifieddatetime,
    cn_c.LASTMODIFIEDBY                                                AS claimnote_lastmodifiedby,
    -- ════════════════════════════════════════════════════════════
    -- CLAIM NOTE — charge-level (latest note for this charge line)
    -- ════════════════════════════════════════════════════════════
    cn_t.CLAIMNOTEID                                                   AS charge_claimnote_id,
    cn_t.CREATEDDATETIME                                               AS charge_claimnote_createddatetime,
    cn_t.CREATEDBY                                                     AS charge_claimnote_createdby,
    cn_t.ACTION                                                        AS charge_claimnote_action,
    cn_t.TRANSFERTYPE                                                  AS charge_claimnote_transfertype,
    cn_t.CLAIMSTATUS                                                   AS charge_claimnote_claimstatus,
    cn_t.PENDINGFLAG                                                   AS charge_claimnote_pendingflag,
    cn_t.PATIENTINSURANCEID                                            AS charge_claimnote_patientinsuranceid,
    cn_t.SCRUBTYPE                                                     AS charge_claimnote_scrubtype,
    -- cn_t.RULECLASS                                                  AS charge_claimnote_ruleclass,
    -- cn_t.CLAIMRULENAME                                              AS charge_claimnote_claimrulename,
    cn_t.SCRUBACTION                                                   AS charge_claimnote_scrubaction,
    -- cn_t.FRONTENDACTION                                             AS charge_claimnote_frontendaction,
    -- cn_t.BACKENDACTION                                              AS charge_claimnote_backendaction,
    cn_t.DENIALSTATUSENTRY                                             AS charge_claimnote_denialstatusentry,
    cn_t.TRANSACTIONPOSTED                                             AS charge_claimnote_transactionposted,
    cn_t.REMARKCODE                                                    AS charge_claimnote_remarkcode,
    cn_t.REMARKCODE_DESCRIPTION                                        AS charge_claimnote_remarkcode_description,
    -- cn_t.ATHENA_KICKCODE                                            AS charge_claimnote_athena_kickcode,
    -- cn_t.KICKDATE                                                   AS charge_claimnote_kickdate,
    cn_t.NOTE                                                          AS charge_claimnote_text,
    cn_t.OUTSTANDING                                                   AS charge_claimnote_outstanding,
    cn_t.LASTUPDATED                                                   AS charge_claimnote_lastupdated
FROM `TRANSACTION` tr                             -- reserved keyword — must be backtick-escaped
LEFT JOIN era_deduped er
    ON  er.CHARGEID  = tr.TRANSACTIONID
    AND er.rn        = 1
INNER JOIN CLAIM c
    ON  c.CLAIMID        = tr.CLAIMID
    AND c.nd_active_flag = 'Y'
LEFT JOIN cd_deduped cd
    ON  cd.CHARGEID  = tr.TRANSACTIONID
    AND cd.rn        = 1
LEFT JOIN vc_deduped vc
    ON  vc.VISITCHARGEID = cd.VISITCHARGEID
    AND vc.rn            = 1
LEFT JOIN pc_deduped pc
    ON  pc.PROCEDURECODE = COALESCE(vc.PROCEDURECODE, tr.PROCEDURECODE)
    AND pc.rn            = 1
LEFT JOIN PROVIDER rp
    ON  rp.PROVIDERID     = c.RENDERINGPROVIDERID
    AND rp.nd_active_flag = 'Y'
LEFT JOIN PROVIDER bp
    ON  bp.PROVIDERID     = c.PRIMARYBILLINGPROVIDERID
    AND bp.nd_active_flag = 'Y'
LEFT JOIN PROVIDER sp
    ON  sp.PROVIDERID     = c.SUPERVISINGPROVIDERID
    AND sp.nd_active_flag = 'Y'
LEFT JOIN PATIENTINSURANCE pi1
    ON  pi1.PATIENTINSURANCEID = COALESCE(c.CLAIMPRIMARYPATIENTINSID, vc.PRIMARYPATIENTINSURANCEID)
    AND pi1.nd_active_flag     = 'Y'
LEFT JOIN PAYER py1
    ON  py1.INSURANCEPACKAGEID = pi1.INSURANCEPACKAGEID
    AND py1.nd_active_flag     = 'Y'
LEFT JOIN INSURANCEPRODUCTTYPE ipt1
    ON  ipt1.INSURANCEPRODUCTTYPEID = pi1.INSURANCEPRODUCTTYPEID
    AND ipt1.nd_active_flag         = 'Y'
LEFT JOIN PATIENTINSURANCE pi2
    ON  pi2.PATIENTINSURANCEID = c.CLAIMSECONDARYPATIENTINSID
    AND pi2.nd_active_flag     = 'Y'
LEFT JOIN PAYER py2
    ON  py2.INSURANCEPACKAGEID = pi2.INSURANCEPACKAGEID
    AND py2.nd_active_flag     = 'Y'
LEFT JOIN INSURANCEPRODUCTTYPE ipt2
    ON  ipt2.INSURANCEPRODUCTTYPEID = pi2.INSURANCEPRODUCTTYPEID
    AND ipt2.nd_active_flag         = 'Y'
LEFT JOIN cn_deduped cn_c
    ON  cn_c.CLAIMID   = c.CLAIMID
    AND cn_c.rn_claim  = 1
LEFT JOIN cn_deduped cn_t
    ON  cn_t.CHARGEID  = tr.TRANSACTIONID
    AND cn_t.rn_charge = 1
WHERE
    tr.nd_active_flag = 'Y'
ORDER BY
    tr.CLAIMID,
    tr.CHARGEFROMDATE,
    tr.TRANSACTIONID;