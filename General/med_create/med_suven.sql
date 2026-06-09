create table suven.fcn_medications_22new as
select * from (
SELECT 
'oldrxmain' AS source,
b.oldrxid AS med_id,
e.patientID AS ndid,
e.encounterid AS eid,
DATE(e.date) AS enc_date,
NULL AS written_date,
NULL AS med_administered_datetime,
NULL AS doc_orderdatetime,
CASE
WHEN CAST(b.startdate as CHAR) = 'None' THEN NULL
WHEN YEAR(b.startdate) < 1991 THEN NULL
ELSE DATE(LEFT(b.startdate, 10))
END AS med_start_date,
CASE
WHEN CAST(b.stopdate as char) = 'None' THEN NULL
WHEN YEAR(b.stopdate) < 1991 THEN NULL
ELSE DATE(LEFT(b.stopdate, 10))
END AS med_end_date,
NULL AS med_createddatetime,
NULL AS doc_createddatetime,
NULL AS last_dispensed_date,
NULL AS sample_expiration_date,
NULL AS administer_expiration_date,
NULL AS earliest_fill_date,
b.ndc_code AS med_code,
COALESCE(c.drugname, i.itemname) AS med_name,
CASE 
WHEN c.ndc IS NOT NULL THEN 'NDC'
WHEN i.keyname IS NOT NULL THEN i.keyname
ELSE NULL 
END AS med_coding_system,
CASE 
WHEN rxcomment IN ('Taking','Takes','Start','Continue') THEN 'Taking'
WHEN rxcomment IN ('Stop','Not-Taking','Discontinued','Discontinue','cancel','Cancelled','cancell','D/C by patient','D/C by another provider') THEN 'Not Taking'
WHEN rxcomment IN ('Refill','Sample/Refill') THEN 'Refill'
WHEN rxcomment IN ('Once') THEN 'Stat'
WHEN rxcomment IN ('never started') THEN 'Never Started'
WHEN rxcomment IN ('Ins Not Covered, Med chg') THEN 'Ins Not Covered, Med chg'
WHEN rxcomment IN ('Error','Entered in error:') THEN 'Errors'
WHEN rxcomment IN ('Awaiting ins. approval:') THEN 'Yet to start'
ELSE NULL 
END AS med_status,
--     e.DoctorsFlag as med_status_flag,
'' as med_status_flag,
--     ia.itemName as med_indication,
''  as med_indication,
ord.med_formulation AS med_formulation,           -- Position 24
ord.med_route AS med_route,                       -- Position 25 (FIXED ORDER)
ord.med_strength AS med_strength,                 -- Position 26
NULL AS med_strength_unit,                        -- Position 27
ord.med_frequency AS med_frequency,               -- Position 28 (FIXED ORDER)
ord.med_pb_qty AS med_pb_qty,                     -- Position 29 (FIXED ORDER)
ord.med_days_supply AS med_days_supply,           -- Position 30 (FIXED ORDER)
ord.med_refills AS med_refills,                   -- Position 31 (FIXED ORDER)
COALESCE(NULLIF(TRIM(ora.additionalinstructions), ''), ora.rxnotes) AS med_directions, -- Position 32
b.FillDate AS fill_date,                          -- (MOVED)
NULL AS med_fill_type,                            -- Position 33
NULL discont_date,
--     e.disconorstopnotes as discont_reason,            -- Position 34
'' as discont_reason,            -- Position 34
CURRENT_TIMESTAMP() as created_datetime,          -- Position 35
'ND' as created_by,                               -- Position 36
CURRENT_TIMESTAMP() as updated_datetime,          -- Position 37 (ADDED)
'ND' as updated_by,                               -- Position 38 (ADDED)
'eCW' as ehr_source_name,                         -- Position 39
'bronze_table' as source_path,                    -- Position 40
'Structured' as data_type,                        -- Position 41
8 as psid            
,b.nd_extracted_date                              -- Position 42

-- SELECT count(b.oldrxid)
FROM enc e 
join suven.pateint_list_21thapri pl on pl.patientid = e.patientID and pl.dbname ='fcn' 
ANd pl.ActiveFlag ='Y'
INNER JOIN oldrxmain b ON e.encounterid = b.encounterid   and e.nd_ActiveFlag ='Y'
and b.nd_ActiveFlag ='Y'
LEFT JOIN oldrxdetail_pivot_new ord ON ord.oldrxid = b.oldrxid
LEFT JOIN oldrxmain_addlinfo ora ON b.oldrxid = ora.oldrxid -- and ora.nd_ActiveFlag ='Y'
LEFT JOIN ndclookupenteries c ON b.ndc_code = c.ndc and c.nd_ActiveFlag ='Y'
LEFT JOIN items i ON b.itemid = i.itemid  and i.nd_ActiveFlag ='Y'
-- LEFT JOIN items ia ON e.AssessId = ia.itemid
LEFT JOIN rx_medication_alert d ON b.encounterid = d.encounterid AND b.itemid = d.itemid 
and d.nd_ActiveFlag ='Y'

/*WHERE b.oldrxid NOT IN(select med_id from rgd_udm_silver.medication)
and b.ndc_code NOT IN(select med_code from rgd_udm_silver.medication)*/
GROUP BY
b.oldrxid, e.patientID, e.encounterid, e.date,
b.startdate, b.stopdate, b.ndc_code, c.drugname, i.itemname, c.ndc, i.keyname,
rxcomment, ora.additionalinstructions, ora.rxnotes, b.FillDate,
ord.med_formulation, ord.med_strength, ord.med_pb_qty, ord.med_days_supply, 
ord.med_refills, ord.med_route, ord.med_frequency -- , e.DoctorsFlag, ia.itemName, e.disconorstopnotes
-- ,e.nd_extracted_date 
) a 

;