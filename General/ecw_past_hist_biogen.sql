
select * from (
WITH 
-- Roll up surgical history at encounter level
surgical_cte AS (
    SELECT 
        encounterID,
        GROUP_CONCAT(CONCAT(date, ' ^ ', reason) SEPARATOR ' + ') AS past_surgical_history
    FROM texas.surgicalhistory 
    GROUP BY encounterID
),
-- Roll up family history at encounter level
family_cte AS (
    SELECT 
        encounterid,
        GROUP_CONCAT(CONCAT(name, ' ^ ', notes) SEPARATOR ' + ') AS family_history_notes
    FROM texas.family
    GROUP BY encounterid
),
-- Combine and roll up social history + structured social history at encounter level
social_cte AS (
    SELECT 
        encounterid,
        GROUP_CONCAT(CONCAT(itemname, ' ^ ', notes) SEPARATOR ' + ') AS social_history_full
    FROM (
        SELECT 
            a.encounterid, 
            b.itemname, 
            a.value AS notes 
        FROM texas.social a
        LEFT JOIN texas.items b ON a.itemid = b.itemid
        UNION ALL
        SELECT 
            a.encounterid, 
            b.itemname, 
            a.value AS notes 
        FROM texas.structsocialhistory a
        LEFT JOIN texas.items b ON a.itemid = b.itemid
    ) combined_social
    GROUP BY encounterid
)
-- Final encounter-level pull
SELECT
    e.patientid,
    e.encounterid,
    case 
	when e.date in ('None') then null 
    when left(e.date,10) REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' then STR_TO_DATE(left(e.date,10), '%Y-%m-%d')
	when left(e.date,10)REGEXP '^[0-9]{2}-[0-9]{2}-[0-9]{4}$' then STR_TO_DATE(left(e.date,10), '%m-%d-%Y')
    end  AS visit_date,
    case when ed.pasthistory in ('None') then null else ed.pasthistory end AS medical_history,
    sh.past_surgical_history,
    f.family_history_notes,
    s.social_history_full
FROM (select * from texas.enc where nd_ActiveFlag = 'Y') e
LEFT JOIN (select * from texas.encounterdata where nd_ActiveFlag = 'Y') ed 
       ON e.encounterid = ed.encounterid
LEFT JOIN surgical_cte sh 
       ON e.encounterid = sh.encounterid
LEFT JOIN family_cte f 
       ON e.encounterid = f.encounterid
LEFT JOIN social_cte s 
       ON e.encounterid = s.encounterid)a
inner join biogen.patient_list pl on a.patientid = pl.ndid and patient_provider = 'Texas'
where visit_date is not null and (medical_history is not null and past_surgical_history is not null and family_history_notes is not null and social_history_full is not null )
;