CREATE TABLE biogen.pasthistory_athenaone as 
-- dcnd
select * from (
with surgical as (
	select
	ndid, encounter_id, encounter_date,
	null as medical_history,
	past_surgical_history,
	null as family_history_notes,
	null as social_history_full
	from(
	SELECT
	    TRIM(ndid) AS ndid,
	    COALESCE(encounter_date, surgery_date) AS encounter_date,
	    eid AS encounter_id,
	    GROUP_CONCAT( CONCAT( COALESCE(surgery_name, ''), ' ', COALESCE(surgery_code, ''), ' ^ ', COALESCE(surgery_reason, '')) SEPARATOR ' + ' ) AS past_surgical_history
	FROM udm_dcnd.surgical_history
	GROUP BY TRIM(ndid), COALESCE(encounter_date, surgery_date), eid )a
	where trim(past_surgical_history) <> '^'
),
social as (
	select
	ndid, encounter_id, encounter_date,
	null as medical_history,
	null as past_surgical_history,
	null as family_history_notes,
	social_history_full from (
	select
		ndid,
		eid as encounter_id,
		COALESCE(encounter_date,social_hist_date) as encounter_date,
		GROUP_CONCAT( CONCAT( COALESCE(social_category, ''), ' ^ ', COALESCE(social_option, ''), ' ', COALESCE(social_notes, '') ) SEPARATOR ' + ') as social_history_full
	from udm_dcnd.social_history sh
	GROUP BY ndid, eid, COALESCE(encounter_date,social_hist_date)
	)a where trim(social_history_full) <> '^'
),
medical as (
	select
	ndid, encounter_id, encounter_date,
	medical_history,
	null as past_surgical_history,
	null as family_history_notes,
	null as social_history_full from (
	select
		ndid,
		eid as encounter_id,
		COALESCE(encounter_date,med_hist_date) as encounter_date,
		GROUP_CONCAT( CONCAT( COALESCE(med_hist_question, ''), ' ^ ', COALESCE(med_hist_answer, '')) SEPARATOR ' + ') as medical_history
	from udm_dcnd.medical_history sh
	GROUP BY ndid, eid, COALESCE(encounter_date,med_hist_date)
	)a where trim(medical_history) <> '^'
),
family as (
	select
	ndid, encounter_id, encounter_date,
	null as medical_history,
	null as past_surgical_history,
	family_history_notes,
	null as social_history_full from (
	select
		ndid,
		eid as encounter_id,
		COALESCE(encounter_date,fam_hist_date) as encounter_date,
		GROUP_CONCAT( CONCAT( COALESCE(fam_hist_relation, ''), ' ^ ', COALESCE(fam_hist_detail, '')) SEPARATOR ' + ') as family_history_notes
	from udm_dcnd.family_history sh
	GROUP BY ndid, eid, COALESCE(encounter_date,fam_hist_date)
	)a where trim(family_history_notes) <> '^'
),
all_keys as (
    SELECT ndid, encounter_date FROM surgical
    UNION
    SELECT ndid, encounter_date FROM social
    UNION
    SELECT ndid, encounter_date FROM medical
    UNION
    SELECT ndid, encounter_date FROM family
)
select
    k.ndid,
    k.encounter_date,
    -- Prioritize encounter_id from surgical, then social, then medical, then family
    COALESCE(s.encounter_id, soc.encounter_id, m.encounter_id, f.encounter_id) AS encounter_id,
    m.medical_history,
    s.past_surgical_history,
    f.family_history_notes,
    soc.social_history_full
from all_keys k
left join surgical s on k.ndid = s.ndid and k.encounter_date = s.encounter_date
left join social soc on k.ndid = soc.ndid and k.encounter_date = soc.encounter_date
left join medical m on k.ndid = m.ndid and k.encounter_date = m.encounter_date
left join family f on k.ndid = f.ndid and k.encounter_date = f.encounter_date)dcnd
UNION ALL
-- raleigh
select * from (
with surgical as (
	select
	ndid, encounter_id, encounter_date,
	null as medical_history,
	past_surgical_history,
	null as family_history_notes,
	null as social_history_full
	from(
	SELECT
	    TRIM(ndid) AS ndid,
	    COALESCE(encounter_date, surgery_date) AS encounter_date,
	    eid AS encounter_id,
	    GROUP_CONCAT( CONCAT( COALESCE(surgery_name, ''), ' ', COALESCE(surgery_code, ''), ' ^ ', COALESCE(surgery_reason, '')) SEPARATOR ' + ' ) AS past_surgical_history
	FROM udm_raleigh.surgical_history
	GROUP BY TRIM(ndid), COALESCE(encounter_date, surgery_date), eid )a
	where trim(past_surgical_history) <> '^'
),
social as (
	select
	ndid, encounter_id, encounter_date,
	null as medical_history,
	null as past_surgical_history,
	null as family_history_notes,
	social_history_full from (
	select
		ndid,
		eid as encounter_id,
		COALESCE(encounter_date,social_hist_date) as encounter_date,
		GROUP_CONCAT( CONCAT( COALESCE(social_category, ''), ' ^ ', COALESCE(social_option, ''), ' ', COALESCE(social_notes, '') ) SEPARATOR ' + ') as social_history_full
	from udm_raleigh.social_history sh
	GROUP BY ndid, eid, COALESCE(encounter_date,social_hist_date)
	)a where trim(social_history_full) <> '^'
),
medical as (
	select
	ndid, encounter_id, encounter_date,
	medical_history,
	null as past_surgical_history,
	null as family_history_notes,
	null as social_history_full from (
	select
		ndid,
		eid as encounter_id,
		COALESCE(encounter_date,med_hist_date) as encounter_date,
		GROUP_CONCAT( CONCAT( COALESCE(med_hist_question, ''), ' ^ ', COALESCE(med_hist_answer, '')) SEPARATOR ' + ') as medical_history
	from udm_raleigh.medical_history sh
	GROUP BY ndid, eid, COALESCE(encounter_date,med_hist_date)
	)a where trim(medical_history) <> '^'
),
family as (
	select
	ndid, encounter_id, encounter_date,
	null as medical_history,
	null as past_surgical_history,
	family_history_notes,
	null as social_history_full from (
	select
		ndid,
		eid as encounter_id,
		COALESCE(encounter_date,fam_hist_date) as encounter_date,
		GROUP_CONCAT( CONCAT( COALESCE(fam_hist_relation, ''), ' ^ ', COALESCE(fam_hist_detail, '')) SEPARATOR ' + ') as family_history_notes
	from udm_raleigh.family_history sh
	GROUP BY ndid, eid, COALESCE(encounter_date,fam_hist_date)
	)a where trim(family_history_notes) <> '^'
),
all_keys as (
    SELECT ndid, encounter_date FROM surgical
    UNION
    SELECT ndid, encounter_date FROM social
    UNION
    SELECT ndid, encounter_date FROM medical
    UNION
    SELECT ndid, encounter_date FROM family
)
select
    k.ndid,
    k.encounter_date,
    -- Prioritize encounter_id from surgical, then social, then medical, then family
    COALESCE(s.encounter_id, soc.encounter_id, m.encounter_id, f.encounter_id) AS encounter_id,
    m.medical_history,
    s.past_surgical_history,
    f.family_history_notes,
    soc.social_history_full
from all_keys k
left join surgical s on k.ndid = s.ndid and k.encounter_date = s.encounter_date
left join social soc on k.ndid = soc.ndid and k.encounter_date = soc.encounter_date
left join medical m on k.ndid = m.ndid and k.encounter_date = m.encounter_date
left join family f on k.ndid = f.ndid and k.encounter_date = f.encounter_date)raleigh
UNION ALL
-- tncpa
select * from (
with surgical as (
	select
	ndid, encounter_id, encounter_date,
	null as medical_history,
	past_surgical_history,
	null as family_history_notes,
	null as social_history_full
	from(
	SELECT
	    TRIM(ndid) AS ndid,
	    COALESCE(encounter_date, surgery_date) AS encounter_date,
	    eid AS encounter_id,
	    GROUP_CONCAT( CONCAT( COALESCE(surgery_name, ''), ' ', COALESCE(surgery_code, ''), ' ^ ', COALESCE(surgery_reason, '')) SEPARATOR ' + ' ) AS past_surgical_history
	FROM udm_tncpa.surgical_history
	GROUP BY TRIM(ndid), COALESCE(encounter_date, surgery_date), eid )a
	where trim(past_surgical_history) <> '^'
),
social as (
	select
	ndid, encounter_id, encounter_date,
	null as medical_history,
	null as past_surgical_history,
	null as family_history_notes,
	social_history_full from (
	select
		ndid,
		eid as encounter_id,
		COALESCE(encounter_date,social_hist_date) as encounter_date,
		GROUP_CONCAT( CONCAT( COALESCE(social_category, ''), ' ^ ', COALESCE(social_option, ''), ' ', COALESCE(social_notes, '') ) SEPARATOR ' + ') as social_history_full
	from udm_tncpa.social_history sh
	GROUP BY ndid, eid, COALESCE(encounter_date,social_hist_date)
	)a where trim(social_history_full) <> '^'
),
medical as (
	select
	ndid, encounter_id, encounter_date,
	medical_history,
	null as past_surgical_history,
	null as family_history_notes,
	null as social_history_full from (
	select
		ndid,
		eid as encounter_id,
		COALESCE(encounter_date,med_hist_date) as encounter_date,
		GROUP_CONCAT( CONCAT( COALESCE(med_hist_question, ''), ' ^ ', COALESCE(med_hist_answer, '')) SEPARATOR ' + ') as medical_history
	from udm_tncpa.medical_history sh
	GROUP BY ndid, eid, COALESCE(encounter_date,med_hist_date)
	)a where trim(medical_history) <> '^'
),
family as (
	select
	ndid, encounter_id, encounter_date,
	null as medical_history,
	null as past_surgical_history,
	family_history_notes,
	null as social_history_full from (
	select
		ndid,
		eid as encounter_id,
		COALESCE(encounter_date,fam_hist_date) as encounter_date,
		GROUP_CONCAT( CONCAT( COALESCE(fam_hist_relation, ''), ' ^ ', COALESCE(fam_hist_detail, '')) SEPARATOR ' + ') as family_history_notes
	from udm_tncpa.family_history sh
	GROUP BY ndid, eid, COALESCE(encounter_date,fam_hist_date)
	)a where trim(family_history_notes) <> '^'
),
all_keys as (
    SELECT ndid, encounter_date FROM surgical
    UNION
    SELECT ndid, encounter_date FROM social
    UNION
    SELECT ndid, encounter_date FROM medical
    UNION
    SELECT ndid, encounter_date FROM family
)
select
    k.ndid,
    k.encounter_date,
    -- Prioritize encounter_id from surgical, then social, then medical, then family
    COALESCE(s.encounter_id, soc.encounter_id, m.encounter_id, f.encounter_id) AS encounter_id,
    m.medical_history,
    s.past_surgical_history,
    f.family_history_notes,
    soc.social_history_full
from all_keys k
left join surgical s on k.ndid = s.ndid and k.encounter_date = s.encounter_date
left join social soc on k.ndid = soc.ndid and k.encounter_date = soc.encounter_date
left join medical m on k.ndid = m.ndid and k.encounter_date = m.encounter_date
left join family f on k.ndid = f.ndid and k.encounter_date = f.encounter_date)tncpa
UNION ALL 
-- tng
select * from (
with surgical as (
	select
	ndid, encounter_id, encounter_date,
	null as medical_history,
	past_surgical_history,
	null as family_history_notes,
	null as social_history_full
	from(
	SELECT
	    TRIM(ndid) AS ndid,
	    COALESCE(encounter_date, surgery_date) AS encounter_date,
	    eid AS encounter_id,
	    GROUP_CONCAT( CONCAT( COALESCE(surgery_name, ''), ' ', COALESCE(surgery_code, ''), ' ^ ', COALESCE(surgery_reason, '')) SEPARATOR ' + ' ) AS past_surgical_history
	FROM udm_tng.surgical_history
	GROUP BY TRIM(ndid), COALESCE(encounter_date, surgery_date), eid )a
	where trim(past_surgical_history) <> '^'
),
social as (
	select
	ndid, encounter_id, encounter_date,
	null as medical_history,
	null as past_surgical_history,
	null as family_history_notes,
	social_history_full from (
	select
		ndid,
		eid as encounter_id,
		COALESCE(encounter_date,social_hist_date) as encounter_date,
		GROUP_CONCAT( CONCAT( COALESCE(social_category, ''), ' ^ ', COALESCE(social_option, ''), ' ', COALESCE(social_notes, '') ) SEPARATOR ' + ') as social_history_full
	from udm_tng.social_history sh
	GROUP BY ndid, eid, COALESCE(encounter_date,social_hist_date)
	)a where trim(social_history_full) <> '^'
),
medical as (
	select
	ndid, encounter_id, encounter_date,
	medical_history,
	null as past_surgical_history,
	null as family_history_notes,
	null as social_history_full from (
	select
		ndid,
		eid as encounter_id,
		COALESCE(encounter_date,med_hist_date) as encounter_date,
		GROUP_CONCAT( CONCAT( COALESCE(med_hist_question, ''), ' ^ ', COALESCE(med_hist_answer, '')) SEPARATOR ' + ') as medical_history
	from udm_tng.medical_history sh
	GROUP BY ndid, eid, COALESCE(encounter_date,med_hist_date)
	)a where trim(medical_history) <> '^'
),
family as (
	select
	ndid, encounter_id, encounter_date,
	null as medical_history,
	null as past_surgical_history,
	family_history_notes,
	null as social_history_full from (
	select
		ndid,
		eid as encounter_id,
		COALESCE(encounter_date,fam_hist_date) as encounter_date,
		GROUP_CONCAT( CONCAT( COALESCE(fam_hist_relation, ''), ' ^ ', COALESCE(fam_hist_detail, '')) SEPARATOR ' + ') as family_history_notes
	from udm_tng.family_history sh
	GROUP BY ndid, eid, COALESCE(encounter_date,fam_hist_date)
	)a where trim(family_history_notes) <> '^'
),
all_keys as (
    SELECT ndid, encounter_date FROM surgical
    UNION
    SELECT ndid, encounter_date FROM social
    UNION
    SELECT ndid, encounter_date FROM medical
    UNION
    SELECT ndid, encounter_date FROM family
)
select
    k.ndid,
    k.encounter_date,
    -- Prioritize encounter_id from surgical, then social, then medical, then family
    COALESCE(s.encounter_id, soc.encounter_id, m.encounter_id, f.encounter_id) AS encounter_id,
    m.medical_history,
    s.past_surgical_history,
    f.family_history_notes,
    soc.social_history_full
from all_keys k
left join surgical s on k.ndid = s.ndid and k.encounter_date = s.encounter_date
left join social soc on k.ndid = soc.ndid and k.encounter_date = soc.encounter_date
left join medical m on k.ndid = m.ndid and k.encounter_date = m.encounter_date
left join family f on k.ndid = f.ndid and k.encounter_date = f.encounter_date)tng;