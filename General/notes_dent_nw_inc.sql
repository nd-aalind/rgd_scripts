CREATE TABLE biogen_april.note_inc_may_dent_nw AS
SELECT
    a.ndid,
    a.eid                AS encounter_id,
    a.enc_start_date     AS encounter_date,
    a.note_type,
    a.note_source,
    a.note,
    CAST(NULL AS SIGNED) AS incremental_id,
    null as udm_active_flag,
    null as udm_unq_id
FROM rgd_udm_silver.notes_part2 a
INNER JOIN biogen_april.patients_demo_inc_combined c ON a.ndid = c.ndid
WHERE a.enc_start_date >= '2026-02-16'
  AND a.enc_start_date <= '2026-03-31' and a.psid in (1,4);