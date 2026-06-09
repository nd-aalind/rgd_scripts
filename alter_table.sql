SET @rownum := 0;

UPDATE kinsula_leq.notes_part2
SET udm_inc_id_new = (@rownum := @rownum + 1);