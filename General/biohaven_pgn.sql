create table biohaven.dent_progressnotes_20260423 as 
SELECT 
    a.*
FROM dent.progressnotes_decryptfinal a
LEFT JOIN dent.enc b 
    ON a.encounterID = b.encounterID
inner join biohaven.pateint_list_21apr c on b.patientid = c.patientid;