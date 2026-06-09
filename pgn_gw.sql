CREATE TABLE udm_staging.gw_progressnotes (
    patientID BIGINT,
    encounterID BIGINT,
    summary LONGTEXT,
    psid INT,
    nd_extracted_date DATE
);

INSERT INTO udm_staging.ecw_progressnotes
SELECT 
    b.patientID,
    a.encounterID,
    a.summary,
    1,
    CURRENT_DATE()
FROM dent.progressnotes_decryptfinal a
LEFT JOIN dent.enc b 
    ON a.encounterID = b.encounterID;