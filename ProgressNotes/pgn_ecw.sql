CREATE TABLE udm_staging.ecw_progressnotes (
    patientID BIGINT,
    encounterID BIGINT,
    enc_date DATE,
    summary LONGTEXT,
    psid INT,
    nd_extracted_date DATE
);

INSERT INTO udm_staging.ecw_progressnotes
SELECT 
    b.patientID,
    a.encounterID,
    date(b.`date`)
    a.summary,
    1,
    CURRENT_DATE()
FROM dent.progressnotes_decryptfinal a
LEFT JOIN dent.enc b 
    ON a.encounterID = b.encounterID;