INSERT INTO udm_staging.notes
(
  ndid,
  eid,
  enc_start_date,
  note,
  note_type,
  note_source,
  created_datetime,
  created_by,
  ehr_source_name,
  source_path,
  data_type,
  psid,
  nd_extracted_date
)
WITH encounter AS (
    SELECT 
        CAST(e.patientid AS SIGNED)      AS ndid,
        CAST(e.encounterID AS SIGNED)    AS eid,
        DATE(e.date)                     AS enc_start_date
    FROM enc e
)
SELECT
    a.ndid,
    a.eid,
    a.enc_start_date,
    a.note            AS note,
    a.note_type      AS note_type,
    CAST(a.note_source AS CHAR(26))     AS note_source,
    CURRENT_TIMESTAMP()                AS created_datetime,
    'ND'                               AS created_by,
    'eCW'                              AS ehr_source_name,
    'bronze_table'                     AS source_path,
    'Structured'                       AS data_type,
    1                                  AS psid,
    null                     AS nd_extracted_date
FROM (
    SELECT e.ndid, e.eid, e.enc_start_date, ed.currentmedication AS note,
           'currentmedication' AS note_type, 'encounterdata' AS note_source
    FROM encounter e
    JOIN encounterdata ed ON e.eid = ed.encounterID
    WHERE ed.currentmedication IS NOT NULL AND LENGTH(ed.currentmedication) > 0
    UNION ALL
    SELECT e.ndid, e.eid, e.enc_start_date, ed.AsmtNotes,
           'AsmtNotes', 'encounterdata'
    FROM encounter e
    JOIN encounterdata ed ON e.eid = ed.encounterID
    WHERE ed.AsmtNotes IS NOT NULL AND LENGTH(ed.AsmtNotes) > 0
    UNION ALL
    SELECT e.ndid, e.eid, e.enc_start_date, ed.ChiefComplaint,
           'ChiefComplaint', 'encounterdata'
    FROM encounter e
    JOIN encounterdata ed ON e.eid = ed.encounterID
    WHERE ed.ChiefComplaint IS NOT NULL AND LENGTH(ed.ChiefComplaint) > 0
    UNION ALL
    SELECT e.ndid, e.eid, e.enc_start_date, ed.HPINotes,
           'HPINotes', 'encounterdata'
    FROM encounter e
    JOIN encounterdata ed ON e.eid = ed.encounterID
    WHERE ed.HPINotes IS NOT NULL AND LENGTH(ed.HPINotes) > 0
    UNION ALL
    SELECT e.ndid, e.eid, e.enc_start_date, ed.ExamNotes,
           'ExamNotes', 'encounterdata'
    FROM encounter e
    JOIN encounterdata ed ON e.eid = ed.encounterID
    WHERE ed.ExamNotes IS NOT NULL AND LENGTH(ed.ExamNotes) > 0
    UNION ALL
    SELECT e.ndid, e.eid, e.enc_start_date, ed.TreatNotes,
           'TreatNotes', 'encounterdata'
    FROM encounter e
    JOIN encounterdata ed ON e.eid = ed.encounterID
    WHERE ed.TreatNotes IS NOT NULL AND LENGTH(ed.TreatNotes) > 0
    UNION ALL
    SELECT e.ndid, e.eid, e.enc_start_date, ed.PastHistory,
           'PastHistory', 'encounterdata'
    FROM encounter e
    JOIN encounterdata ed ON e.eid = ed.encounterID
    WHERE ed.PastHistory IS NOT NULL AND LENGTH(ed.PastHistory) > 0
    UNION ALL
    SELECT e.ndid, e.eid, e.enc_start_date, n.notes,
           'notes', 'notes'
    FROM encounter e
    JOIN notes n ON e.eid = n.encounterID
    WHERE n.notes IS NOT NULL AND LENGTH(n.notes) > 0
    UNION ALL
    SELECT e.ndid, e.eid, e.enc_start_date, t.message,
           'message', 'telenc'
    FROM encounter e
    JOIN telenc t ON e.eid = t.encounterID
    WHERE t.message IS NOT NULL AND LENGTH(t.message) > 0
    UNION ALL
    SELECT e.ndid, e.eid, e.enc_start_date, t.actiontaken,
           'actiontaken', 'telenc'
    FROM encounter e
    JOIN telenc t ON e.eid = t.encounterID
    WHERE t.actiontaken IS NOT NULL AND LENGTH(t.actiontaken) > 0
    UNION ALL
    SELECT e.ndid, e.eid, e.enc_start_date, tn.Notes,
           'TreatNotes', 'treatmentnotes'
    FROM encounter e
    JOIN treatmentnotes tn ON e.eid = tn.encounterID
    WHERE tn.Notes IS NOT NULL AND LENGTH(tn.Notes) > 0
    UNION ALL
    SELECT e.ndid, e.eid, e.enc_start_date, pi.notes,
           'pt_instructions_notes', 'ptinstruction'
    FROM encounter e
    JOIN ptinstruction pi ON e.eid = pi.encounterID
    WHERE pi.notes IS NOT NULL AND LENGTH(pi.notes) > 0
    UNION ALL
    SELECT e.ndid, e.eid, e.enc_start_date, ea.addendum,
           'addendum', 'encaddendums'
    FROM encounter e
    JOIN encaddendums ea ON e.eid = ea.encounterID
    WHERE ea.addendum IS NOT NULL AND LENGTH(ea.addendum) > 0
    UNION ALL
    SELECT e.ndid, e.eid, e.enc_start_date, h.notes,
           'HPINotes', 'hpi'
    FROM encounter e
    JOIN hpi h ON e.eid = h.encounterID
    WHERE h.notes IS NOT NULL AND LENGTH(h.notes) > 0
    UNION ALL
    SELECT e.ndid, e.eid, e.enc_start_date, enc.notes,
           'notes', 'encounters'
    FROM encounter e
    JOIN encounters enc ON e.eid = enc.encounterID
    WHERE enc.notes IS NOT NULL AND LENGTH(enc.notes) > 0
    UNION ALL
    SELECT e.ndid, e.eid, e.enc_start_date, an.notes,
           'notes', 'annualnotes'
    FROM encounter e
    JOIN annualnotes an ON e.eid = an.encounterID
    WHERE an.notes IS NOT NULL AND LENGTH(an.notes) > 0
    UNION ALL
    SELECT e.ndid, e.eid, e.enc_start_date, ps.value,
           'value', 'procedurespl'
    FROM encounter e
    JOIN procedurespl ps ON e.eid = ps.encounterID
    WHERE ps.value IS NOT NULL AND LENGTH(ps.value) > 0
    UNION ALL
    SELECT e.ndid, e.eid, e.enc_start_date, sd.data,
           'data', 'structured_data'
    FROM encounter e
    JOIN structured_data sd ON e.eid = sd.encounterID
    WHERE sd.data IS NOT NULL AND LENGTH(sd.data) > 0
    UNION ALL
    SELECT e.ndid, e.eid, e.enc_start_date, inn.notes,
           'notes', 'interactionnotes'
    FROM encounter e
    JOIN interactionnotes inn ON e.eid = inn.encounterID
    WHERE inn.notes IS NOT NULL AND LENGTH(inn.notes) > 0
    UNION ALL
    SELECT e.ndid, e.eid, e.enc_start_date, inn.provideraction,
           'provideraction', 'interactionnotes'
    FROM encounter e
    JOIN interactionnotes inn ON e.eid = inn.encounterID
    WHERE inn.provideraction IS NOT NULL AND LENGTH(inn.provideraction) > 0
    UNION ALL
    SELECT e.ndid, e.eid, e.enc_start_date, shpi.value,
           'value', 'structhpi'
    FROM encounter e
    JOIN structhpi shpi ON e.eid = shpi.encounterID
    WHERE shpi.value IS NOT NULL AND LENGTH(shpi.value) > 0
    UNION ALL
    SELECT e.ndid, e.eid, e.enc_start_date, shpi.notes,
           'notes', 'structhpi'
    FROM encounter e
    JOIN structhpi shpi ON e.eid = shpi.encounterID
    WHERE shpi.notes IS NOT NULL AND LENGTH(shpi.notes) > 0
    UNION ALL
    SELECT e.ndid, e.eid, e.enc_start_date, edi.curentdelayptrecovery,
           'curentdelayptrecovery', 'edi_dfr_info'
    FROM encounter e
    JOIN edi_dfr_info edi ON e.eid = edi.encounterID
    WHERE edi.curentdelayptrecovery IS NOT NULL
      AND LENGTH(edi.curentdelayptrecovery) > 0
    UNION ALL
    SELECT e.ndid, e.eid, e.enc_start_date, edi.subcomplaint,
           'subcomplaint', 'edi_dfr_info'
    FROM encounter e
    JOIN edi_dfr_info edi ON e.eid = edi.encounterID
    WHERE edi.subcomplaint IS NOT NULL
      AND LENGTH(edi.subcomplaint) > 0
) a;