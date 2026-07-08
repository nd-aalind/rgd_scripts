create table reporting_raleigh.patient_radiology_v2 as 
    SELECT
        d.patientid                                                AS patientid,
        cr.CLINICALRESULTID                                         AS result_id,
        COALESCE(d.CHARTID, ce.CHARTID)                            AS ndid,
        d.CLINICALENCOUNTERID                                       AS eid,
        DATE(ce.ENCOUNTERDATE)                                      AS enc_date,
        ce.CLINICALENCOUNTERID                                      AS enc_id,
        cr.CREATEDDATETIME                                          AS created_datetime,
        d.ORDERDATETIME                                             AS order_date,
        d.OBSERVATIONDATETIME                                       AS perform_date,
        CASE
            WHEN cr.OBSERVATIONDATETIME IS NULL THEN NULL
            WHEN cr.OBSERVATIONDATETIME REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
                THEN DATE(STR_TO_DATE(cr.OBSERVATIONDATETIME, '%Y-%m-%d'))
            WHEN cr.OBSERVATIONDATETIME REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}$'
                THEN DATE(STR_TO_DATE(cr.OBSERVATIONDATETIME, '%Y-%m-%d %H:%i:%s'))
            WHEN cr.OBSERVATIONDATETIME REGEXP '^[0-9]{2}-[0-9]{2}-[0-9]{4}$'
                THEN DATE(STR_TO_DATE(cr.OBSERVATIONDATETIME, '%m-%d-%Y'))
            WHEN cr.OBSERVATIONDATETIME REGEXP '^[0-9]{2}-[0-9]{2}-[0-9]{4} [0-9]{2}:[0-9]{2}:[0-9]{2}$'
                THEN DATE(STR_TO_DATE(cr.OBSERVATIONDATETIME, '%m-%d-%Y %H:%i:%s'))
            ELSE NULL
        END                                                         AS img_date,
        cr.CLINICALORDERTYPE                                        AS study_name,
        cr.CLINICALORDERGENUS                                       AS modality,
        cr.RESULTSTATUS                                             AS result_status,
        (
            SELECT cro_s.RESULTSTATUS
            FROM athenaone.CLINICALRESULTOBSERVATION cro_s
            WHERE cro_s.CLINICALRESULTID = cr.CLINICALRESULTID
              AND cro_s.nd_active_flag = 'Y'
            ORDER BY cro_s.ORDERING ASC
            LIMIT 1
        )                                                           AS img_status,
        cr.REPORTSTATUS                                             AS order_status,
        COALESCE(
            d.DOCUMENTTEXTDATA,
            d.RESULTNOTES,
            (
                SELECT GROUP_CONCAT(cro_f.RESULT ORDER BY cro_f.ORDERING SEPARATOR '\n')
                FROM athenaone.CLINICALRESULTOBSERVATION cro_f
                WHERE cro_f.CLINICALRESULTID = cr.CLINICALRESULTID
                  AND cro_f.nd_active_flag = 'Y'
                  AND cro_f.RESULT IS NOT NULL
            )
        )                                                           AS img_finding,
        COALESCE(d.DOCUMENTTEXTDATA, d.RESULTNOTES)                AS img_report_text,
        d.DOCUMENTID                                                AS report_id,
        cr.ORDERDOCUMENTID                                          AS order_id,
        CASE
            WHEN cr.OBSERVATIONDATETIME IS NULL THEN NULL
            WHEN cr.OBSERVATIONDATETIME REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
                THEN DATE(STR_TO_DATE(cr.OBSERVATIONDATETIME, '%Y-%m-%d'))
            WHEN cr.OBSERVATIONDATETIME REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}$'
                THEN DATE(STR_TO_DATE(cr.OBSERVATIONDATETIME, '%Y-%m-%d %H:%i:%s'))
            WHEN cr.OBSERVATIONDATETIME REGEXP '^[0-9]{2}-[0-9]{2}-[0-9]{4}$'
                THEN DATE(STR_TO_DATE(cr.OBSERVATIONDATETIME, '%m-%d-%Y'))
            WHEN cr.OBSERVATIONDATETIME REGEXP '^[0-9]{2}-[0-9]{2}-[0-9]{4} [0-9]{2}:[0-9]{2}:[0-9]{2}$'
                THEN DATE(STR_TO_DATE(cr.OBSERVATIONDATETIME, '%m-%d-%Y %H:%i:%s'))
            ELSE NULL
        END                                                         AS report_date,
        d.STATUS                                                    AS report_status,
        cr.ORDERDOCUMENTID                                          AS order_prescription,
        cr.CLINICALPROVIDERID                                       AS provider_id,
        COALESCE(
            (
                SELECT prov.BILLEDNAME
                FROM athenaone.PROVIDER prov
                WHERE prov.PROVIDERID = cp.PROVIDERID
                  AND prov.nd_active_flag = 'Y'
                ORDER BY prov.CREATEDDATETIME DESC
                LIMIT 1
            ),
            cp.NAME
        )                                                           AS facility_name,
        cp.NPI                                                      AS provider_npi,
        CONCAT(
            COALESCE(
                (
                    SELECT GROUP_CONCAT(cro_n.OBSERVATIONNOTE ORDER BY cro_n.ORDERING SEPARATOR ' | ')
                    FROM athenaone.CLINICALRESULTOBSERVATION cro_n
                    WHERE cro_n.CLINICALRESULTID = cr.CLINICALRESULTID
                      AND cro_n.nd_active_flag = 'Y'
                      AND cro_n.OBSERVATIONNOTE IS NOT NULL
                ),
                ''
            ),
            ' - ',
            COALESCE(d.PROVIDERNOTE, '')
        )                                                           AS internal_notes,
        cr.EXTERNALNOTE                                             AS note_to_patient,
        d.DEPARTMENTID                                              AS facility,
        d.INTERNALINTERPRETATION                                    AS interpretation,
        d.SOURCE                                                    AS source,
        (
            SELECT GROUP_CONCAT(
                DISTINCT CONCAT(
                    COALESCE(ica_s.DIAGNOSISCODE, ''),
                    ' - ',
                    COALESCE(ica_s.DIAGNOSISCODEDESCRIPTION, '')
                )
                ORDER BY ddicd_s.ORDERING
                SEPARATOR ' | '
            )
            FROM athenaone.DOCUMENTDIAGNOSIS dd_s
            JOIN athenaone.DOCUMENTDIAGNOSISICD10 ddicd_s
                ON dd_s.DOCUMENTDIAGNOSISID = ddicd_s.DOCUMENTDIAGNOSISID
                AND ddicd_s.nd_active_flag = 'Y'
            JOIN athenaone.ICDCODEALL ica_s
                ON ddicd_s.ICDCODEID = ica_s.ICDCODEID
                AND ica_s.ISDELETED = FALSE
            WHERE dd_s.DOCUMENTID = d.DOCUMENTID
              AND dd_s.nd_active_flag = 'Y'
        )                                                           AS diagnosis_codes,
        COALESCE(d.DOCUMENTTEXTDATA, d.RESULTNOTES)                AS report_text
    FROM athenaone.CLINICALRESULT cr
    JOIN athenaone.DOCUMENT d
        ON cr.DOCUMENTID = d.DOCUMENTID
        AND d.nd_active_flag = 'Y'
    LEFT JOIN athenaone.CLINICALENCOUNTER ce
        ON d.CHARTID = ce.CHARTID
        AND d.CLINICALENCOUNTERID = ce.CLINICALENCOUNTERID
        AND ce.nd_active_flag = 'Y'
    LEFT JOIN athenaone.CLINICALPROVIDER cp
        ON cr.CLINICALPROVIDERID = cp.CLINICALPROVIDERID
        AND cp.nd_active_flag = 'Y'
    WHERE cr.CLINICALORDERTYPEGROUP = 'IMAGING'
      AND cr.nd_active_flag = 'Y';