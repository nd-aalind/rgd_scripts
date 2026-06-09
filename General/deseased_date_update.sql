UPDATE 
        rgd_udm_silver.patients rp
        JOIN (SELECt FLOOR(TRIM(pa.PatientID)) ndid,PatDeathFlag
        from mind.Person p
        join mind.Patient pa on p.PersonID = pa.PersonID 
        join rgd_udm_silver.patients rp on rp.ndid = FLOOR(TRIM(pa.PatientID)) -- limit 100000
        where psid =12  ) b ON rp.ndid = b.ndid
        SET rp.pat_deceased_status = PatDeathFlag
        WHERE rp.psid =12 ;