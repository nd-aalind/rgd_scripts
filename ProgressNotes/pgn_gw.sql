insert into rgd_udm_silver.progressnotes_part3
(
ndid,eid,enc_date,notes,psid,nd_extracted_date,Documentid,BINTYPEID
)
SELECT distinct a.Patientid as ndid,b.VisitId as eid,DATE(b.FromDateTime)as enc_date,
a.DOCCONTENT as notes,12 as psid,null nd_extracted_date,a.Documentid
,a.BINTYPEID
from mind. clinicalbin_decrypted_extracteddata a
Join mind.ClinicalDocuments cd on a.Documentid = cd.Documentid
Left join mind.Visit b on cd.VisitId = b.VisitId ;