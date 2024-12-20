﻿SELECT
  main.STUDQUALFOSRESULT.KBUSINESSENTITYID,
  main.STUDQUALFOSRESULT.KACADEMICPROGRAMID,
  main.STUDQUALFOSRESULT.KQUALFOSRESULTCODEID,
  main.X000_Codedescription.LONG AS RESULT,
  main.STUDQUALFOSRESULT.KRESULTYYYYMM,
  main.STUDQUALFOSRESULT.KSTUDQUALFOSRESULTID,
  main.STUDQUALFOSRESULT.FGRADUATIONCEREMONYID,
  main.STUDQUALFOSRESULT.FPOSTPONEMENTCODEID,
  X000_Codedescription1.LONG AS POSTPONE_REAS,
  main.STUDQUALFOSRESULT.RESULTISSUEDATE,
  main.STUDQUALFOSRESULT.DISCONTINUEDATE,
  main.STUDQUALFOSRESULT.FDISCONTINUECODEID,
  X000_Codedescription2.LONG AS DISCONTINUE_REAS,
  main.STUDQUALFOSRESULT.RESULTPASSDATE,
  main.STUDQUALFOSRESULT.FLANGUAGECODEID,
  main.STUDQUALFOSRESULT.ISSUESURNAME,
  main.STUDQUALFOSRESULT.CERTIFICATESEQNUMBER,
  main.STUDQUALFOSRESULT.AVGMARKACHIEVED,
  main.STUDQUALFOSRESULT.PROCESSSEQNUMBER,
  main.STUDQUALFOSRESULT.FRECEIPTID,
  main.STUDQUALFOSRESULT.FRECEIPTLINEID,
  main.STUDQUALFOSRESULT.ISINABSENTIA,
  main.STUDQUALFOSRESULT.FPROGRAMAPID,
  main.STUDQUALFOSRESULT.FISSUETYPECODEID,
  X000_Codedescription3.LONG AS ISSUE_TYPE,
  main.STUDQUALFOSRESULT.DATEPRINTED,
  main.STUDQUALFOSRESULT.LOCKSTAMP,
  main.STUDQUALFOSRESULT.AUDITDATETIME,
  main.STUDQUALFOSRESULT.FAUDITSYSTEMFUNCTIONID,
  main.STUDQUALFOSRESULT.FAUDITUSERCODE,
  main.STUDQUALFOSRESULT.FAPPROVEDBYCODEID,
  main.STUDQUALFOSRESULT.FAPPROVEDBYUSERCODE,
  main.STUDQUALFOSRESULT.DATERESULTAPPROVED,
  main.STUDQUALFOSRESULT.FENROLMENTPRESENTATIONID,
  main.STUDQUALFOSRESULT.CERTDISPATCHDATE,
  main.STUDQUALFOSRESULT.CERTDISPATCHREFNO,
  main.STUDQUALFOSRESULT.ISSUEFIRSTNAMES
FROM
  main.STUDQUALFOSRESULT
  LEFT JOIN main.X000_Codedescription ON main.X000_Codedescription.KCODEDESCID =
    main.STUDQUALFOSRESULT.KQUALFOSRESULTCODEID
  LEFT JOIN main.X000_Codedescription X000_Codedescription1 ON X000_Codedescription1.KCODEDESCID =
    main.STUDQUALFOSRESULT.FPOSTPONEMENTCODEID
  LEFT JOIN main.X000_Codedescription X000_Codedescription2 ON X000_Codedescription2.KCODEDESCID =
    main.STUDQUALFOSRESULT.FDISCONTINUECODEID
  LEFT JOIN main.X000_Codedescription X000_Codedescription3 ON X000_Codedescription3.KCODEDESCID =
    main.STUDQUALFOSRESULT.FISSUETYPECODEID
ORDER BY
  main.STUDQUALFOSRESULT.KBUSINESSENTITYID,
  main.STUDQUALFOSRESULT.AUDITDATETIME DESC
