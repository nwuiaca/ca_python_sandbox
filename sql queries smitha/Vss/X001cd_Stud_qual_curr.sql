﻿SELECT
  X001cc_Stud_qual_curr.KSTUDBUSENTID,
  X001cc_Stud_qual_curr.KENROLSTUDID,
  X001cc_Stud_qual_curr.DATEQUALLEVELSTARTED,
  X001cc_Stud_qual_curr.DATEENROL,
  X001cc_Stud_qual_curr.STARTDATE,
  X001cc_Stud_qual_curr.ENDDATE,
  X001cc_Stud_qual_curr.QUALIFICATIONCODE,
  X001cc_Stud_qual_curr.QUALIFICATIONFIELDOFSTUDY,
  X001cc_Stud_qual_curr.QUALIFICATIONLEVEL,
  X001cc_Stud_qual_curr.QUAL_TYPE,
  X001cc_Stud_qual_curr.ISHEMISSUBSIDY,
  X001cc_Stud_qual_curr.ISMAINQUALLEVEL,
  X001cc_Stud_qual_curr.ENROLACADEMICYEAR,
  X001cc_Stud_qual_curr.ENROLHISTORYYEAR,
  X001cc_Stud_qual_curr.MIN,
  X001cc_Stud_qual_curr.MIN_UNIT,
  X001cc_Stud_qual_curr.MAX,
  X001cc_Stud_qual_curr.MAX_UNIT,
  X001cc_Stud_qual_curr.FSTUDACTIVECODEID,
  X001cc_Stud_qual_curr.ACTIVE_IND,
  X001cc_Stud_qual_curr.FENTRYLEVELCODEID,
  X001cc_Stud_qual_curr.ENTRY_LEVEL,
  X001cc_Stud_qual_curr.FENROLMENTCATEGORYCODEID,
  X001cc_Stud_qual_curr.ENROL_CAT,
  X001cc_Stud_qual_curr.FPRESENTATIONCATEGORYCODEID,
  X001cc_Stud_qual_curr.PRESENT_CAT,
  X001cc_Stud_qual_curr.FFINALSTATUSCODEID,
  X001cc_Stud_qual_curr.QUAL_LEVEL_STATUS_FINAL,
  X001cc_Stud_qual_curr.FLEVYCATEGORYCODEID,
  X001cc_Stud_qual_curr.QUAL_LEVEL_LEVY_CAT,
  X001cc_Stud_qual_curr.CERT_TYPE,
  X001cc_Stud_qual_curr.LEVY_TYPE,
  X001cc_Stud_qual_curr.FBLACKLISTCODEID,
  X001cc_Stud_qual_curr.BLACKLIST,
  X001cc_Stud_qual_curr.FSELECTIONCODEID,
  X001cc_Stud_qual_curr.FOS_SELECTION,
  X001cc_Stud_qual_curr.FBUSINESSENTITYID,
  X001cc_Stud_qual_curr.FORGUNITNUMBER,
  X001cc_Stud_qual_curr.ORGUNIT_TYPE,
  X001cc_Stud_qual_curr.ORGUNIT_NAME,
  X001cc_Stud_qual_curr.FSITEORGUNITNUMBER,
  X001cc_Stud_qual_curr.ISCONDITIONALREG,
  X001cc_Stud_qual_curr.MARKSFINALISEDDATE,
  X001cc_Stud_qual_curr.EXAMSUBMINIMUM,
  X001cc_Stud_qual_curr.ISCUMLAUDE,
  X001cc_Stud_qual_curr.FGRADCERTLANGUAGECODEID,
  X001cc_Stud_qual_curr.ISPOSSIBLEGRADUATE,
  X001cc_Stud_qual_curr.FGRADUATIONCEREMONYID,
  X001cc_Stud_qual_curr.FACCEPTANCETESTCODEID,
  X001cc_Stud_qual_curr.FENROLMENTPRESENTATIONID,
  X001cc_Stud_qual_curr.FQUALPRESENTINGOUID,
  X001cc_Stud_qual_curr.FQUALLEVELAPID,
  X001cc_Stud_qual_curr.FFIELDOFSTUDYAPID,
  X001cc_Stud_qual_curr.FPROGRAMAPID,
  X001cc_Stud_qual_curr.FQUALIFICATIONAPID,
  X000_Student_qual_result_curr.KBUSINESSENTITYID,
  X000_Student_qual_result_curr.KACADEMICPROGRAMID,
  X000_Student_qual_result_curr.KQUALFOSRESULTCODEID,
  X000_Student_qual_result_curr.RESULT,
  X000_Student_qual_result_curr.KRESULTYYYYMM,
  X000_Student_qual_result_curr.KSTUDQUALFOSRESULTID,
  X000_Student_qual_result_curr.FGRADUATIONCEREMONYID AS FGRADUATIONCEREMONYID1,
  X000_Student_qual_result_curr.FPOSTPONEMENTCODEID,
  X000_Student_qual_result_curr.POSTPONE_REAS,
  X000_Student_qual_result_curr.RESULTISSUEDATE,
  X000_Student_qual_result_curr.DISCONTINUEDATE,
  X000_Student_qual_result_curr.FDISCONTINUECODEID,
  X000_Student_qual_result_curr.DISCONTINUE_REAS,
  X000_Student_qual_result_curr.RESULTPASSDATE,
  X000_Student_qual_result_curr.FLANGUAGECODEID,
  X000_Student_qual_result_curr.ISSUESURNAME,
  X000_Student_qual_result_curr.CERTIFICATESEQNUMBER,
  X000_Student_qual_result_curr.AVGMARKACHIEVED,
  X000_Student_qual_result_curr.PROCESSSEQNUMBER,
  X000_Student_qual_result_curr.FRECEIPTID,
  X000_Student_qual_result_curr.FRECEIPTLINEID,
  X000_Student_qual_result_curr.ISINABSENTIA,
  X000_Student_qual_result_curr.FPROGRAMAPID AS FPROGRAMAPID1,
  X000_Student_qual_result_curr.FISSUETYPECODEID,
  X000_Student_qual_result_curr.ISSUE_TYPE,
  X000_Student_qual_result_curr.DATEPRINTED,
  X000_Student_qual_result_curr.LOCKSTAMP,
  X000_Student_qual_result_curr.AUDITDATETIME,
  X000_Student_qual_result_curr.FAUDITSYSTEMFUNCTIONID,
  X000_Student_qual_result_curr.FAUDITUSERCODE,
  X000_Student_qual_result_curr.FAPPROVEDBYCODEID,
  X000_Student_qual_result_curr.FAPPROVEDBYUSERCODE,
  X000_Student_qual_result_curr.DATERESULTAPPROVED,
  X000_Student_qual_result_curr.FENROLMENTPRESENTATIONID AS FENROLMENTPRESENTATIONID1,
  X000_Student_qual_result_curr.CERTDISPATCHDATE,
  X000_Student_qual_result_curr.CERTDISPATCHREFNO,
  X000_Student_qual_result_curr.ISSUEFIRSTNAMES
FROM
  X001cc_Stud_qual_curr
  LEFT JOIN X000_Student_qual_result_curr ON X000_Student_qual_result_curr.KBUSINESSENTITYID =
    X001cc_Stud_qual_curr.KSTUDBUSENTID AND X000_Student_qual_result_curr.FPROGRAMAPID =
    X001cc_Stud_qual_curr.FPROGRAMAPID AND X000_Student_qual_result_curr.KACADEMICPROGRAMID =
    X001cc_Stud_qual_curr.FFIELDOFSTUDYAPID
