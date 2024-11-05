﻿SELECT
  main.QUALLEVELENROLSTUD_CURR.KSTUDBUSENTID,
  main.QUALLEVELENROLSTUD_CURR.KENROLSTUDID,
  main.QUALLEVELENROLSTUD_CURR.STARTDATE,
  main.QUALLEVELENROLSTUD_CURR.ENDDATE,
  main.QUALLEVELENROLSTUD_CURR.DATEENROL,
  main.QUALLEVELENROLSTUD_CURR.FENROLMENTPRESENTATIONID,
  main.QUALLEVELENROLSTUD_CURR.FGRADUATIONCEREMONYID,
  main.QUALLEVELENROLSTUD_CURR.FBLACKLISTCODEID,
  main.QUALLEVELENROLSTUD_CURR.ISHEMISSUBSIDY,
  main.QUALLEVELENROLSTUD_CURR.ISMAINQUALLEVEL,
  main.QUALLEVELENROLSTUD_CURR.ENROLACADEMICYEAR,
  main.QUALLEVELENROLSTUD_CURR.ENROLHISTORYYEAR,
  main.QUALLEVELENROLSTUD_CURR.FACCEPTANCETESTCODEID,
  main.QUALLEVELENROLSTUD_CURR.DATEQUALLEVELSTARTED,
  main.QUALLEVELENROLSTUD_CURR.ISPOSSIBLEGRADUATE,
  main.QUALLEVELENROLSTUD_CURR.FSTUDACTIVECODEID,
  main.QUALLEVELENROLSTUD_CURR.FENTRYLEVELCODEID,
  main.QUALLEVELENROLSTUD_CURR.FGRADCERTLANGUAGECODEID,
  main.QUALLEVELENROLSTUD_CURR.ISCONDITIONALREG,
  main.QUALLEVELENROLSTUD_CURR.ISCUMLAUDE,
  main.QUALLEVELENROLSTUD_CURR.MARKSFINALISEDDATE,
  main.PRESENTOUENROLPRESENTCAT.FQUALPRESENTINGOUID,
  main.PRESENTOUENROLPRESENTCAT.FENROLMENTCATEGORYCODEID,
  main.PRESENTOUENROLPRESENTCAT.FPRESENTATIONCATEGORYCODEID,
  main.PRESENTOUENROLPRESENTCAT.EXAMSUBMINIMUM,
  main.X000_Codedescription.LONG AS BLACKLIST,
  X000_Codedescription1.LONG AS ACTIVE_IND,
  X000_Codedescription2.LONG AS ENTRY_LEVEL,
  X000_Codedescription3.LONG AS ENROL_CAT,
  X000_Codedescription4.LONG AS PRESENT_CAT,
  main.QUALLEVELENROLSTUD_CURR.FPROGRAMAPID
FROM
  main.QUALLEVELENROLSTUD_CURR
  LEFT JOIN main.PRESENTOUENROLPRESENTCAT ON main.PRESENTOUENROLPRESENTCAT.KENROLMENTPRESENTATIONID =
    main.QUALLEVELENROLSTUD_CURR.FENROLMENTPRESENTATIONID
  LEFT JOIN main.X000_Codedescription ON main.X000_Codedescription.KCODEDESCID =
    main.QUALLEVELENROLSTUD_CURR.FBLACKLISTCODEID
  LEFT JOIN main.X000_Codedescription X000_Codedescription1 ON X000_Codedescription1.KCODEDESCID =
    main.QUALLEVELENROLSTUD_CURR.FSTUDACTIVECODEID
  LEFT JOIN main.X000_Codedescription X000_Codedescription2 ON X000_Codedescription2.KCODEDESCID =
    main.QUALLEVELENROLSTUD_CURR.FENTRYLEVELCODEID
  LEFT JOIN main.X000_Codedescription X000_Codedescription3 ON X000_Codedescription3.KCODEDESCID =
    main.PRESENTOUENROLPRESENTCAT.FENROLMENTCATEGORYCODEID
  INNER JOIN main.X000_Codedescription X000_Codedescription4 ON X000_Codedescription4.KCODEDESCID =
    main.PRESENTOUENROLPRESENTCAT.FPRESENTATIONCATEGORYCODEID