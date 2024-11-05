﻿SELECT
  TRANSMASTER.KTRANSMASTERID,
  TRANSMASTER.TRANSCODE,
  TRANSMASTER.FBUSAREAID,
  TRANSMASTERDESC_E.DESCRIP AS DESCRIPTION_E,
  TRANSMASTERDESC_A.DESCRIP AS DESCRIPTION_A,
  TRANSMASTER.FENROLCHECKCODEID,
  X000_CODEDESC_ENROLCHECK.LONG AS ENROL_CHECK_E,
  X000_CODEDESC_ENROLCHECK.LANK AS ENROL_CHECK_A,
  TRANSMASTER.FSUBACCTYPECODEID,
  X000_CODEDESC_SUBACCTYPE.LONG AS SYBACCTYPE_E,
  X000_CODEDESC_SUBACCTYPE.LANK AS SUBACCTYPE_A,
  TRANSMASTER.FAGEANALYSISCTCODEID,
  X000_CODEDESC_AGEANALYSIS.LONG AS AGEANAL_E,
  X000_CODEDESC_AGEANALYSIS.LANK AS AGEANAL_A,
  TRANSMASTER.FGENERALLEDGERTYPECODEID,
  X000_CODEDESC_GLTYPE.LONG AS GLTYPE_E,
  X000_CODEDESC_GLTYPE.LANK AS GLTYPE_A,
  TRANSMASTER.FTRANSGROUPCODEID,
  X000_CODEDESC_TRANSGROUP.LONG AS TRANGROUP_E,
  X000_CODEDESC_TRANSGROUP.LANK AS TRANGROUP_A,
  TRANSMASTER.STARTDATE,
  TRANSMASTER.ENDDATE,
  TRANSMASTER.FREBATETRANSTYPECODEID,
  TRANSMASTER.FINSTALLMENTCODEID,
  TRANSMASTER.FSUBSYSTRANSCODEID,
  TRANSMASTER.ISPERMITTEDTOCREATEMANUALLY,
  TRANSMASTER.ISSUMMARISED,
  TRANSMASTER.ISCONSOLIDATIONNEEDED,
  TRANSMASTER.ISEXTERNALTRANS,
  TRANSMASTER.ISONLYDEBITSSHOWN,
  TRANSMASTER.ISDEBTEXCLUDED,
  TRANSMASTER.ISMISCELLANEOUS,
  TRANSMASTER.LOCKSTAMP,
  TRANSMASTER.AUDITDATETIME,
  TRANSMASTER.FAUDITSYSTEMFUNCTIONID,
  TRANSMASTER.FAUDITUSERCODE,
  TRANSMASTER.ISMAF,
  TRANSMASTER.ISNONREGSTUDENTALLOWED
FROM
  TRANSMASTER
  LEFT JOIN TRANSMASTERDESC TRANSMASTERDESC_A ON TRANSMASTERDESC_A.KTRANSMASTERID = TRANSMASTER.KTRANSMASTERID
    AND TRANSMASTERDESC_A.KSYSLANGUAGECODEID = '2'
  LEFT JOIN TRANSMASTERDESC TRANSMASTERDESC_E ON TRANSMASTERDESC_E.KTRANSMASTERID = TRANSMASTER.KTRANSMASTERID
    AND TRANSMASTERDESC_E.KSYSLANGUAGECODEID = '3'
  LEFT JOIN X000_Codedescription X000_CODEDESC_ENROLCHECK ON X000_CODEDESC_ENROLCHECK.KCODEDESCID =
    TRANSMASTER.FENROLCHECKCODEID
  LEFT JOIN X000_Codedescription X000_CODEDESC_SUBACCTYPE ON X000_CODEDESC_SUBACCTYPE.KCODEDESCID =
    TRANSMASTER.FSUBACCTYPECODEID
  LEFT JOIN X000_Codedescription X000_CODEDESC_AGEANALYSIS ON X000_CODEDESC_AGEANALYSIS.KCODEDESCID =
    TRANSMASTER.FAGEANALYSISCTCODEID
  LEFT JOIN X000_Codedescription X000_CODEDESC_GLTYPE ON X000_CODEDESC_GLTYPE.KCODEDESCID =
    TRANSMASTER.FGENERALLEDGERTYPECODEID
  LEFT JOIN X000_Codedescription X000_CODEDESC_TRANSGROUP ON X000_CODEDESC_TRANSGROUP.KCODEDESCID =
    TRANSMASTER.FTRANSGROUPCODEID
