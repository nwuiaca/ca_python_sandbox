﻿SELECT
  PARTY.KBUSINESSENTITYID,
  PARTY.PARTYTYPE,
  PARTY.NAME,
  PARTY.SURNAME,
  PARTY.INITIALS,
  PARTY.FIRSTNAMES,
  PARTY.NICKNAME,
  PARTY.MAIDENNAME,
  PARTY.DATEOFBIRTH,
  PARTY.FTITLECODEID,
  X000_Codedesc_title.LONG AS TITLE,
  X000_Codedesc_title.LANK AS TITEL,
  PARTY.FGENDERCODEID,
  PARTY.FGENDERCODE,
  X000_Codedesc_gender.LONG AS GENDER,
  X000_Codedesc_gender.LANK AS GESLAG,
  PARTY.FNATIONALITYCODEID,
  X000_Codedesc_nationality.LONG AS NATIONALITY,
  X000_Codedesc_nationality.LANK AS NASIONALITEIT,
  PARTY.FPOPULATIONGROUPCODEID,
  X000_Codedesc_population.LONG AS POPULATION,
  X000_Codedesc_population.LANK AS POPULASIE,
  PARTY.FRACECODEID,
  X000_Codedesc_race.LONG AS RACE,
  X000_Codedesc_race.LANK AS RAS,
  PARTY.ISFOREIGN,
  PARTY.CONTACTPERSONNAME,
  PARTY.FRELIGIOUSAFFILIATIONCODEID,
  PARTY.FPREFERREDCORRCODEID,
  PARTY.FPREFACCCORRCODEID,
  PARTY.LOCKSTAMP,
  PARTY.AUDITDATETIME,
  PARTY.FAUDITSYSTEMFUNCTIONID,
  PARTY.FAUDITUSERCODE
FROM
  PARTY
  LEFT JOIN X000_Codedescription X000_Codedesc_title ON X000_Codedesc_title.KCODEDESCID = PARTY.FTITLECODEID
  LEFT JOIN X000_Codedescription X000_Codedesc_gender ON X000_Codedesc_gender.KCODEDESCID = PARTY.FGENDERCODEID
  LEFT JOIN X000_Codedescription X000_Codedesc_nationality ON X000_Codedesc_nationality.KCODEDESCID =
    PARTY.FNATIONALITYCODEID
  LEFT JOIN X000_Codedescription X000_Codedesc_population ON X000_Codedesc_population.KCODEDESCID =
    PARTY.FPOPULATIONGROUPCODEID
  LEFT JOIN X000_Codedescription X000_Codedesc_race ON X000_Codedesc_race.KCODEDESCID = PARTY.FRACECODEID