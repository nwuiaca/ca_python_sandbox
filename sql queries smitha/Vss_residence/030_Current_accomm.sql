﻿SELECT
  ACCOMMRESIDENCY.KACCOMMRESIDENCYID,
  ACCOMMRESIDENCY.FSTUDENTBUSENTID AS STUDENT,
  ACCOMMRESIDENCY.FRESIDENCEID AS RESIDENCEID,
  X010_Active_residence.NAME,
  ACCOMMRESIDENCY.FROOMTYPECODEID,
  X000_Codedescription.SHORT AS ROOM_TYPE,
  ACCOMMRESIDENCY.STARTDATE,
  ACCOMMRESIDENCY.ENDDATE,
  ACCOMMRESIDENCY.FACCOMMCANCELCODEID,
  X000_Codedescription1.LONG,
  ACCOMMRESIDENCY.ACCOMMCANCELREASONOTHER
FROM
  ACCOMMRESIDENCY
  INNER JOIN X000_Codedescription ON X000_Codedescription.KCODEDESCID = ACCOMMRESIDENCY.FROOMTYPECODEID
  LEFT JOIN X000_Codedescription X000_Codedescription1 ON X000_Codedescription1.KCODEDESCID =
    ACCOMMRESIDENCY.FACCOMMCANCELCODEID
  INNER JOIN X010_Active_residence ON X010_Active_residence.RESIDENCEID = ACCOMMRESIDENCY.FRESIDENCEID
WHERE
  ACCOMMRESIDENCY.STARTDATE >= Date("2018-01-01") AND
  ACCOMMRESIDENCY.ENDDATE <= Date("2018-12-31")
ORDER BY
  STUDENT,
  ACCOMMRESIDENCY.AUDITDATETIME