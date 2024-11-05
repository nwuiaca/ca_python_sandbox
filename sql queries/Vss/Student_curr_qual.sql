﻿SELECT
  main.X104_Stud_curr_qual.KSTUDBUSENTID,
  main.X104_Stud_curr_qual.PROGRAMCODE,
  main.X104_Stud_curr_qual.QUALIFICATIONCODE,
  main.X104_Stud_curr_qual.QUALIFICATIONFIELDOFSTUDY,
  main.X104_Stud_curr_qual.QUALIFICATIONLEVEL,
  main.X104_Stud_curr_qual.ISMAINQUALLEVEL,
  main.X104_Stud_curr_qual.QUAL_TYPE,
  main.X104_Stud_curr_qual.DATEQUALLEVELSTARTED,
  main.X104_Stud_curr_qual.DATEENROL,
  main.X104_Stud_curr_qual.STARTDATE,
  main.X104_Stud_curr_qual.ENDDATE,
  main.X104_Stud_curr_qual.ACTIVE_IND,
  main.X104_Stud_curr_qual.DISCONTINUEDATE,
  main.X104_Stud_curr_qual.DISCONTINUE_REAS,
  main.X104_Stud_curr_qual.RESULT,
  main.X104_Stud_curr_qual.POSTPONE_REAS,
  main.X104_Stud_curr_qual.ENROLACADEMICYEAR,
  main.X104_Stud_curr_qual.ENROLHISTORYYEAR,
  main.X104_Stud_curr_qual.ISHEMISSUBSIDY,
  main.X104_Stud_curr_qual.MIN,
  main.X104_Stud_curr_qual.MIN_UNIT,
  main.X104_Stud_curr_qual.MAX,
  main.X104_Stud_curr_qual.MAX_UNIT,
  main.X104_Stud_curr_qual.ENTRY_LEVEL,
  main.X104_Stud_curr_qual.ENROL_CAT,
  main.X104_Stud_curr_qual.PRESENT_CAT,
  main.X104_Stud_curr_qual.QUAL_LEVEL_STATUS_FINAL,
  main.X104_Stud_curr_qual.QUAL_LEVEL_LEVY_CAT,
  main.X104_Stud_curr_qual.CERT_TYPE,
  main.X104_Stud_curr_qual.LEVY_TYPE,
  main.X104_Stud_curr_qual.BLACKLIST,
  main.X104_Stud_curr_qual.FOS_SELECTION
FROM
  main.X104_Stud_curr_qual
WHERE
  main.X104_Stud_curr_qual.QUAL_TYPE != 'Short Course'
ORDER BY
  main.X104_Stud_curr_qual.KSTUDBUSENTID,
  main.X104_Stud_curr_qual.ISMAINQUALLEVEL DESC