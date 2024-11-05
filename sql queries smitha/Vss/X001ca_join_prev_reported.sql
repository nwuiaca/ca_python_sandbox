SELECT
  X001ba_join_tran_vss.IDNO,
  X001ba_join_tran_vss.STUDENT,
  X001ba_join_tran_vss.NAME,
  X001ba_join_tran_vss.YEAR,
  X001ba_join_tran_vss.CAMPUS,
  X001ba_join_tran_vss.FIRSTNAME,
  X001ba_join_tran_vss.INITIALS,
  X001ba_join_tran_vss.SURNAME,
  X001ba_join_tran_vss.PARTY_AUDITDATETIME,
  X001ba_join_tran_vss.PARTY_AUDITUSERCODE,
  X000_Prev_reported.PROCESS AS PREV_PROCESS,
  X000_Prev_reported.DATE_REPORTED AS PREV_DATE_REPORTED,
  X000_Prev_reported.DATE_RETEST AS PREV_DATE_RETEST
FROM
  X001ba_join_tran_vss
  LEFT JOIN X000_Prev_reported ON X000_Prev_reported.FIELD1 = X001ba_join_tran_vss.STUDENT AND
    X000_Prev_reported.PROCESS = 'idno_list'
