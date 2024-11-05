SELECT
  X001ca_join_prev_reported.IDNO,
  X001ca_join_prev_reported.STUDENT,
  X001ca_join_prev_reported.NAME,
  X001ca_join_prev_reported.YEAR,
  X001ca_join_prev_reported.CAMPUS,
  X001ca_join_prev_reported.FIRSTNAME,
  X001ca_join_prev_reported.INITIALS,
  X001ca_join_prev_reported.SURNAME,
  StrfTime('%m', X001ca_join_prev_reported.DATE_REPORTED) AS MONTH
FROM
  X001ca_join_prev_reported
