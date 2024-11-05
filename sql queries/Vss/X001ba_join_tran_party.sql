SELECT
  X001ab_impo_vssparty.IDNO,
  X001aa_impo_vsstran.STUDENT,
  X001ab_impo_vssparty.NAME,
  X001aa_impo_vsstran.YEAR,
  X001aa_impo_vsstran.CAMPUS,
  X001ab_impo_vssparty.FIRSTNAME,
  X001ab_impo_vssparty.INITIALS,
  X001ab_impo_vssparty.SURNAME,
  X001ab_impo_vssparty.PARTY_AUDITDATETIME,
  X001ab_impo_vssparty.PARTY_AUDITUSERCODE
FROM
  X001aa_impo_vsstran
  LEFT JOIN X001ab_impo_vssparty ON X001ab_impo_vssparty.STUDENT = X001aa_impo_vsstran.STUDENT
ORDER BY
  X001aa_impo_vsstran.STUDENT
