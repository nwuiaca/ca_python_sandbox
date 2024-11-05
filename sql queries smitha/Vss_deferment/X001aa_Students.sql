SELECT
  X000_Students_curr.*,
  X001aa_Trans_balopen.BAL_OPEN,
  X001ad_Trans_balreg.BAL_REG,
  X001ae_Trans_crereg.CRE_REG,
  X001ab_Trans_feereg.FEE_REG
FROM
  X000_Students_curr
  LEFT JOIN X001ad_Trans_balreg ON X001ad_Trans_balreg.STUDENT_VSS = X000_Students_curr.KSTUDBUSENTID
  LEFT JOIN X001ae_Trans_crereg ON X001ae_Trans_crereg.STUDENT_VSS = X000_Students_curr.KSTUDBUSENTID
  LEFT JOIN X001aa_Trans_balopen ON X001aa_Trans_balopen.STUDENT_VSS = X000_Students_curr.KSTUDBUSENTID
  LEFT JOIN X001ab_Trans_feereg ON X001ab_Trans_feereg.STUDENT_VSS = X000_Students_curr.KSTUDBUSENTID
