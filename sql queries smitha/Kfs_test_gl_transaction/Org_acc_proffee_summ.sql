Select
    X001_gl_professional_fee_pay.ORG_NM,
    X001_gl_professional_fee_pay.ACCOUNT_NM,
    Total(X001_gl_professional_fee_pay.CALC_AMOUNT) As Total_CALC_AMOUNT
From
    X001_gl_professional_fee_pay
Group By
    X001_gl_professional_fee_pay.ORG_NM,
    X001_gl_professional_fee_pay.ACCOUNT_NM