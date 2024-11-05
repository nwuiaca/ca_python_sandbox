Select
    X000_GL_trans.ACCOUNT_NBR,
    X000_GL_trans.ACCOUNT_NM,
    Total(X000_GL_trans.CALC_AMOUNT) As Total_CALC_AMOUNT
From
    X000_GL_trans
Where
    (X000_GL_trans.FIN_OBJECT_CD = '7551') Or
    (X000_GL_trans.FIN_OBJECT_CD = '7552') Or
    (X000_GL_trans.FIN_OBJECT_CD = '7553')
Group By
    X000_GL_trans.ACCOUNT_NBR