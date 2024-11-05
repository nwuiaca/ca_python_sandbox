Select
    X000_GL_trans.ACCOUNT_NM,
    X000_GL_trans.CALC_COST_STRING,
    X000_GL_trans.FIN_OBJ_CD_NM,
    X000_GL_trans.FIN_BALANCE_TYP_CD,
    Count(X000_GL_trans.FDOC_NBR) As Count_FDOC_NBR,
    Total(X000_GL_trans.CALC_AMOUNT) As Total_CALC_AMOUNT
From
    X000_GL_trans
Where
    (X000_GL_trans.ACCOUNT_NM Like ("(4532%") And
        X000_GL_trans.FIN_BALANCE_TYP_CD = "AC") Or
    (X000_GL_trans.ACCOUNT_NM Like ("(4532%") And
        X000_GL_trans.FIN_BALANCE_TYP_CD = "CB")
Group By
    X000_GL_trans.ACCOUNT_NM,
    X000_GL_trans.CALC_COST_STRING,
    X000_GL_trans.FIN_OBJ_CD_NM,
    X000_GL_trans.FIN_BALANCE_TYP_CD
Order By
    X000_GL_trans.ACCOUNT_NM,
    X000_GL_trans.FIN_OBJ_CD_NM