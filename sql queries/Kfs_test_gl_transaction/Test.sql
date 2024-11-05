Select
    GL.ORG,
    GL.ACCOUNT_NM,
    GL.CALC_COST_STRING,
    GL.FIN_OBJ_CD_NM,
    cast(BEG.Total_CALC_AMOUNT As Int) As BBUDGET,
    cast(EXT.Total_CALC_AMOUNT As Int) As EXTERNAL,
    cast(ACT.Total_CALC_AMOUNT As Int) As ACTUAL,
    cast(BUD.Total_CALC_AMOUNT As Int) As BUDGET
From
    X002aa_ia_actual_vs_budget GL Left Join
    X002aa_ia_actual_vs_budget ACT On ACT.CALC_COST_STRING = GL.CALC_COST_STRING
            And ACT.FIN_BALANCE_TYP_CD = "AC" Left Join
    X002aa_ia_actual_vs_budget BUD On BUD.CALC_COST_STRING = GL.CALC_COST_STRING
           And BUD.FIN_BALANCE_TYP_CD = "CB" Left Join
    X002aa_ia_actual_vs_budget EXT On EXT.CALC_COST_STRING = GL.CALC_COST_STRING
            And EXT.FIN_BALANCE_TYP_CD = "EX" Left join
    X002aa_ia_actual_vs_budget BEG On BEG.CALC_COST_STRING = GL.CALC_COST_STRING
            And BEG.FIN_BALANCE_TYP_CD = "BB"
            
Group By
    GL.CALC_COST_STRING
Order By
    GL.FIN_OBJ_CD_NM