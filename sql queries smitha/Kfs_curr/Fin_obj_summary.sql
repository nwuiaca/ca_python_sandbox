Select
    x000g.FIN_OBJECT_CD,
    x000g.FIN_OBJ_CD_NM,
    Count(x000g.UNIV_FISCAL_YR) As Count_UNIV_FISCAL_YR
From
    X000_GL_trans x000g
Group By
    x000g.FIN_OBJECT_CD,
    x000g.FIN_OBJ_CD_NM