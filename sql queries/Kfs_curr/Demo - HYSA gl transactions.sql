Select
    gl.UNIV_FISCAL_YR,
    gl.UNIV_FISCAL_PRD_CD,
    gl.ACCOUNT_NM,
    gl.CALC_COST_STRING,
    gl.FIN_OBJ_CD_NM,
    gl.TRANSACTION_DT,
    gl.FDOC_NBR,
    gl.CALC_AMOUNT,
    gl.TRN_LDGR_ENTR_DESC,
    gl.ACCT_TYP_NM,
    gl.TRN_POST_DT,
    gl."TIMESTAMP",
    gl.FIN_COA_CD,
    gl.ACCOUNT_NBR,
    gl.FIN_OBJECT_CD,
    gl.FIN_BALANCE_TYP_CD,
    gl.FIN_OBJ_TYP_CD,
    gl.FDOC_TYP_CD,
    gl.FS_ORIGIN_CD,
    gl.FS_DATABASE_DESC,
    gl.TRN_ENTR_SEQ_NBR,
    gl.FDOC_REF_TYP_CD,
    gl.FS_REF_ORIGIN_CD,
    gl.FDOC_REF_NBR,
    gl.FDOC_REVERSAL_DT,
    gl.TRN_ENCUM_UPDT_CD
From
    X000_GL_trans gl
Where
    gl.ACCOUNT_NM Like ("(8417%") And
    gl.FIN_BALANCE_TYP_CD = "AC"