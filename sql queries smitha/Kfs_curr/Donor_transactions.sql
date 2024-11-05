Select
    gl.UNIV_FISCAL_YR,
    gl.UNIV_FISCAL_PRD_CD,
    gl.CALC_COST_STRING,
    gl.ORG_NM,
    gl.ACCOUNT_NM,
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
    (gl.FIN_OBJECT_CD In ('6251', '5754', '5752') And
        gl.FIN_OBJ_TYP_CD = 'IN' And
        gl.CALC_AMOUNT < 0) Or
    (gl.FIN_OBJECT_CD In ('7502', '7503') And
        gl.CALC_AMOUNT < 0 And
        gl.FDOC_TYP_CD = 'APP')