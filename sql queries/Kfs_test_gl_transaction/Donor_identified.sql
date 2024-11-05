Select
    don.UNIV_FISCAL_YR,
    don.UNIV_FISCAL_PRD_CD,
    don.CALC_COST_STRING,
    don.ORG_NM,
    don.ACCOUNT_NM,
    don.FIN_OBJ_CD_NM,
    don.TRANSACTION_DT,
    don.FDOC_NBR,
    don.CALC_AMOUNT,
    don.TRN_LDGR_ENTR_DESC,
    don.GL_DESCRIPTION,
    don.RECEIPT,
    don.RECEIPT1,
    don.RECEIPT2,
    don.ACCT_TYP_NM,
    don.TRN_POST_DT,
    don."TIMESTAMP",
    don.FIN_COA_CD,
    don.ACCOUNT_NBR,
    don.FIN_OBJECT_CD,
    don.FIN_BALANCE_TYP_CD,
    don.FIN_OBJ_TYP_CD,
    don.FDOC_TYP_CD,
    don.FS_ORIGIN_CD,
    don.FS_DATABASE_DESC,
    don.TRN_ENTR_SEQ_NBR,
    don.RECEIPTDATE,
    don.KRECEIPTID,
    don.NAME,
    don."EXCLUDE",
    don.DONOR_ID,
    own.LOOKUP_DESCRIPTION As DONOR,
    don.UNIQUE_ID
From
    X003ab_donor_receipt don Inner Join
    X000_Own_kfs_lookups own On own.LOOKUP_CODE = don.DONOR_ID
Where
    own.LOOKUP = "IDENTIFY GL DONOR RECORD"