Select
    acc.BNK_CD,
    acc.ROW_ACTV_IND,
    bra.BNK_NM,
    bra.BRANCH_NM,
    acb.BNK_BRANCH_CD,
    acc.BNK_ACCT_NBR,
    Upper(acc.BNK_ACCT_DESC) As BNK_ACCT_DESC,
    acc.CSH_OFST_FIN_COA_CD,
    acc.CSH_OFST_ACCT_NBR,
    acc.CSH_OFST_OBJ_CD,
    acc.CONT_BNK_CD,
    acc.BNK_DPST_IND,
    acc.BNK_DISB_IND,
    acc.BNK_ACH_IND,
    acc.BNK_CHK_IND
From
    FP_BANK_T acc Inner Join
    FP_BANK_EXT_T acb On acb.BNK_CD = acc.BNK_CD Inner Join
    X000_Banks_branches bra On bra.BRANCH_CD = acb.BNK_BRANCH_CD
Order By
    acc.ROW_ACTV_IND Desc,
    bra.BNK_NM,
    bra.BRANCH_NM