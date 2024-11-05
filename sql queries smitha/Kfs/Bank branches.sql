Select
    ban.BNK_CD,
    Upper(ban.BNK_NM) As BNK_NM,
    ban.VER_NBR,
    bra.BRANCH_CD,
    bra.BRANCH_NM,
    bra.VER_NBR As VER_NBR1,
    bra.ACTV_IND
From
    FP_ZA_BANK_T ban Inner Join
    FP_BANK_BRANCH_T bra On bra.BNK_CD = ban.BNK_CD
Order By
    BNK_NM,
    bra.BRANCH_NM