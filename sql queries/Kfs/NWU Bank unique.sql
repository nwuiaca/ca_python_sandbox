Select
    ban.BNK_NM,
    ban.BRANCH_NM,
    ban.BNK_BRANCH_CD,
    ban.BNK_ACCT_NBR,
    ban.BNK_ACCT_DESC
From
    X000_NWU_bank_details ban
Group By
    ban.BNK_BRANCH_CD,
    ban.BNK_ACCT_NBR
Order By
    ban.BNK_ACCT_NBR