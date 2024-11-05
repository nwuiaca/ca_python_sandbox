Select
    acc.PAYEE_ID_NBR,
    acc.PAYEE_ID_TYP_CD,
    Count(acc.VER_NBR) As Count_VER_NBR,
    Max(acc.ACH_ACCT_GNRTD_ID) As Max_ACH_ACCT_GNRTD_ID,
    acc.BNK_ACCT_NBR,
    acc.MODIFICATION_DATE
From
    AUDIT_PDP_PAYEE_ACH_ACCT_T acc
Where
    acc.MODIFICATION_DATE < '2024-03-08'
Group By
    acc.PAYEE_ID_NBR,
    acc.PAYEE_ID_TYP_CD