Select
    pay.EDOC,
    pay.PAYEE_TYPE,
    pay.VENDOR_TYPE_CALC,
    pay.DOC_TYPE,
    pay.VENDOR_ID,
    pay.PAYEE_NAME,
    pay.INV_DT,
    pay.INV_NBR,
    pay.NET_PMT_AMT,
    pay.ACC_LINE,
    pay.ACC_COST_STRING,
    pay.ACC_AMOUNT,
    pay.ORG_NM,
    pay.ACCOUNT_NM,
    pay.FIN_OBJ_CD_NM
From
    X001ad_Report_payments_accroute pay
Where
    pay.PAYEE_TYPE Not In ('S', 'E') And
    pay.DOC_TYPE Not In ('SPDV', 'RV')
Order By
    pay.PAYEE_NAME,
    pay.INV_DT,
    pay.INV_NBR