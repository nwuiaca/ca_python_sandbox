Select
    pay.EDOC,
    pay.PAYEE_TYPE,
    pay.VENDOR_TYPE_CALC,
    pay.DOC_TYPE,
    pay.VENDOR_ID,
    pay.VENDOR_NAME,
    pay.INV_DT,
    pay.INV_NBR,
    pay.NET_PMT_AMT,
    pay.ACC_COST_STRING
From
    X001aa_Report_payments pay
Where
    pay.INV_NBR <> '' And
    pay.PAYEE_TYPE Not In ('S', 'E') And
    pay.DOC_TYPE Not In ('SPDV', 'RV')
Order By
    pay.PAYEE_NAME,
    pay.INV_DT,
    pay.INV_NBR