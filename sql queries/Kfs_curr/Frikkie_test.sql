Select
    pm.EDOC,
    pm.PAYEE_NAME,
    pm.VENDOR_TYPE_CALC,
    pm.DOC_TYPE,
    pm.DISB_NBR,
    pm.DISB_TS,
    pm.PMT_DT,
    pm.INV_DT,
    pm.APPROVE_DATE,
    JulianDay(pm.DISB_TS) - JulianDay(pm.INV_DT) As DAYS,
    pm.COMPLETE_EMP_NO,
    pm.COMPLETE_EMP_NAME,
    pm.COMPLETE_DATE,
    pm.APPROVE_EMP_NO,
    pm.APPROVE_EMP_NAME,
    pm.APPROVE_STATUS,
    pm.A_COUNT,
    pm.NOTE,
    pm.ACC_COST_STRING,
    pm.ACC_DESC
From
    X001ac_Report_payments_approve pm
Where
    pm.VENDOR_TYPE_CALC In ('DV', 'PO') And
    pm.PMT_DT Like '2023-02%'
Order By
    DAYS Desc,
    pm.APPROVE_DATE