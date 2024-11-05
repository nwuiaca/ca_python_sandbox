Select
    v.EDOC,
    v.VENDOR_ID,
    v.VENDOR_NAME,
    v.PMT_DT,
    v.NET_PMT_AMT,
    v.APPROVE_EMP_NO,
    v.APPROVE_EMP_NAME,
    v.A_COUNT,
    v.ACC_COST_STRING
From
    X001ac_Report_payments_approve v
Where
    v.VENDOR_TYPE In ('DV', 'PO') And
    v.PMT_STAT_CD = 'EXTR'