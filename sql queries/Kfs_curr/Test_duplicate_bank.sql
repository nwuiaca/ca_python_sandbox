Select
    X001aa_Report_payments.VENDOR_BANK_NR,
    X001aa_Report_payments.VENDOR_TYPE_CALC,
    SubStr(X001aa_Report_payments.VENDOR_ID, 1, 8) As VENDOR_ID,
    X001aa_Report_payments.VENDOR_NAME,
    Count(X001aa_Report_payments.EDOC) As Count_EDOC
From
    X001aa_Report_payments
Group By
    X001aa_Report_payments.VENDOR_BANK_NR,
    X001aa_Report_payments.VENDOR_TYPE_CALC,
    SubStr(X001aa_Report_payments.VENDOR_ID, 1, 8),
    X001aa_Report_payments.VENDOR_NAME