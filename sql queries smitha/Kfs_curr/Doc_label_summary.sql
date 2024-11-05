Select
    X001aa_Report_payments.DOC_LABEL,
    X001aa_Report_payments.DOC_TYPE,
    Count(X001aa_Report_payments.EDOC) As Count_EDOC,
    Total(X001aa_Report_payments.NET_PMT_AMT) As Total_NET_PMT_AMT
From
    X001aa_Report_payments
Group By
    X001aa_Report_payments.DOC_LABEL,
    X001aa_Report_payments.DOC_TYPE