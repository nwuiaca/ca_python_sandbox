Select
    X000_Payments.PMT_DT,
    X000_Payments.DISB_NBR,
    Total(X000_Payments.NET_PMT_AMT) As Total_NET_PMT_AMT
From
    X000_Payments
Group By
    X000_Payments.PMT_DT,
    X000_Payments.DISB_NBR