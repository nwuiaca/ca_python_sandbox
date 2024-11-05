Select
    X000_Transaction.STUDENT,
    Total(X000_Transaction.AMOUNT) As Total_AMOUNT,
    Count(X000_Transaction.FMODAPID) As Count_tran
From
    X000_Transaction
Where
    X000_Transaction.TRANSCODE In ('042', '052', '381', '500')
Group By
    X000_Transaction.STUDENT