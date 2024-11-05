Select
    X000_Transaction.STUDENT,
    X000_Transaction.MODULE,
    X000_Transaction.MODULE_NAME,
    Count(X000_Transaction.FQUALLEVELAPID) As Count_FQUALLEVELAPID,
    Total(X000_Transaction.AMOUNT) As Total_AMOUNT
From
    X000_Transaction
Where
    X000_Transaction.STUDENT = 24823252
Group By
    X000_Transaction.STUDENT,
    X000_Transaction.MODULE,
    X000_Transaction.MODULE_NAME