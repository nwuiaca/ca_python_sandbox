Select
    X000_Transaction.STUDENT,
    X000_Transaction.AMOUNT,
    Count(X000_Transaction.FMODAPID) As Count_student,
    X000_Transaction.TRANSCODE
From
    X000_Transaction
Where
    X000_Transaction.TRANSCODE = '004'
Group By
    X000_Transaction.STUDENT,
    X000_Transaction.AMOUNT,
    X000_Transaction.TRANSCODE