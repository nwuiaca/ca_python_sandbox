Select
    tran.SOURCE As source,
    Count(tran.STUDENT) As tran_count,
    Total(tran.AMOUNT) As tran_total
From
    X000_Transaction tran
Group By
    tran.SOURCE