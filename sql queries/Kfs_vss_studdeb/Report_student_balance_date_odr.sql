Select
    tran.FBUSENTID As STUDENT,
    tran.CAMPUS,
    Sum(tran.AMOUNT) As Sum_AMOUNT,
    Max(tran.TRANSDATETIME) As Max_TRANSDATETIME,
    Max(tran.KACCTRANSID) As Max_KACCTRANSID
From
    X002aa_vss_tranlist tran
Where
    tran.TRANSDATE <= "2022-05-31"
Group By
    tran.FBUSENTID