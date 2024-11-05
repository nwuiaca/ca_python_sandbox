Select
    tran.STUDENT_VSS,
    tran.CAMPUS_VSS,
    Sum(tran.AMOUNT_VSS) As Sum_AMOUNT_VSS,
    Max(tran.TRANSDATETIME) As Max_TRANSDATETIME,
    Max(tran.KACCTRANSID) As Max_KACCTRANSID
From
    X002ab_vss_transort tran
Where
    tran.TRANSDATE_VSS <= "2024-06-30"
Group By
    tran.STUDENT_VSS