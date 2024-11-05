Select
    X010_Studytrans.FBUSENTID,
    Sum(X010_Studytrans.AMOUNT) As Sum_AMOUNT,
    Count(X010_Studytrans.KACCTRANSID) As Count_KACCTRANSID
From
    X010_Studytrans
Where
    X010_Studytrans.TRANSCODE = '098' And
    X010_Studytrans.AMOUNT > 0
Group By
    X010_Studytrans.FBUSENTID,
    X010_Studytrans.AMOUNT