Select
    s.FBUSENTID,
    Count(s.FSERVICESITE) As TRAN_COUNT,
    Total(s.AMOUNT) As TRAN_VALUE
From
    X010_Studytrans s
Where
    s.TRANSCODE = '021'
Group By
    s.FBUSENTID