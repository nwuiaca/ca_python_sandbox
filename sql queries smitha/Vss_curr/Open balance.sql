Select
    t.FDEBTCOLLECTIONSITE,
    Total(t.AMOUNT) As Total_AMOUNT,
    t.TRANSCODE,
    t.TRANSDATE
From
    X010_Studytrans t
Where
    t.TRANSCODE = '001'
Group By
    t.FDEBTCOLLECTIONSITE,
    t.TRANSCODE,
    t.TRANSDATE