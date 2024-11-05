Select
    t.FDEBTCOLLECTIONSITE,
    Total(t.AMOUNT) As Total_AMOUNT
From
    X010_Studytrans t
Where
    t.TRANSCODE Is Not Null
Group By
    t.FDEBTCOLLECTIONSITE