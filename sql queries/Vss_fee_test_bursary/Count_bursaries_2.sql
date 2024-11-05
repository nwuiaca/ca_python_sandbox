Select
    x000t.FINAIDCODE,
    x000t.FINAIDNAME,
    Total(x000t.AMOUNT) As Total_AMOUNT
From
    X000_Transaction x000t
Group By
    x000t.FINAIDCODE,
    x000t.FINAIDNAME