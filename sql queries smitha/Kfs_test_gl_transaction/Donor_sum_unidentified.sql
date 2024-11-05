Select
    X003ab_donor_receipt.GL_DESCRIPTION,
    Count(X003ab_donor_receipt.FDOC_NBR) As Count_FDOC_NBR,
    Sum(X003ab_donor_receipt.CALC_AMOUNT) As Sum_CALC_AMOUNT
From
    X003ab_donor_receipt
Where
    X003ab_donor_receipt.GL_DESCRIPTION <> "" And
    X003ab_donor_receipt."EXCLUDE" = 0
Group By
    X003ab_donor_receipt.GL_DESCRIPTION