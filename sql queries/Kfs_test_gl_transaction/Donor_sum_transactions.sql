Select
    X003aa_donor_transaction.UNIV_FISCAL_YR,
    X003aa_donor_transaction.UNIV_FISCAL_PRD_CD,
    Count(X003aa_donor_transaction.FDOC_NBR) As Count_Transactions,
    Sum(X003aa_donor_transaction.CALC_AMOUNT) As Sum_Transactions
From
    X003aa_donor_transaction
Group By
    X003aa_donor_transaction.UNIV_FISCAL_YR,
    X003aa_donor_transaction.UNIV_FISCAL_PRD_CD