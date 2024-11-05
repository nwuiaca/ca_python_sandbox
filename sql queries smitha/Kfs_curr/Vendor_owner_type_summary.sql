Select
    p.PAYEE_TYPE,
    p.PAYEE_TYPE_DESC,
    p.OWNER_TYPE,
    p.OWNER_TYPE_DESC,
    p.VENDOR_TYPE,
    p.VENDOR_TYPE_DESC,
    Count(p.VENDOR_ID) As Count_VENDOR_ID
From
    X002aa_Report_payments_summary p
Where
    (p.PAYEE_TYPE = 'V' And
        p.PAYEE_NAME Like '%PTY%') Or
    (p.PAYEE_TYPE = 'V' And
        p.PAYEE_NAME Like '%LTD%') Or
    (p.PAYEE_TYPE = 'V' And
        p.PAYEE_NAME Like '%EDMS%') Or
    (p.PAYEE_TYPE = 'V' And
        p.PAYEE_NAME Like '%BPK%') Or
    (p.PAYEE_TYPE = 'V' And
        p.PAYEE_NAME Like '% CC')
Group By
    p.PAYEE_TYPE,
    p.PAYEE_TYPE_DESC,
    p.OWNER_TYPE,
    p.OWNER_TYPE_DESC,
    p.VENDOR_TYPE,
    p.VENDOR_TYPE_DESC