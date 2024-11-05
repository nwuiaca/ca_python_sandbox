Select
    p.YEAR,
    p.VENDOR_ID,
    p.PAYEE_NAME,
    p.VENDOR_NAME,
    p.REG_NO,
    p.PAYEE_TYPE,
    p.PAYEE_TYPE_DESC,
    p.OWNER_TYPE,
    p.OWNER_TYPE_DESC,
    p.VENDOR_TYPE,
    p.VENDOR_TYPE_DESC,
    p.LAST_PMT_DT,
    p.NET_PMT_AMT,
    p.TRAN_COUNT
From
    X002aa_Report_payments_summary p
Where
    (p.PAYEE_NAME Like ('%PTY%') And
        p.PAYEE_TYPE = 'V' and
        Cast(Substr(p.REG_NO,1,4) as Int) < 1919) Or
    (p.PAYEE_NAME Like ('%PTY%') And
        p.PAYEE_TYPE = 'V' and
        Cast(Substr(p.REG_NO,1,4) as Int) > 2023) Or
    (p.PAYEE_NAME Like ('%LTD%') And
        p.PAYEE_TYPE = 'V' and
        Cast(Substr(p.REG_NO,1,4) as Int) < 1919) Or
    (p.PAYEE_NAME Like ('%LTD%') And
        p.PAYEE_TYPE = 'V' and
        Cast(Substr(p.REG_NO,1,4) as Int) > 2023) Or
    (p.PAYEE_NAME Like ('%EDM%') And
        p.PAYEE_TYPE = 'V' and
        Cast(Substr(p.REG_NO,1,4) as Int) < 1919) Or
    (p.PAYEE_NAME Like ('%EDM%') And
        p.PAYEE_TYPE = 'V' and
        Cast(Substr(p.REG_NO,1,4) as Int) > 2023) Or
    (p.PAYEE_NAME Like ('%BPK%') And
        p.PAYEE_TYPE = 'V' and
        Cast(Substr(p.REG_NO,1,4) as Int) < 1919) Or
    (p.PAYEE_NAME Like ('%BPK%') And
        p.PAYEE_TYPE = 'V' and
        Cast(Substr(p.REG_NO,1,4) as Int) > 2023) Or
    (p.PAYEE_NAME Like ('% BK%') And
        p.PAYEE_TYPE = 'V' and
        Cast(Substr(p.REG_NO,1,4) as Int) < 1919) Or
    (p.PAYEE_NAME Like ('% BK%') And
        p.PAYEE_TYPE = 'V' and
        Cast(Substr(p.REG_NO,1,4) as Int) > 2023) Or
    (p.PAYEE_NAME Like ('% CC%') And
        p.PAYEE_TYPE = 'V' and
        Cast(Substr(p.REG_NO,1,4) as Int) < 1919) Or
    (p.PAYEE_NAME Like ('% CC%') And
        p.PAYEE_TYPE = 'V' and
        Cast(Substr(p.REG_NO,1,4) as Int) > 2023)