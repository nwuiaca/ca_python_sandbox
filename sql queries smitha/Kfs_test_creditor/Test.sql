Select
    X001ba_a_vendor_small_split_payment.VENDOR_ID,
    X001ba_a_vendor_small_split_payment.VNDR_TYP_CD,
    X001ba_a_vendor_small_split_payment.PAYEE_NAME,
    X001ba_a_vendor_small_split_payment.FIN_OBJ_CD_NM,
    Count(X001ba_a_vendor_small_split_payment.EDOC) As Count_EDOC
From
    X001ba_a_vendor_small_split_payment
Group By
    X001ba_a_vendor_small_split_payment.VENDOR_ID,
    X001ba_a_vendor_small_split_payment.VNDR_TYP_CD,
    X001ba_a_vendor_small_split_payment.PAYEE_NAME,
    X001ba_a_vendor_small_split_payment.FIN_OBJ_CD_NM