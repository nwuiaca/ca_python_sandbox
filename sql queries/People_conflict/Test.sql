Select
    tran.exclude_combination2,
    tran.edoc,
    tran.object_code,
    tran.acc_amount,
    --tran.acc_line,
    Count(tran.nwu_number) As Count_nwu_number    
From
    X202aa_active_cipc_vendor_with_transaction tran
Group By
    tran.exclude_combination2,
    tran.edoc,
    tran.object_code,
    tran.acc_amount,
    tran.acc_line