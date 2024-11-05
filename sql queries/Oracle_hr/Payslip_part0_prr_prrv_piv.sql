Select
    piv.UOM,
    piv.NAME,
    prr.RUN_RESULT_ID,
    prrv.RESULT_VALUE
From
    HR.PAY_RUN_RESULTS prr,
    HR.PAY_RUN_RESULT_VALUES prrv,
    HR.PAY_INPUT_VALUES_F piv
Where
    prr.RUN_RESULT_ID = prrv.RUN_RESULT_ID And
    piv.INPUT_VALUE_ID = prrv.INPUT_VALUE_ID And
    piv.UOM = 'M' And
    piv.NAME = 'Pay Value'