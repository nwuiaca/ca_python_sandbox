﻿Select
    paym.EFFECTIVE_START_DATE,
    paym.EFFECTIVE_END_DATE,
    paym.ASSIGNMENT_ID,
    paym.ORG_PAYMENT_METHOD_ID,
    paym.PRIORITY,
    paym.LAST_UPDATE_DATE,
    accs.SEGMENT1,
    accs.SEGMENT2,
    accs.SEGMENT3,
    accs.SEGMENT4,
    PAY_ORG_PAYMENT_METHODS_F.ORG_PAYMENT_METHOD_NAME
From
    PAY_PERSONAL_PAYMENT_METHODS_F paym Inner Join
    PAY_EXTERNAL_ACCOUNTS accs On accs.EXTERNAL_ACCOUNT_ID = paym.EXTERNAL_ACCOUNT_ID Inner Join
    PAY_ORG_PAYMENT_METHODS_F On PAY_ORG_PAYMENT_METHODS_F.ORG_PAYMENT_METHOD_ID = paym.ORG_PAYMENT_METHOD_ID