﻿Select
    PAYROLL_HISTORY_CURR.CLASSIFICATION_NAME,
    PAYROLL_HISTORY_CURR.ELEMENT_NAME,
    Count(PAYROLL_HISTORY_CURR.RUN_RESULT_ID) As Count_RUN_RESULT_ID
From
    PAYROLL_HISTORY_CURR
Group By
    PAYROLL_HISTORY_CURR.CLASSIFICATION_NAME,
    PAYROLL_HISTORY_CURR.ELEMENT_NAME