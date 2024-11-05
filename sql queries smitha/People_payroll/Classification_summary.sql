﻿Select
    PAYROLL_HISTORY_CURR.CLASSIFICATION_NAME,
    PAYROLL_HISTORY_CURR.REPORTING_NAME,
    Count(PAYROLL_HISTORY_CURR.RUN_RESULT_ID) As COUNT_NO
From
    PAYROLL_HISTORY_CURR
Group By
    PAYROLL_HISTORY_CURR.CLASSIFICATION_NAME,
    PAYROLL_HISTORY_CURR.REPORTING_NAME