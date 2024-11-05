﻿Select
    inc.EMPLOYEE_NUMBER,
    inc.EFFECTIVE_DATE,
    Sum(inc.RESULT_VALUE) As NORMAL_INCOME
From
    PAYROLL_HISTORY_CURR inc
Where
    inc.CLASSIFICATION_NAME = 'Normal Income'
Group By
    inc.EMPLOYEE_NUMBER,
    inc.EFFECTIVE_DATE