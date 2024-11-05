Select
    pfp.EMPLOYEE_NUMBER,
    Max(pfp.EFFECTIVE_DATE) As LAST_PAYMENT_DATE,
    Count(pfp.RUN_RESULT_ID) As COUNT_PAYMENTS
From
    X000aa_payroll_history_curr pfp
Where
    pfp.PAYROLL_NAME Like 'UIF Employee%'
Group By
    pfp.EMPLOYEE_NUMBER