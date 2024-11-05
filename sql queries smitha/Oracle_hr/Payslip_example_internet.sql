Select
    -- https://appselangovan.blogspot.com/2015/08/sql-query-to-get-payslip-of-employee.html
    ppa.date_earned,
    per.full_name,
    per.employee_number,
    NVL(pet.reporting_name, pet.element_name),
    piv.NAME,
    prrv.result_value,
    ptp.period_name
From
    pay_payroll_actions ppa,
    pay_assignment_actions pac,
    per_all_assignments_f ass,
    per_all_people_f per,
    pay_run_results prr,
    pay_element_types_f pet,
    pay_input_values_f piv,
    pay_run_result_values prrv,
    per_time_periods_v ptp
Where
    ppa.payroll_action_id = pac.payroll_action_id And
    pac.assignment_id = ass.assignment_id And
    ass.person_id = per.person_id And
    pac.assignment_action_id = prr.assignment_action_id And
    prr.element_type_id = pet.element_type_id And
    prr.run_result_id = prrv.run_result_id And
    pet.element_type_id = piv.element_type_id And
    piv.input_value_id = prrv.input_value_id And
    ppa.time_period_id = ptp.time_period_id And
    ass.effective_end_date = To_Date('12/31/4712', 'MM/DD/RRRR') And
    per.effective_end_date = To_Date('12/31/4712', 'MM/DD/RRRR') And
    pet.element_name = 'Basic Salary' And
    per.employee_number = '21162395' And
    ptp.period_name Like '1 2023 Calendar Month'