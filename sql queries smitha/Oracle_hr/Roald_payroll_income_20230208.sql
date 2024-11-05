Select Distinct
    paaf.organization_id,
    papf.employee_number,
    papf.full_name,
    paaf.assignment_number,
    prr.run_result_id,
    petf.element_name,
    ppa.effective_date,
    pect.classification_name,
    prrv.result_value,
    (Select
         ppd.segment4
     From
         per_position_definitions ppd,
         hr_all_positions_f hapf
     Where
         hapf.position_definition_id = ppd.position_definition_id And
         hapf.position_id = paaf.position_id And
         ppa.effective_date Between hapf.effective_start_date And hapf.effective_end_date) hemis_cat,
    paaf.employee_category
From
    pay_run_results prr,
    pay_run_result_values prrv,
    pay_input_values_f piv,
    pay_element_types_f petf,
    pay_element_classifications_tl pect,
    pay_payroll_actions ppa,
    pay_assignment_actions paa,
    per_all_assignments_f paaf,
    per_all_people_f papf
Where
    prr.run_result_id = prrv.run_result_id And
    piv.input_value_id = prrv.input_value_id And
    petf.element_type_id = prr.element_type_id And
    pect.classification_id = petf.classification_id And
    prr.assignment_action_id = paa.assignment_action_id And
    paa.payroll_action_id = ppa.payroll_action_id And
    paa.assignment_id = paaf.assignment_id And
    ppa.payroll_action_id = paa.payroll_action_id And
    papf.person_id = paaf.person_id And
    piv.UOM = 'M' And
    piv.name = 'Pay Value' And
    paa.action_status = 'C' And
    ppa.action_type In ('R') And
    ppa.effective_date Between '01-MAR-2022' And '31-DEC-2022' And
    ppa.effective_date Between Trunc(paaf.effective_start_date) And Trunc(paaf.effective_end_date) And
    pect.classification_name In ('Normal Income', 'Allowances') And
    Trunc(paaf.effective_end_date) Between papf.effective_start_date And papf.effective_end_date