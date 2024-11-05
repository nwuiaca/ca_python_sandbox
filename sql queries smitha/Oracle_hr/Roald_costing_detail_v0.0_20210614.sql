select xxnwu_oe_code(hou_org.organization_id, ppa.effective_date) oe_code, xxnwu_oe_eng_name(hou_org.organization_id, ppa.effective_date) oe_name 
,hl.location_code, ppa.effective_date, ppf.employee_number, ppf.full_name, ppf.last_name||', '||xxper_util.initials(substr(ppf.first_name,1,decode(instr(ppf.first_name,' '),0,length(ppf.first_name)))||' '||ppf.middle_names) emp_name,
pet.element_name, pcak.segment1 chart_campus, pcak.segment6 account_number, pcak.segment7 object_code,
pcak.segment2, pcak.segment3, pcak.segment4, pcak.segment5,
decode(debit_or_credit,'D',costed_value,0.00) debit,
decode(debit_or_credit,'C',costed_value,0.00) credit

from 
 pay_payroll_actions ppa,
      pay_assignment_actions paa,
      pay_consolidation_sets pcs,
      per_assignments_f paf,
      pay_payrolls_f pay,
      per_people_f ppf,
      hr_organization_units hou_org,
      hr_locations hl,
      pay_costs pc,
      pay_input_values_f piv,
      pay_input_values_f_tl pivtl,
      pay_element_types_f pet,
      pay_element_types_f_tl pettl,
      pay_cost_allocation_keyflex pcak

    WHERE ppa.effective_date between '01-JAN-2018' and '31-JAN-2018'--to_date(to_char(v_date_start,'DD-MON-YY'),'DD-MON-YYYY') and to_date(to_char(v_date_end,'DD-MON-YY'),'DD-MON-YYYY')
    AND ppa.action_type IN('C', 'S', 'EC')

    AND paa.payroll_action_id    = ppa.payroll_action_id
    AND paa.action_status        = 'C'
    AND pcs.consolidation_set_id = ppa.consolidation_set_id
    AND pc.assignment_action_id  = paa.assignment_action_id
    AND piv.input_value_id       = pc.input_value_id
    AND ppa.effective_date BETWEEN piv.effective_start_date AND piv.effective_end_date
    AND piv.input_value_id  = pivtl.input_value_id
    AND pivtl.LANGUAGE      = userenv('LANG')
    AND pet.element_type_id = piv.element_type_id
    AND ppa.effective_date BETWEEN pet.effective_start_date AND pet.effective_end_date
    AND pettl.element_type_id = pet.element_type_id
    AND pettl.LANGUAGE        = userenv('LANG')
    AND paf.assignment_id     = paa.assignment_id
    AND ppa.effective_date BETWEEN paf.effective_start_date AND paf.effective_end_date
    AND pay.payroll_id = paf.payroll_id
    AND pay.payroll_id > 0
    AND ppa.effective_date BETWEEN pay.effective_start_date AND pay.effective_end_date
    AND hou_org.organization_id = paf.organization_id
    AND hl.location_id (+)      = paf.location_id
    AND ppf.person_id           = paf.person_id
--    and pcak.segment6 = nvl(p_account,pcak.segment6) --SPECIFIC ACCOUNT
--    and pcak.segment7 = nvl(p_object,pcak.segment7) --SPECIFIC OBJECT
    AND ppa.effective_date BETWEEN ppf.effective_start_date AND ppf.effective_end_date
    AND pcak.cost_allocation_keyflex_id = pc.cost_allocation_keyflex_id
--    and ppf.person_id = nvl(p_person_id,ppf.person_id)  --SPECIFIC PERSON *PERSON_ID