Select
    peop1.employee_number,
    Concat(peop1.name_address, ' (', peop1.preferred_name, ')') As employee,
    peop1.organization,
    peop1.position_name,
    peop1.grade_calc,
    Concat(peop2.name_address, ' (', peop2.preferred_name, ')') As line2,
    Concat(peop3.name_address, ' (', peop3.preferred_name, ')') As line3,
    Concat(peop4.name_address, ' (', peop4.preferred_name, ')') As line4,
    Concat(head.name_address, ' (', head.preferred_name, ')') As oe_head
From
    ia_people peop1 Left Join
    ia_people peop2 On peop2.employee_number = peop1.supervisor_number Left Join
    ia_people peop3 On peop3.employee_number = peop2.supervisor_number Left Join
    ia_people peop4 On peop4.employee_number = peop3.supervisor_number Left Join
    ia_people head On head.employee_number = peop1.oe_head_number
Where
    -- peop.customer = '{user_param_1:cmd}'
    peop1.customer = 1
Group By
    peop1.name_full