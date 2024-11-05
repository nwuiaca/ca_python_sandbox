Select
    peop1.employee_number,
    peop1.name_address || ' (' || peop1.preferred_name || ')' As employee,
    peop1.organization,
    peop1.position_name,
    peop1.grade_calc,
    peop2.name_address || ' (' || peop2.preferred_name || ')' As line2,
    peop3.name_address || ' (' || peop3.preferred_name || ')' As line3,
    peop4.name_address || ' (' || peop4.preferred_name || ')' As line4,
    ohead.name_address || ' (' || ohead.preferred_name || ')' As oehead,
    1 as customer
From
    X000_PEOPLE peop1 Inner Join
    X000_PEOPLE peop2 On peop2.employee_number = peop1.supervisor_number Inner Join
    X000_PEOPLE peop3 On peop3.employee_number = peop2.supervisor_number Inner Join
    X000_PEOPLE peop4 On peop4.employee_number = peop3.supervisor_number Inner Join
    X000_PEOPLE ohead On ohead.employee_number = peop1.oe_head_number