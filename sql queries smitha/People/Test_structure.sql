Select
    peo1.employee_number,
    peo1.name_address As name_address1,
    peo1.preferred_name,
    poeh.name_address As oe_head,
    poe2.name_address As name_address2,
    poe3.name_address As name_address3,
    poe4.name_address As name_address4,
    X000_PEOPLE.name_address As name_address5
From
    X000_PEOPLE peo1 Left Join
    X000_PEOPLE poeh On poeh.employee_number = peo1.oe_head_number Left Join
    X000_PEOPLE poe2 On poe2.employee_number = peo1.supervisor_number Left Join
    X000_PEOPLE poe3 On poe3.employee_number = poe2.supervisor_number Left Join
    X000_PEOPLE poe4 On poe4.employee_number = poe3.supervisor_number Left Join
    X000_PEOPLE On X000_PEOPLE.employee_number = poe4.supervisor_number