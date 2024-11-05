Select
    v.nwu_number,
    v.company_name,
    v.company_registration_number,
    v.vendor_id,
    v.regno_director,
    i.declaration_id,
    i.interest_id,
    i.entity_name,
    i.entity_registration_number
From
    X200ab_employee_conflict_transaction v Left Join
    X200ac_employee_conflict_transaction i On (i.employee_number = v.nwu_number
                And SubStr(i.entity_registration_number, 1, 10) = v.regno_director)
            Or (i.employee_number = v.nwu_number
                And i.entity_name Like (v.company_name || '%'))