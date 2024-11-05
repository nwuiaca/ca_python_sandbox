Select
    X000_PEOPLE.employee_number,
    X000_PEOPLE.name_full,
    X000_PEOPLE.organization,
    X000_PEOPLE.date_of_birth,
    X000_PEOPLE.employee_age,
    X000_PEOPLE.assign_start_date,
    X000_PEOPLE.service_start_date,
    X000_PEOPLE.service_end_date,
    X000_PEOPLE.end_reason,
    X000_PEOPLE.leaving_reason
From
    X000_PEOPLE
Where
    (X000_PEOPLE.organization Like ('%PEOPLE%') And
        X000_PEOPLE.employee_age >= 63) Or
    (X000_PEOPLE.organization Like ('%P&C%') And
        X000_PEOPLE.employee_age >= 63)