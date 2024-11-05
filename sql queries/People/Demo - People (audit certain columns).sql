Select
    X000_PEOPLE.employee_number,
    X000_PEOPLE.name_full,
    X000_PEOPLE.date_of_birth,
    X000_PEOPLE.employee_age,
    X000_PEOPLE.gender,
    X000_PEOPLE.grade_calc,
    X000_PEOPLE.grade_name,
    X000_PEOPLE.organization_id,
    X000_PEOPLE.organization,
    X000_PEOPLE.location,
    X000_PEOPLE.division,
    X000_PEOPLE.faculty
From
    X000_PEOPLE
Where
    X000_PEOPLE.organization Like ("%audit%")