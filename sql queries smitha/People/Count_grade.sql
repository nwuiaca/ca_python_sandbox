Select
    p.assignment_category,
    p.user_person_type,
    p.employee_category,
    p.grade,
    Count(p.assignment_id) As COUNT_GRADE
From
    X000_PEOPLE p
Group By
    p.assignment_category,
    p.user_person_type,
    p.employee_category,
    p.grade