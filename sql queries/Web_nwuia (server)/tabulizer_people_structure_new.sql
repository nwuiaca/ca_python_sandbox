Select
    peos.employee_number,
    peos.employee,
    peos.organization,
    peos.position_name,
    peos.grade_calc,
    peos.line2,
    peos.line3,
    peos.line4,
    peos.oehead,
    Concat('<a href="#" class="copy-link" data-copy="', peos.employee, ' (', lower(peos.organization), ': ', lower(peos.position_name), ')', '">Copy</a>')
From
    ia_people_structure peos
Where
    -- peos.customer = '{user_param_1:cmd}'
    peos.customer = 1
Group By
    peos.employee_number