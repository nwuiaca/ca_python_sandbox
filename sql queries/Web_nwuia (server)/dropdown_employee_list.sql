Select
    peop.employee_number,
    Concat(peop.name_address, ' (', peop.preferred_name, ') (', peop.organization, ': ', peop.position_name,
    ')') As employee_name
From
    ia_people peop
Where
    (peop.name_full Like ('%rensburg%')) Or
    (peop.employee_number Like ('%21162395%')) Or
    (peop.preferred_name Like ('%albert%')) Or
    (peop.organization Like ('%audit%')) Or
    (peop.position_name Like ('%internal audit%'))
Order By
    peop.name_list,
    peop.initials