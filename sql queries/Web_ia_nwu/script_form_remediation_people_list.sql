Select
    peop.people_employee_number As Number,
    Concat(peop.people_name_addr, " (", peop.people_position_full, ")") As Name
From
    ia_people peop
Where
    (peop.people_full_name Like '%albert%') Or
    (peop.people_known_name Like '%albert%') Or
    (peop.people_position_full Like '%albert%') Or
    (peop.people_employee_number Like '%albert%')
Order By
    peop.people_name_list