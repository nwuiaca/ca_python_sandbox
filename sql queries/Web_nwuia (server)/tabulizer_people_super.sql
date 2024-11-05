Select
    concat(cont.name, ' (', cont.id, ')') as customer,
    peop.employee_number,
    peop.name_list,
    peop.preferred_name,
    peop.gender,
    peop.phone_work,
    peop.phone_mobile,
    Concat('<a href = "mailto: ', lower(peop.email_address), '">', lower(peop.email_address), '</a>') As email_address,
    peop.organization,
    peop.grade_calc,
    peop.position_name
From
    ia_people peop Left Join
    jm4_contact_details cont On cont.id = peop.customer
Group By
    cont.name,
    peop.name_list,    
    peop.employee_number