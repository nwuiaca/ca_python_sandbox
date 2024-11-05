﻿Select
    peop1.employee_number As employee_one,
    peop1.name_list As name_list_one,
    peop1.preferred_name As known_name_one,
    peop1.position_name As position_full_one,
    peop1.location As location_description_one,
    peop1.division As division_one,
    peop1.faculty As faculty_one,
    lower(peop1.email_address) As email_address_one,
    peop1.phone_work As phone_work_one,
    peop1.phone_mobile As phone_mobi_one,
    peop1.internal_box As phone_home_one,
    peop1.organization As org_name_one,
    peop1.grade_calc As grade_calc_one,
    peop2.employee_number As employee_two,
    peop2.name_list As name_list_two,
    peop2.preferred_name As known_name_two,
    peop2.position_name As position_full_two,
    peop2.location As location_description_two,
    peop2.division As division_two,
    peop2.faculty As faculty_two,
    lower(peop2.email_address) As email_address_two,
    peop2.phone_work As phone_work_two,
    peop2.phone_mobile As phone_mobi_two,
    peop2.internal_box As phone_home_two,
    peop3.employee_number As employee_three,
    peop3.name_list As name_list_three,
    peop3.preferred_name As known_name_three,
    peop3.position_name As position_full_three,
    peop3.location As location_description_three,
    peop3.division As division_three,
    peop3.faculty As faculty_three,
    lower(peop3.email_address) As email_address_three,
    peop3.phone_work As phone_work_three,
    peop3.phone_mobile As phone_mobi_three,
    peop3.internal_box As phone_home_three
From
    X000_PEOPLE peop1 Left Join
    X000_PEOPLE peop2 On peop2.employee_number = peop1.supervisor_number Left Join
    X000_PEOPLE peop3 On peop3.employee_number = peop2.supervisor_number;