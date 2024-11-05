Select
    concat(con.name, ' (', con.id, ')') as customer,
    dir.nwu_number,
    dir.employee_name,
    dir.national_identifier,
    dir.user_person_type,
    dir.position_name,
    dir.date_submitted,
    dir.import_date,
    dir.registration_number,
    dir.company_name,
    dir.enterprise_type,
    dir.company_status,
    dir.business_start_date,
    dir.directorship_start_date
From
    ia_director dir Left Join
    jm4_contact_details con On con.id = dir.customer
Group By
    con.name,
    dir.nwu_number,
    dir.registration_number