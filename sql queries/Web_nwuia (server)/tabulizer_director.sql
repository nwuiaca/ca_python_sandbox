Select
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
    ia_director dir
Where
    dir.customer = '1'
Group By
    dir.nwu_number,
    dir.registration_number,
    dir.user_person_type,
    dir.position_name,
    dir.import_date