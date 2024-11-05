Select
    p.employee_number,
    p.person_id,
    p.name_address,
    p.user_person_type,
    t.TEST1 As test,
    p.marital_status,
    m.TEST1 As married,
    p.date_started,
    p.spouse_insurance_status,
    p.person_extra_info_id,
    p.spouse_number,
    p.spouse_address,
    p.spouse_date_of_birth,
    p.spouse_national_identifier,
    p.spouse_passport,
    p.spouse_start_date,
    p.spouse_end_date,
    p.spouse_create_date,
    p.spouse_created_by,
    p.spouse_update_date,
    p.spouse_updated_by,
    p.spouse_update_login,
    p.spouse_age
From
    X009_people_spouse_all p Left Join
    X009_spouse_matrix t On t.PERSON_TYPE = p.user_person_type Left Join
    X009_spouse_matrix m On m.PERSON_TYPE = p.user_person_type
            And m.MARITAL_STATUS = p.marital_status
Group By
    p.employee_number