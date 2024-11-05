Select
    s.employee_number,
    s.person_id,
    s.name_address,
    s.user_person_type,
    s.test,
    s.marital_status,
    s.married,
    s.date_started,
    s.spouse_insurance_status,
    s.spouse_age,
    s.person_extra_info_id,
    s.spouse_number,
    s.spouse_address,
    s.spouse_date_of_birth,
    s.spouse_national_identifier,
    s.spouse_passport,
    s.spouse_start_date,
    s.spouse_end_date,
    s.spouse_create_date,
    s.spouse_created_by,
    s.spouse_update_date,
    s.spouse_updated_by,
    s.spouse_update_login
From
    X009_people_spouse_all s
Where
    s.married Is Null And
    s.spouse_insurance_status > 0