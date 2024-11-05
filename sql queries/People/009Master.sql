Select
    p.employee_number,
    p.person_id,
    p.name_address,
    p.user_person_type,
    p.marital_status,
    p.date_started,
    i.ELEMENT_VALUE As spouse_insurance_status,
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
    s.spouse_update_login,
    s.spouse_age
From
    X000_PEOPLE p Left Join
    X000_GROUPINSURANCE_SPOUSE i On i.EMPLOYEE_NUMBER = p.employee_number Left Join
    X002_SPOUSE_CURR s On s.employee_number = p.employee_number