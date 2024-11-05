Select
    pe.employee_number,
    pe.name_full,
    pe.is_foreign,
    pe.nationality,
    pe.nationality_passport,
    pe.national_identifier,
    pe.passport,
    pe.permit,
    pe.permit_expire
From
    X000_PEOPLE pe
Where
    pe.is_foreign = 'Y'