Select
    'NWU' As ORG,
    SubStr(peop.location, 1, 3) As LOC,
    peop.assignment_category As ACAT,
    peop.employee_category As ECAT,
    peop.employee_number As EMP,
    peop.phone_work As PHONE_WORK
From
    X000_PEOPLE peop
Where
    (peop.phone_work Is Null) Or
    -- (Length(peop.phone_work) <> 10) Or
    (Cast(peop.phone_work As Integer) < 1) Or
    (Cast(peop.phone_work As Integer) > 999999999)    
Order By
    ACAT,
    ECAT,
    EMP;