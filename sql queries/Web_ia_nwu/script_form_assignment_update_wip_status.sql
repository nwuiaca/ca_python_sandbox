Select
    asst.ia_assistat_auto,
    asst.ia_assistat_name
From
    ia_assignment_status asst
Where
    asst.ia_assicate_auto = 10 And
    asst.ia_assistat_active = 1
Order By
    asst.ia_assistat_name