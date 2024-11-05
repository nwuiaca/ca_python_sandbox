Select
    ia_assignment.ia_assi_auto,
    ia_assignment.ia_assi_name,
    ia_assignment.ia_assi_year,
    ia_assignment.ia_assi_priority,
    ia_assignment_category.ia_assicate_name,
    ia_assignment.ia_assi_finishdate
From
    ia_assignment Inner Join
    ia_assignment_category On ia_assignment_category.ia_assicate_auto = ia_assignment.ia_assicate_auto
Where
    ia_assignment.ia_assicate_auto = 9
Order By
    ia_assignment.ia_assi_year,
    ia_assignment.ia_assi_priority