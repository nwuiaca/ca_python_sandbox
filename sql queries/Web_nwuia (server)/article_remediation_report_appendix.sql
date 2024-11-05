Select
    find.ia_find_desc,
    find.ia_find_risk,
    find.ia_find_criteria,
    find.ia_find_procedure,
    find.ia_find_condition,
    find.ia_find_effect,
    find.ia_find_cause,
    find.ia_find_recommend,
    find.ia_find_comment,
    find.ia_find_frequency,
    find.ia_find_definition,
    find.ia_find_reference,
    find.ia_find_name
From
    ia_finding find
Where
    find.ia_find_private = '0' And
    find.ia_find_appendix = 1 And
    find.ia_assi_auto = 549
Order By
    find.ia_find_name