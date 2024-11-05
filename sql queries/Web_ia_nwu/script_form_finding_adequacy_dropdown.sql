Select
    adeq.ia_findadeq_auto As id,
    adeq.ia_findadeq_name As name
From
    ia_finding_adequacy adeq
Where
    adeq.ia_findadeq_active = 1
Order By
    name