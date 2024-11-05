Select
    rate.ia_findrate_auto,
    rate.ia_findrate_name,
    rate.ia_findrate_desc,
    rate.ia_findrate_impact
From
    ia_finding_rate rate
Where
    rate.ia_findrate_active = 1
Order By
    rate.ia_findrate_impact