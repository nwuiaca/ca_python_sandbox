Select
    find.ia_assi_auto,
    find.ia_find_auto,
    alik.ia_findlike_value,
    arat.ia_findrate_impact,
    acon.ia_findcont_value,
    --alik.ia_findlike_value * arat.ia_findrate_impact * (1 - acon.ia_findcont_value) As audit_risk_rate
    Avg(alik.ia_findlike_value * arat.ia_findrate_impact * (1 - acon.ia_findcont_value)) As audit_risk_rate
From
    ia_finding find Inner Join
    ia_finding_likelihood alik On alik.ia_findlike_auto = find.ia_findlike_auto Inner Join
    ia_finding_rate arat On arat.ia_findrate_auto = find.ia_findrate_auto Inner Join
    ia_finding_control acon On acon.ia_findcont_auto = find.ia_findcont_auto
Where
    --find.ia_assi_auto = ".$assignment_id."
    find.ia_assi_auto = 94
Group By
    find.ia_assi_auto