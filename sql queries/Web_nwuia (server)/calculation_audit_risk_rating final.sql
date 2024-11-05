Select
    Avg(alik.ia_findlike_value * arat.ia_findrate_impact * (1 - acon.ia_findcont_value)) As audit_risk_rate
From
    ia_finding find Inner Join
    ia_finding_likelihood alik On alik.ia_findlike_auto = find.ia_findlike_auto Inner Join
    ia_finding_rate arat On arat.ia_findrate_auto = find.ia_findrate_auto Inner Join
    ia_finding_control acon On acon.ia_findcont_auto = find.ia_findcont_auto Inner Join
    ia_assignment assi On assi.ia_assi_auto = find.ia_assi_auto Inner Join
    ia_assignment_category assc On assc.ia_assicate_auto = assi.ia_assicate_auto Inner Join
    ia_assignment_type asst On asst.ia_assitype_auto = assi.ia_assitype_auto Inner Join
    ia_finding_status fins On fins.ia_findstat_auto = find.ia_findstat_auto
Where
    -- find.ia_assi_auto = {$assignment_id} And
    find.ia_assi_auto = 1126 And
    assc.ia_assicate_private = '0' And
    asst.ia_assitype_private = '0' And
    find.ia_find_private = '0' And
    find.ia_find_appendix = 0 And
    fins.ia_findstat_private = '0'
Group By
    find.ia_assi_auto