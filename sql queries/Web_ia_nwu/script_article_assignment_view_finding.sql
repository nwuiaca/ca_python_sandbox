Select
    assi.ia_assi_auto,
    assi.ia_assi_token,
    find.ia_find_auto,
    find.ia_find_name,
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
    rate.ia_findrate_name,
    rate.ia_findrate_desc,
    rate.ia_findrate_impact,
    lhoo.ia_findlike_name,
    lhoo.ia_findlike_desc,
    lhoo.ia_findlike_value,
    cont.ia_findcont_name,
    cont.ia_findcont_desc,
    cont.ia_findcont_value,
    clra.ia_findrate_name As ia_findrate_name_client,
    clra.ia_findrate_desc As ia_findrate_desc_client,
    clra.ia_findrate_impact As ia_findrate_impact_client,
    clli.ia_findlike_name As ia_findlike_name_client,
    clli.ia_findlike_desc As ia_findlike_desc_client,
    clli.ia_findlike_value As ia_findlike_value_client,
    clco.ia_findcont_name As ia_findcont_name_client,
    clco.ia_findcont_desc As ia_findcont_desc_client,
    clco.ia_findcont_value As ia_findcont_value_client
From
    ia_assignment assi Inner Join
    ia_finding find On find.ia_assi_auto = assi.ia_assi_auto Left Join
    ia_finding_rate rate On rate.ia_findrate_auto = find.ia_findrate_auto Left Join
    ia_finding_likelihood lhoo On lhoo.ia_findlike_auto = find.ia_findlike_auto Left Join
    ia_finding_control cont On cont.ia_findcont_auto = find.ia_findcont_auto Left Join
    ia_finding_rate clra On clra.ia_findrate_auto = find.ia_findrate_auto_client Left Join
    ia_finding_likelihood clli On clli.ia_findlike_auto = find.ia_findlike_auto_client Left Join
    ia_finding_control clco On clco.ia_findcont_auto = find.ia_findcont_auto_client
Where
    assi.ia_assi_auto = 341
Order By
    find.ia_find_name