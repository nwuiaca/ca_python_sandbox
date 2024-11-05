Select
    assi.ia_assi_auto,
    assi.ia_assi_name,
    find.ia_find_auto,
    find.ia_find_token,
    find.ia_find_name,
    find.ia_find_note,
    find.ia_find_private,
    find.ia_find_desc_toggle,
    find.ia_find_desc,
    find.ia_findstat_auto,
    find.ia_find_risk_toggle,
    find.ia_find_risk,
    find.ia_find_criteria_toggle,
    find.ia_find_criteria,
    find.ia_find_procedure_toggle,
    find.ia_find_procedure,
    find.ia_find_condition_toggle,
    find.ia_find_condition,
    find.ia_find_effect_toggle,
    find.ia_find_effect,
    find.ia_find_cause_toggle,
    find.ia_find_cause,
    find.ia_find_recommend_toggle,
    find.ia_find_recommend,
    find.ia_find_comment_toggle,
    find.ia_find_comment,
    find.ia_find_frequency_toggle,
    find.ia_find_frequency,
    find.ia_find_definition_toggle,
    find.ia_find_definition,
    find.ia_find_reference_toggle,
    find.ia_find_reference,
    find.ia_findrate_auto,
    rate.ia_findrate_desc,
    find.ia_findlike_auto,
    lhoo.ia_findlike_desc,
    find.ia_findcont_auto,
    cont.ia_findcont_desc,
    find.ia_findrate_auto_client,
    clra.ia_findrate_desc As ia_findrate_desc_client,
    find.ia_findlike_auto_client,
    clli.ia_findlike_desc As ia_findlike_desc_client,
    find.ia_findcont_auto_client,
    clco.ia_findcont_desc As ia_findcont_desc_client,
    faud.ia_findaud_auto,
    faud.ia_findaud_desc,
    adeq.ia_findadeq_auto,
    adeq.ia_findadeq_desc
From
    ia_finding find Left Join
    ia_finding_rate rate On rate.ia_findrate_auto = find.ia_findrate_auto Left Join
    ia_finding_likelihood lhoo On lhoo.ia_findlike_auto = find.ia_findlike_auto Left Join
    ia_finding_control cont On cont.ia_findcont_auto = find.ia_findcont_auto Left Join
    ia_finding_rate clra On clra.ia_findrate_auto = find.ia_findrate_auto_client Left Join
    ia_finding_likelihood clli On clli.ia_findlike_auto = find.ia_findlike_auto_client Left Join
    ia_finding_control clco On clco.ia_findcont_auto = find.ia_findcont_auto_client Inner Join
    ia_assignment assi On assi.ia_assi_auto = find.ia_assi_auto Left Join
    ia_finding_audit faud On faud.ia_findaud_auto = find.ia_findaud_auto Left Join
    ia_finding_adequacy adeq On adeq.ia_findadeq_auto = find.ia_findadeq_auto