Select
    reme.ia_findreme_auto,
    Concat('(', assi.ia_assi_auto, ') ', assi.ia_assi_name) As ia_assi_name,
    find.ia_find_name,
    reme.ia_findreme_auditor,
    reme.ia_findreme_auditor_email,
    reme.ia_findreme_name,
    reme.ia_findreme_mail,
    reme.ia_findreme_mail_message,
    reme.ia_findreme_mail_trigger,
    reme.ia_findreme_client_message,
    reme.ia_findresp_auto,
    reme.ia_findresp_desc,
    reme.ia_findreme_response,
    reme.ia_findreme_formview,
    reme.ia_findreme_token,
    reme.ia_findreme_formclient,
    find.ia_find_desc,
    find.ia_find_condition,
    find.ia_find_effect,
    find.ia_find_cause,
    find.ia_find_recommend,
    reme.ia_findreme_date_send,
    reme.ia_findreme_date_open,
    reme.ia_findreme_date_submit,
    reme.ia_findreme_date_schedule,
    reme.ia_findreme_schedule,
    Concat(clra.ia_findrate_name, ' (', clra.ia_findrate_impact, ') - ', clra.ia_findrate_desc) As ia_findrate_name,
    Concat(clli.ia_findlike_name, ' (', clli.ia_findlike_value, ') - ', clli.ia_findlike_desc) As ia_findlike_name,
    Concat(clco.ia_findcont_name, ' (', clco.ia_findcont_value, ') - ', clco.ia_findcont_desc) As ia_findcont_name,
    reme.ia_findreme_attach,
    ia_user.ia_user_position
From
    ia_finding_remediation reme Inner Join
    ia_finding find On find.ia_find_auto = reme.ia_find_auto Inner Join
    ia_assignment assi On assi.ia_assi_auto = find.ia_assi_auto Left Join
    ia_finding_response resp On resp.ia_findresp_auto = reme.ia_findresp_auto Left Join
    ia_finding_rate clra On clra.ia_findrate_auto = reme.ia_findrate_auto Left Join
    ia_finding_likelihood clli On clli.ia_findlike_auto = reme.ia_findlike_auto Left Join
    ia_finding_control clco On clco.ia_findcont_auto = reme.ia_findcont_auto Left Join
    ia_user On ia_user.ia_user_sysid = assi.ia_user_sysid