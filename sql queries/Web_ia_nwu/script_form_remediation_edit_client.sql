Select
    assi.ia_assi_name,
    find.ia_find_name,
    reme.ia_findreme_mail_trigger,
    reme.ia_findreme_mail_message,
    reme.ia_findresp_auto,
    reme.ia_findreme_response,
    reme.ia_findreme_auditor,
    reme.ia_findreme_auditor_email,
    reme.ia_findreme_client_message,
    reme.ia_findresp_desc,
    reme.ia_findreme_formview,
    reme.ia_findreme_name,
    reme.ia_findreme_mail,
    reme.ia_findreme_date_open,
    reme.ia_findreme_date_submit,
    reme.ia_findrate_auto,
    rate.ia_findrate_desc,
    reme.ia_findlike_auto,
    hood.ia_findlike_desc,
    reme.ia_findcont_auto,
    cont.ia_findcont_desc
From
    ia_finding_remediation reme Inner Join
    ia_finding find On find.ia_find_auto = reme.ia_find_auto Inner Join
    ia_assignment assi On assi.ia_assi_auto = find.ia_assi_auto Left Join
    ia_finding_response resp On resp.ia_findresp_auto = reme.ia_findresp_auto Left Join
    ia_finding_rate rate On rate.ia_findrate_auto = reme.ia_findrate_auto Left Join
    ia_finding_likelihood hood On hood.ia_findlike_auto = reme.ia_findlike_auto Left Join
    ia_finding_control cont On cont.ia_findcont_auto = reme.ia_findcont_auto