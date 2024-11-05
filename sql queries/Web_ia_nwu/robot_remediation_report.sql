Select
    reme.ia_findreme_auditor As auditor,
    assi.ia_assi_year As year,
    cate.ia_assicate_name As category,
    type.ia_assitype_name As type,
    assi.ia_assi_name As assignment,
    find.ia_find_name As finding,
    reme.ia_findreme_name As request_to,
    Case
        When reme.ia_findreme_mail_trigger = 1
        Then 'Auditor'
        When reme.ia_findreme_mail_trigger = 2
        Then 'Client'
        Else 'Closed'
    End As rstatus,
    reme.ia_findreme_date_send As date_send,
    reme.ia_findreme_date_open As date_open,
    reme.ia_findreme_date_submit As date_answered,
    Case
        When reme.ia_findreme_schedule = 1
        Then 'Yes'
        Else ''
    End As sstatus,
    reme.ia_findreme_date_schedule As date_schedule,
    reme.ia_findresp_desc As response,
    reme.ia_findrate_auto As rating,
    reme.ia_findlike_auto As likelihood,
    reme.ia_findcont_auto As effectiveness
From
    ia_finding_remediation reme Inner Join
    ia_finding find On find.ia_find_auto = reme.ia_find_auto Inner Join
    ia_assignment assi On assi.ia_assi_auto = find.ia_assi_auto Left Join
    ia_assignment_category cate On cate.ia_assicate_auto = assi.ia_assicate_auto Left Join
    ia_assignment_type type On type.ia_assitype_auto = assi.ia_assitype_auto
Order By
    auditor,
    category,
    type,
    assignment,
    finding,
    date_send,
    date_answered