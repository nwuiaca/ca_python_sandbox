Select
    Concat(assi.ia_assi_name, '(', assi.ia_assi_auto, ') - ', find.ia_find_name, '(', find.ia_find_auto, ')') As assignment_finding,
    Concat('<a href = "index.php?option=com_rsform&view=rsform&formId=', assi.ia_assi_formedit, '&aid=', assi.ia_assi_auto, '&hash=', assi.ia_assi_token, '&category=', assi.ia_assicate_auto, '" target="_blank" rel="noopener noreferrer">', 'Edit', '</a>') As assignment_edit,
    Concat('<a href = "index.php?option=com_rsform&view=rsform&formId=', find.ia_find_formedit, '&fid=', find.ia_find_auto, '&hash=', find.ia_find_token, '" target="_blank" rel="noopener noreferrer">', 'Edit', '</a>') As finding_edit,
    reme.ia_findreme_name As client,
    Case
        When Cast(reme.ia_findreme_mail_trigger As Int) = 2
        Then "withClient"
        Else "atAudit"
    End As status,
    Date(reme.ia_findreme_date_send) As send,
    Date(reme.ia_findreme_date_submit) As submit,
    iafr.ia_findresp_name As response,
    Concat('<a href = "https://www.ia-nwu.co.za/index.php?option=com_rsform&formId=', reme.ia_findreme_formedit, '&id=',
    reme.ia_findreme_auto, '" target="_blank" rel="noopener noreferrer">Edit</a>', " | ",
    '<a href = "https://www.ia-nwu.co.za/index.php?option=com_rsform&formId=', reme.ia_findreme_formdelete, '&id=',
    reme.ia_findreme_auto, '" target="_blank" rel="noopener noreferrer">Delete</a>', " | ",
    '<a href = "https://www.ia-nwu.co.za/index.php?option=com_content&view=article&id=', reme.ia_findreme_formview,
    '&hash=', reme.ia_findreme_token, '" target="_blank" rel="noopener noreferrer">View</a>', " | ",
    '<a href = "https://www.ia-nwu.co.za/index.php?option=com_rsform&formId=', reme.ia_findreme_formtransfer, '&id=',
    reme.ia_findreme_auto, '" target="_blank" rel="noopener noreferrer">Update</a>') As actions
From
    ia_finding_remediation reme Inner Join
    ia_finding find On find.ia_find_auto = reme.ia_find_auto Inner Join
    ia_assignment assi On assi.ia_assi_auto = find.ia_assi_auto Left Join
    ia_finding_response iafr On iafr.ia_findresp_auto = reme.ia_findresp_auto
Where
    assi.ia_user_sysid = 855 And
    reme.ia_findreme_mail_trigger > 0
Group By
    assi.ia_assi_name,
    find.ia_find_name,
    reme.ia_findreme_employee,
    reme.ia_findreme_date_send,
    reme.ia_findreme_auto