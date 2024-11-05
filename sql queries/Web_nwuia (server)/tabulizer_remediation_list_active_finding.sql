Select
    Concat(assi.ia_assi_name, '(', assi.ia_assi_auto, ') - ', find.ia_find_name, '(', find.ia_find_auto, ')') As assignment_finding,
    Concat('<a title="Assignment edit" href="index.php?option=com_rsform&view=rsform&formId=', assi.ia_assi_formedit, '&recordId=', assi.ia_assi_auto, '&recordHash=', assi.ia_assi_token, '&action=edit&category=', assi.ia_assicate_auto, '" target="_blank" rel="noopener nofollow noreferrer">', 'Edit', '</a>') As assignment_edit,
    Concat('<a title="Finding edit" href="index.php?option=com_rsform&view=rsform&formId=', find.ia_find_formedit, '&recordId=', find.ia_find_auto, '&recordHash=', find.ia_find_token, '&action=edit&assignment=', find.ia_assi_auto, '" target="_blank" rel="noopener nofollow noreferrer">', 'Edit', '</a>') As finding_edit,
    reme.ia_findreme_name As client,
    Case
        When Cast(reme.ia_findreme_mail_trigger As Int) = 0
        Then "Closed"
        When Cast(reme.ia_findreme_mail_trigger As Int) = 2
        Then "withClient"
        Else "atAudit"
    End As status,
    reme.ia_findreme_date_send As send,
    reme.ia_findreme_date_submit As submit,
    iafr.ia_findresp_name As response,
    Concat('<a title="Remediation edit" href="index.php?option=com_rsform&view=rsform&formId=', reme.ia_findreme_formedit, '&recordId=', reme.ia_findreme_auto, '&recordHash=', reme.ia_findreme_token, '&action=edit', '&assignment=', assi.ia_assi_auto, '&finding=', find.ia_find_auto, '" target="_blank" rel="noopener nofollow noreferrer">', 'Edit', '</a>', " | ", '<a title="Remediation copy" href="index.php?option=com_rsform&view=rsform&formId=', reme.ia_findreme_formedit, '&recordId=', reme.ia_findreme_auto, '&recordHash=', reme.ia_findreme_token, '&action=copy', '&assignment=', assi.ia_assi_auto, '&finding=', find.ia_find_auto, '" target="_blank" rel="noopener nofollow noreferrer">', 'Copy', '</a>', " | "'<a title="Remediation delete" href="index.php?option=com_rsform&view=rsform&formId=', reme.ia_findreme_formedit, '&recordId=', reme.ia_findreme_auto, '&recordHash=', reme.ia_findreme_token, '&action=delete', '&assignment=', assi.ia_assi_auto, '&finding=', find.ia_find_auto, '" target="_blank" rel="noopener nofollow noreferrer">', 'Delete', '</a>', " | ", '<a href = "index.php?option=com_content&view=article&id=', reme.ia_findreme_formview, '&hash=', reme.ia_findreme_token, '" target="_blank" rel="noopener nofollow noreferrer">View</a>', " | ", '<a href = "index.php?option=com_rsform&formId=', reme.ia_findreme_formtransfer, '&id=', reme.ia_findreme_auto, '" target="_blank" rel="noopener noreferrer">Update</a>') As actions
From
    ia_finding_remediation reme Inner Join
    ia_finding find On find.ia_find_auto = reme.ia_find_auto Inner Join
    ia_assignment assi On assi.ia_assi_auto = find.ia_assi_auto Left Join
    ia_finding_response iafr On iafr.ia_findresp_auto = reme.ia_findresp_auto
Where
    -- find.ia_find_auto = {user_param_1:int}
    find.ia_find_auto = 1129
Group By
    reme.ia_findreme_date_send,
    ia_findreme_date_submit,    
    reme.ia_findreme_auto