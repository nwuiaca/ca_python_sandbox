Select
    fcon.ia_findcont_value As list_order,
    fcon.ia_findcont_name As name,
    fcon.ia_findcont_desc As description,
    Date(fcon.ia_findcont_from) As date_from,
    Date(fcon.ia_findcont_to) As date_to,
    Case
        When fcon.ia_findcont_active = 1
        Then 'Yes'
        Else 'No'
    End As active,
    Concat('<a href = "index.php?option=com_rsform&formId=', fcon.ia_findcont_form, '&id=', fcon.ia_findcont_auto,
    '&action=edit">Edit</a>', ' | ', '<a href = "index.php?option=com_rsform&formId=', fcon.ia_findcont_form, '&id=',
    fcon.ia_findcont_auto, '&action=copy">Copy</a>', ' | ', '<a href = "index.php?option=com_rsform&formId=',
    fcon.ia_findcont_form, '&id=', fcon.ia_findcont_auto, '&action=delete">Delete</a>') As actions
From
    ia_finding_control fcon
Where
    -- fcon.ia_findcont_customer = '{user_param_1:cmd}' 
    fcon.ia_findcont_customer = 1
Group By
    fcon.ia_findcont_value,
    fcon.ia_findcont_name