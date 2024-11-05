Select
    acon.ia_assicond_name As name,
    acon.ia_assicond_desc As description,
    Date(acon.ia_assicond_from) As date_from,
    Date(acon.ia_assicond_to) As date_to,
    Case
        When acon.ia_assicond_active = 1
        Then 'Yes'
        Else 'No'
    End As active,    
    Concat('<a href = "index.php?option=com_rsform&formId=', acon.ia_assicond_form, '&id=', acon.ia_assicond_auto,
    '&action=edit">Edit</a>', ' | ', '<a href = "index.php?option=com_rsform&formId=', acon.ia_assicond_form, '&id=',
    acon.ia_assicond_auto, '&action=copy">Copy</a>', ' | ', '<a href = "index.php?option=com_rsform&formId=',
    acon.ia_assicond_form, '&id=', acon.ia_assicond_auto, '&action=delete">Delete</a>') As actions
From
    ia_assignment_conducted acon
Where
    -- acon.ia_assicond_customer = '{user_param_1:cmd}' 
    acon.ia_assicond_customer = 1
Group By
    acon.ia_assicond_name