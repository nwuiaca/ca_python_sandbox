Select
    flik.ia_findlike_value As list_order,
    flik.ia_findlike_name As name,
    flik.ia_findlike_desc As description,
    Date(flik.ia_findlike_from) As date_from,
    Date(flik.ia_findlike_to) As date_to,
    Case
        When flik.ia_findlike_active = 1
        Then 'Yes'
        Else 'No'
    End As active,
    Concat('<a href = "index.php?option=com_rsform&formId=', flik.ia_findlike_form, '&id=', flik.ia_findlike_auto,
    '&action=edit">Edit</a>', ' | ', '<a href = "index.php?option=com_rsform&formId=', flik.ia_findlike_form, '&id=',
    flik.ia_findlike_auto, '&action=copy">Copy</a>', ' | ', '<a href = "index.php?option=com_rsform&formId=',
    flik.ia_findlike_form, '&id=', flik.ia_findlike_auto, '&action=delete">Delete</a>') As actions
From
    ia_finding_likelihood flik
Where
    -- flik.ia_findlike_customer = '{user_param_1:cmd}' 
    flik.ia_findlike_customer = 1
Group By
    flik.ia_findlike_value,
    flik.ia_findlike_name