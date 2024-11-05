Select
    aori.ia_assiorig_name As name,
    aori.ia_assiorig_desc As description,
    Date(aori.ia_assiorig_from) As date_from,
    Date(aori.ia_assiorig_to) As date_to,
    Case
        When aori.ia_assiorig_active = 1
        Then 'Yes'
        Else 'No'
    End As active,
    Concat('<a href = "index.php?option=com_rsform&formId=', aori.ia_assiorig_form, '&id=', aori.ia_assiorig_auto,
    '&action=edit">Edit</a>', ' | ', '<a href = "index.php?option=com_rsform&formId=', aori.ia_assiorig_form, '&id=',
    aori.ia_assiorig_auto, '&action=copy">Copy</a>', ' | ', '<a href = "index.php?option=com_rsform&formId=',
    aori.ia_assiorig_form, '&id=', aori.ia_assiorig_auto, '&action=delete">Delete</a>') As actions
From
    ia_assignment_origin aori
Where
    -- aori.ia_assiorig_customer = '{user_param_1:cmd}' 
    aori.ia_assiorig_customer = 1
Group By
    aori.ia_assiorig_name