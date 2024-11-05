Select
    concat(cont.name, ' (', cont.id, ')') as customer,
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
    ia_assignment_origin aori Inner Join
    jm4_contact_details cont On cont.id = aori.ia_assiorig_customer
Group By
    cont.name,
    aori.ia_assiorig_name;