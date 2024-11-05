Select
    concat(cont.name, ' (', cont.id, ')') as customer,
    acat.ia_assicate_name As name,
    acat.ia_assicate_desc As description,
    Date(acat.ia_assicate_from) As date_from,
    Date(acat.ia_assicate_to) As date_to,
    Case
        When acat.ia_assicate_active = 1
        Then 'Yes'
        Else 'No'
    End As active,
    Case
        When acat.ia_assicate_private = 1
        Then 'Yes'
        Else 'No'
    End As private,
    Concat('<a href = "index.php?option=com_rsform&formId=', acat.ia_assicate_form, '&id=', acat.ia_assicate_auto,
    '&action=edit">Edit</a>', ' | ', '<a href = "index.php?option=com_rsform&formId=', acat.ia_assicate_form, '&id=',
    acat.ia_assicate_auto, '&action=copy">Copy</a>', ' | ', '<a href = "index.php?option=com_rsform&formId=',
    acat.ia_assicate_form, '&id=', acat.ia_assicate_auto, '&action=delete">Delete</a>') As actions
From
    ia_assignment_category acat Inner Join
    jm4_contact_details cont On cont.id = acat.ia_assicate_customer
Group By
    cont.name,
    acat.ia_assicate_name;