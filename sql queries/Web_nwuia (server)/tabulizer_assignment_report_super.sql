Select
    concat(cont.name, ' (', cont.id, ')') as customer,
    arep.ia_assirepo_name As name,
    arep.ia_assirepo_desc As description,
    Date(arep.ia_assirepo_from) As date_from,
    Date(arep.ia_assirepo_to) As date_to,
    Case
        When arep.ia_assirepo_active = 1
        Then 'Yes'
        Else 'No'
    End As active,
    Concat('<a href = "index.php?option=com_rsform&formId=', arep.ia_assirepo_form, '&id=', arep.ia_assirepo_auto,
    '&action=edit">Edit</a>', ' | ', '<a href = "index.php?option=com_rsform&formId=', arep.ia_assirepo_form, '&id=',
    arep.ia_assirepo_auto, '&action=copy">Copy</a>', ' | ', '<a href = "index.php?option=com_rsform&formId=',
    arep.ia_assirepo_form, '&id=', arep.ia_assirepo_auto, '&action=delete">Delete</a>') As actions
From
    ia_assignment_report arep Inner Join
    jm4_contact_details cont On cont.id = arep.ia_assirepo_customer
Group By
    cont.name,
    arep.ia_assirepo_name