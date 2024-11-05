Select
    concat(cont.name, ' (', cont.id, ')') as customer,
    fsta.ia_findstat_stat As list_order,
    fsta.ia_findstat_name As name,
    fsta.ia_findstat_desc As description,
    Date(fsta.ia_findstat_from) As date_from,
    Date(fsta.ia_findstat_to) As date_to,
    Case
        When fsta.ia_findstat_active = 1
        Then 'Yes'
        Else 'No'
    End As active,
    Case
        When fsta.ia_findstat_private = 1
        Then 'Yes'
        Else 'No'
    End As private,
    Concat('<a href = "index.php?option=com_rsform&formId=', fsta.ia_findstat_form, '&id=', fsta.ia_findstat_auto,
    '&action=edit">Edit</a>', ' | ', '<a href = "index.php?option=com_rsform&formId=', fsta.ia_findstat_form, '&id=',
    fsta.ia_findstat_auto, '&action=copy">Copy</a>', ' | ', '<a href = "index.php?option=com_rsform&formId=',
    fsta.ia_findstat_form, '&id=', fsta.ia_findstat_auto, '&action=delete">Delete</a>') As actions
From
    ia_finding_status fsta Inner Join
    jm4_contact_details cont On cont.id = fsta.ia_findstat_customer
Group By
    cont.name,
    fsta.ia_findstat_stat,
    fsta.ia_findstat_name