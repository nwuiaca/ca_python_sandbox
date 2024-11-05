Select
    concat(cont.name, ' (', cont.id, ')') as customer,
    frat.ia_findrate_impact As list_order,
    frat.ia_findrate_name As name,
    frat.ia_findrate_desc As description,
    Date(frat.ia_findrate_from) As date_from,
    Date(frat.ia_findrate_to) As date_to,
    Case
        When frat.ia_findrate_active = 1
        Then 'Yes'
        Else 'No'
    End As active,
    Concat('<a href = "index.php?option=com_rsform&formId=', frat.ia_findrate_form, '&id=', frat.ia_findrate_auto,
    '&action=edit">Edit</a>', ' | ', '<a href = "index.php?option=com_rsform&formId=', frat.ia_findrate_form, '&id=',
    frat.ia_findrate_auto, '&action=copy">Copy</a>', ' | ', '<a href = "index.php?option=com_rsform&formId=',
    frat.ia_findrate_form, '&id=', frat.ia_findrate_auto, '&action=delete">Delete</a>') As actions
From
    ia_finding_rate frat Inner Join
    jm4_contact_details cont On cont.id = frat.ia_findrate_customer
Group By
    cont.name,
    frat.ia_findrate_impact,
    frat.ia_findrate_name