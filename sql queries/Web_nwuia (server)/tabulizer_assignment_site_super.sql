Select
    concat(cont.name, ' (', cont.id, ')') as customer,
    site.ia_assisite_name As name,
    site.ia_assisite_desc As description,
    Date(site.ia_assisite_from) As date_from,
    Date(site.ia_assisite_to) As date_to,
    Case
        When site.ia_assisite_active = 1
        Then 'Yes'
        Else 'No'
    End As active,
    Concat('<a href = "index.php?option=com_rsform&formId=', site.ia_assisite_form, '&id=', site.ia_assisite_auto,
    '&action=edit">Edit</a>', ' | ', '<a href = "index.php?option=com_rsform&formId=', site.ia_assisite_form, '&id=',
    site.ia_assisite_auto, '&action=copy">Copy</a>', ' | ', '<a href = "index.php?option=com_rsform&formId=',
    site.ia_assisite_form, '&id=', site.ia_assisite_auto, '&action=delete">Delete</a>') As actions
From
    ia_assignment_site site Inner Join
    jm4_contact_details cont On cont.id = site.ia_assisite_customer
Group By
    cont.name,
    site.ia_assisite_name