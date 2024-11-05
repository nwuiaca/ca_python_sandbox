Select
    concat(cont.name, ' - ', cate.ia_assicate_name) As cust_cate,
    stat.ia_assistat_name As name,
    stat.ia_assistat_desc As description,
    Date(stat.ia_assistat_from) As date_from,
    Date(stat.ia_assistat_to) As date_to,
    Case
        When stat.ia_assistat_active = 1
        Then 'Yes'
        Else 'No'
    End As active,
    Concat('<a href = "index.php?option=com_rsform&formId=', stat.ia_assistat_form, '&recordId=', stat.ia_assistat_auto,
    '&action=edit">Edit</a>', ' | ', '<a href = "index.php?option=com_rsform&formId=', stat.ia_assistat_form, '&recordId=',
    stat.ia_assistat_auto, '&action=copy">Copy</a>', ' | ', '<a href = "index.php?option=com_rsform&formId=',
    stat.ia_assistat_form, '&recordId=', stat.ia_assistat_auto, '&action=delete">Delete</a>') As actions
From
    ia_assignment_status stat Inner Join
    ia_assignment_category cate On cate.ia_assicate_auto = stat.ia_assicate_auto Inner Join
    jm4_contact_details cont On cont.id = stat.ia_assistat_customer
Group By
    concat(cont.name, ' - ', cate.ia_assicate_name),
    stat.ia_assistat_name