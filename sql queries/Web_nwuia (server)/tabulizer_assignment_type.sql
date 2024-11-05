Select
    cate.ia_assicate_name As category,
    type.ia_assitype_file As file,
    Case
        When cate.ia_assicate_private = '1'
        Then 'Yes'
        Else 'No'
    End As cprivate,    
    type.ia_assitype_name As name,
    type.ia_assitype_desc As description,
    Date(type.ia_assitype_from) As date_from,
    Date(type.ia_assitype_to) As date_to,
    Case
        When type.ia_assitype_active = '1'
        Then 'Yes'
        Else 'No'
    End As active,
    Case
        When type.ia_assitype_private = '1'
        Then 'Yes'
        Else 'No'
    End As private,
    Concat('<a href = "index.php?option=com_rsform&formId=', type.ia_assitype_form, '&recordId=', type.ia_assitype_auto,
    '&action=edit">Edit</a>', ' | ', '<a href = "index.php?option=com_rsform&formId=', type.ia_assitype_form, '&recordId=',
    type.ia_assitype_auto, '&action=copy">Copy</a>', ' | ', '<a href = "index.php?option=com_rsform&formId=',
    type.ia_assitype_form, '&recordId=', type.ia_assitype_auto, '&action=delete">Delete</a>') As actions
From
    ia_assignment_type type Inner Join
    ia_assignment_category cate On cate.ia_assicate_auto = type.ia_assicate_auto
Where
    -- type.ia_assitype_customer = '{user_param_1:cmd}'
    type.ia_assitype_customer = 1
Group By
    cate.ia_assicate_name,
    type.ia_assitype_name