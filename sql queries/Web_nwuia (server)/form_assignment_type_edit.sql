Select
    type.ia_assitype_auto,
    type.ia_assicate_auto,
    type.ia_assitype_file,
    type.ia_assitype_customer,
    type.ia_assitype_name,
    type.ia_assitype_desc,
    type.ia_assitype_from,
    type.ia_assitype_to,
    type.ia_assitype_active,
    type.ia_assitype_form,
    type.ia_assitype_editdate,
    type.ia_assitype_private
From
    ia_assignment_type type
Where
    -- type.ia_assitype_auto  = '". $id."'
    type.ia_assitype_auto = 1