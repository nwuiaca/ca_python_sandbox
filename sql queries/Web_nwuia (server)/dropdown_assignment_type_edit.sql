Select
    dtab.ia_assitype_auto As value,
    dtab.ia_assitype_name As label
From
    ia_assignment_type dtab
Where
    -- dtab.ia_assitype_customer = ".$customer_id." And
    dtab.ia_assitype_customer = 1 And
    -- dtab.ia_assicate_auto = ".$record_category." And
    dtab.ia_assicate_auto = 11 And
    -- dtab.ia_assitype_from <= '".$create_date."' And
    dtab.ia_assitype_from <= dtab.ia_assitype_createdate And
    -- dtab.ia_assitype_to >= '".$create_date."'
    dtab.ia_assitype_to >= dtab.ia_assitype_createdate
Order By
    label