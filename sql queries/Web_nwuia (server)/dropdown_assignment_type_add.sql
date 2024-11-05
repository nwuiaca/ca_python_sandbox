Select
    dtab.ia_assitype_auto As value,
    dtab.ia_assitype_name As label
From
    ia_assignment_type dtab
Where
    -- dtab.ia_assitype_active = 2 And
    -- make active = 2 to deliver no results
    dtab.ia_assitype_active = 1 And
    -- dtab.ia_assitype_customer = ".$customer_id." And
    dtab.ia_assitype_customer = 1 And
    -- dtab.ia_assicate_auto = ".$record_category." And
    dtab.ia_assicate_auto = 11 And    
    -- dtab.ia_assitype_from <= '".date('Y-m-d')."' And
    dtab.ia_assitype_from <= Now() And
    -- dtab.ia_assitype_to >= '".date('Y-m-d')."'
    dtab.ia_assitype_to >= Now()
Order By
    label