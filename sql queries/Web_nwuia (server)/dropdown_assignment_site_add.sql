Select
    dtab.ia_assisite_auto As value,
    dtab.ia_assisite_name As label
From
    ia_assignment_site dtab
Where
    dtab.ia_assisite_active = 1 And
    -- dtab.ia_assisite_customer = ".$customer_id." And
    dtab.ia_assisite_customer = 1 And
    -- dtab.ia_assisite_from <= '".date('Y-m-d')."' And
    dtab.ia_assisite_from <= Now() And
    -- dtab.ia_assisite_to >= '".date('Y-m-d')."'
    dtab.ia_assisite_to >= Now()
Order By
    label;