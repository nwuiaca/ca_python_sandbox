Select
    dtab.ia_assisite_auto As value,
    dtab.ia_assisite_name As label
From
    ia_assignment_site dtab
Where
    -- dtab.ia_assisite_customer = ".$customer_id." And
    dtab.ia_assisite_customer = 1 And
    -- dtab.ia_assisite_from <= '".$create_date."' And
    dtab.ia_assisite_from <= dtab.ia_assisite_createdate And
    -- dtab.ia_assisite_to >= '".$create_date."'
    dtab.ia_assisite_to >= dtab.ia_assisite_createdate
Order By
    label