Select
    dtab.ia_assistat_auto As value,
    dtab.ia_assistat_name As label    
From
    ia_assignment_status dtab
Where
    -- dtab.ia_assistat_active = 2 And
    -- make active = 2 to deliver no results
    dtab.ia_assistat_active = 1 And
    -- dtab.ia_assistat_customer = ".$customer_id." And
    dtab.ia_assistat_customer = 1 And
    -- dtab.ia_assicate_auto = ".$record_category." And
    dtab.ia_assicate_auto = 11 And    
    -- dtab.ia_assistat_from <= '".date('Y-m-d')."' And
    dtab.ia_assistat_from <= Now() And
    -- dtab.ia_assistat_to >= '".date('Y-m-d')."'
    dtab.ia_assistat_to >= Now()
Order By
    label