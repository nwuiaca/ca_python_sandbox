Select
    dtab.ia_assicate_auto As value,
    dtab.ia_assicate_name As label
From
    ia_assignment_category dtab
Where
    dtab.ia_assicate_active = 1 And
    -- dtab.ia_assicate_customer = ".$customer_id." And
    dtab.ia_assicate_customer = 1 And
    -- dtab.ia_assicate_from <= '".date('Y-m-d')."' And
    dtab.ia_assicate_from <= Now() And
    -- dtab.ia_assicate_to >= '".date('Y-m-d')."'
    dtab.ia_assicate_to >= Now()
Order By
    label;