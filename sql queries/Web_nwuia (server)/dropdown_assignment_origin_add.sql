Select
    dtab.ia_assiorig_auto As value,
    dtab.ia_assiorig_name As label
From
    ia_assignment_origin dtab
Where
    dtab.ia_assiorig_active = 1 And
    -- dtab.ia_assiorig_customer = ".$customer_id." And
    dtab.ia_assiorig_customer = 1 And
    -- dtab.ia_assiorig_from <= '".date('Y-m-d')."' And
    dtab.ia_assiorig_from <= Now() And
    -- dtab.ia_assiorig_to >= '".date('Y-m-d')."'
    dtab.ia_assiorig_to >= Now()
Order By
    label;