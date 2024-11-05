Select
    dtab.ia_assiorig_auto As value,
    dtab.ia_assiorig_name As label
From
    ia_assignment_origin dtab
Where
    -- dtab.ia_assiorig_customer = ".$customer_id." And
    dtab.ia_assiorig_customer = 1 And
    -- dtab.ia_assiorig_from <= '".$create_date."' And
    dtab.ia_assiorig_from <= dtab.ia_assiorig_createdate And
    -- dtab.ia_assiorig_to >= '".$create_date."'
    dtab.ia_assiorig_to >= dtab.ia_assiorig_createdate
Order By
    label