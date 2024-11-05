Select
    dtab.ia_assicond_auto As value,
    dtab.ia_assicond_name As label
From
    ia_assignment_conducted dtab
Where
    dtab.ia_assicond_active = 1 And
    -- dtab.ia_assicond_customer = ".$customer_id." And
    dtab.ia_assicond_customer = 1 And
    -- dtab.ia_assicond_from <= '".date('Y-m-d')."' And
    dtab.ia_assicond_from <= Now() And
    -- dtab.ia_assicond_to >= '".date('Y-m-d')."'
    dtab.ia_assicond_to >= Now()
Order By
    label;