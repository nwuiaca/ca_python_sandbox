Select
    dtab.ia_assirepo_auto As value,
    dtab.ia_assirepo_name As label
From
    ia_assignment_report dtab
Where
    dtab.ia_assirepo_active = 1 And
    -- dtab.ia_assirepo_customer = ".$customer_id." And
    dtab.ia_assirepo_customer = 1 And
    -- dtab.ia_assirepo_from <= '".date('Y-m-d')."' And
    dtab.ia_assirepo_from <= Now() And
    -- dtab.ia_assirepo_to >= '".date('Y-m-d')."'
    dtab.ia_assirepo_to >= Now()
Order By
    label;