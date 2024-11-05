Select
    flik.ia_findlike_auto As value,
    flik.ia_findlike_name As label
From
    ia_finding_likelihood flik
Where
    flik.ia_findlike_active = 1 And
    -- flik.ia_findlike_customer = ".$customer_id." And
    flik.ia_findlike_customer = 1 And
    -- flik.ia_findlike_from <= '".date('Y-m-d')."' And    
    flik.ia_findlike_from <= Now() And
    -- flik.ia_findlike_to >= '".date('Y-m-d')."'
    flik.ia_findlike_to >= Now()
Order By
    flik.ia_findlike_value,
    label