Select
    fcon.ia_findcont_auto As value,
    fcon.ia_findcont_name As label
From
    ia_finding_control fcon
Where
    fcon.ia_findcont_active = 1 And
    -- fcon.ia_findcont_customer = ".$customer_id." And
    fcon.ia_findcont_customer = 1 And
    -- fcon.ia_findcont_from <= '".date('Y-m-d')."' And    
    fcon.ia_findcont_from <= Now() And
    -- fcon.ia_findcont_to >= '".date('Y-m-d')."'
    fcon.ia_findcont_to >= Now()
Order By
    fcon.ia_findcont_value,
    label