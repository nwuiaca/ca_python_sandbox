Select
    fsta.ia_findstat_auto As value,
    fsta.ia_findstat_name As label
From
    ia_finding_status fsta
Where
    fsta.ia_findstat_active = 1 And
    -- fsta.ia_findstat_customer = ".$customer_id." And
    fsta.ia_findstat_customer = 1 And
    -- fsta.ia_findstat_from <= '".date('Y-m-d')."' And    
    fsta.ia_findstat_from <= Now() And
    -- fsta.ia_findstat_to >= '".date('Y-m-d')."'
    fsta.ia_findstat_to >= Now()
Order By
    fsta.ia_findstat_stat,
    label