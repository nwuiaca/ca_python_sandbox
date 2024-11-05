Select
    frat.ia_findrate_auto As value,
    frat.ia_findrate_name As label
From
    ia_finding_rate frat
Where
    frat.ia_findrate_active = 1 And
    -- frat.ia_findrate_customer = ".$customer_id." And
    frat.ia_findrate_customer = 1 And
    -- frat.ia_findrate_from <= '".date('Y-m-d')."' And    
    frat.ia_findrate_from <= Now() And
    -- frat.ia_findrate_to >= '".date('Y-m-d')."'
    frat.ia_findrate_to >= Now()
Order By
    frat.ia_findrate_impact,
    label