Select
    fcon.ia_findcont_auto As value,
    fcon.ia_findcont_name As label
From
    ia_finding_control fcon
Where
    -- fcon.ia_findcont_customer = ".$customer_id." And
    fcon.ia_findcont_customer = 1 And
    -- fcon.ia_findcont_from <= '".$create_date."' And
    fcon.ia_findcont_from <= fcon.ia_findcont_createdate And
    -- fcon.ia_findcont_to >= '".$create_date."'
    fcon.ia_findcont_to >= fcon.ia_findcont_createdate
Order By
    fcon.ia_findcont_value,
    label