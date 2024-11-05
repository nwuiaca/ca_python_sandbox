Select
    fsta.ia_findstat_auto As value,
    fsta.ia_findstat_name As label
From
    ia_finding_status fsta
Where
    -- fsta.ia_findstat_customer = ".$customer_id." And
    fsta.ia_findstat_customer = 1 And
    -- fsta.ia_findstat_from <= '".$create_date."' And
    fsta.ia_findstat_from <= fsta.ia_findstat_createdate And
    -- fsta.ia_findstat_to >= '".$create_date."'
    fsta.ia_findstat_to >= fsta.ia_findstat_createdate
Order By
    fsta.ia_findstat_stat,
    label