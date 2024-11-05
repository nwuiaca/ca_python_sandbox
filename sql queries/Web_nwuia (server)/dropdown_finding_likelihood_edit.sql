Select
    flik.ia_findlike_auto As value,
    flik.ia_findlike_name As label
From
    ia_finding_likelihood flik
Where
    -- flik.ia_findlike_customer = ".$customer_id." And
    flik.ia_findlike_customer = 1 And
    -- flik.ia_findlike_from <= '".$create_date."' And
    flik.ia_findlike_from <= flik.ia_findlike_createdate And
    -- flik.ia_findlike_to >= '".$create_date."'
    flik.ia_findlike_to >= flik.ia_findlike_createdate
Order By
    flik.ia_findlike_value,
    label