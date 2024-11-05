Select
    dtab.ia_assicate_auto As value,
    dtab.ia_assicate_name As label
From
    ia_assignment_category dtab
Where
    -- dtab.ia_assicate_customer = ".$customer_id." And
    dtab.ia_assicate_customer = 1 And
    -- dtab.ia_assicate_from <= '".$create_date."' And
    dtab.ia_assicate_from <= dtab.ia_assicate_createdate And
    -- dtab.ia_assicate_to >= '".$create_date."'
    dtab.ia_assicate_to >= dtab.ia_assicate_createdate
Order By
    label