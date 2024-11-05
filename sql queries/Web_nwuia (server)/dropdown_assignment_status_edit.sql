Select
    dtab.ia_assistat_auto As value,
    dtab.ia_assistat_name As label
From
    ia_assignment_status dtab
Where
    -- dtab.ia_assistat_customer = ".$customer_id." And
    dtab.ia_assistat_customer = 1 And
    -- dtab.ia_assicate_auto = ".$record_category." And
    dtab.ia_assicate_auto = 11 And
    -- dtab.ia_assistat_from <= '".$create_date."' And
    dtab.ia_assistat_from <= dtab.ia_assistat_createdate And
    -- dtab.ia_assistat_to >= '".$create_date."'
    dtab.ia_assistat_to >= dtab.ia_assistat_createdate
Order By
    label