Select
    dtab.ia_assirepo_auto As value,
    dtab.ia_assirepo_name As label
From
    ia_assignment_report dtab
Where
    -- dtab.ia_assirepo_customer = ".$customer_id." And
    dtab.ia_assirepo_customer = 1 And
    -- dtab.ia_assirepo_from <= '".$create_date."' And
    dtab.ia_assirepo_from <= dtab.ia_assirepo_createdate And
    -- dtab.ia_assirepo_to >= '".$create_date."'
    dtab.ia_assirepo_to >= dtab.ia_assirepo_createdate
Order By
    label