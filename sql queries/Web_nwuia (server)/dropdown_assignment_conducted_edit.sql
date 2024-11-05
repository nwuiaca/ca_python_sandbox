Select
    dtab.ia_assicond_auto As value,
    dtab.ia_assicond_name As label
From
    ia_assignment_conducted dtab
Where
    -- dtab.ia_assicond_customer = ".$customer_id." And
    dtab.ia_assicond_customer = 1 And
    -- dtab.ia_assicond_from <= '".$create_date."' And
    dtab.ia_assicond_from <= dtab.ia_assicond_createdate And
    -- dtab.ia_assicond_to >= '".$create_date."'
    dtab.ia_assicond_to >= dtab.ia_assicond_createdate
Order By
    label