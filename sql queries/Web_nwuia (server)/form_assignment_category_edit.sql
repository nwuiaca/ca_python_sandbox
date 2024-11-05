Select
    acat.ia_assicate_auto,
    acat.ia_assicate_name,
    acat.ia_assicate_desc,
    acat.ia_assicate_from,
    acat.ia_assicate_to,
    acat.ia_assicate_active,
    acat.ia_assicate_private,
    acat.ia_assicate_form,
    acat.ia_assicate_customer
From
    ia_assignment_category acat
Where
    -- acat.ia_assicate_customer = '". $id."'
    acat.ia_assicate_auto = 1