Select
    flik.ia_findlike_auto,
    flik.ia_findlike_customer,
    flik.ia_findlike_value,
    flik.ia_findlike_name,
    flik.ia_findlike_desc,
    flik.ia_findlike_from,
    flik.ia_findlike_to,
    flik.ia_findlike_active,
    flik.ia_findlike_form
From
    ia_finding_likelihood flik
Where
    -- flik.ia_findlike_auto = '". $id."'
    flik.ia_findlike_auto = 1