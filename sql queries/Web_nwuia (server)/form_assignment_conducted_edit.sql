Select
    cond.ia_assicond_auto,
    cond.ia_assicond_customer,
    cond.ia_assicond_name,
    cond.ia_assicond_desc,
    cond.ia_assicond_from,
    cond.ia_assicond_to,
    cond.ia_assicond_active,
    cond.ia_assicond_form
From
    ia_assignment_conducted cond
Where
    -- cond.ia_assicond_auto = '". $id."'
    cond.ia_assicond_auto = 1