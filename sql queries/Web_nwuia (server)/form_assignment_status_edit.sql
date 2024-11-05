Select
    stat.ia_assistat_auto,
    stat.ia_assicate_auto,
    stat.ia_assistat_customer,
    stat.ia_assistat_name,
    stat.ia_assistat_desc,
    stat.ia_assistat_from,
    stat.ia_assistat_to,
    stat.ia_assistat_active,
    stat.ia_assistat_form,
    stat.ia_assistat_editdate
From
    ia_assignment_status stat
Where
    -- stat.ia_assistat_auto = '". $id."'
    stat.ia_assistat_auto = 1;