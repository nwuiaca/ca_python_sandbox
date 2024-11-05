Select
    fcon.ia_findcont_auto,
    fcon.ia_findcont_customer,
    fcon.ia_findcont_value,
    fcon.ia_findcont_name,
    fcon.ia_findcont_desc,
    fcon.ia_findcont_from,
    fcon.ia_findcont_to,
    fcon.ia_findcont_active,
    fcon.ia_findcont_form
From
    ia_finding_control fcon
Where
    -- fcon.ia_findcont_auto = '". $id."'
    fcon.ia_findcont_auto = 1