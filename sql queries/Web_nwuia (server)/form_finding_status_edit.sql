Select
    stat.ia_findstat_auto,
    stat.ia_findstat_stat,
    stat.ia_findstat_name,
    stat.ia_findstat_desc,
    stat.ia_findstat_active,
    stat.ia_findstat_private,
    stat.ia_findstat_from,
    stat.ia_findstat_to,
    stat.ia_findstat_form,
    stat.ia_findstat_customer
From
    ia_finding_status stat
Where
    -- rate.ia_findstat_auto = '". $id."'
    stat.ia_findstat_auto = 1