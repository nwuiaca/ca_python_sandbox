Select
    fsta.ia_findstat_auto As value,
    case
    when fsta.ia_findstat_active = 1 then concat(cont.name, ' - ', fsta.ia_findstat_name, ' (Active) ', date(fsta.ia_findstat_from), '/', date(fsta.ia_findstat_to))
    else concat(cont.name, ' - ', fsta.ia_findstat_name, ' (InActve) ', date(fsta.ia_findstat_from), '/', date(fsta.ia_findstat_to))
    end as label
From
    ia_finding_status fsta Inner Join
    jm4_contact_details cont On cont.id = fsta.ia_findstat_customer
Order By
    cont.name,
    fsta.ia_findstat_stat,
    label