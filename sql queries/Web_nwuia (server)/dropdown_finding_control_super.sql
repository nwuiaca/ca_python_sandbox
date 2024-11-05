Select
    fcon.ia_findcont_auto As value,
    case
    when fcon.ia_findcont_active = 1 then concat(cont.name, ' - ', fcon.ia_findcont_name, ' (Active) ', date(fcon.ia_findcont_from), '/', date(fcon.ia_findcont_to))
    else concat(cont.name, ' - ', fcon.ia_findcont_name, ' (InActve) ', date(fcon.ia_findcont_from), '/', date(fcon.ia_findcont_to))
    end as label
From
    ia_finding_control fcon Inner Join
    jm4_contact_details cont On cont.id = fcon.ia_findcont_customer
Order By
    cont.name,
    fcon.ia_findcont_value,
    label