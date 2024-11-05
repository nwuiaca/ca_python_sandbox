Select
    flik.ia_findlike_auto As value,
    case
    when flik.ia_findlike_active = 1 then concat(cont.name, ' - ', flik.ia_findlike_name, ' (Active) ', date(flik.ia_findlike_from), '/', date(flik.ia_findlike_to))
    else concat(cont.name, ' - ', flik.ia_findlike_name, ' (InActve) ', date(flik.ia_findlike_from), '/', date(flik.ia_findlike_to))
    end as label
From
    ia_finding_likelihood flik Inner Join
    jm4_contact_details cont On cont.id = flik.ia_findlike_customer
Order By
    cont.name,
    flik.ia_findlike_value,
    label