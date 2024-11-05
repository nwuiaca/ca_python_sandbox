Select
    dtab.ia_assicond_auto As value,
    case
    when dtab.ia_assicond_active = 1 then concat(cont.name, ' - ', dtab.ia_assicond_name, ' (Active) ', date(dtab.ia_assicond_from), '/', date(dtab.ia_assicond_to))
    else concat(cont.name, ' - ', dtab.ia_assicond_name, ' (InActve) ', date(dtab.ia_assicond_from), '/', date(dtab.ia_assicond_to))
    end as label
From
    ia_assignment_conducted dtab Inner Join
    jm4_contact_details cont On cont.id = dtab.ia_assicond_customer
Order By
    label