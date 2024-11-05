Select
    dtab.ia_assicate_auto As value,
    case
    when dtab.ia_assicate_active = 1 then concat(cont.name, ' - ', dtab.ia_assicate_name, ' (Active) ', date(dtab.ia_assicate_from), '/', date(dtab.ia_assicate_to))
    else concat(cont.name, ' - ', dtab.ia_assicate_name, ' (InActve) ', date(dtab.ia_assicate_from), '/', date(dtab.ia_assicate_to))
    end as label
From
    ia_assignment_category dtab Inner Join
    jm4_contact_details cont On cont.id = dtab.ia_assicate_customer
Order By
    label