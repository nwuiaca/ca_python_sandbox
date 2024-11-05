Select
    dtab.ia_assiorig_auto As value,
    case
    when dtab.ia_assiorig_active = 1 then concat(cont.name, ' - ', dtab.ia_assiorig_name, ' (Active) ', date(dtab.ia_assiorig_from), '/', date(dtab.ia_assiorig_to))
    else concat(cont.name, ' - ', dtab.ia_assiorig_name, ' (InActve) ', date(dtab.ia_assiorig_from), '/', date(dtab.ia_assiorig_to))
    end as label
From
    ia_assignment_origin dtab Inner Join
    jm4_contact_details cont On cont.id = dtab.ia_assiorig_customer
Order By
    label