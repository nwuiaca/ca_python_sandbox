Select
    dtab.ia_assirepo_auto As value,
    case
    when dtab.ia_assirepo_active = 1 then concat(cont.name, ' - ', dtab.ia_assirepo_name, ' (Active) ', date(dtab.ia_assirepo_from), '/', date(dtab.ia_assirepo_to))
    else concat(cont.name, ' - ', dtab.ia_assirepo_name, ' (InActve) ', date(dtab.ia_assirepo_from), '/', date(dtab.ia_assirepo_to))
    end as label
From
    ia_assignment_report dtab Inner Join
    jm4_contact_details cont On cont.id = dtab.ia_assirepo_customer
Order By
    label