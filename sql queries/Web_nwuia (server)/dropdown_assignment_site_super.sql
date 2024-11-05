Select
    dtab.ia_assisite_auto As value,
    case
    when dtab.ia_assisite_active = 1 then concat(cont.name, ' - ', dtab.ia_assisite_name, ' (Active) ', date(dtab.ia_assisite_from), '/', date(dtab.ia_assisite_to))
    else concat(cont.name, ' - ', dtab.ia_assisite_name, ' (InActve) ', date(dtab.ia_assisite_from), '/', date(dtab.ia_assisite_to))
    end as label
From
    ia_assignment_site dtab Inner Join
    jm4_contact_details cont On cont.id = dtab.ia_assisite_customer
Order By
    label