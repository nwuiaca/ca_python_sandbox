Select
    rate.ia_findrate_auto As value,
    case
    when rate.ia_findrate_active = 1 then concat(cont.name, ' - ', rate.ia_findrate_name, ' (Active) ', date(rate.ia_findrate_from), '/', date(rate.ia_findrate_to))
    else concat(cont.name, ' - ', rate.ia_findrate_name, ' (InActve) ', date(rate.ia_findrate_from), '/', date(rate.ia_findrate_to))
    end as label
From
    ia_finding_rate rate Inner Join
    jm4_contact_details cont On cont.id = rate.ia_findrate_customer
Order By
    cont.name,
    rate.ia_findrate_impact,
    label