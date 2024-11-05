Select
    rate.ia_findrate_auto,
    rate.ia_findrate_customer,
    rate.ia_findrate_impact,
    rate.ia_findrate_name,
    rate.ia_findrate_desc,
    rate.ia_findrate_from,
    rate.ia_findrate_to,
    rate.ia_findrate_active,
    rate.ia_findrate_form
From
    ia_finding_rate rate
Where
    -- rate.ia_findrate_auto = '". $id."'
    rate.ia_findrate_auto = 1