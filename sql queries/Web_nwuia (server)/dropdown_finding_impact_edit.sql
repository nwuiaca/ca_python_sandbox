Select
    rate.ia_findrate_auto As value,
    rate.ia_findrate_name As label
From
    ia_finding_rate rate
Where
    -- rate.ia_findrate_customer = ".$customer_id." And
    rate.ia_findrate_customer = 1 And
    -- rate.ia_findrate_from <= '".$create_date."' And
    rate.ia_findrate_from <= rate.ia_findrate_createdate And
    -- rate.ia_findrate_to >= '".$create_date."'
    rate.ia_findrate_to >= rate.ia_findrate_createdate
Order By
    rate.ia_findrate_impact,
    label