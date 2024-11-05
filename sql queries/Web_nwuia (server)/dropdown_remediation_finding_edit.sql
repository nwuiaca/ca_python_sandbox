Select
    f.ia_find_auto As value,
    Concat(f.ia_find_name, ' (', f.ia_find_auto, ')') As label
From
    ia_finding f Inner Join
    ia_assignment a On a.ia_assi_auto = f.ia_assi_auto
Where
    -- a.ia_assi_auto = ".$assignment_id." And
    a.ia_assi_auto = 89 And
    -- f.ia_find_auto = ".$finding_id."
    f.ia_find_auto = 928