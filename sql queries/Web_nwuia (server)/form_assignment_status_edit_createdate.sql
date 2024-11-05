Select
    date(stat.ia_assistat_createdate) as create_date
From
    ia_assignment_status stat
Where
    -- stat.ia_assistat_auto = ". $record_id."
    stat.ia_assistat_auto = 1;