Select
    date(type.ia_assitype_createdate) as create_date
From
    ia_assignment_type type
Where
    -- type.ia_assitype_auto = ". $record_id."
    type.ia_assitype_auto = 1