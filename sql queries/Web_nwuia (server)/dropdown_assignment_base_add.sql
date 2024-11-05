Select
    assi.ia_assi_auto As value,
    Concat(cate.ia_assicate_name, ' (', assi.ia_assi_year, ') ', ' (', type.ia_assitype_name, ') ', assi.ia_assi_name, ' (', assi.ia_assi_auto, ')') As label
From
    ia_assignment assi Inner Join
    ia_assignment_category cate On cate.ia_assicate_auto = assi.ia_assicate_auto Inner Join
    ia_assignment_type type On type.ia_assitype_auto = assi.ia_assitype_auto
Where
    -- (assi.ia_user_sysid = ".$user_id." And
    (assi.ia_user_sysid = 855 And
        assi.ia_assi_year = year(now())) Or
    -- (assi.ia_user_sysid = ".$user_id." And
    (assi.ia_user_sysid = 855 And
        assi.ia_assi_year = year(date_add(now(), interval 1 year)))
Group by
    Concat(cate.ia_assicate_name, ' (', type.ia_assitype_name, ') ', assi.ia_assi_name, ' (', assi.ia_assi_auto, ')')