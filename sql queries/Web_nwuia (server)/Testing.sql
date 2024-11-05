Select
    user.id,
    user.name,
    user.username,
    user.block,
    gmap.group_id,
    grou.title
From
    jm4_users user Inner Join
    jm4_user_usergroup_map gmap On gmap.user_id = user.id Inner Join
    jm4_usergroups grou On grou.id = gmap.group_id Inner Join
    jm4_viewlevels leve On FIND_IN_SET(grou.id, TRIM(BOTH '[]' FROM REPLACE(leve.field, ' ', ''))) > 0