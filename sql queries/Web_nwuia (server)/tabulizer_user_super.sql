Select
    Case
        When user.block = 1
        Then Concat(cust.name, ' (', umap.customer_id, ') (Inactive)')
        Else Concat(cust.name, ' (', umap.customer_id, ') (Active)')
    End As customer,
    umap.user_id,
    umap.contact_id,
    user.name As user,
    user.username As username,
    user.email As email,
    cont.telephone As telephone,
    cont.mobile As mobile,
    user.lastvisitDate As active,
    grou.title As usergroup,
    Concat('<a href = "index.php?option=com_rsform&formId=3&id=', user.id, '&action=edit">Edit</a>') As actions
From
    ia_user_map umap Inner Join
    jm4_users user On user.id = umap.user_id Inner Join
    jm4_contact_details cust On cust.id = umap.customer_id Inner Join
    jm4_contact_details cont On cont.id = umap.contact_id Inner Join
    jm4_user_usergroup_map gmap On gmap.user_id = user.id Inner Join
    jm4_usergroups grou On grou.id = gmap.group_id
Group By
    user.name,
    grou.title