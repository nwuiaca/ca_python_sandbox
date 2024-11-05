Select
    user.id,
    user.name,
    user.username,
    user.email,
    user.block,
    user.sendEmail,
    user.requireReset,
    umap.customer_id,
    umap.contact_id,
    cont.con_position
From
    jm4_users user Inner Join
    ia_user_map umap On umap.user_id = user.id Inner Join
    jm4_contact_details cont On cont.id = umap.contact_id
Where
    -- user.id = '". $id."'
    user.id = 1