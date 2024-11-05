Select
    umap.customer_id,
    cont.name
From
    ia_user_map umap Inner Join
    jm4_contact_details cont On cont.id = umap.customer_id
Where
    umap.user_id = 855