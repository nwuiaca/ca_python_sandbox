Select
    umap.user_id As value,
    case
      when user.block = 1 then Concat(cont.name, ' (Inactive) - ', user.name)
      else Concat(cont.name, ' (Active) - ', user.name)
    end as label
From
    ia_user_map umap Inner Join
    jm4_users user On user.id = umap.user_id Inner Join
    jm4_contact_details cont On cont.id = umap.customer_id
Order By
    label