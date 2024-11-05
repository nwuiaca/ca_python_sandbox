Select
    user.user_id As value,
    jm4_users.name As label,
    jm4_users.block,
    user.customer_id
From
    ia_user_map user Inner Join
    jm4_users On jm4_users.id = user.user_id
Where
    jm4_users.block = 0 And
    -- user.customer_id = ".$customer_id."
    user.customer_id = 1
Order By
    label;