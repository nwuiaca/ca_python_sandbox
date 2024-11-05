Select
    tend.ia_tend_auto,
    tend.ia_tend_token,
    user.name,
    user.email,
    tend.ia_tend_status,
    tend.ia_tend_number,
    tend.ia_tend_description,
    tend.ia_tend_droplink,
    tend.ia_tend_dateclose
From
    ia_tender tend Inner Join
    jm4_users user On user.id = tend.ia_user_sysid
Where
    tend.ia_tend_token = '412f50c6873e8b8405b1a02656a51117'