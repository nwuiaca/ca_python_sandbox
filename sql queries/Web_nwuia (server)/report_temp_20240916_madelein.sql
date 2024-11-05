Select
    assi.ia_assi_year,
    jm4_users.name,
    type.ia_assitype_name,
    assi.ia_assi_name,
    assi.ia_assi_finishdate,
    assi.ia_assi_offi
From
    ia_assignment assi Inner Join
    ia_assignment_type type On type.ia_assitype_auto = assi.ia_assitype_auto Inner Join
    jm4_users On jm4_users.id = assi.ia_user_sysid