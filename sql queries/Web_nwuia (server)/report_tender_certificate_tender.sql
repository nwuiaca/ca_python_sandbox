Select
    tend.ia_tend_auto,
    user.username,
    user.name,
    user.email,
    site.ia_assisite_name,
    cate.ia_assicate_name,
    tend.ia_tend_number,
    tend.ia_tend_description,
    tend.ia_tend_alternate,
    tend.ia_tend_division,
    tend.ia_tend_owner_name,
    tend.ia_tend_owner_number,
    tend.ia_tend_owner_mail,
    tend.ia_tend_auditor_name,
    tend.ia_tend_auditor_number,
    tend.ia_tend_auditor_mail,
    tend.ia_tend_datesubmit,
    tend.ia_tend_dateclose,
    tend.ia_tend_dateopen
From
    ia_tender tend Inner Join
    jm4_users user On user.id = tend.ia_user_sysid Inner Join
    ia_assignment_site site On site.ia_assisite_auto = tend.ia_assisite_auto Inner Join
    ia_assignment_category cate On cate.ia_assicate_auto = tend.ia_assicate_auto
Where
    tend.ia_tend_token = "f5be9cc3ddf776c94af91e11d326d5b3"