Select
    user.ia_user_name As auditor,
    user.ia_user_active As auditor_active,
    assi.ia_assi_year As year,
    assi.ia_assi_finishdate,
    cate.ia_assicate_name As category,
    cate.ia_assicate_private As category_private,
    type.ia_assitype_name As type,
    type.ia_assitype_private As type_private,
    assi.ia_assi_name As assignment,
    assi.ia_assi_priority As priority,
    find.ia_find_name As finding,
    find.ia_find_private As finding_private,
    stat.ia_findstat_name As wstatus,
    stat.ia_findstat_private As wstatus_private,
    rate.ia_findrate_name As rating,
    rate.ia_findrate_impact As rating_value,
    `like`.ia_findlike_name As likelihood,
    `like`.ia_findlike_value As likelihood_value,
    cont.ia_findcont_name As control,
    cont.ia_findcont_value As control_value
From
    ia_finding find Inner Join
    ia_assignment assi On assi.ia_assi_auto = find.ia_assi_auto Inner Join
    ia_user user On user.ia_user_sysid = assi.ia_user_sysid Inner Join
    ia_assignment_category cate On cate.ia_assicate_auto = assi.ia_assicate_auto Inner Join
    ia_assignment_type type On type.ia_assitype_auto = assi.ia_assitype_auto Inner Join
    ia_finding_status stat On stat.ia_findstat_auto = find.ia_findstat_auto Left Join
    ia_finding_rate rate On rate.ia_findrate_auto = find.ia_findrate_auto Left Join
    ia_finding_likelihood `like` On `like`.ia_findlike_auto = find.ia_findlike_auto Left Join
    ia_finding_control cont On cont.ia_findcont_auto = find.ia_findcont_auto
Where
    (assi.ia_assi_year = '2023' And
        user.ia_user_active = '1' And
        cate.ia_assicate_private = '0' And
        type.ia_assitype_private = '0' And
        find.ia_find_private = '0' And
        stat.ia_findstat_private = '0') Or
    (assi.ia_assi_year < '2023' And
        user.ia_user_active = '1' And
        cate.ia_assicate_private = '0' And
        type.ia_assitype_private = '0' And
        find.ia_find_private = '0' And
        stat.ia_findstat_private = '0' And
        assi.ia_assi_priority < 9) Or
    (user.ia_user_active = '1' And
        cate.ia_assicate_private = '0' And
        type.ia_assitype_private = '0' And
        find.ia_find_private = '0' And
        stat.ia_findstat_private = '0' And
        assi.ia_assi_finishdate >= Str_To_Date('2022-10-01', '%Y-%m-%d') And
        assi.ia_assi_finishdate <= Str_To_Date('2023-09-30', '%Y-%m-%d'))
Order By
    auditor,
    year,
    category,
    type