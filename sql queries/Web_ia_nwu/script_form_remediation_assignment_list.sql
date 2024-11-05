Select
    assi.ia_assi_auto,
    Concat(cate.ia_assicate_name, ' - ', assi.ia_assi_name, ' (', find.ia_assi_auto, ') ', '(', type.ia_assitype_name,
    ')') As ia_assi_namenumb
From
    ia_assignment assi Inner Join
    ia_assignment_category cate On cate.ia_assicate_auto = assi.ia_assicate_auto Inner Join
    ia_assignment_type type On type.ia_assitype_auto = assi.ia_assitype_auto Inner Join
    ia_finding find On find.ia_assi_auto = assi.ia_assi_auto Inner Join
    ia_finding_status On ia_finding_status.ia_findstat_auto = find.ia_findstat_auto
Where
    assi.ia_user_sysid = 855 And
    assi.ia_assi_priority < 9 And
    ia_finding_status.ia_findstat_name = 'Request remediation'
Group By
    assi.ia_assi_auto,
    assi.ia_assi_name
Order By
    cate.ia_assicate_name,
    assi.ia_assi_name