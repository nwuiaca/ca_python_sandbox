Select
    ia_user.ia_user_sysid,
    ia_user.ia_user_name,
    ia_assignment.ia_assi_year,
    ia_assignment.ia_assi_name,
    ia_finding.ia_find_name,
    ia_finding.ia_findrate_auto,
    ia_finding.ia_findlike_auto,
    ia_finding.ia_findcont_auto
From
    ia_user Inner Join
    ia_assignment On ia_assignment.ia_user_sysid = ia_user.ia_user_sysid Inner Join
    ia_finding On ia_finding.ia_assi_auto = ia_assignment.ia_assi_auto
Where
    ia_user.ia_user_name Like ('Yolandie%') And
    ia_assignment.ia_assi_year = '2022'