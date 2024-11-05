Select
    find.ia_find_auto As id,
    user.ia_user_name As auditor,
    assi.ia_assi_year As year,
    Concat(assi.ia_assi_name, ' (', assi.ia_assi_auto, ')') As assiignment,
    find.ia_find_name As finding,
    Concat(Date(find.ia_find_editdate), ' ', Substr(MonthName(find.ia_find_editdate), 1, 3)) As dateedit,
    fist.ia_findstat_name As status,
    Concat('<a href = "https://www.ia-nwu.co.za/index.php?option=com_content&view=article&id=', find.ia_find_formview,
    '&rid=', find.ia_find_auto, '" target="_blank" rel="noopener noreferrer">View</a>') As actions,
    Case
        When Count(reme.ia_findreme_auto) > 1
        Then Concat(Concat(Cast(Count(reme.ia_findreme_auto) As Character), 'Requests'))
        When Count(reme.ia_findreme_auto) > 0
        Then Concat(Concat(Cast(Count(reme.ia_findreme_auto) As Character), 'Request'))
    End As remediation
From
    ia_finding find Inner Join
    ia_assignment assi On assi.ia_assi_auto = find.ia_assi_auto Left Join
    ia_finding_status fist On fist.ia_findstat_auto = find.ia_findstat_auto Left Join
    ia_finding_remediation reme On reme.ia_find_auto = find.ia_find_auto Left Join
    ia_user user On user.ia_user_sysid = assi.ia_user_sysid
Where
    find.ia_find_auto > 0 And
    assi.ia_assi_priority < 9 And
    find.ia_find_private = 0
Group By
    find.ia_find_auto
Order By
    assi.ia_assi_name,
    finding