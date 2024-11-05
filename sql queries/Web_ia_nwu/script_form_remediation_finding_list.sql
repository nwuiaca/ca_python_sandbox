Select
    find.ia_find_auto,
    find.ia_find_name
From
    ia_finding find Inner Join
    ia_finding_status fist On fist.ia_findstat_auto = find.ia_findstat_auto
Where
    find.ia_assi_auto = 424 And
    fist.ia_findstat_name Like "Request remediation"
Order By
    find.ia_find_name