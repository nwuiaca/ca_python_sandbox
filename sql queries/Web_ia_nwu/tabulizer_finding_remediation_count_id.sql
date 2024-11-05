Select
    find.ia_find_auto As id,
    Concat('<a href = "index.php?option=com_rsform&view=rsform&formId=', assi.ia_assi_formedit, '&aid=',
    assi.ia_assi_auto, '&hash=', assi.ia_assi_token, '&category=', assi.ia_assicate_auto,
    '" target="_blank" rel="noopener noreferrer">', Concat(assi.ia_assi_name, ' (', assi.ia_assi_auto, ')'),
    '</a>') As assignment,
    Concat('<a href = "index.php?option=com_rsform&view=rsform&formId=10&fid=', find.ia_find_auto, '&hash=',
    find.ia_find_token, '" target="_blank" rel="noopener noreferrer">', find.ia_find_name, '</a>') As finding,
    Concat(Date(find.ia_find_editdate), ' ', Substr(MonthName(find.ia_find_editdate), 1, 3)) As dateedit,
    fist.ia_findstat_name As status,
    Concat('<a href = "index.php?option=com_content&view=article&id=', find.ia_find_formview, '&rid=',
    find.ia_find_auto, '">View</a>') As actions,
    Case
        When Count(reme.ia_findreme_auto) > 1
        Then Concat('<a href = "https://www.ia-nwu.co.za/index.php?option=com_rsform&view=rsform&formId=3">Add</a>',
            '|', Concat(Cast(Count(reme.ia_findreme_auto) As Character), 'requests'))
        When Count(reme.ia_findreme_auto) > 0
        Then Concat('<a href = "https://www.ia-nwu.co.za/index.php?option=com_rsform&view=rsform&formId=3">Add</a>',
            '|', Concat(Cast(Count(reme.ia_findreme_auto) As Character), 'request'))
        Else '<a href = "https://www.ia-nwu.co.za/index.php?option=com_rsform&view=rsform&formId=3">Add</a>'
    End As remediation
From
    ia_finding find Inner Join
    ia_assignment assi On assi.ia_assi_auto = find.ia_assi_auto Left Join
    ia_finding_status fist On fist.ia_findstat_auto = find.ia_findstat_auto Left Join
    ia_finding_remediation reme On reme.ia_find_auto = find.ia_find_auto
Where
    find.ia_assi_auto = 564
Group By
    find.ia_find_auto