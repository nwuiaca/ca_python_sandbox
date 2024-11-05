Select
    cate.ia_assicate_name As Category,
    type.ia_assitype_name As Type,
    Concat(assi.ia_assi_year, '.', assi.ia_assi_file) As FileRef,
    user.name As Owner,
    Concat('<a title="Assignment edit" href="index.php?option=com_rsform&view=rsform&formId=', assi.ia_assi_formedit,
    '&recordId=', assi.ia_assi_auto, '&recordHash=', assi.ia_assi_token, '&action=edit&category=',
    assi.ia_assicate_auto, '" target="_blank" rel="noopener nofollow noreferrer">', Concat(assi.ia_assi_name, ' (',
    assi.ia_assi_auto, ')'), '</a>') As Assignment,
    Case
        When assi.ia_assi_priority = 1
        Then 'Low'
        When assi.ia_assi_priority = 2
        Then 'Medium'
        When assi.ia_assi_priority = 3
        Then 'High'
        When assi.ia_assi_priority = 7
        Then 'Follow-up'
        When assi.ia_assi_priority = 8
        Then 'Continuous'
        When assi.ia_assi_priority = 9
        Then 'Closed'
        Else 'Inactive'
    End As Priority,
    stat.ia_assistat_name As Status,
    Concat(Date(assi.ia_assi_completedate), ' ', Substr(MonthName(assi.ia_assi_completedate), 1, 3)) As Due,
    Case
        When assi.ia_assi_permission = 855
        Then Concat('<a title="Assignment report" href = "index.php?option=com_content&view=article&id=',
            assi.ia_assi_formview, '&hash=', assi.ia_assi_token,
            '" target="_blank" rel="noopener noreferrer nofollow">', 'Report', '</a>')
        Else Concat('<a title="Assignment copy" href = "index.php?option=com_rsform&view=rsform&formId=',
            assi.ia_assi_formedit, '&recordId=', assi.ia_assi_auto, '&recordHash=', assi.ia_assi_token, '&action=copy',
            '&category=', assi.ia_assicate_auto, '" target="_blank" rel="noopener noreferrer nofollow">', 'Copy',
            '</a>', ' | ', '<a title="Assignment delete" href = "index.php?option=com_rsform&view=rsform&formId=',
            assi.ia_assi_formedit, '&recordId=', assi.ia_assi_auto, '&recordHash=', assi.ia_assi_token,
            '&action=delete', '&category=', assi.ia_assicate_auto,
            '" target="_blank" rel="noopener noreferrer nofollow">', 'Delete', '</a>', ' | ',
            '<a title="Assignment wip" href = "index.php?option=com_rsform&view=rsform&formId=',
            assi.ia_assi_formdelete, '&recordId=', assi.ia_assi_auto, '&recordHash=', assi.ia_assi_token,
            '&action=edit', '&category=', assi.ia_assicate_auto,
            '" target="_blank" rel="noopener noreferrer nofollow">', 'WIP', '</a>', ' | ',
            '<a title="Assignment report" href = "index.php?option=com_content&view=article&id=', assi.ia_assi_formview,
            '&hash=', assi.ia_assi_token, '" target="_blank" rel="noopener noreferrer nofollow">', 'Report', '</a>')
    End As Actions,
    Case
        When Count(find.ia_find_auto) > 1
        Then
            Concat('<a title="Finding add" href = "index.php?option=com_rsform&view=rsform&formId=15&recordId=0&recordHash=0&action=add&assignment=', assi.ia_assi_auto, '" target="_blank" rel="noopener noreferrer nofollow">Add</a>', ' | ', '<a title="Findings" href = "index.php?option=com_content&view=article&id=25&aid=', to_base64(Concat('1:', assi.ia_assi_auto)), '" target="_self" rel="noopener noreferrer nofollow">', Concat(Cast(Count(find.ia_find_auto) As Character), 'Findings'), '</a>')
        When Count(find.ia_find_auto) > 0
        Then
            Concat('<a title="Finding add" href = "index.php?option=com_rsform&view=rsform&formId=15&recordId=0&recordHash=0&action=add&assignment=', assi.ia_assi_auto, '" target="_blank" rel="noopener noreferrer nofollow">Add</a>', ' | ', '<a title="Findings" href = "index.php?option=com_content&view=article&id=25&aid=', to_base64(Concat('1:', assi.ia_assi_auto)), '" target="_self" rel="noopener noreferrer nofollow">', Concat(Cast(Count(find.ia_find_auto) As Character), 'Finding'), '</a>')
        Else
            Concat('<a title="Finding add" href = "index.php?option=com_rsform&view=rsform&formId=15&recordId=0&recordHash=0&action=add&assignment=', assi.ia_assi_auto, '" target="_blank" rel="noopener noreferrer nofollow">Add</a>')
    End As Findings
From
    ia_assignment assi Left Join
    ia_finding find On find.ia_assi_auto = assi.ia_assi_auto Left Join
    ia_assignment_category cate On cate.ia_assicate_auto = assi.ia_assicate_auto Left Join
    ia_assignment_type type On type.ia_assitype_auto = assi.ia_assitype_auto Left Join
    ia_assignment_status stat On stat.ia_assistat_auto = assi.ia_assistat_auto Left Join
    jm4_users user On user.id = assi.ia_user_sysid
Where
    Case
        When Month(Date(Now())) Not In ('10', '11', '12')
        Then (assi.ia_user_sysid = 855 And assi.ia_assi_year = Year(Date_Add(Now(), Interval -1 Year))) Or
            (assi.ia_user_sysid = 855 And assi.ia_assi_year < Year(Date_Add(Now(), Interval -1 Year)) And
            assi.ia_assi_priority < 9) Or
            (assi.ia_user_sysid = 855 And assi.ia_assi_finishdate Between Concat(Year(Date_Add(Now(), Interval
            -2 Year)), '1001') And Concat(Year(Date_Add(Now(), Interval -1 Year)), '0930'))
        Else (assi.ia_user_sysid = 855 And assi.ia_assi_year = Year(Date(Now()))) Or
            (assi.ia_user_sysid = 855 And assi.ia_assi_year < Year(Date(Now())) And assi.ia_assi_priority < 9) Or
            (assi.ia_user_sysid = 855 And assi.ia_assi_finishdate Between Concat(Year(Date_Add(Now(), Interval
            -1 Year)), '1001') And Concat(Year(Date(Now())), '0930'))
    End
Group By
    cate.ia_assicate_name,
    type.ia_assitype_name,
    assi.ia_assi_name,
    assi.ia_assi_auto