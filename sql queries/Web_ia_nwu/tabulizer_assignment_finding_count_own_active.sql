Select
    Concat(assi.ia_assi_year, '.', assi.ia_assi_file) As FileRef,
    cate.ia_assicate_name As Category,
    type.ia_assitype_name As Type,
    ia_user.ia_user_name As Owner,
    Concat('<a href = "index.php?option=com_rsform&view=rsform&formId=', assi.ia_assi_formedit, '&aid=',
    assi.ia_assi_auto, '&hash=', assi.ia_assi_token, '&category=', assi.ia_assicate_auto,
    '" target="_blank" rel="noopener noreferrer">', Concat(assi.ia_assi_name, ' (', assi.ia_assi_auto, ')'),
    '</a>') As Assignment,
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
        Then
            Concat('<a href = "index.php?option=com_rsform&view=rsform&formId=', assi.ia_assi_formedit, '&aid=', assi.ia_assi_auto, '&hash=', assi.ia_assi_token, '&category=', assi.ia_assicate_auto, '" target="_blank" rel="noopener noreferrer">', 'Edit', '</a>', ' | ', '<a href = "index.php?option=com_content&view=article&id=', assi.ia_assi_formview, '&hash=', assi.ia_assi_token, '" target="_blankh" rel="noopener noreferrer">', 'Report', '</a>')
        Else
            Concat('<a href = "index.php?option=com_rsform&view=rsform&formId=', assi.ia_assi_formedit, '&aid=', assi.ia_assi_auto, '&hash=', assi.ia_assi_token, '&category=', assi.ia_assicate_auto, '" target="_blank" rel="noopener noreferrer">', 'Edit', '</a>', ' | ', '<a href = "index.php?option=com_rsform&view=rsform&formId=', assi.ia_assi_formdelete, '&aid=', assi.ia_assi_auto, '&hash=', assi.ia_assi_token, '" target="_blank" rel="noopener noreferrer">', 'Delete', '</a>', ' | ', '<a href = "https://www.ia-nwu.co.za/index.php?option=com_rsform&view=rsform&formId=11&rid=', assi.ia_assi_auto, '&hash=', assi.ia_assi_token, '&category=', assi.ia_assicate_auto, '" target="_blank" rel="noopener noreferrer">', 'WIP</a>', ' | ', '<a href = "index.php?option=com_content&view=article&id=', assi.ia_assi_formview, '&hash=', assi.ia_assi_token, '" target="_blank" rel="noopener noreferrer">', 'Report', '</a>')
    End As Actions,
    Case
        When Count(find.ia_find_auto) > 1
        Then Concat('<a href = "index.php?option=com_rsform&view=rsform&formId=9&aid=', assi.ia_assi_auto,
            '" target="_blank" rel="noopener noreferrer">Add</a>', '|',
            '<a href = "index.php?option=com_content&view=article&id=26&rid=', to_base64(Concat('1:',
            assi.ia_assi_auto)), '" target="_blank" rel="noopener noreferrer">', Concat(Cast(Count(find.ia_find_auto) As
            Character), 'Findings'), '</a>')
        When Count(find.ia_find_auto) > 0
        Then Concat('<a href = "index.php?option=com_rsform&view=rsform&formId=9&aid=', assi.ia_assi_auto,
            '" target="_blank" rel="noopener noreferrer">Add</a>', '|',
            '<a href = "index.php?option=com_content&view=article&id=26&rid=', to_base64(Concat('1:',
            assi.ia_assi_auto)), '" target="_blank" rel="noopener noreferrer">', Concat(Cast(Count(find.ia_find_auto) As
            Character), 'Finding'), '</a>')
        Else Concat('<a href = "index.php?option=com_rsform&view=rsform&formId=9&aid=', assi.ia_assi_auto,
            '" target="_blank" rel="noopener noreferrer">Add</a>')
    End As Findings
From
    ia_assignment assi Left Join
    ia_finding find On find.ia_assi_auto = assi.ia_assi_auto Left Join
    ia_assignment_category cate On cate.ia_assicate_auto = assi.ia_assicate_auto Left Join
    ia_assignment_type type On type.ia_assitype_auto = assi.ia_assitype_auto Left Join
    ia_assignment_status stat On stat.ia_assistat_auto = assi.ia_assistat_auto Left Join
    ia_user On ia_user.ia_user_sysid = assi.ia_user_sysid
Where
    (assi.ia_user_sysid = 855 And
        assi.ia_assi_priority < 9) Or
    (assi.ia_assi_priority < 9 And
        assi.ia_assi_permission = 855)
Group By
    cate.ia_assicate_name,
    type.ia_assitype_name,
    assi.ia_assi_name,
    assi.ia_assi_auto