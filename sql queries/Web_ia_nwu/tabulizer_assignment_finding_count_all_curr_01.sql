Select
    assi.ia_assi_auto As FileNo,
    user.ia_user_name As Auditor,
    assi.ia_assi_year As Year,
    cate.ia_assicate_name As Category,
    type.ia_assitype_name As Type,
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
    Date(assi.ia_assi_startdate) As StartDate,
    Date(assi.ia_assi_completedate) As DueDate,
    Date(assi.ia_assi_finishdate) As CloseDate,
    assi.ia_assi_offi As Official,
    Case
        When Count(find.ia_find_auto) > 1
        Then Concat('<a href = "https://www.ia-nwu.co.za/index.php?option=com_content&view=article&id=26&rid=',
            to_base64(Concat('1:', assi.ia_assi_auto)), '" target="_blank" rel="noopener noreferrer">',
            Concat(Cast(Count(find.ia_find_auto) As Character), 'Findings'), '</a>')
        When Count(find.ia_find_auto) > 0
        Then Concat('<a href = "https://www.ia-nwu.co.za/index.php?option=com_content&view=article&id=26&rid=',
            to_base64(Concat('1:', assi.ia_assi_auto)), '" target="_blank" rel="noopener noreferrer">',
            Concat(Cast(Count(find.ia_find_auto) As Character), 'Finding'), '</a>')
    End As Findings
From
    ia_assignment assi Left Join
    ia_assignment_category cate On cate.ia_assicate_auto = assi.ia_assicate_auto Left Join
    ia_assignment_type type On type.ia_assitype_auto = assi.ia_assitype_auto Left Join
    ia_assignment_status stat On stat.ia_assistat_auto = assi.ia_assistat_auto Left Join
    ia_user user On user.ia_user_sysid = assi.ia_user_sysid Left Join
    ia_finding find On find.ia_assi_auto = assi.ia_assi_auto
Where
    (assi.ia_assi_year = Year(Now()) And
        user.ia_user_active = '1' And
        cate.ia_assicate_private = '0') Or
    (assi.ia_assi_year < Year(Now()) And
        assi.ia_assi_priority < 9 And
        user.ia_user_active = '1' And
        cate.ia_assicate_private = '0') Or
    (Date(assi.ia_assi_finishdate) >= Date_Sub(Concat(Year(Now()), '-10-01'), Interval 1 Year) And
        Date(assi.ia_assi_finishdate) <= Date_Sub(Concat(Year(Now()), '-10-01'), Interval 1 Day) And
        user.ia_user_active = '1' And
        cate.ia_assicate_private = '0')
Group By
    user.ia_user_name,
    cate.ia_assicate_name,
    type.ia_assitype_name,
    assi.ia_assi_name,
    assi.ia_assi_auto