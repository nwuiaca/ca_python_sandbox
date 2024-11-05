Select
    cate.ia_assicate_name As Category,
    type.ia_assitype_name As Type,
    Concat('<a href = "index.php?option=com_rsform&view=rsform&formId=', assi.ia_assi_formedit, '&aid=',
    assi.ia_assi_auto, '&hash=', assi.ia_assi_token, '&category=', assi.ia_assicate_auto,
    '" target="_blank" rel="noopener noreferrer">', Concat(assi.ia_assi_name, '(', assi.ia_assi_auto, ')'),
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
        Else 'Inactive'
    End As Priority,
    stat.ia_assistat_name As Status,
    assi.ia_assi_offi As Official,
    assi.ia_assi_desc As Own,
    Concat('<a href = "https://www.ia-nwu.co.za/index.php?option=com_rsform&view=rsform&formId=11&rid=',
    assi.ia_assi_auto, '&hash=', assi.ia_assi_token, '&category=', assi.ia_assicate_auto,
    '" target="_blank" rel="noopener noreferrer">', 'WIP</a>') As Actions
From
    ia_assignment assi Left Join
    ia_assignment_category cate On cate.ia_assicate_auto = assi.ia_assicate_auto Left Join
    ia_assignment_status stat On stat.ia_assistat_auto = assi.ia_assistat_auto Left Join
    ia_assignment_type type On type.ia_assitype_auto = assi.ia_assitype_auto
Where
    assi.ia_assi_priority < 9 And
    assi.ia_user_sysid = 855
Group By
    cate.ia_assicate_name,
    type.ia_assitype_name,
    assi.ia_assi_name,
    assi.ia_assi_auto