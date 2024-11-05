Select
    assi.ia_assi_auto As `File#`,
    user.ia_user_name As Auditor,
    assi.ia_assi_year As Year,
    cate.ia_assicate_name As Category,
    Type.ia_assitype_name As Type,
    assi.ia_assi_priority As `Priority#`,
    Case
        When assi.ia_assi_priority = 9
        Then "9-Completed"
        When assi.ia_assi_priority = 8
        Then "8-Continuous"
        When assi.ia_assi_priority = 4
        Then "4-Follow-up"
        When assi.ia_assi_priority = 3
        Then "3-High"
        When assi.ia_assi_priority = 2
        Then "2-Medium"
        When assi.ia_assi_priority = 1
        Then "1-Low"
        Else '0-NotStarted'
    End As Priority,
    asta.ia_assistat_name As Status,
    assi.ia_assi_startdate As Start_date,
    assi.ia_assi_completedate As Due_date,
    assi.ia_assi_proofdate As Report_date,
    assi.ia_assi_finishdate As Close_date,
    assi.ia_assi_name As Assignment
From
    ia_assignment assi Inner Join
    ia_assignment_category cate On cate.ia_assicate_auto = assi.ia_assicate_auto Inner Join
    ia_user user On user.ia_user_sysid = assi.ia_user_sysid Inner Join
    ia_assignment_type Type On Type.ia_assitype_auto = assi.ia_assitype_auto Inner Join
    ia_assignment_status asta On asta.ia_assistat_auto = assi.ia_assistat_auto
Where
    assi.ia_assi_priority In ('4', '9') And
    asta.ia_assistat_name != 'Completed'