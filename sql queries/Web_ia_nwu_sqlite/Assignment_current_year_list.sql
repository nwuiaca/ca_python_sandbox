﻿Select
    assi.ia_assi_auto As File,
    user.ia_user_name As Auditor,
    assi.ia_assi_year As Year,
    Case
        When assi.ia_assi_year = 2023
        Then 1
        When assi.ia_assi_priority < 9
        Then 2
        Else 3
    End As Year_indicator,
    cate.ia_assicate_name As Category,
    type.ia_assitype_name As Type,
    Case
        When cate.ia_assicate_name = 'Election'
        Then cate.ia_assicate_name
        When cate.ia_assicate_name = 'Continuous audit'
        Then cate.ia_assicate_name
        When cate.ia_assicate_name = 'Special investigation'
        Then cate.ia_assicate_name
        Else type.ia_assitype_name
    End As Type_calc,
    assi.ia_assi_priority As Priority_number,
    Case
        When assi.ia_assi_priority = 9
        Then 'Closed'
        When assi.ia_assi_priority = 8
        Then 'Continuous'
        When assi.ia_assi_priority = 4
        Then 'Follow-up'
        When assi.ia_assi_priority = 3
        Then 'High'
        When assi.ia_assi_priority = 2
        Then 'Medium'
        When assi.ia_assi_priority = 1
        Then 'Low'
        Else 'Inactive'
    End As Priority_word,
    asta.ia_assistat_name As Assignment_status,
    Case
        When SubStr(asta.ia_assistat_name, 1, 2) = '00'
        Then '1-NotStarted'
        When assi.ia_assi_priority = 8
        Then '8-Continuous'
        When assi.ia_assi_priority = 4
        Then '7-Follow-up'
        When Upper(SubStr(asta.ia_assistat_name, 1, 2)) = 'CO'
        Then '9-Completed'
        When Cast(SubStr(asta.ia_assistat_name, 1, 2) As Integer) >= 1 And Cast(SubStr(asta.ia_assistat_name, 1,
            2) As Integer) <= 10
        Then '2-Planning'
        When Cast(SubStr(asta.ia_assistat_name, 1, 2) As Integer) >= 11 And Cast(SubStr(asta.ia_assistat_name,
            1, 2) As Integer) <= 50
        Then '3-FieldworkInitial'
        When Cast(SubStr(asta.ia_assistat_name, 1, 2) As Integer) >= 51 And Cast(SubStr(asta.ia_assistat_name,
            1, 2) As Integer) <= 79
        Then '4-FieldworkFinal'
        When Cast(SubStr(asta.ia_assistat_name, 1, 2) As Integer) >= 80 And Cast(SubStr(asta.ia_assistat_name,
            1, 2) As Integer) <= 89
        Then '5-DraftReport'
        When Cast(SubStr(asta.ia_assistat_name, 1, 2) As Integer) >= 90 And Cast(SubStr(asta.ia_assistat_name,
            1, 2) As Integer) <= 99
        Then '6-FinalReport'
        Else 'Unknown'
    End As Assignment_status_calc,
    assi.ia_assi_name || ' (' || assi.ia_assi_auto || ')' As Assignment,
    assi.ia_assi_startdate As Date_opened,
    Case
        When Date(assi.ia_assi_startdate) < '2022-10-01'
        Then StrfTime('%Y', assi.ia_assi_startdate) || '-00'
        When Date(assi.ia_assi_startdate) < '2023-10-01'
        Then StrfTime('%Y-%m', assi.ia_assi_startdate)
        When Date(assi.ia_assi_startdate) >= '2022-10-01'
        Then StrfTime('%Y', assi.ia_assi_startdate) || '-00'
        Else StrfTime('%Y-%m', now())
    End As Date_opened_month,
    Case
        When Cast((StrfTime("%s", now()) - StrfTime("%s", assi.ia_assi_startdate)) / 86400.0 As Integer) > 0
        Then Cast((StrfTime("%s", now()) - StrfTime("%s", assi.ia_assi_startdate)) / 86400.0 As Integer)
        Else 0
    End As Date_opened_days,
    assi.ia_assi_completedate As Date_due,
    assi.ia_assi_proofdate As Date_reported,
    assi.ia_assi_finishdate As Date_closed,
    Case
        When assi.ia_assi_priority = 4
        Then assi.ia_assi_proofdate
        When assi.ia_assi_priority = 8
        Then Date(now())
        Else assi.ia_assi_finishdate
    End As Date_closed_calc,
    Case
        When assi.ia_assi_priority = 4
        Then StrfTime('%Y-%m', assi.ia_assi_proofdate)
        When assi.ia_assi_priority = 8
        Then StrfTime('%Y-%m', now())
        When Date(assi.ia_assi_finishdate) >= '2022-10-01' And Date(assi.ia_assi_finishdate) < '2023-10-01'
        Then StrfTime('%Y-%m', assi.ia_assi_finishdate)
        Else '00'
    End As Date_closed_month,
    Case
        When Cast((StrfTime("%s", assi.ia_assi_finishdate) - StrfTime("%s", assi.ia_assi_startdate)) / 86400.0
            As Integer) > 0
        Then Cast((StrfTime("%s", assi.ia_assi_finishdate) - StrfTime("%s", assi.ia_assi_startdate)) / 86400.0
            As Integer)
        Else 0
    End As Days_to_close,
    Case
        When Cast((StrfTime("%s", now()) - StrfTime("%s", assi.ia_assi_proofdate)) / 86400.0 As Integer) > 0
        Then Cast((StrfTime("%s", now()) - StrfTime("%s", assi.ia_assi_proofdate)) / 86400.0 As Integer)
        Else 0
    End As Days_due
From
    ia_assignment assi Inner Join
    ia_user user On user.ia_user_sysid = assi.ia_user_sysid Inner Join
    ia_assignment_status asta On asta.ia_assistat_auto = assi.ia_assistat_auto Inner Join
    ia_assignment_category cate On cate.ia_assicate_auto = assi.ia_assicate_auto Inner Join
    ia_assignment_type type On type.ia_assitype_auto = assi.ia_assitype_auto
Where
    (assi.ia_assi_year = 2023 And
        user.ia_user_active = '1' And
        cate.ia_assicate_private = '0') Or
    (assi.ia_assi_year < 2023 And
        assi.ia_assi_priority < 9 And
        user.ia_user_active = '1' And
        cate.ia_assicate_private = '0') Or
    (assi.ia_assi_finishdate >= '2022-10-01' And
        assi.ia_assi_finishdate <= '2023-09-30' And
        user.ia_user_active = '1' And
        cate.ia_assicate_private = '0')