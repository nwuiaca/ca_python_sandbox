Select
    'Follow-up no finding' As Test,
    assc.File,
    assc.Auditor,
    assc.Year,
    assc.Category,
    assc.Type,
    assc.Priority_word As AssPriority,
    assc.Assignment_status_calc As AssStatus,
    fins.ia_findstat_name As FinStatus,
    assc.Date_reported As Report_final,
    assc.Assignment,
    find.ia_find_name || ' (' || find.ia_find_auto || ')' As Finding,
    assc.ia_user_mail As Mail_user,
    assc.Email_manager1 As Mail_manager1,
    assc.Email_manager2 As Mail_manager2
From
    X000_Assignment_curr assc Left Join
    ia_finding find On find.ia_assi_auto = assc.File Inner Join
    ia_finding_status fins On fins.ia_findstat_auto = find.ia_findstat_auto
Where
    assc.Priority_word Like ('7%') And
    fins.ia_findstat_name <> 'Request remediation'
Order By
    assc.Auditor,
    assc.Category,
    assc.Type,
    assc.Assignment,
    Finding