Select
    'Follow-up no reminder' As Test,
    assc.File,
    assc.Auditor,
    assc.Year,
    assc.Category,
    assc.Type,
    assc.Priority_word As AssPriority,
    assc.Assignment_status_calc As AssStatus,
    fins.ia_findstat_name As FinStatus,
    assc.Date_reported As Report_final,
    reme.ia_findreme_name As Reminder_to,
    reme.ia_findreme_date_schedule As Remind_date,
    assc.Assignment,
    find.ia_find_name || ' (' || find.ia_find_auto || ')' As Finding,
    assc.ia_user_mail As Mail_user,
    assc.Email_manager1 As Mail_manager1,
    assc.Email_manager2 As Mail_manager2
From
    X000_Assignment_curr assc Left Join
    ia_finding find On find.ia_assi_auto = assc.File Inner Join
    ia_finding_status fins On fins.ia_findstat_auto = find.ia_findstat_auto Left Join
    ia_finding_remediation reme On reme.ia_find_auto = find.ia_find_auto
            And reme.ia_findreme_mail_trigger > 0
Where
    assc.Priority_word Like ('7%') And
    fins.ia_findstat_name = 'Request remediation'
Order By
    assc.Auditor,
    assc.Date_reported,
    assc.Assignment,
    find.ia_find_name
