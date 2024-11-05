Select
    'Assignment overdue' As Test,
    assc.File,
    assc.Auditor,
    assc.Year,
    assc.Category,
    assc.Type,
    assc.Priority_word As AssPriority,
    assc.Assignment_status_calc As AssStatus,
    Case
        When assc.Date_case_reported <> ''
        Then assc.Date_case_reported
        Else assc.Date_opened
    End As Date_opened,
    Case
        When assc.Date_due_si <> ''
        Then assc.Date_due_si
        Else assc.Date_due
    End As Date_due,
    Case
        When assc.Days_due_si > 0
        Then assc.Days_due_si
        Else assc.Days_due
    End As Days_overdue,
    assc.Assignment,
    assc.ia_user_mail,
    assc.Email_manager1,
    assc.Email_manager2
From
    X000_Assignment_curr assc
Where
    Case
        When assc.Days_due_si > 0
        Then assc.Days_due_si
        When assc.Days_due > 0 And assc.Date_due_si = ''
        Then assc.Days_due
        Else 0
    End > 0
Order By
    assc.Auditor,
    Days_overdue Desc,
    assc.Assignment