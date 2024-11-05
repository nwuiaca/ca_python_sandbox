Select
    'Assignment priority inconsistent' As Test,
    assc.File,
    assc.Auditor,
    assc.Year,
    assc.Category,
    assc.Type,
    assc.Priority_word As AssPriority,
    assc.Assignment_status_calc As AssStatus,
    assc.Date_closed_calc As Date_closed,
    assc.Assignment,
    assc.ia_user_mail,
    assc.Email_manager1,
    assc.Email_manager2
From
    X000_Assignment_curr assc
Where
    assc.Priority_word Not Like ('9%') And
    assc.Assignment_status_calc Like ('9%')
Order By
    assc.Auditor,
    assc.Category,
    assc.Type,
    assc.Year,
    assc.Assignment