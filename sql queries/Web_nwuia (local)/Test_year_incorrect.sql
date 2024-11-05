Select
    'Assignment year inconsistent' As Test,
    assc.File,
    assc.Auditor,
    assc.Year,
    assc.Year_calc As "Year should be",
    assc.Date_opened,
    assc.Assignment,
    assc.ia_user_mail,
    assc.Email_manager1,
    assc.Email_manager2
From
    X000_Assignment_curr assc
Where
    assc.Year != assc.Year_calc And
    assc.Year_calc > 0
Order By
    assc.Auditor,
    assc.Year,
    assc.Assignment