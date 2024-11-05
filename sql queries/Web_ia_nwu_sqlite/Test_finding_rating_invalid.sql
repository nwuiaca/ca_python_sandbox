Select
    'Finding rating invalid' As Test,
    find.auditor,
    find.year,
    find.Assignment,
    find.finding,
    find.wstatus As status,
    find.ia_user_mail,
    find.Email_manager1,
    find.Email_manager2
From
    X000_Finding_curr find
Where
    ((find.rating_value Is Null) Or
    (find.rating_value = '0') Or
    (find.likelihood_value Is Null) Or
    (find.likelihood_value = '0') Or
    (find.control_value Is Null) Or
    (find.control_value = '0')) 
Order By
    find.auditor,
    find.year,
    find.Assignment,
    find.finding