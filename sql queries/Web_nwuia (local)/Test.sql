Select
    '(' || cast(css.File as text) || ')' || css.Audit_client_email As Unique_id,
    css.Audit_client,
    css.Audit_client_email,
    css.Auditor,
    css.Auditor_email,
    css.Assignment
From
    X002a_Customer_satisfaction_all css
Where
    css.Exclude_record = 0
Group by
    '(' || cast(css.File as text) || ')' || css.Audit_client_email
Order by
    css.File