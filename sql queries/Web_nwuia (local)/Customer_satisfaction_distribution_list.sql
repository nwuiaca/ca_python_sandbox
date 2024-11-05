Select
    cus.File,
    cus.File_finding,
    cus.File_remediation,
    cus.Auditor,
    cus.Auditor_email,
    cus.AssStatus,
    cus.Assignment,
    cus.RemStatus,
    cus.Audit_client,
    cus.Audit_client_email,
    cus.Response,
    cus.Response_description,
    cus.Test_mail,
    cus.Exclude_record
From
    X002a_Customer_satisfaction_all cus
Group By
    cus.File,
    cus.Audit_client    