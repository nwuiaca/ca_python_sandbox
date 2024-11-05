Select
    ass.File,
    fin.ia_find_auto As File_finding,
    rem.ia_findreme_auto As File_remediation,
    Upper(rem.ia_findreme_name) As Audit_client,
    Lower(rem.ia_findreme_mail) As Audit_client_email,
    Upper(ass.Auditor) As Auditor,
    Lower(rem.ia_findreme_auditor_email) As Auditor_email,
    ass.Assignment_status_calc As AssStatus,
    ass.Assignment,
    rem.ia_findreme_mail_trigger As RemStatus,
    rem.ia_findresp_auto As Response,
    rem.ia_findresp_desc As Response_description,
    Case
        When Lower(rem.ia_findreme_mail) In ('21162395@nwu.ac.za', 'madelein.vandermerwe@nwu.ac.za',
            'nicolene.botha@nwu.ac.za', '12119180@nwu.ac.za', '12788074@nwu.ac.za')
        Then 1
        Else 0
    End As Exclude_record
From
    X000_Assignment_curr ass Inner Join
    ia_finding fin On fin.ia_assi_auto = ass.File Inner Join
    ia_finding_remediation rem On rem.ia_find_auto = fin.ia_find_auto
Where
    ass.Assignment_status_calc Like ('9%') And
    rem.ia_findreme_mail_trigger = 0 And
    rem.ia_findresp_desc <> ''