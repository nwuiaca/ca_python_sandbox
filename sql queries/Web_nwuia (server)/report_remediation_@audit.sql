Select
    ass.ia_assi_auto,
    fin.ia_find_auto,
    rem.ia_findreme_auto,
    ass.ia_user_sysid,
    usr.name,
    rem.ia_findreme_mail_trigger,
    rem.ia_findreme_name,
    rem.ia_findreme_date_schedule,
    rem.ia_findreme_date_send,
    rem.ia_findreme_date_open,
    rem.ia_findreme_date_submit,
    rem.ia_findreme_date_update,
    fin.ia_find_name,
    ass.ia_assi_name,
    rem.ia_findreme_createdate
From
    ia_finding_remediation rem Inner Join
    ia_finding fin On fin.ia_find_auto = rem.ia_find_auto Inner Join
    ia_assignment ass On ass.ia_assi_auto = fin.ia_assi_auto Inner Join
    jm4_users usr On usr.id = ass.ia_user_sysid
Where
    rem.ia_findreme_mail_trigger = 1
Order By
    usr.name,
    rem.ia_findreme_createdate