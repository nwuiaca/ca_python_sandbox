Select
    reme.ia_findreme_auto,
    assi.ia_assi_auto,
    assi.ia_assi_name,
    find.ia_find_auto,
    find.ia_find_name
From
    ia_finding_remediation reme Inner Join
    ia_finding find On find.ia_find_auto = reme.ia_find_auto Inner Join
    ia_assignment assi On assi.ia_assi_auto = find.ia_assi_auto
Where
    reme.ia_findreme_mail_trigger > 0 And
    reme.ia_findreme_schedule > 0 And
    reme.ia_findreme_date_schedule <= Now() And
    find.ia_user_sysid = 855