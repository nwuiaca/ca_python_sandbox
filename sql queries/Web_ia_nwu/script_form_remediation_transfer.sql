Select
    reme.ia_findreme_auto,
    reme.ia_find_auto,
    reme.ia_findreme_response,
    reme.ia_findrate_auto,
    reme.ia_findlike_auto,
    reme.ia_findcont_auto,
    find.ia_find_comment,
    reme.ia_findreme_name,
    reme.ia_findreme_date_submit,
    reme.ia_findreme_mail_trigger
From
    ia_finding_remediation reme Inner Join
    ia_finding find On find.ia_find_auto = reme.ia_find_auto
Where
    reme.ia_findreme_auto = 1