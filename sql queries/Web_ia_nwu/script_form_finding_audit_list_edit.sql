Select
    faud.ia_findaud_auto,
    faud.ia_findaud_name,
    faud.ia_findaud_desc,
    faud.ia_findaud_active,
    faud.ia_findaud_from,
    faud.ia_findaud_to,
    faud.ia_findaud_formedit,
    faud.ia_findaud_formview,
    faud.ia_findaud_formdelete
From
    ia_finding_audit faud
Where
    faud.ia_findaud_auto = 1