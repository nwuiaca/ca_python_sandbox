Select
    faud.ia_findaud_auto As id,
    faud.ia_findaud_name As name
From
    ia_finding_audit faud
Where
    faud.ia_findaud_active = 1