Select
    faud.ia_findaud_auto As ID,
    faud.ia_findaud_name As Name,
    faud.ia_findaud_desc As Description,
    Case
        When faud.ia_findaud_active = 1
        Then 'Yes'
        Else 'No'
    End As Active,
    faud.ia_findaud_from As `From`,
    faud.ia_findaud_to As `To`,
    Concat('<a href = "index.php?option=com_rsform&view=rsform&formId=', faud.ia_findaud_formedit, '&idn=', faud.ia_findaud_auto, '&cop=0">', 'Edit', '</a>', ' | ', '<a href = "index.php?option=com_rsform&view=rsform&formId=', faud.ia_findaud_formedit, '&idn=', faud.ia_findaud_auto, '&cop=1">', 'Copy', '</a>', ' | ', '<a href = "index.php?option=com_rsform&view=rsform&formId=', faud.ia_findaud_formdelete, '&idn=', faud.ia_findaud_auto, '">Delete', '</a>') As Actions
From
    ia_finding_audit faud
Group By
    Case
        When faud.ia_findaud_active = 1
        Then 'Yes'
        Else 'No'
    End,
    faud.ia_findaud_name,
    faud.ia_findaud_auto