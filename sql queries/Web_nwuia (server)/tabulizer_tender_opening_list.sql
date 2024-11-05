Select
    tend.ia_tend_number,
    teno.ia_teno_name,
    teno.ia_teno_document,
umberi    teno.ia_teno_docreference,
    teno.ia_teno_item,
    teno.ia_teno_note,
    teno.ia_teno_amount,
    Concat(
    '<a title="Submission add" href="index.php?option=com_rsform&view=rsform&formId=', teno.ia_teno_formedit, '&recordId=0', '&recordHash=0', '&action=add&tender=', teno.ia_tend_auto, '" rel="noopener nofollow noreferrer">Add</a>', ' | ',
    '<a title="Submission edit" href="index.php?option=com_rsform&view=rsform&formId=', teno.ia_teno_formedit, '&recordId=', teno.ia_teno_auto, '&recordHash=', teno.ia_teno_hash, '&action=edit&tender=', teno.ia_tend_auto, '" rel="noopener nofollow noreferrer">Edit</a>', ' | ',
    '<a title="Submission copy" href="index.php?option=com_rsform&view=rsform&formId=', teno.ia_teno_formedit, '&recordId=', teno.ia_teno_auto, '&recordHash=', teno.ia_teno_hash, '&action=copy&tender=', teno.ia_tend_auto, '" rel="noopener nofollow noreferrer">Copy</a>', ' | ',
    '<a title="Submission delete" href="index.php?option=com_rsform&view=rsform&formId=', teno.ia_teno_formedit, '&recordId=', teno.ia_teno_auto, '&recordHash=', teno.ia_teno_hash, '&action=delete&tender=', teno.ia_tend_auto, '" rel="noopener nofollow noreferrer">Delete</a>'
    ) as Actions
From
    ia_tender_opening teno Inner Join
    ia_tender tend On tend.ia_tend_auto = teno.ia_tend_auto
Where
    teno.ia_tend_auto = 834
Group By
    teno.ia_teno_name,
    teno.ia_teno_item
