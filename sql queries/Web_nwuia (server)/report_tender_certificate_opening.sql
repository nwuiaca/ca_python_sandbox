Select
    teno.ia_teno_name,
    teno.ia_teno_document,
    teno.ia_teno_docreference,
    teno.ia_teno_item,
    teno.ia_teno_amount,
    teno.ia_teno_note
From
    ia_tender_opening teno
Where
    teno.ia_tend_auto = 797
Order By
    teno.ia_teno_name,
    teno.ia_teno_item