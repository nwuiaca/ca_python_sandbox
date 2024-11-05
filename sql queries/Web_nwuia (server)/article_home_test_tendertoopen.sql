Select
    tend.ia_tend_number,
    tend.ia_tend_dateclose
From
    ia_tender tend
Where
    tend.ia_tend_auditor_id = 855 And
    tend.ia_tend_status = 1 And
    tend.ia_tend_dateclose <= CurDate()
Order By
    tend.ia_tend_dateclose,
    tend.ia_tend_number