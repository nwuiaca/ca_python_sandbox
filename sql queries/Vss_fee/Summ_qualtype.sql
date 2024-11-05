Select
    STUD.QUAL_TYPE,
    Count(STUD.VALID) As COUNT
From
    X020bx_Student_master_sort STUD
Group By
    STUD.QUAL_TYPE