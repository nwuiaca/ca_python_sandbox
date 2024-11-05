Select
    X000_Student.RESULT,
    Count(X000_Student.KSTUDBUSENTID) As Count_KSTUDBUSENTID
From
    X000_Student
Group By
    X000_Student.RESULT