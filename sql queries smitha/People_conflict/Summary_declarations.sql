Select
    x003d.CATEGORY,
    x003d.PERSON_TYPE,
    x003d.DECLARED,
    Count(x003d.AGE) As EMP_COUNT
From
    X003_dashboard_curr x003d
Group By
    x003d.CATEGORY,
    x003d.PERSON_TYPE,
    x003d.DECLARED