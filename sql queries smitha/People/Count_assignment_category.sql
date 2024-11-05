Select
    x000p.assignment_category,
    Count(x000p.assignment_id) As count
From
    X000_PEOPLE x000p
Group By
    x000p.assignment_category