Select
    X000_Assignment_curr.Category As Category,
    Count(X000_Assignment_curr.File) As Assignment_count
From
    X000_Assignment_curr
Group By
    X000_Assignment_curr.Category
Order By
    Category