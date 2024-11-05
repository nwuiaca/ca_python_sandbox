Select
    X000_PEOPLE.type_of_shift,
    X000_PEOPLE.leave_code,
    Count(X000_PEOPLE.people_update_by) As Count_people
From
    X000_PEOPLE
Group By
    X000_PEOPLE.type_of_shift,
    X000_PEOPLE.leave_code