Select
    leav.PERSON_ID,
    leav.DATE_START,
    leav.DATE_END,
    leav.LEAVE_TYPE,
    Count(leav.UPDATE_LOGIN) As COUNT_ROWS,
    Sum(leav.ABSENCE_DAYS) As SUM_DAYS
From
    X000_Leave_log leav
Where
    leav.DATE_START Like ('2024%') And
    leav.LEAVE_REASON_DESCRIPTION != 'REJECTED'
Group By
    leav.PERSON_ID,
    leav.DATE_START,
    leav.DATE_END,
    leav.LEAVE_TYPE