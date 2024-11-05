Select
    log.STUDENT,
    log.KACCOMMRESIDENCYID,
    log.RESIDENCEID,
    log.NAME,
    Case
        When res.CAMPUS = -9
        Then "Mahikeng Campus"
        When res.CAMPUS = -2
        Then "Vanderbijlpark Campus"
        Else "Potchefstroom Campus"
    End As CAMPUS,
    log.FROOMTYPECODEID,
    log.ROOM_TYPE,
    log.STARTDATE,
    log.ENDDATE,
    log.FACCOMMCANCELCODEID,
    log.LONG,
    log.ACCOMMCANCELREASONOTHER
From
    X003_Previous_accom_log log Left Join
    X001_Previous_residence res On res.RESIDENCEID = log.RESIDENCEID
Where
    log.STARTDATE <= "2023-08-11" And
    log.ENDDATE >= "2023-08-11"
Order By
    log.STUDENT