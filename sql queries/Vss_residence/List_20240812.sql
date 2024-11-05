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
    X003_Current_accom_log log Left Join
    X001_Active_residence res On res.RESIDENCEID = log.RESIDENCEID
Where
    log.STARTDATE <= "2024-08-12" And
    log.ENDDATE >= "2024-08-12"
Order By
    log.STUDENT