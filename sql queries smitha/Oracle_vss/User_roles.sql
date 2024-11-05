Select
    us.KUSERCODE,
    us.FUSERBUSINESSENTITYID,
    us.FORGUNITBUSENTITYID,
    us.FBUSINESSAREAID,
    us.STARTDATE,
    us.ENDDATE,
    us.FUSERTYPECODEID,
    us.DATELASTLOGON,
    ro.KROLEID,
    ro.STARTDATE As ROLE_START,
    ro.ENDDATE As ROLE_END,
    Upper(rn.NAME) As NAME,
    Upper(rn.DESCRIPTION) As DESCRIPTION
From
    SYSTEMUSER us Inner Join
    SYSTEMUSERROLE ro On ro.KUSERCODE = us.KUSERCODE Inner Join
    SYSTEMROLENAME rn On rn.KROLEID = ro.KROLEID
            And rn.KSYSTEMLANGUAGECODE = 3
Where
    SysDate Between us.STARTDATE And Case
        When us.ENDDATE Is Null
        Then To_Date('2099-12-30', 'YYYY-mm-dd')
        Else us.ENDDATE
    End
Order By
    us.DATELASTLOGON Desc,
    us.STARTDATE,
    rn.NAME,
    ROLE_START,
    ROLE_END