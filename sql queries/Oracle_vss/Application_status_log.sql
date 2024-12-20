﻿Select
    saslog.KSTUDYAPPLLOGID,
    saslog.FAPPLICATIONID,
    saslog.FACADPROGAPPLSTATUSCODEID,
    Upper(codlog.CODELONGDESCRIPTION) As CODELONGDESCRIPTION,
    saslog.DATEPROCESSED
From
    STUDYAPPLSTATUSLOG saslog Inner Join
    CODEDESCRIPTION codlog On codlog.KCODEDESCID = saslog.FACADPROGAPPLSTATUSCODEID
Where
    codlog.KSYSTEMLANGUAGECODE = 3