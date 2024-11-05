﻿Select
    "AS".KAPPLSECTIONID,
    "AS".FAPPLFORMID,
    "AS".FAPPLFORMSECTIONTYPECODEID,
    cdas.CODELONGDESCRIPTION
From
    APPLSECTION "AS" Inner Join
    CODEDESCRIPTION cdas On cdas.KCODEDESCID = "AS".FAPPLFORMSECTIONTYPECODEID
Where
    cdas.KSYSTEMLANGUAGECODE = 3