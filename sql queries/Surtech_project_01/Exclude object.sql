﻿Select
    kfs.LOOKUP_CODE,
    kfs.LOOKUP_DESCRIPTION
From
    X000_Own_kfs_lookups kfs
Where
    kfs.LOOKUP = 'TEST ' || 'VENDOR INVOICE GAP SIZE EXCLUDE OBJECT'