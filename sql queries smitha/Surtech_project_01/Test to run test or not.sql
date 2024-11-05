Select
    kfs.LOOKUP_DESCRIPTION
From
    X000_Own_kfs_lookups kfs
Where
    kfs.LOOKUP = 'TEST ' || 'VENDOR INVOICE GAP SIZE' And
    kfs.LOOKUP_CODE = 'RUN'