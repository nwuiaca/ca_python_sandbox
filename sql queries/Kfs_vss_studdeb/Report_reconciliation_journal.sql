Select
    REC.CAMPUS,
    REC.MONTH,
    REC.TRANCODE,
    REC.GL_DESCRIPTION,
    REC.GL_AMOUNT,
    REC.DIFF,
    REC.MATCHED,
    REC.PERIOD,
    REC."CURRENT"
From
    X003ax_vss_gl_join REC
Where
    REC.TRANCODE = 'X' And
    REC.MATCHED = 'X'