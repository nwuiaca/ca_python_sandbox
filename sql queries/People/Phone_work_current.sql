Select
    phon.PHONE_ID,
    phon.PARENT_ID,
    phon.PARENT_TABLE,
    phon.PHONE_TYPE,
    Max(phon.DATE_FROM) As DATE_FROM,
    phon.DATE_TO,
    phon.PHONE_NUMBER
From
    PER_PHONES phon
Where
    phon.PARENT_TABLE = 'PER_ALL_PEOPLE_F' And
    phon.PHONE_TYPE = 'W1' And
    phon.DATE_FROM <= now() And
    phon.DATE_TO >= now()
Group By
    phon.PARENT_ID