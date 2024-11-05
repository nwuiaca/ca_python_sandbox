Select
    coun.coun_iso2 As value,
    coun.coun_name As label
From
    def_country coun
Group By
    coun.coun_name