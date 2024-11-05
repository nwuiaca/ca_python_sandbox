Select
    coun.coun_id,
    coun.coun_name,
    coun.coun_iso2,
    coun.coun_iso3,
    coun.coun_ison,
    coun.coun_dial,
    coun.coun_zone
From
    def_country coun
Where
    -- coun.coun_id = '". $id."'
    coun.coun_id = 1