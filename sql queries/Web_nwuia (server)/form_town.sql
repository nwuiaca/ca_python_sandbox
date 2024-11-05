Select
    town.town_id,
    town.town_coun_iso2,
    town.town_name,
    town.town_suburb,
    town.town_code,
    town.town_dial,
    town.town_coordinates
From
    def_town town
Where
    -- town.town_id = '". $id."'
    town.town_id = 1