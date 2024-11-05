Select
    coun.coun_name,
    town.town_name,
    town.town_suburb,
    town.town_code,
    town.town_dial,
    town.town_coordinates,
    Concat('<a href = "index.php?option=com_rsform&formId=', town.town_form, '&id=', town.town_id,
    '&action=edit">Edit</a>', ' | ', '<a href = "index.php?option=com_rsform&formId=', town.town_form, '&id=',
    town.town_id, '&action=copy">Copy</a>', ' | ', '<a href = "index.php?option=com_rsform&formId=', town.town_form,
    '&id=', town.town_id, '&action=delete">Delete</a>') As actions
From
    def_town town Inner Join
    def_country coun On coun.coun_iso2 = town.town_coun_iso2
Group By
    coun.coun_name,
    town.town_name,
    town.town_suburb