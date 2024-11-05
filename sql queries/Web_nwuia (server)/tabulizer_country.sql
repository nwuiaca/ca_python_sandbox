Select
    Concat('<a href = "https://en.wikipedia.org/wiki/', Lower(coun.coun_name), '" target="_blank">', coun.coun_name,
    '</a>') As coun_name,
    coun.coun_iso2,
    coun.coun_iso3,
    coun.coun_ison,
    coun.coun_dial,
    coun.coun_zone,
    Concat('<a href = "index.php?option=com_rsform&formId=', coun.coun_form, '&id=', coun.coun_id,
    '&action=edit">Edit</a>', ' | ', '<a href = "index.php?option=com_rsform&formId=', coun.coun_form, '&id=',
    coun.coun_id, '&action=copy">Copy</a>', ' | ', '<a href = "index.php?option=com_rsform&formId=', coun.coun_form,
    '&id=', coun.coun_id, '&action=delete">Delete</a>') As actions
From
    def_country coun
Group By
    coun.coun_name