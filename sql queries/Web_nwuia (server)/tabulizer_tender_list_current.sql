Select
    user.name,
    tend.ia_tend_owner_name,
    cate.ia_assicate_name,
    tend.ia_tend_datesubmit,
    tend.ia_tend_number,
    tend.ia_tend_description,
    tend.ia_tend_dateclose,
    tend.ia_tend_dateexpect,
    Case
        When tend.ia_tend_status = 1
        Then 'Submitting'
        When tend.ia_tend_status = 2
        Then 'Opened'
        When tend.ia_tend_status = 3
        Then 'Processing'
        When tend.ia_tend_status = 4
        Then 'Approval1'
        When tend.ia_tend_status = 5
        Then 'Approval2'
        When tend.ia_tend_status = 9
        Then 'Closed'
        Else 'New'
    End As ia_tend_status,
    case
        when ia_tend_status > 1
        then concat('<a title="Tender edit" href = "index.php?option=com_rsform&view=rsform&formId=31&recordId=', tend.ia_tend_auto, '&recordHash=', tend.ia_tend_token, '&action=edit" target="_blank" rel="noopener noreferrer nofollow">Edit</a>', ' | ', '<a title="Tender update" href = "index.php?option=com_rsform&view=rsform&formId=32&recordId=', tend.ia_tend_auto, '&recordHash=', tend.ia_tend_token, '&action=edit" target="_blank" rel="noopener noreferrer nofollow">Update</a>')
        else concat('<a title="Tender edit" href = "index.php?option=com_rsform&view=rsform&formId=31&recordId=', tend.ia_tend_auto, '&recordHash=', tend.ia_tend_token, '&action=edit" target="_blank" rel="noopener noreferrer nofollow">Edit</a>')
    end as Actions
From
    ia_tender tend Left Join
    ia_assignment_category cate On cate.ia_assicate_auto = tend.ia_assicate_auto Left Join
    jm4_users user On user.id = tend.ia_user_sysid
Where
    (tend.ia_user_sysid = 909 And
        tend.ia_tend_year = Year(Date(Now()))) Or
    (tend.ia_tend_permission = 909 And
        tend.ia_tend_year = Year(Date(Now())))
Group By
    tend.ia_tend_number,
    tend.ia_tend_auto
    