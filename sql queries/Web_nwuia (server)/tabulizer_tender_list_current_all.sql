Select
    tend.ia_tend_year,
    user.name,
    tend.ia_tend_owner_name,
    cate.ia_assicate_name,
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
    Concat('<a title="Tender edit" href = "index.php?option=com_rsform&view=rsform&formId=', tend.ia_tend_formedit, '&recordId=', tend.ia_tend_auto, '&recordHash=', tend.ia_tend_token,'&action=edit" target="_blank" rel="noopener noreferrer nofollow">Edit</a>') as Actions
From
    ia_tender tend Left Join
    ia_assignment_category cate On cate.ia_assicate_auto = tend.ia_assicate_auto Left Join
    jm4_users user On user.id = tend.ia_user_sysid
Where
    tend.ia_tend_year = Year(Date(Now()))
Group By
    tend.ia_tend_auto