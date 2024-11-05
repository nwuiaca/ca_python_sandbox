Select
    tend.ia_tend_year,
    user.name,
    tend.ia_tend_owner_name,
    cate.ia_assicate_name,
    tend.ia_tend_number,
    tend.ia_tend_description,
    tend.ia_tend_dateclose,
    tend.ia_tend_auditor_name,
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
    Concat('<a title="Tender edit" href = "index.php?option=com_rsform&view=rsform&formId=', tend.ia_tend_formedit,
    '&recordId=', tend.ia_tend_auto, '&recordHash=', tend.ia_tend_token,
    '&action=edit" target="_blank" rel="noopener noreferrer nofollow">Edit</a>') As Actions,
    Case
        When Count(teno.ia_teno_auto) > 1 And tend.ia_tend_status < 2 And tend.ia_tend_dateclose <= CurDate()
        Then
            Concat('<a title="Submission add" href = "index.php?option=com_rsform&view=rsform&formId=28&recordId=0&recordHash=0&action=add&tender=', tend.ia_tend_auto, '" target="_blank" rel="noopener noreferrer nofollow">Add</a>', ' | ', '<a title="Submissions" href = "index.php?option=com_content&view=article&id=50&tid=', tend.ia_tend_auto, '" target="_self" rel="noopener noreferrer nofollow">', Concat(Cast(Count(teno.ia_teno_auto) As Character), 'Submissions'), '</a>', ' | ', '<a title="Submission certificate" href = "index.php?option=com_content&view=article&id=51', '&hash=', tend.ia_tend_token, '" target="_blank" rel="noopener noreferrer nofollow">', 'Certificate', '</a>', ' | ', '<a title="Submission close" href = "index.php?option=com_rsform&view=rsform&formId=30&recordId=', tend.ia_tend_auto, '&recordHash=', tend.ia_tend_token, '" target="_blank" rel="noopener noreferrer nofollow">Close</a>')
        When Count(teno.ia_teno_auto) > 0 And tend.ia_tend_status < 2 And tend.ia_tend_dateclose <= CurDate()
        Then
            Concat('<a title="Submission add" href = "index.php?option=com_rsform&view=rsform&formId=28&recordId=0&recordHash=0&action=add&tender=', tend.ia_tend_auto, '" target="_blank" rel="noopener noreferrer nofollow">Add</a>', ' | ', '<a title="Submissions" href = "index.php?option=com_content&view=article&id=50&tid=', tend.ia_tend_auto, '" target="_self" rel="noopener noreferrer nofollow">', Concat(Cast(Count(teno.ia_teno_auto) As Character), 'Submission'), '</a>', ' | ', '<a title="Submission certificate" href = "index.php?option=com_content&view=article&id=51', '&hash=', tend.ia_tend_token, '" target="_blank" rel="noopener noreferrer nofollow">', 'Certificate', '</a>', ' | ', '<a title="Submission close" href = "index.php?option=com_rsform&view=rsform&formId=30&recordId=', tend.ia_tend_auto, '&recordHash=', tend.ia_tend_token, '" target="_blank" rel="noopener noreferrer nofollow">Close</a>')
        When tend.ia_tend_status < 2 And tend.ia_tend_dateclose <= CurDate()
        Then
            Concat('<a title="Submission add" href = "index.php?option=com_rsform&view=rsform&formId=28&recordId=0&recordHash=0&action=add&tender=', tend.ia_tend_auto, '" target="_blank" rel="noopener noreferrer nofollow">Add</a>')
        When Count(teno.ia_teno_auto) > 1 And tend.ia_tend_dateclose <= CurDate()
        Then Concat('<a title="Submission certificate" href = "index.php?option=com_content&view=article&id=51',
            '&hash=', tend.ia_tend_token, '" target="_blank" rel="noopener noreferrer nofollow">', 'Certificate',
            '</a>')
        Else ''
    End As submissions
From
    ia_tender tend Left Join
    ia_tender_opening teno On teno.ia_tend_auto = tend.ia_tend_auto Left Join
    ia_assignment_category cate On cate.ia_assicate_auto = tend.ia_assicate_auto Left Join
    jm4_users user On user.id = tend.ia_user_sysid
Where
    tend.ia_tend_status <= 1
Group By
    tend.ia_tend_year,
    tend.ia_tend_dateclose,
    tend.ia_tend_number