Select
    papf.employee_number,
    papf.name_address,
    papf.location,
    papf.position As primary_position,
    papf.position_name As primary_position_name,
    papf.job_name As primary_job,
    Upper(jgr.DISPLAYED_NAME) As displayed_name,
    rol.ATTRIBUTE2 As supplementary_position,
    rol.START_DATE As start_date,
    rol.END_DATE As end_date,
    rol.ATTRIBUTE1 As organization_id,
    pos.POSITION_ID,
    pos.EFFECTIVE_START_DATE,
    pos.EFFECTIVE_END_DATE,
    pos.POSITION_NAME
From
    X000_PEOPLE papf Inner Join
    PER_ROLES rol On rol.PERSON_ID = papf.person_id
            And StrfTime('%Y-%m-%d', '2024-02-13') Between rol.START_DATE And rol.END_DATE Inner Join
    PER_JOB_GROUPS jgr On jgr.JOB_GROUP_ID = rol.JOB_GROUP_ID Inner Join
    X000_POSITIONS pos On pos.POSITION = rol.ATTRIBUTE2
            And StrfTime('%Y-%m-%d', '2024-02-13') Between pos.EFFECTIVE_START_DATE And pos.EFFECTIVE_END_DATE
Where
    rol.ATTRIBUTE2 <> '' And
    jgr.DISPLAYED_NAME = 'NWU Supplementary Roles'