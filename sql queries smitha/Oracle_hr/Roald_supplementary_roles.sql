Select
    papf.employee_number,
    papf.full_name,
    paaf.assignment_number,
    ppt.user_person_type,
    org_ass.organization_id ass_organization,
    ppd.segment1 position_number,
    ppd.segment2 position_name,
    ppd.segment4 hemis_category,
    loc.location_code,
    jgr.displayed_name,
    rol.job_id,
    jbt.name supplementaty_role,
    rol.start_date,
    rol.end_date,
    rol.attribute2 SUP_ROLE_POS
From
    per_roles rol,
    per_jobs job,
    per_jobs_tl jbt,
    per_job_groups jgr,
    hr_all_organization_units org,
    per_all_people_f papf,
    per_all_assignments_f paaf,
    hr_all_organization_units org_ass,
    hr_all_positions_f hapf,
    per_position_definitions ppd,
    hr_locations loc,
    per_person_type_usages_f pptuf,
    per_person_types ppt
Where
    rol.job_id = job.job_id And
    rol.job_group_id = jgr.job_group_id And
    job.job_id = jbt.job_id And
    rol.attribute1 = org.organization_id(+) And
    papf.person_id = rol.person_id And
    org_ass.organization_id = paaf.organization_id And
    paaf.person_id = papf.person_id And
    paaf.position_id = hapf.position_id(+) And
    hapf.position_definition_id = ppd.position_definition_Id(+) And
    loc.location_id = paaf.location_id And
    ppt.person_type_id = pptuf.person_type_id And
    pptuf.person_id = paaf.person_id And
    NVL(rol.end_date, SysDate) Between papf.effective_start_date And papf.effective_end_date And
    NVL(rol.end_date, SysDate) Between paaf.effective_start_date And paaf.effective_end_date And
    NVL(rol.end_date, '30-DEC-4712') >= Trunc(SysDate) And
    paaf.effective_end_date Between hapf.effective_start_date(+) And hapf.effective_end_date(+) And
    paaf.effective_end_date Between pptuf.effective_start_date And pptuf.effective_end_date And
    ppt.user_person_type != 'Retiree' And
    jgr.displayed_name = 'NWU Supplementary Roles'