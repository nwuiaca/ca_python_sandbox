--Nominations - 3 years
--Voting - 3 months

select 
 papf.full_name full_name
,paaf.assignment_number
,papf.employee_number
,papf.last_name last_name
,papf.attribute4 preferred_name
,papf.email_address email_address
,papf.national_identifier
,papf.per_information2 passport
from
 per_all_people_f papf
,per_all_assignments_f paaf
,hr_all_positions_f hrp
,hr_all_organization_units hro
,per_periods_of_service pps
,per_person_type_usages_f pptuf
,per_person_types ppt
,hr_locations hrl
,per_position_definitions ppd
,per_jobs pj
,hr_lookups titlup
,per_grades pg
,per_all_people_f sup
,per_pay_bases ppb
,pay_people_groups ppg
,hr_lookups nat
,hr_lookups mar
,per_phones ph
,hr_lookups sex
,hr_lookups race
,per_assignment_status_types past
,pay_cost_allocation_keyflex pcak
,hr_lookups org
,hr_lookups dis
where
 papf.person_id = paaf.person_id
and paaf.assignment_status_type_id in (1)
and sysdate between paaf.effective_start_date and paaf.effective_end_date
and paaf.effective_end_date between papf.effective_start_date and papf.effective_end_date
and paaf.position_id = hrp.position_id(+)
and paaf.effective_end_date between hrp.effective_start_date(+) and hrp.effective_end_date(+)
and paaf.organization_id = hro.organization_id
and pps.person_id = papf.person_id
and ppt.user_person_type != 'Retiree'
and paaf.effective_end_date between pps.date_start and nvl(pps.actual_termination_date,'31-DEC-4712')
and pptuf.person_id = papf.person_id
and paaf.effective_end_date between pptuf.effective_start_date and pptuf.effective_end_date
and pptuf.person_type_id = ppt.person_type_id
and hrl.location_id = paaf.location_id
and ppd.position_definition_id(+) = hrp.position_definition_id
and pj.job_id(+) = hrp.job_id
and hrp.effective_end_date between pj.date_from(+) and nvl(pj.date_to(+),'31-DEC-4712')
and titlup.lookup_type = 'TITLE'
and titlup.lookup_code = papf.title
and paaf.grade_id = pg.grade_id(+)
and paaf.effective_end_date between pg.date_from(+) and nvl(pg.date_to(+),'31-DEC-4712')
and paaf.effective_end_date between sup.effective_start_date(+) and sup.effective_end_date(+)
and paaf.supervisor_id = sup.person_id(+)
and paaf.pay_basis_id = ppb.pay_basis_id(+)
and ppg.people_group_id(+) = paaf.people_group_id
and nat.lookup_type = 'NATIONALITY'
and nat.lookup_code = papf.nationality
and ppg.enabled_flag(+) = 'Y'
and mar.lookup_code = papf.marital_status
and mar.enabled_flag = 'Y'
and mar.lookup_type = 'MAR_STATUS'
and papf.person_id = ph.parent_id(+)
and ph.phone_type(+) = 'W1'
and sysdate between ph.date_from(+) and nvl(ph.date_to(+),'31-DEC-4712')
and sex.lookup_type = 'SEX'
and sex.lookup_code = papf.sex
and sex.enabled_flag = 'Y'
and race.lookup_type = 'ZA_RACE'
and race.lookup_code = papf.per_information4
and race.enabled_flag = 'Y'
and past.assignment_status_type_id = paaf.assignment_status_type_id
and past.active_flag = 'Y'
and pcak.cost_allocation_keyflex_id(+) = hro.cost_allocation_keyflex_id
and org.lookup_type(+) = 'NWU_ORG_ENTITIES'
and org.lookup_code(+) = hrp.attribute22
and papf.registered_disabled_flag = dis.lookup_code(+)
and dis.lookup_type(+) = 'REGISTERED_DISABLED'
--and ppd.segment4 = '1' --HEMIS category 1 is Academic
and nvl(initcap(paaf.employee_category),decode(ppd.segment4,'1','Academic','Support')) = 'Academic'
--and round(months_between(xxnwu_hr_functions_pkg.get_assignment_end_date(paaf.assignment_id,sysdate),xxnwu_hr_functions_pkg.get_assignment_start_date(paaf.assignment_id,sysdate))) > 3
and ppt.user_person_type in ('Fixed Term Appointment','Permanent Appointment','Temp Fixed Term Contract','Extraordinary Appointment','Temporary Appointment')
and nvl(pps.actual_termination_date,'31-DEC-4712') >= add_months(sysdate,3)










//PRE-TAS
--Nominations - 3 years
--Voting - 3 months

select 
nvl(XXNWU_get_oe_parent_type(hro.name,null,'Faculty'),XXNWU_get_oe_parent_type(hro.name,null,'Division')),
 hrl.location_code location 
,xxnwu_oe_eng_name(hro.organization_id,sysdate) organization
--,xxnwu_oe_code(hro.organization_id, sysdate) oe_code
,papf.full_name full_name
--,paaf.assignment_number
,papf.employee_number
,titlup.meaning title
,papf.last_name last_name
,xxper_util.initials(substr(papf.first_name,1,decode(instr(papf.first_name,' '),0,length(papf.first_name)))||' '||papf.middle_names) initials
,papf.attribute4 preferred_name
,ppt.user_person_type
,decode(ppd.segment4,'1','Academic','Support') academic_support
,hrp.name position_string
,ppd.segment2 position_name
,xxper_util.get_element_screen_value(sysdate,   paaf.assignment_id,   'NWU Allowance NRF',   'Option') Nrf_option
--,pg.name grade
--,past.user_status status
--,to_char(xxnwu_hr_functions_pkg.get_assignment_start_date(paaf.assignment_id,sysdate),'DD-MON-YYYY') assignment_start_date
--,to_char(xxnwu_hr_functions_pkg.get_assignment_end_date(paaf.assignment_id,sysdate),'DD-MON-YYYY') assignment_end_date
--
--,pj.name job
--,pcak.concatenated_segments org_costing
--,sup.employee_number supervisor_number
--,sup.full_name supervisor_name
--,ppb.name salary_basis
--,(select ac.meaning from hr_lookups ac where ac.lookup_type = 'EMP_CAT' and ac.lookup_code = paaf.employment_category) assignment_category
--,ppg.segment1 people_group
--,paaf.ass_attribute1 type_of_shift
--,paaf.ass_attribute2 joint_appt
,papf.email_address email_address
,papf.national_identifier
,papf.per_information2 passport
,nat.meaning nationality
--,mar.meaning marital_status
--,xxnwu_hr_functions_pkg.get_spouse_age(papf.person_id,sysdate) spouse_age
,ph.phone_number work_phone
, xxnwu_hr_functions_pkg.get_phone_number(papf.person_id, 'M',to_date(sysdate)) cellphone
--,sex.meaning gender
--,race.meaning race
--,papf.attribute2 internal_box
,decode(papf.attribute3,'A','Afrikaans','English') corr_language
--,paaf.assignment_id
--,dis.meaning disabled
--,to_char(papf.date_of_birth,'DD-MON-YYYY') date_of_birth
--,round(months_between(sysdate,papf.date_of_birth)/12,1) employee_age
from per_all_people_f papf
,per_all_assignments_f paaf
,hr_all_positions_f hrp
,hr_all_organization_units hro
,per_periods_of_service pps
,per_person_type_usages_f pptuf
,per_person_types ppt
,hr_locations hrl
,per_position_definitions ppd
,per_jobs pj
,hr_lookups titlup
,per_grades pg
,per_all_people_f sup
,per_pay_bases ppb
,pay_people_groups ppg
,hr_lookups nat
,hr_lookups mar
,per_phones ph
,hr_lookups sex
,hr_lookups race
,per_assignment_status_types past
,pay_cost_allocation_keyflex pcak
,hr_lookups org
,hr_lookups dis
where papf.person_id = paaf.person_id
and paaf.assignment_status_type_id in (1)
and sysdate between paaf.effective_start_date and paaf.effective_end_date
and paaf.effective_end_date between papf.effective_start_date and papf.effective_end_date
and paaf.position_id = hrp.position_id(+)
and paaf.effective_end_date between hrp.effective_start_date(+) and hrp.effective_end_date(+)
and paaf.organization_id = hro.organization_id
and pps.person_id = papf.person_id
and ppt.user_person_type != 'Retiree'
and paaf.effective_end_date between pps.date_start and nvl(pps.actual_termination_date,'31-DEC-4712')
and pptuf.person_id = papf.person_id
and paaf.effective_end_date between pptuf.effective_start_date and pptuf.effective_end_date
and pptuf.person_type_id = ppt.person_type_id
and hrl.location_id = paaf.location_id
and ppd.position_definition_id(+) = hrp.position_definition_id
and pj.job_id(+) = hrp.job_id
and hrp.effective_end_date between pj.date_from(+) and nvl(pj.date_to(+),'31-DEC-4712')
and titlup.lookup_type = 'TITLE'
and titlup.lookup_code = papf.title
and paaf.grade_id = pg.grade_id(+)
and paaf.effective_end_date between pg.date_from(+) and nvl(pg.date_to(+),'31-DEC-4712')
and paaf.effective_end_date between sup.effective_start_date(+) and sup.effective_end_date(+)
and paaf.supervisor_id = sup.person_id(+)
and paaf.pay_basis_id = ppb.pay_basis_id(+)
and ppg.people_group_id(+) = paaf.people_group_id
and nat.lookup_type = 'NATIONALITY'
and nat.lookup_code = papf.nationality
and ppg.enabled_flag(+) = 'Y'
and mar.lookup_code = papf.marital_status
and mar.enabled_flag = 'Y'
and mar.lookup_type = 'MAR_STATUS'
and papf.person_id = ph.parent_id(+)
and ph.phone_type(+) = 'W1'
and sysdate between ph.date_from(+) and nvl(ph.date_to(+),'31-DEC-4712')
and sex.lookup_type = 'SEX'
and sex.lookup_code = papf.sex
and sex.enabled_flag = 'Y'
and race.lookup_type = 'ZA_RACE'
and race.lookup_code = papf.per_information4
and race.enabled_flag = 'Y'
and past.assignment_status_type_id = paaf.assignment_status_type_id
and past.active_flag = 'Y'
and pcak.cost_allocation_keyflex_id(+) = hro.cost_allocation_keyflex_id
and org.lookup_type(+) = 'NWU_ORG_ENTITIES'
and org.lookup_code(+) = hrp.attribute22
and papf.registered_disabled_flag = dis.lookup_code(+)
and dis.lookup_type(+) = 'REGISTERED_DISABLED'
and ppd.segment4 = '1' --HEMIS category 1 is Academic
--and round(months_between(xxnwu_hr_functions_pkg.get_assignment_end_date(paaf.assignment_id,sysdate),xxnwu_hr_functions_pkg.get_assignment_start_date(paaf.assignment_id,sysdate))) > 3
and ppt.user_person_type in ('Fixed Term Appointment','Permanent Appointment','Temp Fixed Term Contract','Extraordinary Appointment','Temporary Appointment')
and nvl(pps.actual_termination_date,'31-DEC-4712') >= add_months(sysdate,3)
