select papf.employee_number,
papf.full_name,
    paa.date_projected_start,
    paa.date_projected_end,
xxl.instance_number,
hro.oe_code,
hro.eng_name oe_name,
hro.responsible_person oe_head_number,
(select full_name from hr.per_all_people_f where employee_number = hro.responsible_person and sysdate between effective_start_date and effective_end_date) responsible_person_name,
    xxl.duration absence_days , 
    paat.name leave_type , 
    ppg.group_name ,
    paa.attribute1 reference,
    to_date(paa.attribute2,'yyyy/mm/dd HH24:MI:SS') form_date,
    (select meaning from apps.hr_lookups where lookup_code = paar.name
    and lookup_type = 'ABSENCE_REASON') reason,
    'Pending' status,
     paa.attribute3 external_reference,
    (select employee_number from hr.per_all_people_f where person_id = paa.attribute5 and to_date(paa.attribute2,'yyyy/mm/dd HH24:MI:SS') between effective_start_date and effective_end_date) auth_nr,
    (select full_name from hr.per_all_people_f where person_id = paa.attribute5 and to_date(paa.attribute2,'yyyy/mm/dd HH24:MI:SS') between effective_start_date and effective_end_date) auth_name
  FROM hr.per_absence_attendances paa , 
    hr.per_absence_attendance_types paat , 
    hr.per_all_people_f papf , 
    hr.per_abs_attendance_reasons paar , 
    hr.per_all_assignments_f paaf , 
    hr.pay_people_groups ppg,
    apps.xxnwu_hr_leave_service xxl,
    xxnwu_hr_ods_organizations hro
  WHERE paat.absence_attendance_type_id  = paa.absence_attendance_type_id 
  AND papf.person_id                   = paa.person_id 
  AND paar.abs_attendance_reason_id(+) = paa.abs_attendance_reason_id 
--  and papf.employee_number = :employee_number 
  AND paaf.person_id              = papf.person_id 
  and ppg.people_group_id = paaf.people_group_id 
  AND paa.date_projected_start BETWEEN paaf.effective_start_date AND paaf.effective_end_date 
  AND paa.date_projected_start BETWEEN papf.effective_start_date AND papf.effective_end_date   
   and paa.date_start is null
   and xxl.employee_number = papf.employee_number
   and paa.attribute3 = xxl.external_reference 
   and substr(xxl.leave_type,1,30) = substr(paat.name,1,30)
   and xxl.start_date = paa.date_projected_start
   and xxl.action = 'Pending'
   and paaf.organization_id = hro.organization_id
   and  to_date(paa.attribute2,'yyyy/mm/dd HH24:MI:SS') between hro.name_start_date and nvl(hro.name_end_date,'31-DEC-4712')
   and (paa.date_projected_end <= v_date or paa.date_projected_start <= v_date)
