Select
    burs.student,
    burs.total_burs,
    burs.total_loan,
    burs.staff_discount,
    rela.EMP_NAME_FULL,
    rela.EMP_PERSON_TYPE,
    rela.KRELATEDBUSINESSENTITYID,
    rela.REL_TYPE,
    rela.REL_NAME_FULL,
    rela.REL_PERSON_TYPE,
    burs.active,
    burs.levy_category,
    burs.enrol_category,
    burs.qualification,
    burs.qualification_type,
    burs.discontinue_date,
    burs.discontinue_result,
    burs.discontinue_reason
From
    X001_Bursary_summary_student burs Left Join
    X000_Student_relationship rela On rela.KSTUDBUSENTID = burs.student
Where
    burs.staff_discount <> 0 And
    burs.total_loan <> 0