Select
    burs.student,
    burs.total_burs,
    burs.total_loan,
    burs.total_external,
    burs.total_internal,
    burs.total_research,
    burs.total_trust,
    burs.total_other,
    burs.staff_discount,
    burs.active,
    burs.levy_category,
    burs.enrol_category,
    burs.qualification,
    burs.qualification_type,
    burs.discontinue_date,
    burs.discontinue_result,
    burs.discontinue_reason
From
    X001_Bursary_summary_student burs
Where
    (burs.total_burs <> 0 And
        burs.active = 'INACTIVE' And
        burs.total_loan = 0 And
        burs.enrol_category Not In ('POST DOC')) Or
    (burs.total_burs <> 0 And
        burs.discontinue_date Is Not Null And
        burs.total_loan = 0 And
        burs.enrol_category Not In ('POST DOC') and
        burs.discontinue_result Not In ('COURSE CONVERTED', 'PASS CERTIFICATE', 'PASS CERTIFICATE POSTHUMOUSLY', 'PASS CERTIFICATE WITH DISTINCTION', 'PASS DEGREE', 'PASS DEGREE POSTHUMOUSLY', 'PASS DEGREE WITH DISTINCTION', 'PASS DEGREE WITH DISTINCTION POSTHUMOUSLY', 'PASS DIPLOMA', 'PASS DIPLOMA WITH DISTINCTION'))