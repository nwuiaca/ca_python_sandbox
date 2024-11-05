Select
    tran.STUDENT,
    X000_Student.CAMPUS,
    X000_Student.PRESENT_CAT,
    X030ab_Stud_modu_list.STUDENT As STUDENT1,
    tran.FMODAPID,
    tran.ENROL_ID,
    tran.ENROL_CAT,
    tran.TRAN_COUNT,
    tran.FEE_MODU,
    tran."MAX(TRAN.AUDITDATETIME)",
    tran.MODULE,
    tran.MODULE_NAME,
    tran.FUSERBUSINESSENTITYID,
    tran.NAME_ADDR,
    tran.FAUDITUSERCODE,
    tran.SYSTEM_DESC
From
    X030bb_Trans_feemodu_stud tran Left Join
    X030ab_Stud_modu_list On X030ab_Stud_modu_list.STUDENT = tran.STUDENT
            And X030ab_Stud_modu_list.FMODULEAPID = tran.FMODAPID Left Join
    X000_Student On X000_Student.KSTUDBUSENTID = tran.STUDENT
Where
    X000_Student.CAMPUS Is Not Null And
    X000_Student.PRESENT_CAT = "CONTACT" And
    X030ab_Stud_modu_list.STUDENT Is Null And
    tran.FEE_MODU > 0