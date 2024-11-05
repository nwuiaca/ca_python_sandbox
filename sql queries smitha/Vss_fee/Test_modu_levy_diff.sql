Select
    stud.KENROLSTUDID,
    stud.STUDENT,
    stud.FMODULEAPID,
    stud.CAMPUS,
    stud.PRESENT_ID,
    stud.PRESENT_CATEGORY,
    stud.ENROL_ID,
    stud.ENROL_CATEGORY,
    stud.MODULE,
    stud.MODULE_NAME,
    stud.FINDING,
    fiab.AMOUNT,
    tran.TRAN_COUNT,
    tran.FEE_MODU
From
    X030ab_Stud_modu_list stud Inner Join
    X030aa_Fiabd007_summ fiab On fiab.FMODAPID = stud.FMODULEAPID
            And fiab.CAMPUS = stud.CAMPUS
            And fiab.PRESENT_CAT = stud.PRESENT_CATEGORY
            And fiab.ENROL_CATEGORY = stud.ENROL_CATEGORY Inner Join
    X030bb_Trans_feemodu_stud tran On tran.STUDENT = stud.STUDENT
            And tran.FMODAPID = stud.FMODULEAPID