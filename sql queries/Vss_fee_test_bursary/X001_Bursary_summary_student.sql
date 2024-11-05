Select
    tran.STUDENT As student,
    Total(Distinct tran.AMOUNT) As total_burs,
    Total(Distinct loan.AMOUNT) As total_loan,
    Total(Distinct exte.AMOUNT) As total_external,
    Total(Distinct inte.AMOUNT) As total_internal,
    Total(Distinct rese.AMOUNT) As total_research,
    Total(Distinct trus.AMOUNT) As total_trust,
    Total(Distinct othe.AMOUNT) As total_other,
    staf.TRAN_VALUE As staff_discount,
    stud.ACTIVE_IND As active,
    stud.LEVY_CATEGORY As levy_category,
    stud.ENROL_CAT As enrol_category,
    stud.QUALIFICATION_NAME As qualification,
    stud.QUAL_TYPE As qualification_type,
    stud.DISCONTINUEDATE As discontinue_date,
    stud.RESULT As discontinue_result,
    stud.DISCONTINUE_REAS As discontinue_reason
From
    X000_Transaction tran Left Join
    X000_Transaction loan On loan.STUDENT = tran.STUDENT
            And loan.SOURCE = 'BURSARY-LOAN SCHEMA' Left Join
    X000_Transaction exte On exte.STUDENT = tran.STUDENT
            And exte.SOURCE = 'EXTERNAL FUND' Left Join
    X000_Transaction inte On inte.STUDENT = tran.STUDENT
            And inte.SOURCE = 'UNIVERSITY FUND' Left Join
    X000_Transaction rese On rese.STUDENT = tran.STUDENT
            And rese.SOURCE = 'NRF (RESEARCH FUND)' Left Join
    X000_Transaction trus On trus.STUDENT = tran.STUDENT
            And trus.SOURCE = 'DONATE/TRUST FUND' Left Join
    X000_Transaction othe On othe.STUDENT = tran.STUDENT
            And othe.SOURCE Not In ('BURSARY-LOAN SCHEMA', 'EXTERNAL FUND', 'UNIVERSITY FUND', 'NRF (RESEARCH FUND)',
            'DONATE/TRUST FUND') Left Join
    X000_Student stud On stud.KSTUDBUSENTID = tran.STUDENT Left Join
    X000_Transaction_staffdisc_student staf On staf.STUDENT = tran.STUDENT
Group By
    tran.STUDENT