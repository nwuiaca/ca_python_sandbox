"""
Script to test STUDENT BURSARIES
Created on: 29 Jan 2021
Author: Albert J v Rensburg (NWU21162395)
"""

# IMPORT PYTHON MODULES
import sqlite3
import csv

# IMPORT OWN MODULES
from _my_modules import funcconf
from _my_modules import funccsv
from _my_modules import funcdatn
from _my_modules import funcfile
from _my_modules import funcstat
from _my_modules import funcsys
from _my_modules import funcsms
from _my_modules import functest

# INDEX
"""
ENVIRONMENT
OPEN THE DATABASES
TEMPORARY AREA
BEGIN OF SCRIPT
IMPORT BURSARY MASTER LIST "X000_Bursary_master"
OBTAIN STUDENTS "X000_Student"
BUILD STUDENT RELATIONSHIPS "X000_Student_relationship"
OBTAIN STUDENT TRANSACTIONS "X000_Transaction"
OBTAIN STAFF DISCOUNT STUDENTS "X000_Transaction_staffdisc_student"
BUILD BURSARY VALUE PER STUDENT, BURSARY AND QUALIFICATION TYPE "X001_Bursary_value_student"
BURSARY SUMMARY PER STUDENT "X001_Bursary_summary_student"
END OF SCRIPT
"""

# SCRIPT WIDE VARIABLES
s_function: str = "C303 Student bursary tests"


def student_bursary(s_period: str = "curr"):
    """
    Script to test STUDENT BURSARIES
    :param s_period: str: The financial period
    :return: Nothing
    """

    """*****************************************************************************
    ENVIRONMENT
    *****************************************************************************"""

    # FUNCTION WIDE VARIABLES
    if s_period == "prev":
        s_year = funcdatn.get_previous_year()
    else:
        s_year = funcdatn.get_current_year()
    ed_path: str = f"{funcconf.drive_system}_external_data/"  # External data path
    re_path: str = f"{funcconf.drive_data_results}Vss/"
    so_path: str = f"{funcconf.drive_data_raw}Vss_fee/"  # Source database path
    so_file: str = "Vss_test_bursary.sqlite"
    l_debug: bool = False
    # l_mail: bool = funcconf.l_mail_project
    # l_mail: bool = True
    l_mess: bool = funcconf.l_mess_project
    # l_mess: bool = True
    l_record: bool = True
    l_export: bool = False
    s_burs_code: str = "('042', '052', '381', '500')"  # Current bursary transaction codes
    s_staff_code = "('021')"  # Staff discount transaction code

    # LOG
    funcfile.writelog("Now")
    funcfile.writelog("SCRIPT: " + s_function.upper())
    funcfile.writelog("-" * len("script: "+s_function))
    if l_debug:
        print(s_function.upper())

    # MESSAGE
    if l_mess:
        funcsms.send_telegram("", "administrator", "<b>" + s_function + "</b>")

    """************************************************************************
    OPEN THE DATABASES
    ************************************************************************"""
    funcfile.writelog("OPEN THE DATABASES")
    if l_debug:
        print("OPEN THE DATABASES")

    # OPEN THE SQLITE DATABASE
    # Create the connection
    so_conn = sqlite3.connect(so_path + so_file)
    # Create the cursor
    so_curs = so_conn.cursor()
    funcfile.writelog("OPEN DATABASE: " + so_file)

    # ATTACH VSS DATABASE
    if l_debug:
        print("Attach vss database...")
    so_curs.execute(f"ATTACH DATABASE '{funcconf.drive_data_raw}Vss/Vss.sqlite' AS 'VSS'")
    funcfile.writelog("%t ATTACH DATABASE: Vss.sqlite")
    so_curs.execute(f"ATTACH DATABASE '{funcconf.drive_data_raw}Vss/Vss_curr.sqlite' AS 'VSSCURR'")
    funcfile.writelog("%t ATTACH DATABASE: Vss_curr.sqlite")
    so_curs.execute(f"ATTACH DATABASE '{funcconf.drive_data_raw}Vss/Vss_prev.sqlite' AS 'VSSPREV'")
    funcfile.writelog("%t ATTACH DATABASE: Vss_prev.sqlite")
    so_curs.execute(f"ATTACH DATABASE '{funcconf.drive_data_raw}People/People.sqlite' AS 'PEOPLE'")
    funcfile.writelog("%t ATTACH DATABASE: People.sqlite")

    """************************************************************************
    TEMPORARY AREA
    ************************************************************************"""
    funcfile.writelog("TEMPORARY AREA")
    if l_debug:
        print("TEMPORARY AREA")

    """************************************************************************
    BEGIN OF SCRIPT
    ************************************************************************"""
    funcfile.writelog("BEGIN OF SCRIPT")
    if l_debug:
        print("BEGIN OF SCRIPT")

    """*****************************************************************************
    IMPORT BURSARY MASTER LIST
    *****************************************************************************"""
    if l_debug:
        print("Import bursary master list...")
    sr_file = "X000_Bursary_master"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute("CREATE TABLE " + sr_file + "(ACTIVE TEXT,"
                                                "FINAIDCODE INT,"
                                                "FINAIDNAME TEXT,"
                                                "SOURCE TEXT,"
                                                "DEGREE_TYPE TEXT,"
                                                "COST_STRING TEXT,"
                                                "APPLICATION_PROCESS TEXT,"
                                                "SBL_EVALUATION TEXT,"
                                                "BURSARY_OFFICE_PROCESS TEXT,"
                                                "EMPLOYEE1 TEXT,"
                                                "EMPLOYEE2 TEXT,"
                                                "NOTE TEXT)")

    co = open(ed_path + "303_bursary_master.csv", newline=None)
    co_reader = csv.reader(co)
    for row in co_reader:
        if row[0] == "ACTIVE":
            continue
        else:
            s_cols: str = "INSERT INTO " + sr_file + " VALUES('" +\
                                                row[0] + "'," +\
                                                row[1] + ",'" +\
                                                row[2] + "','" +\
                                                row[3] + "','" +\
                                                row[4] + "','" +\
                                                row[5] + "','" +\
                                                row[6] + "','" +\
                                                row[7] + "','" +\
                                                row[8] + "','" +\
                                                row[9] + "','" +\
                                                row[10] + "','" +\
                                                row[11] + "')"
            so_curs.execute(s_cols)
    so_conn.commit()
    # Close the imported data file
    co.close()
    funcfile.writelog("%t IMPORT TABLE: " + sr_file)

    """************************************************************************
    OBTAIN STUDENTS
    ************************************************************************"""
    funcfile.writelog("OBTAIN STUDENTS")
    if l_debug:
        print("OBTAIN STUDENTS")

    # OBTAIN THE LIST STUDENTS
    # EXCLUDE SHORT COURSE STUDENTS
    if l_debug:
        print("Obtain the registered students...")
    sr_file = "X000_Student"
    s_sql = "CREATE TABLE " + sr_file + " AS" + """
    SELECT
        STUD.*,
        CASE
            WHEN DATEENROL < STARTDATE THEN STARTDATE
            ELSE DATEENROL
        END AS DATEENROL_CALC
    FROM
        %VSS%.X001_Student STUD
    WHERE
        UPPER(STUD.QUAL_TYPE) Not Like '%SHORT COURSE%'
    """
    """
    To exclude some students
    STUD.ISMAINQUALLEVEL = 1 AND
    UPPER(STUD.ACTIVE_IND) = 'ACTIVE'
    """
    if s_period == "prev":
        s_sql = s_sql.replace("%VSS%", "VSSPREV")
    else:
        s_sql = s_sql.replace("%VSS%", "VSSCURR")
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD STUDENT RELATIONSHIPS
    if l_debug:
        print("Build student relationships...")
    sr_file = "X000_Student_relationship"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = "CREATE TABLE " + sr_file + " AS" + """
    Select
        s.KSTUDBUSENTID,
        e.name_full As EMP_NAME_FULL,
        e.user_person_type As EMP_PERSON_TYPE,
        r.KBUSENTRELID,
        r.KRELATIONSHIPTYPECODEID,
        Upper(c.CODELONGDESCRIPTION) As REL_TYPE,
        r.KRELATEDBUSINESSENTITYID,
        p.name_full As REL_NAME_FULL,
        p.user_person_type As REL_PERSON_TYPE,
        Max(r.STARTDATE) As STARTDATE,
        r.ENDDATE,
        r.LOCKSTAMP,
        r.AUDITDATETIME,
        r.FAUDITSYSTEMFUNCTIONID,
        r.FAUDITUSERCODE        
    From
        X000_Student s Left Join
        VSS.BUSINESSENTITYRELATIONSHIP r On r.KBUSINESSENTITYID = s.KSTUDBUSENTID And
            r.KRELATEDBUSINESSENTITYID >= 10000000 And
            r.KRELATIONSHIPTYPECODEID in (6713, 6712, 9719, 6573, 6574, 9714) And
            strftime('%Y-%m-%d', '%DATE%') between r.STARTDATE and ifnull(r.ENDDATE, '4712-12-31') Left Join
        VSS.CODEDESCRIPTION c On c.KCODEDESCID = r.KRELATIONSHIPTYPECODEID And
            c.KSYSTEMLANGUAGECODEID = 3 Left Join
        PEOPLE.X000_PEOPLE e On e.employee_number = Cast(s.KSTUDBUSENTID As TEXT) Left Join
        PEOPLE.X000_PEOPLE p On p.employee_number = Cast(r.KRELATEDBUSINESSENTITYID As TEXT)
    Group By
        s.KSTUDBUSENTID,
        r.KRELATIONSHIPTYPECODEID            
    ;"""
    s_sql = s_sql.replace("%DATE%", funcdatn.get_today_date())
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    """************************************************************************
    OBTAIN STUDENT TRANSACTIONS
    ************************************************************************"""
    funcfile.writelog("OBTAIN STUDENT TRANSACTIONS")
    if l_debug:
        print("OBTAIN STUDENT TRANSACTIONS")

    # OBTAIN STUDENT ACCOUNT TRANSACTIONS
    if l_debug:
        print("Import student transactions...")
    sr_file = "X000_Transaction"
    s_sql = "CREATE TABLE " + sr_file + " AS" + """
    Select
        TRAN.FBUSENTID As STUDENT,
        CASE
            WHEN TRAN.FDEBTCOLLECTIONSITE = '-9' THEN 'MAHIKENG'
            WHEN TRAN.FDEBTCOLLECTIONSITE = '-2' THEN 'VANDERBIJLPARK'
            ELSE 'POTCHEFSTROOM'
        END AS CAMPUS,
        TRAN.TRANSDATE,
        TRAN.TRANSDATETIME,
        CASE
            WHEN SUBSTR(TRAN.TRANSDATE,6,5)='01-01' AND INSTR('001z031z061',TRAN.TRANSCODE)>0 THEN '00'
            WHEN strftime('%Y',TRANSDATE)>strftime('%Y',POSTDATEDTRANSDATE) And
             Strftime('%Y',POSTDATEDTRANSDATE) = '%YEAR%' THEN strftime('%m',POSTDATEDTRANSDATE)
            ELSE strftime('%m',TRAN.TRANSDATE)
        END AS MONTH,
        TRAN.TRANSCODE,
        TRAN.AMOUNT,
        CASE
            WHEN TRAN.AMOUNT > 0 THEN TRAN.AMOUNT
            ELSE 0.00
        END AS AMOUNT_DT,
        CASE
            WHEN TRAN.AMOUNT < 0 THEN TRAN.AMOUNT
            ELSE 0.00
        END AS AMOUNT_CR,
        TRAN.DESCRIPTION_E As TRANSDESC,
        TRAN.AUDITDATETIME,
        TRAN.FUSERBUSINESSENTITYID,
        TRAN.FAUDITUSERCODE,
        TRAN.SYSTEM_DESC,
        TRAN.FFINAIDSITEID,
        TRAN.FINAIDCODE,
        TRAN.FINAIDNAME,
        BURS.SOURCE
    FROM
        %VSS%.X010_Studytrans TRAN LEFT JOIN
        X000_Bursary_master BURS ON BURS.FINAIDCODE = TRAN.FINAIDCODE
    WHERE
        TRAN.TRANSCODE IN %BURSARY%
    ORDER BY
        AUDITDATETIME
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = s_sql.replace("%YEAR%", s_year)
    s_sql = s_sql.replace("%BURSARY%", s_burs_code)
    if s_period == "prev":
        s_sql = s_sql.replace("%VSS%", "VSSPREV")
    else:
        s_sql = s_sql.replace("%VSS%", "VSSCURR")
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # OBTAIN STAFF DISCOUNT STUDENTS
    if l_debug:
        print("Import staff discount students...")
    sr_file = "X000_Transaction_staffdisc_student"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = "CREATE TABLE " + sr_file + " AS" + """
    Select
        s.FBUSENTID As STUDENT,
        Count(s.FSERVICESITE) As TRAN_COUNT,
        Total(s.AMOUNT) As TRAN_VALUE
    From
        %VSS%.X010_Studytrans s
    Where
        s.TRANSCODE In %STAFF%
    Group By
        s.FBUSENTID
    ;"""
    s_sql = s_sql.replace("%STAFF%", s_staff_code)
    if s_period == "prev":
        s_sql = s_sql.replace("%VSS%", "VSSPREV")
    else:
        s_sql = s_sql.replace("%VSS%", "VSSCURR")
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    """************************************************************************
    BUILD BURSARY TEST DATA
    ************************************************************************"""
    funcfile.writelog("BUILD BURSARY TEST DATA")
    if l_debug:
        print("BUILD BURSARY TEST DATA")

    # BUILD BURSARY VALUE PER STUDENT, BURSARY AND QUALIFICATION TYPE
    if l_debug:
        print("Build bursary value summary per student...")
    sr_file = "X001_Bursary_value_student"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = "CREATE TABLE " + sr_file + " AS" + """
    Select
        x000t.STUDENT,
        x000t.FFINAIDSITEID,
        x000t.FINAIDCODE,
        x000t.FINAIDNAME,
        x000s.LEVY_CATEGORY,
        x000b.SOURCE,
        Cast(Round(Total(x000t.AMOUNT),2) As Real) As AMOUNT_TOTAL,
        Cast(Count(x000t.CAMPUS) As Int) As TRAN_COUNT
    From
        X000_Transaction x000t Left Join
        X000_Student x000s On x000s.KSTUDBUSENTID = x000t.STUDENT Left Join
        X000_Bursary_master x000b ON x000b.FINAIDCODE = x000t.FINAIDCODE
    Group By
        x000t.STUDENT,
        x000t.FINAIDCODE,
        x000s.LEVY_CATEGORY
    """
    if s_period == "prev":
        s_sql = s_sql.replace("%VSS%", "VSSPREV")
    else:
        s_sql = s_sql.replace("%VSS%", "VSSCURR")
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BURSARY SUMMARY PER STUDENT
    if l_debug:
        print("Build bursary summary per student...")
    sr_file = "X001_Bursary_summary_student"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = "CREATE TABLE " + sr_file + " AS" + """
    Select
        tran.STUDENT As student,
        stud.CAMPUS as campus,
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
    ;"""
    s_sql = s_sql.replace("%STAFF%", s_staff_code)
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD TABLE: " + sr_file)
    if l_mess:
        so_conn.commit()
        i = funcsys.tablerowcount(so_curs, sr_file)
        funcsms.send_telegram("", "administrator", "<b>" + str(i) + "</b> Bursary students")

    """*****************************************************************************
    TEST BURSARY NOT CANCELLED
    *****************************************************************************"""

    # DECLARE TEST VARIABLES
    i_finding_before: int = 0
    i_finding_after: int = 0
    s_description = "Bursary not cancelled"
    s_file_prefix: str = "X005a"
    s_file_name: str = "bursary_not_cancelled"
    s_finding: str = "BURSARY NOT CANCELLED"
    s_report_file: str = "303_reported.txt"
    s_exclude_list: str = "('PASS CERTIFICATE'," \
                          "'PASS CERTIFICATE POSTHUMOUSLY'," \
                          "'PASS CERTIFICATE WITH DISTINCTION'," \
                          "'PASS DEGREE'," \
                          "'PASS DEGREE POSTHUMOUSLY'," \
                          "'PASS DEGREE WITH DISTINCTION'," \
                          "'PASS DEGREE WITH DISTINCTION POSTHUMOUSLY'," \
                          "'PASS DIPLOMA'," \
                          "'PASS DIPLOMA WITH DISTINCTION'," \
                          "'COURSE CONVERTED')"

    # OBTAIN TEST RUN FLAG
    if functest.get_test_flag(so_curs, "VSS", "TEST " + s_finding, "RUN") == "FALSE":

        if l_debug:
            print('TEST DISABLED')
        funcfile.writelog("TEST " + s_finding + " DISABLED")

    else:

        # LOG
        funcfile.writelog("TEST " + s_finding)
        if l_debug:
            print("TEST " + s_finding)

        # OBTAIN MASTER DATA 1
        if l_debug:
            print("Obtain master data...")
        sr_file: str = s_file_prefix + "aa_" + s_file_name
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        s_sql = "Create Table " + sr_file + " As " + """
        Select
            burs.student,
            burs.campus,
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
                burs.enrol_category Not In ('POST DOC') And
                burs.discontinue_result Not In %RESULT_EXCLUDE%)
        ;"""
        s_sql = s_sql.replace("%RESULT_EXCLUDE%", s_exclude_list)
        so_curs.execute(s_sql)
        funcfile.writelog("%t BUILD TABLE: " + sr_file)
        if l_debug:
            so_conn.commit()

        # IDENTIFY FINDINGS
        if l_debug:
            print("Identify findings...")
        sr_file: str = s_file_prefix + "b_finding"
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        s_sql = "Create Table " + sr_file + " As " + """
        Select
            'NWU' As ORG,
            find.campus As LOC,
            find.student As STUDENT,
            Cast(find.total_burs As Int) As TOTAL_BURS
        From
            %FILEP%%FILEB% find
        ;"""
        s_sql = s_sql.replace("%FILEP%", s_file_prefix)
        s_sql = s_sql.replace("%FILEB%", 'aa_' + s_file_name)
        so_curs.execute(s_sql)
        funcfile.writelog("%t BUILD TABLE: " + sr_file)
        if l_debug:
            so_conn.commit()

        # COUNT THE NUMBER OF FINDINGS
        if l_debug:
            print("Count the number of findings...")
        i_finding_before = funcsys.tablerowcount(so_curs, sr_file)
        funcfile.writelog("%t FINDING: " + str(i_finding_before) + " " + s_finding + " finding(s)")
        if l_debug:
            print("*** Found " + str(i_finding_before) + " exceptions ***")

        # GET PREVIOUS FINDINGS
        if i_finding_before > 0:
            functest.get_previous_finding(so_curs, ed_path, s_report_file, s_finding, "IITTT")
            if l_debug:
                so_conn.commit()

        # SET PREVIOUS FINDINGS
        if i_finding_before > 0:
            functest.set_previous_finding(so_curs)
            if l_debug:
                so_conn.commit()

        # ADD PREVIOUS FINDINGS
        sr_file = s_file_prefix + "d_addprev"
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        if i_finding_before > 0:
            if l_debug:
                print("Join previously reported to current findings...")
            s_sql = "CREATE TABLE " + sr_file + " AS" + """
            Select
                FIND.*,
                Lower('%FINDING%') AS PROCESS,
                '%TODAY%' AS DATE_REPORTED,
                '%DATETEST%' AS DATE_RETEST,
                PREV.PROCESS AS PREV_PROCESS,
                PREV.DATE_REPORTED AS PREV_DATE_REPORTED,
                PREV.DATE_RETEST AS PREV_DATE_RETEST,
                PREV.REMARK
            From
                %FILEP%b_finding FIND Left Join
                Z001ab_setprev PREV ON PREV.FIELD1 = FIND.STUDENT
                    And PREV.FIELD2 = FIND.TOTAL_BURS
            ;"""
            s_sql = s_sql.replace("%FINDING%", s_finding)
            s_sql = s_sql.replace("%FILEP%", s_file_prefix)
            s_sql = s_sql.replace("%TODAY%", funcdatn.get_today_date())
            s_sql = s_sql.replace("%DATETEST%", funcdatn.get_current_month_end_next())
            so_curs.execute(s_sql)
            funcfile.writelog("%t BUILD TABLE: " + sr_file)
            if l_debug:
                so_conn.commit()

        # BUILD LIST TO UPDATE FINDINGS
        sr_file = s_file_prefix + "e_newprev"
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        if i_finding_before > 0:
            if l_debug:
                print("Build list to update findings...")
            s_sql = "CREATE TABLE " + sr_file + " AS " + """
            Select
                PREV.PROCESS,
                PREV.STUDENT AS FIELD1,
                PREV.TOTAL_BURS AS FIELD2,
                PREV.LOC AS FIELD3,
                '' AS FIELD4,
                '' AS FIELD5,
                PREV.DATE_REPORTED,
                PREV.DATE_RETEST,
                PREV.REMARK
            From
                %FILEP%d_addprev PREV
            Where
                PREV.PREV_PROCESS Is Null Or
                PREV.DATE_REPORTED > PREV.PREV_DATE_RETEST And PREV.REMARK = ""        
            ;"""
            s_sql = s_sql.replace("%FILEP%", s_file_prefix)
            so_curs.execute(s_sql)
            funcfile.writelog("%t BUILD TABLE: " + sr_file)
            if l_debug:
                so_conn.commit()
            # Export findings to previous reported file
            i_finding_after = funcsys.tablerowcount(so_curs, sr_file)
            if i_finding_after > 0:
                if l_debug:
                    print("*** " + str(i_finding_after) + " Finding(s) to report ***")
                sx_path = ed_path
                sx_file = s_report_file[:-4]
                # Read the header data
                s_head = funccsv.get_colnames_sqlite(so_conn, sr_file)
                # Write the data
                if l_record:
                    funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file, s_head, "a", ".txt")
                    funcfile.writelog("%t FINDING: " + str(i_finding_after) + " new finding(s) to export")
                    funcfile.writelog("%t EXPORT DATA: " + sr_file)
                if l_mess:
                    funcsms.send_telegram('', 'administrator', '<b>' + str(i_finding_before) + '/' + str(
                        i_finding_after) + '</b> ' + s_description)
            else:
                funcfile.writelog("%t FINDING: No new findings to export")
                if l_debug:
                    print("*** No new findings to report ***")

        # IMPORT OFFICERS FOR MAIL REPORTING PURPOSES
        if i_finding_before > 0 and i_finding_after > 0:
            functest.get_officer(so_curs, "VSS", "TEST " + s_finding + " OFFICER")
            so_conn.commit()

        # IMPORT SUPERVISORS FOR MAIL REPORTING PURPOSES
        if i_finding_before > 0 and i_finding_after > 0:
            functest.get_supervisor(so_curs, "VSS", "TEST " + s_finding + " SUPERVISOR")
            so_conn.commit()

        # ADD CONTACT DETAILS TO FINDINGS
        sr_file = s_file_prefix + "h_detail"
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        if i_finding_before > 0 and i_finding_after > 0:
            if l_debug:
                print("Add contact details to findings...")
            s_sql = "CREATE TABLE " + sr_file + " AS " + """
            Select
                PREV.ORG,
                PREV.LOC,
                Cast(PREV.STUDENT As TEXT) As STUDENT,
                Round(Cast(BURS.total_burs As REAL),2) As BURSARY_VALUE,
                BURS.active As ACTIVE,
                BURS.levy_category As LEVY_CATEGORY,
                BURS.enrol_category As ENROL_CATEGORY,
                BURS.qualification_type As QUALIFICATION_TYPE,
                BURS.qualification As QUALIFICATION,
                BURS.discontinue_date As DISCONTINUE_DATE,
                BURS.discontinue_result As DISCONTINUE_RESULT,
                BURS.discontinue_reason As DISCONTINUE_REASON,
                CAMP_OFF.EMPLOYEE_NUMBER AS CAMP_OFF_NUMB,
                CAMP_OFF.NAME_ADDR AS CAMP_OFF_NAME,
                CAMP_OFF.EMAIL_ADDRESS AS CAMP_OFF_MAIL1,        
                CASE
                    WHEN  CAMP_OFF.EMPLOYEE_NUMBER != '' THEN CAMP_OFF.EMPLOYEE_NUMBER||'@nwu.ac.za'
                    ELSE CAMP_OFF.EMAIL_ADDRESS
                END AS CAMP_OFF_MAIL2,
                CAMP_SUP.EMPLOYEE_NUMBER AS CAMP_SUP_NUMB,
                CAMP_SUP.NAME_ADDR AS CAMP_SUP_NAME,
                CAMP_SUP.EMAIL_ADDRESS AS CAMP_SUP_MAIL1,
                CASE
                    WHEN CAMP_SUP.EMPLOYEE_NUMBER != '' THEN CAMP_SUP.EMPLOYEE_NUMBER||'@nwu.ac.za'
                    ELSE CAMP_SUP.EMAIL_ADDRESS
                END AS CAMP_SUP_MAIL2,
                ORG_OFF.EMPLOYEE_NUMBER AS ORG_OFF_NUMB,
                ORG_OFF.NAME_ADDR AS ORG_OFF_NAME,
                ORG_OFF.EMAIL_ADDRESS AS ORG_OFF_MAIL1,
                CASE
                    WHEN ORG_OFF.EMPLOYEE_NUMBER != '' THEN ORG_OFF.EMPLOYEE_NUMBER||'@nwu.ac.za'
                    ELSE ORG_OFF.EMAIL_ADDRESS
                END AS ORG_OFF_MAIL2,
                ORG_SUP.EMPLOYEE_NUMBER AS ORG_SUP_NUMB,
                ORG_SUP.NAME_ADDR AS ORG_SUP_NAME,
                ORG_SUP.EMAIL_ADDRESS AS ORG_SUP_MAIL1,
                CASE
                    WHEN ORG_SUP.EMPLOYEE_NUMBER != '' THEN ORG_SUP.EMPLOYEE_NUMBER||'@nwu.ac.za'
                    ELSE ORG_SUP.EMAIL_ADDRESS
                END AS ORG_SUP_MAIL2,
                AUD_OFF.EMPLOYEE_NUMBER As AUD_OFF_NUMB,
                AUD_OFF.NAME_ADDR As AUD_OFF_NAME,
                AUD_OFF.EMAIL_ADDRESS As AUD_OFF_MAIL,
                AUD_SUP.EMPLOYEE_NUMBER As AUD_SUP_NUMB,
                AUD_SUP.NAME_ADDR As AUD_SUP_NAME,
                AUD_SUP.EMAIL_ADDRESS As AUD_SUP_MAIL
            From
                %FILEP%d_addprev PREV Left Join
                %FILEP%%FILEB% BURS On BURS.student = PREV.STUDENT Left Join
                Z001af_officer CAMP_OFF On CAMP_OFF.CAMPUS = PREV.LOC Left Join
                Z001af_officer ORG_OFF On ORG_OFF.CAMPUS = PREV.ORG Left Join
                Z001af_officer AUD_OFF On AUD_OFF.CAMPUS = 'AUD' Left Join
                Z001ag_supervisor CAMP_SUP On CAMP_SUP.CAMPUS = PREV.LOC Left Join
                Z001ag_supervisor ORG_SUP On ORG_SUP.CAMPUS = PREV.ORG Left Join
                Z001ag_supervisor AUD_SUP On AUD_SUP.CAMPUS = 'AUD'                    
            Where
                PREV.PREV_PROCESS Is Null Or
                PREV.DATE_REPORTED > PREV.PREV_DATE_RETEST And PREV.REMARK = ""
            Order by
                BURS.ACTIVE,
                PREV.LOC,
                PREV.STUDENT                
            ;"""
            s_sql = s_sql.replace("%FILEP%", s_file_prefix)
            s_sql = s_sql.replace("%FILEN%", s_file_name)
            s_sql = s_sql.replace("%FILEB%", 'aa_' + s_file_name)
            so_curs.execute(s_sql)
            so_conn.commit()
            funcfile.writelog("%t BUILD TABLE: " + sr_file)

        # BUILD THE FINAL TABLE FOR EXPORT AND REPORT
        sr_file = s_file_prefix + "x_" + s_file_name
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        if i_finding_before > 0 and i_finding_after > 0:
            if l_debug:
                print("Build the final report")
            s_sql = "CREATE TABLE " + sr_file + " AS " + """
            Select
                '%FIND%' As Audit_finding,
                FIND.ORG As Organization,
                FIND.LOC As Campus,
                FIND.STUDENT As Student,
                FIND.BURSARY_VALUE As Bursary_value,
                FIND.ACTIVE As Active,
                FIND.LEVY_CATEGORY As Levy_category,
                FIND.ENROL_CATEGORY As Enrol_category,
                FIND.QUALIFICATION_TYPE As Qualification_type,
                FIND.QUALIFICATION As Qualification,
                FIND.DISCONTINUE_DATE As Discontinue_date,
                FIND.DISCONTINUE_RESULT As Discontinue_result,
                FIND.DISCONTINUE_REASON As Discontinue_reason,
                FIND.CAMP_OFF_NAME AS Officer,
                FIND.CAMP_OFF_NUMB AS Officer_Numb,
                FIND.CAMP_OFF_MAIL1 AS Officer_Mail,
                FIND.CAMP_SUP_NAME AS Supervisor,
                FIND.CAMP_SUP_NUMB AS Supervisor_Numb,
                FIND.CAMP_SUP_MAIL1 AS Supervisor_Mail,
                FIND.ORG_OFF_NAME AS Org_Officer,
                FIND.ORG_OFF_NUMB AS Org_Officer_Numb,
                FIND.ORG_OFF_MAIL1 AS Org_Officer_Mail,
                FIND.ORG_SUP_NAME AS Org_Supervisor,
                FIND.ORG_SUP_NUMB AS Org_Supervisor_Numb,
                FIND.ORG_SUP_MAIL1 AS Org_Supervisor_Mail,
                FIND.AUD_OFF_NAME AS Audit_Officer,
                FIND.AUD_OFF_NUMB AS Audit_Officer_Numb,
                FIND.AUD_OFF_MAIL AS Audit_Officer_Mail,
                FIND.AUD_SUP_NAME AS Audit_Supervisor,
                FIND.AUD_SUP_NUMB AS Audit_Supervisor_Numb,
                FIND.AUD_SUP_MAIL AS Audit_Supervisor_Mail
            From
                %FILEP%h_detail FIND
            ;"""
            s_sql = s_sql.replace("%FIND%", s_finding)
            s_sql = s_sql.replace("%FILEP%", s_file_prefix)
            so_curs.execute(s_sql)
            so_conn.commit()
            funcfile.writelog("%t BUILD TABLE: " + sr_file)
            # Export findings
            if l_export and funcsys.tablerowcount(so_curs, sr_file) > 0:
                if l_debug:
                    print("Export findings...")
                sx_path = re_path + funcdatn.get_current_year() + "/"
                sx_file = s_file_prefix + "_" + s_finding.lower() + "_"
                sx_file_dated = sx_file + funcdatn.get_today_date_file()
                s_head = funccsv.get_colnames_sqlite(so_conn, sr_file)
                funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file, s_head)
                funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file_dated, s_head)
                funcfile.writelog("%t EXPORT DATA: " + sx_path + sx_file)
        else:
            s_sql = "CREATE TABLE " + sr_file + " (" + """
            BLANK TEXT
            );"""
            so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
            so_curs.execute(s_sql)
            so_conn.commit()
            funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # MESSAGE
    # if l_mess:
    #     funcsms.send_telegram("", "administrator", "<b>" + s_function + "</b> end")

    """************************************************************************
    END OF SCRIPT
    ************************************************************************"""
    funcfile.writelog("END OF SCRIPT")
    if l_debug:
        print("END OF SCRIPT")

    # CLOSE THE DATABASE CONNECTION
    so_conn.commit()
    so_conn.close()

    # CLOSE THE LOG WRITER
    funcfile.writelog("-" * len("completed: "+s_function))
    funcfile.writelog("COMPLETED: " + s_function.upper())

    return


if __name__ == '__main__':
    try:
        student_bursary("curr")
    except Exception as e:
        funcsys.ErrMessage(e)
