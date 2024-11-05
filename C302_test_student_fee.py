"""
Script to test STUDENT FEES
Created on: 28 Aug 2019
Author: Albert J v Rensburg (NWU21162395)
"""

# IMPORT PYTHON MODULES
import sqlite3
import csv
import zipfile

# IMPORT OWN MODULES
from _my_modules import funcconf
from _my_modules import funccsv
from _my_modules import funcdatn
from _my_modules import funcfile
from _my_modules import funcstat
from _my_modules import funcsys
from _my_modules import funcsms
from _my_modules import functest
from _my_modules import funcmail

""" INDEX **********************************************************************
ENVIRONMENT
OPEN THE DATABASES
TEMPORARY AREA
BEGIN OF SCRIPT

OBTAIN STUDENTS
OBTAIN STUDENT TRANSACTIONS
ADD THE TRANSACTION USER (NB Note deault users)

REGISTRATION FEE MASTER
REGISTRATION FEE REPORTS
TEST REGISTRATION FEE CONTACT NULL (V1.1.1)
TEST REGISTRATION FEE CONTACT NEGATIVE
TEST REGISTRATION FEE CONTACT ZERO (V2.0.2)
TEST REGISTRATION FEE CONTACT ABNORMAL

QUALIFICATION FEE MASTER 1 (Prepare FIAB lists)
QUALIFICATION FEE TEST NO FEE LOADED (V1.1.1)
QUALIFICATION FEE MASTER 2 (Prepare data for incorrectly loaded fees)
QUALIFICATION FEE TEST FEE LOADED INCORRECTLY (V2.0.0)(V2.0.2)
QUALIFICATION FEE MASTER 3 (Join students & transactions)
QUALIFICATION FEE REPORTS
UPDATE FEE SHOULD BE COLUMN
QUALIFICATION FEE TEST NO TRANSACTION CONTACT (V1.1.1)(1 NO TRANSACTION)
QUALIFICATION FEE TEST NEGATIVE TRANSACTION (2 NEGATIVE TRANSACTION)
QUALIFICATION FEE TEST ZERO TRANSACTION CONTACT (V1.1.1)(3 ZERO TRANSACTION)
QUALIFICATION FEE TEST HALF TRANSACTION CONTACT (V1.1.1)(4 HALF TRANSACTION)
QUALIFICATION FEE TEST ABNORMAL TRANSACTION CONTACT (V1.1.1)(6 ABNORMAL TRANSACTION)
QUALIFICATION FEE TEST OVERCHARGE CONTACT (V2.0.0)(V2.0.2)(2 VALID COLUMN)
SECONDARY QUALIFICATION FEE OVERCHARGE CONTACT (V2.0.2)(3 VALID COLUMN)

MODULE FEE MASTER 1
TEST MODULE FEE NOT LOADED (V1.1.1)

END OF SCRIPT
*****************************************************************************"""


def student_fee(s_period="curr"):
    """
    Script to test STUDENT FEE INCOME
    :param s_period: str: The financial period
    :return: Nothing
    """

    """*****************************************************************************
    ENVIRONMENT
    *****************************************************************************"""

    # DECLARE VARIABLES
    if s_period == "prev":
        s_year = funcdatn.get_previous_year()
    else:
        s_year = funcdatn.get_current_year()

    ed_path = f"{funcconf.drive_system}_external_data/"  # External data path
    so_path = f"{funcconf.drive_data_raw}Vss_fee/"  # Source database path
    re_path = f"{funcconf.drive_data_results}Vss/" + s_year

    if s_period == "2019":
        f_reg_fee = 1830.00
        d_sem1_con = "2019-03-05"
        d_sem1_dis = "2019-03-05"
        d_sem2_con = "2019-08-09"
        d_sem2_dis = "2019-08-09"
        d_test_overcharge = "2019-07-15"  # Only month and day used
        so_file = "Vss_test_fee_prev.sqlite"  # Source database
        s_reg_trancode: str = "095"
        s_qual_trancode: str = "004"
        qualification_transaction_code = ['004']
        qualification_transaction_code_str = ', '.join([f"'{item}'" for item in qualification_transaction_code])
        s_modu_trancode: str = "004"
        s_burs_trancode: str = "042z052z381z500"
        # Find these id's from Sqlite->Sqlite_vss_test_fee->Q021aa_qual_nofee_loaded
        s_mba: str = "71500z2381692z2381690z665559"  # Exclude these FQUALLEVELAPID
        s_mpa: str = "665566z618161z618167z618169"  # Exclude these FQUALLEVELAPID
        s_aud: str = "71839z71840z71841z71842z71820z71821z71822z1085390"  # Exclude these FQUALLEVELAPID
        l_record: bool = False
        l_export: bool = False
    elif s_period == "2020":
        f_reg_fee = 1930.00
        d_sem1_con = "2020-02-21"
        d_sem1_dis = "2020-03-09"
        d_sem2_con = "2020-09-04"
        d_sem2_dis = "2020-09-04"
        d_test_overcharge = "2020-07-15"  # Only month and day used
        so_file = "Vss_test_fee_prev.sqlite"  # Source database
        s_reg_trancode: str = "095"
        s_qual_trancode: str = "004"
        qualification_transaction_code = ['004']
        qualification_transaction_code_str = ', '.join([f"'{item}'" for item in qualification_transaction_code])
        s_modu_trancode: str = "004"
        s_burs_trancode: str = "042z052z381z500"
        # Find these id's from Sqlite->Sqlite_vss_test_fee->Q021aa_qual_nofee_loaded
        s_mba: str = "71500z2381692z2381690z665559"  # Exclude these FQUALLEVELAPID
        s_mpa: str = "665566z618161z618167z618169"  # Exclude these FQUALLEVELAPID
        s_aud: str = "71839z71840z71841z71842z71820z71821z71822z1085390"  # Exclude these FQUALLEVELAPID
        l_record: bool = False
        l_export: bool = False
    elif s_period == "2021":
        f_reg_fee = 2020.00
        d_sem1_con = "2021-04-09"
        d_sem1_dis = "2021-04-09"
        d_sem2_con = "2021-09-04"
        d_sem2_dis = "2021-09-04"
        d_test_overcharge = "2021-07-15"  # Only month and day used
        so_file = "Vss_test_fee.sqlite"  # Source database
        s_reg_trancode: str = "095"
        s_qual_trancode: str = "004"
        qualification_transaction_code = ['004']
        qualification_transaction_code_str = ', '.join([f"'{item}'" for item in qualification_transaction_code])
        s_modu_trancode: str = "004"
        s_burs_trancode: str = "042z052z381z500"
        # Find these id's from Sqlite->Sqlite_vss_test_fee->Q021aa_qual_nofee_loaded
        s_mba: str = "71500z2381692z2381690z665559"  # Exclude these FQUALLEVELAPID
        s_mpa: str = "665566z618161z618167z618169"  # Exclude these FQUALLEVELAPID
        s_aud: str = "71839z71840z71841z71842z71820z71821z71822z1085390"  # Exclude these FQUALLEVELAPID
        l_record: bool = False
        l_export: bool = False
    elif s_period == "2022":
        f_reg_fee = 2110.00
        d_sem1_con = "2022-03-11"
        d_sem1_dis = "2022-03-11"
        d_sem2_con = "2022-08-05"
        d_sem2_dis = "2022-08-05"
        d_test_overcharge = "2022-07-15"  # Only month and day used
        so_file = "Vss_test_fee.sqlite"  # Source database
        s_reg_trancode: str = "095"
        s_qual_trancode: str = "004"
        qualification_transaction_code = ['004']
        qualification_transaction_code_str = ', '.join([f"'{item}'" for item in qualification_transaction_code])
        s_modu_trancode: str = "004"
        s_burs_trancode: str = "042z052z381z500"
        # Find these id's from Sqlite->Sqlite_vss_test_fee->Q021aa_qual_nofee_loaded
        s_mba: str = "71500z2381692z2381690z665559"  # Exclude these FQUALLEVELAPID
        s_mpa: str = "665566z618161z618167z618169"  # Exclude these FQUALLEVELAPID
        s_aud: str = "71839z71840z71841z71842z71820z71821z71822z1085390"  # Exclude these FQUALLEVELAPID
        l_record: bool = False
        l_export: bool = False
    elif s_period == "prev":
        f_reg_fee = 2220.00
        d_sem1_con = "2023-03-03"
        d_sem1_dis = "2023-03-03"
        d_sem2_con = "2023-08-04"
        d_sem2_dis = "2023-08-04"
        d_test_overcharge = "2023-07-15"  # Only month and day used
        so_file = "Vss_test_fee.sqlite"  # Source database
        s_reg_trancode: str = "095"
        s_qual_trancode: str = "004"
        qualification_transaction_code = ['004']
        qualification_transaction_code_str = ', '.join([f"'{item}'" for item in qualification_transaction_code])
        s_modu_trancode: str = "004"
        s_burs_trancode: str = "042z052z381z500"
        # Find these id's from Sqlite->Sqlite_vss_test_fee->Q021aa_qual_nofee_loaded
        s_mba: str = "71500z2381692z2381690z665559"  # Exclude these FQUALLEVELAPID
        s_mpa: str = "665566z618161z618167z618169"  # Exclude these FQUALLEVELAPID
        s_aud: str = "71839z71840z71841z71842z71820z71821z71822z1085390"  # Exclude these FQUALLEVELAPID
        l_record: bool = True
        l_export: bool = True
    else:
        f_reg_fee = 2320.00
        d_sem1_con = "2024-03-20"
        d_sem1_dis = "2024-03-20"
        d_sem2_con = "2023-08-02"
        d_sem2_dis = "2023-08-02"
        d_test_overcharge = "2024-07-15"  # Only month and day used
        so_file = "Vss_test_fee.sqlite"  # Source database
        s_reg_trancode: str = "095"
        s_qual_trancode: str = "004"
        qualification_transaction_code = ['004']
        qualification_transaction_code_str = ', '.join([f"'{item}'" for item in qualification_transaction_code])
        s_modu_trancode: str = "004"
        s_burs_trancode: str = "042z052z381z500"
        # Find these id's from Sqlite->Sqlite_vss_test_fee->Q021aa_qual_nofee_loaded
        s_mba: str = "71500z2381692z2381690z665559"  # Exclude these FQUALLEVELAPID
        s_mpa: str = "665566z618161z618167z618169"  # Exclude these FQUALLEVELAPID
        s_aud: str = "71839z71840z71841z71842z71820z71821z71822z1085390"  # Exclude these FQUALLEVELAPID
        l_record: bool = True
        l_export: bool = True

    l_debug: bool = True
    l_mail: bool = funcconf.l_mail_project
    # l_mail: bool = False
    l_mess: bool = funcconf.l_mess_project
    # l_mess: bool = True
    s_desc: str = ""

    # SCRIPT LOG FILE
    if l_debug:
        print("---------------------")
        print("C302_TEST_STUDENT_FEE")
        print("---------------------")
    funcfile.writelog("Now")
    funcfile.writelog("SCRIPT: C302_TEST_STUDENT_FEE")
    funcfile.writelog("-----------------------------")

    # MESSAGE
    if l_mess:
        funcsms.send_telegram('', 'administrator', '<b>C302 Student fee income tests</b>')

    """*****************************************************************************
    OPEN THE DATABASES
    *****************************************************************************"""
    if l_debug:
        print("OPEN THE DATABASES")
    funcfile.writelog("OPEN THE DATABASES")

    # OPEN SQLITE SOURCE table
    if l_debug:
        print("Open sqlite database...")
    with sqlite3.connect(so_path + so_file) as so_conn:
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

    """ ****************************************************************************
    TEMPORARY AREA
    *****************************************************************************"""
    if l_debug:
        print("TEMPORARY AREA")
    funcfile.writelog("TEMPORARY AREA")

    """ ****************************************************************************
    BEGIN OF SCRIPT
    *****************************************************************************"""
    if l_debug:
        print("BEGIN OF SCRIPT")
    funcfile.writelog("BEGIN OF SCRIPT")

    """ ****************************************************************************
    OBTAIN STUDENTS
    *****************************************************************************"""
    if l_debug:
        print("OBTAIN STUDENTS")
    funcfile.writelog("OBTAIN STUDENTS")

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

    """ ****************************************************************************
    OBTAIN STUDENT TRANSACTIONS
    *****************************************************************************"""
    if l_debug:
        print("OBTAIN STUDENT TRANSACTIONS")
    funcfile.writelog("OBTAIN STUDENT TRANSACTIONS")

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
        TRAN.FMODAPID,
        TRAN.ENROL_ID,
        TRAN.ENROL_CATEGORY,        
        TRAN.MODULE,
        TRAN.MODULE_NAME,
        TRAN.FQUALLEVELAPID,
        TRAN.QUALIFICATION,
        TRAN.QUALIFICATION_NAME,
        TRAN.FENROLPRESID
    FROM
        %VSS%.X010_Studytrans TRAN
    WHERE
        TRAN.TRANSCODE <> ''
    ORDER BY
        AUDITDATETIME
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = s_sql.replace("%YEAR%", s_year)
    if s_period == "prev":
        s_sql = s_sql.replace("%VSS%", "VSSPREV")
    else:
        s_sql = s_sql.replace("%VSS%", "VSSCURR")
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # ADD THE TRANSACTION USER
    # USE TRANSACTION USER IF POSITION IS STUDENT ACCOUNTS ELSE
    # USE 26019817 VAN TONDER MRS. HC FOR POTCHEFSTROOM
    # USE 13163140 BIERMAN MS. EJ FOR VANDERBIJLPARK
    # USE 16343778 LEBEKO MRS. KV FOR MAHIKENG
    if l_debug:
        print("Add the transaction user...")
    sr_file = "X000_Transaction_user"
    s_sql = "Create table " + sr_file + " AS" + """
    Select
        TRAN.*,
        CASE
            WHEN PEOP.POSITION_FULL LIKE("%STUDENT ACCOUNTS%") THEN FUSERBUSINESSENTITYID
            WHEN CAMPUS LIKE("P%") THEN 26019817
            WHEN CAMPUS LIKE("V%") THEN 13163140
            WHEN CAMPUS LIKE("M%") THEN 16343778
            ELSE 0 
        END AS FUSER
    From
        X000_Transaction TRAN Left Join
        PEOPLE.X002_PEOPLE_CURR PEOP ON PEOP.EMPLOYEE_NUMBER = CAST(TRAN.FUSERBUSINESSENTITYID AS TEXT)        
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # ADD THE TRANSACTION USER NAME
    if l_debug:
        print("Add the transaction user name...")
    sr_file = "X000_Transaction"
    s_sql = "Create table " + sr_file + " AS" + """
    Select
        TRAN.*,
        PEOP.NAME_ADDR AS FUSERNAME
    From
        X000_Transaction_user TRAN Left Join
        PEOPLE.X002_PEOPLE_CURR PEOP ON PEOP.EMPLOYEE_NUMBER = CAST(TRAN.FUSER AS TEXT)        
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD TABLE: " + sr_file)
    sr_file = "X000_Transaction_user"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)

    """*****************************************************************************
    REGISTRATION FEE MASTER
    *****************************************************************************"""
    if l_debug:
        print("REGISTRATION FEE MASTER")
    funcfile.writelog("REGISTRATION FEE MASTER")

    # DECLARE LOCAL VARIABLES
    l_reg: bool = False

    # CALCULATE THE REGISTRATION FEES LEVIED PER STUDENT
    if l_debug:
        print("Calculate the registration fee transactions...")
    sr_file = "X010_Trans_feereg"
    s_sql = "Create table " + sr_file + " AS" + """
    Select
        TRAN.STUDENT,
        CAST(TOTAL(TRAN.AMOUNT) AS REAL) AS FEE_REG,
        COUNT(TRAN.STUDENT) As TRAN_COUNT,
        MAX(AUDITDATETIME),
        TRAN.FUSER AS FUSERBUSINESSENTITYID,
        TRAN.FUSERNAME AS NAME_ADDR,        
        TRAN.FAUDITUSERCODE,
        TRAN.SYSTEM_DESC        
    From
        X000_Transaction TRAN
    Where
        Instr('%TRANCODE%', Trim(TRAN.TRANSCODE)) > 0
    Group by
      TRAN.STUDENT
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = s_sql.replace("%TRANCODE%", s_reg_trancode)
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD VIEW: " + sr_file)

    # CALCULATE THE REGISTRATION FEE MODE
    i_calc = funcstat.stat_mode(so_curs, "X010_Trans_feereg", "FEE_REG")
    funcfile.writelog("%t STATISTIC MODE: Registration fee R" + str(i_calc))
    if l_debug:
        print("Registration fee: R" + str(i_calc))
    if i_calc == f_reg_fee:
        l_reg = True

    # ADD REGISTRATION LEVIED FEES TO THE STUDENTS LIST
    # EXCLUDE ALL NON MAIN QUALIFICATION QUALIFICATIONS
    # EXCLUDE EMP 10000445 (Mari Prinsloo) as User
    if l_debug:
        print("Join students and registration fees...")
    sr_file = "X010_Student_feereg"
    s_sql = "CREATE TABLE " + sr_file + " AS" + """
    Select
        STUD.KSTUDBUSENTID,
        FEE.FEE_REG,
        CASE
            When STUD.ENROL_CAT = "POST DOC" Then '9 EXCLUDE FROM TEST'
            When FEE.FEE_REG Is Null Then '1 NO TRANSACTION'
            When FEE.FEE_REG < 0 Then '2 NEGATIVE TRANSACTION'
            When FEE.FEE_REG = 0 Then '3 ZERO TRANSACTION'
            When FEE.FEE_REG = %LEVY% Then '4 NORMAL TRANSACTION'
            Else '5 ABNORMAL TRANSACTION'
        END As FEE_TYPE,
        Cast(Case
            When FEE.FEE_REG Is Null Then 0
            Else FEE.FEE_REG
        End As REAL) As FEE_CALC,
        Upper(STUD.ACTIVE_IND) As ACTIVE_IND,
        Upper(STUD.LEVY_CATEGORY) As LEVY_CATEGORY,
        STUD.QUALIFICATION,
        STUD.QUALIFICATION_NAME,
        Upper(STUD.PRESENT_CAT) As PRESENT_CAT,
        Upper(STUD.ENROL_CAT) As ENROL_CAT,
        Upper(STUD.QUAL_TYPE) As QUAL_TYPE,
        Upper(STUD.ENTRY_LEVEL) As ENTRY_LEVEL,
        Upper(STUD.STATUS_FINAL) As STATUS_FINAL,
        STUD.FSITEORGUNITNUMBER,
        STUD.CAMPUS,
        STUD.ORGUNIT_NAME,
        STUD.DATEQUALLEVELSTARTED,
        STUD.STARTDATE,
        STUD.DATEENROL,
        STUD.DATEENROL_CALC,
        STUD.DISCONTINUEDATE,
        STUD.RESULTPASSDATE,
        STUD.RESULT,
        STUD.ISHEMISSUBSIDY,
        STUD.ISMAINQUALLEVEL,
        STUD.ENROLACADEMICYEAR,
        STUD.ENROLHISTORYYEAR,
        STUD.MIN,
        STUD.MIN_UNIT,
        STUD.MAX,
        STUD.MAX_UNIT,
        STUD.CERT_TYPE,
        STUD.LEVY_TYPE,
        STUD.BLACKLIST,
        STUD.LONG,
        STUD.DISCONTINUE_REAS,
        STUD.POSTPONE_REAS,
        STUD.FBUSINESSENTITYID,
        STUD.ORGUNIT_TYPE,
        STUD.ISCONDITIONALREG,
        STUD.MARKSFINALISEDDATE,
        STUD.RESULTISSUEDATE,
        STUD.EXAMSUBMINIMUM,
        STUD.ISCUMLAUDE,
        STUD.ISPOSSIBLEGRADUATE,
        STUD.FACCEPTANCETESTCODEID,
        STUD.FENROLMENTPRESENTATIONID,
        STUD.FQUALLEVELAPID,
        STUD.FPROGRAMAPID,
        FEE.TRAN_COUNT,
        FEE.FUSERBUSINESSENTITYID
    From
        X000_Student STUD Left Join
        X010_Trans_feereg FEE On FEE.STUDENT = STUD.KSTUDBUSENTID
    Where
        STUD.ISMAINQUALLEVEL = 1 And STUD.ISCONDITIONALREG = 0
    Order by
        STUD.ENROL_CAT,
        STUD.KSTUDBUSENTID      
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = s_sql.replace("%LEVY%", str(i_calc))
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    """*****************************************************************************
    REGISTRATION FEE REPORTS
    *****************************************************************************"""
    if l_debug:
        print("REGISTRATION FEE REPORTS")
    funcfile.writelog("REGISTRATION FEE REPORTS")

    # ADD REGISTRATION LEVIED FEES TO THE STUDENTS LIST
    if l_debug:
        print("Report presentation category...")
    sr_file = "X010_Report_feereg_present"
    s_sql = "CREATE TABLE " + sr_file + " AS" + """
    Select
        Upper(FEE.CAMPUS) As CAMPUS,
        Upper(FEE.FEE_TYPE) As FEE_TYPE,
        Upper(FEE.PRESENT_CAT) As PRESENT_CAT,
        Count(FEE.FBUSINESSENTITYID) As COUNT,
        Total(FEE.FEE_CALC) As TOTAL
    From
        X010_Student_feereg FEE
    Group By
        FEE.CAMPUS,
        FEE.FEE_TYPE,
        FEE.PRESENT_CAT
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = s_sql.replace("%LEVY%", str(i_calc))
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # ADD REGISTRATION LEVIED FEES TO THE STUDENTS LIST
    if l_debug:
        print("Report enrolment category...")
    sr_file = "X010_Report_feereg_enrol"
    s_sql = "CREATE TABLE " + sr_file + " AS" + """
    Select
        Upper(FEE.CAMPUS) As CAMPUS,
        Upper(FEE.FEE_TYPE) As FEE_TYPE,
        Upper(FEE.PRESENT_CAT) As PRESENT_CAT,
        Upper(FEE.ENROL_CAT) As ENROL_CAT,
        Count(FEE.FBUSINESSENTITYID) As COUNT,
        Total(FEE.FEE_CALC) As TOTAL
    From
        X010_Student_feereg FEE
    Group By
        FEE.CAMPUS,
        FEE.FEE_TYPE,
        FEE.PRESENT_CAT,
        FEE.ENROL_CAT
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = s_sql.replace("%LEVY%", str(i_calc))
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    """*****************************************************************************
    TEST REGISTRATION FEE CONTACT NULL
    *****************************************************************************"""
    if l_debug:
        print("REGISTRATION FEE CONTACT NULL")
    funcfile.writelog("REGISTRATION FEE CONTACT NULL")

    # EXCLUSIONS
    # Only 1 NO TRANSACTION
    # Contact students only

    # DECLARE VARIABLES
    i_finding_after: int = 0
    s_desc = "Registration fee null or not levied"

    # IDENTIFY REGISTRATION FEES CONTACT NOT LEVIED
    # EXCLUDE DISTANCE STUDENTS
    if l_debug:
        print("Identify null registration fees...")
    sr_file = "X010aa_Regfee_null"
    s_sql = "CREATE TABLE " + sr_file + " AS" + """
    Select
        FIND.*
    From
        X010_Student_feereg FIND
    Where
        FIND.FEE_TYPE Like '1%' And
        FIND.PRESENT_CAT Like 'C%'
    Order by
        FIND.ENROL_CAT,
        FIND.QUAL_TYPE,
        FIND.QUALIFICATION,
        FIND.ENTRY_LEVEL,
        FIND.KSTUDBUSENTID    
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD TABLE: " + sr_file)
    if l_export and funcsys.tablerowcount(so_curs, sr_file) > 0:
        if l_debug:
            print("Export findings...")
        sx_path = re_path + "/"
        sx_file = "Student_fee_test_010aa_reg_fee_contact_null_"
        sx_file_dated = sx_file + funcdatn.get_today_date_file()
        s_head = funccsv.get_colnames_sqlite(so_conn, sr_file)
        funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file, s_head)
        funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file_dated, s_head)
        funcfile.writelog("%t EXPORT DATA: " + sx_path + sx_file)

    # IDENTIFY FINDINGS
    # EXCLUDE CONDITIONAL REGISTRATIONS
    if l_debug:
        print("Identify findings...")
    sr_file = "X010ab_findings"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        'NWU' As ORG,
        FIND.CAMPUS As LOC,
        FIND.KSTUDBUSENTID As STUDENT,
        FIND.FEE_CALC,
        FIND.FUSERBUSINESSENTITYID As USER
    From
        X010aa_Regfee_null FIND
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # COUNT THE NUMBER OF FINDINGS
    i_finding_before: int = funcsys.tablerowcount(so_curs, sr_file)
    if l_debug:
        print("*** Found " + str(i_finding_before) + " exceptions ***")
    funcfile.writelog("%t FINDING: " + str(i_finding_before) + " REGISTRATION FEE NULL finding(s)")

    # GET PREVIOUS FINDINGS
    if l_reg and i_finding_before > 0:
        i = functest.get_previous_finding(so_curs, ed_path, "302_reported.txt", "registration fee null", "ITTTT")
        so_conn.commit()

    # SET PREVIOUS FINDINGS
    if l_reg and i_finding_before > 0:
        i = functest.set_previous_finding(so_curs)
        so_conn.commit()

    # ADD PREVIOUS FINDINGS
    sr_file = "X010ad_add_previous"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if l_reg and i_finding_before > 0:
        if l_debug:
            print("Join previously reported to current findings...")
        s_sql = "CREATE TABLE " + sr_file + " AS" + """
        Select
            FIND.*,
            'registration fee null' AS PROCESS,
            '%TODAY%' AS DATE_REPORTED,
            '%DAYS%' AS DATE_RETEST,
            PREV.PROCESS AS PREV_PROCESS,
            PREV.DATE_REPORTED AS PREV_DATE_REPORTED,
            PREV.DATE_RETEST AS PREV_DATE_RETEST,
            PREV.REMARK
        From
            X010ab_findings FIND Left Join
            Z001ab_setprev PREV ON PREV.FIELD1 = FIND.STUDENT
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        s_sql = s_sql.replace("%TODAY%", funcdatn.get_today_date())
        s_sql = s_sql.replace("%DAYS%", funcdatn.get_current_month_end_next())
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD LIST TO UPDATE FINDINGS
    sr_file = "X010ae_new_previous"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if l_reg and i_finding_before > 0:
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            PREV.PROCESS,
            PREV.STUDENT AS FIELD1,
            '' AS FIELD2,
            '' AS FIELD3,
            '' AS FIELD4,
            '' AS FIELD5,
            PREV.DATE_REPORTED,
            PREV.DATE_RETEST,
            PREV.REMARK
        From
            X010ad_add_previous PREV
        Where
            PREV.PREV_PROCESS Is Null Or
            PREV.DATE_REPORTED > PREV.PREV_DATE_RETEST And PREV.REMARK = ""
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)
        # Export findings to previous reported file
        i_finding_after = funcsys.tablerowcount(so_curs, sr_file)
        if i_finding_after > 0:
            if l_debug:
                print("*** " + str(i_finding_after) + " Finding(s) to report ***")
            sx_path = ed_path
            sx_file = "302_reported"
            # Read the header data
            s_head = funccsv.get_colnames_sqlite(so_conn, sr_file)
            # Write the data
            if l_record:
                funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file, s_head, "a", ".txt")
                funcfile.writelog("%t FINDING: " + str(i_finding_after) + " new finding(s) to export")
                funcfile.writelog("%t EXPORT DATA: " + sr_file)
            if l_mess:
                funcsms.send_telegram('', 'administrator',
                                      '<b>' + str(i_finding_before) + '/' + str(i_finding_after) + '</b> ' + s_desc)
        else:
            if l_debug:
                print("*** No new findings to report ***")
            funcfile.writelog("%t FINDING: No new findings to export")

    # IMPORT OFFICERS FOR MAIL REPORTING PURPOSES
    if l_reg and i_finding_before > 0 and i_finding_after > 0:
        i = functest.get_officer(so_curs, "VSS", "stud_fee_test_reg_fee_null_contact_officer")
        so_conn.commit()

    # IMPORT SUPERVISORS FOR MAIL REPORTING PURPOSES
    if l_reg and i_finding_before > 0 and i_finding_after > 0:
        i = functest.get_supervisor(so_curs, "VSS", "stud_fee_test_reg_fee_null_contact_supervisor")
        so_conn.commit()

    # ADD CONTACT DETAILS TO FINDINGS
    sr_file = "X010ah_detail"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if l_reg and i_finding_before > 0 and i_finding_after > 0:
        if l_debug:
            print("Add contact details to findings...")
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            PREV.ORG,
            PREV.LOC,
            PREV.STUDENT,
            PREV.FEE_CALC,
            STUD.DATEENROL,
            Upper(STUD.PRESENT_CAT) As PRESENT_CAT,
            Upper(STUD.ENROL_CAT) As ENROL_CAT,
            Upper(STUD.QUAL_TYPE) As QUAL_TYPE,
            CAMP_OFF.EMPLOYEE_NUMBER As CAMP_OFF_NUMB,
            CAMP_OFF.NAME_ADDR As CAMP_OFF_NAME,
            CASE
                WHEN  CAMP_OFF.EMPLOYEE_NUMBER <> '' THEN CAMP_OFF.EMPLOYEE_NUMBER||'@nwu.ac.za'
                ELSE CAMP_OFF.EMAIL_ADDRESS
            END As CAMP_OFF_MAIL,
            CAMP_OFF.EMAIL_ADDRESS As CAMP_OFF_MAIL2,
            CAMP_SUP.EMPLOYEE_NUMBER As CAMP_SUP_NUMB,
            CAMP_SUP.NAME_ADDR As CAMP_SUP_NAME,
            CASE
                WHEN CAMP_SUP.EMPLOYEE_NUMBER <> '' THEN CAMP_SUP.EMPLOYEE_NUMBER||'@nwu.ac.za'
                ELSE CAMP_SUP.EMAIL_ADDRESS
            END As CAMP_SUP_MAIL,
            CAMP_SUP.EMAIL_ADDRESS As CAMP_SUP_MAIL2,
            ORG_OFF.EMPLOYEE_NUMBER As ORG_OFF_NUMB,
            ORG_OFF.NAME_ADDR As ORG_OFF_NAME,
            CASE
                WHEN ORG_OFF.EMPLOYEE_NUMBER <> '' THEN ORG_OFF.EMPLOYEE_NUMBER||'@nwu.ac.za'
                ELSE ORG_OFF.EMAIL_ADDRESS
            END As ORG_OFF_MAIL,
            ORG_OFF.EMAIL_ADDRESS As ORG_OFF_MAIL2,
            ORG_SUP.EMPLOYEE_NUMBER As ORG_SUP_NUMB,
            ORG_SUP.NAME_ADDR As ORG_SUP_NAME,
            CASE
                WHEN ORG_SUP.EMPLOYEE_NUMBER <> '' THEN ORG_SUP.EMPLOYEE_NUMBER||'@nwu.ac.za'
                ELSE ORG_SUP.EMAIL_ADDRESS
            END As ORG_SUP_MAIL,
            ORG_SUP.EMAIL_ADDRESS As ORG_SUP_MAIL2,
            PREV.USER As USER_NUMB,
            CASE
                WHEN PREV.USER != '' THEN PEOP.NAME_ADDR
                WHEN CAMP_OFF.NAME_ADDR != '' THEN CAMP_OFF.NAME_ADDR 
                ELSE ''
            END As USER_NAME,
            CASE
                WHEN PREV.USER != '' THEN PREV.USER||'@nwu.ac.za'
                WHEN CAMP_OFF.EMPLOYEE_NUMBER != '' THEN CAMP_OFF.EMPLOYEE_NUMBER||'@nwu.ac.za'
                ELSE ''
            END As USER_MAIL,
            CASE
                WHEN PREV.USER != '' THEN PEOP.EMAIL_ADDRESS
                WHEN CAMP_OFF.EMPLOYEE_NUMBER != '' THEN CAMP_OFF.EMAIL_ADDRESS
                ELSE ''
            END As USER_MAIL2
        From
            X010ad_add_previous PREV Left Join
            Z001af_officer CAMP_OFF On CAMP_OFF.CAMPUS = PREV.LOC Left Join
            Z001af_officer ORG_OFF On ORG_OFF.CAMPUS = PREV.ORG Left Join
            Z001ag_supervisor CAMP_SUP On CAMP_SUP.CAMPUS = PREV.LOC Left Join
            Z001ag_supervisor ORG_SUP On ORG_SUP.CAMPUS = PREV.ORG Left Join
            X000_Student STUD On STUD.KSTUDBUSENTID = PREV.STUDENT Left Join
            PEOPLE.X002_PEOPLE_CURR PEOP ON PEOP.EMPLOYEE_NUMBER = PREV.USER
        Where
            PREV.PREV_PROCESS Is Null Or
            PREV.DATE_REPORTED > PREV.PREV_DATE_RETEST And PREV.REMARK = ""
        ;"""
        """
        WHEN CAMP_OFF.NAME_ADDR != '' THEN CAMP_OFF.NAME_ADDR 
        """
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD THE FINAL TABLE FOR EXPORT AND REPORT
    sr_file = "X010ax_Regfee_null"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if l_debug:
        print("Build the final report")
    if l_reg and i_finding_before > 0 and i_finding_after > 0:
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            'REGISTRATION FEE NULL' As Audit_finding,
            FIND.ORG As 'Organization',
            FIND.LOC As 'Campus',
            FIND.STUDENT As 'Student',
            FIND.FEE_CALC As 'Registration_fee',
            FIND.DATEENROL As 'Date_enrol',
            FIND.PRESENT_CAT As 'Present_cat',
            FIND.ENROL_CAT As 'Enrol_cat',
            FIND.QUAL_TYPE As 'Qualification_type',
            FIND.CAMP_OFF_NAME AS Responsible_Officer,
            FIND.CAMP_OFF_NUMB AS Responsible_Officer_Numb,
            FIND.CAMP_OFF_MAIL AS Responsible_Officer_Mail,
            FIND.CAMP_SUP_NAME AS Supervisor,
            FIND.CAMP_SUP_NUMB AS Supervisor_Numb,
            FIND.CAMP_SUP_MAIL AS Supervisor_Mail,
            FIND.ORG_OFF_NAME AS Org_Officer,
            FIND.ORG_OFF_NUMB AS Org_Officer_Numb,
            FIND.ORG_OFF_MAIL AS Org_Officer_Mail,
            FIND.ORG_SUP_NAME AS Org_Supervisor,
            FIND.ORG_SUP_NUMB AS Org_Supervisor_Numb,
            FIND.ORG_SUP_MAIL AS Org_Supervisor_Mail,
            FIND.USER_NAME As User,
            FIND.USER_NUMB As User_Numb,
            FIND.USER_MAIL As User_Mail
        From
            X010ah_detail FIND
        Order by
            FIND.LOC,
            FIND.PRESENT_CAT,
            FIND.ENROL_CAT,
            FIND.QUAL_TYPE,
            FIND.STUDENT
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)
        # Export findings
        if l_export and funcsys.tablerowcount(so_curs, sr_file) > 0:
            if l_debug:
                print("Export findings...")
            sx_path = re_path + "/"
            sx_file = "Student_fee_test_010ex_reg_fee_contact_null_"
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

    """*****************************************************************************
    TEST REGISTRATION FEE CONTACT NEGATIVE
    *****************************************************************************"""
    if l_debug:
        print("REGISTRATION FEE CONTACT NEGATIVE")
    funcfile.writelog("REGISTRATION FEE CONTACT NEGATIVE")

    # DECLARE VARIABLES
    i_finding_after: int = 0
    s_desc = "Registration fee is a credit or negative amount"

    # IDENTIFY REGISTRATION FEES CONTACT NEGATIVES
    if l_debug:
        print("Identify negative registration fees...")
    sr_file = "X010ba_Regfee_negative"
    s_sql = "CREATE TABLE " + sr_file + " AS" + """
    Select
        FIND.*
    From
        X010_Student_feereg FIND
    Where
        FIND.FEE_TYPE Like '2%' And
        FIND.PRESENT_CAT Like 'C%'
    Order by
        FIND.ENROL_CAT,
        FIND.QUAL_TYPE,
        FIND.QUALIFICATION,
        FIND.ENTRY_LEVEL,
        FIND.KSTUDBUSENTID    
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD TABLE: " + sr_file)
    if l_export and funcsys.tablerowcount(so_curs, sr_file) > 0:
        if l_debug:
            print("Export findings...")
        sx_path = re_path + "/"
        sx_file = "Student_fee_test_010ba_reg_fee_contact_negative_"
        sx_file_dated = sx_file + funcdatn.get_today_date_file()
        s_head = funccsv.get_colnames_sqlite(so_conn, sr_file)
        funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file, s_head)
        funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file_dated, s_head)
        funcfile.writelog("%t EXPORT DATA: " + sx_path + sx_file)

    # IDENTIFY FINDINGS
    if l_debug:
        print("Identify findings...")
    sr_file = "X010bb_findings"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        'NWU' As ORG,
        FIND.CAMPUS As LOC,
        FIND.KSTUDBUSENTID As STUDENT,
        FIND.FEE_CALC,
        FIND.FUSERBUSINESSENTITYID As USER
    From
        X010ba_Regfee_negative FIND
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # COUNT THE NUMBER OF FINDINGS
    i_finding_before: int = funcsys.tablerowcount(so_curs, sr_file)
    if l_debug:
        print("*** Found " + str(i_finding_before) + " exceptions ***")
    funcfile.writelog("%t FINDING: " + str(i_finding_before) + " REGISTRATION FEE NEGATIVE finding(s)")

    # GET PREVIOUS FINDINGS
    sr_file = "X010bc_get_previous"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if l_reg and i_finding_before > 0:
        if l_debug:
            print("Import previously reported findings...")
        so_curs.execute(
            "CREATE TABLE " + sr_file + """
            (PROCESS TEXT,
            FIELD1 INT,
            FIELD2 INT,
            FIELD3 TEXT,
            FIELD4 TEXT,
            FIELD5 TEXT,
            DATE_REPORTED TEXT,
            DATE_RETEST TEXT,
            REMARK TEXT)
            """)
        co = open(ed_path + "302_reported.txt", "r")
        co_reader = csv.reader(co)
        # Read the COLUMN database data
        for row in co_reader:
            # Populate the column variables
            if row[0] == "PROCESS":
                continue
            elif row[0] != "registration fee negative":
                continue
            else:
                s_cols = "INSERT INTO " + sr_file + " VALUES('" + row[0] + "','" + row[1] + "','" + row[2] + "','" + \
                         row[
                             3] + "','" + row[4] + "','" + row[5] + "','" + row[6] + "','" + row[7] + "','" + row[
                             8] + "')"
                so_curs.execute(s_cols)
        so_conn.commit()
        # Close the imported data file
        co.close()
        funcfile.writelog("%t IMPORT TABLE: " + ed_path + "302_reported.txt (" + sr_file + ")")

    # SET PREVIOUS FINDINGS
    sr_file = "X010bc_set_previous"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if l_reg and i_finding_before > 0:
        if l_debug:
            print("Obtain the latest previous finding...")
        s_sql = "Create Table " + sr_file + " As" + """
        Select
            GET.PROCESS,
            GET.FIELD1,
            GET.FIELD2,
            GET.FIELD3,
            GET.FIELD4,
            GET.FIELD5,
            Max(GET.DATE_REPORTED) As DATE_REPORTED,
            GET.DATE_RETEST,
            GET.REMARK
        From
            X010bc_get_previous GET
        Group By
            GET.FIELD1,
            GET.FIELD2        
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # ADD PREVIOUS FINDINGS
    sr_file = "X010bd_add_previous"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if l_reg and i_finding_before > 0:
        if l_debug:
            print("Join previously reported to current findings...")
        s_sql = "CREATE TABLE " + sr_file + " AS" + """
        Select
            FIND.*,
            'registration fee negative' AS PROCESS,
            '%TODAY%' AS DATE_REPORTED,
            '%DAYS%' AS DATE_RETEST,
            PREV.PROCESS AS PREV_PROCESS,
            PREV.DATE_REPORTED AS PREV_DATE_REPORTED,
            PREV.DATE_RETEST AS PREV_DATE_RETEST,
            PREV.REMARK
        From
            X010bb_findings FIND Left Join
            X010bc_set_previous PREV ON PREV.FIELD1 = FIND.STUDENT And
                PREV.FIELD2 = FIND.FEE_CALC
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        s_sql = s_sql.replace("%TODAY%", funcdatn.get_today_date())
        s_sql = s_sql.replace("%DAYS%", funcdatn.get_current_month_end_next())
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD LIST TO UPDATE FINDINGS
    sr_file = "X010be_new_previous"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if l_reg and i_finding_before > 0:
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            PREV.PROCESS,
            PREV.STUDENT AS FIELD1,
            FEE_CALC AS FIELD2,
            '' AS FIELD3,
            '' AS FIELD4,
            '' AS FIELD5,
            PREV.DATE_REPORTED,
            PREV.DATE_RETEST,
            PREV.REMARK
        From
            X010bd_add_previous PREV
        Where
            PREV.PREV_PROCESS Is Null Or
            PREV.DATE_REPORTED > PREV.PREV_DATE_RETEST And PREV.REMARK = ""            
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)
        # Export findings to previous reported file
        i_finding_after = funcsys.tablerowcount(so_curs, sr_file)
        if i_finding_after > 0:
            if l_debug:
                print("*** " + str(i_finding_after) + " Finding(s) to report ***")
            sx_path = ed_path
            sx_file = "302_reported"
            # Read the header data
            s_head = funccsv.get_colnames_sqlite(so_conn, sr_file)
            # Write the data
            if l_record:
                funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file, s_head, "a", ".txt")
                funcfile.writelog("%t FINDING: " + str(i_finding_after) + " new finding(s) to export")
                funcfile.writelog("%t EXPORT DATA: " + sr_file)
            if l_mess:
                funcsms.send_telegram('', 'administrator',
                                      '<b>' + str(i_finding_before) + '/' + str(i_finding_after) + '</b> ' + s_desc)
        else:
            if l_debug:
                print("*** No new findings to report ***")
            funcfile.writelog("%t FINDING: No new findings to export")

    # IMPORT OFFICERS FOR MAIL REPORTING PURPOSES
    sr_file = "X010bf_officer"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if l_reg and i_finding_before > 0:
        if i_finding_after > 0:
            if l_debug:
                print("Import reporting officers for mail purposes...")
            s_sql = "CREATE TABLE " + sr_file + " AS " + """
            Select
                OFFICER.LOOKUP,
                Upper(OFFICER.LOOKUP_CODE) AS CAMPUS,
                OFFICER.LOOKUP_DESCRIPTION AS EMPLOYEE_NUMBER,
                PEOP.NAME_ADDR As NAME,
                PEOP.EMAIL_ADDRESS
            From
                VSS.X000_OWN_LOOKUPS OFFICER Left Join
                PEOPLE.X002_PEOPLE_CURR PEOP ON PEOP.EMPLOYEE_NUMBER = OFFICER.LOOKUP_DESCRIPTION
            Where
                OFFICER.LOOKUP = 'stud_fee_test_reg_fee_negative_contact_officer'
            ;"""
            so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
            so_curs.execute(s_sql)
            so_conn.commit()
            funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # IMPORT SUPERVISORS FOR MAIL REPORTING PURPOSES
    sr_file = "X010bg_supervisor"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if l_reg and i_finding_before > 0 and i_finding_after > 0:
        if l_debug:
            print("Import reporting supervisors for mail purposes...")
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            SUPERVISOR.LOOKUP,
            Upper(SUPERVISOR.LOOKUP_CODE) AS CAMPUS,
            SUPERVISOR.LOOKUP_DESCRIPTION AS EMPLOYEE_NUMBER,
            PEOP.NAME_ADDR As NAME,
            PEOP.EMAIL_ADDRESS
        From
            VSS.X000_OWN_LOOKUPS SUPERVISOR Left Join
            PEOPLE.X002_PEOPLE_CURR PEOP ON PEOP.EMPLOYEE_NUMBER = SUPERVISOR.LOOKUP_DESCRIPTION
        Where
            SUPERVISOR.LOOKUP = 'stud_fee_test_reg_fee_negative_contact_supervisor'
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # ADD CONTACT DETAILS TO FINDINGS
    sr_file = "X010bh_detail"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if l_reg and i_finding_before > 0 and i_finding_after > 0:
        if l_debug:
            print("Add contact details to findings...")
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            PREV.ORG,
            PREV.LOC,
            PREV.STUDENT,
            PREV.FEE_CALC,
            STUD.DATEENROL,
            Upper(STUD.PRESENT_CAT) As PRESENT_CAT,
            Upper(STUD.ENROL_CAT) As ENROL_CAT,
            Upper(STUD.QUAL_TYPE) As QUAL_TYPE,
            CAMP_OFF.EMPLOYEE_NUMBER As CAMP_OFF_NUMB,
            CAMP_OFF.NAME As CAMP_OFF_NAME,
            CASE
                WHEN  CAMP_OFF.EMPLOYEE_NUMBER <> '' THEN CAMP_OFF.EMPLOYEE_NUMBER||'@nwu.ac.za'
                ELSE CAMP_OFF.EMAIL_ADDRESS
            END As CAMP_OFF_MAIL,
            CAMP_OFF.EMAIL_ADDRESS As CAMP_OFF_MAIL2,
            CAMP_SUP.EMPLOYEE_NUMBER As CAMP_SUP_NUMB,
            CAMP_SUP.NAME As CAMP_SUP_NAME,
            CASE
                WHEN CAMP_SUP.EMPLOYEE_NUMBER <> '' THEN CAMP_SUP.EMPLOYEE_NUMBER||'@nwu.ac.za'
                ELSE CAMP_SUP.EMAIL_ADDRESS
            END As CAMP_SUP_MAIL,
            CAMP_SUP.EMAIL_ADDRESS As CAMP_SUP_MAIL2,
            ORG_OFF.EMPLOYEE_NUMBER As ORG_OFF_NUMB,
            ORG_OFF.NAME As ORG_OFF_NAME,
            CASE
                WHEN ORG_OFF.EMPLOYEE_NUMBER <> '' THEN ORG_OFF.EMPLOYEE_NUMBER||'@nwu.ac.za'
                ELSE ORG_OFF.EMAIL_ADDRESS
            END As ORG_OFF_MAIL,
            ORG_OFF.EMAIL_ADDRESS As ORG_OFF_MAIL2,
            ORG_SUP.EMPLOYEE_NUMBER As ORG_SUP_NUMB,
            ORG_SUP.NAME As ORG_SUP_NAME,
            CASE
                WHEN ORG_SUP.EMPLOYEE_NUMBER <> '' THEN ORG_SUP.EMPLOYEE_NUMBER||'@nwu.ac.za'
                ELSE ORG_SUP.EMAIL_ADDRESS
            END As ORG_SUP_MAIL,
            ORG_SUP.EMAIL_ADDRESS As ORG_SUP_MAIL2,
            PREV.USER As USER_NUMB,
            CASE
                WHEN PREV.USER != '' THEN PEOP.NAME_ADDR
                WHEN CAMP_OFF.NAME != '' THEN CAMP_OFF.NAME 
                ELSE ''
            END As USER_NAME,
            CASE
                WHEN PREV.USER != '' THEN PREV.USER||'@nwu.ac.za'
                WHEN CAMP_OFF.EMPLOYEE_NUMBER != '' THEN CAMP_OFF.EMPLOYEE_NUMBER||'@nwu.ac.za'
                ELSE ''
            END As USER_MAIL,
            CASE
                WHEN PREV.USER != '' THEN PEOP.EMAIL_ADDRESS
                WHEN CAMP_OFF.EMPLOYEE_NUMBER != '' THEN CAMP_OFF.EMAIL_ADDRESS
                ELSE ''
            END As USER_MAIL2
        From
            X010bd_add_previous PREV Left Join
            X010bf_officer CAMP_OFF On CAMP_OFF.CAMPUS = PREV.LOC Left Join
            X010bf_officer ORG_OFF On ORG_OFF.CAMPUS = PREV.ORG Left Join
            X010bg_supervisor CAMP_SUP On CAMP_SUP.CAMPUS = PREV.LOC Left Join
            X010bg_supervisor ORG_SUP On ORG_SUP.CAMPUS = PREV.ORG Left Join
            X000_Student STUD On STUD.KSTUDBUSENTID = PREV.STUDENT Left Join
            PEOPLE.X002_PEOPLE_CURR PEOP ON PEOP.EMPLOYEE_NUMBER = PREV.USER
        Where
          PREV.PREV_PROCESS IS NULL
        ;"""
        """
        WHEN CAMP_OFF.NAME != '' THEN CAMP_OFF.NAME 
        """
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD THE FINAL TABLE FOR EXPORT AND REPORT
    sr_file = "X010bx_Regfee_negative"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if l_debug:
        print("Build the final report")
    if l_reg and i_finding_before > 0 and i_finding_after > 0:
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            'REGISTRATION FEE NEGATIVE' As Audit_finding,
            FIND.ORG As 'Organization',
            FIND.LOC As 'Campus',
            FIND.STUDENT As 'Student',
            FIND.FEE_CALC As 'Registration_fee',
            FIND.DATEENROL As 'Date_enrol',
            FIND.PRESENT_CAT As 'Present_cat',
            FIND.ENROL_CAT As 'Enrol_cat',
            FIND.QUAL_TYPE As 'Qualification_type',
            FIND.CAMP_OFF_NAME AS Responsible_Officer,
            FIND.CAMP_OFF_NUMB AS Responsible_Officer_Numb,
            FIND.CAMP_OFF_MAIL AS Responsible_Officer_Mail,
            FIND.CAMP_SUP_NAME AS Supervisor,
            FIND.CAMP_SUP_NUMB AS Supervisor_Numb,
            FIND.CAMP_SUP_MAIL AS Supervisor_Mail,
            FIND.ORG_OFF_NAME AS Org_Officer,
            FIND.ORG_OFF_NUMB AS Org_Officer_Numb,
            FIND.ORG_OFF_MAIL AS Org_Officer_Mail,
            FIND.ORG_SUP_NAME AS Org_Supervisor,
            FIND.ORG_SUP_NUMB AS Org_Supervisor_Numb,
            FIND.ORG_SUP_MAIL AS Org_Supervisor_Mail,
            FIND.USER_NAME As User,
            FIND.USER_NUMB As User_Numb,
            FIND.USER_MAIL As User_Mail
        From
            X010bh_detail FIND
        Order by
            FIND.LOC,
            FIND.PRESENT_CAT,
            FIND.ENROL_CAT,
            FIND.QUAL_TYPE,
            FIND.STUDENT
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)
        # Export findings
        if l_export and funcsys.tablerowcount(so_curs, sr_file) > 0:
            if l_debug:
                print("Export findings...")
            sx_path = re_path + "/"
            sx_file = "Student_fee_test_010ex_reg_fee_contact_negative_"
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

    """*****************************************************************************
    TEST REGISTRATION FEE CONTACT ZERO
    *****************************************************************************"""
    if l_debug:
        print("REGISTRATION FEE CONTACT ZERO")
    funcfile.writelog("REGISTRATION FEE CONTACT ZERO")

    # FILES NEEDED
    # X010_Student_feereg

    # DECLARE TEST VARIABLES
    s_fprefix: str = "X010c"
    s_fname: str = "reg_fee_zero"
    s_finding: str = "REGISTRATION FEE ZERO VALUE TRANSACTION"
    s_xfile: str = "302_reported.txt"
    i_finding_before: int = 0
    i_finding_after: int = 0
    s_desc = "Registration fee transactions has no value"

    # OBTAIN TEST DATA
    if l_debug:
        print("Obtain test data...")
    sr_file: str = s_fprefix + "a_" + s_fname
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        FIND.*
    From
        X010_Student_feereg FIND
    Where
        FIND.FEE_TYPE Like '3%'
    Order by
        FIND.ENROL_CAT,
        FIND.QUAL_TYPE,
        FIND.QUALIFICATION,
        FIND.ENTRY_LEVEL,
        FIND.KSTUDBUSENTID  
    ;"""
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)
    if l_export and funcsys.tablerowcount(so_curs, sr_file) > 0:
        if l_debug:
            print("Export findings...")
        sx_path = re_path + "/"
        sx_file = "Student_fee_test_" + s_fprefix + "_" + s_finding + "_studentlist_"
        sx_file_dated = sx_file + funcdatn.get_today_date_file()
        s_head = funccsv.get_colnames_sqlite(so_conn, sr_file)
        funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file, s_head)
        # funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file_dated, s_head)
        funcfile.writelog("%t EXPORT DATA: " + sx_path + sx_file)

    # SELECT TEST DATA
    if l_debug:
        print("Identify findings...")
    sr_file = s_fprefix + "b_finding"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        'NWU' As ORG,
        FIND.CAMPUS As LOC,
        FIND.KSTUDBUSENTID As ID,
        FIND.QUALIFICATION,
        FIND.FEE_CALC,
        FIND.FUSERBUSINESSENTITYID AS USER
    From
        %FILEP%%FILEN% FIND
    Where
        FIND.PRESENT_CAT Like('C%')    
    ;"""
    s_sql = s_sql.replace("%FILEP%", s_fprefix)
    s_sql = s_sql.replace("%FILEN%", "a_" + s_fname)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # COUNT THE NUMBER OF FINDINGS
    i_finding_before: int = funcsys.tablerowcount(so_curs, sr_file)
    if l_debug:
        print("*** Found " + str(i_finding_before) + " exceptions ***")
    funcfile.writelog("%t FINDING: " + str(i_finding_before) + " " + s_finding + " finding(s)")

    # GET PREVIOUS FINDINGS
    if i_finding_before > 0:
        i = functest.get_previous_finding(so_curs, ed_path, s_xfile, s_finding, "ITTTT")
        so_conn.commit()

    # SET PREVIOUS FINDINGS
    if i_finding_before > 0:
        i = functest.set_previous_finding(so_curs)
        so_conn.commit()

    # ADD PREVIOUS FINDINGS
    sr_file = s_fprefix + "d_addprev"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0:
        if l_debug:
            print("Join previously reported to current findings...")
        s_sql = "CREATE TABLE " + sr_file + " AS" + """
        Select
            FIND.*,
            Lower('%FINDING%') AS PROCESS,
            '%TODAY%' AS DATE_REPORTED,
            '%DAYS%' AS DATE_RETEST,
            PREV.PROCESS AS PREV_PROCESS,
            PREV.DATE_REPORTED AS PREV_DATE_REPORTED,
            PREV.DATE_RETEST AS PREV_DATE_RETEST,
            PREV.REMARK
        From
            %FILEP%b_finding FIND Left Join
            Z001ab_setprev PREV ON PREV.FIELD1 = FIND.ID And
                PREV.FIELD2 = FIND.QUALIFICATION
        ;"""
        s_sql = s_sql.replace("%FINDING%", s_finding)
        s_sql = s_sql.replace("%FILEP%", s_fprefix)
        s_sql = s_sql.replace("%TODAY%", funcdatn.get_today_date())
        s_sql = s_sql.replace("%DAYS%", funcdatn.get_current_month_end_next())
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD LIST TO UPDATE FINDINGS
    sr_file = s_fprefix + "e_newprev"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0:
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            PREV.PROCESS,
            PREV.ID AS FIELD1,
            PREV.QUALIFICATION AS FIELD2,
            '' AS FIELD3,
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
        s_sql = s_sql.replace("%FILEP%", s_fprefix)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)
        # Export findings to previous reported file
        i_finding_after = funcsys.tablerowcount(so_curs, sr_file)
        if i_finding_after > 0:
            if l_debug:
                print("*** " + str(i_finding_after) + " Finding(s) to report ***")
            sx_path = ed_path
            sx_file = s_xfile[:-4]
            # Read the header data
            s_head = funccsv.get_colnames_sqlite(so_conn, sr_file)
            # Write the data
            if l_record:
                funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file, s_head, "a", ".txt")
                funcfile.writelog("%t FINDING: " + str(i_finding_after) + " new finding(s) to export")
                funcfile.writelog("%t EXPORT DATA: " + sr_file)
            if l_mess:
                funcsms.send_telegram('',
                                      'administrator',
                                      '<b>' + str(i_finding_before) + '/' +
                                      str(i_finding_after) + '</b> ' + s_desc)
        else:
            if l_debug:
                print("*** No new findings to report ***")
            funcfile.writelog("%t FINDING: No new findings to export")

    # IMPORT OFFICERS FOR MAIL REPORTING PURPOSES
    if i_finding_before > 0 and i_finding_after > 0:
        i = functest.get_officer(so_curs, "VSS", "TEST " + s_finding + " OFFICER")
        so_conn.commit()

    # IMPORT SUPERVISORS FOR MAIL REPORTING PURPOSES
    if i_finding_before > 0 and i_finding_after > 0:
        i = functest.get_supervisor(so_curs, "VSS", "TEST " + s_finding + " SUPERVISOR")
        so_conn.commit()

    # ADD CONTACT DETAILS TO FINDINGS
    sr_file = s_fprefix + "h_detail"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0 and i_finding_after > 0:
        if l_debug:
            print("Add contact details to findings...")
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            PREV.ORG,
            PREV.LOC,
            PREV.ID,
            MAST.DATEENROL,
            MAST.DISCONTINUEDATE,
            PREV.QUALIFICATION,
            MAST.QUALIFICATION_NAME,
            MAST.QUAL_TYPE As QUALIFICATION_TYPE,        
            MAST.PRESENT_CAT,
            MAST.ENROL_CAT,
            CAMP_OFF.EMPLOYEE_NUMBER AS CAMP_OFF_NUMB,
            CAMP_OFF.NAME_ADDR AS CAMP_OFF_NAME,
            CASE
                WHEN  CAMP_OFF.EMPLOYEE_NUMBER != '' THEN CAMP_OFF.EMPLOYEE_NUMBER||'@nwu.ac.za'
                ELSE CAMP_OFF.EMAIL_ADDRESS
            END AS CAMP_OFF_MAIL,
            CAMP_OFF.EMAIL_ADDRESS AS CAMP_OFF_MAIL2,        
            CAMP_SUP.EMPLOYEE_NUMBER AS CAMP_SUP_NUMB,
            CAMP_SUP.NAME_ADDR AS CAMP_SUP_NAME,
            CASE
                WHEN CAMP_SUP.EMPLOYEE_NUMBER != '' THEN CAMP_SUP.EMPLOYEE_NUMBER||'@nwu.ac.za'
                ELSE CAMP_SUP.EMAIL_ADDRESS
            END AS CAMP_SUP_MAIL,
            CAMP_SUP.EMAIL_ADDRESS AS CAMP_SUP_MAIL2,
            ORG_OFF.EMPLOYEE_NUMBER AS ORG_OFF_NUMB,
            ORG_OFF.NAME_ADDR AS ORG_OFF_NAME,
            CASE
                WHEN ORG_OFF.EMPLOYEE_NUMBER != '' THEN ORG_OFF.EMPLOYEE_NUMBER||'@nwu.ac.za'
                ELSE ORG_OFF.EMAIL_ADDRESS
            END AS ORG_OFF_MAIL,
            ORG_OFF.EMAIL_ADDRESS AS ORG_OFF_MAIL2,
            ORG_SUP.EMPLOYEE_NUMBER AS ORG_SUP_NUMB,
            ORG_SUP.NAME_ADDR AS ORG_SUP_NAME,
            CASE
                WHEN ORG_SUP.EMPLOYEE_NUMBER != '' THEN ORG_SUP.EMPLOYEE_NUMBER||'@nwu.ac.za'
                ELSE ORG_SUP.EMAIL_ADDRESS
            END AS ORG_SUP_MAIL,
            ORG_SUP.EMAIL_ADDRESS AS ORG_SUP_MAIL2,
            CASE
                WHEN PEOP.NAME_ADDR Is Not Null THEN PREV.USER
            END AS U_NUMB,
            CASE
                WHEN PEOP.NAME_ADDR Is Not Null THEN PEOP.NAME_ADDR
                ELSE CAMP_OFF.NAME_ADDR
            END AS U_NAME, 
            CASE
                WHEN PEOP.NAME_ADDR Is Not Null THEN PREV.USER||'@nwu.ac.za'
                WHEN  CAMP_OFF.EMPLOYEE_NUMBER != '' THEN CAMP_OFF.EMPLOYEE_NUMBER||'@nwu.ac.za'
                ELSE CAMP_OFF.EMAIL_ADDRESS
            END AS U_MAIL        
        From
            %FILEP%d_addprev PREV Left Join
            %FILEP%%FILEN% MAST On MAST.KSTUDBUSENTID = PREV.ID And
                MAST.CAMPUS = PREV.LOC And
                MAST.QUALIFICATION = PREV.QUALIFICATION Left Join
            Z001af_officer CAMP_OFF On CAMP_OFF.CAMPUS = PREV.LOC Left Join
            Z001af_officer ORG_OFF On ORG_OFF.CAMPUS = PREV.ORG Left Join
            Z001ag_supervisor CAMP_SUP On CAMP_SUP.CAMPUS = PREV.LOC Left Join
            Z001ag_supervisor ORG_SUP On ORG_SUP.CAMPUS = PREV.ORG Left Join
            PEOPLE.X002_PEOPLE_CURR PEOP ON PEOP.EMPLOYEE_NUMBER = Cast(PREV.USER AS TEXT)
        Where
            PREV.PREV_PROCESS Is Null Or
            PREV.DATE_REPORTED > PREV.PREV_DATE_RETEST And PREV.REMARK = ""
        Order By
            PREV.LOC,
            PREV.QUALIFICATION,
            PREV.ID        
        ;"""
        s_sql = s_sql.replace("%FILEP%", s_fprefix)
        s_sql = s_sql.replace("%FILEN%", "a_" + s_fname)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD THE FINAL TABLE FOR EXPORT AND REPORT
    sr_file = s_fprefix + "x_" + s_fname
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if l_debug:
        print("Build the final report")
    if i_finding_before > 0 and i_finding_after > 0:
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            '%FIND%' As Audit_finding,
            FIND.ORG As Organization,
            FIND.LOC As Campus,
            FIND.ID As Student,
            FIND.DATEENROL As Date_enrol,
            FIND.DISCONTINUEDATE As Date_discontinue,
            FIND.QUALIFICATION As Qualification,
            FIND.QUALIFICATION_NAME As Qualification_name,
            FIND.QUALIFICATION_TYPE As Qualification_type,        
            FIND.PRESENT_CAT As Presentation_category,
            FIND.ENROL_CAT As Enrol_category,        
            FIND.CAMP_OFF_NAME AS Responsible_Officer,
            FIND.CAMP_OFF_NUMB AS Responsible_Officer_Numb,
            FIND.CAMP_OFF_MAIL AS Responsible_Officer_Mail,
            FIND.CAMP_SUP_NAME AS Supervisor,
            FIND.CAMP_SUP_NUMB AS Supervisor_Numb,
            FIND.CAMP_SUP_MAIL AS Supervisor_Mail,
            FIND.ORG_OFF_NAME AS Org_Officer,
            FIND.ORG_OFF_NUMB AS Org_Officer_Numb,
            FIND.ORG_OFF_MAIL AS Org_Officer_Mail,
            FIND.ORG_SUP_NAME AS Org_Supervisor,
            FIND.ORG_SUP_NUMB AS Org_Supervisor_Numb,
            FIND.ORG_SUP_MAIL AS Org_Supervisor_Mail,            
            FIND.U_NUMB As Tran_owner_numb,
            FIND.U_NAME As Tran_owner,
            FIND.U_MAIL As Tran_owner_mail
        From
            %FILEP%h_detail FIND
        ;"""
        s_sql = s_sql.replace("%FIND%", s_finding)
        s_sql = s_sql.replace("%FILEP%", s_fprefix)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)
        # Export findings
        if l_export and funcsys.tablerowcount(so_curs, sr_file) > 0:
            if l_debug:
                print("Export findings...")
            sx_path = re_path + "/"
            sx_file = "Registration_test_" + s_fprefix + "_" + s_finding.lower() + "_"
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

    """*****************************************************************************
    TEST REGISTRATION FEE CONTACT ABNORMAL
    *****************************************************************************"""
    if l_debug:
        print("TEST REGISTRATION FEE CONTACT ABNORMAL")
    funcfile.writelog("TEST REGISTRATION FEE CONTACT ABNORMAL")

    # DECLARE VARIABLES
    i_finding_after: int = 0
    s_desc = "Registration fee abnormal"

    # IDENTIFY REGISTRATION FEE AMOUNTS NOT MODE
    if l_debug:
        print("Identify abnormal registration fees...")
    sr_file = "X010ea_Regfee_abnormal"
    s_sql = "CREATE TABLE " + sr_file + " AS" + """
    Select
        FIND.*
    From
        X010_Student_feereg FIND
    Where
        FIND.FEE_TYPE Like '5%' And
        FIND.PRESENT_CAT Like 'C%'
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = s_sql.replace("%AMOUNT%", str(i_calc))
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD TABLE: " + sr_file)
    if l_export and funcsys.tablerowcount(so_curs, sr_file) > 0:
        if l_debug:
            print("Export findings...")
        sx_path = re_path + "/"
        sx_file = "Student_fee_test_010ea_reg_fee_contact_abnormal_"
        sx_file_dated = sx_file + funcdatn.get_today_date_file()
        s_head = funccsv.get_colnames_sqlite(so_conn, sr_file)
        funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file, s_head)
        funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file_dated, s_head)
        funcfile.writelog("%t EXPORT DATA: " + sx_path + sx_file)

    # IDENTIFY FINDINGS
    if l_debug:
        print("Identify findings...")
    sr_file = "X010eb_findings"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        'NWU' As ORG,
        FIND.CAMPUS As LOC,
        FIND.KSTUDBUSENTID As STUDENT,
        FIND.FEE_CALC,
        FIND.FUSERBUSINESSENTITYID As USER
    From
        X010ea_Regfee_abnormal FIND
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # COUNT THE NUMBER OF FINDINGS
    i_finding_before: int = funcsys.tablerowcount(so_curs, sr_file)
    if l_debug:
        print("*** Found " + str(i_finding_before) + " exceptions ***")
    funcfile.writelog("%t FINDING: " + str(i_finding_before) + " REGISTRATION FEE ABNORMAL finding(s)")

    # GET PREVIOUS FINDINGS
    sr_file = "X010ec_get_previous"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if l_reg and i_finding_before > 0:
        if l_debug:
            print("Import previously reported findings...")
        so_curs.execute(
            "CREATE TABLE " + sr_file + """
            (PROCESS TEXT,
            FIELD1 INT,
            FIELD2 REAL,
            FIELD3 TEXT,
            FIELD4 TEXT,
            FIELD5 TEXT,
            DATE_REPORTED TEXT,
            DATE_RETEST TEXT,
            REMARK TEXT)
            """)
        co = open(ed_path + "302_reported.txt", "r")
        co_reader = csv.reader(co)
        # Read the COLUMN database data
        for row in co_reader:
            # Populate the column variables
            if row[0] == "PROCESS":
                continue
            elif row[0] != "registration fee abnormal":
                continue
            else:
                s_cols = "INSERT INTO " + sr_file + " VALUES('" + row[0] + "','" + row[1] + "','" + row[2] + "','" + \
                         row[
                             3] + "','" + row[4] + "','" + row[5] + "','" + row[6] + "','" + row[7] + "','" + row[
                             8] + "')"
                so_curs.execute(s_cols)
        so_conn.commit()
        # Close the imported data file
        co.close()
        funcfile.writelog("%t IMPORT TABLE: " + ed_path + "302_reported.txt (" + sr_file + ")")

    # SET PREVIOUS FINDINGS
    sr_file = "X010ec_set_previous"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if l_reg and i_finding_before > 0:
        if l_debug:
            print("Obtain the latest previous finding...")
        s_sql = "Create Table " + sr_file + " As" + """
        Select
            GET.PROCESS,
            GET.FIELD1,
            GET.FIELD2,
            GET.FIELD3,
            GET.FIELD4,
            GET.FIELD5,
            Max(GET.DATE_REPORTED) As DATE_REPORTED,
            GET.DATE_RETEST,
            GET.REMARK
        From
            X010ec_get_previous GET
        Group By
            GET.FIELD1,
            GET.FIELD2        
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # ADD PREVIOUS FINDINGS
    sr_file = "X010ed_add_previous"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if l_reg and i_finding_before > 0:
        if l_debug:
            print("Join previously reported to current findings...")
        s_sql = "CREATE TABLE " + sr_file + " AS" + """
        Select
            FIND.*,
            'registration fee abnormal' AS PROCESS,
            '%TODAY%' AS DATE_REPORTED,
            '%DAYS%' AS DATE_RETEST,
            PREV.PROCESS AS PREV_PROCESS,
            PREV.DATE_REPORTED AS PREV_DATE_REPORTED,
            PREV.DATE_RETEST AS PREV_DATE_RETEST,
            PREV.REMARK
        From
            X010eb_findings FIND Left Join
            X010ec_set_previous PREV ON PREV.FIELD1 = FIND.STUDENT And
                PREV.FIELD2 = FIND.FEE_CALC
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        s_sql = s_sql.replace("%TODAY%", funcdatn.get_today_date())
        s_sql = s_sql.replace("%DAYS%", funcdatn.get_current_month_end_next())
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD LIST TO UPDATE FINDINGS
    sr_file = "X010ee_new_previous"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if l_reg and i_finding_before > 0:
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            PREV.PROCESS,
            PREV.STUDENT AS FIELD1,
            FEE_CALC AS FIELD2,
            '' AS FIELD3,
            '' AS FIELD4,
            '' AS FIELD5,
            PREV.DATE_REPORTED,
            PREV.DATE_RETEST,
            PREV.REMARK
        From
            X010ed_add_previous PREV
        Where
            PREV.PREV_PROCESS Is Null Or
            PREV.DATE_REPORTED > PREV.PREV_DATE_RETEST And PREV.REMARK = ""
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)
        # Export findings to previous reported file
        i_finding_after = funcsys.tablerowcount(so_curs, sr_file)
        if i_finding_after > 0:
            if l_debug:
                print("*** " + str(i_finding_after) + " Finding(s) to report ***")
            sx_path = ed_path
            sx_file = "302_reported"
            # Read the header data
            s_head = funccsv.get_colnames_sqlite(so_conn, sr_file)
            # Write the data
            if l_record:
                funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file, s_head, "a", ".txt")
                funcfile.writelog("%t FINDING: " + str(i_finding_after) + " new finding(s) to export")
                funcfile.writelog("%t EXPORT DATA: " + sr_file)
            if l_mess:
                funcsms.send_telegram('', 'administrator',
                                      '<b>' + str(i_finding_before) + '/' + str(i_finding_after) + '</b> ' + s_desc)
        else:
            if l_debug:
                print("*** No new findings to report ***")
            funcfile.writelog("%t FINDING: No new findings to export")

    # IMPORT OFFICERS FOR MAIL REPORTING PURPOSES
    sr_file = "X010ef_officer"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if l_reg and i_finding_before > 0:
        if i_finding_after > 0:
            if l_debug:
                print("Import reporting officers for mail purposes...")
            s_sql = "CREATE TABLE " + sr_file + " AS " + """
            Select
                OFFICER.LOOKUP,
                Upper(OFFICER.LOOKUP_CODE) AS CAMPUS,
                OFFICER.LOOKUP_DESCRIPTION AS EMPLOYEE_NUMBER,
                PEOP.NAME_ADDR As NAME,
                PEOP.EMAIL_ADDRESS
            From
                VSS.X000_OWN_LOOKUPS OFFICER Left Join
                PEOPLE.X002_PEOPLE_CURR PEOP ON PEOP.EMPLOYEE_NUMBER = OFFICER.LOOKUP_DESCRIPTION
            Where
                OFFICER.LOOKUP = 'stud_fee_test_reg_fee_abnormal_contact_officer'
            ;"""
            so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
            so_curs.execute(s_sql)
            so_conn.commit()
            funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # IMPORT SUPERVISORS FOR MAIL REPORTING PURPOSES
    sr_file = "X010eg_supervisor"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if l_reg and i_finding_before > 0 and i_finding_after > 0:
        if l_debug:
            print("Import reporting supervisors for mail purposes...")
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            SUPERVISOR.LOOKUP,
            Upper(SUPERVISOR.LOOKUP_CODE) AS CAMPUS,
            SUPERVISOR.LOOKUP_DESCRIPTION AS EMPLOYEE_NUMBER,
            PEOP.NAME_ADDR As NAME,
            PEOP.EMAIL_ADDRESS
        From
            VSS.X000_OWN_LOOKUPS SUPERVISOR Left Join
            PEOPLE.X002_PEOPLE_CURR PEOP ON PEOP.EMPLOYEE_NUMBER = SUPERVISOR.LOOKUP_DESCRIPTION
        Where
            SUPERVISOR.LOOKUP = 'stud_fee_test_reg_fee_abnormal_contact_supervisor'
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # ADD CONTACT DETAILS TO FINDINGS
    sr_file = "X010eh_detail"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if l_reg and i_finding_before > 0 and i_finding_after > 0:
        if l_debug:
            print("Add contact details to findings...")
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            PREV.ORG,
            PREV.LOC,
            PREV.STUDENT,
            PREV.FEE_CALC,
            STUD.DATEENROL,
            Upper(STUD.PRESENT_CAT) As PRESENT_CAT,
            Upper(STUD.ENROL_CAT) As ENROL_CAT,
            Upper(STUD.QUAL_TYPE) As QUAL_TYPE,
            CAMP_OFF.EMPLOYEE_NUMBER As CAMP_OFF_NUMB,
            CAMP_OFF.NAME As CAMP_OFF_NAME,
            CASE
                WHEN  CAMP_OFF.EMPLOYEE_NUMBER <> '' THEN CAMP_OFF.EMPLOYEE_NUMBER||'@nwu.ac.za'
                ELSE CAMP_OFF.EMAIL_ADDRESS
            END As CAMP_OFF_MAIL,
            CAMP_OFF.EMAIL_ADDRESS As CAMP_OFF_MAIL2,
            CAMP_SUP.EMPLOYEE_NUMBER As CAMP_SUP_NUMB,
            CAMP_SUP.NAME As CAMP_SUP_NAME,
            CASE
                WHEN CAMP_SUP.EMPLOYEE_NUMBER <> '' THEN CAMP_SUP.EMPLOYEE_NUMBER||'@nwu.ac.za'
                ELSE CAMP_SUP.EMAIL_ADDRESS
            END As CAMP_SUP_MAIL,
            CAMP_SUP.EMAIL_ADDRESS As CAMP_SUP_MAIL2,
            ORG_OFF.EMPLOYEE_NUMBER As ORG_OFF_NUMB,
            ORG_OFF.NAME As ORG_OFF_NAME,
            CASE
                WHEN ORG_OFF.EMPLOYEE_NUMBER <> '' THEN ORG_OFF.EMPLOYEE_NUMBER||'@nwu.ac.za'
                ELSE ORG_OFF.EMAIL_ADDRESS
            END As ORG_OFF_MAIL,
            ORG_OFF.EMAIL_ADDRESS As ORG_OFF_MAIL2,
            ORG_SUP.EMPLOYEE_NUMBER As ORG_SUP_NUMB,
            ORG_SUP.NAME As ORG_SUP_NAME,
            CASE
                WHEN ORG_SUP.EMPLOYEE_NUMBER <> '' THEN ORG_SUP.EMPLOYEE_NUMBER||'@nwu.ac.za'
                ELSE ORG_SUP.EMAIL_ADDRESS
            END As ORG_SUP_MAIL,
            ORG_SUP.EMAIL_ADDRESS As ORG_SUP_MAIL2,
            PREV.USER As USER_NUMB,
            CASE
                WHEN PREV.USER != '' THEN PEOP.NAME_ADDR
                WHEN CAMP_OFF.NAME != '' THEN CAMP_OFF.NAME 
                ELSE ''
            END As USER_NAME,
            CASE
                WHEN PREV.USER != '' THEN PREV.USER||'@nwu.ac.za'
                WHEN CAMP_OFF.EMPLOYEE_NUMBER != '' THEN CAMP_OFF.EMPLOYEE_NUMBER||'@nwu.ac.za'
                ELSE ''
            END As USER_MAIL,
            CASE
                WHEN PREV.USER != '' THEN PEOP.EMAIL_ADDRESS
                WHEN CAMP_OFF.EMPLOYEE_NUMBER != '' THEN CAMP_OFF.EMAIL_ADDRESS
                ELSE ''
            END As USER_MAIL2
        From
            X010ed_add_previous PREV Left Join
            X010ef_officer CAMP_OFF On CAMP_OFF.CAMPUS = PREV.LOC Left Join
            X010ef_officer ORG_OFF On ORG_OFF.CAMPUS = PREV.ORG Left Join
            X010eg_supervisor CAMP_SUP On CAMP_SUP.CAMPUS = PREV.LOC Left Join
            X010eg_supervisor ORG_SUP On ORG_SUP.CAMPUS = PREV.ORG Left Join
            X000_Student STUD On STUD.KSTUDBUSENTID = PREV.STUDENT Left Join
            PEOPLE.X002_PEOPLE_CURR PEOP ON PEOP.EMPLOYEE_NUMBER = PREV.USER
        Where
          PREV.PREV_PROCESS IS NULL
        ;"""
        """
        WHEN CAMP_OFF.NAME != '' THEN CAMP_OFF.NAME 
        """
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD THE FINAL TABLE FOR EXPORT AND REPORT
    sr_file = "X010ex_Regfee_abnormal"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if l_debug:
        print("Build the final report")
    if l_reg and i_finding_before > 0 and i_finding_after > 0:
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            'REGISTRATION FEE ABNORMAL' As Audit_finding,
            FIND.ORG As 'Organization',
            FIND.LOC As 'Campus',
            FIND.STUDENT As 'Student',
            FIND.FEE_CALC As 'Registration_fee',
            FIND.DATEENROL As 'Date_enrol',
            FIND.PRESENT_CAT As 'Present_cat',
            FIND.ENROL_CAT As 'Enrol_cat',
            FIND.QUAL_TYPE As 'Qualification_type',
            FIND.CAMP_OFF_NAME AS Responsible_Officer,
            FIND.CAMP_OFF_NUMB AS Responsible_Officer_Numb,
            FIND.CAMP_OFF_MAIL AS Responsible_Officer_Mail,
            FIND.CAMP_SUP_NAME AS Supervisor,
            FIND.CAMP_SUP_NUMB AS Supervisor_Numb,
            FIND.CAMP_SUP_MAIL AS Supervisor_Mail,
            FIND.ORG_OFF_NAME AS Org_Officer,
            FIND.ORG_OFF_NUMB AS Org_Officer_Numb,
            FIND.ORG_OFF_MAIL AS Org_Officer_Mail,
            FIND.ORG_SUP_NAME AS Org_Supervisor,
            FIND.ORG_SUP_NUMB AS Org_Supervisor_Numb,
            FIND.ORG_SUP_MAIL AS Org_Supervisor_Mail,
            FIND.USER_NAME As User,
            FIND.USER_NUMB As User_Numb,
            FIND.USER_MAIL As User_Mail
        From
            X010eh_detail FIND
        Order by
            FIND.LOC,
            FIND.PRESENT_CAT,
            FIND.ENROL_CAT,
            FIND.QUAL_TYPE,
            FIND.STUDENT
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)
        # Export findings
        if l_export and funcsys.tablerowcount(so_curs, sr_file) > 0:
            if l_debug:
                print("Export findings...")
            sx_path = re_path + "/"
            sx_file = "Student_fee_test_010ex_reg_fee_contact_abnormal_"
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

    """*****************************************************************************
    QUALIFICATION FEE MASTER 1
    *****************************************************************************"""
    if l_debug:
        print("QUALIFICATION FEE MASTER")
    funcfile.writelog("QUALIFICATION FEE MASTER")

    # IMPORT QUALIFICATION LEVY LIST
    sr_file = "X020aa_Fiabd007"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if l_debug:
        print("Import vss qualification fees...")
    so_curs.execute(
        "Create Table " + sr_file + """
        (ACAD_PROG_FEE_TYPE INT,
        ACAD_PROG_FEE_DESC TEXT,
        APOMOD INT,
        FPRESENTATIONCATEGORYCODEID INT,
        PRESENT_CAT TEXT,
        FENROLMENTCATEGORYCODEID INT,
        ENROL_CATEGORY TEXT,
        FSITEORGUNITNUMBER INT,
        CAMPUS TEXT,
        FQUALLEVELAPID INT,
        QUALIFICATION TEXT,
        QUALIFICATION_NAME TEXT,
        LEVY_CATEGORY TEXT,
        TRANSCODE TEXT,
        UMPT_REGU INT,
        AMOUNT REAL,
        START_DATE TEXT,
        END_DATE TEXT)
        """)
    co = open(ed_path + "302_fiapd007_qual_" + s_period + ".csv", "r", encoding="utf-8")
    co_reader = csv.reader(co)
    # Read the COLUMN database data
    for row in co_reader:
        # Populate the column variables
        # if l_debug:
        #   print(row[0])
        if "Academic Program Fee Type" in row[0]:
            continue
        elif row[0] == "":
            continue
        else:
            s_cols = "Insert Into " + sr_file + " Values(" \
                                                "" + row[0] + "," \
                                                "'" + row[1] + "'," \
                                                "" + row[2] + "," \
                                                "" + row[3] + "," \
                                                "'" + row[4] + "'," \
                                                "" + row[5] + "," \
                                                "'" + row[6] + "'," \
                                                "" + row[7] + "," \
                                                "'" + row[8] + "'," \
                                                "" + row[9] + "," \
                                                "'" + row[10] + "'," \
                                                "'" + row[11] + "'," \
                                                "'" + row[12] + "'," \
                                                "'" + row[13] + "'," \
                                                "" + row[14] + "," \
                                                "" + row[15] + "," \
                                                "'" + row[16] + "'," \
                                                "'" + row[17] + "')"

        # if l_debug:
            #   print(s_cols)
            so_curs.execute(s_cols)
    so_conn.commit()
    # Close the imported data file
    co.close()
    funcfile.writelog("%t IMPORT TABLE: " + ed_path + "302_fiapd007_qual_period.csv (" + sr_file + ")")

    # SUMM FIAB LEVY LIST
    if l_debug:
        print("Build summary of levy list...")
    sr_file = "X020aa_Fiabd007_summ"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        FIAB.FQUALLEVELAPID,
        Upper(FIAB.CAMPUS) As CAMPUS,
        FIAB.FPRESENTATIONCATEGORYCODEID,
        Upper(FIAB.PRESENT_CAT) As PRESENT_CAT,
        FIAB.FENROLMENTCATEGORYCODEID,
        Upper(FIAB.ENROL_CATEGORY) As ENROL_CATEGORY,
        FIAB.UMPT_REGU As FEEYEAR,
        Max(FIAB.AMOUNT) As AMOUNT,
        Count(FIAB.ACAD_PROG_FEE_TYPE) As COUNT,
        FIAB.QUALIFICATION,
        FIAB.QUALIFICATION_NAME,
        Max(FIAB.START_DATE) As START_DATE
    From
        X020aa_Fiabd007 FIAB
    Group By
        FIAB.FQUALLEVELAPID,
        FIAB.CAMPUS,
        FIAB.FPRESENTATIONCATEGORYCODEID,
        FIAB.FENROLMENTCATEGORYCODEID,
        FIAB.UMPT_REGU
    ;"""
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD LIST OF QUALIFICATIONS PLUS STATS
    if l_debug:
        print("Build summary of qualifications levied...")
    sr_file = "X020aa_Trans_feequal"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = "Create table " + sr_file + " AS" + f"""
    Select
        TRAN.FQUALLEVELAPID,
        TRAN.QUALIFICATION,
        TRAN.QUALIFICATION_NAME,
        CAST(COUNT(TRAN.STUDENT) As INT) As TRAN_COUNT_ALL,
        CAST(TOTAL(TRAN.AMOUNT) AS REAL) AS FEE_QUAL_ALL
    From
        X000_Transaction TRAN
    Where
        TRAN.TRANSCODE In ({qualification_transaction_code_str}) And
        TRAN.FMODAPID = 0
    Group by
        TRAN.FQUALLEVELAPID
    ;"""
    # s_sql = s_sql.replace("%TRANCODE%", s_qual_trancode)
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # CALCULATE THE QUALIFICATION FEES LEVIED PER STUDENT
    if l_debug:
        print("Calculate the qualification fees levied per student...")
    sr_file = "X020ab_Trans_feequal_stud"
    s_sql = "Create table " + sr_file + " AS" + f"""
    Select
        TRAN.STUDENT,
        CAST(COUNT(TRAN.STUDENT) As INT) As TRAN_COUNT,
        CAST(TOTAL(TRAN.AMOUNT) AS REAL) AS FEE_QUAL,
        MAX(TRAN.AUDITDATETIME),
        TRAN.FUSER AS FUSERBUSINESSENTITYID,
        TRAN.FUSERNAME AS NAME_ADDR,
        TRAN.FAUDITUSERCODE,
        TRAN.SYSTEM_DESC
    From
        X000_Transaction TRAN
    Where
        TRAN.TRANSCODE In ({qualification_transaction_code_str}) And
        TRAN.FMODAPID = 0
    Group by
        TRAN.STUDENT
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    # s_sql = s_sql.replace("%TRANCODE%", s_qual_trancode)
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # CALCULATE THE QUALIFICATION FEES LEVIED PER STUDENT
    if l_debug:
        print("Calculate the qualification fees levied per student...")
    sr_file = "X020ab_Trans_feequal_stud_level"
    s_sql = "Create table " + sr_file + " AS" + f"""
    Select
        TRAN.STUDENT,
        TRAN.FQUALLEVELAPID,
        CAST(COUNT(TRAN.STUDENT) As INT) As TRAN_COUNT,
        CAST(TOTAL(TRAN.AMOUNT) AS REAL) AS FEE_QUAL,
        MAX(TRAN.AUDITDATETIME),
        TRAN.FUSER AS FUSERBUSINESSENTITYID,
        TRAN.FUSERNAME AS NAME_ADDR,
        TRAN.FAUDITUSERCODE,
        TRAN.SYSTEM_DESC
    From
        X000_Transaction TRAN
    Where
        TRAN.TRANSCODE In ({qualification_transaction_code_str}) And
        TRAN.FMODAPID = 0
    Group by
        TRAN.STUDENT,
        TRAN.FQUALLEVELAPID
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = s_sql.replace("%TRANCODE%", s_qual_trancode)
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # CALCULATE THE STATISTIC MODE FOR EACH QUALIFICATION
    if l_debug:
        print("Calculate the qualification statistic mode...")
    i_value: int = 0
    sr_file = "X020ac_Trans_feequal_mode"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute("CREATE TABLE " + sr_file + " (FQUALLEVELAPID INT, AMOUNT REAL)")
    for qual in so_curs.execute("SELECT FQUALLEVELAPID FROM X020aa_Trans_feequal").fetchall():
        try:
            i_value = funcstat.stat_mode(so_curs, "X020ab_Trans_feequal_stud_level",
                                         "FEE_QUAL", "FQUALLEVELAPID = " + str(qual[0]))
            if i_value < 0:
                i_value = 0
        except Exception as e:
            # funcsys.ErrMessage(e) if you want error to log
            if "".join(e.args).find("no unique mode") >= 0:
                i_value = funcstat.stat_highest_value(so_curs, "X020ab_Trans_feequal_stud_level", "FEE_QUAL",
                                                      "FQUALLEVELAPID = " + str(qual[0]))
            else:
                i_value = 0
        s_cols = "INSERT INTO " + sr_file + " VALUES(" + str(qual[0]) + ", " + str(i_value) + ")"
        so_curs.execute(s_cols)
    funcfile.writelog("%t BUILD TABLE: " + sr_file)
    so_conn.commit()

    # COMBINE MODE AND LEVY LIST
    if l_debug:
        print("Combine mode and levy list...")
    sr_file = "X020ac_Trans_feequal_mode_fiab"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        MODE.FQUALLEVELAPID,
        MODE.AMOUNT As MODE_FEE,
        FIAB.AMOUNT As LIST_FEE,
        Cast(Case
            When FIAB.AMOUNT Is Null Then MODE.AMOUNT
            When FIAB.AMOUNT = 0 Then MODE.AMOUNT
            Else FIAB.AMOUNT
        End As REAL) As FEE_MODE,
        Cast(Case
            When MODE.AMOUNT = 0 Or FIAB.AMOUNT = 0 Then 2
            When MODE.AMOUNT Is Null Or FIAB.AMOUNT Is Null Then 2
            When MODE.AMOUNT > 0 And FIAB.AMOUNT > 0 And MODE.AMOUNT <> FIAB.AMOUNT Then 1
            Else 0
        End As INT) As DIFF,
        FIAB.QUALIFICATION,
        FIAB.QUALIFICATION_NAME
    From
        X020ac_Trans_feequal_mode MODE Left Join
        X020aa_Fiabd007_summ FIAB On FIAB.FQUALLEVELAPID = MODE.FQUALLEVELAPID
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # CALCULATE THE NUMBER OF MODULES
    if l_debug:
        print("Calculate the number modules...")
    sr_file = "X020ad_Student_module_calc"
    s_sql = "Create table " + sr_file + " AS" + """
    Select
        MODU.KSTUDBUSENTID,
        MODU.FQUALLEVELAPID,
        MODU.COURSESEMESTER,
        Cast(Case
            When MODU.COURSESEMESTER = '0' And Instr(MODU.COMPLETE_REASON,'RECOGN') > 0 Then 0
            When MODU.COURSESEMESTER = '0' Then 1        
            Else 0
        End As INT) As S0,
        Cast(Case
            When MODU.COURSESEMESTER = '1' And Instr(MODU.COMPLETE_REASON,'RECOGN') > 0 Then 0
            When MODU.COURSESEMESTER = '1' And Instr(MODU.COMPLETE_REASON,'DISCON') > 0 And MODU.DATE_RESU <= '%DSEM1%' Then 0
            When MODU.COURSESEMESTER = '1' Then 1        
            Else 0
        End As INT) As S1,
        Cast(Case
            When MODU.COURSESEMESTER = '2' And Instr(MODU.COMPLETE_REASON,'RECOGN') > 0 Then 0
            When MODU.COURSESEMESTER = '2' And Instr(MODU.COMPLETE_REASON,'DISCON') > 0 And MODU.DATE_RESU <= '%DSEM2%' Then 0
            When MODU.COURSESEMESTER = '2' Then 1        
            Else 0
        End As INT) As S2,
        Cast(Case
            When MODU.COURSESEMESTER = '3' And Instr(MODU.COMPLETE_REASON,'RECOGN') > 0 Then 0
            When MODU.COURSESEMESTER = '3' Then 1        
            Else 0
        End As INT) As S3,
        Cast(Case
            When MODU.COURSESEMESTER = '4' And Instr(MODU.COMPLETE_REASON,'RECOGN') > 0 Then 0
            When MODU.COURSESEMESTER = '4' Then 1        
            Else 0
        End As INT) As S4,
        Cast(Case
            When MODU.COURSESEMESTER = '5' And Instr(MODU.COMPLETE_REASON,'RECOGN') > 0 Then 0
            When MODU.COURSESEMESTER = '5' Then 1        
            Else 0
        End As INT) As S5,
        Cast(Case
            When MODU.COURSESEMESTER = '6' And Instr(MODU.COMPLETE_REASON,'RECOGN') > 0 Then 0
            When MODU.COURSESEMESTER = '6' Then 1        
            Else 0
        End As INT) As S6,
        Cast(Case
            When MODU.COURSESEMESTER = '7' And Instr(MODU.COMPLETE_REASON,'RECOGN') > 0 Then 0
            When MODU.COURSESEMESTER = '7' Then 1        
            Else 0
        End As INT) As S7,
        Cast(Case
            When MODU.COURSESEMESTER = '8' And Instr(MODU.COMPLETE_REASON,'RECOGN') > 0 Then 0
            When MODU.COURSESEMESTER = '8' Then 1        
            Else 0
        End As INT) As S8,
        Cast(Case
            When MODU.COURSESEMESTER = '9' And Instr(MODU.COMPLETE_REASON,'RECOGN') > 0 Then 0
            When MODU.COURSESEMESTER = '9' Then 1        
            Else 0
        End As INT) As S9
    From
        %VSS%.X001_Student_module MODU
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if s_period == "prev":
        s_sql = s_sql.replace("%VSS%", "VSSPREV")
    else:
        s_sql = s_sql.replace("%VSS%", "VSSCURR")
    s_sql = s_sql.replace("%DSEM1%", d_sem1_con)
    s_sql = s_sql.replace("%DSEM2%", d_sem2_con)
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD TABLE: " + sr_file)
    so_conn.commit()

    # CALCULATE THE NUMBER OF MODULES PER STUDENT
    if l_debug:
        print("Calculate the number modules per student...")
    sr_file = "X020ad_Student_module_summ"
    s_sql = "Create table " + sr_file + " AS" + """
    Select
        MODU.KSTUDBUSENTID,
        MODU.FQUALLEVELAPID,
        Cast(Sum(MODU.S0) As INT) As SEM0,
        Cast(Sum(MODU.S1) As INT) As SEM1,
        Cast(Sum(MODU.S2) As INT) As SEM2,
        Cast(Sum(MODU.S3) As INT) As SEM3,
        Cast(Sum(MODU.S4) As INT) As SEM4,
        Cast(Sum(MODU.S5) As INT) As SEM5,
        Cast(Sum(MODU.S6) As INT) As SEM6,
        Cast(Sum(MODU.S7) As INT) As SEM7,
        Cast(Sum(MODU.S8) As INT) As SEM8,
        Cast(Sum(MODU.S9) As INT) As SEM9
    From
        X020ad_Student_module_calc MODU
    Group By
        MODU.KSTUDBUSENTID,
        MODU.FQUALLEVELAPID
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD TABLE: " + sr_file)
    so_conn.commit()

    # IDENTIFY COURSE CONVERTERS
    # Student may appear multiple times
    if l_debug:
        print("Identify course converters...")
    sr_file = "X020ae_Student_convert"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select Distinct
        CONV.KSTUDBUSENTID,
        STUD.FQUALLEVELAPID,
        Case
            When CONV.FQUALLEVELAPID = STUD.FQUALLEVELAPID Then 'FROM'
            Else 'TO' 
        End As CONV_IND
    From
        X000_Student STUD Inner Join
        X000_Student CONV On STUD.KSTUDBUSENTID = CONV.KSTUDBUSENTID
    Where
        CONV.RESULT Like ('%CONVERT%')
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # IDENTIFY COURSE CONVERTERS
    # Only allow one conversion
    if l_debug:
        print("Identify course converters...")
    sr_file = "X020ae_Student_convert_master"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        STUD.KSTUDBUSENTID,
        STU2.FQUALLEVELAPID,
        STU2.CONV_IND,
        FEES.FEE_QUAL As FEE_QUAL_TOTAL
    From
        X020ae_Student_convert STUD Inner Join
        X020ae_Student_convert STU2 On STU2.KSTUDBUSENTID = STUD.KSTUDBUSENTID Left Join
        X020ab_Trans_feequal_stud FEES On FEES.STUDENT = STUD.KSTUDBUSENTID
    Group By
        STUD.KSTUDBUSENTID,
        STU2.FQUALLEVELAPID,
        STU2.CONV_IND
    Having
        Count(STUD.FQUALLEVELAPID) = 2
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # CALCULATE THE BURSARY FEES LEVIED PER STUDENT
    if l_debug:
        print("Calculate the bursary fees levied per student...")
    sr_file = "X020af_Trans_feeburs_stud"
    s_sql = "Create table " + sr_file + " AS" + """
    Select
        TRAN.STUDENT,
        CAST(COUNT(TRAN.STUDENT) As INT) As TRAN_COUNT,
        CAST(TOTAL(TRAN.AMOUNT) AS REAL) AS FEE_BURS,
        MAX(TRAN.AUDITDATETIME),
        TRAN.FUSER AS FUSERBUSINESSENTITYID,
        TRAN.FUSERNAME AS NAME_ADDR,
        TRAN.FAUDITUSERCODE,
        TRAN.SYSTEM_DESC
    From
        X000_Transaction TRAN
    Where
        Instr('%TRANCODE%', Trim(TRAN.TRANSCODE)) > 0
    Group by
        TRAN.STUDENT
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = s_sql.replace("%TRANCODE%", s_burs_trancode)
    # if l_debug:
    #   print(s_sql)
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    """*****************************************************************************
    QUALIFICATION FEE TEST NO FEE LOADED
    *****************************************************************************"""
    if l_debug:
        print("QUALIFICATION FEE TEST NO FEE LOADED")
    funcfile.writelog("QUALIFICATION FEE TEST NO FEE LOADED")

    # FILES NEEDED
    # X000_Student - Short course students already removed

    # EXCLUDE
    # MBA, MPA, AUD students
    # Occasional students
    # Conditional students
    # Non degree

    # DECLARE VARIABLES
    i_finding_after: int = 0
    s_desc = "No qualification fee loaded"

    # ISOLATE QUALIFICATIONS WITH NO LINKED LEVIES
    if l_debug:
        print("identify qualifications with no linked levies...")
    sr_file = "X021aa_Qual_nofee_loaded"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = "Create table " + sr_file + " AS" + """
    Select
        STUD.KSTUDBUSENTID,
        STUD.CAMPUS,
        STUD.FQUALLEVELAPID,
        STUD.FENROLMENTCATEGORYCODEID As ENROL_ID,
        STUD.ENROL_CAT,
        STUD.FPRESENTATIONCATEGORYCODEID As PRESENT_ID,
        STUD.PRESENT_CAT,
        FIAB.COUNT,
        FIAB.AMOUNT,
        STUD.QUALIFICATION,
        STUD.QUALIFICATION_NAME,
        Case
            When STUD.QUALIFICATION_NAME Like ('%NON DEGREE%') Then '0 NONDEGREE'
            When STUD.QUALIFICATION_NAME Like ('%OCCASIONAL STUD%') Then '0 OCCASIONAL STUDENT'
            When STUD.QUALIFICATION_NAME Like ('%OCCATIONAL STUD%') Then '0 OCCASIONAL STUDENT'
            When STUD.QUALIFICATION_NAME Like ('%OCC. STUD.%') Then '0 OCCASIONAL STUDENT'
            When Instr('%MBA%',STUD.FQUALLEVELAPID) > 0 Then '0 MASTER BUSINESS ADMINISTRATION'
            When Instr('%MPA%',STUD.FQUALLEVELAPID) > 0 Then '0 MASTER PUBLIC ADMINISTRATION'
            When Instr('%AUD%',STUD.FQUALLEVELAPID) > 0 Then '0 AUDHS / NURSE HEALTH SC'
            When STUD.ISCONDITIONALREG = 1 Then '0 CONDITIONAL REGISTRATION'
            Else '1'
        End As FINDING    
    From
        X000_Student STUD Left Join
        X020aa_Fiabd007_summ  FIAB On FIAB.FQUALLEVELAPID = STUD.FQUALLEVELAPID And
            FIAB.CAMPUS = STUD.CAMPUS And
            FIAB.FPRESENTATIONCATEGORYCODEID = STUD.FPRESENTATIONCATEGORYCODEID And
            FIAB.FENROLMENTCATEGORYCODEID = STUD.FENROLMENTCATEGORYCODEID
    ;"""
    s_sql = s_sql.replace("%MBA%", s_mba)
    s_sql = s_sql.replace("%MPA%", s_mpa)
    s_sql = s_sql.replace("%AUD%", s_aud)
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD TABLE: " + sr_file)
    so_conn.commit()

    # BUILD SUMMARY OF QUALIFICATIONS NOT LINKED
    if l_debug:
        print("Build summary of qualifications with no linked levies...")
    sr_file = "X021aa_Qual_nofee_loaded_summ"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = "Create table " + sr_file + " AS" + """
    Select
        LOAD.FQUALLEVELAPID,
        LOAD.CAMPUS,
        LOAD.ENROL_CAT,
        LOAD.PRESENT_CAT,
        LOAD.QUALIFICATION,
        LOAD.QUALIFICATION_NAME,
        Count(LOAD.KSTUDBUSENTID) As COUNT_STUD
    From
        X021aa_Qual_nofee_loaded LOAD
    Where
        LOAD.AMOUNT Is Null And
        LOAD.FINDING Like ('1%')
    Group By
        LOAD.ENROL_CAT,
        LOAD.PRESENT_CAT,
        LOAD.CAMPUS,
        LOAD.FQUALLEVELAPID
    ;"""
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD TABLE: " + sr_file)
    so_conn.commit()

    # BUILD STUDENTS LIST NOT LINKED TO LEVIES
    if l_debug:
        print("Build student list not linked to levies...")
    sr_file = "X021aa_Qual_nofee_loaded_stud"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = "Create table " + sr_file + " AS" + """
    Select
        LOAD.FINDING,
        STUD.*
    From
        X000_Student STUD Left Join
        X021aa_Qual_nofee_loaded LOAD On LOAD.KSTUDBUSENTID = STUD.KSTUDBUSENTID
    Where
        LOAD.AMOUNT Is Null Or
        LOAD.FINDING Like('0%')
    ;"""
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD TABLE: " + sr_file)
    so_conn.commit()
    if funcsys.tablerowcount(so_curs, sr_file) > 0:  # Ignore l_export flag - should export every time
        if l_debug:
            print("Export findings...")
        sx_path = re_path + "/"
        sx_file = "Student_fee_test_021ax_qual_fee_not_loaded_studentlist_"  # File X021_findings_list
        sx_file_dated = sx_file + funcdatn.get_today_date_file()
        s_head = funccsv.get_colnames_sqlite(so_conn, sr_file)
        funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file, s_head)
        # funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file_dated, s_head)
        funcfile.writelog("%t EXPORT DATA: " + sx_path + sx_file)

    # IDENTIFY FINDINGS
    if l_debug:
        print("Identify findings...")
    sr_file = "X021ab_findings"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        'NWU' As ORG,
        FIND.CAMPUS As LOC,
        FIND.FQUALLEVELAPID As ID,
        FIND.PRESENT_CAT,
        FIND.ENROL_CAT,
        FIND.QUALIFICATION,
        FIND.QUALIFICATION_NAME,
        FIND.COUNT_STUD
    From
        X021aa_Qual_nofee_loaded_summ FIND
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # COUNT THE NUMBER OF FINDINGS
    i_finding_before: int = funcsys.tablerowcount(so_curs, sr_file)
    if l_debug:
        print("*** Found " + str(i_finding_before) + " exceptions ***")
    funcfile.writelog("%t FINDING: " + str(i_finding_before) + " QUALIFICATION NO FEE LOADED finding(s)")

    # GET PREVIOUS FINDINGS
    if i_finding_before > 0:
        i = functest.get_previous_finding(so_curs, ed_path, "302_reported.txt", "qualification no fee loaded", "ITTTT")
        so_conn.commit()

    # SET PREVIOUS FINDINGS
    if i_finding_before > 0:
        i = functest.set_previous_finding(so_curs)
        so_conn.commit()

    # ADD PREVIOUS FINDINGS
    sr_file = "X021ad_add_previous"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0:
        if l_debug:
            print("Join previously reported to current findings...")
        s_sql = "CREATE TABLE " + sr_file + " AS" + """
        Select
            FIND.*,
            'qualification no fee loaded' AS PROCESS,
            '%TODAY%' AS DATE_REPORTED,
            '%DAYS%' AS DATE_RETEST,
            PREV.PROCESS AS PREV_PROCESS,
            PREV.DATE_REPORTED AS PREV_DATE_REPORTED,
            PREV.DATE_RETEST AS PREV_DATE_RETEST,
            PREV.REMARK
        From
            X021ab_findings FIND Left Join
            Z001ab_setprev PREV ON PREV.FIELD1 = FIND.ID And
                PREV.FIELD2 = FIND.LOC And
                PREV.FIELD3 = FIND.PRESENT_CAT And
                PREV.FIELD4 = FIND.ENROL_CAT And
                PREV.FIELD5 = FIND.QUALIFICATION
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        s_sql = s_sql.replace("%TODAY%", funcdatn.get_today_date())
        s_sql = s_sql.replace("%DAYS%", funcdatn.get_current_month_end_next())
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD LIST TO UPDATE FINDINGS
    sr_file = "X021ae_new_previous"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0:
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            PREV.PROCESS,
            PREV.ID AS FIELD1,
            PREV.LOC AS FIELD2,
            PREV.PRESENT_CAT AS FIELD3,
            PREV.ENROL_CAT AS FIELD4,
            PREV.QUALIFICATION AS FIELD5,
            PREV.DATE_REPORTED,
            PREV.DATE_RETEST,
            PREV.REMARK
        From
            X021ad_add_previous PREV
        Where
            PREV.PREV_PROCESS Is Null Or
            PREV.DATE_REPORTED > PREV.PREV_DATE_RETEST And PREV.REMARK = ''
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)
        # Export findings to previous reported file
        i_finding_after = funcsys.tablerowcount(so_curs, sr_file)
        if i_finding_after > 0:
            if l_debug:
                print("*** " + str(i_finding_after) + " Finding(s) to report ***")
            sx_path = ed_path
            sx_file = "302_reported"
            # Read the header data
            s_head = funccsv.get_colnames_sqlite(so_conn, sr_file)
            # Write the data
            if l_record:
                funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file, s_head, "a", ".txt")
                funcfile.writelog("%t FINDING: " + str(i_finding_after) + " new finding(s) to export")
                funcfile.writelog("%t EXPORT DATA: " + sr_file)
            if l_mess:
                funcsms.send_telegram('', 'administrator',
                                      '<b>' + str(i_finding_before) + '/' + str(i_finding_after) + '</b> ' + s_desc)
        else:
            if l_debug:
                print("*** No new findings to report ***")
            funcfile.writelog("%t FINDING: No new findings to export")

    # IMPORT OFFICERS FOR MAIL REPORTING PURPOSES
    sr_file = "X021af_officer"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0 and i_finding_after > 0:
        i = functest.get_officer(so_curs, "VSS", "stud_fee_test_qual_no_fee_loaded_officer")
        so_conn.commit()

    # IMPORT SUPERVISORS FOR MAIL REPORTING PURPOSES
    sr_file = "X021ag_supervisor"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0 and i_finding_after > 0:
        i = functest.get_supervisor(so_curs, "VSS", "stud_fee_test_qual_no_fee_loaded_supervisor")
        so_conn.commit()

    # ADD CONTACT DETAILS TO FINDINGS
    sr_file = "X021ah_detail"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0 and i_finding_after > 0:
        if l_debug:
            print("Add contact details to findings...")
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            PREV.ORG,
            PREV.LOC,
            PREV.ID,
            PREV.PRESENT_CAT,
            PREV.ENROL_CAT,
            PREV.QUALIFICATION,
            PREV.QUALIFICATION_NAME,
            PREV.COUNT_STUD,
            CAMP_OFF.EMPLOYEE_NUMBER As CAMP_OFF_NUMB,
            CAMP_OFF.NAME_ADDR As CAMP_OFF_NAME,
            CASE
                WHEN  CAMP_OFF.EMPLOYEE_NUMBER <> '' THEN CAMP_OFF.EMPLOYEE_NUMBER||'@nwu.ac.za'
                ELSE CAMP_OFF.EMAIL_ADDRESS
            END As CAMP_OFF_MAIL,
            CAMP_OFF.EMAIL_ADDRESS As CAMP_OFF_MAIL2,
            CAMP_SUP.EMPLOYEE_NUMBER As CAMP_SUP_NUMB,
            CAMP_SUP.NAME_ADDR As CAMP_SUP_NAME,
            CASE
                WHEN CAMP_SUP.EMPLOYEE_NUMBER <> '' THEN CAMP_SUP.EMPLOYEE_NUMBER||'@nwu.ac.za'
                ELSE CAMP_SUP.EMAIL_ADDRESS
            END As CAMP_SUP_MAIL,
            CAMP_SUP.EMAIL_ADDRESS As CAMP_SUP_MAIL2,
            ORG_OFF.EMPLOYEE_NUMBER As ORG_OFF_NUMB,
            ORG_OFF.NAME_ADDR As ORG_OFF_NAME,
            CASE
                WHEN ORG_OFF.EMPLOYEE_NUMBER <> '' THEN ORG_OFF.EMPLOYEE_NUMBER||'@nwu.ac.za'
                ELSE ORG_OFF.EMAIL_ADDRESS
            END As ORG_OFF_MAIL,
            ORG_OFF.EMAIL_ADDRESS As ORG_OFF_MAIL2,
            ORG_SUP.EMPLOYEE_NUMBER As ORG_SUP_NUMB,
            ORG_SUP.NAME_ADDR As ORG_SUP_NAME,
            CASE
                WHEN ORG_SUP.EMPLOYEE_NUMBER <> '' THEN ORG_SUP.EMPLOYEE_NUMBER||'@nwu.ac.za'
                ELSE ORG_SUP.EMAIL_ADDRESS
            END As ORG_SUP_MAIL,
            ORG_SUP.EMAIL_ADDRESS As ORG_SUP_MAIL2
        From
            X021ad_add_previous PREV Left Join
            Z001af_officer CAMP_OFF On CAMP_OFF.CAMPUS = PREV.LOC Left Join
            Z001af_officer ORG_OFF On ORG_OFF.CAMPUS = PREV.ORG Left Join
            Z001ag_supervisor CAMP_SUP On CAMP_SUP.CAMPUS = PREV.LOC Left Join
            Z001ag_supervisor ORG_SUP On ORG_SUP.CAMPUS = PREV.ORG
        Where
            PREV.PREV_PROCESS Is Null Or
            PREV.DATE_REPORTED > PREV.PREV_DATE_RETEST And PREV.REMARK = ''
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD THE FINAL TABLE FOR EXPORT AND REPORT
    sr_file = "X021ax_Qual_nofee_loaded"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if l_debug:
        print("Build the final report")
    if i_finding_before > 0 and i_finding_after > 0:
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            'QUALIFICATION NO FEE LOADED' As Audit_finding,
            FIND.ORG As 'Organization',
            FIND.LOC As 'Campus',
            FIND.ID As 'Quallevelid',
            FIND.PRESENT_CAT As 'Pres_category',
            FIND.ENROL_CAT As 'Enrol_category',
            FIND.QUALIFICATION As 'Qualification',
            FIND.QUALIFICATION_NAME As 'Qualification_name',
            FIND.COUNT_STUD As 'Student_count',
            FIND.CAMP_OFF_NAME AS Responsible_Officer,
            FIND.CAMP_OFF_NUMB AS Responsible_Officer_Numb,
            FIND.CAMP_OFF_MAIL AS Responsible_Officer_Mail,
            FIND.CAMP_SUP_NAME AS Supervisor,
            FIND.CAMP_SUP_NUMB AS Supervisor_Numb,
            FIND.CAMP_SUP_MAIL AS Supervisor_Mail,
            FIND.ORG_OFF_NAME AS Org_Officer,
            FIND.ORG_OFF_NUMB AS Org_Officer_Numb,
            FIND.ORG_OFF_MAIL AS Org_Officer_Mail,
            FIND.ORG_SUP_NAME AS Org_Supervisor,
            FIND.ORG_SUP_NUMB AS Org_Supervisor_Numb,
            FIND.ORG_SUP_MAIL AS Org_Supervisor_Mail
        From
            X021ah_detail FIND
        Order by
            FIND.LOC,
            FIND.QUALIFICATION
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)
        # Export findings
        if l_export and funcsys.tablerowcount(so_curs, sr_file) > 0:
            if l_debug:
                print("Export findings...")
            sx_path = re_path + "/"
            sx_file = "Student_fee_test_021ax_qual_fee_not_loaded_"
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

    """*****************************************************************************
    QUALIFICATION FEE MASTER 2 - JOIN QUALIFICATIONS PRESENTED AND FIAB LIST
    *****************************************************************************"""
    if l_debug:
        print("QUALIFICATION FEE MASTER 2")
    funcfile.writelog("QUALIFICATION FEE MASTER 2")

    # NOTE - This code is place here because it rely on
    #   table X021aa_Qual_nofee_loaded prepared in the
    #   QUALIFICATION FEE TEST NO FEE LOADED

    # BUILD SUMMARY OF QUALIFICATIONS PRESENTED
    if l_debug:
        print("Build summary of qualifications with no linked levies...")
    sr_file = "X022aa_Qual_present_summary"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = "Create table " + sr_file + " AS" + """
    Select
        LOAD.FQUALLEVELAPID,
        LOAD.CAMPUS,
        LOAD.ENROL_ID,
        LOAD.ENROL_CAT,
        LOAD.PRESENT_ID,
        LOAD.PRESENT_CAT,
        LOAD.QUALIFICATION,
        LOAD.QUALIFICATION_NAME,
        Count(LOAD.KSTUDBUSENTID) As COUNT_STUD
    From
        X021aa_Qual_nofee_loaded LOAD
    Where
        LOAD.FINDING Like ('1%')
    Group By
        LOAD.FQUALLEVELAPID,
        LOAD.CAMPUS,
        LOAD.ENROL_ID,
        LOAD.PRESENT_ID
    ;"""
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD TABLE: " + sr_file)
    so_conn.commit()

    # IDENTIFY FIAB ENTRIES WHICG IS PRESENTED
    if l_debug:
        print("Identify FIAB entries which is presented...")
    sr_file = "X022ab_Qual_present_fiablist"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = "Create table " + sr_file + " AS" + """
    Select
        FIAB.FQUALLEVELAPID,
        FIAB.CAMPUS,
        FIAB.FPRESENTATIONCATEGORYCODEID,
        FIAB.FENROLMENTCATEGORYCODEID,
        FIAB.AMOUNT
    From
        X020aa_Fiabd007_summ FIAB Inner Join
        X022aa_Qual_present_summary PRES On PRES.FQUALLEVELAPID = FIAB.FQUALLEVELAPID
                And PRES.CAMPUS = FIAB.CAMPUS
                And PRES.PRESENT_ID = FIAB.FPRESENTATIONCATEGORYCODEID
                And PRES.ENROL_ID = FIAB.FENROLMENTCATEGORYCODEID
    Group By
        FIAB.FQUALLEVELAPID,
        FIAB.FPRESENTATIONCATEGORYCODEID,
        FIAB.FENROLMENTCATEGORYCODEID,
        FIAB.AMOUNT
    ;"""
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD TABLE: " + sr_file)
    so_conn.commit()

    # IDENTIFY FIAB ENTRIES WITH MORE THAN ONE CLASS FEE
    if l_debug:
        print("Identify FIAB entries with more than one class fee...")
    sr_file = "X022ac_Qual_present_count"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = "Create table " + sr_file + " AS" + """
    Select
        SUMM.FQUALLEVELAPID,
        SUMM.FPRESENTATIONCATEGORYCODEID,
        SUMM.FENROLMENTCATEGORYCODEID,
        Count(SUMM.AMOUNT) As COUNT_OCC
    From
        X022ab_Qual_present_fiablist SUMM
    Group By
        SUMM.FQUALLEVELAPID,
        SUMM.FPRESENTATIONCATEGORYCODEID,
        SUMM.FENROLMENTCATEGORYCODEID
    Having
        COUNT_OCC > 1    
    ;"""
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD TABLE: " + sr_file)
    so_conn.commit()

    # IDENTIFY FIAB ENTRIES WITH DIFFERENT AMOUNTS
    if l_debug:
        print("Identify FIAB entries with different amounts...")
    sr_file = "X022ad_Qual_present_merge"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = "Create table " + sr_file + " AS" + """
    Select
        FIAB.FQUALLEVELAPID,
        FIAB.CAMPUS,
        PRES.PRESENT_ID,
        PRES.PRESENT_CAT,
        PRES.ENROL_ID,
        PRES.ENROL_CAT,
        PRES.QUALIFICATION,
        PRES.QUALIFICATION_NAME,
        PRES.COUNT_STUD,
        FIAB.AMOUNT
    From
        X022ab_Qual_present_fiablist FIAB Inner Join
        X022aa_Qual_present_summary PRES On PRES.FQUALLEVELAPID = FIAB.FQUALLEVELAPID
                And PRES.CAMPUS = FIAB.CAMPUS
                And PRES.PRESENT_ID = FIAB.FPRESENTATIONCATEGORYCODEID
                And PRES.ENROL_ID = FIAB.FENROLMENTCATEGORYCODEID Inner Join
        X022ac_Qual_present_count COUN On COUN.FQUALLEVELAPID = FIAB.FQUALLEVELAPID
                And COUN.FPRESENTATIONCATEGORYCODEID = FIAB.FPRESENTATIONCATEGORYCODEID
                And COUN.FENROLMENTCATEGORYCODEID = FIAB.FENROLMENTCATEGORYCODEID
    ;"""
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD TABLE: " + sr_file)
    so_conn.commit()

    # IDENTIFY FIAB ENTRIES WITH DIFFERENT AMOUNTS
    if l_debug:
        print("Identify FIAB entries with different amounts...")
    sr_file = "X022ae_Qual_present_final"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = "Create table " + sr_file + " AS" + """
    Select
        MERG.*,
        MERG.FQUALLEVELAPID As FQUALLEVELAPID1,
        MERG.CAMPUS As CAMPUS1,
        Count(MERG.PRESENT_ID) As COUNT_OCC
    From
        X022ad_Qual_present_merge MERG
    Group By
        MERG.FQUALLEVELAPID,
        MERG.CAMPUS
    Order by
        QUALIFICATION,
        CAMPUS    
    ;"""
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD TABLE: " + sr_file)
    so_conn.commit()

    """*****************************************************************************
    QUALIFICATION FEE TEST FEE LOADED INCORRECTLY
    *****************************************************************************"""
    if l_debug:
        print("QUALIFICATION FEE TEST FEE LOADED INCORRECTLY")
    funcfile.writelog("QUALIFICATION FEE TEST FEE LOADED INCORRECTLY")

    # FILES NEEDED
    # X022ae_Qual_present_final

    # DECLARE TEST VARIABLES
    s_fprefix: str = "X022b"
    s_finding: str = "QUALIFICATION FEE LOADED INCORRECTLY"
    s_xfile: str = "302_reported.txt"
    i_finding_before: int = 0
    i_finding_after: int = 0
    s_desc = "Qualification fee incorrectly loaded and differ between campuses"

    # OBTAIN TEST DATA
    if l_debug:
        print("Obtain test data...")
    sr_file: str = s_fprefix + "a_fee_loaded_incorrectly"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        TEST.FQUALLEVELAPID,
        TEST.CAMPUS,
        TEST.PRESENT_ID,
        TEST.PRESENT_CAT,
        TEST.ENROL_ID,
        TEST.ENROL_CAT,
        TEST.QUALIFICATION,
        TEST.QUALIFICATION_NAME,
        TEST.COUNT_STUD,
        TEST.AMOUNT,
        TEST.COUNT_OCC
    From
        X022ae_Qual_present_final TEST
    ;"""
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # SELECT TEST DATA
    if l_debug:
        print("Identify findings...")
    sr_file = s_fprefix + "b_finding"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        'NWU' As ORG,
        FIND.CAMPUS As LOC,    
        FIND.QUALIFICATION,
        FIND.QUALIFICATION_NAME,
        FIND.FQUALLEVELAPID,
        FIND.PRESENT_ID,
        FIND.PRESENT_CAT,
        FIND.ENROL_ID,
        FIND.ENROL_CAT,
        FIND.COUNT_STUD,
        FIND.AMOUNT
    From
        %FILEP%a_fee_loaded_incorrectly FIND
    Where
        FIND.COUNT_OCC = 1 And
        FIND.AMOUNT = 0.00
    ;"""
    s_sql = s_sql.replace("%FILEP%", s_fprefix)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # COUNT THE NUMBER OF FINDINGS
    i_finding_before: int = funcsys.tablerowcount(so_curs, sr_file)
    if l_debug:
        print("*** Found " + str(i_finding_before) + " exceptions ***")
    funcfile.writelog("%t FINDING: " + str(i_finding_before) + " " + s_finding + " finding(s)")

    # GET PREVIOUS FINDINGS
    if i_finding_before > 0:
        i = functest.get_previous_finding(so_curs, ed_path, s_xfile, s_finding, "ITIIT")
        so_conn.commit()

    # SET PREVIOUS FINDINGS
    if i_finding_before > 0:
        i = functest.set_previous_finding(so_curs)
        so_conn.commit()

    # ADD PREVIOUS FINDINGS
    sr_file = s_fprefix + "d_addprev"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0:
        if l_debug:
            print("Join previously reported to current findings...")
        s_sql = "CREATE TABLE " + sr_file + " AS" + """
        Select
            FIND.*,
            Lower('%FINDING%') AS PROCESS,
            '%TODAY%' AS DATE_REPORTED,
            '%DAYS%' AS DATE_RETEST,
            PREV.PROCESS AS PREV_PROCESS,
            PREV.DATE_REPORTED AS PREV_DATE_REPORTED,
            PREV.DATE_RETEST AS PREV_DATE_RETEST,
            PREV.REMARK
        From
            %FILEP%b_finding FIND Left Join
            Z001ab_setprev PREV ON PREV.FIELD1 = FIND.FQUALLEVELAPID And
                PREV.FIELD2 = FIND.LOC And
                PREV.FIELD3 = FIND.PRESENT_ID And
                PREV.FIELD4 = FIND.ENROL_ID And
                PREV.FIELD5 = FIND.QUALIFICATION
        ;"""
        s_sql = s_sql.replace("%FINDING%", s_finding)
        s_sql = s_sql.replace("%FILEP%", s_fprefix)
        s_sql = s_sql.replace("%TODAY%", funcdatn.get_today_date())
        s_sql = s_sql.replace("%DAYS%", funcdatn.get_current_month_end_next())
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD LIST TO UPDATE FINDINGS
    sr_file = s_fprefix + "e_newprev"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0:
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            PREV.PROCESS,
            PREV.FQUALLEVELAPID AS FIELD1,
            PREV.LOC AS FIELD2,
            PREV.PRESENT_ID AS FIELD3,
            PREV.ENROL_ID AS FIELD4,
            PREV.QUALIFICATION AS FIELD5,
            PREV.DATE_REPORTED,
            PREV.DATE_RETEST,
            PREV.REMARK
        From
            %FILEP%d_addprev PREV
        Where
            PREV.PREV_PROCESS Is Null Or
            PREV.DATE_REPORTED > PREV.PREV_DATE_RETEST And PREV.REMARK = ""        
        ;"""
        s_sql = s_sql.replace("%FILEP%", s_fprefix)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)
        # Export findings to previous reported file
        i_finding_after = funcsys.tablerowcount(so_curs, sr_file)
        if i_finding_after > 0:
            if l_debug:
                print("*** " + str(i_finding_after) + " Finding(s) to report ***")
            sx_path = ed_path
            sx_file = s_xfile[:-4]
            # Read the header data
            s_head = funccsv.get_colnames_sqlite(so_conn, sr_file)
            # Write the data
            if l_record:
                funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file, s_head, "a", ".txt")
                funcfile.writelog("%t FINDING: " + str(i_finding_after) + " new finding(s) to export")
                funcfile.writelog("%t EXPORT DATA: " + sr_file)
            if l_mess:
                funcsms.send_telegram('', 'administrator',
                                      '<b>' + str(i_finding_before) + '/' + str(i_finding_after) + '</b> ' + s_desc)
        else:
            if l_debug:
                print("*** No new findings to report ***")
            funcfile.writelog("%t FINDING: No new findings to export")

    # IMPORT OFFICERS FOR MAIL REPORTING PURPOSES
    if i_finding_before > 0 and i_finding_after > 0:
        i = functest.get_officer(so_curs, "VSS", "TEST " + s_finding + " OFFICER")
        so_conn.commit()

    # IMPORT SUPERVISORS FOR MAIL REPORTING PURPOSES
    if i_finding_before > 0 and i_finding_after > 0:
        i = functest.get_supervisor(so_curs, "VSS", "TEST " + s_finding + " SUPERVISOR")
        so_conn.commit()

    # ADD CONTACT DETAILS TO FINDINGS
    sr_file = s_fprefix + "h_detail"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0 and i_finding_after > 0:
        if l_debug:
            print("Add contact details to findings...")
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            PREV.ORG,
            PREV.LOC,
            PREV.QUALIFICATION,
            PREV.QUALIFICATION_NAME,
            PREV.FQUALLEVELAPID,
            PREV.PRESENT_ID,
            PREV.PRESENT_CAT,
            PREV.ENROL_ID,
            PREV.ENROL_CAT,
            PREV.COUNT_STUD,
            PREV.AMOUNT,    
            CAMP_OFF.EMPLOYEE_NUMBER As CAMP_OFF_NUMB,
            CAMP_OFF.NAME_ADDR As CAMP_OFF_NAME,
            CAMP_OFF.EMAIL_ADDRESS As CAMP_OFF_MAIL,
            CAMP_SUP.EMPLOYEE_NUMBER As CAMP_SUP_NUMB,
            CAMP_SUP.NAME_ADDR As CAMP_SUP_NAME,
            CAMP_SUP.EMAIL_ADDRESS As CAMP_SUP_MAIL,
            ORG_OFF.EMPLOYEE_NUMBER As ORG_OFF_NUMB,
            ORG_OFF.NAME_ADDR As ORG_OFF_NAME,
            ORG_OFF.EMAIL_ADDRESS As ORG_OFF_MAIL,
            ORG_SUP.EMPLOYEE_NUMBER As ORG_SUP_NUMB,
            ORG_SUP.NAME_ADDR As ORG_SUP_NAME,
            ORG_SUP.EMAIL_ADDRESS As ORG_SUP_MAIL
        From
            %FILEP%d_addprev PREV
            Left Join Z001af_officer CAMP_OFF On CAMP_OFF.CAMPUS = PREV.LOC
            Left Join Z001af_officer ORG_OFF On ORG_OFF.CAMPUS = PREV.ORG
            Left Join Z001ag_supervisor CAMP_SUP On CAMP_SUP.CAMPUS = PREV.LOC
            Left Join Z001ag_supervisor ORG_SUP On ORG_SUP.CAMPUS = PREV.ORG
        Where
            PREV.PREV_PROCESS Is Null Or
            PREV.DATE_REPORTED > PREV.PREV_DATE_RETEST And PREV.REMARK = ""
        ;"""
        s_sql = s_sql.replace("%FILEP%", s_fprefix)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD THE FINAL TABLE FOR EXPORT AND REPORT
    sr_file = s_fprefix + "x_fee_loaded_incorrectly"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if l_debug:
        print("Build the final report")
    if i_finding_before > 0 and i_finding_after > 0:
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            '%FIND%' As Audit_finding,
            FIND.ORG As Organization,
            FIND.LOC As Campus,
            FIND.FQUALLEVELAPID As Qualification_id,
            FIND.QUALIFICATION As Qualification,
            FIND.QUALIFICATION_NAME As Qualification_name,
            FIND.PRESENT_CAT As Present_category,
            FIND.ENROL_CAT As Enrol_category,
            FIND.COUNT_STUD As Student_count,
            FIND.AMOUNT As Fee_amount,    
            FIND.CAMP_OFF_NAME AS Responsible_Officer,
            FIND.CAMP_OFF_NUMB AS Responsible_Officer_Numb,
            FIND.CAMP_OFF_MAIL AS Responsible_Officer_Mail,
            FIND.CAMP_SUP_NAME AS Supervisor,
            FIND.CAMP_SUP_NUMB AS Supervisor_Numb,
            FIND.CAMP_SUP_MAIL AS Supervisor_Mail,
            FIND.ORG_OFF_NAME AS Org_Officer,
            FIND.ORG_OFF_NUMB AS Org_Officer_Numb,
            FIND.ORG_OFF_MAIL AS Org_Officer_Mail,
            FIND.ORG_SUP_NAME AS Org_Supervisor,
            FIND.ORG_SUP_NUMB AS Org_Supervisor_Numb,
            FIND.ORG_SUP_MAIL AS Org_Supervisor_Mail            
        From
            %FILEP%h_detail FIND
        ;"""
        s_sql = s_sql.replace("%FIND%", s_finding)
        s_sql = s_sql.replace("%FILEP%", s_fprefix)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)
        # Export findings
        if l_export and funcsys.tablerowcount(so_curs, sr_file) > 0:
            if l_debug:
                print("Export findings...")
            sx_path = re_path + "/"
            sx_file = "Qualification_test_" + s_fprefix + "_" + s_finding.lower() + "_"
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

    """*****************************************************************************
    QUALIFICATION FEE MASTER 3 - JOIN STUDENT AND TRANSACTION AND NO FEE LOADED TEST
    *****************************************************************************"""
    if l_debug:
        print("QUALIFICATION FEE MASTER 3")
    funcfile.writelog("QUALIFICATION FEE MASTER 3")

    # Short course students already removed in OBTAIN STUDENTS
    # Remove NON CONTACT students
    # Remove students identified in QUALIFICATION FEE TEST NO FEE LOADED

    # JOIN STUDENTS AND TRANSACTIONS
    if l_debug:
        print("Join students and transactions...")
    sr_file = "X020ba_Student_master"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        Cast(0 As INT) As VALID,
        STUD.CAMPUS,
        STUD.KSTUDBUSENTID,
        Cast(Case
            When FEES.FEE_QUAL Is Null Then 0
            Else Round(FEES.FEE_QUAL,2)
        End As REAL) As FEE_LEVIED,
        Cast(Case
            When MODE.AMOUNT Is Null Then 0
            Else Round(MODE.AMOUNT,2)
        End As REAL) As FEE_MODE,
        Cast(Case
            When MODE.AMOUNT Is Null Then 0
            Else Round(MODE.AMOUNT/2,2)
        End As REAL) As FEE_MODE_HALF,
        CONV.CONV_IND,
        CONV.FEE_QUAL_TOTAL,
        '' As FEE_SHOULD_BE,
        Case
            When STUD.ENROL_CAT = "POST DOC" Then '9 EXCLUDE POST DOC'
            When STUD.ISCONDITIONALREG = '1' Then  '9 EXCLUDE CONDITIONAL REG'
            When Instr('%MBA%',STUD.FQUALLEVELAPID) != 0 Then '9 EXCLUDE MBA'
            When Instr('%MPA%',STUD.FQUALLEVELAPID) != 0 Then '9 EXCLUDE MPA'
            When STUD.QUALIFICATION_NAME Like ('%NON DEGREE%') Then '9 EXCLUDE NON DEGREE'
            When STUD.QUALIFICATION_NAME Like ('%OCCASIONAL STUD%') Then '9 OCCASIONAL STUDENT'
            When STUD.QUALIFICATION_NAME Like ('%OCCATIONAL STUD%') Then '9 OCCASIONAL STUDENT'
            When STUD.QUALIFICATION_NAME Like ('%MTH IN%') Then '9 MTH STUDENT'
            When FEES.FEE_QUAL Is Null And MODE.AMOUNT Is Null Then '1 NO TRANS/NO FEE'
            When FEES.FEE_QUAL Is Null And MODE.AMOUNT = 0 Then '1 NO TRANS/ZERO FEE'
            When FEES.FEE_QUAL Is Null Then '1 NO TRANS/WITH FEE'
            When FEES.FEE_QUAL < 0 Then '2 NEGATIVE TRANSACTION'
            When FEES.FEE_QUAL = 0 Then '3 ZERO TRANSACTION'
            When FEES.FEE_QUAL = Round(MODE.AMOUNT/2,2) Then '4 HALF TRANSACTION'
            When FEES.FEE_QUAL = MODE.AMOUNT Then '5 NORMAL TRANSACTION'
            Else '6 ABNORMAL TRANSACTION'
        End As FEE_LEVIED_TYPE,
        STUD.QUALIFICATION,
        STUD.QUALIFICATION_NAME,
        STUD.QUAL_TYPE,
        Case
            When STUD.QUAL_TYPE Like ('%DOCTORAL%') Then 'POSTGRADUATE'
            When STUD.QUAL_TYPE Like ('%MASTERS%') Then 'POSTGRADUATE'
            Else 'UNDERGRADUATE'
        End As QUAL_TYPE_FEE,
        STUD.ACTIVE_IND,
        STUD.LEVY_CATEGORY,
        STUD.PRESENT_CAT,
        STUD.ENROL_CAT,
        STUD.ENTRY_LEVEL,
        STUD.STATUS_FINAL,
        STUD.FSITEORGUNITNUMBER As FSITE,
        STUD.ORGUNIT_NAME,
        STUD.DATEQUALLEVELSTARTED,
        STUD.STARTDATE,
        STUD.DATEENROL,
        STUD.DISCONTINUEDATE,
        Cast(Case
            When STUD.DISCONTINUEDATE > STUD.DATEENROL Then Julianday(STUD.DISCONTINUEDATE) - Julianday(STUD.DATEENROL) 
            Else 0
        End As INT) As DAYS_REG,
        Case
            When Upper(STUD.RESULT) Like 'PASS C%' Then ''
            When Upper(STUD.RESULT) Like 'PASS D%' Then ''
            Else STUD.DISCONTINUEDATE 
        End As DISCDATE_CALC,
        Case
            When Upper(STUD.RESULT) Like 'PASS C%' Then STUD.RESULTPASSDATE
            When Upper(STUD.RESULT) Like 'PASS D%' Then STUD.RESULTPASSDATE
            Else '' 
        End As RESULTPASSDATE,
        Cast(Case
            When Upper(STUD.RESULT) Like 'PASS C%' And STUD.RESULTPASSDATE > STUD.DATEENROL Then Julianday(STUD.RESULTPASSDATE) - Julianday(STUD.DATEENROL)
            When Upper(STUD.RESULT) Like 'PASS D%' And STUD.RESULTPASSDATE > STUD.DATEENROL Then Julianday(STUD.RESULTPASSDATE) - Julianday(STUD.DATEENROL) 
            Else 0
        End As INT) As DAYS_PASS,
        Case
            When Upper(STUD.RESULT) Like 'PASS C%' Then STUD.RESULTISSUEDATE
            When Upper(STUD.RESULT) Like 'PASS D%' Then STUD.RESULTISSUEDATE
            Else '' 
        End As RESULTISSUEDATE,
        STUD.RESULT,
        STUD.CEREMONYDATETIME,
        STUD.CEREMONY,
        STUD.ISHEMISSUBSIDY,
        STUD.ISMAINQUALLEVEL,
        STUD.ENROLACADEMICYEAR,
        STUD.ENROLHISTORYYEAR,
        STUD.CALCHISTORYYEAR,
        STUD.FEEHISTORYYEAR,
        Cast(Case
            When SEM1 Is Null Then 0
            Else 1
        End As INT) As SEM0,
        Cast(SEME.SEM1 As INT) As SEM1,
        Cast(SEME.SEM2 As INT) As SEM2,
        Cast(SEME.SEM7 As INT) As SEM7,
        Cast(SEME.SEM8 As INT) As SEM8,
        Cast(SEME.SEM8 As INT) As SEM9,
        REGF.FEE_TYPE As REG_FEE_TYPE,
        BURS.FEE_BURS,
        STUD.MIN,
        STUD.MIN_UNIT,
        STUD.MAX,
        STUD.MAX_UNIT,
        STUD.CERT_TYPE,
        STUD.LEVY_TYPE,
        STUD.BLACKLIST,
        STUD.LONG,
        STUD.DISCONTINUE_REAS,
        STUD.POSTPONE_REAS,
        STUD.FBUSINESSENTITYID,
        STUD.ORGUNIT_TYPE,
        STUD.ISCONDITIONALREG,
        STUD.MARKSFINALISEDDATE,
        STUD.EXAMSUBMINIMUM,
        STUD.ISCUMLAUDE,
        STUD.ISPOSSIBLEGRADUATE,
        STUD.FACCEPTANCETESTCODEID,
        STUD.FENROLMENTPRESENTATIONID,
        STUD.FQUALLEVELAPID,
        STUD.FPROGRAMAPID,
        FEES.TRAN_COUNT,    
        FEES.FUSERBUSINESSENTITYID,
        FEES.NAME_ADDR,
        FEES.FAUDITUSERCODE,
        FEES.SYSTEM_DESC,
        Cast(1 As INT) As PERIOD_A_IND
    From
        X000_Student STUD Left Join
        X021aa_Qual_nofee_loaded_stud FIND On FIND.KSTUDBUSENTID = STUD.KSTUDBUSENTID And
            FIND.QUALIFICATION = STUD.QUALIFICATION Left Join
        X020ab_Trans_feequal_stud_level FEES On FEES.STUDENT = STUD.KSTUDBUSENTID And
            FEES.FQUALLEVELAPID = STUD.FQUALLEVELAPID Left Join
        X020aa_Fiabd007_summ MODE On MODE.FQUALLEVELAPID = STUD.FQUALLEVELAPID And
            MODE.CAMPUS = STUD.CAMPUS And
            MODE.FPRESENTATIONCATEGORYCODEID = STUD.FPRESENTATIONCATEGORYCODEID And
            MODE.FENROLMENTCATEGORYCODEID = STUD.FENROLMENTCATEGORYCODEID And
            MODE.FEEYEAR = STUD.FEEHISTORYYEAR Left Join
        X020ad_Student_module_summ SEME On SEME.KSTUDBUSENTID = STUD.KSTUDBUSENTID And
            SEME.FQUALLEVELAPID = STUD.FQUALLEVELAPID Left Join
        X010_Student_feereg REGF On REGF.KSTUDBUSENTID = STUD.KSTUDBUSENTID Left Join
        X020ae_Student_convert_master CONV On CONV.KSTUDBUSENTID = STUD.KSTUDBUSENTID And
            CONV.FQUALLEVELAPID = STUD.FQUALLEVELAPID Left join
        X020af_Trans_feeburs_stud BURS On BURS.STUDENT = STUD.KSTUDBUSENTID     
    Where
        FIND.KSTUDBUSENTID Is Null    
    ;"""
    """
    Where
        STUD.PRESENT_CAT Like ('C%') And
        FIND.KSTUDBUSENTID Is Null    
    """
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    """
    # UPDATE FEE TEST 53 AND 54 PERIOD COLUMN
    # DO TEST ONLY IF TODAY DATE WITHIN PERIOD
    if l_debug:
        print("Update test 53_54_period_a_ind column...")
    d1 = (int(d_test_overcharge[5:7]), int(d_test_overcharge[-2:]))  # Begin date
    d2 = (int(funcdatn.get_current_month()), int(funcdatn.get_today_day()))  # Now
    d3 = (12, 31)  # End date
    if d1 <= d2 <= d3:
        s_sql = "UPDATE " + sr_file + " SET PERIOD_A_IND = 1;"
        so_curs.execute(s_sql)
        so_conn.commit()
    funcfile.writelog("%t UPDATE COLUMN: Update test 53_54_period_a_ind column")
    """

    # UPDATE FEE SHOULD BE COLUMN
    if l_debug:
        print("Update qualification fee should be column...")
    s_sql = "UPDATE " + sr_file + """
    SET FEE_SHOULD_BE = 
    CASE

        When QUAL_TYPE_FEE Like 'P%' And PRESENT_CAT Like 'D%' Then 'DP DISTANCE POSTGRADUATE' 
        When QUAL_TYPE_FEE Like 'U%' And PRESENT_CAT Like 'D%' Then 'DU DISTANCE UNDERGRADUATE' 

        When QUAL_TYPE_FEE Like 'P%' And PRESENT_CAT Like 'C%' And DISCDATE_CALC Is Null And SEM1 Is Null Then '43 CP NULL SEM FULL PAYMENT RQD' 
        When QUAL_TYPE_FEE Like 'P%' And PRESENT_CAT Like 'C%' And DISCDATE_CALC Is Null And SEM9 > 0 Then '49 CP FULL PAYMENT RQD' 
        When QUAL_TYPE_FEE Like 'P%' And PRESENT_CAT Like 'C%' And DISCDATE_CALC Is Null And SEM8 > 0 Then '48 CP FULL PAYMENT RQD' 
        When QUAL_TYPE_FEE Like 'P%' And PRESENT_CAT Like 'C%' And DISCDATE_CALC Is Null And SEM7 > 0 Then '47 CP FULL PAYMENT RQD' 
        When QUAL_TYPE_FEE Like 'P%' And PRESENT_CAT Like 'C%' And DISCDATE_CALC Is Null And SEM1 = 0 And SEM2 = 0 And SEM7 = 0 Then '44 CP ZERO SEM FULL PAYMENT RQD' 
        When QUAL_TYPE_FEE Like 'P%' And PRESENT_CAT Like 'C%' And DISCDATE_CALC Is Null And SEM2 > 0 And SEM1 = 0 Then '42 CP 2ND SEM HALF PAYMENT RQD' 
        When QUAL_TYPE_FEE Like 'P%' And PRESENT_CAT Like 'C%' And DISCDATE_CALC Is Null And SEM1 > 0 And SEM2 = 0 Then '41 CP 1ST SEM HALF PAYMENT RQD' 
        When QUAL_TYPE_FEE Like 'P%' And PRESENT_CAT Like 'C%' And DISCDATE_CALC Is Null Then '40 CP FULL PAYMENT RQD' 

        When QUAL_TYPE_FEE Like 'P%' And PRESENT_CAT Like 'C%' And DISCDATE_CALC = '' And FEE_LEVIED_TYPE Like('3%') And ENTRY_LEVEL Like('NON-ENTER%') And STATUS_FINAL Like('FINAL%') And DAYS_PASS <= 180 Then '50 CP PASS MARKONLY NO PAYMENT RQD' 
        When QUAL_TYPE_FEE Like 'P%' And PRESENT_CAT Like 'C%' And DISCDATE_CALC = '' And SEM0 = 1 And SEM1 > 0 And SEM2 = 0 And SEM7 = 0 Then '51 CP PASS 1ST SEM HALF PAYMENT RQD' 
        When QUAL_TYPE_FEE Like 'P%' And PRESENT_CAT Like 'C%' And DISCDATE_CALC = '' And SEM0 = 1 And SEM1 = 0 And SEM2 > 0 And SEM7 = 0 Then '52 CP PASS 2ND SEM HALF PAYMENT RQD' 
        When QUAL_TYPE_FEE Like 'P%' And PRESENT_CAT Like 'C%' And DISCDATE_CALC = '' And CEREMONY = 'JULY' And FEE_BURS Is Null And PERIOD_A_IND = 1 Then '53 CP PASS WGRAD NOBURS HALF PAYMENT RQD'
        When QUAL_TYPE_FEE Like 'P%' And PRESENT_CAT Like 'C%' And DISCDATE_CALC = '' And CEREMONY = 'JULY' And FEE_BURS < 0 And PERIOD_A_IND = 1 Then '54 CP PASS WGRAD BURS HALF PAYMENT RQD'
        When QUAL_TYPE_FEE Like 'P%' And PRESENT_CAT Like 'C%' And DISCDATE_CALC = '' Then '55 CP PASS FULL PAYMENT RQD' 

        When QUAL_TYPE_FEE Like 'P%' And PRESENT_CAT Like 'C%' And DISCDATE_CALC <= '%DSEM1%' Then '10 CP NO PAYMENT RQD' 
        When QUAL_TYPE_FEE Like 'P%' And PRESENT_CAT Like 'C%' And DISCDATE_CALC <= '%DSEM%' Then '20 CP DISC 1ST HALF PAYMENT RQD' 
        When QUAL_TYPE_FEE Like 'P%' And PRESENT_CAT Like 'C%' And DISCDATE_CALC > '%DSEM2%' Then '30 CP DISC 2ND FULL PAYMENT RQD'

        When QUAL_TYPE_FEE Like 'U%' And PRESENT_CAT Like 'C%' And DISCDATE_CALC Is Null And SEM1 Is Null Then '43 CU NULL SEM FULL PAYMENT RQD' 
        When QUAL_TYPE_FEE Like 'U%' And PRESENT_CAT Like 'C%' And DISCDATE_CALC Is Null And SEM9 > 0 Then '49 CU FULL PAYMENT RQD' 
        When QUAL_TYPE_FEE Like 'U%' And PRESENT_CAT Like 'C%' And DISCDATE_CALC Is Null And SEM8 > 0 Then '48 CU FULL PAYMENT RQD' 
        When QUAL_TYPE_FEE Like 'U%' And PRESENT_CAT Like 'C%' And DISCDATE_CALC Is Null And SEM7 > 0 Then '47 CU FULL PAYMENT RQD' 
        When QUAL_TYPE_FEE Like 'U%' And PRESENT_CAT Like 'C%' And DISCDATE_CALC Is Null And SEM1 = 0 And SEM2 = 0 And SEM7 = 0 Then '44 CU ZERO SEM FULL PAYMENT RQD'
        When QUAL_TYPE_FEE Like 'U%' And PRESENT_CAT Like 'C%' And DISCDATE_CALC Is Null And SEM2 = 1 And SEM1 = 0 And ISMAINQUALLEVEL = 0 And ISPOSSIBLEGRADUATE = 1 Then '12 CU 2ND SEM NOTMAIN NO PAYMENT RQD' 
        When QUAL_TYPE_FEE Like 'U%' And PRESENT_CAT Like 'C%' And DISCDATE_CALC Is Null And SEM2 > 0 And SEM1 = 0 Then '42 CU 2ND SEM HALF PAYMENT RQD' 
        When QUAL_TYPE_FEE Like 'U%' And PRESENT_CAT Like 'C%' And DISCDATE_CALC Is Null And SEM1 = 1 And SEM2 = 0 And ISMAINQUALLEVEL = 0 And ISPOSSIBLEGRADUATE = 1 Then '11 CU 1ST SEM NOTMAIN NO PAYMENT RQD'
        When QUAL_TYPE_FEE Like 'U%' And PRESENT_CAT Like 'C%' And DISCDATE_CALC Is Null And SEM1 > 0 And SEM2 = 0 Then '41 CU 1ST SEM HALF PAYMENT RQD' 
        When QUAL_TYPE_FEE Like 'U%' And PRESENT_CAT Like 'C%' And DISCDATE_CALC Is Null Then '40 CU FULL PAYMENT RQD' 

        When QUAL_TYPE_FEE Like 'U%' And PRESENT_CAT Like 'C%' And DISCDATE_CALC = '' And FEE_LEVIED_TYPE Like('3%') And ENTRY_LEVEL Like('NON-ENTER%') And STATUS_FINAL Like('FINAL%') And DAYS_PASS <= 180 Then '50 CU PASS MARKONLY NO PAYMENT RQD' 
        When QUAL_TYPE_FEE Like 'U%' And PRESENT_CAT Like 'C%' And DISCDATE_CALC = '' And SEM0 = 1 And SEM1 > 0 And SEM2 = 0 And SEM7 = 0 Then '51 CU PASS 1ST SEM HALF PAYMENT RQD' 
        When QUAL_TYPE_FEE Like 'U%' And PRESENT_CAT Like 'C%' And DISCDATE_CALC = '' And SEM0 = 1 And SEM1 = 0 And SEM2 > 0 And SEM7 = 0 Then '52 CU PASS 2ND SEM HALF PAYMENT RQD' 
        When QUAL_TYPE_FEE Like 'U%' And PRESENT_CAT Like 'C%' And DISCDATE_CALC = '' And CEREMONY = 'JULY' And FEE_BURS Is Null And PERIOD_A_IND = 1 Then '53 CU PASS WGRAD NOBURS HALF PAYMENT RQD'
        When QUAL_TYPE_FEE Like 'U%' And PRESENT_CAT Like 'C%' And DISCDATE_CALC = '' And CEREMONY = 'JULY' And FEE_BURS < 0 And PERIOD_A_IND = 1 Then '54 CU PASS WGRAD BURS HALF PAYMENT RQD'
        When QUAL_TYPE_FEE Like 'U%' And PRESENT_CAT Like 'C%' And DISCDATE_CALC = '' Then '55 CU PASS FULL PAYMENT RQD' 

        When QUAL_TYPE_FEE Like 'U%' And PRESENT_CAT Like 'C%' And DISCDATE_CALC <= '%DSEM1%' Then '10 CU NO PAYMENT RQD' 
        When QUAL_TYPE_FEE Like 'U%' And PRESENT_CAT Like 'C%' And DISCDATE_CALC <= '%DSEM2%' Then '20 CU DISC 1ST HALF PAYMENT RQD' 
        When QUAL_TYPE_FEE Like 'U%' And PRESENT_CAT Like 'C%' And DISCDATE_CALC > '%DSEM2%' Then '30 CU DISC 2ND FULL PAYMENT RQD'

        Else '90 NO ALLOCATION FULL PAYMENT RQD' 
    END
    ;"""
    s_sql = s_sql.replace("%DSEM1%", d_sem1_con)
    s_sql = s_sql.replace("%DSEM2%", d_sem2_con)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t UPDATE COLUMN: Valid qualification fee")

    # UPDATE VALID COLUMN
    if l_debug:
        print("Update qualification fee valid column...")
    so_curs.execute("UPDATE " + sr_file + """
                    SET VALID = 
                    CASE
                        When FEE_LEVIED_TYPE Like '2%' Then 0
                        When FEE_LEVIED_TYPE Like '9%' Then 1

                        When FEE_SHOULD_BE Like '11%' And FEE_LEVIED_TYPE Like '4%' Then 3
                        When FEE_SHOULD_BE Like '11%' And FEE_LEVIED_TYPE Like '5%' Then 3
                        When FEE_SHOULD_BE Like '12%' And FEE_LEVIED_TYPE Like '4%' Then 3
                        When FEE_SHOULD_BE Like '12%' And FEE_LEVIED_TYPE Like '5%' Then 3
                        When FEE_SHOULD_BE Like '1%' And FEE_LEVIED_TYPE Like '1%' Then 1
                        When FEE_SHOULD_BE Like '1%' And FEE_LEVIED_TYPE Like '3%' Then 1
                        When FEE_SHOULD_BE Like '1%' And FEE_LEVIED_TYPE Like '4%' Then 2
                        When FEE_SHOULD_BE Like '1%' And FEE_LEVIED_TYPE Like '5%' Then 2

                        When FEE_SHOULD_BE Like '2%' And FEE_LEVIED_TYPE Like '4%' Then 1
                        When FEE_SHOULD_BE Like '2%' And FEE_LEVIED_TYPE Like '5%' Then 2

                        When FEE_SHOULD_BE Like '3%' And FEE_LEVIED_TYPE Like '5%' Then 1

                        When FEE_SHOULD_BE Like '43%' And FEE_LEVIED_TYPE Like '3%' Then 1
                        When FEE_SHOULD_BE Like '44%' And FEE_LEVIED_TYPE Like '3%' Then 1
                        When FEE_SHOULD_BE Like '42%' And FEE_LEVIED_TYPE Like '4%' Then 1
                        When FEE_SHOULD_BE Like '42%' And FEE_LEVIED_TYPE Like '5%' Then 2
                        When FEE_SHOULD_BE Like '41%' And FEE_LEVIED_TYPE Like '4%' Then 1
                        When FEE_SHOULD_BE Like '41%' And FEE_LEVIED_TYPE Like '5%' Then 2
                        When FEE_SHOULD_BE Like '4%' And FEE_LEVIED_TYPE Like '5%' Then 1

                        When FEE_SHOULD_BE Like '50%' And FEE_LEVIED_TYPE Like '3%' Then 1
                        When FEE_SHOULD_BE Like '51%' And FEE_LEVIED_TYPE Like '4%' Then 1
                        When FEE_SHOULD_BE Like '51%' And FEE_LEVIED_TYPE Like '5%' Then 2
                        When FEE_SHOULD_BE Like '52%' And FEE_LEVIED_TYPE Like '4%' Then 1
                        When FEE_SHOULD_BE Like '52%' And FEE_LEVIED_TYPE Like '5%' Then 2
                        When FEE_SHOULD_BE Like '53%' And FEE_LEVIED_TYPE Like '4%' Then 1
                        When FEE_SHOULD_BE Like '53%' And FEE_LEVIED_TYPE Like '5%' Then 2
                        When FEE_SHOULD_BE Like '54%' And FEE_LEVIED_TYPE Like '4%' Then 1
                        When FEE_SHOULD_BE Like '54%' And FEE_LEVIED_TYPE Like '5%' Then 2
                        When FEE_SHOULD_BE Like '55%' And FEE_LEVIED_TYPE Like '5%' Then 1

                        When FEE_SHOULD_BE Like '8%' And FEE_LEVIED_TYPE Like '5%' Then 1

                        Else 0
                    END
                    ;""")
    so_conn.commit()
    funcfile.writelog("%t UPDATE COLUMN: Valid qualification fee")

    # IDENTIFY STUDENTS WITH TWO OR MORE HALF LEVY TRANSACTIONS
    if l_debug:
        print("Identify multiple half levy students...")
    sr_file = "X020bb_Student_multiple_half"
    s_sql = "Create table " + sr_file + " AS" + """
    Select
        STUD.KSTUDBUSENTID,
        Cast(Count(STUD.VALID) As INT) As FEE_COUNT_HALF
    From
        X020ba_Student_master STUD
    Where
        STUD.FEE_LEVIED_TYPE Like ('4%')
    Group By
        STUD.KSTUDBUSENTID
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD TABLE: " + sr_file)
    so_conn.commit()

    # TODO Further analysis of students identified as marks only

    """
    NOTE
    Use the following in the script following for test purposes.
            When STUD.CONV_IND != '' And STUD.FEE_MODE = STUD.FEE_QUAL_TOTAL Then 3
            When STUD.CONV_IND != '' And STUD.FEE_LEVIED > 0 And STUD.FEE_LEVIED = Round(STUD.FEE_QUAL_TOTAL/2,2) Then 4 
            When STUD.CONV_IND != '' And STUD.FEE_LEVIED > 0 And STUD.FEE_QUAL_TOTAL >= STUD.FEE_LEVIED Then 5
            When STUD.CONV_IND != '' And FEE_QUAL_TOTAL > 0 And FEE_SHOULD_BE Like('%HALF%') And FEE_MODE_HALF <= FEE_QUAL_TOTAL Then 6 
            When STUD.CONV_IND != '' And FEE_QUAL_TOTAL > 0 And FEE_SHOULD_BE Like('%FULL%') And FEE_MODE <= FEE_QUAL_TOTAL Then 6 
    """

    # JOIN STUDENTS AND TRANSACTIONS
    if l_debug:
        print("Join students and transactions...")
    sr_file = "X020bx_Student_master_sort"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        Case
            When HALF.FEE_COUNT_HALF = 2 Then 1
            When STUD.CONV_IND != '' And STUD.FEE_MODE = STUD.FEE_QUAL_TOTAL Then 1
            When STUD.CONV_IND != '' And STUD.FEE_LEVIED > 0 And STUD.FEE_LEVIED = Round(STUD.FEE_QUAL_TOTAL/2,2) Then 1 
            When STUD.CONV_IND != '' And STUD.FEE_LEVIED > 0 And STUD.FEE_QUAL_TOTAL >= STUD.FEE_LEVIED Then 1
            When STUD.CONV_IND != '' And FEE_QUAL_TOTAL > 0 And FEE_SHOULD_BE Like('%HALF%') And FEE_MODE_HALF <= FEE_QUAL_TOTAL Then 1 
            When STUD.CONV_IND != '' And FEE_QUAL_TOTAL > 0 And FEE_SHOULD_BE Like('%FULL%') And FEE_MODE <= FEE_QUAL_TOTAL Then 1
            Else VALID
        End As VALID,
        STUD.CAMPUS,
        STUD.KSTUDBUSENTID,
        STUD.FEE_LEVIED,
        STUD.FEE_MODE,
        STUD.FEE_MODE_HALF,
        STUD.CONV_IND,
        STUD.FEE_QUAL_TOTAL,
        HALF.FEE_COUNT_HALF,
        STUD.FEE_SHOULD_BE,
        STUD.FEE_LEVIED_TYPE,
        STUD.QUALIFICATION,
        STUD.QUALIFICATION_NAME,
        STUD.QUAL_TYPE,
        STUD.QUAL_TYPE_FEE,
        STUD.ACTIVE_IND,
        STUD.LEVY_CATEGORY,
        STUD.PRESENT_CAT,
        STUD.ENROL_CAT,
        STUD.ENTRY_LEVEL,
        STUD.STATUS_FINAL,
        STUD.FSITE,
        STUD.ORGUNIT_NAME,
        STUD.DATEQUALLEVELSTARTED,
        STUD.STARTDATE,
        STUD.DATEENROL,
        STUD.DISCONTINUEDATE,
        STUD.DAYS_REG,
        STUD.DISCDATE_CALC,
        STUD.RESULTPASSDATE,
        STUD.DAYS_PASS,
        STUD.RESULTISSUEDATE,
        STUD.RESULT,
        STUD.CEREMONYDATETIME,
        STUD.CEREMONY,
        STUD.ISHEMISSUBSIDY,
        STUD.ISMAINQUALLEVEL,
        STUD.ENROLACADEMICYEAR,
        STUD.ENROLHISTORYYEAR,
        STUD.CALCHISTORYYEAR,
        STUD.FEEHISTORYYEAR,        
        STUD.SEM0,
        STUD.SEM1,
        STUD.SEM2,
        STUD.SEM7,
        STUD.SEM8,
        STUD.SEM9,
        STUD.REG_FEE_TYPE,
        STUD.FEE_BURS,
        STUD.MIN,
        STUD.MIN_UNIT,
        STUD.MAX,
        STUD.MAX_UNIT,
        STUD.CERT_TYPE,
        STUD.LEVY_TYPE,
        STUD.BLACKLIST,
        STUD.LONG,
        STUD.DISCONTINUE_REAS,
        STUD.POSTPONE_REAS,
        STUD.FBUSINESSENTITYID,
        STUD.ORGUNIT_TYPE,
        STUD.ISCONDITIONALREG,
        STUD.MARKSFINALISEDDATE,
        STUD.EXAMSUBMINIMUM,
        STUD.ISCUMLAUDE,
        STUD.ISPOSSIBLEGRADUATE,
        STUD.FACCEPTANCETESTCODEID,
        STUD.FENROLMENTPRESENTATIONID,
        STUD.FQUALLEVELAPID,
        STUD.FPROGRAMAPID,
        STUD.TRAN_COUNT,
        STUD.FUSERBUSINESSENTITYID,
        STUD.NAME_ADDR,
        STUD.FAUDITUSERCODE,
        STUD.SYSTEM_DESC
    From
        X020ba_Student_master STUD Left Join
        X020bb_Student_multiple_half HALF On HALF.KSTUDBUSENTID = STUD.KSTUDBUSENTID
    Order By
        STUD.FEE_LEVIED_TYPE,
        STUD.FEE_SHOULD_BE
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)
    if l_export and funcsys.tablerowcount(so_curs, sr_file) > 0:
        if l_debug:
            print("Export findings...")
        sx_path = re_path + "/"
        sx_file = "Student_fee_test_020bx_qual_fee_studentlist_"
        sx_file_dated = sx_file + funcdatn.get_today_date_file()
        s_head = funccsv.get_colnames_sqlite(so_conn, sr_file)
        funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file, s_head)
        # funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file_dated, s_head)
        funcfile.writelog("%t EXPORT DATA: " + sx_path + sx_file)

        # Zip the file
        zip_file_name = sx_path + 'Student_fee_test_020bx_qual_fee_studentlist_.zip'  # Name of zip file to be created
        # files_to_zip = ['example1.txt', 'example2.txt']  # List of files to be zipped
        files_to_zip = [sx_path + 'Student_fee_test_020bx_qual_fee_studentlist_.csv']  # List of files to be zipped
        archive = zipfile.ZipFile(zip_file_name, 'w')
        # Adding files to zip
        for file in files_to_zip:
            archive.write(file, compress_type=zipfile.ZIP_DEFLATED)
        archive.close()  # Closing file
        funcfile.writelog("%t COMPRESS DATA: " + zip_file_name)

    if l_mail:
        funcmail.send_mail('vss_list_020bx_studentlist')

    # LIST OF STUDENTS REGISTERED FOR MARK ONLY AND THEIR MODULES
    if l_debug:
        print("Build list of student module marks...")
    sr_file = "X020bc_Student_module_mark"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        STUD.KSTUDBUSENTID,
        MODU.MODULE,
        MODU.MODULE_NAME,
        MODU.COMPLETE_REASON,
        MODU.PART_RESU,
        MODU.DATE_RESU
    From
        X020bx_Student_master_sort STUD Left Join
        %VSS%.X001_Student_module MODU On MODU.KSTUDBUSENTID = STUD.KSTUDBUSENTID 
    Where
        STUD.FEE_SHOULD_BE Like ('50%')
    Order By
        STUD.KSTUDBUSENTID    
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if s_period == "prev":
        s_sql = s_sql.replace("%VSS%", "VSSPREV")
    else:
        s_sql = s_sql.replace("%VSS%", "VSSCURR")
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # EXPORT INVALID TRANSACTIONS
    # NOTE - Created for Corlia de Beer and she can modify outlay hereof
    if l_debug:
        print("Build table of invalid qualification fees...")
    sr_file = "X020bx_Student_master_export"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        STUD.*
    From
        X020bx_Student_master_sort STUD
    Where
        STUD.VALID != 1        
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)
    if funcsys.tablerowcount(so_curs, sr_file) > 0:  # Ignore l_export flag - should export every time
        if l_debug:
            print("Export findings...")
        sx_path = re_path + "/"
        sx_file = "Student_fee_test_020bx_qual_fee_invalid_studentlist_"
        sx_file_dated = sx_file + funcdatn.get_today_date_file()
        s_head = funccsv.get_colnames_sqlite(so_conn, sr_file)
        funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file, s_head)
        # funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file_dated, s_head)
        funcfile.writelog("%t EXPORT DATA: " + sx_path + sx_file)

    """
    NOTE - Explanation of calculated fields in above script and qualification master export

        VALID:  0 = Qualification fee invalid.
                1 = Qualification fee valid.
                2 = Qualification fee overcharge.
                3 = Qualification fee overcharge. Not main qualification need 1 module for degree.
        FEE_LEVIED: Total qualification fee levied on the student account per qualification.
        FEE_MODE: Qualification fee loaded onto system which constitute full fee.
        FEE_MODE_HALF: Half or 50% of the FEE_MODE.
        CONV_IND:   FROM = Qualification was discontinued and converted to another qualification.
                    TO = New qualification which follows converted qualification.
        FEE_QUAL_TOTAL: The total qualification fee levied irrespective of qualification.
        FEE_COUNT_HALF: 1 = One half (50%) transaction occurred on the student account.
                        2 = Multiple half (50%) transactions occurred on the student account.
        FEE_SHOULD_BE:  A calculated proposal of what we think the qualification should be on the student account. 
                        Legend = NN CC CCCCCCCCCCCCCCCCCCCCCC
                        NN  Proposed levy category. What we calculated the the qualification levy should be.
                        CC  CP = Contact postgraduate
                            CU = Contact undergraduate
                            DP = Distance postgraduate
                            DU = Distance undergraduate
                        10 NO PAYMENT RQD: No qualification fee should be levied.
                        11 CU 1ST SEM NOTMAIN NO PAYMENT RQD: Not main qualification need 1 first semester module for degree. 
                        12 CU 2ND SEM NOTMAIN NO PAYMENT RQD: Not main qualification need 1 second semester module for degree. 
                        20 DISC 1ST HALF PAYMENT RQD: Discontinued in first semester. Levy 50%.
                        30 DISC 2ND FULL PAYMENT RQD: Discontinued in second semester. Levy 100%.
                        40 FULL PAYMENT RQD: Student studied full year. Levy 100%.
                        41 1ST SEM HALF PAYMENT RQD: Student studied only first semester. Levy 50%. See SEM1.
                        42 2ND SEM HALF PAYMENT RQD: Student studied second semester only. Levy 50%. See SEM2.
                        43 NULL SEM FULL PAYMENT RQD: Student registered for no modules. Levy 100%. No SEM1 to SEM9.
                        47 FULL PAYMENT RQD: Student registered for a year module. Levy 100%. See SEM7.
                        48 FULL PAYMENT RQD: Student registered for honours or masters script. Levy 100%. See SEM8.
                        49 FULL PAYMENT RQD: Student registered for doctoral script. Levy 100%. See SEM9.
                        50 PASS PASS MARKONLY NO PAYMENT RQD: Student only registered for marks. No Levy.
                        51 PASS 1ST SEM HALF PAYMENT RQD: Student studied only first semester and passed. Levy 50%. See SEM1.
                        52 PASS 2ND SEM HALF PAYMENT RQD: Student studied second semester only and passed. Levy 50%. See SEM2.
                        53 PASS WGRAD NOBURS HALF PAYMENT RQD: Student passed and allowed winter graduation with no bursary. Levy 50%.
                        54 PASS WGRAD BURS FULL PAYMENT RQD: Students passed and allowed winter graduation with bursary. Levy 100%.
                        55 PASS FULL PAYMENT RQD: Student registered for a year module and passed. Levy 100%. See SEM7.
        FEE_LEVIED_TYPE:    The actual fee levied category.
                            1 = NO TRANSACTION. No qualification fee transaction whatsoever was recorded on the student account.
                            2 = NEGATIVE TRANSACTION. The total of the qualification fee transactions is a credit or negative amount.
                            3 = ZERO TRANSACTION. Multiple qualification transactions were recorded, but the total is no value.
                            4 = HALF TRANSACTION. The total the qualification fee transactions amounts to half (50%) of the full fee.
                            5 = NORMAL TRANSACTION. The total of the qualification fee transactions is equal to the full fee.
                            6 = ABNORMAL TRANSACTION. The total of the qualification fee is not equal to the full or half the fee.
        DAYS_REG:   The number of days registered. Discontinue date minus enrol date.
        DISCDATE_CALC:  Discontinue date excluding passed students.
        SEM1:   Number of first semester modules.
        SEM2:   Number of second semester modules.
        SEM7:   Number of year modules.
        SEM8:   Number of honours / masters scripts. ?
        SEM9:   Number of doctorate scripts. ?
        REG_FEE_TYPE:   Registration fee levied category. See categories FEE_LEVIED_TYPE.
        TRAN_COUNT: The number of qualification (type 004) transactions on the student account.
        NAME:ADDR:  The name of the employee who did the last qualification fee transaction on the student account.
    """

    """*****************************************************************************
    QUALIFICATION FEE REPORTS
    *****************************************************************************"""
    if l_debug:
        print("QUALIFICATION FEE REPORTS")
    funcfile.writelog("QUALIFICATION FEE REPORTS")

    # SUMMARIZE QUALIFICATION FEE INCOME
    if l_debug:
        print("Summarize qualification fee income...")
    sr_file = "X020ca_Report_qual_income_summ"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = "Create Table " + sr_file + " As " + """
    Select
        STUD.ORGUNIT_NAME As Faculty,
        STUD.CAMPUS As Campus,
        STUD.FEE_LEVIED_TYPE As Fee_type,
        STUD.PRESENT_CAT As Present_category,
        STUD.ENROL_CAT As Enrol_category,
        Count(STUD.KSTUDBUSENTID) As Student_count,
        Sum(STUD.FEE_LEVIED) As Total_income
    From
        X020ba_Student_master STUD
    Group By
        STUD.ORGUNIT_NAME,
        STUD.CAMPUS,
        STUD.FEE_LEVIED_TYPE,
        STUD.PRESENT_CAT,
        STUD.ENROL_CAT
    ;"""
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    """*****************************************************************************
    QUALIFICATION FEE TEST NO TRANSACTION CONTACT
    *****************************************************************************"""
    if l_debug:
        print("QUALIFICATION FEE TEST NO TRANSACTION CONTACT")
    funcfile.writelog("QUALIFICATION FEE TEST NO TRANSACTION CONTACT")

    # FILES NEEDED
    # X020ba_Student_master

    # DECLARE VARIABLES
    i_finding_after: int = 0
    s_desc = "No qualification fee levied for contact students"

    # ISOLATE QUALIFICATIONS WITH NO TRANSACTIONS - CONTACT STUDENTS ONLY
    if l_debug:
        print("Isolate qualifications with no transactions...")
    sr_file = "X021ba_Qual_nofee_transaction"
    s_sql = "Create table " + sr_file + " AS" + """
    Select
        STUD.*
    From
        X020bx_Student_master_sort STUD
    Where
        STUD.VALID = 0 And
        STUD.FEE_LEVIED_TYPE Like ('1 NO TRANS/WITH FEE') And
        STUD.FEE_SHOULD_BE Like ('% C%')        
    Order By
        STUD.CAMPUS,
        STUD.FEE_SHOULD_BE,
        STUD.KSTUDBUSENTID
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD TABLE: " + sr_file)
    so_conn.commit()
    if l_export and funcsys.tablerowcount(so_curs, sr_file) > 0:
        if l_debug:
            print("Export findings...")
        sx_path = re_path + "/"
        sx_file = "Student_fee_test_021bx_qual_fee_no_transaction_studentlist_"
        sx_file_dated = sx_file + funcdatn.get_today_date_file()
        s_head = funccsv.get_colnames_sqlite(so_conn, sr_file)
        funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file, s_head)
        # funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file_dated, s_head)
        funcfile.writelog("%t EXPORT DATA: " + sx_path + sx_file)

    # IDENTIFY FINDINGS
    if l_debug:
        print("Identify findings...")
    sr_file = "X021bb_findings"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        'NWU' As ORG,
        FIND.CAMPUS As LOC,
        FIND.KSTUDBUSENTID As ID,
        FIND.QUALIFICATION,
        FIND.FEE_LEVIED,
        FIND.FUSERBUSINESSENTITYID As USER
    From
        X021ba_Qual_nofee_transaction FIND
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # COUNT THE NUMBER OF FINDINGS
    i_finding_before: int = funcsys.tablerowcount(so_curs, sr_file)
    if l_debug:
        print("*** Found " + str(i_finding_before) + " exceptions ***")
    funcfile.writelog("%t FINDING: " + str(i_finding_before) + " QUALIFICATION NULL FEE NOTRAN finding(s)")

    # GET PREVIOUS FINDINGS
    if i_finding_before > 0:
        i = functest.get_previous_finding(so_curs, ed_path, "302_reported.txt", "qualification no transaction", "ITTTT")
        so_conn.commit()

    # SET PREVIOUS FINDINGS
    if i_finding_before > 0:
        i = functest.set_previous_finding(so_curs)
        so_conn.commit()

    # ADD PREVIOUS FINDINGS
    sr_file = "X021bd_add_previous"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0:
        if l_debug:
            print("Join previously reported to current findings...")
        s_sql = "CREATE TABLE " + sr_file + " AS" + """
        Select
            FIND.*,
            'qualification no transaction' AS PROCESS,
            '%TODAY%' AS DATE_REPORTED,
            '%DAYS%' AS DATE_RETEST,
            PREV.PROCESS AS PREV_PROCESS,
            PREV.DATE_REPORTED AS PREV_DATE_REPORTED,
            PREV.DATE_RETEST AS PREV_DATE_RETEST,
            PREV.REMARK
        From
            X021bb_findings FIND Left Join
            Z001ab_setprev PREV ON PREV.FIELD1 = FIND.ID And
                PREV.FIELD2 = FIND.QUALIFICATION
        ;"""
        s_sql = s_sql.replace("%TODAY%", funcdatn.get_today_date())
        s_sql = s_sql.replace("%DAYS%", funcdatn.get_current_month_end_next())
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD LIST TO UPDATE FINDINGS
    sr_file = "X021be_new_previous"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0:
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            PREV.PROCESS,
            PREV.ID AS FIELD1,
            PREV.QUALIFICATION AS FIELD2,
            '' AS FIELD3,
            '' AS FIELD4,
            '' AS FIELD5,
            PREV.DATE_REPORTED,
            PREV.DATE_RETEST,
            PREV.REMARK
        From
            X021bd_add_previous PREV
        Where
            PREV.PREV_PROCESS Is Null Or
            PREV.DATE_REPORTED > PREV.PREV_DATE_RETEST And PREV.REMARK = ""
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)
        # Export findings to previous reported file
        i_finding_after = funcsys.tablerowcount(so_curs, sr_file)
        if i_finding_after > 0:
            if l_debug:
                print("*** " + str(i_finding_after) + " Finding(s) to report ***")
            sx_path = ed_path
            sx_file = "302_reported"
            # Read the header data
            s_head = funccsv.get_colnames_sqlite(so_conn, sr_file)
            # Write the data
            if l_record:
                funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file, s_head, "a", ".txt")
                funcfile.writelog("%t FINDING: " + str(i_finding_after) + " new finding(s) to export")
                funcfile.writelog("%t EXPORT DATA: " + sr_file)
            if l_mess:
                funcsms.send_telegram('', 'administrator',
                                      '<b>' + str(i_finding_before) + '/' + str(i_finding_after) + '</b> ' + s_desc)
        else:
            if l_debug:
                print("*** No new findings to report ***")
            funcfile.writelog("%t FINDING: No new findings to export")

    # IMPORT OFFICERS FOR MAIL REPORTING PURPOSES
    if i_finding_before > 0 and i_finding_after > 0:
        i = functest.get_officer(so_curs, "VSS", "stud_fee_test_qual_no_fee_transaction_officer")
        so_conn.commit()

    # IMPORT SUPERVISORS FOR MAIL REPORTING PURPOSES
    if i_finding_before > 0 and i_finding_after > 0:
        i = functest.get_supervisor(so_curs, "VSS", "stud_fee_test_qual_no_fee_transaction_supervisor")
        so_conn.commit()

    # ADD CONTACT DETAILS TO FINDINGS
    sr_file = "X021bh_detail"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0 and i_finding_after > 0:
        if l_debug:
            print("Add contact details to findings...")
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            PREV.ORG,
            PREV.LOC,
            PREV.ID,
            PREV.QUALIFICATION,
            MAST.QUALIFICATION_NAME,
            MAST.FEE_LEVIED,
            MAST.FEE_SHOULD_BE,
            MAST.FEE_MODE,
            MAST.DATEENROL,
            MAST.RESULTPASSDATE,
            MAST.DISCONTINUEDATE,
            CAMP_OFF.EMPLOYEE_NUMBER As CAMP_OFF_NUMB,
            CAMP_OFF.NAME_ADDR As CAMP_OFF_NAME,
            CASE
                WHEN  CAMP_OFF.EMPLOYEE_NUMBER <> '' THEN CAMP_OFF.EMPLOYEE_NUMBER||'@nwu.ac.za'
                ELSE CAMP_OFF.EMAIL_ADDRESS
            END As CAMP_OFF_MAIL,
            CAMP_OFF.EMAIL_ADDRESS As CAMP_OFF_MAIL2,
            CAMP_SUP.EMPLOYEE_NUMBER As CAMP_SUP_NUMB,
            CAMP_SUP.NAME_ADDR As CAMP_SUP_NAME,
            CASE
                WHEN CAMP_SUP.EMPLOYEE_NUMBER <> '' THEN CAMP_SUP.EMPLOYEE_NUMBER||'@nwu.ac.za'
                ELSE CAMP_SUP.EMAIL_ADDRESS
            END As CAMP_SUP_MAIL,
            CAMP_SUP.EMAIL_ADDRESS As CAMP_SUP_MAIL2,
            ORG_OFF.EMPLOYEE_NUMBER As ORG_OFF_NUMB,
            ORG_OFF.NAME_ADDR As ORG_OFF_NAME,
            CASE
                WHEN ORG_OFF.EMPLOYEE_NUMBER <> '' THEN ORG_OFF.EMPLOYEE_NUMBER||'@nwu.ac.za'
                ELSE ORG_OFF.EMAIL_ADDRESS
            END As ORG_OFF_MAIL,
            ORG_OFF.EMAIL_ADDRESS As ORG_OFF_MAIL2,
            ORG_SUP.EMPLOYEE_NUMBER As ORG_SUP_NUMB,
            ORG_SUP.NAME_ADDR As ORG_SUP_NAME,
            CASE
                WHEN ORG_SUP.EMPLOYEE_NUMBER <> '' THEN ORG_SUP.EMPLOYEE_NUMBER||'@nwu.ac.za'
                ELSE ORG_SUP.EMAIL_ADDRESS
            END As ORG_SUP_MAIL,
            ORG_SUP.EMAIL_ADDRESS As ORG_SUP_MAIL2,
            PREV.USER
        From
            X021bd_add_previous PREV Left Join
            X021ba_Qual_nofee_transaction MAST On MAST.KSTUDBUSENTID = PREV.ID And
                MAST.QUALIFICATION = PREV.QUALIFICATION Left Join
            Z001af_officer CAMP_OFF On CAMP_OFF.CAMPUS = PREV.LOC Left Join
            Z001af_officer ORG_OFF On ORG_OFF.CAMPUS = PREV.ORG Left Join
            Z001ag_supervisor CAMP_SUP On CAMP_SUP.CAMPUS = PREV.LOC Left Join
            Z001ag_supervisor ORG_SUP On ORG_SUP.CAMPUS = PREV.ORG
        Where
            PREV.PREV_PROCESS Is Null Or
            PREV.DATE_REPORTED > PREV.PREV_DATE_RETEST And PREV.REMARK = ""
        ;"""
        """
        WHEN CAMP_OFF.NAME_ADDR != '' THEN CAMP_OFF.NAME_ADDR 
        """
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD THE FINAL TABLE FOR EXPORT AND REPORT
    sr_file = "X021bx_Qual_nofee_transaction"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if l_debug:
        print("Build the final report")
    if i_finding_before > 0 and i_finding_after > 0:
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            'QUALIFICATION FEE NO TRANSACTION' As Audit_finding,
            FIND.ORG As 'Organization',
            FIND.LOC As 'Campus',
            FIND.ID As 'Student',
            FIND.QUALIFICATION As 'Qualification',
            FIND.QUALIFICATION_NAME As 'Qualification_name',
            FIND.FEE_LEVIED As 'Fee_levied',
            FIND.FEE_SHOULD_BE As 'Fee_should_be',
            FIND.FEE_MODE As 'Qual_fee',
            FIND.DATEENROL As 'Date_enrol',
            FIND.RESULTPASSDATE As 'Date_pass',
            FIND.DISCONTINUEDATE As 'Date_discontinue',
            FIND.CAMP_OFF_NAME AS Responsible_Officer,
            FIND.CAMP_OFF_NUMB AS Responsible_Officer_Numb,
            FIND.CAMP_OFF_MAIL AS Responsible_Officer_Mail,
            FIND.CAMP_SUP_NAME AS Supervisor,
            FIND.CAMP_SUP_NUMB AS Supervisor_Numb,
            FIND.CAMP_SUP_MAIL AS Supervisor_Mail,
            FIND.ORG_OFF_NAME AS Org_Officer,
            FIND.ORG_OFF_NUMB AS Org_Officer_Numb,
            FIND.ORG_OFF_MAIL AS Org_Officer_Mail,
            FIND.ORG_SUP_NAME AS Org_Supervisor,
            FIND.ORG_SUP_NUMB AS Org_Supervisor_Numb,
            FIND.ORG_SUP_MAIL AS Org_Supervisor_Mail
        From
            X021bh_detail FIND
        Order by
            FIND.LOC,
            FIND.QUALIFICATION,
            FIND.FEE_SHOULD_BE,
            FIND.ID
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)
        # Export findings
        if l_export and funcsys.tablerowcount(so_curs, sr_file) > 0:
            if l_debug:
                print("Export findings...")
            sx_path = re_path + "/"
            sx_file = "Student_fee_test_021bx_qual_fee_no_transaction_"
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

    """*****************************************************************************
    QUALIFICATION FEE TEST NEGATIVE TRANSACTION
    *****************************************************************************"""
    if l_debug:
        print("QUALIFICATION FEE TEST NEGATIVE TRANSACTION")
    funcfile.writelog("QUALIFICATION FEE TEST NEGATIVE TRANSACTION")

    # FILES NEEDED
    # X020ba_Student_master

    # DECLARE VARIABLES
    i_finding_after: int = 0
    s_desc = "Qualification fee a credit or negative amount for contact student"

    # ISOLATE QUALIFICATIONS WITH NO TRANSACTIONS - CONTACT STUDENTS ONLY
    if l_debug:
        print("Isolate qualifications with negative value transactions...")
    sr_file = "X021da_Qual_negativefee_transaction"
    s_sql = "Create table " + sr_file + " AS" + """
    Select
        STUD.*
    From
        X020ba_Student_master STUD
    Where
        STUD.VALID = 0 And
        STUD.FEE_LEVIED_TYPE Like ('2%')
    Order By
        STUD.CAMPUS,
        STUD.FEE_SHOULD_BE,
        STUD.KSTUDBUSENTID
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD TABLE: " + sr_file)
    so_conn.commit()
    if funcsys.tablerowcount(so_curs, sr_file) > 0:  # Ignore l_export flag - should export every time
        if l_debug:
            print("Export findings...")
        sx_path = re_path + "/"
        sx_file = "Student_fee_test_021dx_qual_fee_negative_transaction_studentlist_"
        sx_file_dated = sx_file + funcdatn.get_today_date_file()
        s_head = funccsv.get_colnames_sqlite(so_conn, sr_file)
        funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file, s_head)
        # funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file_dated, s_head)
        funcfile.writelog("%t EXPORT DATA: " + sx_path + sx_file)

    # IDENTIFY FINDINGS
    if l_debug:
        print("Identify findings...")
    sr_file = "X021db_findings"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        'NWU' As ORG,
        FIND.CAMPUS As LOC,
        FIND.KSTUDBUSENTID As ID,
        FIND.QUALIFICATION,
        FIND.FEE_LEVIED,
        FIND.FUSERBUSINESSENTITYID As USER
    From
        X021da_Qual_negativefee_transaction FIND
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # COUNT THE NUMBER OF FINDINGS
    i_finding_before: int = funcsys.tablerowcount(so_curs, sr_file)
    if l_debug:
        print("*** Found " + str(i_finding_before) + " exceptions ***")
    funcfile.writelog("%t FINDING: " + str(i_finding_before) + " QUALIFICATION NEGATIVE FEE TRAN finding(s)")

    # GET PREVIOUS FINDINGS
    sr_file = "X021dc_get_previous"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0:
        if l_debug:
            print("Import previously reported findings...")
        so_curs.execute(
            "CREATE TABLE " + sr_file + """
            (PROCESS TEXT,
            FIELD1 INT,
            FIELD2 TEXT,
            FIELD3 REAL,
            FIELD4 TEXT,
            FIELD5 TEXT,
            DATE_REPORTED TEXT,
            DATE_RETEST TEXT,
            REMARK TEXT)
            """)
        co = open(ed_path + "302_reported.txt", "r")
        co_reader = csv.reader(co)
        # Read the COLUMN database data
        for row in co_reader:
            # Populate the column variables
            if row[0] == "PROCESS":
                continue
            elif row[0] != "qualification negative transaction":
                continue
            else:
                s_cols = "INSERT INTO " + sr_file + " VALUES('" + row[0] + "','" + row[1] + "','" + row[2] + "','" + \
                         row[
                             3] + "','" + row[4] + "','" + row[5] + "','" + row[6] + "','" + row[7] + "','" + row[
                             8] + "')"
                so_curs.execute(s_cols)
        so_conn.commit()
        # Close the imported data file
        co.close()
        funcfile.writelog("%t IMPORT TABLE: " + ed_path + "302_reported.txt (" + sr_file + ")")

    # SET PREVIOUS FINDINGS
    sr_file = "X021dc_set_previous"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0:
        if l_debug:
            print("Obtain the latest previous finding...")
        s_sql = "Create Table " + sr_file + " As" + """
        Select
            GET.PROCESS,
            GET.FIELD1,
            GET.FIELD2,
            GET.FIELD3,
            GET.FIELD4,
            GET.FIELD5,
            Max(GET.DATE_REPORTED) As DATE_REPORTED,
            GET.DATE_RETEST,
            GET.REMARK
        From
            X021dc_get_previous GET
        Group By
            GET.FIELD1,
            GET.FIELD2,
            GET.FIELD3        
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # ADD PREVIOUS FINDINGS
    sr_file = "X021dd_add_previous"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0:
        if l_debug:
            print("Join previously reported to current findings...")
        s_sql = "CREATE TABLE " + sr_file + " AS" + """
        Select
            FIND.*,
            'qualification negative transaction' AS PROCESS,
            '%TODAY%' AS DATE_REPORTED,
            '%DAYS%' AS DATE_RETEST,
            PREV.PROCESS AS PREV_PROCESS,
            PREV.DATE_REPORTED AS PREV_DATE_REPORTED,
            PREV.DATE_RETEST AS PREV_DATE_RETEST,
            PREV.REMARK
        From
            X021db_findings FIND Left Join
            X021dc_set_previous PREV ON PREV.FIELD1 = FIND.ID And
                PREV.FIELD2 = FIND.QUALIFICATION And
                PREV.FIELD3 = FIND.FEE_LEVIED
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        s_sql = s_sql.replace("%TODAY%", funcdatn.get_today_date())
        s_sql = s_sql.replace("%DAYS%", funcdatn.get_current_month_end_next())
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD LIST TO UPDATE FINDINGS
    sr_file = "X021de_new_previous"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0:
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            PREV.PROCESS,
            PREV.ID AS FIELD1,
            PREV.QUALIFICATION AS FIELD2,
            PREV.FEE_LEVIED AS FIELD3,
            '' AS FIELD4,
            '' AS FIELD5,
            PREV.DATE_REPORTED,
            PREV.DATE_RETEST,
            PREV.REMARK
        From
            X021dd_add_previous PREV
        Where
            PREV.PREV_PROCESS Is Null Or
            PREV.DATE_REPORTED > PREV.PREV_DATE_RETEST And PREV.REMARK = ""
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)
        # Export findings to previous reported file
        i_finding_after = funcsys.tablerowcount(so_curs, sr_file)
        if i_finding_after > 0:
            if l_debug:
                print("*** " + str(i_finding_after) + " Finding(s) to report ***")
            sx_path = ed_path
            sx_file = "302_reported"
            # Read the header data
            s_head = funccsv.get_colnames_sqlite(so_conn, sr_file)
            # Write the data
            if l_record:
                funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file, s_head, "a", ".txt")
                funcfile.writelog("%t FINDING: " + str(i_finding_after) + " new finding(s) to export")
                funcfile.writelog("%t EXPORT DATA: " + sr_file)
            if l_mess:
                funcsms.send_telegram('', 'administrator',
                                      '<b>' + str(i_finding_before) + '/' + str(i_finding_after) + '</b> ' + s_desc)
        else:
            if l_debug:
                print("*** No new findings to report ***")
            funcfile.writelog("%t FINDING: No new findings to export")

    # IMPORT OFFICERS FOR MAIL REPORTING PURPOSES
    sr_file = "X021df_officer"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0:
        if i_finding_after > 0:
            if l_debug:
                print("Import reporting officers for mail purposes...")
            s_sql = "CREATE TABLE " + sr_file + " AS " + """
            Select
                OFFICER.LOOKUP,
                Upper(OFFICER.LOOKUP_CODE) AS CAMPUS,
                OFFICER.LOOKUP_DESCRIPTION AS EMPLOYEE_NUMBER,
                PEOP.NAME_ADDR As NAME,
                PEOP.EMAIL_ADDRESS
            From
                VSS.X000_OWN_LOOKUPS OFFICER Left Join
                PEOPLE.X002_PEOPLE_CURR PEOP ON PEOP.EMPLOYEE_NUMBER = OFFICER.LOOKUP_DESCRIPTION
            Where
                OFFICER.LOOKUP = 'stud_fee_test_qual_negative_fee_transaction_officer'
            ;"""
            so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
            so_curs.execute(s_sql)
            so_conn.commit()
            funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # IMPORT SUPERVISORS FOR MAIL REPORTING PURPOSES
    sr_file = "X021dg_supervisor"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0 and i_finding_after > 0:
        if l_debug:
            print("Import reporting supervisors for mail purposes...")
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            SUPERVISOR.LOOKUP,
            Upper(SUPERVISOR.LOOKUP_CODE) AS CAMPUS,
            SUPERVISOR.LOOKUP_DESCRIPTION AS EMPLOYEE_NUMBER,
            PEOP.NAME_ADDR As NAME,
            PEOP.EMAIL_ADDRESS
        From
            VSS.X000_OWN_LOOKUPS SUPERVISOR Left Join
            PEOPLE.X002_PEOPLE_CURR PEOP ON PEOP.EMPLOYEE_NUMBER = SUPERVISOR.LOOKUP_DESCRIPTION
        Where
            SUPERVISOR.LOOKUP = 'stud_fee_test_qual_negative_fee_transaction_supervisor'
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # ADD CONTACT DETAILS TO FINDINGS
    sr_file = "X021dh_detail"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0 and i_finding_after > 0:
        if l_debug:
            print("Add contact details to findings...")
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            PREV.ORG,
            PREV.LOC,
            PREV.ID,
            PREV.QUALIFICATION,
            MAST.QUALIFICATION_NAME,
            MAST.FEE_LEVIED,
            MAST.FEE_SHOULD_BE,
            MAST.FEE_MODE,
            MAST.DATEENROL,
            MAST.RESULTPASSDATE,
            MAST.DISCONTINUEDATE,
            CAMP_OFF.EMPLOYEE_NUMBER As CAMP_OFF_NUMB,
            CAMP_OFF.NAME As CAMP_OFF_NAME,
            CASE
                WHEN  CAMP_OFF.EMPLOYEE_NUMBER <> '' THEN CAMP_OFF.EMPLOYEE_NUMBER||'@nwu.ac.za'
                ELSE CAMP_OFF.EMAIL_ADDRESS
            END As CAMP_OFF_MAIL,
            CAMP_OFF.EMAIL_ADDRESS As CAMP_OFF_MAIL2,
            CAMP_SUP.EMPLOYEE_NUMBER As CAMP_SUP_NUMB,
            CAMP_SUP.NAME As CAMP_SUP_NAME,
            CASE
                WHEN CAMP_SUP.EMPLOYEE_NUMBER <> '' THEN CAMP_SUP.EMPLOYEE_NUMBER||'@nwu.ac.za'
                ELSE CAMP_SUP.EMAIL_ADDRESS
            END As CAMP_SUP_MAIL,
            CAMP_SUP.EMAIL_ADDRESS As CAMP_SUP_MAIL2,
            ORG_OFF.EMPLOYEE_NUMBER As ORG_OFF_NUMB,
            ORG_OFF.NAME As ORG_OFF_NAME,
            CASE
                WHEN ORG_OFF.EMPLOYEE_NUMBER <> '' THEN ORG_OFF.EMPLOYEE_NUMBER||'@nwu.ac.za'
                ELSE ORG_OFF.EMAIL_ADDRESS
            END As ORG_OFF_MAIL,
            ORG_OFF.EMAIL_ADDRESS As ORG_OFF_MAIL2,
            ORG_SUP.EMPLOYEE_NUMBER As ORG_SUP_NUMB,
            ORG_SUP.NAME As ORG_SUP_NAME,
            CASE
                WHEN ORG_SUP.EMPLOYEE_NUMBER <> '' THEN ORG_SUP.EMPLOYEE_NUMBER||'@nwu.ac.za'
                ELSE ORG_SUP.EMAIL_ADDRESS
            END As ORG_SUP_MAIL,
            ORG_SUP.EMAIL_ADDRESS As ORG_SUP_MAIL2,
            PREV.USER As USER_NUMB,
            CASE
                WHEN PREV.USER != '' THEN PEOP.NAME_ADDR
                WHEN CAMP_OFF.NAME != '' THEN CAMP_OFF.NAME 
                ELSE ''
            END As USER_NAME,
            CASE
                WHEN PREV.USER != '' THEN PREV.USER||'@nwu.ac.za'
                WHEN CAMP_OFF.EMPLOYEE_NUMBER != '' THEN CAMP_OFF.EMPLOYEE_NUMBER||'@nwu.ac.za'
                ELSE ''
            END As USER_MAIL,
            CASE
                WHEN PREV.USER != '' THEN PEOP.EMAIL_ADDRESS
                WHEN CAMP_OFF.EMPLOYEE_NUMBER != '' THEN CAMP_OFF.EMAIL_ADDRESS
                ELSE ''
            END As USER_MAIL2
        From
            X021dd_add_previous PREV Left Join
            X021da_Qual_negativefee_transaction MAST On MAST.KSTUDBUSENTID = PREV.ID And
                MAST.QUALIFICATION = PREV.QUALIFICATION Left Join
            X021df_officer CAMP_OFF On CAMP_OFF.CAMPUS = PREV.LOC Left Join
            X021df_officer ORG_OFF On ORG_OFF.CAMPUS = PREV.ORG Left Join
            X021dg_supervisor CAMP_SUP On CAMP_SUP.CAMPUS = PREV.LOC Left Join
            X021dg_supervisor ORG_SUP On ORG_SUP.CAMPUS = PREV.ORG Left Join
            PEOPLE.X002_PEOPLE_CURR PEOP ON PEOP.EMPLOYEE_NUMBER = PREV.USER
        Where
          PREV.PREV_PROCESS IS NULL
        ;"""
        """
        WHEN CAMP_OFF.NAME != '' THEN CAMP_OFF.NAME 
        """
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD THE FINAL TABLE FOR EXPORT AND REPORT
    sr_file = "X021dx_Qual_negativefee_transaction"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if l_debug:
        print("Build the final report")
    if i_finding_before > 0 and i_finding_after > 0:
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            'QUALIFICATION FEE NEGATIVE VALUE TRANSACTION' As Audit_finding,
            FIND.ORG As 'Organization',
            FIND.LOC As 'Campus',
            FIND.ID As 'Student',
            FIND.QUALIFICATION As 'Qualification',
            FIND.QUALIFICATION_NAME As 'Qualification_name',
            FIND.FEE_LEVIED As 'Fee_levied',
            FIND.FEE_SHOULD_BE As 'Fee_should_be',
            FIND.FEE_MODE As 'Qual_fee',
            FIND.DATEENROL As 'Date_enrol',
            FIND.RESULTPASSDATE As 'Date_pass',
            FIND.DISCONTINUEDATE As 'Date_discontinue',
            FIND.CAMP_OFF_NAME AS Responsible_Officer,
            FIND.CAMP_OFF_NUMB AS Responsible_Officer_Numb,
            FIND.CAMP_OFF_MAIL AS Responsible_Officer_Mail,
            FIND.CAMP_SUP_NAME AS Supervisor,
            FIND.CAMP_SUP_NUMB AS Supervisor_Numb,
            FIND.CAMP_SUP_MAIL AS Supervisor_Mail,
            FIND.ORG_OFF_NAME AS Org_Officer,
            FIND.ORG_OFF_NUMB AS Org_Officer_Numb,
            FIND.ORG_OFF_MAIL AS Org_Officer_Mail,
            FIND.ORG_SUP_NAME AS Org_Supervisor,
            FIND.ORG_SUP_NUMB AS Org_Supervisor_Numb,
            FIND.ORG_SUP_MAIL AS Org_Supervisor_Mail,
            FIND.USER_NAME As User,
            FIND.USER_NUMB As User_Numb,
            FIND.USER_MAIL As User_Mail
        From
            X021dh_detail FIND
        Order by
            FIND.LOC,
            FIND.QUALIFICATION,
            FIND.FEE_SHOULD_BE,
            FIND.ID
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)
        # Export findings
        if l_export and funcsys.tablerowcount(so_curs, sr_file) > 0:
            if l_debug:
                print("Export findings...")
            sx_path = re_path + "/"
            sx_file = "Student_fee_test_021dx_qual_fee_negative_transaction_"
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

    """*****************************************************************************
    QUALIFICATION FEE TEST ZERO TRANSACTION CONTACT
    *****************************************************************************"""
    if l_debug:
        print("QUALIFICATION FEE TEST ZERO TRANSACTION CONTACT")
    funcfile.writelog("QUALIFICATION FEE TEST ZERO TRANSACTION CONTACT")

    # FILES NEEDED
    # X020ba_Student_master

    # DECLARE VARIABLES
    i_finding_after: int = 0
    s_desc = "Qualification fee transactions amount to no value for contact students"

    # ISOLATE QUALIFICATIONS WITH ZERO TRANSACTIONS
    if l_debug:
        print("Isolate qualifications with zero value transactions...")
    sr_file = "X021ca_Qual_zerofee_transaction"
    s_sql = "Create table " + sr_file + " AS" + """
    Select
        STUD.*
    From
        X020bx_Student_master_sort STUD
    Where
        STUD.VALID = 0 And
        STUD.FEE_LEVIED_TYPE Like ('3%') And
        STUD.FEE_SHOULD_BE Like ('% C%')      
    Order By
        STUD.CAMPUS,
        STUD.FEE_SHOULD_BE,
        STUD.KSTUDBUSENTID
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD TABLE: " + sr_file)
    so_conn.commit()
    if l_export and funcsys.tablerowcount(so_curs, sr_file) > 0:
        if l_debug:
            print("Export findings...")
        sx_path = re_path + "/"
        sx_file = "Student_fee_test_021cx_qual_fee_zero_transaction_studentlist_"
        sx_file_dated = sx_file + funcdatn.get_today_date_file()
        s_head = funccsv.get_colnames_sqlite(so_conn, sr_file)
        funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file, s_head)
        # funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file_dated, s_head)
        funcfile.writelog("%t EXPORT DATA: " + sx_path + sx_file)

    # IDENTIFY FINDINGS
    if l_debug:
        print("Identify findings...")
    sr_file = "X021cb_findings"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        'NWU' As ORG,
        FIND.CAMPUS As LOC,
        FIND.KSTUDBUSENTID As ID,
        FIND.QUALIFICATION,
        FIND.FEE_LEVIED,
        FIND.FUSERBUSINESSENTITYID As USER,
        FIND.NAME_ADDR As USER_NAME,
        FIND.SYSTEM_DESC
    From
        X021ca_Qual_zerofee_transaction FIND
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # COUNT THE NUMBER OF FINDINGS
    i_finding_before: int = funcsys.tablerowcount(so_curs, sr_file)
    if l_debug:
        print("*** Found " + str(i_finding_before) + " exceptions ***")
    funcfile.writelog("%t FINDING: " + str(i_finding_before) + " QUALIFICATION ZERO FEE TRAN finding(s)")

    # GET PREVIOUS FINDINGS
    sr_file = "X021cc_get_previous"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0:
        if l_debug:
            print("Import previously reported findings...")
        so_curs.execute(
            "CREATE TABLE " + sr_file + """
            (PROCESS TEXT,
            FIELD1 INT,
            FIELD2 TEXT,
            FIELD3 REAL,
            FIELD4 TEXT,
            FIELD5 TEXT,
            DATE_REPORTED TEXT,
            DATE_RETEST TEXT,
            REMARK TEXT)
            """)
        co = open(ed_path + "302_reported.txt", "r")
        co_reader = csv.reader(co)
        # Read the COLUMN database data
        for row in co_reader:
            # Populate the column variables
            if row[0] == "PROCESS":
                continue
            elif row[0] != "qualification zero transaction":
                continue
            else:
                s_cols = "INSERT INTO " + sr_file + " VALUES('" + row[0] + "','" + row[1] + "','" + row[2] + "','" + \
                         row[
                             3] + "','" + row[4] + "','" + row[5] + "','" + row[6] + "','" + row[7] + "','" + row[
                             8] + "')"
                so_curs.execute(s_cols)
        so_conn.commit()
        # Close the imported data file
        co.close()
        funcfile.writelog("%t IMPORT TABLE: " + ed_path + "302_reported.txt (" + sr_file + ")")

    # SET PREVIOUS FINDINGS
    sr_file = "X021cc_set_previous"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0:
        if l_debug:
            print("Obtain the latest previous finding...")
        s_sql = "Create Table " + sr_file + " As" + """
        Select
            GET.PROCESS,
            GET.FIELD1,
            GET.FIELD2,
            GET.FIELD3,
            GET.FIELD4,
            GET.FIELD5,
            Max(GET.DATE_REPORTED) As DATE_REPORTED,
            GET.DATE_RETEST,
            GET.REMARK
        From
            X021cc_get_previous GET
        Group By
            GET.FIELD1,
            GET.FIELD2,
            GET.FIELD3        
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # ADD PREVIOUS FINDINGS
    sr_file = "X021cd_add_previous"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0:
        if l_debug:
            print("Join previously reported to current findings...")
        s_sql = "CREATE TABLE " + sr_file + " AS" + """
        Select
            FIND.*,
            'qualification zero transaction' AS PROCESS,
            '%TODAY%' AS DATE_REPORTED,
            '%DAYS%' AS DATE_RETEST,
            PREV.PROCESS AS PREV_PROCESS,
            PREV.DATE_REPORTED AS PREV_DATE_REPORTED,
            PREV.DATE_RETEST AS PREV_DATE_RETEST,
            PREV.REMARK
        From
            X021cb_findings FIND Left Join
            X021cc_set_previous PREV ON PREV.FIELD1 = FIND.ID And
                PREV.FIELD2 = FIND.QUALIFICATION And
                PREV.FIELD3 = FIND.FEE_LEVIED
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        s_sql = s_sql.replace("%TODAY%", funcdatn.get_today_date())
        s_sql = s_sql.replace("%DAYS%", funcdatn.get_current_month_end_next())
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD LIST TO UPDATE FINDINGS
    sr_file = "X021ce_new_previous"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0:
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            PREV.PROCESS,
            PREV.ID AS FIELD1,
            PREV.QUALIFICATION AS FIELD2,
            PREV.FEE_LEVIED AS FIELD3,
            '' AS FIELD4,
            '' AS FIELD5,
            PREV.DATE_REPORTED,
            PREV.DATE_RETEST,
            PREV.REMARK
        From
            X021cd_add_previous PREV
        Where
            PREV.PREV_PROCESS Is Null Or
            PREV.DATE_REPORTED > PREV.PREV_DATE_RETEST And PREV.REMARK = ""
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)
        # Export findings to previous reported file
        i_finding_after = funcsys.tablerowcount(so_curs, sr_file)
        if i_finding_after > 0:
            if l_debug:
                print("*** " + str(i_finding_after) + " Finding(s) to report ***")
            sx_path = ed_path
            sx_file = "302_reported"
            # Read the header data
            s_head = funccsv.get_colnames_sqlite(so_conn, sr_file)
            # Write the data
            if l_record:
                funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file, s_head, "a", ".txt")
                funcfile.writelog("%t FINDING: " + str(i_finding_after) + " new finding(s) to export")
                funcfile.writelog("%t EXPORT DATA: " + sr_file)
            if l_mess:
                funcsms.send_telegram('', 'administrator',
                                      '<b>' + str(i_finding_before) + '/' + str(i_finding_after) + '</b> ' + s_desc)
        else:
            if l_debug:
                print("*** No new findings to report ***")
            funcfile.writelog("%t FINDING: No new findings to export")

    # IMPORT OFFICERS FOR MAIL REPORTING PURPOSES
    sr_file = "X021cf_officer"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0:
        if i_finding_after > 0:
            if l_debug:
                print("Import reporting officers for mail purposes...")
            s_sql = "CREATE TABLE " + sr_file + " AS " + """
            Select
                OFFICER.LOOKUP,
                Upper(OFFICER.LOOKUP_CODE) AS CAMPUS,
                OFFICER.LOOKUP_DESCRIPTION AS EMPLOYEE_NUMBER,
                PEOP.NAME_ADDR As NAME,
                PEOP.EMAIL_ADDRESS
            From
                VSS.X000_OWN_LOOKUPS OFFICER Left Join
                PEOPLE.X002_PEOPLE_CURR PEOP ON PEOP.EMPLOYEE_NUMBER = OFFICER.LOOKUP_DESCRIPTION
            Where
                OFFICER.LOOKUP = 'stud_fee_test_qual_zero_fee_transaction_officer'
            ;"""
            so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
            so_curs.execute(s_sql)
            so_conn.commit()
            funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # IMPORT SUPERVISORS FOR MAIL REPORTING PURPOSES
    sr_file = "X021cg_supervisor"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0 and i_finding_after > 0:
        if l_debug:
            print("Import reporting supervisors for mail purposes...")
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            SUPERVISOR.LOOKUP,
            Upper(SUPERVISOR.LOOKUP_CODE) AS CAMPUS,
            SUPERVISOR.LOOKUP_DESCRIPTION AS EMPLOYEE_NUMBER,
            PEOP.NAME_ADDR As NAME,
            PEOP.EMAIL_ADDRESS
        From
            VSS.X000_OWN_LOOKUPS SUPERVISOR Left Join
            PEOPLE.X002_PEOPLE_CURR PEOP ON PEOP.EMPLOYEE_NUMBER = SUPERVISOR.LOOKUP_DESCRIPTION
        Where
            SUPERVISOR.LOOKUP = 'stud_fee_test_qual_zero_fee_transaction_supervisor'
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # ADD CONTACT DETAILS TO FINDINGS
    sr_file = "X021ch_detail"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0 and i_finding_after > 0:
        if l_debug:
            print("Add contact details to findings...")
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            PREV.ORG,
            PREV.LOC,
            PREV.ID,
            PREV.QUALIFICATION,
            MAST.QUALIFICATION_NAME,
            MAST.FEE_LEVIED,
            MAST.FEE_SHOULD_BE,
            MAST.FEE_MODE,
            MAST.DATEENROL,
            MAST.RESULTPASSDATE,
            MAST.DISCONTINUEDATE,
            CAMP_OFF.EMPLOYEE_NUMBER As CAMP_OFF_NUMB,
            CAMP_OFF.NAME As CAMP_OFF_NAME,
            CASE
                WHEN  CAMP_OFF.EMPLOYEE_NUMBER <> '' THEN CAMP_OFF.EMPLOYEE_NUMBER||'@nwu.ac.za'
                ELSE CAMP_OFF.EMAIL_ADDRESS
            END As CAMP_OFF_MAIL,
            CAMP_OFF.EMAIL_ADDRESS As CAMP_OFF_MAIL2,
            CAMP_SUP.EMPLOYEE_NUMBER As CAMP_SUP_NUMB,
            CAMP_SUP.NAME As CAMP_SUP_NAME,
            CASE
                WHEN CAMP_SUP.EMPLOYEE_NUMBER <> '' THEN CAMP_SUP.EMPLOYEE_NUMBER||'@nwu.ac.za'
                ELSE CAMP_SUP.EMAIL_ADDRESS
            END As CAMP_SUP_MAIL,
            CAMP_SUP.EMAIL_ADDRESS As CAMP_SUP_MAIL2,
            ORG_OFF.EMPLOYEE_NUMBER As ORG_OFF_NUMB,
            ORG_OFF.NAME As ORG_OFF_NAME,
            CASE
                WHEN ORG_OFF.EMPLOYEE_NUMBER <> '' THEN ORG_OFF.EMPLOYEE_NUMBER||'@nwu.ac.za'
                ELSE ORG_OFF.EMAIL_ADDRESS
            END As ORG_OFF_MAIL,
            ORG_OFF.EMAIL_ADDRESS As ORG_OFF_MAIL2,
            ORG_SUP.EMPLOYEE_NUMBER As ORG_SUP_NUMB,
            ORG_SUP.NAME As ORG_SUP_NAME,
            CASE
                WHEN ORG_SUP.EMPLOYEE_NUMBER <> '' THEN ORG_SUP.EMPLOYEE_NUMBER||'@nwu.ac.za'
                ELSE ORG_SUP.EMAIL_ADDRESS
            END As ORG_SUP_MAIL,
            ORG_SUP.EMAIL_ADDRESS As ORG_SUP_MAIL2,
            Case
                When PREV.USER_NAME != '' Then PREV.USER
                Else CAMP_OFF.EMPLOYEE_NUMBER
            End As U_NUMB,
            Case
                When PREV.USER_NAME != '' Then PREV.USER_NAME
                Else CAMP_OFF.NAME
            End As U_NAME, 
            Case
                When PREV.USER_NAME != '' Then PREV.USER||'@nwu.ac.za'
                Else CAMP_OFF.EMAIL_ADDRESS
            End As U_MAIL, 
            PREV.SYSTEM_DESC        
        From
            X021cd_add_previous PREV Left Join
            X021ca_Qual_zerofee_transaction MAST On MAST.KSTUDBUSENTID = PREV.ID And
                MAST.QUALIFICATION = PREV.QUALIFICATION Left Join
            X021cf_officer CAMP_OFF On CAMP_OFF.CAMPUS = PREV.LOC Left Join
            X021cf_officer ORG_OFF On ORG_OFF.CAMPUS = PREV.ORG Left Join
            X021cg_supervisor CAMP_SUP On CAMP_SUP.CAMPUS = PREV.LOC Left Join
            X021cg_supervisor ORG_SUP On ORG_SUP.CAMPUS = PREV.ORG
        Where
            PREV.PREV_PROCESS Is Null Or
            PREV.DATE_REPORTED > PREV.PREV_DATE_RETEST And PREV.REMARK = ""
        ;"""
        """
        WHEN CAMP_OFF.NAME != '' THEN CAMP_OFF.NAME 
        """
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD THE FINAL TABLE FOR EXPORT AND REPORT
    sr_file = "X021cx_Qual_zerofee_transaction"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if l_debug:
        print("Build the final report")
    if i_finding_before > 0 and i_finding_after > 0:
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            'QUALIFICATION FEE ZERO VALUE TRANSACTION' As Audit_finding,
            FIND.ORG As 'Organization',
            FIND.LOC As 'Campus',
            FIND.ID As 'Student',
            FIND.QUALIFICATION As 'Qualification',
            FIND.QUALIFICATION_NAME As 'Qualification_name',
            FIND.FEE_LEVIED As 'Fee_levied',
            FIND.FEE_SHOULD_BE As 'Fee_should_be',
            FIND.FEE_MODE As 'Qual_fee',
            FIND.DATEENROL As 'Date_enrol',
            FIND.RESULTPASSDATE As 'Date_pass',
            FIND.DISCONTINUEDATE As 'Date_discontinue',
            FIND.CAMP_OFF_NAME AS Responsible_Officer,
            FIND.CAMP_OFF_NUMB AS Responsible_Officer_Numb,
            FIND.CAMP_OFF_MAIL AS Responsible_Officer_Mail,
            FIND.CAMP_SUP_NAME AS Supervisor,
            FIND.CAMP_SUP_NUMB AS Supervisor_Numb,
            FIND.CAMP_SUP_MAIL AS Supervisor_Mail,
            FIND.ORG_OFF_NAME AS Org_Officer,
            FIND.ORG_OFF_NUMB AS Org_Officer_Numb,
            FIND.ORG_OFF_MAIL AS Org_Officer_Mail,
            FIND.ORG_SUP_NAME AS Org_Supervisor,
            FIND.ORG_SUP_NUMB AS Org_Supervisor_Numb,
            FIND.ORG_SUP_MAIL AS Org_Supervisor_Mail,
            FIND.U_NAME As User,
            FIND.U_NUMB As User_Numb,
            FIND.U_MAIL As User_Mail
        From
            X021ch_detail FIND
        Order by
            FIND.LOC,
            FIND.QUALIFICATION,
            FIND.FEE_SHOULD_BE,
            FIND.ID
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)
        # Export findings
        if l_export and funcsys.tablerowcount(so_curs, sr_file) > 0:
            if l_debug:
                print("Export findings...")
            sx_path = re_path + "/"
            sx_file = "Student_fee_test_021cx_qual_fee_zero_transaction_"
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

    """*****************************************************************************
    QUALIFICATION FEE TEST HALF TRANSACTION CONTACT
    *****************************************************************************"""
    if l_debug:
        print("QUALIFICATION FEE TEST HALF TRANSACTION CONTACT")
    funcfile.writelog("QUALIFICATION FEE TEST HALF TRANSACTION CONTACT")

    # FILES NEEDED
    # X020ba_Student_master

    # DECLARE VARIABLES
    i_finding_after: int = 0
    s_desc = "Qualification fee half levied for contact student"

    # ISOLATE QUALIFICATIONS WITH HALF TRANSACTIONS - CONTACT STUDENTS ONLY
    if l_debug:
        print("Isolate qualifications with half value transactions...")
    sr_file = "X021ea_Qual_halffee_transaction"
    s_sql = "Create table " + sr_file + " AS" + """
    Select
        STUD.*
    From
        X020bx_Student_master_sort STUD
    Where
        STUD.VALID = 0 And
        STUD.FEE_LEVIED_TYPE Like ('4%') And
        STUD.FEE_COUNT_HALF < 2 And
        STUD.FEE_SHOULD_BE Like ('% C%')        
    Order By
        STUD.CAMPUS,
        STUD.FEE_SHOULD_BE,
        STUD.KSTUDBUSENTID
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD TABLE: " + sr_file)
    so_conn.commit()

    # IDENTIFY FINDINGS
    if l_debug:
        print("Identify findings...")
    sr_file = "X021eb_findings"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        'NWU' As ORG,
        FIND.CAMPUS As LOC,
        FIND.KSTUDBUSENTID As ID,
        FIND.QUALIFICATION,
        FIND.FEE_LEVIED,
        FIND.FUSERBUSINESSENTITYID As USER,
        FIND.NAME_ADDR As USER_NAME,
        FIND.SYSTEM_DESC    
    From
        X021ea_Qual_halffee_transaction FIND
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # COUNT THE NUMBER OF FINDINGS
    i_finding_before: int = funcsys.tablerowcount(so_curs, sr_file)
    if l_debug:
        print("*** Found " + str(i_finding_before) + " exceptions ***")
    funcfile.writelog("%t FINDING: " + str(i_finding_before) + " QUALIFICATION HALF FEE TRAN finding(s)")

    # GET PREVIOUS FINDINGS
    sr_file = "X021ec_get_previous"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0:
        if l_debug:
            print("Import previously reported findings...")
        so_curs.execute(
            "CREATE TABLE " + sr_file + """
            (PROCESS TEXT,
            FIELD1 INT,
            FIELD2 TEXT,
            FIELD3 REAL,
            FIELD4 TEXT,
            FIELD5 TEXT,
            DATE_REPORTED TEXT,
            DATE_RETEST TEXT,
            REMARK TEXT)
            """)
        co = open(ed_path + "302_reported.txt", "r")
        co_reader = csv.reader(co)
        # Read the COLUMN database data
        for row in co_reader:
            # Populate the column variables
            if row[0] == "PROCESS":
                continue
            elif row[0] != "qualification half transaction":
                continue
            else:
                s_cols = "INSERT INTO " + sr_file + " VALUES('" + row[0] + "','" + row[1] + "','" + row[2] + "','" + \
                         row[
                             3] + "','" + row[4] + "','" + row[5] + "','" + row[6] + "','" + row[7] + "','" + row[
                             8] + "')"
                so_curs.execute(s_cols)
        so_conn.commit()
        # Close the imported data file
        co.close()
        funcfile.writelog("%t IMPORT TABLE: " + ed_path + "302_reported.txt (" + sr_file + ")")

    # SET PREVIOUS FINDINGS
    sr_file = "X021ec_set_previous"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0:
        if l_debug:
            print("Obtain the latest previous finding...")
        s_sql = "Create Table " + sr_file + " As" + """
        Select
            GET.PROCESS,
            GET.FIELD1,
            GET.FIELD2,
            GET.FIELD3,
            GET.FIELD4,
            GET.FIELD5,
            Max(GET.DATE_REPORTED) As DATE_REPORTED,
            GET.DATE_RETEST,
            GET.REMARK
        From
            X021ec_get_previous GET
        Group By
            GET.FIELD1,
            GET.FIELD2,
            GET.FIELD3        
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # ADD PREVIOUS FINDINGS
    sr_file = "X021ed_add_previous"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0:
        if l_debug:
            print("Join previously reported to current findings...")
        s_sql = "CREATE TABLE " + sr_file + " AS" + """
        Select
            FIND.*,
            'qualification half transaction' AS PROCESS,
            '%TODAY%' AS DATE_REPORTED,
            '%DAYS%' AS DATE_RETEST,
            PREV.PROCESS AS PREV_PROCESS,
            PREV.DATE_REPORTED AS PREV_DATE_REPORTED,
            PREV.DATE_RETEST AS PREV_DATE_RETEST,
            PREV.REMARK
        From
            X021eb_findings FIND Left Join
            X021ec_set_previous PREV ON PREV.FIELD1 = FIND.ID And
                PREV.FIELD2 = FIND.QUALIFICATION And
                PREV.FIELD3 = FIND.FEE_LEVIED
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        s_sql = s_sql.replace("%TODAY%", funcdatn.get_today_date())
        s_sql = s_sql.replace("%DAYS%", funcdatn.get_current_month_end_next())
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD LIST TO UPDATE FINDINGS
    sr_file = "X021ee_new_previous"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0:
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            PREV.PROCESS,
            PREV.ID AS FIELD1,
            PREV.QUALIFICATION AS FIELD2,
            PREV.FEE_LEVIED AS FIELD3,
            '' AS FIELD4,
            '' AS FIELD5,
            PREV.DATE_REPORTED,
            PREV.DATE_RETEST,
            PREV.REMARK
        From
            X021ed_add_previous PREV
        Where
            PREV.PREV_PROCESS Is Null Or
            PREV.DATE_REPORTED > PREV.PREV_DATE_RETEST And PREV.REMARK = ""
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)
        # Export findings to previous reported file
        i_finding_after = funcsys.tablerowcount(so_curs, sr_file)
        if i_finding_after > 0:
            if l_debug:
                print("*** " + str(i_finding_after) + " Finding(s) to report ***")
            sx_path = ed_path
            sx_file = "302_reported"
            # Read the header data
            s_head = funccsv.get_colnames_sqlite(so_conn, sr_file)
            # Write the data
            if l_record:
                funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file, s_head, "a", ".txt")
                funcfile.writelog("%t FINDING: " + str(i_finding_after) + " new finding(s) to export")
                funcfile.writelog("%t EXPORT DATA: " + sr_file)
            if l_mess:
                funcsms.send_telegram('', 'administrator',
                                      '<b>' + str(i_finding_before) + '/' + str(i_finding_after) + '</b> ' + s_desc)
        else:
            if l_debug:
                print("*** No new findings to report ***")
            funcfile.writelog("%t FINDING: No new findings to export")

    # IMPORT OFFICERS FOR MAIL REPORTING PURPOSES
    sr_file = "X021ef_officer"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0:
        if i_finding_after > 0:
            if l_debug:
                print("Import reporting officers for mail purposes...")
            s_sql = "CREATE TABLE " + sr_file + " AS " + """
            Select
                OFFICER.LOOKUP,
                Upper(OFFICER.LOOKUP_CODE) AS CAMPUS,
                OFFICER.LOOKUP_DESCRIPTION AS EMPLOYEE_NUMBER,
                PEOP.NAME_ADDR As NAME,
                PEOP.EMAIL_ADDRESS
            From
                VSS.X000_OWN_LOOKUPS OFFICER Left Join
                PEOPLE.X002_PEOPLE_CURR PEOP ON PEOP.EMPLOYEE_NUMBER = OFFICER.LOOKUP_DESCRIPTION
            Where
                OFFICER.LOOKUP = 'stud_fee_test_qual_half_fee_transaction_officer'
            ;"""
            so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
            so_curs.execute(s_sql)
            so_conn.commit()
            funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # IMPORT SUPERVISORS FOR MAIL REPORTING PURPOSES
    sr_file = "X021eg_supervisor"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0 and i_finding_after > 0:
        if l_debug:
            print("Import reporting supervisors for mail purposes...")
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            SUPERVISOR.LOOKUP,
            Upper(SUPERVISOR.LOOKUP_CODE) AS CAMPUS,
            SUPERVISOR.LOOKUP_DESCRIPTION AS EMPLOYEE_NUMBER,
            PEOP.NAME_ADDR As NAME,
            PEOP.EMAIL_ADDRESS
        From
            VSS.X000_OWN_LOOKUPS SUPERVISOR Left Join
            PEOPLE.X002_PEOPLE_CURR PEOP ON PEOP.EMPLOYEE_NUMBER = SUPERVISOR.LOOKUP_DESCRIPTION
        Where
            SUPERVISOR.LOOKUP = 'stud_fee_test_qual_half_fee_transaction_supervisor'
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # ADD CONTACT DETAILS TO FINDINGS
    sr_file = "X021eh_detail"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0 and i_finding_after > 0:
        if l_debug:
            print("Add contact details to findings...")
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            PREV.ORG,
            PREV.LOC,
            PREV.ID,
            PREV.QUALIFICATION,
            MAST.QUALIFICATION_NAME,
            MAST.FEE_LEVIED,
            MAST.FEE_SHOULD_BE,
            MAST.FEE_MODE,
            MAST.DATEENROL,
            MAST.RESULTPASSDATE,
            MAST.DISCONTINUEDATE,
            CAMP_OFF.EMPLOYEE_NUMBER As CAMP_OFF_NUMB,
            CAMP_OFF.NAME As CAMP_OFF_NAME,
            CASE
                WHEN  CAMP_OFF.EMPLOYEE_NUMBER <> '' THEN CAMP_OFF.EMPLOYEE_NUMBER||'@nwu.ac.za'
                ELSE CAMP_OFF.EMAIL_ADDRESS
            END As CAMP_OFF_MAIL,
            CAMP_OFF.EMAIL_ADDRESS As CAMP_OFF_MAIL2,
            CAMP_SUP.EMPLOYEE_NUMBER As CAMP_SUP_NUMB,
            CAMP_SUP.NAME As CAMP_SUP_NAME,
            CASE
                WHEN CAMP_SUP.EMPLOYEE_NUMBER <> '' THEN CAMP_SUP.EMPLOYEE_NUMBER||'@nwu.ac.za'
                ELSE CAMP_SUP.EMAIL_ADDRESS
            END As CAMP_SUP_MAIL,
            CAMP_SUP.EMAIL_ADDRESS As CAMP_SUP_MAIL2,
            ORG_OFF.EMPLOYEE_NUMBER As ORG_OFF_NUMB,
            ORG_OFF.NAME As ORG_OFF_NAME,
            CASE
                WHEN ORG_OFF.EMPLOYEE_NUMBER <> '' THEN ORG_OFF.EMPLOYEE_NUMBER||'@nwu.ac.za'
                ELSE ORG_OFF.EMAIL_ADDRESS
            END As ORG_OFF_MAIL,
            ORG_OFF.EMAIL_ADDRESS As ORG_OFF_MAIL2,
            ORG_SUP.EMPLOYEE_NUMBER As ORG_SUP_NUMB,
            ORG_SUP.NAME As ORG_SUP_NAME,
            CASE
                WHEN ORG_SUP.EMPLOYEE_NUMBER <> '' THEN ORG_SUP.EMPLOYEE_NUMBER||'@nwu.ac.za'
                ELSE ORG_SUP.EMAIL_ADDRESS
            END As ORG_SUP_MAIL,
            ORG_SUP.EMAIL_ADDRESS As ORG_SUP_MAIL2,
            Case
                When PREV.USER_NAME != '' Then PREV.USER
                Else CAMP_OFF.EMPLOYEE_NUMBER
            End As U_NUMB,
            Case
                When PREV.USER_NAME != '' Then PREV.USER_NAME
                Else CAMP_OFF.NAME
            End As U_NAME, 
            Case
                When PREV.USER_NAME != '' Then PREV.USER||'@nwu.ac.za'
                Else CAMP_OFF.EMAIL_ADDRESS
            End As U_MAIL, 
            PREV.SYSTEM_DESC          
        From
            X021ed_add_previous PREV Left Join
            X021ea_Qual_halffee_transaction MAST On MAST.KSTUDBUSENTID = PREV.ID And
                MAST.QUALIFICATION = PREV.QUALIFICATION Left Join
            X021ef_officer CAMP_OFF On CAMP_OFF.CAMPUS = PREV.LOC Left Join
            X021ef_officer ORG_OFF On ORG_OFF.CAMPUS = PREV.ORG Left Join
            X021eg_supervisor CAMP_SUP On CAMP_SUP.CAMPUS = PREV.LOC Left Join
            X021eg_supervisor ORG_SUP On ORG_SUP.CAMPUS = PREV.ORG
        Where
            PREV.PREV_PROCESS Is Null Or
            PREV.DATE_REPORTED > PREV.PREV_DATE_RETEST And PREV.REMARK = ""
        ;"""
        """
        WHEN CAMP_OFF.NAME != '' THEN CAMP_OFF.NAME 
        """
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD THE FINAL TABLE FOR EXPORT AND REPORT
    sr_file = "X021ex_Qual_halffee_transaction"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if l_debug:
        print("Build the final report")
    if i_finding_before > 0 and i_finding_after > 0:
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            'QUALIFICATION FEE HALF VALUE TRANSACTION' As Audit_finding,
            FIND.ORG As 'Organization',
            FIND.LOC As 'Campus',
            FIND.ID As 'Student',
            FIND.QUALIFICATION As 'Qualification',
            FIND.QUALIFICATION_NAME As 'Qualification_name',
            FIND.FEE_LEVIED As 'Fee_levied',
            FIND.FEE_SHOULD_BE As 'Fee_should_be',
            FIND.FEE_MODE As 'Qual_fee',
            FIND.DATEENROL As 'Date_enrol',
            FIND.RESULTPASSDATE As 'Date_pass',
            FIND.DISCONTINUEDATE As 'Date_discontinue',
            FIND.CAMP_OFF_NAME AS Responsible_Officer,
            FIND.CAMP_OFF_NUMB AS Responsible_Officer_Numb,
            FIND.CAMP_OFF_MAIL AS Responsible_Officer_Mail,
            FIND.CAMP_SUP_NAME AS Supervisor,
            FIND.CAMP_SUP_NUMB AS Supervisor_Numb,
            FIND.CAMP_SUP_MAIL AS Supervisor_Mail,
            FIND.ORG_OFF_NAME AS Org_Officer,
            FIND.ORG_OFF_NUMB AS Org_Officer_Numb,
            FIND.ORG_OFF_MAIL AS Org_Officer_Mail,
            FIND.ORG_SUP_NAME AS Org_Supervisor,
            FIND.ORG_SUP_NUMB AS Org_Supervisor_Numb,
            FIND.ORG_SUP_MAIL AS Org_Supervisor_Mail,
            FIND.U_NAME As User,
            FIND.U_NUMB As User_Numb,
            FIND.U_MAIL As User_Mail        
        From
            X021eh_detail FIND
        Order by
            FIND.LOC,
            FIND.QUALIFICATION,
            FIND.FEE_SHOULD_BE,
            FIND.ID
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)
        # Export findings
        if l_export and funcsys.tablerowcount(so_curs, sr_file) > 0:
            if l_debug:
                print("Export findings...")
            sx_path = re_path + "/"
            sx_file = "Student_fee_test_021ex_qual_fee_half_transaction_"
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

    """*****************************************************************************
    QUALIFICATION FEE TEST ABNORMAL TRANSACTION CONTACT
    *****************************************************************************"""
    if l_debug:
        print("QUALIFICATION FEE TEST ABNORMAL TRANSACTION CONTACT")
    funcfile.writelog("QUALIFICATION FEE TEST ABNORMAL TRANSACTION CONTACT")

    # FILES NEEDED
    # X020ba_Student_master

    # DECLARE VARIABLES
    i_finding_after: int = 0
    s_desc = "Qualification fee abnormal amount for contact student"

    # ISOLATE QUALIFICATIONS WITH ABNORMAL TRANSACTIONS - CONTACT STUDENTS ONLY
    if l_debug:
        print("Isolate qualifications with abnormal value transactions...")
    sr_file = "X021fa_Qual_abnormalfee_transaction"
    s_sql = "Create table " + sr_file + " AS" + """
    Select
        STUD.*
    From
        X020bx_Student_master_sort STUD
    Where
        STUD.VALID = 0 And
        STUD.FEE_LEVIED_TYPE Like ('6%') And
        STUD.FEE_SHOULD_BE Like ('% C%')        
    Order By
        STUD.CAMPUS,
        STUD.FEE_SHOULD_BE,
        STUD.KSTUDBUSENTID
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD TABLE: " + sr_file)
    so_conn.commit()
    if l_export and funcsys.tablerowcount(so_curs, sr_file) > 0:
        if l_debug:
            print("Export findings...")
        sx_path = re_path + "/"
        sx_file = "Student_fee_test_021fx_qual_fee_abnormal_transaction_studentlist_"
        sx_file_dated = sx_file + funcdatn.get_today_date_file()
        s_head = funccsv.get_colnames_sqlite(so_conn, sr_file)
        funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file, s_head)
        # funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file_dated, s_head)
        funcfile.writelog("%t EXPORT DATA: " + sx_path + sx_file)

    # IDENTIFY FINDINGS
    if l_debug:
        print("Identify findings...")
    sr_file = "X021fb_findings"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        'NWU' As ORG,
        FIND.CAMPUS As LOC,
        FIND.KSTUDBUSENTID As ID,
        FIND.QUALIFICATION,
        FIND.FEE_LEVIED,
        FIND.FUSERBUSINESSENTITYID As USER,
        FIND.NAME_ADDR As USER_NAME,
        FIND.SYSTEM_DESC       
    From
        X021fa_Qual_abnormalfee_transaction FIND
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # COUNT THE NUMBER OF FINDINGS
    i_finding_before: int = funcsys.tablerowcount(so_curs, sr_file)
    if l_debug:
        print("*** Found " + str(i_finding_before) + " exceptions ***")
    funcfile.writelog("%t FINDING: " + str(i_finding_before) + " QUALIFICATION ABNORMAL FEE TRAN finding(s)")

    # GET PREVIOUS FINDINGS
    sr_file = "X021fc_get_previous"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0:
        if l_debug:
            print("Import previously reported findings...")
        so_curs.execute(
            "CREATE TABLE " + sr_file + """
            (PROCESS TEXT,
            FIELD1 INT,
            FIELD2 TEXT,
            FIELD3 REAL,
            FIELD4 TEXT,
            FIELD5 TEXT,
            DATE_REPORTED TEXT,
            DATE_RETEST TEXT,
            REMARK TEXT)
            """)
        co = open(ed_path + "302_reported.txt", "r")
        co_reader = csv.reader(co)
        # Read the COLUMN database data
        for row in co_reader:
            # Populate the column variables
            if row[0] == "PROCESS":
                continue
            elif row[0] != "qualification abnormal transaction":
                continue
            else:
                s_cols = "INSERT INTO " + sr_file + " VALUES('" + row[0] + "','" + row[1] + "','" + row[2] + "','" + \
                         row[
                             3] + "','" + row[4] + "','" + row[5] + "','" + row[6] + "','" + row[7] + "','" + row[
                             8] + "')"
                so_curs.execute(s_cols)
        so_conn.commit()
        # Close the imported data file
        co.close()
        funcfile.writelog("%t IMPORT TABLE: " + ed_path + "302_reported.txt (" + sr_file + ")")

    # SET PREVIOUS FINDINGS
    sr_file = "X021fc_set_previous"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0:
        if l_debug:
            print("Obtain the latest previous finding...")
        s_sql = "Create Table " + sr_file + " As" + """
        Select
            GET.PROCESS,
            GET.FIELD1,
            GET.FIELD2,
            GET.FIELD3,
            GET.FIELD4,
            GET.FIELD5,
            Max(GET.DATE_REPORTED) As DATE_REPORTED,
            GET.DATE_RETEST,
            GET.REMARK
        From
            X021fc_get_previous GET
        Group By
            GET.FIELD1,
            GET.FIELD2,
            GET.FIELD3        
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # ADD PREVIOUS FINDINGS
    sr_file = "X021fd_add_previous"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0:
        if l_debug:
            print("Join previously reported to current findings...")
        s_sql = "CREATE TABLE " + sr_file + " AS" + """
        Select
            FIND.*,
            'qualification abnormal transaction' AS PROCESS,
            '%TODAY%' AS DATE_REPORTED,
            '%DAYS%' AS DATE_RETEST,
            PREV.PROCESS AS PREV_PROCESS,
            PREV.DATE_REPORTED AS PREV_DATE_REPORTED,
            PREV.DATE_RETEST AS PREV_DATE_RETEST,
            PREV.REMARK
        From
            X021fb_findings FIND Left Join
            X021fc_set_previous PREV ON PREV.FIELD1 = FIND.ID And
                PREV.FIELD2 = FIND.QUALIFICATION And
                PREV.FIELD3 = FIND.FEE_LEVIED
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        s_sql = s_sql.replace("%TODAY%", funcdatn.get_today_date())
        s_sql = s_sql.replace("%DAYS%", funcdatn.get_current_month_end_next())
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD LIST TO UPDATE FINDINGS
    sr_file = "X021fe_new_previous"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0:
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            PREV.PROCESS,
            PREV.ID AS FIELD1,
            PREV.QUALIFICATION AS FIELD2,
            PREV.FEE_LEVIED AS FIELD3,
            '' AS FIELD4,
            '' AS FIELD5,
            PREV.DATE_REPORTED,
            PREV.DATE_RETEST,
            PREV.REMARK
        From
            X021fd_add_previous PREV
        Where
            PREV.PREV_PROCESS Is Null Or
            PREV.DATE_REPORTED > PREV.PREV_DATE_RETEST And PREV.REMARK = ""
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)
        # Export findings to previous reported file
        i_finding_after = funcsys.tablerowcount(so_curs, sr_file)
        if i_finding_after > 0:
            if l_debug:
                print("*** " + str(i_finding_after) + " Finding(s) to report ***")
            sx_path = ed_path
            sx_file = "302_reported"
            # Read the header data
            s_head = funccsv.get_colnames_sqlite(so_conn, sr_file)
            # Write the data
            if l_record:
                funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file, s_head, "a", ".txt")
                funcfile.writelog("%t FINDING: " + str(i_finding_after) + " new finding(s) to export")
                funcfile.writelog("%t EXPORT DATA: " + sr_file)
            if l_mess:
                funcsms.send_telegram('', 'administrator',
                                      '<b>' + str(i_finding_before) + '/' + str(i_finding_after) + '</b> ' + s_desc)
        else:
            if l_debug:
                print("*** No new findings to report ***")
            funcfile.writelog("%t FINDING: No new findings to export")

    # IMPORT OFFICERS FOR MAIL REPORTING PURPOSES
    sr_file = "X021ff_officer"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0:
        if i_finding_after > 0:
            if l_debug:
                print("Import reporting officers for mail purposes...")
            s_sql = "CREATE TABLE " + sr_file + " AS " + """
            Select
                OFFICER.LOOKUP,
                Upper(OFFICER.LOOKUP_CODE) AS CAMPUS,
                OFFICER.LOOKUP_DESCRIPTION AS EMPLOYEE_NUMBER,
                PEOP.NAME_ADDR As NAME,
                PEOP.EMAIL_ADDRESS
            From
                VSS.X000_OWN_LOOKUPS OFFICER Left Join
                PEOPLE.X002_PEOPLE_CURR PEOP ON PEOP.EMPLOYEE_NUMBER = OFFICER.LOOKUP_DESCRIPTION
            Where
                OFFICER.LOOKUP = 'stud_fee_test_qual_abnormal_fee_transaction_officer'
            ;"""
            so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
            so_curs.execute(s_sql)
            so_conn.commit()
            funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # IMPORT SUPERVISORS FOR MAIL REPORTING PURPOSES
    sr_file = "X021fg_supervisor"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0 and i_finding_after > 0:
        if l_debug:
            print("Import reporting supervisors for mail purposes...")
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            SUPERVISOR.LOOKUP,
            Upper(SUPERVISOR.LOOKUP_CODE) AS CAMPUS,
            SUPERVISOR.LOOKUP_DESCRIPTION AS EMPLOYEE_NUMBER,
            PEOP.NAME_ADDR As NAME,
            PEOP.EMAIL_ADDRESS
        From
            VSS.X000_OWN_LOOKUPS SUPERVISOR Left Join
            PEOPLE.X002_PEOPLE_CURR PEOP ON PEOP.EMPLOYEE_NUMBER = SUPERVISOR.LOOKUP_DESCRIPTION
        Where
            SUPERVISOR.LOOKUP = 'stud_fee_test_qual_abnormal_fee_transaction_supervisor'
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # ADD CONTACT DETAILS TO FINDINGS
    sr_file = "X021fh_detail"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0 and i_finding_after > 0:
        if l_debug:
            print("Add contact details to findings...")
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            PREV.ORG,
            PREV.LOC,
            PREV.ID,
            PREV.QUALIFICATION,
            MAST.QUALIFICATION_NAME,
            MAST.FEE_LEVIED,
            MAST.FEE_SHOULD_BE,
            MAST.FEE_MODE,
            MAST.DATEENROL,
            MAST.RESULTPASSDATE,
            MAST.DISCONTINUEDATE,
            CAMP_OFF.EMPLOYEE_NUMBER As CAMP_OFF_NUMB,
            CAMP_OFF.NAME As CAMP_OFF_NAME,
            CASE
                WHEN  CAMP_OFF.EMPLOYEE_NUMBER <> '' THEN CAMP_OFF.EMPLOYEE_NUMBER||'@nwu.ac.za'
                ELSE CAMP_OFF.EMAIL_ADDRESS
            END As CAMP_OFF_MAIL,
            CAMP_OFF.EMAIL_ADDRESS As CAMP_OFF_MAIL2,
            CAMP_SUP.EMPLOYEE_NUMBER As CAMP_SUP_NUMB,
            CAMP_SUP.NAME As CAMP_SUP_NAME,
            CASE
                WHEN CAMP_SUP.EMPLOYEE_NUMBER <> '' THEN CAMP_SUP.EMPLOYEE_NUMBER||'@nwu.ac.za'
                ELSE CAMP_SUP.EMAIL_ADDRESS
            END As CAMP_SUP_MAIL,
            CAMP_SUP.EMAIL_ADDRESS As CAMP_SUP_MAIL2,
            ORG_OFF.EMPLOYEE_NUMBER As ORG_OFF_NUMB,
            ORG_OFF.NAME As ORG_OFF_NAME,
            CASE
                WHEN ORG_OFF.EMPLOYEE_NUMBER <> '' THEN ORG_OFF.EMPLOYEE_NUMBER||'@nwu.ac.za'
                ELSE ORG_OFF.EMAIL_ADDRESS
            END As ORG_OFF_MAIL,
            ORG_OFF.EMAIL_ADDRESS As ORG_OFF_MAIL2,
            ORG_SUP.EMPLOYEE_NUMBER As ORG_SUP_NUMB,
            ORG_SUP.NAME As ORG_SUP_NAME,
            CASE
                WHEN ORG_SUP.EMPLOYEE_NUMBER <> '' THEN ORG_SUP.EMPLOYEE_NUMBER||'@nwu.ac.za'
                ELSE ORG_SUP.EMAIL_ADDRESS
            END As ORG_SUP_MAIL,
            ORG_SUP.EMAIL_ADDRESS As ORG_SUP_MAIL2,
            Case
                When PREV.USER_NAME != '' Then PREV.USER
                Else CAMP_OFF.EMPLOYEE_NUMBER
            End As U_NUMB,
            Case
                When PREV.USER_NAME != '' Then PREV.USER_NAME
                Else CAMP_OFF.NAME
            End As U_NAME, 
            Case
                When PREV.USER_NAME != '' Then PREV.USER||'@nwu.ac.za'
                Else CAMP_OFF.EMAIL_ADDRESS
            End As U_MAIL, 
            PREV.SYSTEM_DESC          
        From
            X021fd_add_previous PREV Left Join
            X021fa_Qual_abnormalfee_transaction MAST On MAST.KSTUDBUSENTID = PREV.ID And
                MAST.QUALIFICATION = PREV.QUALIFICATION Left Join
            X021ff_officer CAMP_OFF On CAMP_OFF.CAMPUS = PREV.LOC Left Join
            X021ff_officer ORG_OFF On ORG_OFF.CAMPUS = PREV.ORG Left Join
            X021fg_supervisor CAMP_SUP On CAMP_SUP.CAMPUS = PREV.LOC Left Join
            X021fg_supervisor ORG_SUP On ORG_SUP.CAMPUS = PREV.ORG
        Where
            PREV.PREV_PROCESS Is Null Or
            PREV.DATE_REPORTED > PREV.PREV_DATE_RETEST And PREV.REMARK = ""
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD THE FINAL TABLE FOR EXPORT AND REPORT
    sr_file = "X021fx_Qual_abnormalfee_transaction"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if l_debug:
        print("Build the final report")
    if i_finding_before > 0 and i_finding_after > 0:
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            'QUALIFICATION FEE ABNORMAL VALUE TRANSACTION' As Audit_finding,
            FIND.ORG As 'Organization',
            FIND.LOC As 'Campus',
            FIND.ID As 'Student',
            FIND.QUALIFICATION As 'Qualification',
            FIND.QUALIFICATION_NAME As 'Qualification_name',
            FIND.FEE_LEVIED As 'Fee_levied',
            FIND.FEE_SHOULD_BE As 'Fee_should_be',
            FIND.FEE_MODE As 'Qual_fee',
            FIND.DATEENROL As 'Date_enrol',
            FIND.RESULTPASSDATE As 'Date_pass',
            FIND.DISCONTINUEDATE As 'Date_discontinue',
            FIND.CAMP_OFF_NAME AS Responsible_Officer,
            FIND.CAMP_OFF_NUMB AS Responsible_Officer_Numb,
            FIND.CAMP_OFF_MAIL AS Responsible_Officer_Mail,
            FIND.CAMP_SUP_NAME AS Supervisor,
            FIND.CAMP_SUP_NUMB AS Supervisor_Numb,
            FIND.CAMP_SUP_MAIL AS Supervisor_Mail,
            FIND.ORG_OFF_NAME AS Org_Officer,
            FIND.ORG_OFF_NUMB AS Org_Officer_Numb,
            FIND.ORG_OFF_MAIL AS Org_Officer_Mail,
            FIND.ORG_SUP_NAME AS Org_Supervisor,
            FIND.ORG_SUP_NUMB AS Org_Supervisor_Numb,
            FIND.ORG_SUP_MAIL AS Org_Supervisor_Mail,
            FIND.U_NAME As User,
            FIND.U_NUMB As User_Numb,
            FIND.U_MAIL As User_Mail        
        From
            X021fh_detail FIND
        Order by
            FIND.LOC,
            FIND.QUALIFICATION,
            FIND.FEE_SHOULD_BE,
            FIND.ID
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)
        # Export findings
        if l_export and funcsys.tablerowcount(so_curs, sr_file) > 0:
            if l_debug:
                print("Export findings...")
            sx_path = re_path + "/"
            sx_file = "Student_fee_test_021fx_qual_fee_abnormal_transaction_"
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

    """*****************************************************************************
    QUALIFICATION FEE TEST OVERCHARGE CONTACT
    *****************************************************************************"""
    if l_debug:
        print("QUALIFICATION FEE TEST OVERCHARGE CONTACT")
    funcfile.writelog("QUALIFICATION FEE TEST OVERCHARGE CONTACT")

    # FILES NEEDED
    # X020bx_Student_master_sort

    # DECLARE TEST VARIABLES
    s_fprefix: str = "X021g"
    s_finding: str = "QUALIFICATION FEE OVERCHARGE"
    s_xfile: str = "302_reported.txt"
    i_finding_before: int = 0
    i_finding_after: int = 0
    s_desc = "Student account overcharged with qualification fee"

    # OBTAIN TEST DATA
    if l_debug:
        print("Obtain test data...")
    sr_file: str = s_fprefix + "a_qual_fee_overcharge"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            STUD.*
        From
            X020bx_Student_master_sort STUD
        Where
            STUD.VALID = 2 And
            STUD.FEE_SHOULD_BE Like ('% C%')        
        Order By
            STUD.CAMPUS,
            STUD.FEE_SHOULD_BE,
            STUD.KSTUDBUSENTID
    ;"""
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)
    if l_export and funcsys.tablerowcount(so_curs, sr_file) > 0:
        if l_debug:
            print("Export findings...")
        sx_path = re_path + "/"
        sx_file = "Student_fee_test_" + s_fprefix + "_" + s_finding + "_studentlist_"
        sx_file_dated = sx_file + funcdatn.get_today_date_file()
        s_head = funccsv.get_colnames_sqlite(so_conn, sr_file)
        funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file, s_head)
        # funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file_dated, s_head)
        funcfile.writelog("%t EXPORT DATA: " + sx_path + sx_file)

    # SELECT TEST DATA
    if l_debug:
        print("Identify findings...")
    sr_file = s_fprefix + "b_finding"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        'NWU' As ORG,
        FIND.CAMPUS As LOC,
        FIND.KSTUDBUSENTID As ID,
        FIND.QUALIFICATION,
        FIND.FEE_LEVIED
    From
        %FILEP%a_qual_fee_overcharge FIND
    Order by
        LOC,
        QUALIFICATION,
        ID   
    ;"""
    s_sql = s_sql.replace("%FILEP%", s_fprefix)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # COUNT THE NUMBER OF FINDINGS
    i_finding_before: int = funcsys.tablerowcount(so_curs, sr_file)
    if l_debug:
        print("*** Found " + str(i_finding_before) + " exceptions ***")
    funcfile.writelog("%t FINDING: " + str(i_finding_before) + " " + s_finding + " finding(s)")

    # GET PREVIOUS FINDINGS
    if i_finding_before > 0:
        i = functest.get_previous_finding(so_curs, ed_path, s_xfile, s_finding, "ITTRT")
        so_conn.commit()

    # SET PREVIOUS FINDINGS
    if i_finding_before > 0:
        i = functest.set_previous_finding(so_curs)
        so_conn.commit()

    # ADD PREVIOUS FINDINGS
    sr_file = s_fprefix + "d_addprev"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0:
        if l_debug:
            print("Join previously reported to current findings...")
        s_sql = "CREATE TABLE " + sr_file + " AS" + """
        Select
            FIND.*,
            Lower('%FINDING%') AS PROCESS,
            '%TODAY%' AS DATE_REPORTED,
            '%DAYS%' AS DATE_RETEST,
            PREV.PROCESS AS PREV_PROCESS,
            PREV.DATE_REPORTED AS PREV_DATE_REPORTED,
            PREV.DATE_RETEST AS PREV_DATE_RETEST,
            PREV.REMARK
        From
            %FILEP%b_finding FIND Left Join
            Z001ab_setprev PREV ON PREV.FIELD1 = FIND.ID And
                PREV.FIELD2 = FIND.LOC And
                PREV.FIELD3 = FIND.QUALIFICATION And
                PREV.FIELD4 = FIND.FEE_LEVIED
        ;"""
        s_sql = s_sql.replace("%FINDING%", s_finding)
        s_sql = s_sql.replace("%FILEP%", s_fprefix)
        s_sql = s_sql.replace("%TODAY%", funcdatn.get_today_date())
        s_sql = s_sql.replace("%DAYS%", funcdatn.get_current_month_end_next())
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD LIST TO UPDATE FINDINGS
    sr_file = s_fprefix + "e_newprev"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0:
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            PREV.PROCESS,
            PREV.ID AS FIELD1,
            PREV.LOC AS FIELD2,
            PREV.QUALIFICATION AS FIELD3,
            PREV.FEE_LEVIED AS FIELD4,
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
        s_sql = s_sql.replace("%FILEP%", s_fprefix)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)
        # Export findings to previous reported file
        i_finding_after = funcsys.tablerowcount(so_curs, sr_file)
        if i_finding_after > 0:
            if l_debug:
                print("*** " + str(i_finding_after) + " Finding(s) to report ***")
            sx_path = ed_path
            sx_file = s_xfile[:-4]
            # Read the header data
            s_head = funccsv.get_colnames_sqlite(so_conn, sr_file)
            # Write the data
            if l_record:
                funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file, s_head, "a", ".txt")
                funcfile.writelog("%t FINDING: " + str(i_finding_after) + " new finding(s) to export")
                funcfile.writelog("%t EXPORT DATA: " + sr_file)
            if l_mess:
                funcsms.send_telegram('', 'administrator',
                                      '<b>' + str(i_finding_before) + '/' + str(i_finding_after) + '</b> ' + s_desc)
        else:
            if l_debug:
                print("*** No new findings to report ***")
            funcfile.writelog("%t FINDING: No new findings to export")

    # IMPORT OFFICERS FOR MAIL REPORTING PURPOSES
    if i_finding_before > 0 and i_finding_after > 0:
        i = functest.get_officer(so_curs, "VSS", "TEST " + s_finding + " OFFICER")
        so_conn.commit()

    # IMPORT SUPERVISORS FOR MAIL REPORTING PURPOSES
    if i_finding_before > 0 and i_finding_after > 0:
        i = functest.get_supervisor(so_curs, "VSS", "TEST " + s_finding + " SUPERVISOR")
        so_conn.commit()

    # ADD CONTACT DETAILS TO FINDINGS
    sr_file = s_fprefix + "h_detail"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0 and i_finding_after > 0:
        if l_debug:
            print("Add contact details to findings...")
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            PREV.ORG,
            PREV.LOC,
            PREV.ID,
            PREV.FEE_LEVIED,
            MAST.FEE_LEVIED_TYPE,
            MAST.FEE_SHOULD_BE,
            MAST.FEE_MODE,
            MAST.FEE_MODE_HALF,
            MAST.FEE_BURS,
            PREV.QUALIFICATION,
            MAST.QUALIFICATION_NAME,
            MAST.PRESENT_CAT,
            MAST.ENROL_CAT,
            MAST.SEM1,
            MAST.SEM2,
            MAST.SEM7,        
            CAMP_OFF.EMPLOYEE_NUMBER AS CAMP_OFF_NUMB,
            CAMP_OFF.NAME_ADDR AS CAMP_OFF_NAME,
            CASE
                WHEN  CAMP_OFF.EMPLOYEE_NUMBER != '' THEN CAMP_OFF.EMPLOYEE_NUMBER||'@nwu.ac.za'
                ELSE CAMP_OFF.EMAIL_ADDRESS
            END AS CAMP_OFF_MAIL,
            CAMP_OFF.EMAIL_ADDRESS AS CAMP_OFF_MAIL2,        
            CAMP_SUP.EMPLOYEE_NUMBER AS CAMP_SUP_NUMB,
            CAMP_SUP.NAME_ADDR AS CAMP_SUP_NAME,
            CASE
                WHEN CAMP_SUP.EMPLOYEE_NUMBER != '' THEN CAMP_SUP.EMPLOYEE_NUMBER||'@nwu.ac.za'
                ELSE CAMP_SUP.EMAIL_ADDRESS
            END AS CAMP_SUP_MAIL,
            CAMP_SUP.EMAIL_ADDRESS AS CAMP_SUP_MAIL2,
            ORG_OFF.EMPLOYEE_NUMBER AS ORG_OFF_NUMB,
            ORG_OFF.NAME_ADDR AS ORG_OFF_NAME,
            CASE
                WHEN ORG_OFF.EMPLOYEE_NUMBER != '' THEN ORG_OFF.EMPLOYEE_NUMBER||'@nwu.ac.za'
                ELSE ORG_OFF.EMAIL_ADDRESS
            END AS ORG_OFF_MAIL,
            ORG_OFF.EMAIL_ADDRESS AS ORG_OFF_MAIL2,
            ORG_SUP.EMPLOYEE_NUMBER AS ORG_SUP_NUMB,
            ORG_SUP.NAME_ADDR AS ORG_SUP_NAME,
            CASE
                WHEN ORG_SUP.EMPLOYEE_NUMBER != '' THEN ORG_SUP.EMPLOYEE_NUMBER||'@nwu.ac.za'
                ELSE ORG_SUP.EMAIL_ADDRESS
            END AS ORG_SUP_MAIL,
            ORG_SUP.EMAIL_ADDRESS AS ORG_SUP_MAIL2,
            CASE
                WHEN MAST.NAME_ADDR != '' THEN MAST.FUSERBUSINESSENTITYID
            END AS U_NUMB,
            CASE
                WHEN MAST.NAME_ADDR != '' THEN MAST.NAME_ADDR
                ELSE CAMP_OFF.NAME_ADDR
            END AS U_NAME, 
            CASE
                WHEN MAST.NAME_ADDR != '' THEN MAST.FUSERBUSINESSENTITYID||'@nwu.ac.za'
                WHEN  CAMP_OFF.EMPLOYEE_NUMBER != '' THEN CAMP_OFF.EMPLOYEE_NUMBER||'@nwu.ac.za'
                ELSE CAMP_OFF.EMAIL_ADDRESS
            END AS U_MAIL, 
            MAST.SYSTEM_DESC        
        From
            %FILEP%d_addprev PREV Left Join
            %FILEP%a_qual_fee_overcharge MAST On MAST.KSTUDBUSENTID = PREV.ID And
                MAST.CAMPUS = PREV.LOC And
                MAST.QUALIFICATION = PREV.QUALIFICATION And
                MAST.FEE_LEVIED = PREV.FEE_LEVIED Left Join
            Z001af_officer CAMP_OFF On CAMP_OFF.CAMPUS = PREV.LOC Left Join
            Z001af_officer ORG_OFF On ORG_OFF.CAMPUS = PREV.ORG Left Join
            Z001ag_supervisor CAMP_SUP On CAMP_SUP.CAMPUS = PREV.LOC Left Join
            Z001ag_supervisor ORG_SUP On ORG_SUP.CAMPUS = PREV.ORG
        Where
            PREV.PREV_PROCESS Is Null Or
            PREV.DATE_REPORTED > PREV.PREV_DATE_RETEST And PREV.REMARK = ""
        ;"""
        s_sql = s_sql.replace("%FILEP%", s_fprefix)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD THE FINAL TABLE FOR EXPORT AND REPORT
    sr_file = s_fprefix + "x_qual_fee_overcharge"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if l_debug:
        print("Build the final report")
    if i_finding_before > 0 and i_finding_after > 0:
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            '%FIND%' As Audit_finding,
            FIND.ORG As Organization,
            FIND.LOC As Campus,
            FIND.ID As Student,
            FIND.FEE_LEVIED As Fee_levied,
            FIND.FEE_LEVIED_TYPE As Fee_levied_type,
            FIND.FEE_SHOULD_BE As Fee_should_be_type,
            FIND.FEE_MODE As Fee_normal,
            FIND.FEE_MODE_HALF As Fee_half,
            FIND.FEE_BURS As Bursary_received,
            FIND.QUALIFICATION As Qualification,
            FIND.QUALIFICATION_NAME As Qualification_name,
            FIND.PRESENT_CAT As Present_category,
            FIND.ENROL_CAT As Enrol_category,
            FIND.SEM1 As Sem1_mod,
            FIND.SEM2 As Sem2_mod,
            FIND.SEM7 As Year_mod,
            FIND.CAMP_OFF_NAME AS Responsible_Officer,
            FIND.CAMP_OFF_NUMB AS Responsible_Officer_Numb,
            FIND.CAMP_OFF_MAIL AS Responsible_Officer_Mail,
            FIND.CAMP_SUP_NAME AS Supervisor,
            FIND.CAMP_SUP_NUMB AS Supervisor_Numb,
            FIND.CAMP_SUP_MAIL AS Supervisor_Mail,
            FIND.ORG_OFF_NAME AS Org_Officer,
            FIND.ORG_OFF_NUMB AS Org_Officer_Numb,
            FIND.ORG_OFF_MAIL AS Org_Officer_Mail,
            FIND.ORG_SUP_NAME AS Org_Supervisor,
            FIND.ORG_SUP_NUMB AS Org_Supervisor_Numb,
            FIND.ORG_SUP_MAIL AS Org_Supervisor_Mail,            
            FIND.U_NUMB As Tran_owner_numb,
            FIND.U_NAME As Tran_owner,
            FIND.U_MAIL As Tran_owner_mail,
            FIND.SYSTEM_DESC As System_description            
        From
            %FILEP%h_detail FIND
        ;"""
        s_sql = s_sql.replace("%FIND%", s_finding)
        s_sql = s_sql.replace("%FILEP%", s_fprefix)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)
        # Export findings
        if l_export and funcsys.tablerowcount(so_curs, sr_file) > 0:
            if l_debug:
                print("Export findings...")
            sx_path = re_path + "/"
            sx_file = "Qualification_test_" + s_fprefix + "_" + s_finding.lower() + "_"
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

    """*****************************************************************************
    SECONDARY QUALIFICATION FEE OVERCHARGE CONTACT
    *****************************************************************************"""
    if l_debug:
        print("SECONDARY QUALIFICATION FEE OVERCHARGE CONTACT")
    funcfile.writelog("SECONDARY QUALIFICATION FEE OVERCHARGE CONTACT")

    # FILES NEEDED
    # X020bx_Student_master_sort

    # DECLARE TEST VARIABLES
    s_fprefix: str = "X021h"
    s_fname: str = "secqual_fee_overcharge"
    s_finding: str = "SECONDARY QUALIFICATION FEE OVERCHARGE"
    s_xfile: str = "302_reported.txt"
    i_finding_before: int = 0
    i_finding_after: int = 0
    s_desc = "Contact student account overcharged for a secondary qualification"

    # OBTAIN TEST DATA
    if l_debug:
        print("Obtain test data...")
    sr_file: str = s_fprefix + "a_" + s_fname
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            STUD.*
        From
            X020bx_Student_master_sort STUD
        Where
            STUD.VALID = 3 And
            STUD.FEE_SHOULD_BE Like ('% C%')        
        Order By
            STUD.CAMPUS,
            STUD.FEE_SHOULD_BE,
            STUD.KSTUDBUSENTID
    ;"""
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)
    if l_export and funcsys.tablerowcount(so_curs, sr_file) > 0:
        if l_debug:
            print("Export findings...")
        sx_path = re_path + "/"
        sx_file = "Student_fee_test_" + s_fprefix + "_" + s_finding + "_studentlist_"
        sx_file_dated = sx_file + funcdatn.get_today_date_file()
        s_head = funccsv.get_colnames_sqlite(so_conn, sr_file)
        funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file, s_head)
        # funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file_dated, s_head)
        funcfile.writelog("%t EXPORT DATA: " + sx_path + sx_file)

    # SELECT TEST DATA
    if l_debug:
        print("Identify findings...")
    sr_file = s_fprefix + "b_finding"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        'NWU' As ORG,
        FIND.CAMPUS As LOC,
        FIND.KSTUDBUSENTID As ID,
        FIND.QUALIFICATION,
        FIND.FEE_LEVIED
    From
        %FILEP%%FILEN% FIND
    Order by
        LOC,
        QUALIFICATION,
        ID   
    ;"""
    s_sql = s_sql.replace("%FILEP%", s_fprefix)
    s_sql = s_sql.replace("%FILEN%", "a_" + s_fname)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # COUNT THE NUMBER OF FINDINGS
    i_finding_before: int = funcsys.tablerowcount(so_curs, sr_file)
    if l_debug:
        print("*** Found " + str(i_finding_before) + " exceptions ***")
    funcfile.writelog("%t FINDING: " + str(i_finding_before) + " " + s_finding + " finding(s)")

    # GET PREVIOUS FINDINGS
    if i_finding_before > 0:
        i = functest.get_previous_finding(so_curs, ed_path, s_xfile, s_finding, "ITTRT")
        so_conn.commit()

    # SET PREVIOUS FINDINGS
    if i_finding_before > 0:
        i = functest.set_previous_finding(so_curs)
        so_conn.commit()

    # ADD PREVIOUS FINDINGS
    sr_file = s_fprefix + "d_addprev"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0:
        if l_debug:
            print("Join previously reported to current findings...")
        s_sql = "CREATE TABLE " + sr_file + " AS" + """
        Select
            FIND.*,
            Lower('%FINDING%') AS PROCESS,
            '%TODAY%' AS DATE_REPORTED,
            '%DAYS%' AS DATE_RETEST,
            PREV.PROCESS AS PREV_PROCESS,
            PREV.DATE_REPORTED AS PREV_DATE_REPORTED,
            PREV.DATE_RETEST AS PREV_DATE_RETEST,
            PREV.REMARK
        From
            %FILEP%b_finding FIND Left Join
            Z001ab_setprev PREV ON PREV.FIELD1 = FIND.ID And
                PREV.FIELD2 = FIND.LOC And
                PREV.FIELD3 = FIND.QUALIFICATION And
                PREV.FIELD4 = FIND.FEE_LEVIED
        ;"""
        s_sql = s_sql.replace("%FINDING%", s_finding)
        s_sql = s_sql.replace("%FILEP%", s_fprefix)
        s_sql = s_sql.replace("%TODAY%", funcdatn.get_today_date())
        s_sql = s_sql.replace("%DAYS%", funcdatn.get_current_month_end_next())
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD LIST TO UPDATE FINDINGS
    sr_file = s_fprefix + "e_newprev"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0:
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            PREV.PROCESS,
            PREV.ID AS FIELD1,
            PREV.LOC AS FIELD2,
            PREV.QUALIFICATION AS FIELD3,
            PREV.FEE_LEVIED AS FIELD4,
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
        s_sql = s_sql.replace("%FILEP%", s_fprefix)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)
        # Export findings to previous reported file
        i_finding_after = funcsys.tablerowcount(so_curs, sr_file)
        if i_finding_after > 0:
            if l_debug:
                print("*** " + str(i_finding_after) + " Finding(s) to report ***")
            sx_path = ed_path
            sx_file = s_xfile[:-4]
            # Read the header data
            s_head = funccsv.get_colnames_sqlite(so_conn, sr_file)
            # Write the data
            if l_record:
                funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file, s_head, "a", ".txt")
                funcfile.writelog("%t FINDING: " + str(i_finding_after) + " new finding(s) to export")
                funcfile.writelog("%t EXPORT DATA: " + sr_file)
            if l_mess:
                funcsms.send_telegram('', 'administrator',
                                      '<b>' + str(i_finding_before) + '/' + str(i_finding_after) + '</b> ' + s_desc)
        else:
            if l_debug:
                print("*** No new findings to report ***")
            funcfile.writelog("%t FINDING: No new findings to export")

    # IMPORT OFFICERS FOR MAIL REPORTING PURPOSES
    if i_finding_before > 0 and i_finding_after > 0:
        i = functest.get_officer(so_curs, "VSS", "TEST " + s_finding + " OFFICER")
        so_conn.commit()

    # IMPORT SUPERVISORS FOR MAIL REPORTING PURPOSES
    if i_finding_before > 0 and i_finding_after > 0:
        i = functest.get_supervisor(so_curs, "VSS", "TEST " + s_finding + " SUPERVISOR")
        so_conn.commit()

    # ADD CONTACT DETAILS TO FINDINGS
    sr_file = s_fprefix + "h_detail"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0 and i_finding_after > 0:
        if l_debug:
            print("Add contact details to findings...")
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            PREV.ORG,
            PREV.LOC,
            PREV.ID,
            PREV.FEE_LEVIED,
            MAST.FEE_LEVIED_TYPE,
            MAST.FEE_SHOULD_BE,
            MAST.FEE_MODE,
            MAST.FEE_MODE_HALF,
            MAST.FEE_BURS,
            PREV.QUALIFICATION,
            MAST.QUALIFICATION_NAME,
            MAST.PRESENT_CAT,
            MAST.ENROL_CAT,
            MAST.SEM1,
            MAST.SEM2,
            MAST.SEM7,        
            CAMP_OFF.EMPLOYEE_NUMBER AS CAMP_OFF_NUMB,
            CAMP_OFF.NAME_ADDR AS CAMP_OFF_NAME,
            CASE
                WHEN  CAMP_OFF.EMPLOYEE_NUMBER != '' THEN CAMP_OFF.EMPLOYEE_NUMBER||'@nwu.ac.za'
                ELSE CAMP_OFF.EMAIL_ADDRESS
            END AS CAMP_OFF_MAIL,
            CAMP_OFF.EMAIL_ADDRESS AS CAMP_OFF_MAIL2,        
            CAMP_SUP.EMPLOYEE_NUMBER AS CAMP_SUP_NUMB,
            CAMP_SUP.NAME_ADDR AS CAMP_SUP_NAME,
            CASE
                WHEN CAMP_SUP.EMPLOYEE_NUMBER != '' THEN CAMP_SUP.EMPLOYEE_NUMBER||'@nwu.ac.za'
                ELSE CAMP_SUP.EMAIL_ADDRESS
            END AS CAMP_SUP_MAIL,
            CAMP_SUP.EMAIL_ADDRESS AS CAMP_SUP_MAIL2,
            ORG_OFF.EMPLOYEE_NUMBER AS ORG_OFF_NUMB,
            ORG_OFF.NAME_ADDR AS ORG_OFF_NAME,
            CASE
                WHEN ORG_OFF.EMPLOYEE_NUMBER != '' THEN ORG_OFF.EMPLOYEE_NUMBER||'@nwu.ac.za'
                ELSE ORG_OFF.EMAIL_ADDRESS
            END AS ORG_OFF_MAIL,
            ORG_OFF.EMAIL_ADDRESS AS ORG_OFF_MAIL2,
            ORG_SUP.EMPLOYEE_NUMBER AS ORG_SUP_NUMB,
            ORG_SUP.NAME_ADDR AS ORG_SUP_NAME,
            CASE
                WHEN ORG_SUP.EMPLOYEE_NUMBER != '' THEN ORG_SUP.EMPLOYEE_NUMBER||'@nwu.ac.za'
                ELSE ORG_SUP.EMAIL_ADDRESS
            END AS ORG_SUP_MAIL,
            ORG_SUP.EMAIL_ADDRESS AS ORG_SUP_MAIL2,
            CASE
                WHEN MAST.NAME_ADDR != '' THEN MAST.FUSERBUSINESSENTITYID
            END AS U_NUMB,
            CASE
                WHEN MAST.NAME_ADDR != '' THEN MAST.NAME_ADDR
                ELSE CAMP_OFF.NAME_ADDR
            END AS U_NAME, 
            CASE
                WHEN MAST.NAME_ADDR != '' THEN MAST.FUSERBUSINESSENTITYID||'@nwu.ac.za'
                WHEN  CAMP_OFF.EMPLOYEE_NUMBER != '' THEN CAMP_OFF.EMPLOYEE_NUMBER||'@nwu.ac.za'
                ELSE CAMP_OFF.EMAIL_ADDRESS
            END AS U_MAIL, 
            MAST.SYSTEM_DESC        
        From
            %FILEP%d_addprev PREV Left Join
            %FILEP%%FILEN% MAST On MAST.KSTUDBUSENTID = PREV.ID And
                MAST.CAMPUS = PREV.LOC And
                MAST.QUALIFICATION = PREV.QUALIFICATION And
                MAST.FEE_LEVIED = PREV.FEE_LEVIED Left Join
            Z001af_officer CAMP_OFF On CAMP_OFF.CAMPUS = PREV.LOC Left Join
            Z001af_officer ORG_OFF On ORG_OFF.CAMPUS = PREV.ORG Left Join
            Z001ag_supervisor CAMP_SUP On CAMP_SUP.CAMPUS = PREV.LOC Left Join
            Z001ag_supervisor ORG_SUP On ORG_SUP.CAMPUS = PREV.ORG
        Where
            PREV.PREV_PROCESS Is Null Or
            PREV.DATE_REPORTED > PREV.PREV_DATE_RETEST And PREV.REMARK = ""
        ;"""
        s_sql = s_sql.replace("%FILEP%", s_fprefix)
        s_sql = s_sql.replace("%FILEN%", "a_" + s_fname)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD THE FINAL TABLE FOR EXPORT AND REPORT
    sr_file = s_fprefix + "x_" + s_fname
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if l_debug:
        print("Build the final report")
    if i_finding_before > 0 and i_finding_after > 0:
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            '%FIND%' As Audit_finding,
            FIND.ORG As Organization,
            FIND.LOC As Campus,
            FIND.ID As Student,
            FIND.FEE_LEVIED As Fee_levied,
            FIND.FEE_LEVIED_TYPE As Fee_levied_type,
            FIND.FEE_SHOULD_BE As Fee_should_be_type,
            FIND.FEE_MODE As Fee_normal,
            FIND.FEE_MODE_HALF As Fee_half,
            FIND.FEE_BURS As Bursary_received,
            FIND.QUALIFICATION As Qualification,
            FIND.QUALIFICATION_NAME As Qualification_name,
            FIND.PRESENT_CAT As Present_category,
            FIND.ENROL_CAT As Enrol_category,
            FIND.SEM1 As Sem1_mod,
            FIND.SEM2 As Sem2_mod,
            FIND.SEM7 As Year_mod,
            FIND.CAMP_OFF_NAME AS Responsible_Officer,
            FIND.CAMP_OFF_NUMB AS Responsible_Officer_Numb,
            FIND.CAMP_OFF_MAIL AS Responsible_Officer_Mail,
            FIND.CAMP_SUP_NAME AS Supervisor,
            FIND.CAMP_SUP_NUMB AS Supervisor_Numb,
            FIND.CAMP_SUP_MAIL AS Supervisor_Mail,
            FIND.ORG_OFF_NAME AS Org_Officer,
            FIND.ORG_OFF_NUMB AS Org_Officer_Numb,
            FIND.ORG_OFF_MAIL AS Org_Officer_Mail,
            FIND.ORG_SUP_NAME AS Org_Supervisor,
            FIND.ORG_SUP_NUMB AS Org_Supervisor_Numb,
            FIND.ORG_SUP_MAIL AS Org_Supervisor_Mail,            
            FIND.U_NUMB As Tran_owner_numb,
            FIND.U_NAME As Tran_owner,
            FIND.U_MAIL As Tran_owner_mail,
            FIND.SYSTEM_DESC As System_description            
        From
            %FILEP%h_detail FIND
        ;"""
        s_sql = s_sql.replace("%FIND%", s_finding)
        s_sql = s_sql.replace("%FILEP%", s_fprefix)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)
        # Export findings
        if l_export and funcsys.tablerowcount(so_curs, sr_file) > 0:
            if l_debug:
                print("Export findings...")
            sx_path = re_path + "/"
            sx_file = "Qualification_test_" + s_fprefix + "_" + s_finding.lower() + "_"
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

    """*****************************************************************************
    MODULE FEE MASTER 1 - PREPARE MODULE MASTER FILES
    *****************************************************************************"""
    if l_debug:
        print("MODULE FEE MASTER")
    funcfile.writelog("MODULE FEE MASTER")

    # IMPORT MODULE LEVY LIST
    sr_file = "X030aa_Fiabd007"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if l_debug:
        print("Import vss module fees...")
    so_curs.execute(
        "Create Table " + sr_file + """
        (ACAD_PROG_FEE_TYPE INT,
        ACAD_PROG_FEE_DESC TEXT,
        APOMOD INT,
        FPRESENTATIONCATEGORYCODEID INT,
        PRESENT_CAT TEXT,
        FENROLMENTCATEGORYCODEID INT,
        ENROL_CATEGORY TEXT,
        FSITEORGUNITNUMBER INT,
        CAMPUS TEXT,
        OE_CODE INT,
        SCHOOL TEXT,
        FMODAPID INT,
        MODULE TEXT,
        MODULE_NAME TEXT,
        MODULE_TYPE TEXT,
        MODULE_TYPE_NAME TEXT,
        TRANSCODE TEXT,
        AMOUNT REAL,
        START_DATE,
        END_DATE)
        """)
    co = open(ed_path + "302_fiapd007_modu_" + s_period + ".csv", "r", encoding="utf-8")
    co_reader = csv.reader(co)
    # Read the COLUMN database data
    for row in co_reader:
        # Populate the column variables
        # if l_debug:
        #   print(row[0])
        if "Academic Program Fee Type" in row[0]:
            continue
        elif row[0] == "":
            continue
        else:
            s_cols = "Insert Into " + sr_file + " Values(" \
                                                "" + row[0] + "," \
                                                "'" + row[1] + "'," \
                                                "" + row[2] + "," \
                                                "" + row[3] + "," \
                                                "'" + row[4] + "'," \
                                                "" + row[5] + "," \
                                                "'" + row[6] + "'," \
                                                "" + row[7] + "," \
                                                "'" + row[8] + "'," \
                                                "" + row[9] + "," \
                                                "'" + row[10].replace(",","").replace("'","") + "'," \
                                                "" + row[11] + "," \
                                                "'" + row[12] + "'," \
                                                "'" + row[13].replace(",","").replace("'","") + "'," \
                                                "'" + row[14] + "'," \
                                                "'" + row[15] + "'," \
                                                "'" + row[16] + "'," \
                                                "" + row[17] + "," \
                                                "'" + row[18] + "'," \
                                                "'" + row[19] + "')"

            s_cols = s_cols.replace("A'S ", "A ")
            s_cols = s_cols.replace("E'S ", "E ")
            s_cols = s_cols.replace("N'S ", "N ")
            # if l_debug:
            #   print(s_cols)
            so_curs.execute(s_cols)
    so_conn.commit()
    # Close the imported data file
    co.close()
    funcfile.writelog("%t IMPORT TABLE: " + ed_path + "302_fiapd007_modu_period.csv (" + sr_file + ")")

    # SUMM FIAB LEVY LIST
    if l_debug:
        print("Build summary of module levy list...")
    sr_file = "X030aa_Fiabd007_summ"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        FIAB.FMODAPID,
        Upper(FIAB.CAMPUS) As CAMPUS,
        FIAB.FPRESENTATIONCATEGORYCODEID,
        Upper(FIAB.PRESENT_CAT) As PRESENT_CAT,
        FIAB.FENROLMENTCATEGORYCODEID,
        Upper(FIAB.ENROL_CATEGORY) As ENROL_CATEGORY,    
        Max(FIAB.AMOUNT) As AMOUNT,
        Cast(Count(FIAB.ACAD_PROG_FEE_TYPE) As INT) As COUNT,
        FIAB.MODULE,
        FIAB.MODULE_NAME,
        Max(FIAB.START_DATE) As START_DATE        
    From
        X030aa_Fiabd007 FIAB
    Group By
        FIAB.FMODAPID,
        FIAB.CAMPUS,
        FIAB.FPRESENTATIONCATEGORYCODEID,
        FIAB.FENROLMENTCATEGORYCODEID
    ;"""
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD SUMMARY OF ALL MODULES PRESENTED
    if l_debug:
        print("Build list of all modules presented...")
    sr_file = "X030ab_Stud_modu_list"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = "Create table " + sr_file + " AS" + """
    Select
        STUD.KENROLSTUDID,
        STUD.KSTUDBUSENTID As STUDENT,
        STUD.FMODULEAPID,
        STUD.CAMPUS,
        STUD.FPRESENTATIONCATEGORYCODEID As PRESENT_ID,
        STUD.PRESENT_CATEGORY,    
        STUD.FENROLMENTCATEGORYCODEID As ENROL_ID,
        STUD.ENROL_CATEGORY,
        STUD.MODULE,
        STUD.MODULE_NAME,
        Case
            When STUD.ISCONDITIONALREG = 1 Then '0 CONDITIONAL REGISTRATION'
            When STUD.ENROL_CATEGORY Like('%EXAM ONLY%') Then '0 EXAM ONLY'
            When STUD.ENROL_CATEGORY Like('%ROLL OVER%') Then '0 ROLL OVER'
            Else '1'
        End As FINDING    
    From
        %VSS%.X001_Student_module STUD
    Where
            UPPER(STUD.ENROL_CATEGORY) Not Like '%SHORT COURSE%'
    ;"""
    if s_period == "prev":
        s_sql = s_sql.replace("%VSS%", "VSSPREV")
    else:
        s_sql = s_sql.replace("%VSS%", "VSSCURR")
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD SUMMARY OF ALL MODULES PRESENTED
    if l_debug:
        print("Build summary of all modules presented...")
    sr_file = "X030ac_Stud_module_summ"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = "Create table " + sr_file + " AS" + """
    Select
        STUD.FMODULEAPID,
        STUD.CAMPUS,
        STUD.PRESENT_ID,
        STUD.PRESENT_CATEGORY,    
        STUD.ENROL_ID,
        STUD.ENROL_CATEGORY,
        STUD.MODULE,
        STUD.MODULE_NAME,
        Count(STUD.KENROLSTUDID) As COUNT_STUD
    From
        X030ab_Stud_modu_list STUD
    Where
        STUD.FINDING Like('1%')        
    Group By
        STUD.FMODULEAPID,
        STUD.CAMPUS,
        STUD.PRESENT_ID,
        STUD.ENROL_ID,
        STUD.MODULE
    ;"""
    if s_period == "prev":
        s_sql = s_sql.replace("%VSS%", "VSSPREV")
    else:
        s_sql = s_sql.replace("%VSS%", "VSSCURR")
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD LIST OF MODULES PLUS STATS
    if l_debug:
        print("Build summary of modules levied from transactions...")
    sr_file = "X030bb_Trans_feemodu"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = "Create table " + sr_file + " AS" + """
    Select
        TRAN.FMODAPID,
        TRAN.ENROL_ID,
        TRAN.ENROL_CATEGORY As ENROL_CAT,
        TRAN.MODULE,
        TRAN.MODULE_NAME,
        TRAN.AMOUNT,
        CAST(COUNT(TRAN.STUDENT) As INT) As TRAN_COUNT_ALL,
        CAST(TOTAL(TRAN.AMOUNT) AS REAL) AS FEE_MODU_ALL
    From
        X000_Transaction TRAN
    Where
        Instr('%TRANCODE%', Trim(TRAN.TRANSCODE)) > 0 And TRAN.FMODAPID != 0
    Group by
        TRAN.FMODAPID,
        TRAN.ENROL_ID
    ;"""
    s_sql = s_sql.replace("%TRANCODE%", s_modu_trancode)
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # CALCULATE THE MODULE FEES LEVIED PER STUDENT
    if l_debug:
        print("Calculate the module fees levied per student...")
    sr_file = "X030bb_Trans_feemodu_stud"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = "Create table " + sr_file + " AS" + """
    Select
        TRAN.STUDENT,
        TRAN.FMODAPID,
        TRAN.ENROL_ID,
        TRAN.ENROL_CATEGORY As ENROL_CAT,
        CAST(COUNT(TRAN.STUDENT) As INT) As TRAN_COUNT,
        CAST(TOTAL(TRAN.AMOUNT) AS REAL) AS FEE_MODU,
        MAX(TRAN.AUDITDATETIME),
        TRAN.MODULE,
        TRAN.MODULE_NAME,
        TRAN.FUSER AS FUSERBUSINESSENTITYID,
        TRAN.FUSERNAME AS NAME_ADDR,
        TRAN.FAUDITUSERCODE,
        TRAN.SYSTEM_DESC
    From
        X000_Transaction TRAN
    Where
        Instr('%TRANCODE%', Trim(TRAN.TRANSCODE)) > 0 And TRAN.FMODAPID != 0
    Group by
        TRAN.STUDENT,
        TRAN.FMODAPID,
        TRAN.ENROL_ID
    ;"""
    s_sql = s_sql.replace("%TRANCODE%", s_modu_trancode)
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # NOTE - Function fully functional. Take long time to complete (30min). Not used at the moment.
    """
    # CALCULATE THE STATISTIC MODE FOR EACH QUALIFICATION
    if l_debug:
        print("Calculate the module transaction statistic mode...")
    i_value: int = 0
    sr_file = "X030bc_Trans_feemodu_mode"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute("CREATE TABLE " + sr_file + " (FMODAPID INT, ENROL_ID INT, AMOUNT REAL)")
    for qual in so_curs.execute("SELECT FMODAPID, ENROL_ID FROM X030ab_Trans_feemodu").fetchall():
            # if l_debug:
            #   print("FMODAPID = " + str(qual[0]) + " And ENROL_CAT = " + str(qual[1]))
        try:
            i_value = funcstat.stat_mode(so_curs,
                                         "X030ab_Trans_feemodu_stud",
                                         "FEE_MODU",
                                         "FMODAPID = " + str(qual[0]) + " And ENROL_ID = " + str(qual[1])
                                         )
            if i_value < 0:
                i_value = 0
        except Exception as e:
            # funcsys.ErrMessage(e) if you want error to log
            if "".join(e.args).find("no unique mode") >= 0:
                i_value = funcstat.stat_highest_value(so_curs,
                                                      "X030ab_Trans_feemodu_stud",
                                                      "FEE_MODU",
                                                      "FMODAPID = " + str(qual[0]) + " And ENROL_ID = " + str(qual[1])
                                                      )
            else:
                i_value = 0
            # if l_debug:
            #   print(i_value)
        s_cols = "INSERT INTO " + sr_file + " VALUES(" + str(qual[0]) + ", " + str(qual[1]) + ", " + str(i_value) + ")"
        so_curs.execute(s_cols)
    funcfile.writelog("%t BUILD TABLE: " + sr_file)
    so_conn.commit()
    """

    # BUILD SUMMARY OF ALL MODULES PRESENTED
    if l_debug:
        print("Build summary of all modules presented...")
    sr_file = "X030bd_Stud_modu_present"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = "Create table " + sr_file + " AS" + """
    Select
        STUD.KENROLMENTPRESENTATIONID,
        STUD.FMODULEAPID,
        STUD.FENROLMENTCATEGORYCODEID As ENROL_ID,
        STUD.ENROL_CATEGORY,
        STUD.FPRESENTATIONCATEGORYCODEID As PRESENT_ID,
        STUD.PRESENT_CATEGORY,
        Count(STUD.KENROLSTUDID) As COUNT
    From
        %VSS%.X001_Student_module STUD
    Group By
        STUD.KENROLMENTPRESENTATIONID,
        STUD.FMODULEAPID,
        STUD.FENROLMENTCATEGORYCODEID,
        STUD.FPRESENTATIONCATEGORYCODEID
    ;"""
    if s_period == "prev":
        s_sql = s_sql.replace("%VSS%", "VSSPREV")
    else:
        s_sql = s_sql.replace("%VSS%", "VSSCURR")
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    """*****************************************************************************
    TEST MODULE FEE NOT LOADED
    *****************************************************************************"""
    if l_debug:
        print("MODULE FEE NOT LOADED")
    funcfile.writelog("MODULE FEE NOT LOADED")

    # DECLARE VARIABLES
    i_finding_after: int = 0
    s_desc = "Module fee not loaded"

    # JOIN MODULES PRESENTED AND LEVY LIST
    if l_debug:
        print("Join modules presented and levy list...")
    sr_file = "X031aa_Modu_nofee_loaded"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = "Create Table " + sr_file + " As" + """
    Select
        STUD.FMODULEAPID,
        STUD.CAMPUS,
        STUD.PRESENT_ID,
        STUD.PRESENT_CATEGORY,
        STUD.ENROL_ID,
        STUD.ENROL_CATEGORY,
        STUD.MODULE,
        STUD.MODULE_NAME,
        STUD.COUNT_STUD,
        FIAB.COUNT,
        FIAB.AMOUNT
    From
        X030ac_Stud_module_summ STUD Left Join
        X030aa_Fiabd007_summ FIAB On FIAB.FMODAPID = STUD.FMODULEAPID
                And FIAB.CAMPUS = STUD.CAMPUS
                And FIAB.FPRESENTATIONCATEGORYCODEID = STUD.PRESENT_ID
                And FIAB.FENROLMENTCATEGORYCODEID = STUD.ENROL_ID
                And FIAB.MODULE = STUD.MODULE   
    ;"""
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # IDENTIFY FINDINGS
    # NOTE Exclude distance students
    if l_debug:
        print("Identify findings...")
    sr_file = "X031ab_findings"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        'NWU' As ORG,
        FIND.CAMPUS AS LOC,
        FIND.FMODULEAPID As ID,
        FIND.PRESENT_ID,
        FIND.PRESENT_CATEGORY,
        FIND.ENROL_ID,
        FIND.ENROL_CATEGORY,
        FIND.MODULE,
        FIND.MODULE_NAME,
        FIND.COUNT_STUD
    From
        X031aa_Modu_nofee_loaded FIND
    Where
        FIND.AMOUNT Is Null    
    Order By
        FIND.ENROL_CATEGORY,
        FIND.MODULE_NAME    
    ;"""
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # COUNT THE NUMBER OF FINDINGS
    i_finding_before: int = funcsys.tablerowcount(so_curs, sr_file)
    if l_debug:
        print("*** Found " + str(i_finding_before) + " exceptions ***")
    funcfile.writelog("%t FINDING: " + str(i_finding_before) + " MODULE NO FEE LOADED finding(s)")

    # GET PREVIOUS FINDINGS
    if i_finding_before > 0:
        i = functest.get_previous_finding(so_curs, ed_path, "302_reported.txt", "module no fee loaded", "ITIIT")
        so_conn.commit()

    # SET PREVIOUS FINDINGS
    if i_finding_before > 0:
        i = functest.set_previous_finding(so_curs)
        so_conn.commit()

    # ADD PREVIOUS FINDINGS
    sr_file = "X031ad_add_previous"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0:
        if l_debug:
            print("Join previously reported to current findings...")
        s_sql = "CREATE TABLE " + sr_file + " AS" + """
        Select
            FIND.*,
            'module no fee loaded' AS PROCESS,
            '%TODAY%' AS DATE_REPORTED,
            '%DAYS%' AS DATE_RETEST,
            PREV.PROCESS AS PREV_PROCESS,
            PREV.DATE_REPORTED AS PREV_DATE_REPORTED,
            PREV.DATE_RETEST AS PREV_DATE_RETEST,
            PREV.REMARK
        From
            X031ab_findings FIND Left Join
            Z001ab_setprev PREV ON PREV.FIELD1 = FIND.ID And
                PREV.FIELD2 = FIND.LOC And
                PREV.FIELD3 = FIND.PRESENT_ID And
                PREV.FIELD4 = FIND.ENROL_ID And
                PREV.FIELD5 = FIND.MODULE
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        s_sql = s_sql.replace("%TODAY%", funcdatn.get_today_date())
        s_sql = s_sql.replace("%DAYS%", funcdatn.get_current_month_end_next())
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD LIST TO UPDATE FINDINGS
    sr_file = "X031ae_new_previous"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0:
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            PREV.PROCESS,
            PREV.ID AS FIELD1,
            PREV.LOC AS FIELD2,
            PREV.PRESENT_ID AS FIELD3,
            PREV.ENROL_ID AS FIELD4,
            PREV.MODULE AS FIELD5,
            PREV.DATE_REPORTED,
            PREV.DATE_RETEST,
            PREV.REMARK
        From
            X031ad_add_previous PREV
        Where
            PREV.PREV_PROCESS Is Null Or
            PREV.DATE_REPORTED > PREV.PREV_DATE_RETEST And PREV.REMARK = ""
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)
        # Export findings to previous reported file
        i_finding_after = funcsys.tablerowcount(so_curs, sr_file)
        if i_finding_after > 0:
            if l_debug:
                print("*** " + str(i_finding_after) + " Finding(s) to report ***")
            sx_path = ed_path
            sx_file = "302_reported"
            # Read the header data
            s_head = funccsv.get_colnames_sqlite(so_conn, sr_file)
            # Write the data
            if l_record:
                funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file, s_head, "a", ".txt")
                funcfile.writelog("%t FINDING: " + str(i_finding_after) + " new finding(s) to export")
                funcfile.writelog("%t EXPORT DATA: " + sr_file)
            if l_mess:
                funcsms.send_telegram('', 'administrator',
                                      '<b>' + str(i_finding_before) + '/' + str(i_finding_after) + '</b> ' + s_desc)
        else:
            if l_debug:
                print("*** No new findings to report ***")
            funcfile.writelog("%t FINDING: No new findings to export")

    # IMPORT OFFICERS FOR MAIL REPORTING PURPOSES
    if i_finding_before > 0 and i_finding_after > 0:
        i = functest.get_officer(so_curs, "VSS", "stud_fee_test_modu_no_fee_loaded_officer")
        so_conn.commit()

    # IMPORT SUPERVISORS FOR MAIL REPORTING PURPOSES
    if i_finding_before > 0 and i_finding_after > 0:
        i = functest.get_supervisor(so_curs, "VSS", "stud_fee_test_modu_no_fee_loaded_supervisor")
        so_conn.commit()

    # ADD CONTACT DETAILS TO FINDINGS
    sr_file = "X031ah_detail"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0 and i_finding_after > 0:
        if l_debug:
            print("Add contact details to findings...")
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            PREV.ORG,
            PREV.LOC,
            PREV.ID,
            PREV.MODULE,
            PREV.MODULE_NAME,
            PREV.PRESENT_ID,
            PREV.PRESENT_CATEGORY,
            PREV.ENROL_ID,
            PREV.ENROL_CATEGORY,
            PREV.COUNT_STUD,
            CAMP_OFF.EMPLOYEE_NUMBER As CAMP_OFF_NUMB,
            CAMP_OFF.NAME_ADDR As CAMP_OFF_NAME,
            CASE
                WHEN  CAMP_OFF.EMPLOYEE_NUMBER <> '' THEN CAMP_OFF.EMPLOYEE_NUMBER||'@nwu.ac.za'
                ELSE CAMP_OFF.EMAIL_ADDRESS
            END As CAMP_OFF_MAIL,
            CAMP_OFF.EMAIL_ADDRESS As CAMP_OFF_MAIL2,
            CAMP_SUP.EMPLOYEE_NUMBER As CAMP_SUP_NUMB,
            CAMP_SUP.NAME_ADDR As CAMP_SUP_NAME,
            CASE
                WHEN CAMP_SUP.EMPLOYEE_NUMBER <> '' THEN CAMP_SUP.EMPLOYEE_NUMBER||'@nwu.ac.za'
                ELSE CAMP_SUP.EMAIL_ADDRESS
            END As CAMP_SUP_MAIL,
            CAMP_SUP.EMAIL_ADDRESS As CAMP_SUP_MAIL2,
            ORG_OFF.EMPLOYEE_NUMBER As ORG_OFF_NUMB,
            ORG_OFF.NAME_ADDR As ORG_OFF_NAME,
            CASE
                WHEN ORG_OFF.EMPLOYEE_NUMBER <> '' THEN ORG_OFF.EMPLOYEE_NUMBER||'@nwu.ac.za'
                ELSE ORG_OFF.EMAIL_ADDRESS
            END As ORG_OFF_MAIL,
            ORG_OFF.EMAIL_ADDRESS As ORG_OFF_MAIL2,
            ORG_SUP.EMPLOYEE_NUMBER As ORG_SUP_NUMB,
            ORG_SUP.NAME_ADDR As ORG_SUP_NAME,
            CASE
                WHEN ORG_SUP.EMPLOYEE_NUMBER <> '' THEN ORG_SUP.EMPLOYEE_NUMBER||'@nwu.ac.za'
                ELSE ORG_SUP.EMAIL_ADDRESS
            END As ORG_SUP_MAIL,
            ORG_SUP.EMAIL_ADDRESS As ORG_SUP_MAIL2
        From
            X031ad_add_previous PREV Left Join
            Z001af_officer CAMP_OFF On CAMP_OFF.CAMPUS = PREV.LOC Left Join
            Z001af_officer ORG_OFF On ORG_OFF.CAMPUS = PREV.ORG Left Join
            Z001ag_supervisor CAMP_SUP On CAMP_SUP.CAMPUS = PREV.LOC Left Join
            Z001ag_supervisor ORG_SUP On ORG_SUP.CAMPUS = PREV.ORG
        Where
            PREV.PREV_PROCESS Is Null Or
            PREV.DATE_REPORTED > PREV.PREV_DATE_RETEST And PREV.REMARK = ""
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD THE FINAL TABLE FOR EXPORT AND REPORT
    sr_file = "X031ax_Modu_nofee_loaded"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if l_debug:
        print("Build the final report")
    if i_finding_before > 0 and i_finding_after > 0:
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            'MODULE NO FEE LOADED' As Audit_finding,
            FIND.ORG As 'Organization',
            FIND.LOC As 'Campus',
            FIND.ID As 'Module_id',
            FIND.MODULE As 'Module',
            FIND.MODULE_NAME As 'Module_name',
            FIND.PRESENT_CATEGORY As 'Present',
            FIND.ENROL_CATEGORY As 'Enrol',
            FIND.COUNT_STUD As 'Student_count',
            FIND.CAMP_OFF_NAME AS Responsible_Officer,
            FIND.CAMP_OFF_NUMB AS Responsible_Officer_Numb,
            FIND.CAMP_OFF_MAIL AS Responsible_Officer_Mail,
            FIND.CAMP_SUP_NAME AS Supervisor,
            FIND.CAMP_SUP_NUMB AS Supervisor_Numb,
            FIND.CAMP_SUP_MAIL AS Supervisor_Mail,
            FIND.ORG_OFF_NAME AS Org_Officer,
            FIND.ORG_OFF_NUMB AS Org_Officer_Numb,
            FIND.ORG_OFF_MAIL AS Org_Officer_Mail,
            FIND.ORG_SUP_NAME AS Org_Supervisor,
            FIND.ORG_SUP_NUMB AS Org_Supervisor_Numb,
            FIND.ORG_SUP_MAIL AS Org_Supervisor_Mail
        From
            X031ah_detail FIND
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)
        # Export findings
        if l_export and funcsys.tablerowcount(so_curs, sr_file) > 0:
            if l_debug:
                print("Export findings...")
            sx_path = re_path + "/"
            sx_file = "Student_fee_test_031ax_modu_fee_not_loaded_"
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
    #     funcsms.send_telegram('', 'administrator', '<b>STUDENT FEE</b> income tests end.')

    """ ****************************************************************************
    END OF SCRIPT
    *****************************************************************************"""
    if l_debug:
        print("END OF SCRIPT")
    funcfile.writelog("END OF SCRIPT")

    # CLOSE THE DATABASE CONNECTION
    so_conn.commit()
    so_conn.close()

    # CLOSE THE LOG WRITER *********************************************************
    funcfile.writelog("--------------------------------")
    funcfile.writelog("COMPLETED: C302_TEST_STUDENT_FEE")

    return


if __name__ == '__main__':
    try:
        student_fee("prev")
    except Exception as e:
        funcsys.ErrMessage(e)
