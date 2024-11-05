"""
SCRIPT TO TEST GL TRANSACTIONS
Created: 2 Jul 2019
Updated 23 Feb 2024
Author: Albert J v Rensburg (NWU:21162395)
"""

# IMPORT PYTHON MODULES
import csv
import sqlite3

# IMPORT OWN MODULES
from _my_modules import funcconf
from _my_modules import funcfile
from _my_modules import funccsv
from _my_modules import funcdatn
from _my_modules import funcmail
from _my_modules import funcsms
from _my_modules import funcsys
from _my_modules import funcstr
from _my_modules import functest
from _my_modules import funcstat


""" INDEX **********************************************************************
ENVIRONMENT
OPEN THE DATABASES
BEGIN OF SCRIPT
PROFESSIONAL FEES GL MASTER FILE
TEST PROFESSIONAL FEES PAID TO STUDENTS Deactivated on 20240126 by Albert
IA ACTUAL VS BUDGET
DONOR GL TRANSACTION IDENTIFICATION
END OF SCRIPT
*****************************************************************************"""


def gl_test_transactions():
    """
    Script to test GL transactions
    :return: int
    """

    """*****************************************************************************
    ENVIRONMENT
    *****************************************************************************"""

    # DECLARE VARIABLES
    so_path = f"{funcconf.drive_data_raw}Kfs/"  # Source database path
    so_file = "Kfs_test_gl_transaction.sqlite"  # Source database
    ed_path = f"{funcconf.drive_system}_external_data/"  # External data path
    re_path = f"{funcconf.drive_data_results}Kfs/"  # Results path
    l_debug: bool = False
    l_export = False
    l_mess: bool = funcconf.l_mess_project
    # l_mess: bool = False
    l_record = True

    # OPEN THE SCRIPT LOG FILE
    if l_debug:
        print("-------------------------")
        print("C202_GL_TEST_TRANSACTIONS")
        print("-------------------------")
    funcfile.writelog("Now")
    funcfile.writelog("SCRIPT: C202_GL_TEST_TRANSACTIONS")
    funcfile.writelog("---------------------------------")

    # MESSAGE
    if l_mess:
        funcsms.send_telegram("", "administrator", "<b>C202 Kfs gl transaction tests</b>")

    """*****************************************************************************
    OPEN THE DATABASES
    *****************************************************************************"""
    if l_debug:
        print("OPEN THE DATABASES")
    funcfile.writelog("OPEN THE DATABASES")

    # OPEN THE WORKING DATABASE
    with sqlite3.connect(so_path+so_file) as sqlite_connection:
        sqlite_cursor = sqlite_connection.cursor()
    funcfile.writelog("OPEN DATABASE: " + so_file)

    # ATTACH DATA SOURCES
    sqlite_cursor.execute(f"ATTACH DATABASE '{funcconf.drive_data_raw}Kfs/Kfs.sqlite' AS 'KFS'")
    funcfile.writelog("%t ATTACH DATABASE: KFS.SQLITE")
    sqlite_cursor.execute(f"ATTACH DATABASE '{funcconf.drive_data_raw}Kfs/Kfs_curr.sqlite' AS 'KFSCURR'")
    funcfile.writelog("%t ATTACH DATABASE: KFS_CURR.SQLITE")
    sqlite_cursor.execute(f"ATTACH DATABASE '{funcconf.drive_data_raw}Kfs/Kfs_prev.sqlite' AS 'KFSPREV'")
    funcfile.writelog("%t ATTACH DATABASE: KFS_PREV.SQLITE")
    sqlite_cursor.execute(f"ATTACH DATABASE '{funcconf.drive_data_raw}Kfs/Kfs_2022.sqlite' AS 'KFS2022'")
    funcfile.writelog("%t ATTACH DATABASE: KFS_2022.SQLITE")
    sqlite_cursor.execute(f"ATTACH DATABASE '{funcconf.drive_data_raw}People/People.sqlite' AS 'PEOPLE'")
    funcfile.writelog("%t ATTACH DATABASE: PEOPLE.SQLITE")
    sqlite_cursor.execute(f"ATTACH DATABASE '{funcconf.drive_data_raw}Vss/Vss_curr.sqlite' AS 'VSSCURR'")
    funcfile.writelog("%t ATTACH DATABASE: VSS_CURR.SQLITE")
    sqlite_cursor.execute(f"ATTACH DATABASE '{funcconf.drive_data_raw}Vss/Vss_prev.sqlite' AS 'VSSPREV'")
    funcfile.writelog("%t ATTACH DATABASE: VSS_PREV.SQLITE")

    """ ****************************************************************************
    BEGIN OF SCRIPT
    *****************************************************************************"""
    if l_debug:
        print("BEGIN OF SCRIPT")
    funcfile.writelog("BEGIN OF SCRIPT")

    """ ****************************************************************************
    PROFESSIONAL FEES GL MASTER FILE
    *****************************************************************************"""

    if l_debug:
        print("PROFESSIONAL FEES GL MASTER FILE")
    funcfile.writelog("PROFESSIONAL FEES GL MASTER FILE")

    # OBTAIN GL PROFESSIONAL FEE TRANSACTIONS
    if l_debug:
        print("Obtain gl professional (2056) fee transactions...")
    sr_file: str = "X001_gl_professional_fee"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        GL.*,
        ACC.ACCT_MGR_UNVL_ID As ACC_MGR,
        ACC.ACCT_SPVSR_UNVL_ID As ACC_SUP,
        ACC.ACCT_FSC_OFC_UID As ACC_FIS,
        CASE
            WHEN ACC.ACCT_PHYS_CMP_CD = 'P' THEN 'POTCHEFSTROOM'
            WHEN ACC.ACCT_PHYS_CMP_CD = 'V' THEN 'VAAL TRIANGLE'
            WHEN ACC.ACCT_PHYS_CMP_CD = 'M' THEN 'MAFIKENG'
            ELSE 'NWU'
        END As ACC_CAMPUS 
    From
        KFSCURR.X000_GL_trans GL Left Join
        KFS.X000_Account ACC On ACC.ACCOUNT_NBR = GL.ACCOUNT_NBR
    Where
        GL.FS_DATABASE_DESC = 'KFS' And
        Instr(GL.CALC_COST_STRING, '.2056') > 0
    ;"""
    sqlite_cursor.execute("DROP TABLE IF EXISTS " + sr_file)
    sqlite_cursor.execute(s_sql)
    sqlite_connection.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # ADD PAYMENT DETAILS TO GL TRANSACTIONS
    if l_debug:
        print("Add payment details to transactions...")
    sr_file: str = "X001_gl_professional_fee_pay"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        GL.*,
        PAY.VENDOR_ID,
        PAY.PAYEE_NAME As STUDENT_NAME,
        PAY.INV_NBR,
        PAY.PAYEE_TYP_DESC,
        PAY.COMPLETE_EMP_NO As EMP_INI,
        PAY.APPROVE_EMP_NO As EMP_APP
    From
        X001_gl_professional_fee GL Inner Join
        KFSCURR.X001aa_Report_payments PAY On PAY.CUST_PMT_DOC_NBR = GL.FDOC_NBR And
            PAY.NET_PMT_AMT = GL.CALC_AMOUNT      
    ;"""
    sqlite_cursor.execute("DROP TABLE IF EXISTS " + sr_file)
    sqlite_cursor.execute(s_sql)
    sqlite_connection.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    """ ****************************************************************************
    TEST PROFESSIONAL FEES PAID TO STUDENTS
    *****************************************************************************"""

    # Deactivated on 20240126 by Albert
    l_run_professional_fee_student: bool = False
    if not l_run_professional_fee_student:

        # TODO Delete table no longer used
        sqlite_cursor.execute("DROP TABLE IF EXISTS X001aa_professional_fee_student")
        sqlite_cursor.execute("DROP TABLE IF EXISTS X001ab_findings")
        sqlite_cursor.execute("DROP TABLE IF EXISTS X001ac_get_previous")
        sqlite_cursor.execute("DROP TABLE IF EXISTS X001ad_add_previous")
        sqlite_cursor.execute("DROP TABLE IF EXISTS X001ae_new_previous")
        # sqlite_cursor.execute("DROP TABLE IF EXISTS X001ax_professional_fee_student")

    else:

        if l_debug:
            print("PROFESSIONAL FEES PAID TO STUDENTS")
        funcfile.writelog("PROFESSIONAL FEES PAID TO STUDENTS")

        # DECLARE VARIABLES
        i_finding_after: int = 0

        # OBTAIN TEST DATA
        if l_debug:
            print("Obtain test data...")
        sr_file: str = "X001aa_professional_fee_student"
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            GL.*,
            STUD.KSTUDBUSENTID As STUDENT,
            CASE
                WHEN STUD.FSITEORGUNITNUMBER = -1 THEN 'POT'
                WHEN STUD.FSITEORGUNITNUMBER = -2 THEN 'VAA'
                WHEN STUD.FSITEORGUNITNUMBER = -9 THEN 'MAF'
                ELSE 'OTH'
            END As LOC 
        From
            X001_gl_professional_fee_pay GL Inner Join
            VSSCURR.X001_student STUD On Substr(GL.VENDOR_ID,1,8) = STUD.KSTUDBUSENTID And
                STUD.ISMAINQUALLEVEL = '1'
        Order By
            GL.TIMESTAMP    
        ;"""
        sqlite_cursor.execute("DROP TABLE IF EXISTS " + sr_file)
        sqlite_cursor.execute(s_sql)
        sqlite_connection.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

        # IDENTIFY FINDINGS
        if l_debug:
            print("Identify findings...")
        sr_file = "X001ab_findings"
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            'NWU' As ORG,
            CURR.LOC,
            CURR.STUDENT,
            CURR.FDOC_NBR,
            CURR.CALC_COST_STRING,
            EMP_INI,
            ACC_MGR
        From
            X001aa_professional_fee_student CURR
        ;"""
        sqlite_cursor.execute("DROP TABLE IF EXISTS " + sr_file)
        sqlite_cursor.execute(s_sql)
        sqlite_connection.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

        # COUNT THE NUMBER OF FINDINGS
        i_finding_before: int = funcsys.tablerowcount(sqlite_cursor, sr_file)
        if l_debug:
            print("*** Found " + str(i_finding_before) + " exceptions ***")
        funcfile.writelog("%t FINDING: " + str(i_finding_before) + " PROF FEE PAID TO STUDENT finding(s)")

        # GET PREVIOUS FINDINGS
        sr_file = "X001ac_get_previous"
        sqlite_cursor.execute("DROP TABLE IF EXISTS " + sr_file)
        if i_finding_before > 0:
            if l_debug:
                print("Import previously reported findings...")
            sqlite_cursor.execute(
                "CREATE TABLE " + sr_file + """
                (PROCESS TEXT,
                FIELD1 INT,
                FIELD2 TEXT,
                FIELD3 TEXT,
                FIELD4 TEXT,
                FIELD5 TEXT,
                DATE_REPORTED TEXT,
                DATE_RETEST TEXT,
                DATE_MAILED TEXT)
                """)
            # s_cols = ""
            co = open(ed_path + "202_reported.txt", "r")
            co_reader = csv.reader(co)
            # Read the COLUMN database data
            for row in co_reader:
                # Populate the column variables
                if row[0] == "PROCESS":
                    continue
                elif row[0] != "prof fee paid to student":
                    continue
                else:
                    s_cols = "INSERT INTO " + sr_file + " VALUES('" + row[0] + "','" + row[1] + "','" + row[2] + "','" + \
                             row[
                                 3] + "','" + row[4] + "','" + row[5] + "','" + row[6] + "','" + row[7] + "','" + row[
                                 8] + "')"
                    sqlite_cursor.execute(s_cols)
            sqlite_connection.commit()
            # Close the imported data file
            co.close()
            funcfile.writelog("%t IMPORT TABLE: " + ed_path + "001_reported.txt (" + sr_file + ")")

        # ADD PREVIOUS FINDINGS
        sr_file = "X001ad_add_previous"
        sqlite_cursor.execute("DROP TABLE IF EXISTS " + sr_file)
        if i_finding_before > 0:
            if l_debug:
                print("Join previously reported to current findings...")
            s_sql = "CREATE TABLE " + sr_file + " AS" + """
            Select
                FIND.*,
                'prof fee paid to student' AS PROCESS,
                '%TODAY%' AS DATE_REPORTED,
                '%DAYS%' AS DATE_RETEST,
                PREV.PROCESS AS PREV_PROCESS,
                PREV.DATE_REPORTED AS PREV_DATE_REPORTED,
                PREV.DATE_RETEST AS PREV_DATE_RETEST,
                PREV.DATE_MAILED
            From
                X001ab_findings FIND Left Join
                X001ac_get_previous PREV ON PREV.FIELD1 = FIND.STUDENT AND
                    PREV.FIELD2 = FIND.FDOC_NBR And
                    PREV.FIELD3 = FIND.CALC_COST_STRING And
                    PREV.DATE_RETEST >= Date('%TODAY%')
            ;"""
            sqlite_cursor.execute("DROP TABLE IF EXISTS " + sr_file)
            s_sql = s_sql.replace("%TODAY%", funcdatn.get_today_date())
            s_sql = s_sql.replace("%DAYS%", funcdatn.get_today_plusdays(366))
            sqlite_cursor.execute(s_sql)
            sqlite_connection.commit()
            funcfile.writelog("%t BUILD TABLE: " + sr_file)

        # BUILD LIST TO UPDATE FINDINGS
        # NOTE ADD CODE
        sr_file = "X001ae_new_previous"
        sqlite_cursor.execute("DROP TABLE IF EXISTS " + sr_file)
        if i_finding_before > 0:
            s_sql = "CREATE TABLE " + sr_file + " AS " + """
            Select
                PREV.PROCESS,
                PREV.STUDENT AS FIELD1,
                PREV.FDOC_NBR AS FIELD2,
                PREV.CALC_COST_STRING AS FIELD3,
                '' AS FIELD4,
                '' AS FIELD5,
                PREV.DATE_REPORTED,
                PREV.DATE_RETEST,
                PREV.DATE_MAILED
            From
                X001ad_add_previous PREV
            Where
                PREV.PREV_PROCESS Is Null
            ;"""
            sqlite_cursor.execute("DROP TABLE IF EXISTS " + sr_file)
            sqlite_cursor.execute(s_sql)
            sqlite_connection.commit()
            funcfile.writelog("%t BUILD TABLE: " + sr_file)
            # Export findings to previous reported file
            i_finding_after = funcsys.tablerowcount(sqlite_cursor, sr_file)
            if i_finding_after > 0:
                if l_debug:
                    print("*** " + str(i_finding_after) + " Finding(s) to report ***")
                sx_path = ed_path
                sx_file = "202_reported"
                # Read the header data
                s_head = funccsv.get_colnames_sqlite(sqlite_connection, sr_file)
                # Write the data
                if l_record:
                    funccsv.write_data(sqlite_connection, "main", sr_file, sx_path, sx_file, s_head, "a", ".txt")
                    funcfile.writelog("%t FINDING: " + str(i_finding_after) + " new finding(s) to export")
                    funcfile.writelog("%t EXPORT DATA: " + sr_file)
                if l_mess:
                    s_desc = "Professional fee student"
                    funcsms.send_telegram('', 'administrator',
                                          '<b>' + str(i_finding_before) + '/' + str(i_finding_after) + '</b> ' + s_desc)
            else:
                if l_debug:
                    print("*** No new findings to report ***")
                funcfile.writelog("%t FINDING: No new findings to export")

        # IMPORT OFFICERS FOR MAIL REPORTING PURPOSES
        sr_file = "X001af_officer"
        sqlite_cursor.execute("DROP TABLE IF EXISTS " + sr_file)
        if i_finding_before > 0:
            if i_finding_after > 0:
                if l_debug:
                    print("Import reporting officers for mail purposes...")
                s_sql = "CREATE TABLE " + sr_file + " AS " + """
                Select
                    OFFICER.LOOKUP,
                    OFFICER.LOOKUP_CODE AS CAMPUS,
                    OFFICER.LOOKUP_DESCRIPTION AS EMPLOYEE_NUMBER,
                    PEOP.NAME_ADDR As NAME,
                    PEOP.EMAIL_ADDRESS
                From
                    PEOPLE.X000_OWN_HR_LOOKUPS OFFICER Left Join
                    PEOPLE.X002_PEOPLE_CURR PEOP ON
                        PEOP.EMPLOYEE_NUMBER = OFFICER.LOOKUP_DESCRIPTION
                Where
                    OFFICER.LOOKUP = 'TEST_GL_OBJECT_PROF_FEE_PAID_TO_STUDENT_OFFICER'
                ;"""
                sqlite_cursor.execute("DROP TABLE IF EXISTS " + sr_file)
                sqlite_cursor.execute(s_sql)
                sqlite_connection.commit()
                funcfile.writelog("%t BUILD TABLE: " + sr_file)

        # IMPORT SUPERVISORS FOR MAIL REPORTING PURPOSES
        sr_file = "X001ag_supervisor"
        sqlite_cursor.execute("DROP TABLE IF EXISTS " + sr_file)
        if i_finding_before > 0 and i_finding_after > 0:
            if l_debug:
                print("Import reporting supervisors for mail purposes...")
            s_sql = "CREATE TABLE " + sr_file + " AS " + """
            Select
                SUPERVISOR.LOOKUP,
                SUPERVISOR.LOOKUP_CODE AS CAMPUS,
                SUPERVISOR.LOOKUP_DESCRIPTION AS EMPLOYEE_NUMBER,
                PEOP.NAME_ADDR As NAME,
                PEOP.EMAIL_ADDRESS
            From
                PEOPLE.X000_OWN_HR_LOOKUPS SUPERVISOR Left Join
                PEOPLE.X002_PEOPLE_CURR PEOP ON 
                    PEOP.EMPLOYEE_NUMBER = SUPERVISOR.LOOKUP_DESCRIPTION
            Where
                SUPERVISOR.LOOKUP = 'TEST_GL_OBJECT_PROF_FEE_PAID_TO_STUDENT_SUPERVISOR'
            ;"""
            sqlite_cursor.execute("DROP TABLE IF EXISTS " + sr_file)
            sqlite_cursor.execute(s_sql)
            sqlite_connection.commit()
            funcfile.writelog("%t BUILD TABLE: " + sr_file)

        # ADD CONTACT DETAILS TO FINDINGS
        sr_file = "X001ah_detail"
        sqlite_cursor.execute("DROP TABLE IF EXISTS " + sr_file)
        if i_finding_before > 0 and i_finding_after > 0:
            if l_debug:
                print("Add contact details to findings...")
            s_sql = "CREATE TABLE " + sr_file + " AS " + """
            Select
                PREV.ORG,
                PREV.LOC,
                PREV.STUDENT,
                MASTER.STUDENT_NAME,
                PREV.FDOC_NBR,
                MASTER.TRANSACTION_DT,
                MASTER.CALC_AMOUNT,
                MASTER.TRN_LDGR_ENTR_DESC,
                MASTER.PAYEE_TYP_DESC,
                MASTER.INV_NBR,
                PREV.CALC_COST_STRING,
                MASTER.ORG_NM,
                MASTER.ACCOUNT_NM,
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
                PREV.EMP_INI,
                INI.NAME_ADDR As INAME,
                PREV.EMP_INI||'@nwu.ac.za' As IMAIL,
                INI.EMAIL_ADDRESS As IMAIL2,
                PREV.ACC_MGR,
                ACCM.NAME_ADDR As ANAME,
                PREV.ACC_MGR||'@nwu.ac.za' As AMAIL,
                ACCM.EMAIL_ADDRESS As AMAIL2
            From
                X001ad_add_previous PREV
                Left Join X001af_officer CAMP_OFF On CAMP_OFF.CAMPUS = PREV.LOC
                Left Join X001af_officer ORG_OFF On ORG_OFF.CAMPUS = PREV.ORG
                Left Join X001ag_supervisor CAMP_SUP On CAMP_SUP.CAMPUS = PREV.LOC
                Left Join X001ag_supervisor ORG_SUP On ORG_SUP.CAMPUS = PREV.ORG
                Left Join X001aa_professional_fee_student MASTER On MASTER.STUDENT = PREV.STUDENT And
                    MASTER.FDOC_NBR = PREV.FDOC_NBR And
                    MASTER.CALC_COST_STRING = PREV.CALC_COST_STRING
                Left Join PEOPLE.X002_PEOPLE_CURR INI On INI.EMPLOYEE_NUMBER = PREV.EMP_INI 
                Left Join PEOPLE.X002_PEOPLE_CURR ACCM On ACCM.EMPLOYEE_NUMBER = PREV.ACC_MGR 
            Where
              PREV.PREV_PROCESS IS NULL
            ;"""
            sqlite_cursor.execute("DROP TABLE IF EXISTS " + sr_file)
            sqlite_cursor.execute(s_sql)
            sqlite_connection.commit()
            funcfile.writelog("%t BUILD TABLE: " + sr_file)

        # BUILD THE FINAL TABLE FOR EXPORT AND REPORT
        sr_file = "X001ax_professional_fee_student"
        sqlite_cursor.execute("DROP TABLE IF EXISTS " + sr_file)
        if l_debug:
            print("Build the final report")
        if i_finding_before > 0 and i_finding_after > 0:
            s_sql = "CREATE TABLE " + sr_file + " AS " + """
            Select
                'PROFESSIONAL FEE PAID TO STUDENT' As Audit_finding,
                FIND.STUDENT As Student,
                FIND.STUDENT_NAME As Name,
                FIND.FDOC_NBR As Edoc,
                FIND.TRANSACTION_DT As Date,
                FIND.INV_NBR As Invoice,
                FIND.CALC_AMOUNT As Amount,
                FIND.PAYEE_TYP_DESC As Vendor_type,
                CASE
                    WHEN Instr(FIND.TRN_LDGR_ENTR_DESC,'<VATI-0>') > 0 THEN Substr(FIND.TRN_LDGR_ENTR_DESC,9) 
                    ELSE FIND.TRN_LDGR_ENTR_DESC
                END As Description,
                FIND.CALC_COST_STRING As Account,
                FIND.ORG_NM As Organization,
                FIND.ACCOUNT_NM As Account_name,
                FIND.EMP_INI As Initiator,
                FIND.INAME As Initiator_name,
                FIND.IMAIL As Initiator_mail,
                FIND.ACC_MGR As Acc_manager,
                FIND.ANAME As Acc_manager_name,
                FIND.AMAIL As Acc_manager_mail,
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
                X001ah_detail FIND
            ;"""
            sqlite_cursor.execute("DROP TABLE IF EXISTS " + sr_file)
            sqlite_cursor.execute(s_sql)
            sqlite_connection.commit()
            funcfile.writelog("%t BUILD TABLE: " + sr_file)
            # Export findings
            if l_export and funcsys.tablerowcount(sqlite_cursor, sr_file) > 0:
                if l_debug:
                    print("Export findings...")
                sx_path = re_path + funcdatn.get_current_year() + "/"
                sx_file = "Gltran_test_001ax_professional_fee_student_"
                sx_file_dated = sx_file + funcdatn.get_today_date_file()
                s_head = funccsv.get_colnames_sqlite(sqlite_connection, sr_file)
                funccsv.write_data(sqlite_connection, "main", sr_file, sx_path, sx_file, s_head)
                funccsv.write_data(sqlite_connection, "main", sr_file, sx_path, sx_file_dated, s_head)
                funcfile.writelog("%t EXPORT DATA: " + sx_path + sx_file)
        else:
            s_sql = "CREATE TABLE " + sr_file + " (" + """
            BLANK TEXT
            );"""
            sqlite_cursor.execute("DROP TABLE IF EXISTS " + sr_file)
            sqlite_cursor.execute(s_sql)
            sqlite_connection.commit()
            funcfile.writelog("%t BUILD TABLE: " + sr_file)

    """*****************************************************************************
    IA ACTUAL VS BUDGET
    *****************************************************************************"""

    # FILES NEEDED
    # X000_GL_trans

    # DEFAULT TRANSACTION OWNER PEOPLE

    # DECLARE TEST VARIABLES
    # i_finding_before = 0
    i_finding_after = 0
    s_description = "IA Actual vs budget"
    s_file_prefix: str = "X002a"
    s_file_name: str = "ia_actual_vs_budget"
    s_finding: str = "IA ACTUAL VS BUDGET"
    s_report_file: str = "202_reported.txt"

    # OBTAIN TEST RUN FLAG
    if functest.get_test_flag(sqlite_cursor, "KFS", "TEST " + s_finding, "RUN") == "FALSE":

        if l_debug:
            print('TEST DISABLED')
        funcfile.writelog("TEST " + s_finding + " DISABLED")

    else:

        # LOG
        funcfile.writelog("TEST " + s_finding)
        if l_debug:
            print("TEST " + s_finding)

        # OBTAIN MASTER DATA
        if l_debug:
            print("Obtain master data...")
        sr_file: str = s_file_prefix + "a_" + s_file_name
        sqlite_cursor.execute("DROP TABLE IF EXISTS " + sr_file)
        s_sql = "Create Table " + sr_file + " As " + """
        Select
            'NWU' ORG,
            GL.FIN_COA_CD As LOC,
            GL.ORG_NM,
            GL.ACCOUNT_NM,
            GL.CALC_COST_STRING,
            GL.FIN_OBJ_CD_NM,
            GL.FIN_BALANCE_TYP_CD,
            Count(GL.FDOC_NBR) As Count_FDOC_NBR,
            Total(GL.CALC_AMOUNT) As Total_CALC_AMOUNT
        From
            KFSCURR.X000_GL_trans GL
        Where
            GL.ACCOUNT_NM Like ("(4532%")
        Group By
            GL.ACCOUNT_NM,
            GL.CALC_COST_STRING,
            GL.FIN_OBJ_CD_NM,
            GL.FIN_BALANCE_TYP_CD
        Order By
            GL.ACCOUNT_NM,
            GL.FIN_OBJ_CD_NM
        ;"""
        sqlite_cursor.execute(s_sql)
        funcfile.writelog("%t BUILD TABLE: " + sr_file)
        if l_debug:
            sqlite_connection.commit()

        # IDENTIFY FINDINGS
        if l_debug:
            print("Identify findings...")
        sr_file = s_file_prefix + "b_finding"
        sqlite_cursor.execute("DROP TABLE IF EXISTS " + sr_file)
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            GL.ORG,
            GL.LOC,
            GL.ORG_NM,
            GL.ACCOUNT_NM,
            GL.CALC_COST_STRING,
            GL.FIN_OBJ_CD_NM,
            Cast(Case
                When ACT.Total_CALC_AMOUNT is null Then '0'
                When ACT.Total_CALC_AMOUNT = '' Then '0'
                Else ACT.Total_CALC_AMOUNT
            End As Int) As ACTUAL,
            Cast(Case
                When BUD.Total_CALC_AMOUNT is null Then '0'
                When BUD.Total_CALC_AMOUNT = '' Then '0'
                Else BUD.Total_CALC_AMOUNT
            End As Int) As BUDGET,
            cast( ACT.Total_CALC_AMOUNT/BUD.Total_CALC_AMOUNT*100 As Real) As PERCENT
        From
            %FILEP%%FILEN% GL Left Join
            %FILEP%%FILEN% ACT On ACT.CALC_COST_STRING = GL.CALC_COST_STRING
                    And ACT.FIN_BALANCE_TYP_CD = "AC" Left Join
            %FILEP%%FILEN% BUD On BUD.CALC_COST_STRING = GL.CALC_COST_STRING
                   And BUD.FIN_BALANCE_TYP_CD = "CB"
        Group By
            GL.CALC_COST_STRING
        Order By
            GL.FIN_OBJ_CD_NM
        ;"""
        s_sql = s_sql.replace("%FILEP%", s_file_prefix)
        s_sql = s_sql.replace("%FILEN%", "a_" + s_file_name)
        sqlite_cursor.execute(s_sql)
        funcfile.writelog("%t BUILD TABLE: " + sr_file)
        if l_debug:
            sqlite_connection.commit()

        # COUNT THE NUMBER OF FINDINGS
        if l_debug:
            print("Count the number of findings...")
        i_finding_before: int = funcsys.tablerowcount(sqlite_cursor, sr_file)
        funcfile.writelog("%t FINDING: " + str(i_finding_before) + " " + s_finding + " finding(s)")
        if l_debug:
            print("*** Found " + str(i_finding_before) + " exceptions ***")

        # GET PREVIOUS FINDINGS
        if i_finding_before > 0:
            functest.get_previous_finding(sqlite_cursor, ed_path, s_report_file, s_finding, "TIITT")
            if l_debug:
                sqlite_connection.commit()

        # SET PREVIOUS FINDINGS
        if i_finding_before > 0:
            functest.set_previous_finding(sqlite_cursor)
            if l_debug:
                sqlite_connection.commit()

        # ADD PREVIOUS FINDINGS
        sr_file = s_file_prefix + "d_addprev"
        sqlite_cursor.execute("DROP TABLE IF EXISTS " + sr_file)
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
                Z001ab_setprev PREV ON PREV.FIELD1 = FIND.CALC_COST_STRING And
                PREV.FIELD2 = FIND.ACTUAL And
                PREV.FIELD3 = FIND.BUDGET
            ;"""
            s_sql = s_sql.replace("%FINDING%", s_finding)
            s_sql = s_sql.replace("%FILEP%", s_file_prefix)
            s_sql = s_sql.replace("%TODAY%", funcdatn.get_today_date())
            s_sql = s_sql.replace("%DATETEST%", funcdatn.get_current_month_end_next(0))
            sqlite_cursor.execute(s_sql)
            funcfile.writelog("%t BUILD TABLE: " + sr_file)
            if l_debug:
                sqlite_connection.commit()

        # BUILD LIST TO UPDATE FINDINGS
        sr_file = s_file_prefix + "e_newprev"
        sqlite_cursor.execute("DROP TABLE IF EXISTS " + sr_file)
        if i_finding_before > 0:
            if l_debug:
                print("Build list to update findings...")
            s_sql = "CREATE TABLE " + sr_file + " AS " + """
            Select
                PREV.PROCESS,
                PREV.CALC_COST_STRING AS FIELD1,
                PREV.ACTUAL AS FIELD2,
                PREV.BUDGET AS FIELD3,
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
            sqlite_cursor.execute(s_sql)
            funcfile.writelog("%t BUILD TABLE: " + sr_file)
            if l_debug:
                sqlite_connection.commit()
            # Export findings to previous reported file
            i_finding_after = funcsys.tablerowcount(sqlite_cursor, sr_file)
            if i_finding_after > 0:
                if l_debug:
                    print("*** " + str(i_finding_after) + " Finding(s) to report ***")
                sx_path = ed_path
                sx_file = s_report_file[:-4]
                # Read the header data
                s_head = funccsv.get_colnames_sqlite(sqlite_connection, sr_file)
                # Write the data
                if l_record:
                    funccsv.write_data(sqlite_connection, "main", sr_file, sx_path, sx_file, s_head, "a", ".txt")
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
            functest.get_officer(sqlite_cursor, "KFS", "TEST " + s_finding + " OFFICER")
            sqlite_connection.commit()

        # IMPORT SUPERVISORS FOR MAIL REPORTING PURPOSES
        if i_finding_before > 0 and i_finding_after > 0:
            functest.get_supervisor(sqlite_cursor, "KFS", "TEST " + s_finding + " SUPERVISOR")
            sqlite_connection.commit()

        # ADD CONTACT DETAILS TO FINDINGS
        sr_file = s_file_prefix + "h_detail"
        sqlite_cursor.execute("DROP TABLE IF EXISTS " + sr_file)
        if i_finding_before > 0 and i_finding_after > 0:
            if l_debug:
                print("Add contact details to findings...")
            s_sql = "CREATE TABLE " + sr_file + " AS " + """
            Select
                PREV.ORG,
                PREV.LOC,
                PREV.ORG_NM,
                PREV.ACCOUNT_NM,
                PREV.CALC_COST_STRING,
                PREV.FIN_OBJ_CD_NM,
                PREV.ACTUAL,
                PREV.BUDGET,
                PREV.PERCENT,
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
                Z001af_officer CAMP_OFF On CAMP_OFF.CAMPUS = PREV.LOC Left Join
                Z001af_officer ORG_OFF On ORG_OFF.CAMPUS = PREV.ORG Left Join
                Z001af_officer AUD_OFF On AUD_OFF.CAMPUS = 'AUD' Left Join
                Z001ag_supervisor CAMP_SUP On CAMP_SUP.CAMPUS = PREV.LOC Left Join
                Z001ag_supervisor ORG_SUP On ORG_SUP.CAMPUS = PREV.ORG Left Join
                Z001ag_supervisor AUD_SUP On AUD_SUP.CAMPUS = 'AUD'                    
            Where
                PREV.PREV_PROCESS Is Null Or
                PREV.DATE_REPORTED > PREV.PREV_DATE_RETEST And PREV.REMARK = ""
            ;"""
            s_sql = s_sql.replace("%FILEP%", s_file_prefix)
            s_sql = s_sql.replace("%FILEN%", s_file_name)
            sqlite_cursor.execute(s_sql)
            sqlite_connection.commit()
            funcfile.writelog("%t BUILD TABLE: " + sr_file)

        # BUILD THE FINAL TABLE FOR EXPORT AND REPORT
        sr_file = s_file_prefix + "x_" + s_file_name
        sqlite_cursor.execute("DROP TABLE IF EXISTS " + sr_file)
        if i_finding_before > 0 and i_finding_after > 0:
            if l_debug:
                print("Build the final report")
            s_sql = "CREATE TABLE " + sr_file + " AS " + """
            Select
                '%FIND%' As Audit_finding,
                FIND.ORG As Organization,
                FIND.LOC As Campus,
                FIND.ORG_NM As Division,
                FIND.ACCOUNT_NM As Account,
                FIND.CALC_COST_STRING As Cost_string,
                FIND.FIN_OBJ_CD_NM As Object_name,
                FIND.ACTUAL As R_Actual,
                FIND.BUDGET As R_Budget,
                FIND.PERCENT As Per_Used,                
                FIND.CAMP_OFF_NAME AS Responsible_officer,
                FIND.CAMP_OFF_NUMB AS Responsible_officer_numb,
                FIND.CAMP_OFF_MAIL1 AS Responsible_officer_mail,
                FIND.CAMP_OFF_MAIL2 AS Responsible_officer_mail_alt,
                FIND.CAMP_SUP_NAME AS Supervisor,
                FIND.CAMP_SUP_NUMB AS Supervisor_numb,
                FIND.CAMP_SUP_MAIL1 AS Supervisor_mail,
                FIND.ORG_OFF_NAME AS Org_officer,
                FIND.ORG_OFF_NUMB AS Org_officer_numb,
                FIND.ORG_OFF_MAIL1 AS Org_officer_mail,
                FIND.ORG_SUP_NAME AS Org_supervisor,
                FIND.ORG_SUP_NUMB AS Org_supervisor_numb,
                FIND.ORG_SUP_MAIL1 AS Org_supervisor_mail,
                FIND.AUD_OFF_NAME AS Audit_officer,
                FIND.AUD_OFF_NUMB AS Audit_officer_numb,
                FIND.AUD_OFF_MAIL AS Audit_officer_mail,
                FIND.AUD_SUP_NAME AS Audit_supervisor,
                FIND.AUD_SUP_NUMB AS Audit_supervisor_numb,
                FIND.AUD_SUP_MAIL AS Audit_supervisor_mail
            From
                %FILEP%h_detail FIND
            ;"""
            s_sql = s_sql.replace("%FIND%", s_finding)
            s_sql = s_sql.replace("%FILEP%", s_file_prefix)
            sqlite_cursor.execute(s_sql)
            sqlite_connection.commit()
            funcfile.writelog("%t BUILD TABLE: " + sr_file)
            # Export findings
            if l_export and funcsys.tablerowcount(sqlite_cursor, sr_file) > 0:
                if l_debug:
                    print("Export findings...")
                sx_path = re_path + funcdatn.get_current_year() + "/"
                sx_file = s_file_prefix + "_" + s_finding.lower() + "_"
                sx_file_dated = sx_file + funcdatn.get_today_date_file()
                s_head = funccsv.get_colnames_sqlite(sqlite_connection, sr_file)
                funccsv.write_data(sqlite_connection, "main", sr_file, sx_path, sx_file, s_head)
                funccsv.write_data(sqlite_connection, "main", sr_file, sx_path, sx_file_dated, s_head)
                funcfile.writelog("%t EXPORT DATA: " + sx_path + sx_file)
        else:
            s_sql = "CREATE TABLE " + sr_file + " (" + """
            BLANK TEXT
            );"""
            sqlite_cursor.execute("DROP TABLE IF EXISTS " + sr_file)
            sqlite_cursor.execute(s_sql)
            sqlite_connection.commit()
            funcfile.writelog("%t BUILD TABLE: " + sr_file)

    """ ****************************************************************************
    DONOR GL TRANSACTION IDENTIFICATION
    *****************************************************************************"""

    if l_debug:
        print("DONOR GL TRANSACTION IDENTIFICATION")
    funcfile.writelog("DONOR GL TRANSACTION IDENTIFICATION")

    # Extract gl transactions as needed
    if l_debug:
        print('Obtain the 2022 transactions')
    sr_file: str = "X003aa_donor_transaction"
    s_sql = f"CREATE TABLE {sr_file} AS " + f"""
    Select
        gl.UNIV_FISCAL_YR,
        gl.UNIV_FISCAL_PRD_CD,
        gl.CALC_COST_STRING,
        gl.ORG_NM,
        gl.ACCOUNT_NM,
        gl.FIN_OBJ_CD_NM,
        gl.TRANSACTION_DT,
        gl.FDOC_NBR,
        gl.CALC_AMOUNT,
        Upper(gl.TRN_LDGR_ENTR_DESC) As TRN_LDGR_ENTR_DESC,
        '' As GL_DESCRIPTION,
        Cast('0' As Int) As RECEIPT,
        Cast('0' As Int) As RECEIPT1,
        Cast('0' As Int) As RECEIPT2,        
        gl.ACCT_TYP_NM,
        gl.TRN_POST_DT,
        gl."TIMESTAMP",
        gl.FIN_COA_CD,
        gl.ACCOUNT_NBR,
        gl.FIN_OBJECT_CD,
        gl.FIN_BALANCE_TYP_CD,
        gl.FIN_OBJ_TYP_CD,
        gl.FDOC_TYP_CD,
        gl.FS_ORIGIN_CD,
        Upper(gl.FS_DATABASE_DESC) As FS_DATABASE_DESC,
        gl.TRN_ENTR_SEQ_NBR
    From
        KFS2022.X000_GL_trans gl
    Where
        (gl.FIN_OBJECT_CD In ('6251', '5754', '5752') And
            gl.FIN_OBJ_TYP_CD = 'IN' And
            gl.FS_DATABASE_DESC Like("RECEIPTS") And
            gl.CALC_AMOUNT < 0 And
            gl.TRANSACTION_DT >= '2022-06-01') Or
        (gl.FIN_OBJECT_CD In ('7502', '7503') And
            gl.CALC_AMOUNT < 0 And
            gl.FDOC_TYP_CD = 'APP' And
            gl.TRANSACTION_DT >= '2022-06-01')
    ;"""
    sqlite_cursor.execute("DROP TABLE IF EXISTS " + sr_file)
    sqlite_cursor.execute(s_sql)
    sqlite_connection.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # Add the 2023 data
    if l_debug:
        print("Add the 2023 transactions")
    s_sql = f'''
    Insert Into {sr_file} (
        UNIV_FISCAL_YR,
        UNIV_FISCAL_PRD_CD,
        CALC_COST_STRING,
        ORG_NM,
        ACCOUNT_NM,
        FIN_OBJ_CD_NM,
        TRANSACTION_DT,
        FDOC_NBR,
        CALC_AMOUNT,
        TRN_LDGR_ENTR_DESC,
        GL_DESCRIPTION,
        RECEIPT,
        RECEIPT1,
        RECEIPT2,        
        ACCT_TYP_NM,
        TRN_POST_DT,
        "TIMESTAMP",
        FIN_COA_CD,
        ACCOUNT_NBR,
        FIN_OBJECT_CD,
        FIN_BALANCE_TYP_CD,
        FIN_OBJ_TYP_CD,
        FDOC_TYP_CD,
        FS_ORIGIN_CD,
        FS_DATABASE_DESC,
        TRN_ENTR_SEQ_NBR
    )
    Select
        gl.UNIV_FISCAL_YR,
        gl.UNIV_FISCAL_PRD_CD,
        gl.CALC_COST_STRING,
        gl.ORG_NM,
        gl.ACCOUNT_NM,
        gl.FIN_OBJ_CD_NM,
        gl.TRANSACTION_DT,
        gl.FDOC_NBR,
        gl.CALC_AMOUNT,
        Upper(gl.TRN_LDGR_ENTR_DESC) As TRN_LDGR_ENTR_DESC,
        '' As GL_DESCRIPTION,
        Cast('0' As Int) As RECEIPT,
        Cast('0' As Int) As RECEIPT1,
        Cast('0' As Int) As RECEIPT2,        
        gl.ACCT_TYP_NM,
        gl.TRN_POST_DT,
        gl."TIMESTAMP",
        gl.FIN_COA_CD,
        gl.ACCOUNT_NBR,
        gl.FIN_OBJECT_CD,
        gl.FIN_BALANCE_TYP_CD,
        gl.FIN_OBJ_TYP_CD,
        gl.FDOC_TYP_CD,
        gl.FS_ORIGIN_CD,
        Upper(gl.FS_DATABASE_DESC) As FS_DATABASE_DESC,
        gl.TRN_ENTR_SEQ_NBR
    From
        KFSPREV.X000_GL_trans gl
    Where
        (gl.FIN_OBJECT_CD In ('6251', '5754', '5752') And
            gl.FIN_OBJ_TYP_CD = 'IN' And
            gl.CALC_AMOUNT < 0) Or
        (gl.FIN_OBJECT_CD In ('7502', '7503') And
            gl.CALC_AMOUNT < 0 And
            gl.FDOC_TYP_CD = 'APP')
    '''
    sqlite_cursor.execute(s_sql)
    sqlite_connection.commit()


    # Add the 2024 data
    if l_debug:
        print("Add the 2024 transactions")
    s_sql = f'''
    Insert Into {sr_file} (
        UNIV_FISCAL_YR,
        UNIV_FISCAL_PRD_CD,
        CALC_COST_STRING,
        ORG_NM,
        ACCOUNT_NM,
        FIN_OBJ_CD_NM,
        TRANSACTION_DT,
        FDOC_NBR,
        CALC_AMOUNT,
        TRN_LDGR_ENTR_DESC,
        GL_DESCRIPTION,
        RECEIPT,
        RECEIPT1,
        RECEIPT2,        
        ACCT_TYP_NM,
        TRN_POST_DT,
        "TIMESTAMP",
        FIN_COA_CD,
        ACCOUNT_NBR,
        FIN_OBJECT_CD,
        FIN_BALANCE_TYP_CD,
        FIN_OBJ_TYP_CD,
        FDOC_TYP_CD,
        FS_ORIGIN_CD,
        FS_DATABASE_DESC,
        TRN_ENTR_SEQ_NBR
    )
    Select
        gl.UNIV_FISCAL_YR,
        gl.UNIV_FISCAL_PRD_CD,
        gl.CALC_COST_STRING,
        gl.ORG_NM,
        gl.ACCOUNT_NM,
        gl.FIN_OBJ_CD_NM,
        gl.TRANSACTION_DT,
        gl.FDOC_NBR,
        gl.CALC_AMOUNT,
        Upper(gl.TRN_LDGR_ENTR_DESC) As TRN_LDGR_ENTR_DESC,
        '' As GL_DESCRIPTION,
        Cast('0' As Int) As RECEIPT,
        Cast('0' As Int) As RECEIPT1,
        Cast('0' As Int) As RECEIPT2,        
        gl.ACCT_TYP_NM,
        gl.TRN_POST_DT,
        gl."TIMESTAMP",
        gl.FIN_COA_CD,
        gl.ACCOUNT_NBR,
        gl.FIN_OBJECT_CD,
        gl.FIN_BALANCE_TYP_CD,
        gl.FIN_OBJ_TYP_CD,
        gl.FDOC_TYP_CD,
        gl.FS_ORIGIN_CD,
        Upper(gl.FS_DATABASE_DESC) As FS_DATABASE_DESC,
        gl.TRN_ENTR_SEQ_NBR
    From
        KFSCURR.X000_GL_trans gl
    Where
        (gl.FIN_OBJECT_CD In ('6251', '5754', '5752') And
            gl.FIN_OBJ_TYP_CD = 'IN' And
            gl.CALC_AMOUNT < 0 And
            gl.TRANSACTION_DT <= '2024-05-31') Or
        (gl.FIN_OBJECT_CD In ('7502', '7503') And
            gl.CALC_AMOUNT < 0 And
            gl.FDOC_TYP_CD = 'APP' And
            gl.TRANSACTION_DT <= '2024-05-31')
    '''
    sqlite_cursor.execute(s_sql)
    sqlite_connection.commit()

    # Extract the first receipt number from the gl description
    if l_debug:
        print('Extract the first receipt number')
    s_sql = f"""
    UPDATE {sr_file}
    SET RECEIPT1 = CAST(
                        SUBSTR(TRN_LDGR_ENTR_DESC, 
                            INSTR(TRN_LDGR_ENTR_DESC, ' ') + 1, 
                            INSTR(SUBSTR(TRN_LDGR_ENTR_DESC, INSTR(TRN_LDGR_ENTR_DESC, ' ') + 1), ' ')) 
                       AS INTEGER)
    WHERE FS_ORIGIN_CD = '09'
    """
    # Execute the SQL statement
    sqlite_cursor.execute(s_sql)

    # Extract the second receipt number from the gl description
    if l_debug:
        print('Extract the second receipt number')
    s_sql = f"""
    UPDATE {sr_file}
    SET RECEIPT2 = CAST(
                        SUBSTR(TRN_LDGR_ENTR_DESC,
                               INSTR(TRN_LDGR_ENTR_DESC, '(') + 1,
                               INSTR(TRN_LDGR_ENTR_DESC, ')') - INSTR(TRN_LDGR_ENTR_DESC, '(') - 1) 
                       AS INTEGER)
    WHERE TRN_LDGR_ENTR_DESC Like ('VSS RECEIPT%')
    """
    # Execute the SQL statement
    sqlite_cursor.execute(s_sql)

    # Build one receipt number column
    if l_debug:
        print('Combine the receipt numbers')
    s_sql = f"""
    UPDATE {sr_file}
    SET RECEIPT =
    Case
     When RECEIPT1 > 0 Then RECEIPT1
     When RECEIPT2 > 0 Then RECEIPT2
     Else 0
    End
    """
    # Execute the SQL statement
    sqlite_cursor.execute(s_sql)
    sqlite_connection.commit()

    # Add receipt detail to the gl transactions
    if l_debug:
        print('Add the VSS receipt detail to the gl transactions')
    sr_file: str = "X003ab_donor_receipt"
    s_sql = "CREATE TABLE " + sr_file + " AS " + f"""
    Select
        gl.UNIV_FISCAL_YR,
        gl.UNIV_FISCAL_PRD_CD,
        gl.CALC_COST_STRING,
        gl.ORG_NM,
        gl.ACCOUNT_NM,
        gl.FIN_OBJ_CD_NM,
        gl.TRANSACTION_DT,
        gl.FDOC_NBR,
        gl.CALC_AMOUNT,
        gl.TRN_LDGR_ENTR_DESC,
        gl.GL_DESCRIPTION,
        gl.RECEIPT,
        gl.RECEIPT1,
        gl.RECEIPT2,
        gl.ACCT_TYP_NM,
        gl.TRN_POST_DT,
        gl."TIMESTAMP",
        gl.FIN_COA_CD,
        gl.ACCOUNT_NBR,
        gl.FIN_OBJECT_CD,
        gl.FIN_BALANCE_TYP_CD,
        gl.FIN_OBJ_TYP_CD,
        gl.FDOC_TYP_CD,
        gl.FS_ORIGIN_CD,
        gl.FS_DATABASE_DESC,
        gl.TRN_ENTR_SEQ_NBR,
        rc.RECEIPTDATE,
        rc.KRECEIPTID,
        Upper(rc.NAME) As NAME,
        Cast('0' As Int) As EXCLUDE,
        '' As DONOR_ID,
        gl.FDOC_NBR ||'-'|| gl.RECEIPT || Cast(gl.CALC_AMOUNT As TEXT) As UNIQUE_ID              
    From
        X003aa_donor_transaction gl Left Join
        VSSCURR.RECEIPT rc On rc.RECEIPTNO = gl.RECEIPT
            And Date(rc.RECEIPTDATE) = Date(gl.TRANSACTION_DT)        
    ;"""
    sqlite_cursor.execute("DROP TABLE IF EXISTS " + sr_file)
    sqlite_cursor.execute(s_sql)
    sqlite_connection.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # Read the list of words to exclude in the vendor names
    words_to_remove = funcstat.stat_list(sqlite_cursor,
                                           "KFS.X000_Own_kfs_lookups",
                                           "LOOKUP_CODE",
                                           "LOOKUP='EXCLUDE GL DONOR WORD'")

    if l_debug:
        print("WORDS TO EXCLUDE:")
        print(words_to_remove)
        pass

    # CLEAN THE GL DESCRIPTION COLUMN

    # Select the dirty data from the table
    sqlite_cursor.execute(f"Select rowid, TRN_LDGR_ENTR_DESC From {sr_file}")
    rows = sqlite_cursor.fetchall()

    # Clean the data and update the clean_data column
    for row in rows:
        row_id = row[0]
        dirty_data = row[1]
        clean_data = funcstr.clean_paragraph(dirty_data, words_to_remove, 'a')

        # Update the clean_data column with the cleaned data
        sqlite_cursor.execute(f"UPDATE {sr_file} SET GL_DESCRIPTION = ? WHERE rowid = ?", (clean_data, row_id))

    # Commit the changes and close the connection
    sqlite_connection.commit()

    # IDENTIFY DONORS

    # Read the list of donors to identify
    records_to_identify = funcstat.stat_list_reverse(sqlite_cursor,
                                             "KFS.X000_Own_kfs_lookups",
                                             "LOOKUP_CODE",
                                             "LOOKUP='IDENTIFY GL DONOR RECORD'")

    if l_debug:
        print("DONORS TO INCLUDE:")
        print(records_to_identify)
        pass

    # Construct the SQL query with NOT LIKE for each word
    query_parts = [f'GL_DESCRIPTION LIKE "%{word}%"' for word in records_to_identify]
    inclusion_criteria = ' Or '.join(query_parts)

    # Query to fetch all the descriptions from the general ledger
    s_sql = f"SELECT rowid, GL_DESCRIPTION FROM {sr_file} WHERE {inclusion_criteria}"
    sqlite_cursor.execute(s_sql)

    # Fetch all the rows in the general ledger
    rows = sqlite_cursor.fetchall()

    # Iterate over each row and check if any company name is contained in the description
    # matches = []
    for row in rows:
        record_id = row[0]
        gl_description = row[1]
        for company_name in records_to_identify:
            if company_name.upper() in gl_description:  # Case-insensitive match
                if l_debug:
                    # print(company_name)
                    # print(gl_description)
                    pass
                # matches.append((row[0], company_name))
                # Update the clean_data column with the cleaned data
                sqlite_cursor.execute(f"UPDATE {sr_file} SET EXCLUDE = 1, DONOR_ID = ? WHERE rowid = ?", (company_name, record_id))
                break  # Stop looking for other company names if a match is found

    """
    if l_debug:
        # print(matches)
        pass
    """

    # Commit the changes and close the connection
    sqlite_connection.commit()

    # EXCLUDE DONORS

    # Read the list of words to exclude in the vendor names
    records_to_remove = funcstat.stat_list_reverse(sqlite_cursor,
                                           "KFS.X000_Own_kfs_lookups",
                                           "LOOKUP_CODE",
                                           "LOOKUP='EXCLUDE GL DONOR RECORD'")

    if l_debug:
        print("WORDS TO EXCLUDE:")
        print(records_to_remove)
        pass

    # Construct the SQL query with NOT LIKE for each word
    query_parts = [f'GL_DESCRIPTION LIKE "%{word}%"' for word in records_to_remove]
    # Test on receipt name too
    # query_parts = [f'GL_DESCRIPTION LIKE "%{word}%" Or NAME LIKE "%{word}%"' for word in records_to_remove]
    exclusion_criteria = ' Or '.join(query_parts)

    if l_debug:
        print("EXCLUSION CRITERIA:")
        print(exclusion_criteria)
        pass

    # Update the table with excluded records
    if l_debug:
        print('Mark records for exclusion')
    s_sql = f"""
    UPDATE {sr_file}
    SET EXCLUDE = 
    Case
        When {exclusion_criteria} Then 2
        Else 0
    End
    Where
        EXCLUDE = 0
    """
    # Execute the SQL statement
    sqlite_cursor.execute(s_sql)
    sqlite_connection.commit()

    # Extract the identified donors

    # Add receipt detail to the gl transactions
    if l_debug:
        print('Add the VSS receipt detail to the gl transactions')
    sr_file: str = "X003ac_donor_all_transaction"
    sqlite_cursor.execute("DROP TABLE IF EXISTS " + sr_file)
    sr_file: str = "X003ax_donor_all_transaction"
    s_sql = "CREATE TABLE " + sr_file + " AS " + f"""
    Select
        don.FDOC_NBR,
        don.UNIV_FISCAL_YR As YEAR,
        don.UNIV_FISCAL_PRD_CD As MONTH,
        don.TRANSACTION_DT As DATE,
        don.CALC_AMOUNT As AMOUNT,
        don.TRN_LDGR_ENTR_DESC As DESCRIPTION_RAW,
        don.GL_DESCRIPTION As DESCRIPTION_CLEANED,
        don."EXCLUDE" As STATUS_ID,
        Case
            When don."EXCLUDE" = 1 Then "DONOR IDENTIFIED"
            When don."EXCLUDE" = 2 Then "EXCLUDED FROM DONOR IDENTIFICATION"
            Else "DONOR TO BE IDENTIFIED IN GL TRANSACTION"
        END As STATUS,
        don.DONOR_ID,
        own.LOOKUP_DESCRIPTION As DONOR,
        don.CALC_COST_STRING As COST_STRING,
        don.ORG_NM As ORG_NAME,
        don.ACCOUNT_NM As ACCOUNT_NAME,
        don.FIN_OBJ_CD_NM As OBJECT_NAME,
        don.ACCT_TYP_NM As ACCOUNT_TYPE_NAME,
        don.TRN_POST_DT As DATE_POSTED,
        don."TIMESTAMP" As TIME_POSTED,
        don.FIN_BALANCE_TYP_CD,
        don.FIN_OBJ_TYP_CD,
        don.FDOC_TYP_CD,
        don.FS_ORIGIN_CD,
        don.FS_DATABASE_DESC,
        don.RECEIPT,
        don.KRECEIPTID,
        don.RECEIPTDATE,
        don.NAME
    From
        X003ab_donor_receipt don Left Join
        X000_Own_kfs_lookups own On own.LOOKUP_CODE = don.DONOR_ID And own.LOOKUP = "IDENTIFY GL DONOR RECORD"
    Order By
        don.TRANSACTION_DT,
        don.FDOC_NBR
    ;"""
    sqlite_cursor.execute("DROP TABLE IF EXISTS " + sr_file)
    sqlite_cursor.execute(s_sql)
    sqlite_connection.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    """ ****************************************************************************
    END OF SCRIPT
    *****************************************************************************"""
    if l_debug:
        print("END OF SCRIPT")
    funcfile.writelog("END OF SCRIPT")

    # CLOSE THE DATABASE CONNECTION
    sqlite_connection.commit()
    sqlite_connection.close()

    # CLOSE THE LOG WRITER
    funcfile.writelog("------------------------------------")
    funcfile.writelog("COMPLETED: C202_GL_TEST_TRANSACTIONS")

    return


if __name__ == '__main__':
    try:
        gl_test_transactions()
    except Exception as e:
        funcsys.ErrMessage(e, funcconf.l_mess_project, "C202_gl_test_transactions", "C202_gl_test_transactions")
