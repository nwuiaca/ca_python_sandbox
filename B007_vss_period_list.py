"""
Script to build standard VSS period lists
Created on: 6 Jan 2020
Created by: AB Janse van Rensburg (NWU:21162395)
"""

# IMPORT PYTHON MODULES
import csv
import sqlite3

# IMPORT OWN MODULES
from _my_modules import funcconf
from _my_modules import funcdatn
from _my_modules import funcfile
from _my_modules import funcstudent
from _my_modules import funcsms
from _my_modules import funcsys

""" INDEX **********************************************************************
ENVIRONMENT
BUILD STANDARD LOOKUP TABLES
BUILD STUDENTS
STUDENT ACCOUNT TRANSACTIONS
*****************************************************************************"""


def vss_period_list(s_period="curr"):
    """
    Script to build standard KFS lists
    :type s_period: str: The financial period (curr, prev or year)
    :return: Nothing
    """

    # DECLARE VARIABLES
    l_debug: bool = False
    l_message: bool = funcconf.l_mess_project
    # l_message: bool = False

    """*************************************************************************
    ENVIRONMENT
    *************************************************************************"""
    print("ENVIRONMENT")
    funcfile.writelog("ENVIRONMENT")

    # DECLARE VARIABLES
    l_debug: bool = False
    so_path: str = f"{funcconf.drive_data_raw}Vss/"  # Source database path
    if s_period == "curr":
        s_year = funcdatn.get_current_year()
        so_file = "Vss_curr.sqlite"  # Source database
    elif s_period == "prev":
        s_year = funcdatn.get_previous_year()
        so_file = "Vss_prev.sqlite"  # Source database
    else:
        s_year = s_period
        so_file = "Vss_" + s_year + ".sqlite"  # Source database
    re_path: str = f"{funcconf.drive_data_results}Vss/"  # Results
    ed_path: str = f"{funcconf.drive_system}_external_data/"  # External data location
    s_sql: str = ""  # SQL statements
    l_export: bool = False  # Export files

    # LOG
    print("--------------------")
    print("B007_VSS_PERIOD_LIST")
    print("--------------------")
    funcfile.writelog("Now")
    funcfile.writelog("SCRIPT: B007_VSS_PERIOD_LIST")
    funcfile.writelog("----------------------------")

    # MESSAGE
    if l_message:
        funcsms.send_telegram("", "administrator", "<b>B007 Student " + s_year + " period lists</b>")

    # OPEN DATABASE
    with sqlite3.connect(so_path+so_file) as so_conn:
        so_curs = so_conn.cursor()
    funcfile.writelog("OPEN DATABASE: " + so_file)

    # ATTACH DATA SOURCES
    so_curs.execute(f"ATTACH DATABASE '{funcconf.drive_data_raw}Vss/Vss.sqlite' AS 'VSS'")
    funcfile.writelog("%t ATTACH DATABASE: VSS.SQLITE")

    """*************************************************************************
    BUILD STANDARD LOOKUP TABLES
    *************************************************************************"""
    print("BUILD STANDARD LOOKUP TABLES")
    funcfile.writelog("BUILD STANDARD LOOKUP TABLES")

    # CREATE STUDENT MODULE RESULT MASTER
    sr_file = "X000_Student_module_result"
    print("Build student module result master...")
    s_sql = "Create Table " + sr_file + " As " + """
    Select
        RESM.KFINALMODULERESULTID,
        RESM.KENROLSTUDID,
        RESM.KSTUDBUSENTID,
        RESM.KSTUDASSESID,
        RESM.FRESULTMASTERID,
        RESM.FRESULTCODEID,
        Upper(RESC.LONG) As RESULT_TYPE,
        RESM.FMARKTYPECODEID,
        Upper(MARC.LONG) As MARK_TYPE,
        RESM.DATEACHIEVED,
        RESM.MARKACHIEVED,
        RESM.ISPARTICIPATIONMARK,
        RESM.OPPNO,
        RESM.ISMARKCHANGED,
        RESM.LOCKSTAMP,
        RESM.AUDITDATETIME,
        RESM.FAUDITSYSTEMFUNCTIONID,
        RESM.FAUDITUSERCODE
    From
        FINALMODULERESULT RESM Left Join
        VSS.X000_Codedescription RESC On RESC.KCODEDESCID = RESM.FRESULTCODEID Left Join
        VSS.X000_Codedescription MARC On MARC.KCODEDESCID = RESM.FMARKTYPECODEID
    Order By
        KSTUDBUSENTID,
        KENROLSTUDID,
        DATEACHIEVED    
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD VIEW: " + sr_file)

    # CREATE STUDENT MODULE PARTICIPATION RESULT MASTER
    sr_file = "X000_Student_module_result_participate"
    print("Build student module participation result master...")
    s_sql = "Create View " + sr_file + " As " + """
    Select
        RESU.KENROLSTUDID,
        RESU.KSTUDBUSENTID,
        Case
            When Cast(RESU.MARKACHIEVED As INT) != 0 And RESU.FRESULTCODEID = 0 Then ' MARK (' || Trim(RESU.MARKACHIEVED) || ')'
            When Cast(RESU.MARKACHIEVED As INT) != 0 Then Trim(RESU.RESULT_TYPE) || ' (' || Trim(RESU.MARKACHIEVED) || ')'
            When RESU.FRESULTCODEID = 0 Then 'NONE'
            Else RESU.RESULT_TYPE
        End As PART_RESU,
        Max(RESU.DATEACHIEVED) As DATEACHIEVED
    From
        X000_Student_module_result RESU
    Where
        RESU.MARK_TYPE Like ('PART%')
    Group By
        RESU.KENROLSTUDID
    ;"""
    so_curs.execute("DROP VIEW IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD VIEW: " + sr_file)

    # CREATE STUDENT MODULE EXAM RESULT MASTER
    sr_file = "X000_Student_module_result_exam"
    print("Build student module exam result master...")
    s_sql = "Create View " + sr_file + " As " + """
    Select
        RESU.KENROLSTUDID,
        RESU.KSTUDBUSENTID,
        Case
            When Cast(RESU.MARKACHIEVED As INT) != 0 And RESU.FRESULTCODEID = 0 Then ' MARK (' || Trim(RESU.MARKACHIEVED) || ')'
            When Cast(RESU.MARKACHIEVED As INT) != 0 Then Trim(RESU.RESULT_TYPE) || ' (' || Trim(RESU.MARKACHIEVED) || ')'
            When RESU.FRESULTCODEID = 0 Then 'NONE'
            Else RESU.RESULT_TYPE
        End As EXAM_RESU,
        Max(RESU.DATEACHIEVED) As DATEACHIEVED
    From
        X000_Student_module_result RESU
    Where
        RESU.MARK_TYPE Like ('FINAL EXAM%')
    Group By
        RESU.KENROLSTUDID
    ;"""
    so_curs.execute("DROP VIEW IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD VIEW: " + sr_file)

    # CREATE STUDENT MODULE FINA RESULT MASTER
    sr_file = "X000_Student_module_result_final"
    print("Build student module final result master...")
    s_sql = "Create View " + sr_file + " As " + """
    Select
        RESU.KENROLSTUDID,
        RESU.KSTUDBUSENTID,
        Case
            When Cast(RESU.MARKACHIEVED As INT) != 0 And RESU.FRESULTCODEID = 0 Then ' MARK (' || Trim(RESU.MARKACHIEVED) || ')'
            When Cast(RESU.MARKACHIEVED As INT) != 0 Then Trim(RESU.RESULT_TYPE) || ' (' || Trim(RESU.MARKACHIEVED) || ')'
            When RESU.FRESULTCODEID = 0 Then 'NONE'
            Else RESU.RESULT_TYPE
        End As FINAL_RESU,
        Max(RESU.DATEACHIEVED) As DATEACHIEVED
    From
        X000_Student_module_result RESU
    Where
        RESU.MARK_TYPE Like ('FINAL MARK%')
    Group By
        RESU.KENROLSTUDID
    ;"""
    so_curs.execute("DROP VIEW IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD VIEW: " + sr_file)

    """*************************************************************************
    BUILD STUDENTS
    *************************************************************************"""
    print("BUILD STUDENTS")
    funcfile.writelog("BUILD STUDENTS")

    funcstudent.studentlist(so_conn, re_path, s_period, True)

    """*************************************************************************
    STUDENT ACCOUNT TRANSACTIONS
    *************************************************************************"""

    # BUILD CURRENT YEAR TRANSACTIONS
    print("Build transactions...")
    sr_file = "X010_Studytrans"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        TRAN.KACCTRANSID,
        TRAN.FACCID,
        STUD.FBUSENTID,
        TRAN.FSERVICESITE,
        TRAN.FDEBTCOLLECTIONSITE,
        TRAN.TRANSDATE,
        TRAN.AMOUNT,
        TRAN.FTRANSMASTERID,
        MAST.TRANSCODE,
        MAST.DESCRIPTION_E,
        MAST.DESCRIPTION_A,
        TRAN.TRANSDATETIME,
        TRAN.MONTHENDDATE,
        TRAN.POSTDATEDTRANSDATE,
        TRAN.FFINAIDSITEID,
        BURS.FINAIDCODE,
        BURS.FINAIDNAME,
        BURS.FINAIDNAAM,
        TRAN.FRESIDENCELOGID,
        TRAN.FLEVYLOGID,
        TRAN.FQUALLEVELAPID,
        QUAL.QUALIFICATION,
        QUAL.QUALIFICATION_NAME,
        TRAN.FMODAPID,
        MODU.FENROLMENTCATEGORYCODEID As ENROL_ID,
        MODU.ENROL_CATEGORY,        
        MODU.MODULE,
        MODU.MODULE_NAME,
        TRAN.FPROGAPID,
        TRAN.FENROLPRESID,
        TRAN.FRESIDENCEID,
        TRAN.FRECEIPTID,
        TRAN.FROOMTYPECODEID,
        TRAN.REFERENCENO,
        TRAN.FSUBACCTYPECODEID,
        TRAN.FDEPOSITCODEID,
        TRAN.FDEPOSITTYPECODEID,
        TRAN.FVARIABLEAMOUNTTYPECODEID,
        TRAN.FDEPOSITTRANSTYPECODEID,
        TRAN.RESIDENCETRANSTYPE,
        TRAN.FSTUDYTRANSTYPECODEID,
        TRAN.ISSHOWN,
        TRAN.ISCREATEDMANUALLY,
        TRAN.FTRANSINSTID,
        TRAN.FMONTHENDORGUNITNO,
        TRAN.LOCKSTAMP,
        TRAN.AUDITDATETIME,
        TRAN.FAUDITSYSTEMFUNCTIONID,
        SYSF.SYSTEM_DESC,
        TRAN.FAUDITUSERCODE,
        USER.FUSERBUSINESSENTITYID,
        TRAN.FORIGINSYSTEMFUNCTIONID,
        TRAN.FPAYMENTREQUESTID
    From
        STUDYTRANS TRAN Left Join
        VSS.STUDACC STUD ON STUD.KACCID = TRAN.FACCID Left Join
        VSS.X000_Transmaster MAST ON MAST.KTRANSMASTERID = TRAN.FTRANSMASTERID Left Join
        VSS.X000_Qualifications QUAL On QUAL.KENROLMENTPRESENTATIONID = TRAN.FENROLPRESID Left Join
        VSS.X000_Modules MODU On MODU.KENROLMENTPRESENTATIONID = TRAN.FENROLPRESID Left Join
        VSS.X004_Bursaries BURS ON BURS.KFINAIDSITEID = TRAN.FFINAIDSITEID Left Join
        VSS.SYSTEMUSER USER ON USER.KUSERCODE = TRAN.FAUDITUSERCODE Left Join
        VSS.X000_Systemfunction SYSF On SYSF.KSYSTEMFUNCTIONID = TRAN.FAUDITSYSTEMFUNCTIONID
    Order By
        TRAN.TRANSDATETIME
    """
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # MESSAGE
    if l_message:
        i = funcsys.tablerowcount(so_curs, sr_file)
        funcsms.send_telegram("", "administrator", "<b>" + str(i) + "</b> Transactions")

    # Close the connection *********************************************************
    so_conn.close()

    # Close the log writer *********************************************************
    funcfile.writelog("-------------------------")
    funcfile.writelog("COMPLETED: B007_VSS_PERIOD_LIST")

    return


if __name__ == '__main__':
    try:
        vss_period_list("curr")
    except Exception as e:
        funcsys.ErrMessage(e, l_message, "B007_vss_period_list", "B007_vss_period_list")
