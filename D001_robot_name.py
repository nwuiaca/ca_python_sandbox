"""
SEARCH THE PARTY FILE IN VSS FOR NAMES, NWU NUMBERS AND ID'S
Script: D001_robot_name.py
Author: Albert B Janse van Rensburg (NWU:21162395)
Created: 2 May 2020
"""

# IMPORT PYTHON MODULES
import sqlite3

# IMPORT OWN MODULES
from _my_modules import funcconf
from _my_modules import funcfile
from _my_modules import funcsms
from _my_modules import funcsys

# INDEX
"""
ENVIRONMENT
DO THE SEARCH
END OF SCRIPT
"""


def robot_name(s_list: str = "a", s_main: str = "", s_secondary: str = ""):
    """
    SEARCH VSS.PARTY FOR NAMES, NUMBERS AND ID'S
    :param s_list: List filter
    :param s_main: Main search parameter
    :param s_secondary: Secondary search parameter
    :return: str: Info in message format
    """

    # VARIABLES
    s_main = s_main.replace(".", " ")
    s_return_message: str = ""
    l_debug: bool = True

    """*************************************************************************
    ENVIRONMENT
    *************************************************************************"""
    funcfile.writelog("ENVIRONMENT")
    if l_debug:
        print("ENVIRONMENT")

    # DECLARE VARIABLES
    so_path: str = f"{funcconf.drive_data_raw}Vss/"  # Source database path
    so_file: str = "Vss.sqlite"  # Source database
    re_path: str = f"{funcconf.drive_data_results}Vss/"  # Results
    ed_path: str = f"{funcconf.drive_system}_external_data/"  # External data location
    s_sql: str = ""  # SQL statements
    l_export: bool = False  # Export files
    # l_mess: bool = funcconf.l_mess_project
    l_mess: bool = False

    # LOG
    funcfile.writelog("Now")
    funcfile.writelog("SCRIPT: D001_ROBOT_NAME")
    funcfile.writelog("-----------------------")
    if l_debug:
        print("---------------")
        print("D001_ROBOT_NAME")
        print("---------------")

    # MESSAGE
    if l_mess:
        funcsms.send_telegram("", "administrator", "<b>D001 ROBOT NAME</b>")

    # OPEN DATABASE
    with sqlite3.connect(so_path + so_file) as so_conn:
        so_curs = so_conn.cursor()
    funcfile.writelog("OPEN DATABASE: " + so_file)
    # ATTACH DATA SOURCES
    so_curs.execute(f"ATTACH DATABASE '{funcconf.drive_data_raw}People/People.sqlite' AS 'PEOPLE'")
    funcfile.writelog("%t ATTACH DATABASE: PEOPLE.SQLITE")
    so_curs.execute(f"ATTACH DATABASE '{funcconf.drive_data_raw}Kfs/Kfs_curr.sqlite' AS 'KFSCURR'")
    funcfile.writelog("%t ATTACH DATABASE: VSS_CURR.SQLITE")
    so_curs.execute(f"ATTACH DATABASE '{funcconf.drive_data_raw}Vss/Vss_curr.sqlite' AS 'VSSCURR'")
    funcfile.writelog("%t ATTACH DATABASE: VSS_CURR.SQLITE")
    """*****************************************************************************
    DO THE SEARCH
    *****************************************************************************"""
    funcfile.writelog("DO THE SEARCH")
    if l_debug:
        print("DO THE SEARCH")

    # BUILD THE WHERE CLAUSE
    s_where = "(%MAIN1% %SECONDARY% %LIST%) or (%MAIN2% %SECONDARY% %LIST%) or (%MAIN3% %SECONDARY% %LIST%)"

    # MAIN PARAMETER FULL_NAME
    if s_main[0:1] == "*":
        s_main1 = "PAR.FULL_NAME != ''"
    else:
        s_main1 = "PAR.FULL_NAME Like('%" + s_main.upper() + "%')"

    # MAIN PARAMETER NWU NUMBER
    if s_main[0:1] == "*":
        s_main2 = "PAR.KBUSINESSENTITYID > 0"
    else:
        s_main2 = "CAST(PAR.KBUSINESSENTITYID AS TEXT) Like('%" + s_main.upper() + "%')"

    # MAIN PARAMETER ID NUMBER
    if s_main[0:1] == "*":
        s_main3 = "PAR.IDNO != ''"
    else:
        s_main3 = "PAR.IDNO Like('%" + s_main.upper() + "%')"

    # SECONDARY PARAMETER NICKNAME
    if s_secondary[0:1] == "*":
        s_secondary1 = ""
    else:
        s_secondary1 = "and PAR.NICKNAME Like('%" + s_secondary.upper() + "%')"

    # TODO Sort out payee only searches. Take very long with no end result

    # LIST PARAMETER
    if s_list[0:1] == "e":  # Employee
        s_list1 = "and PEO.EMPLOYEE_NUMBER Is Not Null"
    # elif s_list[0:1] == "p":  # Payee
    #     s_list1 = "and KFS.VENDOR_ID Is Not Null"
    elif s_list[0:1] == "s":  # Student
        s_list1 = "and STU.KSTUDBUSENTID Is Not Null"
    elif s_list[0:1] == "v":  # Vendor
        s_list1 = "and PAR.PARTYTYPE = 2"
    else:
        s_list1 = ""

    s_where = s_where.replace("%MAIN1%", s_main1)
    s_where = s_where.replace("%MAIN2%", s_main2)
    s_where = s_where.replace("%MAIN3%", s_main3)
    s_where = s_where.replace("%SECONDARY%", s_secondary1)
    s_where = s_where.replace("%LIST%", s_list1)
    if l_debug:
        print(s_where)

    # LOCATE THE RECORDS
    sr_file = "X000_Party_lookup"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if s_where != "":
        funcfile.writelog("PARTY LOOKUP")
        if l_debug:
            print("Party lookup...")
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            PAR.KBUSINESSENTITYID,
            PAR.FULL_NAME,
            PAR.TITLE,
            PAR.NICKNAME,
            PAR.MAIDENNAME,
            PAR.IDNO,
            PAR.PASSPORT,
            PAR.STUDYPERMIT,
            PAR.NATIONALITY,
            CASE
                WHEN PEO.EMPLOYEE_NUMBER Is Not Null THEN True
                ELSE False
            END AS EMPLOYEE,
            CASE
                WHEN STU.KSTUDBUSENTID Is Not Null THEN True
                ELSE False
            END AS STUDENT,
            CASE
                WHEN KFS.VENDOR_ID Is Not Null THEN True
                ELSE False
            END AS VENDOR,
            PAR.PARTYTYPE,
            KFS.YEAR,
            KFS.VENDOR_ID
        From
            X000_Party PAR Left Join
            PEOPLE.X002_PEOPLE_CURR PEO On PEO.EMPLOYEE_NUMBER = CAST(PAR.KBUSINESSENTITYID AS TEXT) Left Join
            VSSCURR.X001_Student STU On STU.KSTUDBUSENTID = PAR.KBUSINESSENTITYID Left Join
            KFSCURR.X002aa_Report_payments_summary KFS ON SUBSTR(KFS.VENDOR_ID,1,8) = CAST(PAR.KBUSINESSENTITYID AS TEXT)   
        Where
            %WHERE%
        Order By
            PAR.SURNAME,
            PAR.INITIALS,
            PAR.NICKNAME,
            PAR.IDNO Desc    
        """
        s_sql = s_sql.replace("%WHERE%", s_where)
        so_curs.execute(s_sql)
        funcfile.writelog("%t BUILD TABLE: " + sr_file)
    else:
        s_sql = "CREATE TABLE " + sr_file + " (" + """
        BLANK TEXT
        );"""
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD MESSAGE FROM DATA
    i_records = funcsys.tablerowcount(so_curs, sr_file)
    if l_debug:
        print(i_records)
    if i_records > 0:

        """
        row[0] = KBUSINESSENTITYID
        row[1] = FULL_NAME 
        row[2] = TITLE
        row[3] = NICKNAME
        row[4] = MAIDENNAME
        row[5] = IDNO
        row[6] = PASSPORT
        row[7] = STUDYPERMIT
        row[8] = NATIONALITY
        row[9] = EMPLOYEE
        row[10] = STUDENT
        row[11] = VENDOR
        row[12] = PARTYTYPE
        """

        s_return_message = ""
        for row in so_curs.execute("SELECT * from " + sr_file).fetchall():
            s_return_message += "\n"
            if row[12] == 2:  # Do not split vendor name
                s_return_message += "<b>" + row[1] + "</b>"
                if row[3]:
                    s_return_message += "\n " + row[3]
            else:  # Split name
                s_return_message += "<b>" + row[1][0:row[1].find(")")+1] + "</b>\n"
                if " " + row[1][row[1].find(")")+2:]:
                    s_return_message += " " + row[1][row[1].find(")")+2:]
            if row[9] == 1 or row[10] == 1 or row[11] == 1:
                s_return_message += "\n"
                if row[9] == 1:
                    s_return_message += " -employee"
                if row[10] == 1:
                    s_return_message += " -student"
                if row[11] == 1:
                    s_return_message += " -payee"
            s_return_message += "\n  /nwu " + str(row[0])
            if row[5]:  # Show only if not null
                s_return_message += "\n  /id " + str(row[5])
            if len(s_return_message) > 3900:
                break
        if len(s_return_message) > 3900:
            s_return_message = "<b>Found " + str(i_records) + " records.</b>\n\n" + \
                               "Found too many records to display. Please refine your search!\n\n" + \
                               s_return_message
        else:
            s_return_message = "<b>Found " + str(i_records) + " records.</b>\n\n" + s_return_message

    """*****************************************************************************
    END OF SCRIPT
    *****************************************************************************"""
    funcfile.writelog("END OF SCRIPT")
    if l_debug:
        print("END OF SCRIPT")

    # COMMIT DATA
    so_conn.commit()

    # CLOSE THE DATABASE CONNECTION
    so_conn.close()

    # CLOSE THE LOG WRITER
    funcfile.writelog("--------------------------")
    funcfile.writelog("COMPLETED: D001_ROBOT_NAME")

    return s_return_message[0:4096]


if __name__ == '__main__':
    try:
        s_return = robot_name("a", "rensburg", "albert")
        if funcconf.l_mess_project:
            print("RETURN: " + s_return)
            print("LENGTH: " + str(len(s_return)))
    except Exception as e:
        funcsys.ErrMessage(e, funcconf.l_mess_project, "D001_robot_name", "D001_robot_name")
