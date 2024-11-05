"""
PREPARE AND SEND AUDIT REPORTS VIA EMAIL FROM THE NWUIA WEB
SPECIAL INVESTIGATION PROGRESS REPORT
Script: D004_robot_report_audit_sir.py
Author: Albert B Janse van Rensburg (NWU:21162395)
Created: 31 Mar 2022
"""

# IMPORT PYTHON MODULES
# import sqlite3
import re

# IMPORT OWN MODULES
from _my_modules import funcconf
from _my_modules import funcdatn
from _my_modules import funcfile
from _my_modules import funcmail
from _my_modules import funcmysql
from _my_modules import funcsms
from _my_modules import funcsys

# INDEX
"""
ENVIRONMENT
OPEN THE DATABASES
BUILD THE REPORT
END OF SCRIPT
"""

# VARIABLES
s_function: str = "D004_report_audit_sir"


def robot_report_audit_sir(s_year: str = "", s_type: str = "", s_name: str = "", s_mail: str = ""):
    """
    SEARCH VSS.PARTY FOR NAMES, NUMBERS AND ID'S
    :param s_year: Working year
    :param s_type: Report type
    :param s_name: The name of the requester / recipient
    :param s_mail: The requester mail address
    :return: str: Info in message format
    """

    from datetime import datetime

    # DECLARE VARIABLES
    l_debug: bool = False

    """*************************************************************************
    ENVIRONMENT
    *************************************************************************"""
    if l_debug:
        print("ENVIRONMENT")

    # DECLARE VARIABLES
    re_path: str = f"{funcconf.drive_data_results}Audit/"  # Results

    l_mess: bool = funcconf.l_mess_project
    # l_mess: bool = False
    s_file: str = "Report_assignment_sir.csv"
    s_message: str = "Special investigation work in progress (sir) report for year " + s_year + " "

    # LOG
    funcfile.writelog("Now")
    funcfile.writelog("SCRIPT: " + s_function.upper())
    funcfile.writelog("-" * len("script: "+s_function))
    if l_debug:
        print(s_function.upper())

    # MESSAGE
    if l_mess:
        funcsms.send_telegram("", "administrator", "<b>" + s_function.upper() + "</b>")

    """****************************************************************************
    OPEN THE DATABASES
    ****************************************************************************"""

    if l_debug:
        print("OPEN THE MYSQL DATABASE")
    funcfile.writelog("OPEN THE MYSQL DATABASE")

    # VARIABLES
    s_source_database: str = "Web_nwu_ia"

    # OPEN THE SOURCE FILE
    ms_from_connection = funcmysql.mysql_open(s_source_database)
    ms_from_cursor = ms_from_connection.cursor()
    funcfile.writelog("%t OPEN MYSQL DATABASE: " + s_source_database)

    """*****************************************************************************
    BUILD THE REPORT
    *****************************************************************************"""
    funcfile.writelog("BUILD THE REPORT")
    if l_debug:
        print("BUILD THE REPORT")

    # BUILD THE HEADINGS
    s_line: str = ""
    s_line = "AUDITOR,"
    s_line += "YEAR,"
    s_line += "ASSIGNMENT,"
    s_line += "CASE_YEAR,"
    s_line += "CASE_NUMBER,"
    s_line += "BOX_YEAR,"
    s_line += "BOX_NUMBER,"
    s_line += "REPORTED_DATE,"
    s_line += "DATE_21,"
    s_line += "DATE_90,"
    s_line += "START_DATE,"
    s_line += "DUE_DATE,"
    s_line += "COMPLETE_DATE,"
    s_line += "TYPE,"
    s_line += "PRIORITY,"
    s_line += "ACCUSED,"
    s_line += "ISSUE,"
    s_line += "STATUS,"
    s_line += "DEPARTMENT,"
    s_line += "REFERENCE,"
    s_line += "VALUE,"
    s_line += "NOTES_OFFICIAL,"
    s_line += "NOTES_OWN"
    funcfile.writelog(s_line, re_path, s_file)
    s_line = ""

    # BUILD THE WHERE CLAUSE
    if s_year == "0":
        s_where = f"(ia.ia_assicate_auto = 9)"
    elif s_year != "":
        s_where = f"(ia_assi_year = {s_year} and ia.ia_assicate_auto = 9)"
        s_where += " or "
        s_where += f"(ia_assi_year < {s_year} and ia.ia_assicate_auto = 9 and ia_assi_priority < 8)"
        s_where += " or "
        s_where += "(" \
                   "Date(ia_assi_finishdate) >= Date_Sub(Concat(Year(Now()), '-10-01'), Interval 1 Year)" \
                   " And " \
                   "Date(ia_assi_finishdate) <= Date_Sub(Concat(Year(Now()), '-10-01'), Interval 1 Day)" \
                   " And " \
                   "ia.ia_assicate_auto = 9" \
                   ")"
    else:
        s_where = "(ia_assi_year = " + funcdatn.get_current_year() + " and ia.ia_assicate_auto = 9)"
        s_where += " or "
        s_where += "(ia_assi_year < " + funcdatn.get_current_year() + " and ia.ia_assicate_auto = 9 and ia_assi_priority < 8)"
        s_where += " or "
        s_where += "(ia_assi_finishdate >= '" + funcdatn.get_previous_year() + "-10-01'"
        s_where += " and "
        s_where += "ia_assi_finishdate <= '" + funcdatn.get_current_year() + "-09-30'"
        s_where += " and "
        s_where += "ca.ia_assicate_private = '0'"

        s_where += " And "
        s_where += "ia.ia_assicate_auto = 9)"

    # BUILD THE SQL QUERY
    s_sql = """
    SELECT
        us.name,
        ia.ia_assi_year,
        ia.ia_assi_name,
        ia.ia_assi_si_caseyear,
        ia.ia_assi_si_casenumber,
        ia.ia_assi_si_boxyear,
        ia.ia_assi_si_boxnumber,
        ia.ia_assi_si_reportdate,
        ia.ia_assi_si_report1date,
        ia.ia_assi_si_report2date,
        ia.ia_assi_startdate,
        ia.ia_assi_completedate,
        ia.ia_assi_finishdate,    
        at.ia_assitype_name, 
        ia.ia_assi_priority,
        ia.ia_assi_si_accused,
        ia.ia_assi_si_issue,
        st.ia_assistat_name,
        co.ia_assicond_name,
        ia.ia_assi_si_reference,
        ia.ia_assi_si_value,
        ia.ia_assi_offi,
        ia.ia_assi_desc,
        ia.ia_assi_auto,
        ia.ia_assicate_auto
    FROM
        ia_assignment ia Left Join
        jm4_users us On us.id = ia.ia_user_sysid Left Join
        ia_assignment_type at On at.ia_assitype_auto = ia.ia_assitype_auto Left Join
        ia_assignment_status st On st.ia_assistat_auto = ia.ia_assistat_auto Left Join
        ia_assignment_conducted co On co.ia_assicond_auto = ia.ia_assicond_auto
    WHERE %WHERE%
    ORDER BY
        ia_assi_si_caseyear,
        ia_assi_si_casenumber
    ;
    """
    s_sql = s_sql.replace("%WHERE%", s_where)

    if l_debug:
        print(s_sql)

    # BUILD THE ASSIGNMENT RECORD
    for row in ms_from_cursor.execute(s_sql).fetchall():

        # USER NAME
        if l_debug:
            print(row[0])
        s_line = '"' + row[0] + '",'

        # YEAR
        if l_debug:
            print(row[1])
        s_line += str(row[1]) + ","

        # NAME
        if l_debug:
            print(row[2])
            print(row[23])
        s_line += '"' + row[2].replace(",", "") + ' (' + str(row[23]) + ')",'

        # CASE YEAR
        if l_debug:
            print(row[3])
        s_line += str(row[3]) + ","

        # CASE NUMBER
        if l_debug:
            print(row[4])
        s_line += str(row[4]) + ","

        # BOX YEAR
        if l_debug:
            print(row[5])
        s_line += str(row[5]) + ","

        # BOX NUMBER
        if l_debug:
            print(row[6])
        s_line += str(row[6]) + ","

        # REPORTED DATE
        if l_debug:
            print(row[7])
        if row[7]:
            s_line += datetime.strftime(row[7], "%Y-%m-%d") + ","
        else:
            s_line += ","

        # 21 DATE
        if l_debug:
            print(row[8])
        if row[8]:
            s_line += datetime.strftime(row[8], "%Y-%m-%d") + ","
        else:
            s_line += ","

        # 90 DATE
        if l_debug:
            print(row[9])
        if row[9]:
            s_line += datetime.strftime(row[9], "%Y-%m-%d") + ","
        else:
            s_line += ","

        # START DATE
        if l_debug:
            print(row[10])
        if row[10]:
            s_line += datetime.strftime(row[10], "%Y-%m-%d") + ","
        else:
            s_line += ","

        # DUE DATE
        if l_debug:
            print(row[11])
        if row[11]:
            s_line += datetime.strftime(row[11], "%Y-%m-%d") + ","
        else:
            s_line += ","

        # FINISH DATE
        if l_debug:
            print(row[12])
        if row[12]:
            s_line += datetime.strftime(row[12], "%Y-%m-%d") + ","
        else:
            s_line += ","

        # TYPE
        if l_debug:
            print(row[13])
        s_line += '"' + row[13] + '",'

        # PRIORITY
        if l_debug:
            print(row[14])
        s_priority: str = '"Inactive"'
        if row[14] == "9":
            s_priority = '"Closed"'
        elif row[14] == "8":
            s_priority = '"Continuous"'
        elif row[14] == "7":
            s_priority = '"Follow-up"'
        elif row[14] == "3":
            s_priority = '"High"'
        elif row[14] == "2":
            s_priority = '"Medium"'
        elif row[14] == "1":
            s_priority = '"Low"'
        s_line += s_priority + ","

        # ACCUSED
        if l_debug:
            print(row[15])
        s_line += '"' + row[15] + '",'

        # ISSUE
        if l_debug:
            print(row[16])
        # s_data = row[16].replace(",", "")
        # s_data = s_data.replace("'", "")
        # s_data = s_data.replace('"', "")
        # s_line += '"' + s_data + '",'
        s_line += '"' + re.sub('\W+', ' ', row[16]) + '",'

        # STATUS
        if l_debug:
            print(row[17])
        s_line += '"' + row[17] + '",'

        # DEPARTMENT
        if l_debug:
            print(row[18])
        s_line += '"' + row[18] + '",'

        # REFERENCE
        if l_debug:
            print(row[19])
        # s_data = row[19].replace(",", "")
        # s_data = s_data.replace("'", "")
        # s_data = s_data.replace('"', "")
        # s_line += '"' + s_data + '",'
        s_line += '"' + re.sub('\W+', ' ', row[19]) + '",'

        # VALUE
        if l_debug:
            print(row[20])
            print(s_line)
        s_line += str(row[20]) + ","
        if l_debug:
            print(s_line)

        # NOTES_OFFICIAL
        if l_debug:
            print(row[21])
        # s_line += '"' + row[21] + '",'
        # s_data = row[21].replace(",", "")
        # s_data = s_data.replace("'", "")
        # s_data = s_data.replace('"', "")
        # s_line += '"' + s_data + '",'
        s_line += '"' + re.sub('\W+', ' ', row[21]) + '",'

        # NOTES_OWN
        if l_debug:
            print(row[22])
        # s_data = row[22].replace(",", "")
        # s_data = s_data.replace("'", "")
        # s_data = s_data.replace('"', "")
        # s_line += '"' + s_data + '"'
        s_line += '"' + re.sub('\W+', ' ', row[22]) + '",'

        if l_debug:
            print(s_line)

        # WRITE TO FILE
        funcfile.writelog(s_line, re_path, s_file)
        s_line = ""

    funcfile.writelog("%t Special investigation sir report requested by " + s_name)
    s_report = "Include all special investigations for the year mentioned and "
    s_report += "all previous special investigations with an unclosed priority."

    # MAIL THE AUDIT REPORT
    if s_name != "" and s_mail != "":
        funcfile.writelog("%t Special investigation sir report mailed to " + s_mail)
        if l_debug:
            print("Send the report...")
        s_body: str = "Attached please find special investigation progress (sir) report as requested for " + \
                      s_year + "."
        s_body += "\n\r"
        s_body += s_report
        funcmail.send(s_name,
                      s_mail,
                      "E",
                      "Report special investigation progress (sir) " + s_year,
                      s_body,
                      re_path,
                      s_file)
        s_message += " was mailed to " + s_mail

    # POPULATE THE RETURN MESSAGE
    s_return_message = s_message

    # DELETE THE MAILED FILE
    if funcfile.file_delete(re_path, s_file):
        funcfile.writelog("%t Special investigation sir report deleted")
        if l_debug:
            print("Delete the report...")

    """*****************************************************************************
    END OF SCRIPT
    *****************************************************************************"""
    funcfile.writelog("END OF SCRIPT")
    if l_debug:
        print("END OF SCRIPT")

    # CLOSE THE LOG WRITER
    funcfile.writelog("-" * len("completed: "+s_function))
    funcfile.writelog("COMPLETED: " + s_function.upper())

    return s_return_message[0:4096]


if __name__ == '__main__':
    try:
        s_return = robot_report_audit_sir("0", "0", "Albert", "21162395@nwu.ac.za")
        if funcconf.l_mess_project:
            print("RETURN: " + s_return)
            print("LENGTH: " + str(len(s_return)))
    except Exception as e:
        funcsys.ErrMessage(e, funcconf.l_mess_project, s_function, s_function)
