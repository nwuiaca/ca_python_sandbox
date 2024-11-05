"""
PREPARE AND SEND AUDIT REPORTS VIA EMAIL FROM THE NWUIA WEB
WORK IN PROGRESS REPORT
Script: D003_robot_report_audit_wip.py
Author: Albert B Janse van Rensburg (NWU:21162395)
Created: 20 Nov 2020
"""

# IMPORT PYTHON MODULES
# import sqlite3

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
s_function: str = "D003_report_audit_wip"


def robot_report_audit_wip(s_year: str = "", s_type: str = "", s_name: str = "", s_mail: str = ""):
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
    s_file: str = "Report_assignment_wip.csv"
    s_message: str = "Audit assignment work in progress (wip) report for year " + s_year + " "

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
    s_line += "ORIGIN,"
    s_line += "CATEGORY,"
    s_line += "TYPE,"
    s_line += "ASSIGNMENT,"
    s_line += "PRIORITY,"
    s_line += "STATUS,"
    s_line += "START_DATE,"
    s_line += "DUE_DATE,"
    s_line += "CLOSE_DATE,"
    s_line += "ASSURANCE_AUDIT,"
    s_line += "ASSURANCE_CLIENT,"
    s_line += "NOTES_OFFICIAL,"
    s_line += "NOTES_OWN"
    funcfile.writelog(s_line, re_path, s_file)
    s_line = ""

    # BUILD THE WHERE CLAUSE
    if s_year != "":
        s_where = "ia_assi_year = " + s_year + " and ca.ia_assicate_private = '0'"
        s_where += " or "
        s_where += "ia_assi_year < " + s_year + " and ia_assi_priority < 9 and ca.ia_assicate_private = '0'"
        s_where += " or "
        s_where += "ia_assi_finishdate >= '" + str(int(s_year)-1) + "-10-01'"
        s_where += " and "
        s_where += "ia_assi_finishdate <= '" + s_year + "-09-30'"
        s_where += " and "
        s_where += "ca.ia_assicate_private = '0'"
    else:
        s_where = "ia_assi_year = " + funcdatn.get_current_year() + " and ca.ia_assicate_private = '0'"
        s_where += " or "
        s_where += "ia_assi_year < " + funcdatn.get_current_year() + " and ia_assi_priority < 9 and ca.ia_assicate_private = '0'"
        s_where += " or "
        s_where += "ia_assi_finishdate >= '" + funcdatn.get_previous_year() + "-10-01'"
        s_where += " and "
        s_where += "ia_assi_finishdate <= '" + funcdatn.get_current_year() + "-09-30'"
        s_where += " and "
        s_where += "ca.ia_assicate_private = '0'"

    # BUILD THE SQL QUERY
    s_sql = """
    SELECT
        us.name,
        ia.ia_assi_year,
        Case
            When og.ia_assiorig_name <> '' Then og.ia_assiorig_name
            Else ''
        End As ia_assiorig_name,
        Case
            When ca.ia_assicate_name <> '' Then ca.ia_assicate_name
            Else ''
        End As ia_assicate_name,
        Case
            When at.ia_assitype_name <> '' Then at.ia_assitype_name
            Else ''
        End As ia_assitype_name,
        ia.ia_assi_name,
        ia.ia_assi_priority,
        Case
            When st.ia_assistat_name <> '' Then st.ia_assistat_name
            Else ''
        End As ia_assistat_name,
        ia.ia_assi_startdate,
        ia.ia_assi_completedate,
        ia.ia_assi_finishdate,
        Case
            When ia.ia_assi_riskscore = 0 Then '' 
            When ia.ia_assi_riskscore <= 4 Then Concat('(', ia.ia_assi_riskscore, ') Positive') 
            When ia.ia_assi_riskscore <= 10 Then Concat('(', ia.ia_assi_riskscore, ') Satisfactory') 
            When ia.ia_assi_riskscore <= 15 Then Concat('(', ia.ia_assi_riskscore, ') Limited') 
            When ia.ia_assi_riskscore <= 25 Then Concat('(', ia.ia_assi_riskscore, ') No') 
            Else ''
        End as assurance,
        Case
            When ia.ia_assi_riskscore_client = 0 Then '' 
            When ia.ia_assi_riskscore_client <= 4 Then Concat('(', ia.ia_assi_riskscore_client, ') Positive') 
            When ia.ia_assi_riskscore_client <= 10 Then Concat('(', ia.ia_assi_riskscore_client, ') Satisfactory') 
            When ia.ia_assi_riskscore_client <= 15 Then Concat('(', ia.ia_assi_riskscore_client, ') Limited') 
            When ia.ia_assi_riskscore_client <= 25 Then Concat('(', ia.ia_assi_riskscore_client, ') No') 
            Else ''
        End as assurance_client,        
        ia.ia_assi_offi,
        ia.ia_assi_desc,
        ia.ia_assi_auto
    FROM
        ia_assignment ia Left Join
        jm4_users us On us.id = ia.ia_user_sysid Left Join
        ia_assignment_type at On at.ia_assitype_auto = ia.ia_assitype_auto Left Join
        ia_assignment_status st On st.ia_assistat_auto = ia.ia_assistat_auto Left Join
        ia_assignment_category ca On ca.ia_assicate_auto = ia.ia_assicate_auto Left Join
        ia_assignment_origin og On og.ia_assiorig_auto = ia.ia_assiorig_auto
    WHERE %WHERE%
    ORDER BY
        name,
        ia_assicate_name,
        ia_assitype_name,
        ia_assi_name
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

        # ORIGIN
        if l_debug:
            print('rowcontents: ', row[2])
            print('rowtype: ', type(row[2]))
        s_line += '"' + row[2] + '",'

        # CATEGORY
        if l_debug:
            print('rowcontents: ', row[3])
            print('rowtype: ', type(row[3]))
        s_line += '"' + row[3] + '",'

        # TYPE
        if l_debug:
            print('rowcontents: ', row[4])
            print('rowtype: ', type(row[4]))
        s_line += '"' + row[4] + '",'

        # NAME
        if l_debug:
            print(row[5])
            print(row[15])
        s_line += '"' + row[5].replace(",", "") + ' (' + str(row[15]) + ')",'

        # PRIORITY
        if l_debug:
            print(row[6])
        s_priority: str = '"Inactive"'
        if row[6] == "9":
            s_priority = '"Closed"'
        elif row[6] == "8":
            s_priority = '"Continuous"'
        elif row[6] == "4":
            s_priority = '"Follow-up"'
        elif row[6] == "3":
            s_priority = '"High"'
        elif row[6] == "2":
            s_priority = '"Medium"'
        elif row[6] == "1":
            s_priority = '"Low"'
        s_line += s_priority + ","

        # STATUS
        if l_debug:
            print(row[7])
        s_line += '"' + row[7] + '",'

        # START DATE
        if l_debug:
            print(row[8])
        if row[8]:
            s_line += datetime.strftime(row[8], "%Y-%m-%d") + ","
        else:
            s_line += ","

        # DUE DATE
        if l_debug:
            print(row[9])
        if row[9]:
            s_line += datetime.strftime(row[8], "%Y-%m-%d") + ","
        else:
            s_line += ","

        # FINISH DATE
        if l_debug:
            print(row[10])
        if row[10]:
            s_line += datetime.strftime(row[9], "%Y-%m-%d") + ","
        else:
            s_line += ","

        # AUDIT RISK SCORE
        if l_debug:
            print(row[11])
        if row[11]:
            s_line += '"' + str(row[11]) + '",'
        else:
            s_line += ","

        # CLIENT RISK SCORE
        if l_debug:
            print(row[12])
        if row[12]:
            s_line += '"' + str(row[12]) + '",'
        else:
            s_line += ","

        # NOTES_OFFICIAL
        if l_debug:
            print(row[13])
        s_data = row[13].replace(",", "")
        s_data = s_data.replace("'", "")
        s_data = s_data.replace('"', "")
        s_line += '"' + s_data + '",'

        # NOTES_OWN
        if l_debug:
            print(row[14])
        s_data = row[14].replace(",", "")
        s_data = s_data.replace("'", "")
        s_data = s_data.replace('"', "")
        s_line += '"' + s_data + '"'

        if l_debug:
            print(s_line)

        # WRITE TO FILE
        funcfile.writelog(s_line, re_path, s_file)
        s_line = ""

    funcfile.writelog("%t Audit assignment wip report requested by " + s_name)
    s_report = "Include all assignments for the year mentioned and "
    s_report += "all previous assignments with an unclosed priority."

    # MAIL THE AUDIT REPORT
    if s_name != "" and s_mail != "":
        funcfile.writelog("%t Audit assignment wip report mailed to " + s_mail)
        if l_debug:
            print("Send the report...")
        s_body: str = "Attached please find audit assignment work in progress (wip) report as requested for " + \
                      s_year + "."
        s_body += "\n\r"
        s_body += s_report
        funcmail.send(s_name,
                      s_mail,
                      "E",
                      "Report audit assignment work in progress (wip) " + s_year,
                      s_body,
                      re_path,
                      s_file)
        s_message += " was mailed to " + s_mail

    # POPULATE THE RETURN MESSAGE
    s_return_message = s_message

    # DELETE THE MAILED FILE
    if funcfile.file_delete(re_path, s_file):
        funcfile.writelog("%t Audit assignment wip report deleted")
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
        s_return = robot_report_audit_wip("2024", "0", "Albert", "21162395@nwu.ac.za")
        if funcconf.l_mess_project:
            print("RETURN: " + s_return)
            print("LENGTH: " + str(len(s_return)))
    except Exception as e:
        funcsys.ErrMessage(e, funcconf.l_mess_project, s_function, s_function)
