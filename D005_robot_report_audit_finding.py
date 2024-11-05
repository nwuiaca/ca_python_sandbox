"""
PREPARE AND SEND AUDIT REPORTS VIA EMAIL FROM THE NWUIA WEB
FINDINGS REPORT
Script: D005_robot_report_audit_findings.py
Author: Albert B Janse van Rensburg (NWU:21162395)
Created: 3 May 2023
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
s_function: str = "D005_report_audit_finding"


def robot_report_audit_finding(s_year: str = "", s_type: str = "", s_name: str = "", s_mail: str = ""):
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
    s_file: str = "Report_assignment_finding.csv"
    s_message: str = "Audit assignment finding report for year " + s_year + " "

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
    s_line += "CATEGORY,"
    s_line += "TYPE,"
    s_line += "ASSIGNMENT,"
    s_line += "FINDING,"
    s_line += "WSTATUS,"
    s_line += "RATING,"
    s_line += "LIKELIHOOD,"
    s_line += "EFFECTIVENESS,"
    s_line += "ASSURANCE_AUDIT,"
    s_line += "ASSURANCE_CLIENT,"
    s_line += "ASTATUS"
    funcfile.writelog(s_line, re_path, s_file)
    s_line = ""

    # BUILD THE WHERE CLAUSE
    if s_year != "":
        s_where = "ia_assi_year = " + s_year + " and cate.ia_assicate_private = '0' and type.ia_assitype_private = '0' and find.ia_find_private = '0' and find.ia_find_appendix = 0 and fist.ia_findstat_private = '0'"
        s_where += " or "
        s_where += "ia_assi_year < " + s_year + " and ia_assi_priority < 9 and cate.ia_assicate_private = '0' and type.ia_assitype_private = '0' and find.ia_find_private = '0' and find.ia_find_appendix = 0 and fist.ia_findstat_private = '0'"
        s_where += " or "
        s_where += "ia_assi_finishdate >= '" + str(int(s_year)-1) + "-10-01'"
        s_where += " and "
        s_where += "ia_assi_finishdate <= '" + s_year + "-09-30'"
        s_where += " and "
        s_where += "cate.ia_assicate_private = '0' and type.ia_assitype_private = '0' and find.ia_find_private = '0' and find.ia_find_appendix = 0 and fist.ia_findstat_private = '0'"
    else:
        s_where = "ia_assi_year = " + funcdatn.get_current_year() + " and cate.ia_assicate_private = '0' and type.ia_assitype_private = '0' and find.ia_find_private = '0' and find.ia_find_appendix = 0 and fist.ia_findstat_private = '0'"
        s_where += " or "
        s_where += "ia_assi_year < " + funcdatn.get_current_year() + " and ia_assi_priority < 9 and cate.ia_assicate_private = '0' and type.ia_assitype_private = '0' and find.ia_find_private = '0' and find.ia_find_appendix = 0 and fist.ia_findstat_private = '0'"
        s_where += " or "
        s_where += "ia_assi_finishdate >= '" + funcdatn.get_previous_year() + "-10-01'"
        s_where += " and "
        s_where += "ia_assi_finishdate <= '" + funcdatn.get_current_year() + "-09-30'"
        s_where += " and "
        s_where += "cate.ia_assicate_private = '0' and type.ia_assitype_private = '0' and find.ia_find_private = '0' and find.ia_find_appendix = 0 and fist.ia_findstat_private = '0'"

    # BUILD THE SQL QUERY
    s_sql = """
    SELECT
        us.name As auditor,
        assi.ia_assi_year As year,
        Case
            When cate.ia_assicate_name <> '' Then cate.ia_assicate_name
            Else ''
        End As category,
        Case
            When type.ia_assitype_name <> '' Then type.ia_assitype_name
            Else ''
        End As type,    
        Concat(assi.ia_assi_name, ' (', assi.ia_assi_auto, ')') As assignment,
        Concat(find.ia_find_name, ' (', find.ia_find_auto, ')') As finding,
        fist.ia_findstat_name As wstatus,
        rate.ia_findrate_name As rating,
        likh.ia_findlike_name As likelihood,
        cont.ia_findcont_name As control_effectiveness,
        Case
            When find.ia_find_riskscore = 0 Then '' 
            When find.ia_find_riskscore <= 4 Then Concat('(', find.ia_find_riskscore, ') Positive') 
            When find.ia_find_riskscore <= 10 Then Concat('(', find.ia_find_riskscore, ') Satisfactory') 
            When find.ia_find_riskscore <= 15 Then Concat('(', find.ia_find_riskscore, ') Limited') 
            When find.ia_find_riskscore <= 25 Then Concat('(', find.ia_find_riskscore, ') No') 
            Else ''
        End as assurance,
        Case
            When find.ia_find_riskscore_client = 0 Then '' 
            When find.ia_find_riskscore_client <= 4 Then Concat('(', find.ia_find_riskscore_client, ') Positive') 
            When find.ia_find_riskscore_client <= 10 Then Concat('(', find.ia_find_riskscore_client, ') Satisfactory') 
            When find.ia_find_riskscore_client <= 15 Then Concat('(', find.ia_find_riskscore_client, ') Limited') 
            When find.ia_find_riskscore_client <= 25 Then Concat('(', find.ia_find_riskscore_client, ') No') 
            Else ''
        End as assurance_client,
        aust.ia_findaud_name As astatus
    FROM
        ia_finding find Inner Join
        ia_assignment assi On assi.ia_assi_auto = find.ia_assi_auto Left Join
        ia_finding_status fist On fist.ia_findstat_auto = find.ia_findstat_auto Left Join
        jm4_users us On us.id = assi.ia_user_sysid Left Join
        ia_finding_rate rate On rate.ia_findrate_auto = find.ia_findrate_auto Left Join
        ia_finding_control cont On cont.ia_findcont_auto = find.ia_findcont_auto Left Join
        ia_finding_likelihood likh On likh.ia_findlike_auto = find.ia_findlike_auto Left Join
        ia_finding_audit aust On aust.ia_findaud_auto = find.ia_findaud_auto Left Join
        ia_assignment_category cate On cate.ia_assicate_auto = assi.ia_assicate_auto Left Join
        ia_assignment_type type On type.ia_assitype_auto = assi.ia_assitype_auto        
    WHERE %WHERE%
    ORDER BY
        name,
        ia_assicate_name,
        ia_assitype_name,
        ia_assi_name,
        ia_find_name
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

        # CATEGORY
        if l_debug:
            print('rowcontents: ', row[2])
            print('rowtype: ', type(row[2]))
        s_line += '"' + row[2] + '",'

        # TYPE
        if l_debug:
            print('rowcontents: ', row[3])
            print('rowtype: ', type(row[3]))
        s_line += '"' + row[3] + '",'

        # ASSIGNMENT
        if l_debug:
            print(row[4])
        s_line += '"' + row[4] + '",'

        # FINDING
        if l_debug:
            print(row[5])
        s_line += '"' + row[5] + '",'

        # WSTATUS
        if l_debug:
            print(row[6])
        s_line += '"' + row[6] + '",'

        # RATING
        if l_debug:
            print(row[7])
        if row[7]:
            s_line += '"' + row[7] + '",'
        else:
            s_line += ","

        # LIKELIHOOD
        if l_debug:
            print(row[8])
        if row[8]:
            s_line += '"' + row[8] + '",'
        else:
            s_line += ","

        # EFFECTIVENESS
        if l_debug:
            print(row[9])
        if row[9]:
            s_line += '"' + row[9] + '",'
        else:
            s_line += ","

        # AUDIT RISK SCORE
        if l_debug:
            print(row[10])
        if row[10]:
            s_line += '"' + str(row[10]) + '",'
        else:
            s_line += ","

        # CLIENT RISK SCORE
        if l_debug:
            print(row[11])
        if row[11]:
            s_line += '"' + str(row[11]) + '",'
        else:
            s_line += ","

        # ASTATUS
        if l_debug:
            print(row[12])
        if row[12]:
            s_line += '"' + row[12] + '"'
        else:
            s_line += ""

        # CATEGORY
        # if l_debug:
        #     print('rowcontents: ', row[2])
        #     print('rowtype: ', type(row[2]))
        # s_line += '"' + row[2] + '",'

        if l_debug:
            print(s_line)

        # WRITE TO FILE
        funcfile.writelog(s_line, re_path, s_file)
        s_line = ""

    funcfile.writelog("%t Audit assignment finding report requested by " + s_name)
    s_report = "Include all findings for the year mentioned and "
    s_report += "all previous findings with an unclosed priority."

    # MAIL THE AUDIT REPORT
    if s_name != "" and s_mail != "":
        funcfile.writelog("%t Audit assignment finding report mailed to " + s_mail)
        if l_debug:
            print("Send the report...")
        s_body: str = "Attached please find audit assignment finding report as requested for " + \
                      s_year + "."
        s_body += "\n\r"
        s_body += s_report
        funcmail.send(s_name,
                      s_mail,
                      "E",
                      "Report audit assignment finding " + s_year,
                      s_body,
                      re_path,
                      s_file)
        s_message += " was mailed to " + s_mail

    # POPULATE THE RETURN MESSAGE
    s_return_message = s_message

    # DELETE THE MAILED FILE
    if funcfile.file_delete(re_path, s_file):
        funcfile.writelog("%t Audit assignment finding report deleted")
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
        s_return = robot_report_audit_finding("2024", "0", "Albert", "21162395@nwu.ac.za")
        if funcconf.l_mess_project:
            print("RETURN: " + s_return)
            print("LENGTH: " + str(len(s_return)))
    except Exception as e:
        funcsys.ErrMessage(e, funcconf.l_mess_project, s_function, s_function)
