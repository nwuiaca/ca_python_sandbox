"""
PREPARE AND SEND AUDIT REPORTS VIA EMAIL FROM THE NWUIA WEB
Script: D002_robot_report_audit_assignment.py
Author: Albert B Janse van Rensburg (NWU:21162395)
Created: 31 Oct 2020
"""

# IMPORT PYTHON MODULES
# import sqlite3

# IMPORT OWN MODULES
from _my_modules import funcconf
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
s_function: str = "D002_report_audit_assignment"


def robot_report_audit_assignment(assignment_number: str = "", s_name: str = "", s_mail: str = ""):
    """
    SEARCH VSS.PARTY FOR NAMES, NUMBERS AND ID'S
    :param assignment_number: Assignment number
    :param s_name: The name of the requester / recipient
    :param s_mail: The requester mail address
    :return: str: Info in message format
    """

    # VARIABLES
    # s_function: str = "D002_report_audit_assignment"
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
    s_file: str = f"Report_assignment_{assignment_number}.html"
    assignment_report: str = ""
    s_message: str = f"Audit assignment report ({assignment_number})"
    assignment_report_footer: str = ""
    assignment_report_signature: str = ""
    log_entries = []

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

    # Build the document intro
    html_intro = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Assignment</title>
    <style>
        body {
            font-family: Arial, sans-serif;
            font-size: 13px;
        }
    </style>        
</head>
<body>
    """
    log_entries.append(html_intro)

    """*****************************************************************************
    BUILD THE REPORT
    *****************************************************************************"""
    funcfile.writelog("BUILD THE REPORT")
    if l_debug:
        print("BUILD THE REPORT")

    # BUILD THE ASSIGNMENT RECORD
    s_sql: str = f"""
    Select
        assi.ia_assi_auto,
        assi.ia_assi_name,
        assi.ia_assi_header,
        assi.ia_assi_header_text,
        assi.ia_assi_report,
        assi.ia_assi_report_text1,
        assi.ia_assi_report_text2,
        assi.ia_assi_footer,
        assi.ia_assi_signature,
        assi.ia_assi_year,
        assi.ia_assi_file        
    From
        ia_assignment assi
    Where
        assi.ia_assi_auto = {assignment_number}    
    ;"""
    
    for row in ms_from_cursor.execute(s_sql).fetchall():

        # Log
        if l_debug:
            print(row[1])
        funcfile.writelog(f"%t Audit assignment report {assignment_number} requested by {s_name}")
        
        # The message
        assignment_report = row[1]
        s_message += " " + assignment_report + " was mailed to " + s_mail

        # EXPORT THE ASSIGNMENT RECORD

        # Assignment header
        if row[2]:
            log_entries.append(row[2])

        # Assignment header title
        header_line = f"<h1>{row[3]}</h1>" if row[3] else "<h1>Audit report</h1>"
        log_entries.append(header_line)

        # Assignment report
        if row[4]:
            log_entries.append(row[4])

        # Store assignment data for later use

        # Additional assignment data
        assignment_finding_header_single = f"<h1>{row[5]}</h1>" if row[5] else "<h1>Audit finding</h1>"
        assignment_finding_header_multiple = f"<h1>{row[6]}<h1>" if row[6] else "<h1>Audit findings</h1>"
        assignment_report_footer = row[7] if row[7] else ""
        assignment_report_signature = row[8] if row[8] else ""
        assignment_reference = '<br />'
        assignment_reference += f'<p><span style="font-size: 8pt;">File reference: {row[9]}.{row[10]}</span></p>'

    # Write to file after assignment record is complete
    my_string = ' '.join(map(str, log_entries))
    funcfile.writelog(my_string, re_path, s_file)
    log_entries.clear()

    # BUILD THE FINDING RECORD
    s_sql = f"""
    Select
        assi.ia_assi_auto,
        assi.ia_assi_token,
        find.ia_find_auto,
        find.ia_find_name,
        find.ia_find_desc,
        find.ia_find_risk,
        find.ia_find_criteria,
        find.ia_find_procedure,
        find.ia_find_condition,
        find.ia_find_effect,
        find.ia_find_cause,
        find.ia_find_recommend,
        find.ia_find_comment,
        find.ia_find_frequency,
        find.ia_find_definition,
        find.ia_find_reference,
        find.ia_find_riskmatrix_toggle,
        rate.ia_findrate_name,
        rate.ia_findrate_desc,
        rate.ia_findrate_impact,
        lhoo.ia_findlike_name,
        lhoo.ia_findlike_desc,
        lhoo.ia_findlike_value,
        cont.ia_findcont_name,
        cont.ia_findcont_desc,
        cont.ia_findcont_value,
        clra.ia_findrate_name As ia_findrate_name_client,
        clra.ia_findrate_desc As ia_findrate_desc_client,
        clra.ia_findrate_impact As ia_findrate_impact_client,
        clli.ia_findlike_name As ia_findlike_name_client,
        clli.ia_findlike_desc As ia_findlike_desc_client,
        clli.ia_findlike_value As ia_findlike_value_client,
        clco.ia_findcont_name As ia_findcont_name_client,
        clco.ia_findcont_desc As ia_findcont_desc_client,
        clco.ia_findcont_value As ia_findcont_value_client
    From
        ia_assignment assi Inner Join
        ia_finding find On find.ia_assi_auto = assi.ia_assi_auto Left join
        ia_finding_rate rate On rate.ia_findrate_auto = find.ia_findrate_auto Left Join
        ia_finding_likelihood lhoo On lhoo.ia_findlike_auto = find.ia_findlike_auto Left Join
        ia_finding_control cont On cont.ia_findcont_auto = find.ia_findcont_auto Left Join
        ia_finding_rate clra On clra.ia_findrate_auto = find.ia_findrate_auto_client Left Join
        ia_finding_likelihood clli On clli.ia_findlike_auto = find.ia_findlike_auto_client Left Join
        ia_finding_control clco On clco.ia_findcont_auto = find.ia_findcont_auto_client
    Where
        assi.ia_assi_auto = {assignment_number} And
        find.ia_find_private = '0'
    Order By
        find.ia_find_appendix,
        find.ia_find_name
    ;"""

    records = ms_from_cursor.execute(s_sql).fetchall()

    # Do only if there are findings
    i_records = len(records)

    # Print singular or multiple audit finding heading
    if i_records > 1:
        log_entries.append(assignment_finding_header_multiple)
    else:
        log_entries.append(assignment_finding_header_single)

    if i_records > 0:

        # Found findings to report
        for row in records:

            if l_debug:
                print(row[3])

            # Finding description
            if row[4]:
                log_entries.append(row[4])

            # Finding risk
            if row[5]:
                log_entries.append(row[5])

            # Audit evaluation of the effectiveness of controls
            if row[16] == 'a' or row[16] == 'b':

                if int(row[19]) > 0 or int(row[22]) > 0 or float(row[25]) > 0:
                    log_entries.append("<h2>Audit evaluation of the effectiveness of controls</h2>")

                if int(row[19]) > 0:
                    log_entries.append(f"<strong>Impact rating</strong> - {row[17]} ({row[19]})<br>")
                    log_entries.append(f"{row[18]}<br><br>")

                if int(row[22]) > 0:
                    log_entries.append(f"<strong>Likelihood</strong> - {row[20]} ({row[22]})<br>")
                    log_entries.append(f"{row[21]}<br><br>")

                if float(row[25]) > 0:
                    log_entries.append(f"<strong>Effectiveness</strong> - {row[23]} ({row[25]})<br>")
                    log_entries.append(f"{row[24]}<br>")

            # Audit evaluation of the effectiveness of controls
            if row[16] == 'm' or row[16] == 'b':

                if int(row[28]) > 0 or int(row[31]) > 0 or float(row[34]) > 0:
                    log_entries.append("<h2>Management evaluation of the effectiveness of controls</h2>")

                if int(row[28]) > 0:
                    log_entries.append(f"<strong>Impact rating</strong> - {row[26]} ({row[28]})<br>")
                    log_entries.append(f"{row[27]}<br><br>")

                if int(row[31]) > 0:
                    log_entries.append(f"<strong>Likelihood</strong> - {row[29]} ({row[31]})<br>")
                    log_entries.append(f"{row[30]}<br><br>")

                if float(row[34]) > 0:
                    log_entries.append(f"<strong>Effectiveness</strong> - {row[32]} ({row[34]})<br>")
                    log_entries.append(f"{row[33]}<br>")

            # Finding criteria to reference
            for i in range(6, 15):
                if row[i]:
                    log_entries.append(row[i])

    else:

        # No findings - end the report
        pass

    # Add the assignment footer and signature
    log_entries.append(assignment_report_footer)
    log_entries.append(assignment_report_signature)
    log_entries.append(assignment_reference)


    # Build the document end
    html_end = """
</body>
</html>
    """
    log_entries.append(html_end)

    # Write to file after assignment record is complete
    my_string = ' '.join(map(str, log_entries))
    funcfile.writelog(my_string, re_path, s_file)
    log_entries.clear()

    # MAIL THE AUDIT REPORT
    if s_name != "" and s_mail != "":
        funcfile.writelog("%t Audit assignment report " + assignment_number + " mailed to " + s_mail)
        if l_debug:
            print("Send the report...")
        s_body: str = "Attached please find audit assignment report as requested:\n\r"
        s_body += "\n\r"
        s_body += assignment_report
        funcmail.send(s_name,
                      s_mail,
                      "E",
                      "Report audit assignment number (" + assignment_number + ")",
                      s_body,
                      re_path,
                      s_file)

    # POPULATE THE RETURN MESSAGE
    s_return_message = s_message

    # DELETE THE MAILED FILE
    if funcfile.file_delete(re_path, s_file):
        funcfile.writelog("%t Audit assignment report " + assignment_number + " deleted")
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
        s_return = robot_report_audit_assignment("649", "Albert", "21162395@nwu.ac.za")
        if funcconf.l_mess_project:
            print("RETURN: " + s_return)
            print("LENGTH: " + str(len(s_return)))
    except Exception as e:
        funcsys.ErrMessage(e, funcconf.l_mess_project, s_function, s_function)
