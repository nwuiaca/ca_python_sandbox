"""
PREPARE AND SEND PEOPLE REPORTS VIA EMAIL FROM THE NWUIACA SYSTEM
PERSON (DIRECTOR) CONFLICT OF INTEREST REPORT
Script: D104_robot_report_person_director.py
Author: Albert B Janse van Rensburg (NWU:21162395)
Created: 18 October 2023
"""

# IMPORT PYTHON MODULES
import sqlite3
from datetime import datetime

# IMPORT OWN MODULES
from _my_modules import funcconf
from _my_modules import funcsys
from _my_modules import funcsqlite
from _my_modules import funccsv
from _my_modules import funcdatn
from _my_modules import funcfile
from _my_modules import funcmail
from _my_modules import funcsms
from _my_modules import funcstat

# INDEX
"""
ENVIRONMENT
OPEN THE DATABASES
BEGIN OF SCRIPT
BUILD THE DIRECTORS REPORT
END OF SCRIPT
"""

# VARIABLES
s_function: str = "D104_robot_report_person_director"


def robot_report_person_director(s_nwu: str = "", s_name: str = "", s_mail: str = ""):
    """
    REPORT EMPLOYEE PERSON DIRECTOR CONFLICT OF INTERESTS
    :param s_nwu: NWU Number
    :param s_name: The name of the requester / recipient
    :param s_mail: The requester mail address
    :return: str: Info in message format
    """

    # DECLARE VARIABLES
    l_debug: bool = False

    """*************************************************************************
    ENVIRONMENT
    *************************************************************************"""
    if l_debug:
        print("ENVIRONMENT")

    # DECLARE VARIABLES
    s_description: str = "Directorship report"
    so_path: str = f"{funcconf.drive_data_raw}People_conflict/"  # Source database path
    so_file: str = "People_conflict.sqlite"  # Source database
    re_path: str = f"{funcconf.drive_data_results}People/" + funcdatn.get_current_year() + "/"  # Results
    l_mess: bool = funcconf.l_mess_project
    # l_mess: bool = False
    l_mailed: bool = False

    # LOG
    if l_debug:
        print(s_function.upper())
    funcfile.writelog("Now")
    funcfile.writelog("SCRIPT: " + s_function.upper())
    funcfile.writelog("-" * len("script: "+s_function))
    funcfile.writelog("%t " + s_description + " for " + s_nwu + " requested by " + s_name)

    # MESSAGE
    if l_mess:
        funcsms.send_telegram("", "administrator", "<b>" + s_function.upper() + "</b>")

    """*****************************************************************************
    OPEN THE DATABASES
    *****************************************************************************"""
    funcfile.writelog("OPEN THE DATABASES")
    if l_debug:
        print("OPEN THE DATABASES")

    # OPEN THE WORKING DATABASE
    with sqlite3.connect(so_path+so_file) as so_conn:
        so_curs = so_conn.cursor()
    funcfile.writelog("OPEN DATABASE: " + so_file)

    # ATTACH DATA SOURCES
    so_curs.execute(f"ATTACH DATABASE '{funcconf.drive_data_raw}People/People.sqlite' AS 'PEOPLE'")
    funcfile.writelog("%t ATTACH DATABASE: PEOPLE.SQLITE")

    """ ****************************************************************************
    BUILD THE DIRECTORS REPORT
    *****************************************************************************"""
    funcfile.writelog("BUILD THE DIRECTORS REPORT")
    if l_debug:
        print("BUILD THE DIRECTORS REPORT")

    # OBTAIN THE NAME OF THE PERSON
    s_lookup_name = funcfile.get_field_value(so_curs,
                                             "PEOPLE.X000_PEOPLE",
                                             "name_address||' ('||preferred_name||')' ",
                                             "employee_number = '" + s_nwu + "'")
    if l_debug:
        print("FIELD LOOKUP: " + s_name)

    s_message: str = s_description + " for <b>" + s_lookup_name + '(' + s_nwu + ")</b>."

    # BUILD THE TABLE
    if l_debug:
        print("Build declarations table...")

    sr_file = 'Y000_report_directors_124211'
    so_curs.execute("Drop table if exists " + sr_file)
    sr_file = 'Y000_report_directors_124743'
    so_curs.execute("Drop table if exists " + sr_file)
    sr_file = 'Y000_report_directors_125950'
    so_curs.execute("Drop table if exists " + sr_file)

    now = datetime.now()
    current_time = now.strftime("%H%M%S")
    s_file_prefix: str = "Y000_"
    s_file_name: str = f"report_directors_{current_time}"
    sr_file = s_file_prefix + s_file_name
    so_curs.execute("Drop table if exists " + sr_file)
    s_sql = "Create table "+sr_file+" AS " + """
    Select
        s.nwu_number,
        s.employee_name,
        s.national_identifier,
        s.user_person_type,
        s.position_name,    
        s.date_submitted,
        s.import_date,
        s.registration_number,
        s.company_name,
        s.enterprise_type,
        s.company_status,
        s.history_date,
        s.business_start_date,
        s.directorship_status,
        s.directorship_start_date,
        s.directorship_end_date,
        s.directorship_type,
        s.directorship_interest
    From
        X004g_searchworks_results_history s
    Where
        s.nwu_number = '%PERSON%'
    Order By
        s.registration_number,
        s.directorship_start_date
    ;"""
    s_sql = s_sql.replace("%PERSON%", s_nwu)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: "+sr_file)

    # RECORDS FOUND
    if funcsys.tablerowcount(so_curs, sr_file) > 0:

        # BUILD THE MESSAGE
        l_records = funcstat.stat_list(so_curs,
                                       sr_file,
                                       "company_name")
        s_message += '\n\n'
        s_message += 'Companies:'
        for item in l_records:
            s_message += '\n'
            for element in item:
                s_message += element

        # EXPORT RECORDS
        if l_debug:
            print("Export findings...")
        sx_file = sr_file + ".csv"
        funcsqlite.sqlite_to_csv(so_curs, sr_file, re_path + sx_file)
        funcfile.writelog("%t EXPORT DATA: " + re_path + sx_file)

        # MAIL THE REPORT
        s_report = "Include all CIPC directorships on record since our process started in September 2023!"
        if s_name != "" and s_mail != "":
            l_mailed = True
            funcfile.writelog("%t Directors mailed to " + s_mail)
            if l_debug:
                print("Send the report...")
            s_body: str = "Attached please find the CIPC companies with directorships for " + s_nwu + "."
            s_body += "\n\r"
            s_body += s_report
            funcmail.send(s_name,
                          s_mail,
                          "E",
                          s_description + " for " + s_nwu,
                          s_body,
                          re_path,
                          sx_file)

        # DELETE THE MAILED FILE AND THE SQLite temporary table
        if funcfile.file_delete(re_path, sx_file):
            funcfile.writelog("%t Directors deleted")
            if l_debug:
                print("Delete the report...")
        so_curs.execute("Drop table if exists " + sr_file)

    else:

        s_message += "\n\n"
        s_message += "No directorships on record."

    # POPULATE THE RETURN MESSAGE
    if l_mailed:
        s_message += "\n\n"
        s_message += "Report was mailed to " + s_mail

    s_return_message = s_message

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
        s_return = robot_report_person_director("21162395", "Albert", "21162395@nwu.ac.za")
        if funcconf.l_mess_project:
            print("RETURN: " + s_return)
            print("LENGTH: " + str(len(s_return)))
    except Exception as e:
        funcsys.ErrMessage(e, funcconf.l_mess_project, s_function, s_function)
