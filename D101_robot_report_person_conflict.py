"""
PREPARE AND SEND PEOPLE REPORTS VIA EMAIL FROM THE NWUIACA SYSTEM
PERSON (EMPLOYEE) CONFLICT OF INTEREST REPORT
Script: D101_robot_report_person_conflict.py
Author: Albert B Janse van Rensburg (NWU:21162395)
Created: 20 Jul 2021
"""

# IMPORT OWN MODULES
from _my_modules import funcconf
from _my_modules import funcsys

# INDEX
"""
ENVIRONMENT
OPEN THE DATABASES
BEGIN OF SCRIPT
BUILD THE DECLARATIONS REPORT
BUILD THE INTERESTS REPORT
END OF SCRIPT
"""

# VARIABLES
s_function: str = "D101_robot_report_person_conflict"


def robot_report_person_conflict(s_nwu: str = "", s_name: str = "", s_mail: str = ""):
    """
    REPORT EMPLOYEE PERSON CONFLICT OF INTERESTS

    :param s_nwu: NWU Number
    :param s_name: The name of the requester / recipient
    :param s_mail: The requester mail address
    :return: str: Info in message format
    """

    # IMPORT PYTHON MODULES
    import sqlite3
    from datetime import datetime

    # IMPORT OWN MODULES
    from _my_modules import funcconf
    from _my_modules import funccsv
    from _my_modules import funcdatn
    from _my_modules import funcfile
    from _my_modules import funcmail
    from _my_modules import funcsms
    from _my_modules import funcstat

    # DECLARE VARIABLES
    l_debug: bool = False

    """*************************************************************************
    ENVIRONMENT
    *************************************************************************"""
    if l_debug:
        print("ENVIRONMENT")

    # DECLARE VARIABLES
    s_description: str = "Conflict of interest reports"
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
    BUILD THE DECLARATIONS REPORT
    *****************************************************************************"""
    funcfile.writelog("BUILD THE DECLARATIONS REPORT")
    if l_debug:
        print("BUILD THE DECLARATIONS REPORT")

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
    s_file_prefix: str = "Y000_"
    s_file_name: str = "report_declarations_all"
    sr_file = s_file_prefix + s_file_name
    so_curs.execute("Drop table if exists " + sr_file)
    s_sql = "Create table "+sr_file+" AS " + """
    Select
        d.DECLARATION_ID,
        d.EMPLOYEE_NUMBER,
        p.name_full As NAME_FULL,
        d.DECLARATION_DATE,
        d.UNDERSTAND_POLICY_FLAG,
        d.INTEREST_TO_DECLARE_FLAG,
        d.FULL_DISCLOSURE_FLAG,
        Upper(d.STATUS) As STATUS,
        d.LINE_MANAGER,
        m.name_full As MANAGER_NAME_FULL,
        d.REJECTION_REASON,
        d.CREATION_DATE,
        d.AUDIT_USER,
        d.LAST_UPDATE_DATE,
        d.LAST_UPDATED_BY,
        d.EXTERNAL_REFERENCE
    From
        X000_declarations_all d Left Join
        PEOPLE.X000_PEOPLE p On p.employee_number = d.EMPLOYEE_NUMBER Left Join
        PEOPLE.X000_PEOPLE m On m.employee_number = d.LINE_MANAGER
    Where
        d.EMPLOYEE_NUMBER = '%PERSON%'
    Order By
        d.LAST_UPDATE_DATE
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
                                       "DECLARATION_DATE||' ('||INTEREST_TO_DECLARE_FLAG||') '||STATUS")
        s_message += '\n\n'
        s_message += 'Declarations on:'
        for item in l_records:
            s_message += '\n'
            for element in item:
                s_message += element

        # EXPORT RECORDS
        print("Export findings...")
        sx_path = re_path
        sx_file = sr_file + "_"
        sx_file_dated = sx_file + funcdatn.get_today_date_file()
        s_head = funccsv.get_colnames_sqlite(so_conn, sr_file)
        # funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file, s_head)
        funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file_dated, s_head)
        funcfile.writelog("%t EXPORT DATA: " + sx_path + sx_file_dated)

        # MAIL THE REPORT
        s_report = "All DIY declarations included!"
        if s_name != "" and s_mail != "":
            l_mailed = True
            funcfile.writelog("%t Declarations mailed to " + s_mail)
            if l_debug:
                print("Send the report...")
            s_body: str = "Attached please find conflict of interest declarations for " + s_nwu + "."
            s_body += "\n\r"
            s_body += s_report
            funcmail.send(s_name,
                          s_mail,
                          "E",
                          s_description + " for " + s_nwu,
                          s_body,
                          re_path,
                          sx_file_dated + ".csv")

        # DELETE THE MAILED FILE
        if funcfile.file_delete(re_path, sx_file_dated):
            funcfile.writelog("%t Declarations deleted")
            if l_debug:
                print("Delete the report...")

    else:
        s_message += "\n\n"
        s_message += "No declarations on record."

    """ ****************************************************************************
    BUILD THE INTERESTS REPORT
    *****************************************************************************"""
    funcfile.writelog("BUILD THE INTERESTS REPORT")
    if l_debug:
        print("BUILD THE INTERESTS REPORT")

    # BUILD THE TABLE
    if l_debug:
        print("Build interests table...")
    s_file_prefix: str = "Y000_"
    s_file_name: str = "report_interests_all"
    sr_file = s_file_prefix + s_file_name
    so_curs.execute("Drop table if exists " + sr_file)
    s_sql = "Create table "+sr_file+" AS " + """
    Select
        i.INTEREST_ID,
        i.DECLARATION_ID,
        i.EMPLOYEE_NUMBER,
        p.name_full As NAME_FULL,
        i.DECLARATION_DATE,
        i.CONFLICT_TYPE_ID,
        Upper(i.CONFLICT_TYPE) As CONFLICT_TYPE,
        i.INTEREST_TYPE_ID,
        Upper(i.INTEREST_TYPE) As INTEREST_TYPE,
        i.STATUS_ID,
        Upper(i.INTEREST_STATUS) As INTEREST_STATUS,
        i.PERC_SHARE_INTEREST,
        Upper(i.ENTITY_NAME) As ENTITY_NAME,
        i.ENTITY_REGISTRATION_NUMBER,
        Upper(i.OFFICE_ADDRESS) As OFFICE_ADDRESS,
        Upper(i.DESCRIPTION) As DESCRIPTION,
        i.DIR_APPOINTMENT_DATE,
        i.LINE_MANAGER,
        m.name_full As MANAGER_NAME_FULL,
        i.NEXT_LINE_MANAGER,
        i.INDUSTRY_CLASS_ID,
        Upper(i.INDUSTRY_TYPE) As INDUSTRY_TYPE,
        i.TASK_PERF_AGREEMENT,
        i.MITIGATION_AGREEMENT,
        i.REJECTION_REASON,
        i.CREATION_DATE,
        i.AUDIT_USER,
        i.LAST_UPDATE_DATE,
        i.LAST_UPDATED_BY,
        i.EXTERNAL_REFERENCE
    From
        X000_interests_all i Left Join
        PEOPLE.X000_PEOPLE p On p.employee_number = i.EMPLOYEE_NUMBER Left Join
        PEOPLE.X000_PEOPLE m On m.employee_number = i.LINE_MANAGER
    Where
        i.EMPLOYEE_NUMBER = '%PERSON%'
    Order By
        i.LAST_UPDATE_DATE    
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
                                       "DECLARATION_DATE||' - '||INTEREST_STATUS")
        s_message += '\n\n'
        s_message += 'Interests declared:'
        for item in l_records:
            s_message += '\n'
            for element in item:
                s_message += element

        # EXPORT RECORDS
        print("Export findings...")
        sx_path = re_path
        sx_file = sr_file + "_"
        sx_file_dated = sx_file + funcdatn.get_today_date_file()
        s_head = funccsv.get_colnames_sqlite(so_conn, sr_file)
        # funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file, s_head)
        funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file_dated, s_head)
        funcfile.writelog("%t EXPORT DATA: " + sx_path + sx_file_dated)

        # MAIL THE REPORT
        s_report = "All DIY interests included!"
        if s_name != "" and s_mail != "":
            l_mailed = True
            funcfile.writelog("%t Interests mailed to " + s_mail)
            if l_debug:
                print("Send the report...")
            s_body: str = "Attached please find conflict of interest interests for " + s_nwu + "."
            s_body += "\n\r"
            s_body += s_report
            funcmail.send(s_name,
                          s_mail,
                          "E",
                          s_description + " for " + s_nwu,
                          s_body,
                          re_path,
                          sx_file_dated + ".csv")

        # DELETE THE MAILED FILE
        if funcfile.file_delete(re_path, sx_file_dated):
            funcfile.writelog("%t Interests deleted")
            if l_debug:
                print("Delete the report...")

    else:
        s_message += "\n\n"
        s_message += "No interests on record."

    # POPULATE THE RETURN MESSAGE
    if l_mailed:
        s_message += "\n\n"
        s_message += "Reports were mailed to " + s_mail + "."
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
        s_return = robot_report_person_conflict("21162395", "Albert", "21162395@nwu.ac.za")
        if funcconf.l_mess_project:
            print("RETURN: " + s_return)
            print("LENGTH: " + str(len(s_return)))
    except Exception as e:
        funcsys.ErrMessage(e, funcconf.l_mess_project, s_function, s_function)
