"""
PREPARE AND SEND PEOPLE REPORTS VIA EMAIL FROM THE NWUIACA SYSTEM
PERSON (EMPLOYEE) LEAVE REPORT
Script: D102_robot_report_person_leave.py
Author: Albert B Janse van Rensburg (NWU:21162395)
Created: 21 Jul 2021
"""

# IMPORT OWN MODULES
from _my_modules import funcconf
from _my_modules import funcsys

# INDEX
"""
ENVIRONMENT
OPEN THE DATABASES
BEGIN OF SCRIPT
BUILD THE LEAVE REPORT
END OF SCRIPT
"""

# VARIABLES
s_function: str = "D102_robot_report_person_leave"


def robot_report_person_leave(s_nwu: str = "", s_name: str = "", s_mail: str = ""):
    """
    REPORT EMPLOYEE PERSON LEAVE

    :param s_nwu: NWU Number
    :param s_name: The name of the requester / recipient
    :param s_mail: The requester mail address
    :return: str: Info in message format
    """

    # IMPORT PYTHON MODULES
    import sqlite3
    from datetime import datetime

    # IMPORT OWN MODULES
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
    s_description: str = "Leave report"
    so_path: str = f"{funcconf.drive_data_raw}People_leave/"  # Source database path
    so_file: str = "People_leave.sqlite"  # Source database
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
    BUILD THE LEAVE REPORT
    *****************************************************************************"""
    funcfile.writelog("BUILD THE LEAVE REPORT")
    if l_debug:
        print("BUILD THE LEAVE REPORT")

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
        print("Build leave table...")
    s_file_prefix: str = "Y000_"
    s_file_name: str = "report_leave_all"
    sr_file = s_file_prefix + s_file_name
    so_curs.execute("Drop table if exists " + sr_file)
    s_sql = "Create table "+sr_file+" AS " + """
    Select
        aa.ABSENCE_ATTENDANCE_ID,
        aa.EMPLOYEE_NUMBER,
        pe.name_address As EMPLOYEE_NAME,
        aa.BUSINESS_GROUP_ID,
        aa.DATE_NOTIFICATION,
        aa.DATE_START,
        aa.DATE_END,
        aa.ABSENCE_DAYS,
        aa.ABSENCE_ATTENDANCE_TYPE_ID,
        at.NAME AS LEAVE_TYPE,
        aa.ABS_ATTENDANCE_REASON_ID,
        ar.NAME AS LEAVE_REASON,
        ar.MEANING AS REASON_DESCRIP,
        aa.AUTHORISING_PERSON_ID,
        ap.name_address AS AUTHORISE_NAME,
        aa.ABSENCE_HOURS,
        aa.OCCURRENCE,
        aa.SSP1_ISSUED,
        aa.PROGRAM_APPLICATION_ID,
        aa.ATTRIBUTE1,
        aa.ATTRIBUTE2,
        aa.ATTRIBUTE3,
        aa.ATTRIBUTE4,
        aa.ATTRIBUTE5,
        aa.LAST_UPDATE_DATE,
        aa.LAST_UPDATED_BY,
        aa.LAST_UPDATE_LOGIN,
        aa.CREATED_BY,
        aa.CREATION_DATE,
        aa.REASON_FOR_NOTIFICATION_DELAY,
        aa.ACCEPT_LATE_NOTIFICATION_FLAG,
        aa.OBJECT_VERSION_NUMBER,
        at.INPUT_VALUE_ID,
        at.ABSENCE_CATEGORY,
        at.MEANING AS TYPE_DESCRIP
    FROM
        X100_Per_absence_attendances aa Left Join
        X102_Per_absence_attendance_types at ON at.ABSENCE_ATTENDANCE_TYPE_ID = aa.ABSENCE_ATTENDANCE_TYPE_ID Left Join
        X101_Per_abs_attendance_reasons ar ON ar.ABS_ATTENDANCE_REASON_ID = aa.ABS_ATTENDANCE_REASON_ID Left Join
        PEOPLE.X000_PEOPLE pe On pe.employee_number = aa.EMPLOYEE_NUMBER Left Join
        PEOPLE.X000_PEOPLE ap On pe.person_id = aa.AUTHORISING_PERSON_ID
    Where
        aa.EMPLOYEE_NUMBER = '%PERSON%'
    Order By
        aa.EMPLOYEE_NUMBER,
        aa.DATE_START,
        aa.DATE_END        
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
                                       "DATE_START||' '||DATE_END||' ('||ABSENCE_DAYS||') '||LEAVE_TYPE")
        s_message += '\n\n'
        s_message += 'Leave periods for this and previous year:'
        s_data: str = ''
        for item in l_records:
            s_data = ''
            for element in item:
                s_data += element
            print(s_data)
            print(funcdatn.get_current_year())
            if funcdatn.get_current_year() in s_data or  funcdatn.get_previous_year() in s_data:
                s_message += '\n'
                s_message += s_data

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
        s_report = "All DIY leave included!"
        if s_name != "" and s_mail != "":
            l_mailed = True
            funcfile.writelog("%t Leave report mailed to " + s_mail)
            if l_debug:
                print("Send the report...")
            s_body: str = "Attached please find leave report for " + s_lookup_name + " (" + s_nwu + ")."
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
            funcfile.writelog("%t Leave deleted")
            if l_debug:
                print("Delete the report...")

    else:

        s_message += "\n\n"
        s_message += "No leave on record."

    # POPULATE THE RETURN MESSAGE
    if l_mailed:
        s_message += "\n\n"
        s_message += "Report was mailed to " + s_mail + "."

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
        s_return = robot_report_person_leave("21162395", "Albert", "21162395@nwu.ac.za")
        if funcconf.l_mess_project:
            print("RETURN: " + s_return)
            print("LENGTH: " + str(len(s_return)))
    except Exception as e:
        funcsys.ErrMessage(e, funcconf.l_mess_project, s_function, s_function)
