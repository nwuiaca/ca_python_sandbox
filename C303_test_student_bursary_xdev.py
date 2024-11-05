"""
Script to test STUDENT BURSARIES
Created on: 29 Jan 2021
Author: Albert J v Rensburg (NWU21162395)
"""

# IMPORT PYTHON MODULES
import sqlite3
import csv

# IMPORT OWN MODULES
from _my_modules import funcconf
from _my_modules import funccsv
from _my_modules import funcdatn
from _my_modules import funcfile
from _my_modules import funcstat
from _my_modules import funcsys
from _my_modules import funcsms
from _my_modules import functest

# INDEX
"""
ENVIRONMENT
OPEN THE DATABASES
TEMPORARY AREA
BEGIN OF SCRIPT
END OF SCRIPT
"""

# SCRIPT WIDE VARIABLES
s_function: str = "C303_test_student_bursary_xdev"


def student_bursary(s_period: str = "curr"):
    """
    Script to test STUDENT BURSARIES
    :param s_period: str: The financial period
    :return: Nothing
    """

    """*****************************************************************************
    ENVIRONMENT
    *****************************************************************************"""

    # FUNCTION WIDE VARIABLES
    """
    if s_period == "prev":
        s_year = funcdatn.get_previous_year()
    else:
        s_year = funcdatn.get_current_year()
    """
    ed_path: str = f"{funcconf.drive_system}_external_data/"  # External data path
    re_path: str = f"{funcconf.drive_data_results}Vss/"
    so_path: str = f"{funcconf.drive_data_raw}Vss_fee/"  # Source database path
    so_file: str = "Vss_test_bursary.sqlite"
    l_debug: bool = True
    # l_mail: bool = funcconf.l_mail_project
    l_mail: bool = False
    # l_mess: bool = funcconf.l_mess_project
    l_mess: bool = False
    l_record: bool = False
    l_export: bool = True
    s_burs_code: str = "('042', '052', '381', '500')"  # Current bursary transaction codes
    s_staff_code = "('021')"  # Staff discount transaction code

    # LOG
    funcfile.writelog("Now")
    funcfile.writelog("SCRIPT: " + s_function.upper())
    funcfile.writelog("-" * len("script: "+s_function))
    if l_debug:
        print(s_function.upper())

    # MESSAGE
    if l_mess:
        funcsms.send_telegram("", "administrator", "<b>" + s_function + "</b>")

    """************************************************************************
    OPEN THE DATABASES
    ************************************************************************"""
    funcfile.writelog("OPEN THE DATABASES")
    if l_debug:
        print("OPEN THE DATABASES")

    # OPEN THE SQLITE DATABASE
    # Create the connection
    so_conn = sqlite3.connect(so_path + so_file)
    # Create the cursor
    so_curs = so_conn.cursor()
    funcfile.writelog("OPEN DATABASE: " + so_file)

    # ATTACH VSS DATABASE
    if l_debug:
        print("Attach vss database...")
    so_curs.execute(f"ATTACH DATABASE '{funcconf.drive_data_raw}Vss/Vss.sqlite' AS 'VSS'")
    funcfile.writelog("%t ATTACH DATABASE: Vss.sqlite")
    so_curs.execute(f"ATTACH DATABASE '{funcconf.drive_data_raw}Vss/Vss_curr.sqlite' AS 'VSSCURR'")
    funcfile.writelog("%t ATTACH DATABASE: Vss_curr.sqlite")
    so_curs.execute(f"ATTACH DATABASE '{funcconf.drive_data_raw}Vss/Vss_prev.sqlite' AS 'VSSPREV'")
    funcfile.writelog("%t ATTACH DATABASE: Vss_prev.sqlite")
    so_curs.execute(f"ATTACH DATABASE '{funcconf.drive_data_raw}People/People.sqlite' AS 'PEOPLE'")
    funcfile.writelog("%t ATTACH DATABASE: People.sqlite")

    """************************************************************************
    TEMPORARY AREA
    ************************************************************************"""
    funcfile.writelog("TEMPORARY AREA")
    if l_debug:
        print("TEMPORARY AREA")

    """************************************************************************
    BEGIN OF SCRIPT
    ************************************************************************"""
    funcfile.writelog("BEGIN OF SCRIPT")
    if l_debug:
        print("BEGIN OF SCRIPT")

    """*****************************************************************************
    TEST BURSARY STAFF DISCOUNT AND NSFAS
    *****************************************************************************"""

    # DECLARE TEST VARIABLES
    i_finding_before = 0
    i_finding_after = 0
    s_description = "Bursary staff discount and nsfas"
    s_file_prefix: str = "X005b"
    s_file_name: str = "bursary_staff_discount_and_nsfas"
    s_finding: str = "BURSARY STAFF DISCOUNT AND NSFAS"
    s_report_file: str = "303_reported.txt"

    # OBTAIN TEST RUN FLAG
    if functest.get_test_flag(so_curs, "VSS", "TEST " + s_finding, "RUN") == "FALSE":

        if l_debug:
            print('TEST DISABLED')
        funcfile.writelog("TEST " + s_finding + " DISABLED")

    else:

        # LOG
        funcfile.writelog("TEST " + s_finding)
        if l_debug:
            print("TEST " + s_finding)

        # OBTAIN MASTER DATA 1
        if l_debug:
            print("Obtain master data...")
        sr_file: str = s_file_prefix + "aa_" + s_file_name
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        s_sql = "Create Table " + sr_file + " As " + """
        Select
            burs.student,
            burs.total_burs,
            burs.total_loan,
            burs.total_external,
            burs.total_internal,
            burs.total_research,
            burs.total_trust,
            burs.total_other,
            burs.staff_discount,
            burs.active,
            burs.levy_category,
            burs.enrol_category,
            burs.qualification,
            burs.qualification_type,
            burs.discontinue_date,
            burs.discontinue_result,
            burs.discontinue_reason
        From
            X001_Bursary_summary_student burs
        Where
            burs.staff_discount <> 0
        ;"""
        # s_sql = s_sql.replace("%RESULT_EXCLUDE%", s_exclude_list)
        so_curs.execute(s_sql)
        funcfile.writelog("%t BUILD TABLE: " + sr_file)
        if l_debug:
            so_conn.commit()

    """************************************************************************
    END OF SCRIPT
    ************************************************************************"""
    funcfile.writelog("END OF SCRIPT")
    if l_debug:
        print("END OF SCRIPT")

    # CLOSE THE DATABASE CONNECTION
    so_conn.commit()
    so_conn.close()

    # CLOSE THE LOG WRITER
    funcfile.writelog("-" * len("completed: "+s_function))
    funcfile.writelog("COMPLETED: " + s_function.upper())

    return


if __name__ == '__main__':
    try:
        student_bursary("curr")
    except Exception as e:
        funcsys.ErrMessage(e)
