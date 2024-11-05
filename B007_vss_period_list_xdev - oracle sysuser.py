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
from _my_modules import funcoracle

""" INDEX **********************************************************************
ENVIRONMENT
*****************************************************************************"""


def vss_period_list(s_period="curr"):
    """
    Script to build standard KFS lists
    :type s_period: str: The financial period (curr, prev or year)
    :return: Nothing
    """

    # Declare initial variables
    script_name: str = 'B007_VSS_PERIOD_LIST_XDEV'
    l_debug: bool = True
    l_message: bool = funcconf.l_mess_project
    l_message: bool = False


    # Open the log writer
    if l_debug:
        print(f"{script_name}")
    funcfile.writelog("Now")
    funcfile.writelog(f"SCRIPT: {script_name}")
    funcfile.writelog("----------------------------")

    """*************************************************************************
    ENVIRONMENT
    *************************************************************************"""
    if l_debug:
        print("ENVIRONMENT")
    funcfile.writelog("ENVIRONMENT")

    # DECLARE VARIABLES
    so_path: str = f"{funcconf.drive_data_raw}Vss/"  # Source database path
    if s_period == "curr":
        s_year = funcdatn.get_current_year()
        so_file = "Vss_curr.sqlite"  # Source database
        s_table_name: str = 'User login recent'
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

    """************************************************************************
    EXTRACT VSS SYSTEM USERS DIRECT FROM VSS ORACLE
    ************************************************************************"""

    # Build the Oracle sql statement
    s_sql = """
    Select
        su.KUSERCODE As USER_CODE,
        su.FUSERBUSINESSENTITYID As NWU_NUMBER,
        su.STARTDATE,
        su.ENDDATE,
        codses.CODELONGDESCRIPTION As LIMITER,
        codtyp.CODELONGDESCRIPTION As USER_TYPE,
        su.DATELASTLOGON As LAST_LOGIN,
        To_Char(Cast(su.DATELASTLOGON As Date), 'hh24:mi') As LAST_LOGON_TIME,
        su.NOTE
    From
        SYSTEMUSER su Left Join
        CODEDESCRIPTION codses On codses.KCODEDESCID = su.FSESSIONTIMELIMITCODEID
                And codses.KSYSTEMLANGUAGECODE = 3 Left Join
        CODEDESCRIPTION codtyp On codtyp.KCODEDESCID = su.FUSERTYPECODEID
                And codtyp.KSYSTEMLANGUAGECODE = 3
    Where
        Trunc(su.DATELASTLOGON) >= Trunc(SysDate - 2)
    Order By
        LAST_LOGIN Desc
    """
    # s_sql = s_sql.replace("%BEGIN%", year_start)
    # s_sql = s_sql.replace("%END%", year_end)
    if l_debug:
        print('Run result SQL script')
        print(s_sql)

    # Execute the query
    try:
        funcoracle.oracle_sql_to_sqlite('Vss current', '000b_Table - oracle.csv', s_table_name, s_sql)
    except Exception as er:
        funcsys.ErrMessage(er)

    # MESSAGE
    if l_message:
        i = funcsys.tablerowcount(so_curs, sr_file)
        funcsms.send_telegram("", "administrator", "<b>" + str(i) + "</b> Transactions")

    """************************************************************************
    END OF SCRIPT
    ************************************************************************"""
    funcfile.writelog("END OF SCRIPT")
    if l_debug:
        print("END OF SCRIPT")

    # Close the connection
    so_conn.close()

    # Close the log writer
    funcfile.writelog("-------------------------")
    funcfile.writelog(f"COMPLETED: {script_name}")

    return


if __name__ == '__main__':
    try:
        vss_period_list("curr")
    except Exception as e:
        funcsys.ErrMessage(e, l_message, f"{script_name}", f"{script_name}")
