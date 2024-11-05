"""
Script to prepare, export and mail Searchworks requests
Created on: 21 September 2023
Author: Yolandie Koekemoer (NWU:12788074)
Author: Albert J van Rensburg (NWU:21162395)
"""

# IMPORT PYTHON MODULES
import datetime
import sys
import sqlite3
import csv
import shutil
import os
import pandas as pd

# IMPORT OWN MODULES
# sys.path.append('_my_modules')
from _my_modules import funcconf
from _my_modules import funcdatn
from _my_modules import funcsys
from _my_modules import funcfile
from _my_modules import funcmail
from _my_modules import funcsms
from _my_modules import funcsqlite
from _my_modules import funcstat

# INDEX
"""
ENVIRONMENT
OPEN THE DATABASES
TEMPORARY AREA
BEGIN OF SCRIPT
BUILD TABLE WITH PREVIOUS SUBMITTED EMPLOYEES
READ THE WATCHLIST DATA
BUILD NEW SUBMISSIONS IN SQLITE
BUILD THE EMAIL
IMPORT SEARCHWORKS CIPC RESULTS IF A FILE WITH DATA EXISTS
NO SEARCHWORKS DIRECTORS IMPORTED
END OF SCRIPT
"""

# LOGIC
"""

"""

# SCRIPT WIDE VARIABLES
s_function: str = "B011_searchworks"


def searchworks_submit(l_override_date: bool = False):

    """****************************************************************************
    ENVIRONMENT
    Format can be different and not exact.
    <Tab> to shift in
    <Shift><Tab> to shift out
    ****************************************************************************"""

    # DECLARE VARIABLES
    l_debug: bool = True  # Enable the display of on-screen debug messages.
    l_mess: bool = funcconf.l_mess_project  # Enable / disable the robot communicator message function.
    # l_mess: bool = False  # Enable / disable the robot communicator message function.
    so_path: str = f"{funcconf.drive_data_raw}People_conflict/"  # Source database path
    so_file: str = "People_conflict.sqlite"  # Source database
    sw_path: str = f"{funcconf.drive_system}searchworks/"
    csv_master_name: str = "01_master_submit.csv"
    csv_send_name: str = "02_nwu_submit_" + funcdatn.get_today_date_file() + ".csv"
    csv_watchlist_name: str = "03_searchworks_watchlist.csv"
    csv_watchlist_new: str = "03_searchworks_watchlist_" + funcdatn.get_today_date_file() + ".csv"
    xls_results: str = "NWU CIPC Results.xlsx"
    csv_results: str = "04_cipc_results_" + funcdatn.get_today_date_file() + ".csv"
    csv_history: str = "05_cipc_history_" + funcdatn.get_today_date_file() + ".csv"

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
    if l_debug:
        print("OPEN THE DATABASES")
    funcfile.writelog("OPEN THE DATABASES")

    # OPEN THE WORKING DATABASE
    funcfile.writelog("OPEN DATABASE: " + so_file)
    with sqlite3.connect(so_path + so_file) as sqlite_connection:
        sqlite_cursor = sqlite_connection.cursor()

    # ATTACH DATA SOURCES
    funcfile.writelog("%t ATTACH DATABASE: People.sqlite")
    sqlite_cursor.execute(f"ATTACH DATABASE '{funcconf.drive_data_raw}People/People.sqlite' AS 'PEOPLE'")
    sqlite_cursor.execute(f"ATTACH DATABASE '{funcconf.drive_data_raw}Kfs/Kfs.sqlite' AS 'KFS'")
    funcfile.writelog("%t ATTACH DATABASE: KFS.SQLITE")

    """************************************************************************
    TEMPORARY AREA
    ************************************************************************"""
    if l_debug:
        print("TEMPORARY AREA")
    funcfile.writelog("TEMPORARY AREA")

    """************************************************************************
    BEGIN OF SCRIPT
    ************************************************************************"""
    if l_debug:
        print("BEGIN OF SCRIPT")
    funcfile.writelog("BEGIN OF SCRIPT")

    """************************************************************************
    END OF SCRIPT
    ************************************************************************"""
    if l_debug:
        print("END OF SCRIPT")
    funcfile.writelog("END OF SCRIPT")

    # Commit the changes and close the connection
    sqlite_connection.commit()
    sqlite_connection.close()

    return


if __name__ == '__main__':
    try:
        searchworks_submit(True)
    except Exception as e:
        funcsys.ErrMessage(e, funcconf.l_mess_project, s_function, s_function)
