"""
Script to build kfs creditor payment tests
Created on: 16 Apr 2019
Author: Albert J v Rensburg (NWU21162395)
"""

# IMPORT PYTHON MODULES
import csv
import sqlite3
import datetime

# IMPORT OWN MODULES
from _my_modules import funcconf
from _my_modules import funcfile
from _my_modules import funccsv
from _my_modules import funcdatn
from _my_modules import funcsms
from _my_modules import funcstat
from _my_modules import funcsys
from _my_modules import functest

"""*****************************************************************************
ENVIRONMENT
*****************************************************************************"""

# DECLARE VARIABLES
l_debug: bool = True
so_path: str = f"{funcconf.drive_data_raw}Kfs/"  # Source database path
so_file: str = "Kfs_test_creditor.sqlite"  # Source database
re_path: str = f"{funcconf.drive_data_results}Kfs/"  # Results path
external_data_path: str = f"{funcconf.drive_system}_external_data/"  # External data path
l_export: bool = False
# l_mail: bool = funcconf.l_mail_project
l_mail: bool = False
# l_mess: bool = funcconf.l_mess_project
l_mess: bool = False
l_record: bool = False

# OPEN THE SCRIPT LOG FILE
funcfile.writelog("Now")
funcfile.writelog("SCRIPT: C201_CREDITOR_TEST_DEV")
funcfile.writelog("------------------------------")
if l_debug:
    print("----------------------")
    print("C201_CREDITOR_TEST_DEV")
    print("----------------------")

"""*****************************************************************************
OPEN THE DATABASES
*****************************************************************************"""
funcfile.writelog("OPEN THE DATABASES")
if l_debug:
    print("OPEN THE DATABASES")

# OPEN THE WORKING DATABASE
with sqlite3.connect(so_path + so_file) as sqlite_connection:
    sqlite_cursor = sqlite_connection.cursor()
funcfile.writelog("OPEN DATABASE: " + so_file)

# ATTACH DATA SOURCES
sqlite_cursor.execute(f"ATTACH DATABASE '{funcconf.drive_data_raw}Kfs/Kfs.sqlite' AS 'KFS'")
funcfile.writelog("%t ATTACH DATABASE: KFS.SQLITE")
sqlite_cursor.execute(f"ATTACH DATABASE '{funcconf.drive_data_raw}Kfs/Kfs_curr.sqlite' AS 'KFSCURR'")
funcfile.writelog("%t ATTACH DATABASE: KFS_CURR.SQLITE")
sqlite_cursor.execute(f"ATTACH DATABASE '{funcconf.drive_data_raw}Kfs/Kfs_prev.sqlite' AS 'KFSPREV'")
funcfile.writelog("%t ATTACH DATABASE: KFS_PREV.SQLITE")
sqlite_cursor.execute(f"ATTACH DATABASE '{funcconf.drive_data_raw}People/People.sqlite' AS 'PEOPLE'")
funcfile.writelog("%t ATTACH DATABASE: PEOPLE.SQLITE")
sqlite_cursor.execute(f"ATTACH DATABASE '{funcconf.drive_data_raw}People_conflict/People_conflict.sqlite' AS 'CONFLICT'")
funcfile.writelog("%t ATTACH DATABASE: CONFLICT.SQLITE")

""" ****************************************************************************
BEGIN OF SCRIPT
*****************************************************************************"""
funcfile.writelog("BEGIN OF SCRIPT")
if l_debug:
    print("BEGIN OF SCRIPT")

"""*****************************************************************************
END OF SCRIPT
*****************************************************************************"""
funcfile.writelog("END OF SCRIPT")
if l_debug:
    print("END OF SCRIPT")

# CLOSE THE DATABASE CONNECTION
sqlite_connection.close()

# CLOSE THE LOG WRITER
funcfile.writelog("---------------------------------")
funcfile.writelog("COMPLETED: C201_CREDITOR_TEST_DEV")
