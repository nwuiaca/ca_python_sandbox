""" Script to extract STUDENT DEFERMENTS FOR THE CURRENT AND PREVIOUS YEAR *****
Created on: 19 MAR 2018
Author: Albert J v Rensburg (NWU21162395)
*****************************************************************************"""

""" INDEX **********************************************************************
ENVIRONMENT
OPEN THE DATABASES
TEMPORARY AREA
BEGIN OF SCRIPT
END OF SCRIPT
*****************************************************************************"""

"""*****************************************************************************
ENVIRONMENT
*****************************************************************************"""

# IMPORT PYTHON MODULES
import sys

# ADD OWN MODULE PATH
sys.path.append('../_my_modules')

# IMPORT PYTHON OBJECTS
import csv
import datetime
import sqlite3

# IMPORT OWN MODULES
import funcconf
import funcdatn
import funccsv
import funcfile

# SCRIPT LOG FILE
funcfile.writelog("Now")
funcfile.writelog("SCRIPT: REPORT_VSS_DEFERMENTS")
funcfile.writelog("-----------------------------")
print("---------------------")    
print("REPORT_VSS_DEFERMENTS")
print("---------------------")
ilog_severity = 1

# DECLARE VARIABLES
# s_period = "prev"
# s_year = "2018"
so_path = f"{funcconf.drive_data_raw}Vss_deferment/" #Source database path
so_file = "Vss_deferment.sqlite" #Source database
re_path = f"{funcconf.drive_data_results}Vss/"
ed_path = f"{funcconf.drive_system}_external_data/"
s_sql = "" #SQL statements
l_export = True
l_mail = False
l_record = False
l_vacuum = False

"""*****************************************************************************
OPEN THE DATABASES
*****************************************************************************"""
print("OPEN THE DATABASES")
funcfile.writelog("OPEN THE DATABASES")

# OPEN SQLITE SOURCE table
print("Open sqlite database...")
with sqlite3.connect(so_path+so_file) as so_conn:
    so_curs = so_conn.cursor()
funcfile.writelog("OPEN DATABASE: " + so_file)

# ATTACH VSS DATABASE
print("Attach vss database...")
so_curs.execute(f"ATTACH DATABASE '{funcconf.drive_data_raw}Vss/Vss.sqlite' AS 'VSS'")
funcfile.writelog("%t ATTACH DATABASE: Vss.sqlite")

""" ****************************************************************************
TEMPORARY AREA
*****************************************************************************"""
print("TEMPORARY AREA")
funcfile.writelog("TEMPORARY AREA")

""" ****************************************************************************
BEGIN OF SCRIPT
*****************************************************************************"""
print("BEGIN OF SCRIPT")
funcfile.writelog("BEGIN OF SCRIPT")



""" ****************************************************************************
END OF SCRIPT
*****************************************************************************"""
print("END OF SCRIPT")
funcfile.writelog("END OF SCRIPT")

# CLOSE THE DATABASE CONNECTION
if l_vacuum == True:
    print("Vacuum the database...")
    so_conn.commit()
    so_conn.execute('VACUUM')
    funcfile.writelog("%t VACUUM DATABASE: " + so_file)
so_conn.commit()
so_conn.close()

# CLOSE THE LOG WRITER *********************************************************
funcfile.writelog("------------------------------------")
funcfile.writelog("COMPLETED: REPORT_VSS_DEFERMENTS_DEV")
