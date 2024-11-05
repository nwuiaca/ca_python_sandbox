"""
Script to build standard VSS lists
Created on: 01 Mar 2018
Copyright: Albert J v Rensburg
"""

""" INDEX **********************************************************************
BUILD PREVIOUS YEAR STUDENTS
BUILD MODULES
*****************************************************************************"""

# Import python modules
import sqlite3
import sys

# Add own module path
sys.path.append('../_my_modules')

# Import own modules
import funcconf
import funcfile
import funcstudent

# Open the script log file ******************************************************

print("----------------------------")
print("REPORT_VSS_STUDENT_LIST_PREV")
print("----------------------------")
funcfile.writelog("Now")
funcfile.writelog("SCRIPT: REPORT_VSS_STUDENT_LIST_PREV")
funcfile.writelog("------------------------------------")
ilog_severity = 1

# Declare variables
so_path = f"{funcconf.drive_data_raw}Vss/" #Source database path
so_file = "Vss.sqlite" #Source database
re_path = f"{funcconf.drive_data_results}Vss/" #Results
ed_path = f"{funcconf.drive_system}_external_data/"
s_sql = "" #SQL statements
l_export = False

# Open the SOURCE file
with sqlite3.connect(so_path+so_file) as so_conn:
    so_curs = so_conn.cursor()
funcfile.writelog("OPEN DATABASE: " + so_file)

"""*************************************************************************
BUILD STUDENT LIST
*************************************************************************"""

# OBTAIN DEFERMENT YEAR
print("Note")
print("----")
print("1. Period Students should exist in VSS for the year.")
print("")
s_year = input("Students year? (yyyy) ")
print("")

funcfile.writelog("DEFERMENT YEAR " + s_year)

funcstudent.Studentlist(so_conn,re_path,'peri',s_year,True)

# Close the connection *********************************************************
so_conn.close()

# Close the log writer *********************************************************
funcfile.writelog("---------------------------------------")
funcfile.writelog("COMPLETED: REPORT_VSS_STUDENT_LIST_PREV")
