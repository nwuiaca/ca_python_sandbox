""" C300_TEST_STUDENT_GENERAL **************************************************
*** Script to vss student general items
*** Albert J van Rensburg (21162395)
*** 25 Jun 2018
*****************************************************************************"""

# Import python modules
import csv
import datetime
import sqlite3
import sys

# Add own module path
sys.path.append('../_my_modules')

# Import own modules
import funcconf
import funcdatn
import funccsv
import funcfile
import funcmail
import funcsys

# Open the script log file ******************************************************

funcfile.writelog("Now")
funcfile.writelog("SCRIPT: C300_TEST_STUDENT_GENERAL_DEV")
funcfile.writelog("----------------------------------=--")
print("-------------------------")
print("C300_TEST_STUDENT_GENERAL")
print("-------------------------")
ilog_severity = 1

# Declare variables
so_path = f"{funcconf.drive_data_raw}Vss_general/" #Source database path
re_path = f"{funcconf.drive_data_results}Vss/" #Results
ed_path = f"{funcconf.drive_system}_external_data/"
so_file = "Vss_general.sqlite" #Source database
s_sql = "" #SQL statements
l_mail = True
l_export = True

# Open the SOURCE file
with sqlite3.connect(so_path+so_file) as so_conn:
    so_curs = so_conn.cursor()

funcfile.writelog("OPEN DATABASE: " + so_file)

so_curs.execute(f"ATTACH DATABASE '{funcconf.drive_data_raw}vss/Vss.sqlite' AS 'VSS'")
funcfile.writelog("%t ATTACH DATABASE: Vss.sqlite")

""" ****************************************************************************
BEGIN
*****************************************************************************"""


 
""" ****************************************************************************
END
*****************************************************************************"""

# Close the table connection ***************************************************
so_conn.close()

# Close the log writer *********************************************************
funcfile.writelog("----------------------------------------")
funcfile.writelog("COMPLETED: C300_TEST_STUDENT_GENERAL_DEV")
