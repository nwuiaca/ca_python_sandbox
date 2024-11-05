"""
Script to export KFS current payments
Created on: 19 Aug 2019
Author: Albert J van Rensburg (NWU21162395)
"""

# IMPORT PYTHON MODULES
import sqlite3

# IMPORT OWN MODULES
from _my_modules import funcconf
from _my_modules import funccsv
from _my_modules import funcdatn
from _my_modules import funcfile

"""*****************************************************************************
ENVIRONMENT
*****************************************************************************"""

# ASK QUESTIONS - WHICH YEAR
print("")
s_year = input("For which period? (yyyy) ")
print("")
s_period: str = ""
if s_year == funcdatn.get_current_year():
    s_period = "curr"
elif s_year == funcdatn.get_previous_year():
    s_period = "prev"
else:
    s_period = s_year

# ASK QUESTIONS - WHICH TABLE
print("")
print("(S)ingle all payments")
print("(I)nitiator details")
print("(A)pprover details")
print("(C)ost string details")
print("")
s_tabl = input("Which table? (SIAC) ").upper()
print("")

# OPEN THE LOG WRITER
funcfile.writelog("Now")
funcfile.writelog("SCRIPT: REPORT KFS PAYMENTS CURRENT")
funcfile.writelog("-----------------------------------")
print("---------------------------")
print("REPORT KFS PAYMENTS CURRENT")
print("---------------------------")

# DECLARE VARIABLES
so_path = f"{funcconf.drive_data_raw}Kfs/"  # Source database path
so_file = "Kfs_" + s_period + ".sqlite"  # Source database
l_export: bool = True
s_sql = ""  # SQL statements

"""*****************************************************************************
OPEN THE DATABASES
*****************************************************************************"""
print("OPEN THE DATABASES")
funcfile.writelog("OPEN THE DATABASES")

# OPEN THE WORKING DATABASE
with sqlite3.connect(so_path + so_file) as so_conn:
    so_curs = so_conn.cursor()
funcfile.writelog("OPEN DATABASE: " + so_file)

""" ****************************************************************************
BEGIN OF SCRIPT
*****************************************************************************"""
print("BEGIN OF SCRIPT")
funcfile.writelog("BEGIN OF SCRIPT")

# Export the declaration data
if s_tabl == 'I':
    sr_file = "X001ab_Report_payments_initiate"
    sx_file = "Creditor_report_payments_001ab_initiate_"
elif s_tabl == 'A':
    sr_file = "X001ac_Report_payments_approve"
    sx_file = "Creditor_report_payments_001ac_approve_"
elif s_tabl == 'C':
    sr_file = "X001ad_Report_payments_accroute"
    sx_file = "Creditor_report_payments_001ad_accroute_"
else:
    sr_file = "X001aa_Report_payments"
    sx_file = "Creditor_report_payments_001aa_"
if l_export == True:
    if s_period == "curr":
        sx_path = f"{funcconf.drive_data_results}Kfs/" + funcdatn.get_current_year() + "/"
    elif s_period == "prev":
        sx_path = f"{funcconf.drive_data_results}Kfs/" + funcdatn.get_previous_year() + "/"
    else:
        sx_path = f"{funcconf.drive_data_results}Kfs/" + s_period + "/"
    print("Export data..." + sx_path + sx_file)
    s_head = funccsv.get_colnames_sqlite(so_conn, sr_file)
    funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file, s_head)
    funcfile.writelog("%t EXPORT DATA: " + sx_path + sx_file)

""" ****************************************************************************
END OF SCRIPT
*****************************************************************************"""
print("END OF SCRIPT")
funcfile.writelog("END OF SCRIPT")

# CLOSE THE DATABASE CONNECTION
so_conn.commit()
so_conn.close()

# Close the log writer *********************************************************
funcfile.writelog("--------------------------------------")
funcfile.writelog("COMPLETED: REPORT KFS PAYMENTS CURRENT")
