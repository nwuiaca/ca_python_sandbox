"""
Script to export KFS gl transactions
Created on: 15 Nov 2019
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

# ASK QUESTIONS - WHICH COST STRING
print("")
s_cost = input("COSTzSEARCHzSTRINGS? ")
print("")

# OPEN THE LOG WRITER
funcfile.writelog("Now")
funcfile.writelog("SCRIPT: REPORT KFS GL EXPORT")
funcfile.writelog("----------------------------")
print("--------------------")
print("REPORT KFS GL EXPORT")
print("--------------------")

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

# OPEN THE DATA
# JOIN STUDENTS AND TRANSACTIONS
print("Build the data file...")
sr_file = "X003aa_Report_gl_export"
s_sql = "CREATE TABLE " + sr_file + " AS " + """
Select
    GL.*
From
    X000_GL_trans GL
Where
    Instr(Trim(GL.CALC_COST_STRING), '%COST%') > 0
;"""
# Instr('%COST%', Trim(GL.CALC_COST_STRING)) > 0
# 'PC.1A00709.1101zPC.3C00225.1101zPC.3C00226.1101zPC.3C00227.1101zPC.3C00228.1101'
# 'P.1A00176.1101zP.3P00041.1101zP.3P00042.1101zP.3P00045.1101zP.3P00048.1101'
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
s_sql = s_sql.replace("%COST%", s_cost)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: " + sr_file)

# EXPORT THE DATA
sx_file = "Kfs_report_gl_"
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
