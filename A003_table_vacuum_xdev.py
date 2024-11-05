"""
Script to vacuum (clear) test finding export files
by Albert J v Rensburg (NWU21162395) on 16 Mar 2020
"""

# IMPORT PYTHON MODULES
import csv
import sqlite3

# IMPORT OWN MODULES
from _my_modules import funcfile
from _my_modules import funcsms

""" INDEX **********************************************************************
ENVIRONMENT
BEGIN OF SCRIPT
END OF SCRIPT
*****************************************************************************"""

"""*****************************************************************************
ENVIRONMENT
*****************************************************************************"""

# DECLARE VARIABLES
l_mess: bool = True
i_counter: int = 0

# SCRIPT LOG FILE
funcfile.writelog("Now")
funcfile.writelog("SCRIPT: A003_TABLE_VACUUM")
funcfile.writelog("-------------------------")
print("-----------------")
print("A003_TABLE_VACUUM")
print("-----------------")

# SEND MESSAGE
if l_mess:
    funcsms.send_telegram("", "administrator", "<b>Vacuming</b> the test finding database tables.")

"""*****************************************************************************
BEGIN OF SCRIPT
*****************************************************************************"""
print("BEGIN OF SCRIPT")
funcfile.writelog("BEGIN OF SCRIPT")

# READ THE TEXT FILE
co = open("000d_Table_vacuum.csv", newline=None)
co_reader = csv.reader(co)

# READ THE CONTACTS TABLE ROW BY ROW
for row in co_reader:

    # POPULATE THE COLUMN VARIABLES
    # COLUMNS: ACTIVE[0], PATH[1], DATABASE[2], TABLE[3], DESCRIPTION[4]
    if row[0] == "ACTIVE":
        continue
    elif row[0] == "X":
        continue
    else:
        print("Vacuum: " + row[1]+row[2]+"/"+row[3])

    # OPEN THE DATABASE
    with sqlite3.connect(row[1] + row[2]) as so_conn:
        so_curs = so_conn.cursor()
    funcfile.writelog("OPEN DATABASE: " + row[2])

    # CREATE BLANK TABLE
    s_sql = "CREATE TABLE " + row[3] + " (BLANK TEXT);"
    so_curs.execute("DROP TABLE IF EXISTS " + row[3])
    so_curs.execute(s_sql)

    # CLOSE THE DATABASE
    so_conn.commit()
    so_conn.close()

    # UPDATE THE VARIABLES
    i_counter += 1

    funcfile.writelog("%t VACUUM TABLE: " + row[3])

# CLOSE THE TEXT FILE
co.close()

"""*****************************************************************************
END OF SCRIPT
*****************************************************************************"""
print("END OF SCRIPT")
funcfile.writelog("END OF SCRIPT")

# SEND MESSAGE
if l_mess:
    funcsms.send_telegram("", "administrator", "<b>" + str(i_counter) + "</b> Tables vacuumed.")

# CLOSE THE LOG WRITER
funcfile.writelog("----------------------------")
funcfile.writelog("COMPLETED: A003_TABLE_VACUUM")
