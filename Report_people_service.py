""" Script to extract list of employees for certain period
Created on: 11 May 2018
Author: Albert J v Rensburg (21162395)
"""

# Import python modules
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
import funcpeople

# Open the script log file ******************************************************

funcfile.writelog("Now")
funcfile.writelog("SCRIPT: REPORT_PEOPLE_SERVICE")
funcfile.writelog("-----------------------------")
print("PEOPLE SERVICE")
print("--------------")
ilog_severity = 1

# Declare variables
so_path = f"{funcconf.drive_data_raw}People/" #Source database path
so_file = "People.sqlite" #Source database
s_sql = "" #SQL statements
s_export = "True"
sd_acti = "Y"

# Open the SOURCE file
with sqlite3.connect(so_path+so_file) as so_conn:
    so_curs = so_conn.cursor()

funcfile.writelog("OPEN DATABASE: " + so_file)

# Ask the questions

print("")
s_fr = funcdatn.get_previous_month_begin()
s_fr = input("From which date? (yyyy-mm-dd) ")
s_to = funcdatn.get_previous_month_end()
s_to = input("  To which date? (yyyy-mm-dd) ")
s_on = funcdatn.get_previous_month_end()
s_on = input("  On which date? (yyyy-mm-dd) ")
# Input active or not
print("")
print("Default:"+sd_acti)
s_acti = input("Only include active people? ")
if s_acti == "":
    s_acti = sd_acti
print("")

# 12 Build current assignment round 1 ******************************************

funcpeople.assign01(so_conn,"X001_ASSIGNMENT_PERI_01",s_fr,s_to,s_on,"Build period assignments 1...")

# 13 Build current assignment round 2 ******************************************

funcpeople.assign02(so_conn,"X001_ASSIGNMENT_PERI","X001_ASSIGNMENT_PERI_01","Build period assignments 2...")

if s_export == "True":

    # Data export
    sr_file = "X001_ASSIGNMENT_PERI"
    sr_filet = sr_file
    sx_path = f"{funcconf.drive_data_results}People/" + funcdatn.get_current_year() + "/"
    sx_file = "Assignment_001_period_"
    sx_filet = sx_file + s_fr + "_" + s_to

    print("Export period assignments..." + sx_path + sx_filet)

    # Read the header data
    s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)

    # Write the data
    funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_filet, s_head)

    funcfile.writelog("%t EXPORT DATA: " + sx_path + sx_file)

# 17 Build PHONE MOBILE PERI ***************************************************

print("Build period mobile phones...")

s_sql = "CREATE VIEW X000_PHONE_MOBI_PERI AS " + """
SELECT
  X001_ASSIGNMENT_PERI.PERSON_ID,
  X001_ASSIGNMENT_PERI.DATE_EMP_LOOKUP,
  PER_PHONES.PHONE_ID,
  PER_PHONES.DATE_FROM,
  PER_PHONES.DATE_TO,
  PER_PHONES.PHONE_TYPE,
  PER_PHONES.PHONE_NUMBER AS PHONE_MOBI
FROM
  PER_PHONES
  INNER JOIN X001_ASSIGNMENT_PERI ON PER_PHONES.PARENT_ID = X001_ASSIGNMENT_PERI.PERSON_ID AND PER_PHONES.DATE_FROM <=
    X001_ASSIGNMENT_PERI.DATE_EMP_LOOKUP AND PER_PHONES.DATE_TO >= X001_ASSIGNMENT_PERI.DATE_EMP_LOOKUP
WHERE    
     PER_PHONES.PHONE_TYPE = 'M'
ORDER BY
  X001_ASSIGNMENT_PERI.PERSON_ID,
  PER_PHONES.DATE_FROM
"""
so_curs.execute("DROP VIEW IF EXISTS X000_PHONE_MOBI_PERI")
so_curs.execute(s_sql)
so_conn.commit()

funcfile.writelog("%t BUILD VIEW: X000_PHONE_MOBI_PERI")

# 18 Build PHONE MOBILE PERI LIST **********************************************

print("Build period mobile phone list...")

s_sql = "CREATE VIEW X000_PHONE_MOBI_PERI_LIST AS " + """
SELECT
  X000_PHONE_MOBI_PERI.PERSON_ID,
  X000_PHONE_MOBI_PERI.DATE_EMP_LOOKUP,
  X000_PHONE_MOBI_PERI.PHONE_ID,
  X000_PHONE_MOBI_PERI.DATE_FROM,
  X000_PHONE_MOBI_PERI.DATE_TO,
  X000_PHONE_MOBI_PERI.PHONE_TYPE,
  X000_PHONE_MOBI_PERI.PHONE_MOBI
FROM
  X000_PHONE_MOBI_PERI
GROUP BY
  X000_PHONE_MOBI_PERI.PERSON_ID
"""
so_curs.execute("DROP VIEW IF EXISTS X000_PHONE_MOBI_PERI_LIST")
so_curs.execute(s_sql)
so_conn.commit()

funcfile.writelog("%t BUILD VIEW: X000_PHONE_MOBI_PERI_LIST")

# 19 Build PHONE WORK PERI ***************************************************

print("Build period work phones...")

s_sql = "CREATE VIEW X000_PHONE_WORK_PERI AS " + """
SELECT
  X001_ASSIGNMENT_PERI.PERSON_ID,
  X001_ASSIGNMENT_PERI.DATE_EMP_LOOKUP,
  PER_PHONES.PHONE_ID,
  PER_PHONES.DATE_FROM,
  PER_PHONES.DATE_TO,
  PER_PHONES.PHONE_TYPE,
  PER_PHONES.PHONE_NUMBER AS PHONE_WORK
FROM
  PER_PHONES
  INNER JOIN X001_ASSIGNMENT_PERI ON PER_PHONES.PARENT_ID = X001_ASSIGNMENT_PERI.PERSON_ID AND PER_PHONES.DATE_FROM <=
    X001_ASSIGNMENT_PERI.DATE_EMP_LOOKUP AND PER_PHONES.DATE_TO >= X001_ASSIGNMENT_PERI.DATE_EMP_LOOKUP
WHERE    
     PER_PHONES.PHONE_TYPE = 'W1'
ORDER BY
  X001_ASSIGNMENT_PERI.PERSON_ID,
  PER_PHONES.DATE_FROM
"""
so_curs.execute("DROP VIEW IF EXISTS X000_PHONE_WORK_PERI")
so_curs.execute(s_sql)
so_conn.commit()

funcfile.writelog("%t BUILD VIEW: X000_PHONE_WORK_PERI")

# 20 Build PHONE WORK PERI LIST **********************************************

print("Build period work phone list...")

s_sql = "CREATE VIEW X000_PHONE_WORK_PERI_LIST AS " + """
SELECT
  X000_PHONE_WORK_PERI.PERSON_ID,
  X000_PHONE_WORK_PERI.DATE_EMP_LOOKUP,
  X000_PHONE_WORK_PERI.PHONE_ID,
  X000_PHONE_WORK_PERI.DATE_FROM,
  X000_PHONE_WORK_PERI.DATE_TO,
  X000_PHONE_WORK_PERI.PHONE_TYPE,
  X000_PHONE_WORK_PERI.PHONE_WORK
FROM
  X000_PHONE_WORK_PERI
GROUP BY
  X000_PHONE_WORK_PERI.PERSON_ID
"""
so_curs.execute("DROP VIEW IF EXISTS X000_PHONE_WORK_PERI_LIST")
so_curs.execute(s_sql)
so_conn.commit()

funcfile.writelog("%t BUILD VIEW: X000_PHONE_WORK_PERI_LIST")

# 21 Build PHONE HOME PERI ***************************************************

print("Build period home phones...")

s_sql = "CREATE VIEW X000_PHONE_HOME_PERI AS " + """
SELECT
  X001_ASSIGNMENT_PERI.PERSON_ID,
  X001_ASSIGNMENT_PERI.DATE_EMP_LOOKUP,
  PER_PHONES.PHONE_ID,
  PER_PHONES.DATE_FROM,
  PER_PHONES.DATE_TO,
  PER_PHONES.PHONE_TYPE,
  PER_PHONES.PHONE_NUMBER AS PHONE_HOME
FROM
  PER_PHONES
  INNER JOIN X001_ASSIGNMENT_PERI ON PER_PHONES.PARENT_ID = X001_ASSIGNMENT_PERI.PERSON_ID AND PER_PHONES.DATE_FROM <=
    X001_ASSIGNMENT_PERI.DATE_EMP_LOOKUP AND PER_PHONES.DATE_TO >= X001_ASSIGNMENT_PERI.DATE_EMP_LOOKUP
WHERE    
     PER_PHONES.PHONE_TYPE = 'H1'
ORDER BY
  X001_ASSIGNMENT_PERI.PERSON_ID,
  PER_PHONES.DATE_FROM
"""
so_curs.execute("DROP VIEW IF EXISTS X000_PHONE_HOME_PERI")
so_curs.execute(s_sql)
so_conn.commit()

funcfile.writelog("%t BUILD VIEW: X000_PHONE_HOME_PERI")

# 22 Build PHONE HOME PERI LIST **********************************************

print("Build period home phone list...")

s_sql = "CREATE VIEW X000_PHONE_HOME_PERI_LIST AS " + """
SELECT
  X000_PHONE_HOME_PERI.PERSON_ID,
  X000_PHONE_HOME_PERI.DATE_EMP_LOOKUP,
  X000_PHONE_HOME_PERI.PHONE_ID,
  X000_PHONE_HOME_PERI.DATE_FROM,
  X000_PHONE_HOME_PERI.DATE_TO,
  X000_PHONE_HOME_PERI.PHONE_TYPE,
  X000_PHONE_HOME_PERI.PHONE_HOME
FROM
  X000_PHONE_HOME_PERI
GROUP BY
  X000_PHONE_HOME_PERI.PERSON_ID
"""
so_curs.execute("DROP VIEW IF EXISTS X000_PHONE_HOME_PERI_LIST")
so_curs.execute(s_sql)
so_conn.commit()

funcfile.writelog("%t BUILD VIEW: X000_PHONE_HOME_PERI_LIST")

# 16 Build PEOPLE CURRENT ******************************************************

funcpeople.people01(so_conn,"X002_PEOPLE_PERI","X001_ASSIGNMENT_PERI","PERI","Build period people...",s_acti)

if s_export == "True":
    
    # Data export
    sr_file = "X002_PEOPLE_PERI"
    sr_filet = sr_file
    sx_path = f"{funcconf.drive_data_results}People/" + funcdatn.get_current_year() + "/"
    sx_file = "People_002_period_"
    sx_filet = sx_file + s_on

    print("Export period people..." + sx_path + sx_filet)

    # Read the header data
    s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)

    # Write the data
    funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_filet, s_head)

    funcfile.writelog("%t EXPORT DATA: " + sx_path + sx_file)

# Delete some unncessary files *************************************************

so_curs.execute("DROP VIEW IF EXISTS X000_PHONE_HOME_PERI")
so_curs.execute("DROP VIEW IF EXISTS X000_PHONE_HOME_PERI_LIST")
so_curs.execute("DROP VIEW IF EXISTS X000_PHONE_MOBI_PERI")
so_curs.execute("DROP VIEW IF EXISTS X000_PHONE_MOBI_PERI_LIST")
so_curs.execute("DROP VIEW IF EXISTS X000_PHONE_WORK_PERI")
so_curs.execute("DROP VIEW IF EXISTS X000_PHONE_WORK_PERI_LIST")    

# Close the connection *********************************************************
so_conn.close()

# Close the log writer *********************************************************
funcfile.writelog("COMPLETED")
