"""
Script to build standard VSS lists
Created on: 01 Mar 2018
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

# Open the script log file ******************************************************

funcfile.writelog()
funcfile.writelog("Now")
funcfile.writelog("SCRIPT: Report_people_leave_per_person")
ilog_severity = 1

# Declare variables
so_path = f"{funcconf.drive_data_raw}People_leave/" #Source database path
so_file = "People_leave.sqlite" #Source database
s_sql = "" #SQL statements

# Open the SOURCE file
with sqlite3.connect(so_path+so_file) as so_conn:
    so_curs = so_conn.cursor()

funcfile.writelog("OPEN DATABASE: " + so_file)

so_curs.execute(f"ATTACH DATABASE '{funcconf.drive_data_raw}People/People.sqlite' AS 'PEOPLE'")    
funcfile.writelog("%t ATTACH DATABASE: People.sqlite")

print("PEOPLE LEAVE PER PERSON")
print("-----------------------")

print("")
s_em = input("Employee? ")
print("")

# Build CURRENT ABSENCE ATTENDANCES ********************************************

print("Build employee absence attendances...")

s_sql = "CREATE TABLE X104_PER_ABSENCE_ATTENDANCES_EMPL AS " + """
SELECT
  aa.EMPLOYEE_NUMBER,
  aa.PERSON_ID,
  aa.ABSENCE_ATTENDANCE_ID,
  aa.BUSINESS_GROUP_ID,
  aa.DATE_NOTIFICATION,
  aa.DATE_START,
  aa.DATE_END,
  aa.ABSENCE_DAYS,
  aa.ABSENCE_ATTENDANCE_TYPE_ID,
  at.NAME AS LEAVE_TYPE,
  aa.ABS_ATTENDANCE_REASON_ID,
  ar.NAME AS LEAVE_REASON,
  ar.MEANING AS REASON_DESCRIP,
  aa.AUTHORISING_PERSON_ID,
  pe.EMPLOYEE_NUMBER AS EMPLOYEE_AUTHORISE,
  aa.ABSENCE_HOURS,
  aa.OCCURRENCE,
  aa.SSP1_ISSUED,
  aa.PROGRAM_APPLICATION_ID,
  aa.ATTRIBUTE1,
  aa.ATTRIBUTE2,
  aa.ATTRIBUTE3,
  aa.ATTRIBUTE4,
  aa.ATTRIBUTE5,
  aa.LAST_UPDATE_DATE,
  aa.LAST_UPDATED_BY,
  aa.LAST_UPDATE_LOGIN,
  aa.CREATED_BY,
  aa.CREATION_DATE,
  aa.REASON_FOR_NOTIFICATION_DELAY,
  aa.ACCEPT_LATE_NOTIFICATION_FLAG,
  aa.OBJECT_VERSION_NUMBER,
  at.INPUT_VALUE_ID,
  at.ABSENCE_CATEGORY,
  at.MEANING AS TYPE_DESCRIP
FROM
  X100_Per_absence_attendances aa LEFT JOIN
  X102_Per_absence_attendance_types at ON at.ABSENCE_ATTENDANCE_TYPE_ID = aa.ABSENCE_ATTENDANCE_TYPE_ID LEFT JOIN
  X101_Per_abs_attendance_reasons ar ON ar.ABS_ATTENDANCE_REASON_ID = aa.ABS_ATTENDANCE_REASON_ID LEFT JOIN
  PEOPLE.PER_ALL_PEOPLE_F pe ON pe.PERSON_ID = aa.AUTHORISING_PERSON_ID AND
    pe.EFFECTIVE_START_DATE <= aa.DATE_START AND pe.EFFECTIVE_END_DATE >= aa.DATE_START
;"""

if s_em != "":
   s_sql += """
WHERE
  (aa.EMPLOYEE_NUMBER = '""" + s_em + "')"

s_sql += """
ORDER BY
  aa.EMPLOYEE_NUMBER,
  aa.DATE_START,
  aa.DATE_END
"""

# print(s_sql)

"""
WHERE
  (aa.DATE_START >= Date("%CYEARB%") AND
  aa.DATE_END <= Date("%CYEARE%")) OR
  (aa.DATE_START >= Date("%CYEARB%") AND
  aa.DATE_START <= Date("%CYEARE%")) OR
  (aa.DATE_END >= Date("%CYEARB%") AND
  aa.DATE_END <= Date("%CYEARE%"))
"""

so_curs.execute("DROP TABLE IF EXISTS X104_PER_ABSENCE_ATTENDANCES_EMPL")
so_curs.execute(s_sql)
so_conn.commit()

funcfile.writelog("%t BUILD TABLE: X104_Per_absence_attendances_empl")

# Export the declaration data

sr_file = "X104_Per_absence_attendances_empl"
sr_filet = sr_file
sx_path = f"{funcconf.drive_data_results}People/" + funcdatn.get_current_year() + "/"
sx_file = "Leave_104_lst_transact_" + s_em + "_"
sx_filet = sx_file + funcdatn.get_today_date_file() #Today

print("Export data..." + sx_path + sx_filet)

# Read the header data
s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)

# Write the data
funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_filet, s_head)

funcfile.writelog("%t EXPORT DATA: Employee leave transactions")

# Close the connection *********************************************************
so_conn.close()

# Close the log writer *********************************************************
funcfile.writelog("COMPLETED")
