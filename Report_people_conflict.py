"""
Script to extract EMPLOYEE CONFLICT OF INTEREST
Created on: 15 FEB 2018
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
funcfile.writelog("SCRIPT: Report_people_conflict")
ilog_severity = 1

# Declare variables
so_path = f"{funcconf.drive_data_raw}People_conflict/" #Source database path
so_file = "People_conflict.sqlite" #Source database
s_sql = "" #SQL statements

# Open the SOURCE file
with sqlite3.connect(so_path+so_file) as so_conn:
    so_curs = so_conn.cursor()
funcfile.writelog("OPEN DATABASE: " + so_file)

so_curs.execute(f"ATTACH DATABASE '{funcconf.drive_data_raw}People/People.sqlite' AS 'PEOPLE'")
funcfile.writelog("%t ATTACH DATABASE: People.sqlite")

# BUILD ALL DECLARATIONS AND LOOKUPS *******************************************

print("CONFLICT OF INTEREST")
print("--------------------")
print("Build the declarations...")

s_sql = """
CREATE TABLE X000_ALL_DECLARATIONS AS
SELECT
  XXNWU_COI_DECLARATIONS.DECLARATION_ID,
  XXNWU_COI_DECLARATIONS.EMPLOYEE_NUMBER,
  XXNWU_COI_DECLARATIONS.DECLARATION_DATE,
  XXNWU_COI_DECLARATIONS.UNDERSTAND_POLICY_FLAG,
  XXNWU_COI_DECLARATIONS.INTEREST_TO_DECLARE_FLAG,
  XXNWU_COI_DECLARATIONS.FULL_DISCLOSURE_FLAG,
  PEOPLE.HR_LOOKUPS.MEANING AS STATUS,
  XXNWU_COI_DECLARATIONS.LINE_MANAGER,
  XXNWU_COI_DECLARATIONS.REJECTION_REASON,
  XXNWU_COI_DECLARATIONS.CREATION_DATE,
  XXNWU_COI_DECLARATIONS.AUDIT_USER,
  XXNWU_COI_DECLARATIONS.LAST_UPDATE_DATE,
  XXNWU_COI_DECLARATIONS.LAST_UPDATED_BY,
  XXNWU_COI_DECLARATIONS.EXTERNAL_REFERENCE
FROM
  XXNWU_COI_DECLARATIONS
  LEFT JOIN PEOPLE.HR_LOOKUPS ON PEOPLE.HR_LOOKUPS.LOOKUP_CODE = XXNWU_COI_DECLARATIONS.STATUS_ID AND HR_LOOKUPS.LOOKUP_TYPE =
    "NWU_COI_STATUS"
WHERE
  PEOPLE.HR_LOOKUPS.START_DATE_ACTIVE <= XXNWU_COI_DECLARATIONS.DECLARATION_DATE AND
  PEOPLE.HR_LOOKUPS.END_DATE_ACTIVE >= XXNWU_COI_DECLARATIONS.DECLARATION_DATE
ORDER BY
  XXNWU_COI_DECLARATIONS.EMPLOYEE_NUMBER,
  XXNWU_COI_DECLARATIONS.DECLARATION_DATE
"""
so_curs.execute("DROP TABLE IF EXISTS X000_ALL_DECLARATIONS")
so_curs.execute(s_sql)

funcfile.writelog("%t BUILD TABLE: X000 All declarations")

# BUILD THE INTERESTS AND LOOKUPS **********************************************

print("Build the interests...")

s_sql = """
CREATE TABLE X000_ALL_INTERESTS AS
SELECT
  XXNWU_COI_INTERESTS.INTEREST_ID,
  XXNWU_COI_INTERESTS.DECLARATION_ID,
  XXNWU_COI_DECLARATIONS.EMPLOYEE_NUMBER,
  XXNWU_COI_DECLARATIONS.DECLARATION_DATE,
  XXNWU_COI_INTERESTS.CONFLICT_TYPE_ID,
  HR_LOOKUPS2.MEANING AS CONFLICT_TYPE,
  XXNWU_COI_INTERESTS.INTEREST_TYPE_ID,
  HR_LOOKUPS1.MEANING AS INTEREST_TYPE,
  XXNWU_COI_INTERESTS.STATUS_ID,
  HR_LOOKUPS3.MEANING AS INTEREST_STATUS,
  XXNWU_COI_INTERESTS.PERC_SHARE_INTEREST,
  XXNWU_COI_INTERESTS.ENTITY_NAME,
  XXNWU_COI_INTERESTS.ENTITY_REGISTRATION_NUMBER,
  XXNWU_COI_INTERESTS.OFFICE_ADDRESS,
  XXNWU_COI_INTERESTS.DESCRIPTION,
  XXNWU_COI_INTERESTS.DIR_APPOINTMENT_DATE,
  XXNWU_COI_INTERESTS.LINE_MANAGER,
  XXNWU_COI_INTERESTS.NEXT_LINE_MANAGER,
  XXNWU_COI_INTERESTS.INDUSTRY_CLASS_ID,
  PEOPLE.HR_LOOKUPS.MEANING AS INDUSTRY_TYPE,
  XXNWU_COI_INTERESTS.TASK_PERF_AGREEMENT,
  XXNWU_COI_INTERESTS.MITIGATION_AGREEMENT,
  XXNWU_COI_INTERESTS.REJECTION_REASON,
  XXNWU_COI_INTERESTS.CREATION_DATE,
  XXNWU_COI_INTERESTS.AUDIT_USER,
  XXNWU_COI_INTERESTS.LAST_UPDATE_DATE,
  XXNWU_COI_INTERESTS.LAST_UPDATED_BY,
  XXNWU_COI_INTERESTS.EXTERNAL_REFERENCE
FROM
  XXNWU_COI_INTERESTS
  LEFT JOIN XXNWU_COI_DECLARATIONS ON XXNWU_COI_DECLARATIONS.DECLARATION_ID = XXNWU_COI_INTERESTS.DECLARATION_ID
  LEFT JOIN PEOPLE.HR_LOOKUPS ON PEOPLE.HR_LOOKUPS.LOOKUP_CODE = XXNWU_COI_INTERESTS.INTEREST_TYPE_ID AND HR_LOOKUPS.LOOKUP_TYPE =
    "NWU_COI_INDUSTRY_CLASS"
  LEFT JOIN PEOPLE.HR_LOOKUPS HR_LOOKUPS1 ON HR_LOOKUPS1.LOOKUP_CODE = XXNWU_COI_INTERESTS.INTEREST_TYPE_ID AND
    HR_LOOKUPS1.LOOKUP_TYPE = "NWU_COI_INTEREST_TYPES"
  LEFT JOIN PEOPLE.HR_LOOKUPS HR_LOOKUPS2 ON HR_LOOKUPS2.LOOKUP_CODE = XXNWU_COI_INTERESTS.CONFLICT_TYPE_ID AND
    HR_LOOKUPS2.LOOKUP_TYPE = "NWU_COI_CONFLICT_TYPE"
  LEFT JOIN PEOPLE.HR_LOOKUPS HR_LOOKUPS3 ON HR_LOOKUPS3.LOOKUP_CODE = XXNWU_COI_INTERESTS.STATUS_ID AND
    HR_LOOKUPS3.LOOKUP_TYPE = "NWU_COI_STATUS"
"""
so_curs.execute("DROP TABLE IF EXISTS X000_ALL_INTERESTS")
so_curs.execute(s_sql)

funcfile.writelog("%t BUILD TABLE: X000 All Interests")

# BUILD THE PREVIOUS YEAR DECLARATIONS *****************************************

print("Select the previous year declarations...")

s_sql = "CREATE TABLE X001_DECLARATIONS_PREV AS" + """
SELECT
  X000_ALL_DECLARATIONS.DECLARATION_ID,
  X000_ALL_DECLARATIONS.EMPLOYEE_NUMBER,
  X000_ALL_DECLARATIONS.DECLARATION_DATE,
  X000_ALL_DECLARATIONS.UNDERSTAND_POLICY_FLAG,
  X000_ALL_DECLARATIONS.INTEREST_TO_DECLARE_FLAG,
  X000_ALL_DECLARATIONS.FULL_DISCLOSURE_FLAG,
  X000_ALL_DECLARATIONS.STATUS,
  X000_ALL_DECLARATIONS.LINE_MANAGER,
  X000_ALL_DECLARATIONS.REJECTION_REASON,
  X000_ALL_DECLARATIONS.CREATION_DATE,
  X000_ALL_DECLARATIONS.AUDIT_USER,
  X000_ALL_DECLARATIONS.LAST_UPDATE_DATE,
  X000_ALL_DECLARATIONS.LAST_UPDATED_BY,
  X000_ALL_DECLARATIONS.EXTERNAL_REFERENCE
FROM
  X000_ALL_DECLARATIONS
WHERE
  X000_ALL_DECLARATIONS.DECLARATION_DATE >= Date("%PYEARB%") AND
  X000_ALL_DECLARATIONS.DECLARATION_DATE <= Date("%PYEARE%")
"""
s_sql = s_sql.replace("%PYEARB%",funcdatn.get_previous_year_begin())
s_sql = s_sql.replace("%PYEARE%",funcdatn.get_previous_year_end())
so_curs.execute("DROP TABLE IF EXISTS X001_DECLARATIONS_PREV")
so_curs.execute(s_sql)

funcfile.writelog("%t BUILD TABLE: X001 Previous year declarations")

# Export the declaration data

sr_file = "X001_DECLARATIONS_PREV"
sr_filet = sr_file
sx_path = f"{funcconf.drive_data_results}People/" + funcdatn.get_previous_year() + "/"
sx_file = "Conflict_001_lst_declarations_"
sx_filet = sx_file + funcdatn.get_today_date_file() #Today


print("Export data..." + sx_path + sx_filet)

# Read the header data
s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)

# Write the data
funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_file, s_head)
funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_filet, s_head)

funcfile.writelog("%t EXPORT DATA: Previous year declarations")

# BUILD THE CURRENT YEAR DECLARATIONS ******************************************

print("Select the current year declarations...")

s_sql = "CREATE TABLE X001_DECLARATIONS_CURR AS" + """
SELECT
  X000_ALL_DECLARATIONS.DECLARATION_ID,
  X000_ALL_DECLARATIONS.EMPLOYEE_NUMBER,
  X000_ALL_DECLARATIONS.DECLARATION_DATE,
  X000_ALL_DECLARATIONS.UNDERSTAND_POLICY_FLAG,
  X000_ALL_DECLARATIONS.INTEREST_TO_DECLARE_FLAG,
  X000_ALL_DECLARATIONS.FULL_DISCLOSURE_FLAG,
  X000_ALL_DECLARATIONS.STATUS,
  X000_ALL_DECLARATIONS.LINE_MANAGER,
  X000_ALL_DECLARATIONS.REJECTION_REASON,
  X000_ALL_DECLARATIONS.CREATION_DATE,
  X000_ALL_DECLARATIONS.AUDIT_USER,
  X000_ALL_DECLARATIONS.LAST_UPDATE_DATE,
  X000_ALL_DECLARATIONS.LAST_UPDATED_BY,
  X000_ALL_DECLARATIONS.EXTERNAL_REFERENCE
FROM
  X000_ALL_DECLARATIONS
WHERE
  X000_ALL_DECLARATIONS.DECLARATION_DATE >= Date("%CYEARB%") AND
  X000_ALL_DECLARATIONS.DECLARATION_DATE <= Date("%CYEARE%")
"""
s_sql = s_sql.replace("%CYEARB%",funcdatn.get_current_year_begin())
s_sql = s_sql.replace("%CYEARE%",funcdatn.get_current_year_end())
so_curs.execute("DROP TABLE IF EXISTS X001_DECLARATIONS_CURR")
so_curs.execute(s_sql)

funcfile.writelog("%t BUILD TABLE: X001 Current year declarations")

# Export the declaration data

sr_file = "X001_DECLARATIONS_CURR"
sr_filet = sr_file
sx_path = f"{funcconf.drive_data_results}People/" + funcdatn.get_current_year() + "/"
sx_file = "Conflict_001_lst_declarations_"
sx_filet = sx_file + funcdatn.get_today_date_file() #Today

print("Export data..." + sx_path + sx_filet)

# Read the header data
s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)

# Write the data
funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_file, s_head)
funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_filet, s_head)

funcfile.writelog("%t EXPORT DATA: Current year declarations")

# BUILD THE PREVIOUS YEAR INTERESTS ********************************************

print("Select the previous year interests...")

s_sql = "CREATE TABLE X002_INTERESTS_PREV AS" + """
SELECT
  X000_all_interests.INTEREST_ID,
  X000_all_interests.DECLARATION_ID,
  X000_all_interests.EMPLOYEE_NUMBER,
  X000_all_interests.DECLARATION_DATE,
  X000_all_interests.CONFLICT_TYPE,
  X000_all_interests.INTEREST_TYPE,
  X000_all_interests.PERC_SHARE_INTEREST,
  X000_all_interests.INDUSTRY_TYPE,
  X000_all_interests.ENTITY_NAME,
  X000_all_interests.ENTITY_REGISTRATION_NUMBER,
  X000_all_interests.OFFICE_ADDRESS,
  X000_all_interests.DESCRIPTION,
  X000_all_interests.INTEREST_STATUS,
  X000_all_interests.LINE_MANAGER,
  X000_all_interests.NEXT_LINE_MANAGER,
  X000_all_interests.DIR_APPOINTMENT_DATE,
  X000_all_interests.TASK_PERF_AGREEMENT,
  X000_all_interests.MITIGATION_AGREEMENT,
  X000_all_interests.REJECTION_REASON,
  X000_all_interests.CREATION_DATE,
  X000_all_interests.AUDIT_USER,
  X000_all_interests.LAST_UPDATE_DATE,
  X000_all_interests.LAST_UPDATED_BY,
  X000_all_interests.EXTERNAL_REFERENCE
FROM
  X001_declarations_prev
  LEFT JOIN X000_all_interests ON X000_all_interests.DECLARATION_ID = X001_declarations_prev.DECLARATION_ID
WHERE
  X001_declarations_prev.INTEREST_TO_DECLARE_FLAG == "Y"
ORDER BY
  X000_all_interests.EMPLOYEE_NUMBER,
  X000_all_interests.LAST_UPDATE_DATE
"""
so_curs.execute("DROP TABLE IF EXISTS X002_INTERESTS_PREV")
so_curs.execute(s_sql)

funcfile.writelog("%t BUILD TABLE: X002 Previous interests")

# Export the current year interest data

sr_file = "X002_INTERESTS_PREV"
sr_filet = sr_file
sx_path = f"{funcconf.drive_data_results}People/" + funcdatn.get_previous_year() + "/"
sx_file = "Conflict_002_lst_interests_"
sx_filet = sx_file + funcdatn.get_today_date_file() #Today

print("Export data..." + sx_path + sx_filet)

# Read the header data
s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)

# Write the data
funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_file, s_head)
funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_filet, s_head)

funcfile.writelog("%t EXPORT DATA: Current year interests")

# BUILD THE CURRENT YEAR INTERESTS *********************************************

print("Select the current year interests...")

s_sql = "CREATE TABLE X002_INTERESTS_CURR AS" + """
SELECT
  X000_all_interests.INTEREST_ID,
  X000_all_interests.DECLARATION_ID,
  X000_all_interests.EMPLOYEE_NUMBER,
  X000_all_interests.DECLARATION_DATE,
  X000_all_interests.CONFLICT_TYPE,
  X000_all_interests.INTEREST_TYPE,
  X000_all_interests.PERC_SHARE_INTEREST,
  X000_all_interests.INDUSTRY_TYPE,
  X000_all_interests.ENTITY_NAME,
  X000_all_interests.ENTITY_REGISTRATION_NUMBER,
  X000_all_interests.OFFICE_ADDRESS,
  X000_all_interests.DESCRIPTION,
  X000_all_interests.INTEREST_STATUS,
  X000_all_interests.LINE_MANAGER,
  X000_all_interests.NEXT_LINE_MANAGER,
  X000_all_interests.DIR_APPOINTMENT_DATE,
  X000_all_interests.TASK_PERF_AGREEMENT,
  X000_all_interests.MITIGATION_AGREEMENT,
  X000_all_interests.REJECTION_REASON,
  X000_all_interests.CREATION_DATE,
  X000_all_interests.AUDIT_USER,
  X000_all_interests.LAST_UPDATE_DATE,
  X000_all_interests.LAST_UPDATED_BY,
  X000_all_interests.EXTERNAL_REFERENCE,
  PEOPLE.X002_PEOPLE_CURR.SUPERVISOR
FROM
  X001_declarations_curr
  LEFT JOIN X000_all_interests ON X000_all_interests.DECLARATION_ID = X001_declarations_curr.DECLARATION_ID
  LEFT JOIN PEOPLE.X002_PEOPLE_CURR ON PEOPLE.X002_PEOPLE_CURR.EMPLOYEE_NUMBER = X000_all_interests.EMPLOYEE_NUMBER
WHERE
  X001_declarations_curr.INTEREST_TO_DECLARE_FLAG == "Y"
ORDER BY
  X000_all_interests.EMPLOYEE_NUMBER,
  X000_all_interests.LAST_UPDATE_DATE
"""
so_curs.execute("DROP TABLE IF EXISTS X002_INTERESTS_CURR")
so_curs.execute(s_sql)

funcfile.writelog("%t BUILD TABLE: X002 Current interests")

# Export the current year interest data

sr_file = "X002_INTERESTS_CURR"
sr_filet = sr_file
sx_path = f"{funcconf.drive_data_results}People/" + funcdatn.get_current_year() + "/"
sx_file = "Conflict_002_lst_interests_"
sx_filet = sx_file + funcdatn.get_today_date_file() #Today

print("Export data..." + sx_path + sx_filet)

# Read the header data
s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)

# Write the data
funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_file, s_head)
funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_filet, s_head)

funcfile.writelog("%t EXPORT DATA: Current year interests")

# Close the table connection ***************************************************
so_conn.close()
