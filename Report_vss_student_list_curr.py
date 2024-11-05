# Import python modules
import sqlite3
import sys

# Import own modules
from _my_modules import funcconf
from _my_modules import funcdatn
from _my_modules import funccsv
from _my_modules import funcfile

# Open the script log file ******************************************************
funcfile.writelog("Now")
funcfile.writelog("SCRIPT: REPORT VSS STUDENT LIST")
funcfile.writelog("-------------------------------")
print("-------------------")
print("REPORT STUDENT LIST")
print("-------------------")
ilog_severity = 1

# Declare variables
so_path = f"{funcconf.drive_data_raw}Vss/" #Source database path
re_path = f"{funcconf.drive_data_results}Vss/" #Results
so_file = "Vss_curr.sqlite" #Source database
ed_path = f"{funcconf.drive_system}_external_data/"
s_sql = "" #SQL statements

# Open the SOURCE file
with sqlite3.connect(so_path+so_file) as so_conn:
    so_curs = so_conn.cursor()
funcfile.writelog("OPEN DATABASE: " + so_file)

# Build test code **************************************************************

# Ask student list on which day ************************************************
print("")
s_date = input("Students on which day? (yyyy-mm-dd) ")
print("")

funcfile.writelog("STUDENT LIST ON " + s_date)

# Build current student qualification results **********************************
print("Build list of current students on specific day...")
sr_file = "X001_Student_selected"
s_sql = "CREATE TABLE "+ sr_file +" AS " + """
SELECT
  STUDENT.*
FROM
  X001_Student STUDENT
WHERE
  (STUDENT.DATEENROL <= Date('%DAY%') AND
  STUDENT.DISCONTINUEDATE >= Date('%DAY%') AND
  Upper(STUDENT.QUAL_TYPE) != 'SHORT COURSE') OR
  (STUDENT.DATEENROL <= Date('%DAY%') AND
  STUDENT.DISCONTINUEDATE IS NULL AND
  Upper(STUDENT.QUAL_TYPE) != 'SHORT COURSE')
"""
s_sql = s_sql.replace("%DAY%",s_date)
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
so_curs.execute(s_sql)
so_conn.commit()

funcfile.writelog("%t BUILD TABLE: " + sr_file)

# Export the data
print("Export list of students on specific date...")
sr_filet = sr_file
sx_path = re_path + funcdatn.get_current_year() + "/"
sx_file = "Student_001_list_" + s_date.replace("-","") + "_"
s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)
funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_file, s_head)
funcfile.writelog("%t EXPORT DATA: "+sx_path+sx_file)

# Close the connection *********************************************************
so_conn.close()

# Close the log writer *********************************************************
funcfile.writelog("------------------------------")
funcfile.writelog("COMPLETED: REPORT STUDENT LIST")
