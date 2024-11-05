""" Script to extract RESIDENCES
Created on: 27 FEB 2018
Copyright: Albert J v Rensburg
"""

""" SCRIPT DESCRIPTION (and reulting table/view) *******************************
Can be run at any time depeninding on update status of dependancies listed
01 Build previous year residences (X001_Previous_residence)
02 Build current year residences (X001_Active_residence)
03 Build previous year rates (X002_Previous_rate)
04 Build current year rates (X002_Current_rate)
05 Build the previous year accomodation log (X003_Previous_accom_log)
06 Build the current year accomodation log (X003_Current_accom_log)
**************************************************************************** """

""" DEPENDANCIES ***************************************************************
CODEDESCRIPTION table (Vss.sqlite)
RESIDENCE table (Vss_residence.sqlite)
RESIDENCENAME table (Vss_residence.sqlite)
TRANSINST (Vss_residence.sqlite)
ACCOMMRESIDENCY (Vss_residence.sqlite)
**************************************************************************** """

# Import python modules
import sqlite3
import sys

# Add own module path
# sys.path.append('X:\\Python\\_my_modules')
from _my_modules import funcconf
from _my_modules import funcdatn
from _my_modules import funccsv
from _my_modules import funcfile

# Log
funcfile.writelog()
funcfile.writelog("Now")
funcfile.writelog("SCRIPT: Report_vss_residences")

# Declare variables
so_path = f"{funcconf.drive_data_raw}Vss_residence/" #Source database path
so_file = "Vss_residence.sqlite" #Source database
s_sql = "" #SQL statements

# Open the SOURCE file
with sqlite3.connect(so_path+so_file) as so_conn:
    so_curs = so_conn.cursor()
    
funcfile.writelog("OPEN DATABASE: " + so_file)

so_curs.execute(f"ATTACH DATABASE '{funcconf.drive_data_raw}Vss/Vss.sqlite' AS 'VSS'")
funcfile.writelog("%t ATTACH DATABASE: Vss.sqlite")

# BUILD CODE DESCRIPTIONS
print("Build code descriptions...")
s_sql = "CREATE TABLE X000_Codedescription AS " + """
SELECT
    CODE.KCODEDESCID,
    CODE.CODELONGDESCRIPTION AS LANK,
    CODE.CODESHORTDESCRIPTION AS KORT,
    LONG.CODELONGDESCRIPTION AS LONG,
    LONG.CODESHORTDESCRIPTION AS SHORT
FROM
    CODEDESCRIPTION CODE Inner Join
    CODEDESCRIPTION LONG ON LONG.KCODEDESCID = CODE.KCODEDESCID
WHERE
    CODE.KSYSTEMLANGUAGECODEID = 2 AND
    LONG.KSYSTEMLANGUAGECODEID = 3
;"""
so_curs.execute("DROP TABLE IF EXISTS X000_Codedescription")
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: X000_Codedescription")




print("RESIDENCE")
print("---------")

# 01 Extract the previous year residences and add lookups **********************

print("Extract the previous year residences...")

s_sql = "CREATE TABLE X001_Previous_residence AS " + """
SELECT
  RESIDENCE.KRESIDENCEID AS RESIDENCEID,
  RESIDENCENAME.NAME,
  X000_Codedescription.SHORT AS RESIDENCE_TYPE,
  RESIDENCE.FSITEORGUNITNUMBER AS CAMPUS,
  RESIDENCE.STARTDATE,
  RESIDENCE.ENDDATE,
  RESIDENCE.RESIDENCECAPACITY
FROM
  RESIDENCE
  LEFT JOIN RESIDENCENAME ON RESIDENCENAME.KRESIDENCEID = RESIDENCE.KRESIDENCEID
  LEFT JOIN X000_Codedescription ON X000_Codedescription.KCODEDESCID = RESIDENCE.FRESIDENCETYPECODEID
WHERE
  (RESIDENCENAME.KSYSTEMLANGUAGECODEID = 3 AND
  RESIDENCE.STARTDATE <= Date("%PYEARB%") AND
  RESIDENCE.ENDDATE >= Date("%PYEARE%")) OR
  (RESIDENCENAME.KSYSTEMLANGUAGECODEID = 3 AND
  RESIDENCE.ENDDATE >= Date("%PYEARB%") AND
  RESIDENCE.ENDDATE <= Date("%PYEARE%")) OR
  (RESIDENCENAME.KSYSTEMLANGUAGECODEID = 3 AND
  RESIDENCE.STARTDATE >= Date("%PYEARB%") AND
  RESIDENCE.STARTDATE <= Date("%PYEARE%"))
ORDER BY
  RESIDENCEID
"""
s_sql = s_sql.replace("%PYEARB%",funcdatn.get_previous_year_begin())
s_sql = s_sql.replace("%PYEARE%",funcdatn.get_previous_year_end())
so_curs.execute("DROP TABLE IF EXISTS X001_Previous_residence")
so_curs.execute(s_sql)
so_conn.commit()

funcfile.writelog("%t BUILD TABLE: X001_Previous_residence")
                  
# Data export
sr_file = "X001_Previous_residence"
sr_filet = sr_file
sx_path = f"{funcconf.drive_data_results}Debtorstud/" + funcdatn.get_previous_year() + "/"
sx_file = "Residence_001_residence_"
sx_filet = sx_file + funcdatn.get_previous_year()

print("Export previous residences..." + sx_path + sx_filet)

# Read the header data
s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)

# Write the data
funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_filet, s_head)

funcfile.writelog("%t EXPORT DATA: " + sx_path + sx_file)

# 02 Extract the current residences and add lookups ****************************

print("Extract the current year residences...")

s_sql = "CREATE TABLE X001_Active_residence AS " + """
SELECT
  RESIDENCE.KRESIDENCEID AS RESIDENCEID,
  RESIDENCENAME.NAME,
  X000_Codedescription.SHORT AS RESIDENCE_TYPE,
  RESIDENCE.FSITEORGUNITNUMBER AS CAMPUS,
  RESIDENCE.STARTDATE,
  RESIDENCE.ENDDATE,
  RESIDENCE.RESIDENCECAPACITY
FROM
  RESIDENCE
  LEFT JOIN RESIDENCENAME ON RESIDENCENAME.KRESIDENCEID = RESIDENCE.KRESIDENCEID
  LEFT JOIN X000_Codedescription ON X000_Codedescription.KCODEDESCID = RESIDENCE.FRESIDENCETYPECODEID
WHERE
  (RESIDENCENAME.KSYSTEMLANGUAGECODEID = 3 AND
  RESIDENCE.STARTDATE <= Date("%CYEARB%") AND
  RESIDENCE.ENDDATE >= Date("%CYEARE%")) OR
  (RESIDENCENAME.KSYSTEMLANGUAGECODEID = 3 AND
  RESIDENCE.ENDDATE >= Date("%CYEARB%") AND
  RESIDENCE.ENDDATE <= Date("%CYEARE%")) OR
  (RESIDENCENAME.KSYSTEMLANGUAGECODEID = 3 AND
  RESIDENCE.STARTDATE >= Date("%CYEARB%") AND
  RESIDENCE.STARTDATE <= Date("%CYEARE%"))
ORDER BY
  RESIDENCEID
"""
s_sql = s_sql.replace("%CYEARB%",funcdatn.get_current_year_begin())
s_sql = s_sql.replace("%CYEARE%",funcdatn.get_current_year_end())
so_curs.execute("DROP TABLE IF EXISTS X001_Active_residence")
so_curs.execute(s_sql)
so_conn.commit()

funcfile.writelog("%t BUILD TABLE: X001_Active_residence")

# Data export
sr_file = "X001_active_residence"
sr_filet = sr_file
sx_path = f"{funcconf.drive_data_results}Debtorstud/" + funcdatn.get_current_year() + "/"
sx_file = "Residence_001_residence_"
sx_filet = sx_file + funcdatn.get_today_date_file()

print("Export active residences..." + sx_path + sx_filet)

# Read the header data
s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)

# Write the data
funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_file, s_head)
funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_filet, s_head)

funcfile.writelog("%t EXPORT DATA: " + sx_path + sx_file)

# 03 Extract the previous year rates *******************************************

print("Extract the previous year residence rates...")

s_sql = "CREATE TABLE X002_Previous_rate AS " + """
SELECT
  TRANSINST.FRESIDENCEID,
  X001_Previous_residence.NAME,
  TRANSINST.FROOMTYPECODEID,
  X000_Codedescription.SHORT AS ROOM_TYPE,
  TRANSINST.STARTDATE,
  TRANSINST.ENDDATE,
  TRANSINST.AMOUNT,
  TRANSINST.DAILYRATE,
  TRANSINST.KTRANSINSTID
FROM
  TRANSINST
  LEFT JOIN X000_Codedescription ON X000_Codedescription.KCODEDESCID = TRANSINST.FROOMTYPECODEID
  INNER JOIN X001_Previous_residence ON X001_Previous_residence.RESIDENCEID = TRANSINST.FRESIDENCEID
WHERE
  (TRANSINST.STARTDATE >= Date("%PYEARB%") AND
  TRANSINST.STARTDATE <= Date("%PYEARE%")) OR
  (TRANSINST.ENDDATE >= Date("%PYEARB%") AND
  TRANSINST.ENDDATE <= Date("%PYEARE%")) OR
  (TRANSINST.STARTDATE <= Date("%PYEARB%") AND
  TRANSINST.ENDDATE >= Date("%PYEARE%"))
"""
s_sql = s_sql.replace("%PYEARB%",funcdatn.get_previous_year_begin())
s_sql = s_sql.replace("%PYEARE%",funcdatn.get_previous_year_end())
so_curs.execute("DROP TABLE IF EXISTS X002_Previous_rate")
so_curs.execute(s_sql)
so_conn.commit()

funcfile.writelog("%t BUILD TABLE: X002_Previous_rate")

# Data export
sr_file = "X002_Previous_rate"
sr_filet = sr_file
sx_path = f"{funcconf.drive_data_results}Debtorstud/" + funcdatn.get_previous_year() + "/"
sx_file = "Residence_002_rate_"
sx_filet = sx_file + funcdatn.get_previous_year()

print("Export data..." + sx_path + sx_filet)

# Read the header data
s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)

# Write the data
funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_filet, s_head)

funcfile.writelog("%t EXPORT DATA: " + sx_path + sx_file)

# 04 Extract the current year rates ********************************************

print("Extract the current year residence rates...")

s_sql = "CREATE TABLE X002_Current_rate AS " + """
SELECT
  TRANSINST.FRESIDENCEID,
  X001_Active_residence.NAME,
  TRANSINST.FROOMTYPECODEID,
  X000_Codedescription.SHORT AS ROOM_TYPE,
  TRANSINST.STARTDATE,
  TRANSINST.ENDDATE,
  TRANSINST.AMOUNT,
  TRANSINST.DAILYRATE,
  TRANSINST.KTRANSINSTID
FROM
  TRANSINST
  LEFT JOIN X000_Codedescription ON X000_Codedescription.KCODEDESCID = TRANSINST.FROOMTYPECODEID
  INNER JOIN X001_Active_residence ON X001_Active_residence.RESIDENCEID = TRANSINST.FRESIDENCEID
WHERE
  (TRANSINST.STARTDATE >= Date("%CYEARB%") AND
  TRANSINST.STARTDATE <= Date("%CYEARE%")) OR
  (TRANSINST.ENDDATE >= Date("%CYEARB%") AND
  TRANSINST.ENDDATE <= Date("%CYEARE%")) OR
  (TRANSINST.STARTDATE <= Date("%CYEARB%") AND
  TRANSINST.ENDDATE >= Date("%CYEARE%"))
"""
s_sql = s_sql.replace("%CYEARB%",funcdatn.get_current_year_begin())
s_sql = s_sql.replace("%CYEARE%",funcdatn.get_current_year_end())
so_curs.execute("DROP TABLE IF EXISTS X002_Current_rate")
so_curs.execute(s_sql)
so_conn.commit()

funcfile.writelog("%t BUILD TABLE: X002_Current_rate")

# Data export
sr_file = "X002_Current_rate"
sr_filet = sr_file
sx_path = f"{funcconf.drive_data_results}Debtorstud/" + funcdatn.get_current_year() + "/"
sx_file = "Residence_002_rate_"
sx_filet = sx_file + funcdatn.get_today_date_file()

print("Export data..." + sx_path + sx_filet)

# Read the header data
s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)

# Write the data
funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_file, s_head)
funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_filet, s_head)

funcfile.writelog("%t EXPORT DATA: " + sx_path + sx_file)

# 05 Extract the previous year accomodation log*********************************

print("Extract the current accommodation log...")
s_sql = "CREATE TABLE X003_Previous_accom_log AS" + """
SELECT
    ar.KACCOMMRESIDENCYID,
    ar.FSTUDENTBUSENTID AS STUDENT,
    ar.FRESIDENCEID AS RESIDENCEID,
    act_res.NAME AS RESIDENCE_NAME,
    ar.FROOMTYPECODEID,
    cd.SHORT AS ROOM_TYPE,
    ar.STARTDATE,
    ar.ENDDATE,
    ar.FACCOMMCANCELCODEID,
    cd1.LONG AS CANCELLATION_REASON,
    ar.ACCOMMCANCELREASONOTHER,
    ar.AUDITDATETIME
FROM
    ACCOMMRESIDENCY ar
INNER JOIN
    X000_Codedescription cd ON cd.KCODEDESCID = ar.FROOMTYPECODEID
LEFT JOIN
    X000_Codedescription cd1 ON cd1.KCODEDESCID = ar.FACCOMMCANCELCODEID
INNER JOIN
    X001_Previous_residence act_res ON act_res.RESIDENCEID = ar.FRESIDENCEID
WHERE
    ar.STARTDATE >= Date('%PYEARB%')
    AND ar.ENDDATE <= Date('%PYEARE%')
ORDER BY
    STUDENT, ar.AUDITDATETIME
;"""
s_sql = s_sql.replace("%PYEARB%",funcdatn.get_previous_year_begin())
s_sql = s_sql.replace("%PYEARE%",funcdatn.get_previous_year_end())
so_curs.execute("DROP TABLE IF EXISTS X003_Previous_accom_log")
so_curs.execute(s_sql)
so_conn.commit()

funcfile.writelog("%t BUILD TABLE: X003_Previous_accom_log")

# Data export
sr_file = "X003_Previous_accom_log"
sr_filet = sr_file
sx_path = f"{funcconf.drive_data_results}Debtorstud/" + funcdatn.get_previous_year() + "/"
sx_file = "Residence_003_log_"
sx_filet = sx_file + funcdatn.get_previous_year()

print("Export data..." + sx_path + sx_filet)

# Read the header data
s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)

# Write the data
funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_filet, s_head)

funcfile.writelog("%t EXPORT DATA: " + sx_path + sx_file)

# 06 Extract the current year accomodation log**********************************

print("Extract the current accommodation log...")
s_sql = "CREATE TABLE X003_Current_accom_log AS" + """
SELECT
    ar.KACCOMMRESIDENCYID,
    ar.FSTUDENTBUSENTID AS STUDENT,
    ar.FRESIDENCEID AS RESIDENCEID,
    act_res.NAME AS RESIDENCE_NAME,
    ar.FROOMTYPECODEID,
    cd.SHORT AS ROOM_TYPE,
    ar.STARTDATE,
    ar.ENDDATE,
    ar.FACCOMMCANCELCODEID,
    cd1.LONG AS CANCELLATION_REASON,
    ar.ACCOMMCANCELREASONOTHER,
    ar.AUDITDATETIME
FROM
    ACCOMMRESIDENCY ar
INNER JOIN
    X000_Codedescription cd ON cd.KCODEDESCID = ar.FROOMTYPECODEID
LEFT JOIN
    X000_Codedescription cd1 ON cd1.KCODEDESCID = ar.FACCOMMCANCELCODEID
INNER JOIN
    X001_Active_residence act_res ON act_res.RESIDENCEID = ar.FRESIDENCEID
WHERE
    ar.STARTDATE >= Date('%CYEARB%')
    AND ar.ENDDATE <= Date('%CYEARE%')
ORDER BY
    STUDENT, ar.AUDITDATETIME
;"""

s_sql = s_sql.replace("%CYEARB%",funcdatn.get_current_year_begin())
s_sql = s_sql.replace("%CYEARE%",funcdatn.get_current_year_end())
so_curs.execute("DROP TABLE IF EXISTS X003_Current_accom_log")
so_curs.execute(s_sql)
so_conn.commit()

funcfile.writelog("%t BUILD TABLE: X003_Current_accom_log")

# Data export
sr_file = "X003_Current_accom_log"
sr_filet = sr_file
sx_path = f"{funcconf.drive_data_results}Debtorstud/" + funcdatn.get_current_year() + "/"
sx_file = "Residence_003_log_"
sx_filet = sx_file + funcdatn.get_today_date_file()

print("Export data..." + sx_path + sx_filet)

# Read the header data
s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)

# Write the data
funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_file, s_head)
funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_filet, s_head)

funcfile.writelog("%t EXPORT DATA: " + sx_path + sx_file)

# Close the table connection ***************************************************
so_conn.close()
funcfile.writelog("COMPLETED")
