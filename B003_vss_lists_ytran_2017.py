# Import python modules
import csv
import datetime
import sqlite3
import sys

# Add own module path
sys.path.append('../_my_modules')
#print(sys.path)

# Import own modules
import funcconf
import funcdatn
import funccsv
import funcfile

# Open the script log file ******************************************************

funcfile.writelog("Now")
funcfile.writelog("SCRIPT: B003_VSS_LISTS_DEV")
funcfile.writelog("--------------------------")
print("--------------")
print("B003_VSS_LISTS")
print("--------------")
ilog_severity = 1

# Declare variables
so_path = f"{funcconf.drive_data_raw}Vss/" #Source database path
re_path = f"{funcconf.drive_data_results}Vss/" #Results
so_file = "Vss.sqlite" #Source database
ed_path = f"{funcconf.drive_system}_external_data/"
s_sql = "" #SQL statements

# Open the SOURCE file
with sqlite3.connect(so_path+so_file) as so_conn:
    so_curs = so_conn.cursor()

funcfile.writelog("OPEN DATABASE: " + so_file)

"""*****************************************************************************
BEGIN
*****************************************************************************"""

# BUILD PERIOD TRANSACTIONS ******************************************
print("Build period transactions...")
sr_file = "X010_Studytrans_2017"
s_sql = "CREATE TABLE " + sr_file + " AS " + """
SELECT
  STUDYTRANS_2017.KACCTRANSID,
  STUDYTRANS_2017.FACCID,
  STUDACC.FBUSENTID,
  STUDYTRANS_2017.FSERVICESITE,
  STUDYTRANS_2017.FDEBTCOLLECTIONSITE,
  STUDYTRANS_2017.TRANSDATE,
  STUDYTRANS_2017.AMOUNT,
  STUDYTRANS_2017.FTRANSMASTERID,
  X000_Transmaster.TRANSCODE,
  X000_Transmaster.DESCRIPTION_E,
  X000_Transmaster.DESCRIPTION_A,
  STUDYTRANS_2017.TRANSDATETIME,
  STUDYTRANS_2017.MONTHENDDATE,
  STUDYTRANS_2017.POSTDATEDTRANSDATE,
  STUDYTRANS_2017.FFINAIDSITEID,
  X004_Bursaries.FINAIDCODE,
  X004_Bursaries.FINAIDNAAM,
  STUDYTRANS_2017.FRESIDENCELOGID,
  STUDYTRANS_2017.FLEVYLOGID,
  STUDYTRANS_2017.FMODAPID,
  STUDYTRANS_2017.FQUALLEVELAPID,
  STUDYTRANS_2017.FPROGAPID,
  STUDYTRANS_2017.FENROLPRESID,
  STUDYTRANS_2017.FRESIDENCEID,
  STUDYTRANS_2017.FRECEIPTID,
  STUDYTRANS_2017.FROOMTYPECODEID,
  STUDYTRANS_2017.REFERENCENO,
  STUDYTRANS_2017.FSUBACCTYPECODEID,
  STUDYTRANS_2017.FDEPOSITCODEID,
  STUDYTRANS_2017.FDEPOSITTYPECODEID,
  STUDYTRANS_2017.FVARIABLEAMOUNTTYPECODEID,
  STUDYTRANS_2017.FDEPOSITTRANSTYPECODEID,
  STUDYTRANS_2017.RESIDENCETRANSTYPE,
  STUDYTRANS_2017.FSTUDYTRANSTYPECODEID,
  STUDYTRANS_2017.ISSHOWN,
  STUDYTRANS_2017.ISCREATEDMANUALLY,
  STUDYTRANS_2017.FTRANSINSTID,
  STUDYTRANS_2017.FMONTHENDORGUNITNO,
  STUDYTRANS_2017.LOCKSTAMP,
  STUDYTRANS_2017.AUDITDATETIME,
  STUDYTRANS_2017.FAUDITSYSTEMFUNCTIONID,
  STUDYTRANS_2017.FAUDITUSERCODE,
  SYSTEMUSER.FUSERBUSINESSENTITYID,
  STUDYTRANS_2017.FORIGINSYSTEMFUNCTIONID,
  STUDYTRANS_2017.FPAYMENTREQUESTID
FROM
  STUDYTRANS_2017
  LEFT JOIN STUDACC ON STUDACC.KACCID = STUDYTRANS_2017.FACCID
  LEFT JOIN X000_Transmaster ON X000_Transmaster.KTRANSMASTERID = STUDYTRANS_2017.FTRANSMASTERID
  LEFT JOIN X004_Bursaries ON X004_Bursaries.KFINAIDSITEID = STUDYTRANS_2017.FFINAIDSITEID
  LEFT JOIN SYSTEMUSER ON SYSTEMUSER.KUSERCODE = STUDYTRANS_2017.FAUDITUSERCODE
ORDER BY
  STUDYTRANS_2017.TRANSDATETIME
"""
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD VIEW: " + sr_file) 

"""*****************************************************************************
BEGIN
*****************************************************************************"""

# Close the connection *********************************************************
so_conn.close()

# Close the log writer *********************************************************
funcfile.writelog("-----------------------------")
funcfile.writelog("COMPLETED: B003_VSS_LISTS_DEV")
