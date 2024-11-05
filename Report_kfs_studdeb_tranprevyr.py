"""
Script to build GL Student debtor control account reports
Created on: 13 Mar 2018
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

# Open the script log file ******************************************************

sl_path = "X:/Python/"
fl_type = 1
fl = open(sl_path + "Logs/" + "Python_log_" + datetime.datetime.now().strftime("%Y%m%d") + ".txt", "a")
fl.write("\r\n")
fl.write("----------------"+"\r\n")
fl.write(datetime.datetime.now().strftime("%Y-%m-%d %H:%M")+"\r\n")
fl.write("SCRIPT: Report_kfs_studdeb_tranprevyr"+"\r\n")

# Declare variables
so_path = f"{funcconf.drive_data_raw}" #Source database path
so_file = "Kfs.sqlite" #Source database
s_sql = "" #SQL statements

# Open the SOURCE file
with sqlite3.connect(so_path+so_file) as so_conn:
    so_curs = so_conn.cursor()

fl.write("\r\n")
fl.write(datetime.datetime.now().strftime("%H:%M:%S") + " OPEN DATABASE: Kfs\r\n")

print("KFS STUDENT DEBTOR CONTROL ACC TRANSACTIONS")
print("-------------------------------------------")

# Extract transactions *********************************************************

print("Extract transactions...")

s_sql = "CREATE TABLE X010_Studdeb_tranlist_prevyr AS " + """
SELECT
  *
FROM
  X000_GL_trans_prev
WHERE
  (X000_GL_trans_prev.FIN_OBJECT_CD = '7551') OR
  (X000_GL_trans_prev.FIN_OBJECT_CD = '7552') OR
  (X000_GL_trans_prev.FIN_OBJECT_CD = '7553')
;"""
so_curs.execute("DROP TABLE IF EXISTS X010_Studdeb_tranlist_prevyr")
so_curs.execute(s_sql)
so_conn.commit()

fl.write(datetime.datetime.now().strftime("%H:%M:%S") + " BUILD TABLE: X010_Studdeb_tranlist_prevyr\r\n")

# Add calculated fields to the gl transaction list *****************************

print("Add columns...")

# Calc campus name
so_curs.execute("ALTER TABLE X010_Studdeb_tranlist_prevyr ADD COLUMN CAMPUS TEXT;")
so_curs.execute("UPDATE X010_Studdeb_tranlist_prevyr " + """
                SET CAMPUS = 
                CASE
                   WHEN ACCOUNT_NBR = "1G00362" THEN "POTCHEFSTROOM CAMPUS"
                   WHEN ACCOUNT_NBR = "1G00963" THEN "VAAL TRIANGLE CAMPUS"
                   WHEN ACCOUNT_NBR = "1G00265" THEN "MAFIKENG CAMPUS"
                   WHEN ACCOUNT_NBR = "1G00267" THEN "MAFIKENG CAMPUS"
                   ELSE "NEW_ACCOUNT_ALLOCATE"
                END
                ;""")
so_conn.commit()
fl.write(datetime.datetime.now().strftime("%H:%M:%S") + " CALC COLUMN: Campus\r\n")

# Calc campus code
so_curs.execute("ALTER TABLE X010_Studdeb_tranlist_prevyr ADD COLUMN CAMPUS_VSS TEXT;")
so_curs.execute("UPDATE X010_Studdeb_tranlist_prevyr " + """
                SET CAMPUS_VSS = 
                CASE
                   WHEN CAMPUS = "POTCHEFSTROOM CAMPUS" THEN "-1"
                   WHEN CAMPUS = "VAAL TRIANGLE CAMPUS" THEN "-2"
                   ELSE "-9"
                END
                ;""")
so_conn.commit()
fl.write(datetime.datetime.now().strftime("%H:%M:%S") + " CALC COLUMN: Campus code\r\n")

# Calc bursary code ************************************************************
so_curs.execute("ALTER TABLE X010_Studdeb_tranlist_prevyr ADD COLUMN BURSARY_CODE TEXT;")
so_curs.execute("UPDATE X010_Studdeb_tranlist_prevyr " + """
                SET BURSARY_CODE = 
                CASE
                   WHEN FS_ORIGIN_CD = 10 AND INSTR(UPPER(TRN_LDGR_ENTR_DESC),"BEURS:") > 0 AND INSTR(UPPER(TRN_LDGR_ENTR_DESC),"STATUS:U") > 0 THEN SUBSTR(TRN_LDGR_ENTR_DESC,INSTR(UPPER(TRN_LDGR_ENTR_DESC),"BEURS:")+6,INSTR(UPPER(TRN_LDGR_ENTR_DESC),"STUDENT:")-8)
                   WHEN FS_ORIGIN_CD = 10 AND INSTR(UPPER(TRN_LDGR_ENTR_DESC),"BURSARY:") > 0 AND INSTR(UPPER(TRN_LDGR_ENTR_DESC),"STATUS:U") > 0 THEN SUBSTR(TRN_LDGR_ENTR_DESC,INSTR(UPPER(TRN_LDGR_ENTR_DESC),"BURSARY:")+8,INSTR(UPPER(TRN_LDGR_ENTR_DESC),"STUDENT:")-10)
                   WHEN FS_ORIGIN_CD = 10 AND INSTR(UPPER(TRN_LDGR_ENTR_DESC),"BURSARY :") > 0 AND INSTR(UPPER(TRN_LDGR_ENTR_DESC),"STATUS : K") > 0 THEN SUBSTR(TRN_LDGR_ENTR_DESC,INSTR(UPPER(TRN_LDGR_ENTR_DESC),"BURSARY:")+11,INSTR(UPPER(TRN_LDGR_ENTR_DESC),"LEARNER :")-12)
                   ELSE ""
                END
                ;""")
so_conn.commit()
fl.write(datetime.datetime.now().strftime("%H:%M:%S") + " CALC COLUMN: Bursary code\r\n")

# Calc student number **********************************************************
so_curs.execute("ALTER TABLE X010_Studdeb_tranlist_prevyr ADD COLUMN STUDENT TEXT;")
so_curs.execute("UPDATE X010_Studdeb_tranlist_prevyr " + """
                SET STUDENT = 
                CASE
                   WHEN FS_ORIGIN_CD = 10 AND INSTR(UPPER(TRN_LDGR_ENTR_DESC),"STATUS:U") > 0 THEN SUBSTR(TRN_LDGR_ENTR_DESC,INSTR(UPPER(TRN_LDGR_ENTR_DESC),"STUDENT:")+8,8)
                   WHEN FS_ORIGIN_CD = 10 AND INSTR(UPPER(TRN_LDGR_ENTR_DESC),"STATUS : K") > 0 THEN SUBSTR(TRN_LDGR_ENTR_DESC,INSTR(UPPER(TRN_LDGR_ENTR_DESC),"LEARNER :")+10,8)
                   ELSE ""
                END
                ;""")
so_conn.commit()
fl.write(datetime.datetime.now().strftime("%H:%M:%S") + " CALC COLUMN: Student number\r\n")

# Temp description - Remove characters from description ************************
so_curs.execute("ALTER TABLE X010_Studdeb_tranlist_prevyr ADD COLUMN TEMP TEXT;")
so_curs.execute("UPDATE X010_Studdeb_tranlist_prevyr SET TEMP = REPLACE(TRN_LDGR_ENTR_DESC,'0','');")
so_curs.execute("UPDATE X010_Studdeb_tranlist_prevyr SET TEMP = REPLACE(TEMP,'1','');")
so_curs.execute("UPDATE X010_Studdeb_tranlist_prevyr SET TEMP = REPLACE(TEMP,'2','');")
so_curs.execute("UPDATE X010_Studdeb_tranlist_prevyr SET TEMP = REPLACE(TEMP,'3','');")
so_curs.execute("UPDATE X010_Studdeb_tranlist_prevyr SET TEMP = REPLACE(TEMP,'4','');")
so_curs.execute("UPDATE X010_Studdeb_tranlist_prevyr SET TEMP = REPLACE(TEMP,'5','');")
so_curs.execute("UPDATE X010_Studdeb_tranlist_prevyr SET TEMP = REPLACE(TEMP,'6','');")
so_curs.execute("UPDATE X010_Studdeb_tranlist_prevyr SET TEMP = REPLACE(TEMP,'7','');")
so_curs.execute("UPDATE X010_Studdeb_tranlist_prevyr SET TEMP = REPLACE(TEMP,'8','');")
so_curs.execute("UPDATE X010_Studdeb_tranlist_prevyr SET TEMP = REPLACE(TEMP,'9','');")
so_curs.execute("UPDATE X010_Studdeb_tranlist_prevyr SET TEMP = REPLACE(TEMP,'(','');")
so_curs.execute("UPDATE X010_Studdeb_tranlist_prevyr SET TEMP = REPLACE(TEMP,')','');")
so_curs.execute("UPDATE X010_Studdeb_tranlist_prevyr SET TEMP = REPLACE(TEMP,'.','');")
so_curs.execute("UPDATE X010_Studdeb_tranlist_prevyr SET TEMP = REPLACE(TEMP,'-','');")
so_curs.execute("UPDATE X010_Studdeb_tranlist_prevyr SET TEMP = REPLACE(TEMP,':','');")
so_curs.execute("UPDATE X010_Studdeb_tranlist_prevyr SET TEMP = REPLACE(TEMP,'/','');")

so_conn.commit()
fl.write(datetime.datetime.now().strftime("%H:%M:%S") + " CALC COLUMN: Temp description\r\n")

# Calc transaction description *************************************************
so_curs.execute("ALTER TABLE X010_Studdeb_tranlist_prevyr ADD COLUMN DESCRIPTION TEXT;")
so_curs.execute("UPDATE X010_Studdeb_tranlist_prevyr " + """
                SET DESCRIPTION = 
                CASE
                   WHEN FS_ORIGIN_CD = 11 THEN UPPER(TEMP)
                   WHEN FS_ORIGIN_CD = 10 AND INSTR(UPPER(TRN_LDGR_ENTR_DESC),"BEURS:") > 0 THEN "BEURSE EN LENINGS KLASGELDE"
                   WHEN FS_ORIGIN_CD = 10 AND INSTR(UPPER(TRN_LDGR_ENTR_DESC),"BURSARY:") > 0 THEN "BURSARIES AND LOANS TUITION FEES"
                   WHEN FS_ORIGIN_CD = 10 AND INSTR(UPPER(TRN_LDGR_ENTR_DESC),"LEARNER :") > 0 THEN "BURSARY CANCELLATION TUITION FEES"
                   ELSE "X "||UPPER(TRN_LDGR_ENTR_DESC)||" ORIGIN:"||UPPER(FS_DATABASE_DESC)||" EDOC:"||UPPER(FDOC_NBR)
                END
                ;""")
so_conn.commit()
fl.write(datetime.datetime.now().strftime("%H:%M:%S") + " CALC COLUMN: Description\r\n")

# Build transactions ***********************************************************

print("Build transaction file...")

s_sql = "CREATE VIEW X011_Studdeb_transort AS " + """
SELECT
  X010_Studdeb_tranlist_prevyr.CAMPUS,
  X010_Studdeb_tranlist_prevyr.CAMPUS_VSS,
  X010_Studdeb_tranlist_prevyr.UNIV_FISCAL_YR AS YEAR,
  X010_Studdeb_tranlist_prevyr.UNIV_FISCAL_PRD_CD AS MONTH,
  X010_Studdeb_tranlist_prevyr.CALC_COST_STRING AS COST_STRING,
  X010_Studdeb_tranlist_prevyr.TRANSACTION_DT AS DATE_TRAN,
  X010_Studdeb_tranlist_prevyr.FDOC_NBR AS EDOC,
  X010_Studdeb_tranlist_prevyr.CALC_AMOUNT AS AMOUNT,
  X010_Studdeb_tranlist_prevyr.TRN_LDGR_ENTR_DESC AS DESC_FULL,
  X010_Studdeb_tranlist_prevyr.DESCRIPTION AS DESC_VSS,
  X010_Studdeb_tranlist_prevyr.BURSARY_CODE AS BURSARY,
  X010_Studdeb_tranlist_prevyr.STUDENT,
  X010_Studdeb_tranlist_prevyr.FS_ORIGIN_CD AS ORIGIN_CODE,
  X010_Studdeb_tranlist_prevyr.FS_DATABASE_DESC AS ORIGIN,
  X010_Studdeb_tranlist_prevyr.ORG_NM AS ORG_NAME,
  X010_Studdeb_tranlist_prevyr.ACCOUNT_NM AS ACC_NAME,
  X010_Studdeb_tranlist_prevyr.FIN_OBJ_CD_NM AS OBJ_NAME,
  X010_Studdeb_tranlist_prevyr.ACCT_TYP_NM AS ACC_TYPE,
  X010_Studdeb_tranlist_prevyr.TRN_POST_DT AS DATE_POST,
  X010_Studdeb_tranlist_prevyr."TIMESTAMP" AS TIME_POST,
  X010_Studdeb_tranlist_prevyr.FIN_COA_CD AS ORG,
  X010_Studdeb_tranlist_prevyr.ACCOUNT_NBR AS ACC,
  X010_Studdeb_tranlist_prevyr.FIN_OBJECT_CD AS OBJ,
  X010_Studdeb_tranlist_prevyr.FIN_BALANCE_TYP_CD AS BAL_TYPE,
  X010_Studdeb_tranlist_prevyr.FIN_OBJ_TYP_CD AS OBJ_TYPE,
  X010_Studdeb_tranlist_prevyr.FDOC_TYP_CD AS DOC_TYPE,
  X010_Studdeb_tranlist_prevyr.TRN_ENTR_SEQ_NBR,
  X010_Studdeb_tranlist_prevyr.FDOC_REF_TYP_CD,
  X010_Studdeb_tranlist_prevyr.FS_REF_ORIGIN_CD,
  X010_Studdeb_tranlist_prevyr.FDOC_REF_NBR,
  X010_Studdeb_tranlist_prevyr.FDOC_REVERSAL_DT,
  X010_Studdeb_tranlist_prevyr.TRN_ENCUM_UPDT_CD
FROM
  X010_Studdeb_tranlist_prevyr
ORDER BY
  X010_Studdeb_tranlist_prevyr.CAMPUS,
  TIME_POST
;"""
so_curs.execute("DROP VIEW IF EXISTS X011_Studdeb_transort")
so_curs.execute(s_sql)
so_conn.commit()
fl.write(datetime.datetime.now().strftime("%H:%M:%S") + " BUILD VIEW: X011_Studdeb_transort\r\n")

# Export the data

print("Export student debtor gl transactions...")

sr_file = "X011_Studdeb_transort"
sr_filet = sr_file
sx_path = "O:/Nwudata/Debtorstud/" + funcdatn.get_current_year() + "/"
sx_file = "Debtor_011_gltran_"
sx_filet = sx_file + funcdatn.get_previous_year()

# Read the header data
s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)

# Write the data
funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_filet, s_head)

fl.write(datetime.datetime.now().strftime("%H:%M:%S") + " EXPORT DATA: X011_Studdeb_transort\r\n")

# Build vss gl transactions ****************************************************

print("Build vss transaction file...")

s_sql = "CREATE VIEW X012_Studdeb_tranvss AS " + """
SELECT
  X011_Studdeb_transort.CAMPUS AS gle02_CoaName,
  X011_Studdeb_transort.CAMPUS_VSS AS gle02_XCamp,
  X011_Studdeb_transort.MONTH AS gle09_YearId,
  X011_Studdeb_transort.AMOUNT AS gle15_XAmou,
  X011_Studdeb_transort.DATE_TRAN AS gle17_Date,
  X011_Studdeb_transort.STUDENT AS gle50_Stud,
  X011_Studdeb_transort.BURSARY AS gle51_Burs,
  X011_Studdeb_transort.DESC_FULL AS gle52_Desc,
  X011_Studdeb_transort.DESC_VSS AS gle53_Desc
FROM
  X011_Studdeb_transort
;"""
so_curs.execute("DROP VIEW IF EXISTS X012_Studdeb_tranvss")
so_curs.execute(s_sql)
so_conn.commit()
fl.write(datetime.datetime.now().strftime("%H:%M:%S") + " BUILD VIEW: X012_Studdeb_tranvss\r\n")

# Export the data

print("Export student debtor gl vss transactions...")

sr_file = "X012_Studdeb_tranvss"
sr_filet = sr_file
sx_path = "O:/Nwudata/Debtorstud/" + funcdatn.get_current_year() + "/"
sx_file = "Debtor_012_gltran_vss_"
sx_filet = sx_file + funcdatn.get_previous_year()

# Read the header data
s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)

# Write the data
funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_filet, s_head)

fl.write(datetime.datetime.now().strftime("%H:%M:%S") + " EXPORT DATA: X012_Studdeb_tranvss\r\n")

# Build gl balances ************************************************************

print("Build gl balances file...")

s_sql = "CREATE VIEW X013_Studdeb_glbal AS " + """
SELECT
  X010_Studdeb_tranlist_prevyr.CAMPUS AS CAMPUS,
  Sum(X010_Studdeb_tranlist_prevyr.CALC_AMOUNT) AS BALANCE
FROM
  X010_Studdeb_tranlist_prevyr
GROUP BY
  X010_Studdeb_tranlist_prevyr.CAMPUS
;"""
so_curs.execute("DROP VIEW IF EXISTS X013_Studdeb_glbal")
so_curs.execute(s_sql)
so_conn.commit()
fl.write(datetime.datetime.now().strftime("%H:%M:%S") + " BUILD VIEW: X013_Studdeb_glbal\r\n")

# Export the data

print("Export student debtor gl balances...")

sr_file = "X013_Studdeb_glbal"
sr_filet = sr_file
sx_path = "O:/Nwudata/Debtorstud/" + funcdatn.get_current_year() + "/"
sx_file = "Debtor_013_glbal_"
sx_filet = sx_file + funcdatn.get_previous_year()

# Read the header data
s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)

# Write the data
funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_filet, s_head)

fl.write(datetime.datetime.now().strftime("%H:%M:%S") + " EXPORT DATA: X013_Studdeb_glbal\r\n")

# Build gl balances per month **************************************************

print("Build gl balances per month file...")

s_sql = "CREATE VIEW X014_Studdeb_glbalmonth AS " + """
SELECT
  X010_Studdeb_tranlist_prevyr.CAMPUS AS CAMPUS,
  X010_Studdeb_tranlist_prevyr.UNIV_FISCAL_PRD_CD AS MONTH,
  Sum(X010_Studdeb_tranlist_prevyr.CALC_AMOUNT) AS BALANCE
FROM
  X010_Studdeb_tranlist_prevyr
GROUP BY
  X010_Studdeb_tranlist_prevyr.CAMPUS,
  X010_Studdeb_tranlist_prevyr.UNIV_FISCAL_PRD_CD
;"""
so_curs.execute("DROP VIEW IF EXISTS X014_Studdeb_glbalmonth")
so_curs.execute(s_sql)
so_conn.commit()
fl.write(datetime.datetime.now().strftime("%H:%M:%S") + " BUILD VIEW: X014_Studdeb_glbalmonth\r\n")

# Export the data

print("Export student debtor gl per month balances...")

sr_file = "X014_Studdeb_glbalmonth"
sr_filet = sr_file
sx_path = "O:/Nwudata/Debtorstud/" + funcdatn.get_current_year() + "/"
sx_file = "Debtor_014_glbalmonth_"
sx_filet = sx_file + funcdatn.get_previous_year()

# Read the header data
s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)

# Write the data
funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_filet, s_head)

fl.write(datetime.datetime.now().strftime("%H:%M:%S") + " EXPORT DATA: X014_Studdeb_glbalmonth\r\n")

# Build gl summary per transaction type ****************************************

print("Build gl summary per type...")

s_sql = "CREATE VIEW X015_Studdeb_glsummtype AS " + """
SELECT
  X010_Studdeb_tranlist_prevyr.CAMPUS,
  X010_Studdeb_tranlist_prevyr.UNIV_FISCAL_PRD_CD AS MONTH,
  X010_Studdeb_tranlist_prevyr.DESCRIPTION,
  Sum(X010_Studdeb_tranlist_prevyr.CALC_AMOUNT) AS AMOUNT
FROM
  X010_Studdeb_tranlist_prevyr
GROUP BY
  X010_Studdeb_tranlist_prevyr.CAMPUS,
  X010_Studdeb_tranlist_prevyr.UNIV_FISCAL_PRD_CD,
  X010_Studdeb_tranlist_prevyr.DESCRIPTION
ORDER BY
  MONTH DESC
;"""
so_curs.execute("DROP VIEW IF EXISTS X015_Studdeb_glsummtype")
so_curs.execute(s_sql)
so_conn.commit()
fl.write(datetime.datetime.now().strftime("%H:%M:%S") + " BUILD VIEW: X015_Studdeb_glsummtype\r\n")

# Export the data

print("Export student debtor gl summary per type...")

sr_file = "X015_Studdeb_glsummtype"
sr_filet = sr_file
sx_path = "O:/Nwudata/Debtorstud/" + funcdatn.get_current_year() + "/"
sx_file = "Debtor_015_glsummtype_"
sx_filet = sx_file + funcdatn.get_previous_year()

# Read the header data
s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)

# Write the data
funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_filet, s_head)

fl.write(datetime.datetime.now().strftime("%H:%M:%S") + " EXPORT DATA: X015_Studdeb_glsummtype\r\n")

# Close the table connection ***************************************************
so_conn.close()

# Close the log writer *********************************************************
fl.close()
