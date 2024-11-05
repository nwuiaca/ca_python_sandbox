""" C200_REPORT_STUDDEB_RECON **************************************************
***
*** Script to compare VSS and GL student transactions
***
*** Albert J van Rensburg (21162395)
*** 26 Jun 2018
***
*****************************************************************************"""

""" CONTENTS *******************************************************************
LIST GL TRANSACTIONS
LIST VSS TRANSACTIONS
JOIN VSS & GL MONTHLY TOTALS
JOIN VSS & GL TRANSACTIONS
TEST MATCHED TRANSACTION TYPES
TEST TRANSACTION TYPES IN VSS BUT NOT IN GL
TEST TRANSACTION TYPES IN GL BUT NOT IN VSS
BURSARY VSS GL RECON
TEST BURSARY INGL NOVSS
TEST BURSARY INVSS NOGL
TEST BURSARY POST TO DIFF CAMPUS IN GL
*****************************************************************************"""

# Import python modules
import csv
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
import funcmail
import funcsys
import funcmysql


# Open the script log file ******************************************************

funcfile.writelog("Now")
funcfile.writelog("SCRIPT: C200_REPORT_STUDDEB_RECON")
funcfile.writelog("---------------------------------")
print("-------------------------")
print("C200_REPORT_STUDDEB_RECON")
print("-------------------------")
ilog_severity = 1

# Declare variables
so_path = f"{funcconf.drive_data_raw}Kfs_vss_studdeb/" #Source database path
re_path = f"{funcconf.drive_data_results}Debtorstud/" #Results
ed_path = f"{funcconf.drive_system}_external_data/" #External data
so_file = "Kfs_vss_studdeb.sqlite" #Source database
s_sql = "" #SQL statements
l_mail = True
l_export = True

# Open the SOURCE file
with sqlite3.connect(so_path+so_file) as so_conn:
    so_curs = so_conn.cursor()
funcfile.writelog("OPEN DATABASE: " + so_file)

# Attach data sources
so_curs.execute(f"ATTACH DATABASE '{funcconf.drive_data_raw}People/People.sqlite' AS 'PEOPLE'")
funcfile.writelog("%t ATTACH DATABASE: PEOPLE.SQLITE")
so_curs.execute(f"ATTACH DATABASE '{funcconf.drive_data_raw}Kfs/Kfs.sqlite' AS 'KFS'")
funcfile.writelog("%t ATTACH DATABASE: KFS.SQLITE")
so_curs.execute(f"ATTACH DATABASE '{funcconf.drive_data_raw}Vss/Vss.sqlite' AS 'VSS'")
funcfile.writelog("%t ATTACH DATABASE: VSS.SQLITE")

# Open the MYSQL DESTINATION table
s_database = "Web_ia_nwu"
ms_cnxn = funcmysql.mysql_open(s_database)
ms_curs = ms_cnxn.cursor()
funcfile.writelog("%t OPEN MYSQL DATABASE: " + s_database)    

# Create MYSQL VSS GL MONTHLY BALANCES TO WEB table ****************************
# Depracated 20231104 with move to new web server
'''
print("Build mysql vss gl monthly balances...")
ms_curs.execute("DROP TABLE IF EXISTS ia_finding_5")
funcfile.writelog("%t DROP MYSQL TABLE: ia_finding_5")
s_sql = """
CREATE TABLE IF NOT EXISTS ia_finding_5 (
ia_find_auto INT(11),
ia_find5_auto INT(11) AUTO_INCREMENT,
ia_find5_campus VARCHAR(20),
ia_find5_month VARCHAR(2),
ia_find5_vss_tran_dt DECIMAL(20,2),
ia_find5_vss_tran_ct DECIMAL(20,2),
ia_find5_vss_tran DECIMAL(20,2),
ia_find5_vss_runbal DECIMAL(20,2),
ia_find5_gl_tran DECIMAL(20,2),
ia_find5_gl_runbal DECIMAL(20,2),
ia_find5_diff DECIMAL(20,2),
ia_find5_move DECIMAL(20,2),
ia_find5_officer_camp VARCHAR(10),
ia_find5_officer_name_camp VARCHAR(50),
ia_find5_officer_mail_camp VARCHAR(100),
ia_find5_officer_org VARCHAR(10),
ia_find5_officer_name_org VARCHAR(50),
ia_find5_officer_mail_org VARCHAR(100),
ia_find5_supervisor_camp VARCHAR(10),
ia_find5_supervisor_name_camp VARCHAR(50),
ia_find5_supervisor_mail_camp VARCHAR(100),
ia_find5_supervisor_org VARCHAR(10),
ia_find5_supervisor_name_org VARCHAR(50),
ia_find5_supervisor_mail_org VARCHAR(100),
PRIMARY KEY (ia_find5_auto),
INDEX fb_order_ia_find5_campus_INDEX (ia_find5_campus),
INDEX fb_order_ia_find5_month_INDEX (ia_find5_month)
)
ENGINE = InnoDB
CHARSET=utf8mb4
COLLATE utf8mb4_unicode_ci
COMMENT = 'Table to store vss and gl monthly balances'
""" + ";"
ms_curs.execute(s_sql)
funcfile.writelog("%t CREATE MYSQL TABLE: ia_finding_5 (vss gl monthly balances per campus per month)")
# Open the SOURCE file to obtain column headings
print("Build mysql vss gl monthly balance columns...")
funcfile.writelog("OPEN DATABASE: ia_finding_5")
s_head = funcmysql.get_colnames_sqlite_text(so_curs,"X002ex_vss_gl_balance_month","ia_find5_")
s_head = "(`ia_find_auto`, " + s_head.rstrip(", ") + ")"
#print(s_head)
# Open the SOURCE file to obtain the data
print("Insert mysql vss gl monthly balance data...")
with sqlite3.connect(so_path+so_file) as rs_conn:
    rs_conn.row_factory = sqlite3.Row
rs_curs = rs_conn.cursor()
rs_curs.execute("SELECT * FROM X002ex_vss_gl_balance_month")
rows = rs_curs.fetchall()
i_tota = 0
i_coun = 0
for row in rows:
    s_data = "(5, "
    for member in row:
        #print(type(member))
        if type(member) == str:
            s_data = s_data + "'" + member + "', "
        elif type(member) == int:
            s_data = s_data + str(member) + ", "
        elif type(member) == float:
            s_data = s_data + str(member) + ", "
        else:
            s_data = s_data + "'', "
    s_data = s_data.rstrip(", ") + ")"
    #print(s_data)
    s_sql = "INSERT INTO `ia_finding_5` " + s_head + " VALUES " + s_data + ";"
    ms_curs.execute(s_sql)
    i_tota = i_tota + 1
    i_coun = i_coun + 1
    if i_coun == 100:
        ms_cnxn.commit()
        i_coun = 0
ms_cnxn.commit()
print("Inserted " + str(i_tota) + " rows...")
funcfile.writelog("%t POPULATE MYSQL TABLE: ia_finding_5 with " + str(i_tota) + " rows")
'''

"""*************************************************************************
***
*** JOIN VSS & GL TRANSACTIONS
***
***
*************************************************************************"""


# Create MYSQL VSS GL COMPARISON PER CAMPUS PER MONTH TO WEB table *************
print("Build mysql vss gl comparison campus month...")
ms_curs.execute("DROP TABLE IF EXISTS ia_finding_6")
funcfile.writelog("%t DROP MYSQL TABLE: ia_finding_6")
s_sql = """
CREATE TABLE IF NOT EXISTS ia_finding_6 (
ia_find_auto INT(11),
ia_find6_auto INT(11) AUTO_INCREMENT,
ia_find6_campus VARCHAR(20),
ia_find6_month VARCHAR(2),
ia_find6_trancode VARCHAR(5),
ia_find6_vss_description VARCHAR(150),
ia_find6_vss_amount DECIMAL(20,2),
ia_find6_gl_description VARCHAR(150),
ia_find6_gl_amount DECIMAL(20,2),
ia_find6_diff DECIMAL(20,2),
ia_find6_matched VARCHAR(2),
ia_find6_period VARCHAR(7),
PRIMARY KEY (ia_find6_auto),
INDEX fb_order_ia_find6_campus_INDEX (ia_find6_campus),
INDEX fb_order_ia_find6_month_INDEX (ia_find6_month)
)
ENGINE = InnoDB
CHARSET=utf8mb4
COLLATE utf8mb4_unicode_ci
COMMENT = 'Table to store vss and gl monthly comparisons'
""" + ";"
ms_curs.execute(s_sql)
funcfile.writelog("%t CREATE MYSQL TABLE: ia_finding_6 (vss gl comparison per campus per month)")
# Open the SOURCE file to obtain column headings
print("Build mysql vss gl comparison columns...")
funcfile.writelog("OPEN DATABASE: ia_finding_6")
s_head = funcmysql.get_colnames_sqlite_text(so_curs,"X003ax_vss_gl_join","ia_find6_")
s_head = "(ia_find_auto, " + s_head.rstrip(", ") + ")"
#print(s_head)
# Open the SOURCE file to obtain the data
print("Insert mysql vss gl comparison data...")
with sqlite3.connect(so_path+so_file) as rs_conn:
    rs_conn.row_factory = sqlite3.Row
rs_curs = rs_conn.cursor()
rs_curs.execute("SELECT * FROM X003ax_vss_gl_join")
rows = rs_curs.fetchall()
i_tota = 0
i_coun = 0
for row in rows:
    s_data = "(6, "
    for member in row:
        #print(type(member))
        if type(member) == str:
            s_data = s_data + "'" + member + "', "
        elif type(member) == int:
            s_data = s_data + str(member) + ", "
        elif type(member) == float:
            s_data = s_data + str(member) + ", "
        else:
            s_data = s_data + "'', "
    s_data = s_data.rstrip(", ") + ")"
    #print(s_data)
    s_sql = "INSERT INTO `ia_finding_6` " + s_head + " VALUES " + s_data + ";"
    ms_curs.execute(s_sql)
    i_tota = i_tota + 1
    i_coun = i_coun + 1
    if i_coun == 100:
        ms_cnxn.commit()
        i_coun = 0
ms_cnxn.commit()
print("Inserted " + str(i_tota) + " rows...")
funcfile.writelog("%t POPULATE MYSQL TABLE: ia_finding_6 with " + str(i_tota) + " rows")

# Close the table connection ***************************************************
so_conn.close()
ms_cnxn.commit()    
ms_cnxn.close()    

# Close the log writer *********************************************************
funcfile.writelog("------------------------------------")
funcfile.writelog("COMPLETED: C200_REPORT_STUDDEB_RECON")
