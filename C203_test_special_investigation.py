"""
Script various special investigation related tests
Created on: 24 November 2023
Author: Albert J v Rensburg (NWU21162395)
"""

# IMPORT PYTHON MODULES
import csv
import sqlite3
# from fuzzywuzzy import fuzz

# IMPORT OWN MODULES
from _my_modules import funcconf
from _my_modules import funccsv
from _my_modules import funcdatn
from _my_modules import funcfile
from _my_modules import funcsms
from _my_modules import funcsys
from _my_modules import functest
from _my_modules import funcstr
from _my_modules import funcstat

# INDEX
"""
ENVIRONMENT
OPEN THE DATABASES
BEGIN OF SCRIPT
END OF SCRIPT
"""

"""*****************************************************************************
ENVIRONMENT
*****************************************************************************"""

# DECLARE VARIABLES
source_database_path: str = f"{funcconf.drive_data_raw}Kfs/"  # Source database path
source_database_name: str = "Kfs_test_si.sqlite"  # Source database
source_database: str = source_database_path + source_database_name
external_data_path: str = f"{funcconf.drive_system}_external_data/"  # external data path
results_path: str = f"{funcconf.drive_data_results}Kfs/" + funcdatn.get_current_year() + "/"  # Results path
s_sql: str = ""  # SQL statements
l_debug: bool = True
l_export: bool = False
l_mess: bool = False
l_mail: bool = False
l_record: bool = False

# OPEN THE SCRIPT LOG FILE
if l_debug:
    print("----------------")
    print("C203_KFS_TEST_SI")
    print("----------------")
funcfile.writelog("Now")
funcfile.writelog("SCRIPT: C203_KFS_TEST_SI")
funcfile.writelog("------------------------")

if l_mess:
    funcsms.send_telegram('', 'administrator', 'Testing <b>special investigations</b>.')

"""*****************************************************************************
OPEN THE DATABASES
*****************************************************************************"""
if l_debug:
    print("OPEN THE DATABASES")
funcfile.writelog("OPEN THE DATABASES")

# OPEN THE WORKING DATABASE
with sqlite3.connect(source_database) as sqlite_connection:
    sqlite_cursor = sqlite_connection.cursor()
funcfile.writelog("OPEN DATABASE: " + source_database)

# ATTACH DATA SOURCES
sqlite_cursor.execute(f"ATTACH DATABASE '{funcconf.drive_data_raw}People/People.sqlite' AS 'PEOPLE'")
funcfile.writelog("%t ATTACH DATABASE: PEOPLE.SQLITE")
sqlite_cursor.execute(f"ATTACH DATABASE '{funcconf.drive_data_raw}People_leave/People_leave.sqlite' AS 'LEAVE'")
funcfile.writelog("%t ATTACH DATABASE: PAYROLL.SQLITE")
sqlite_cursor.execute(f"ATTACH DATABASE '{funcconf.drive_data_raw}People_payroll/People_payroll.sqlite' AS 'PAYROLL'")
funcfile.writelog("%t ATTACH DATABASE: PAYROLL.SQLITE")
sqlite_cursor.execute(f"ATTACH DATABASE '{funcconf.drive_data_raw}Kfs/Kfs.sqlite' AS 'KFS'")
funcfile.writelog("%t ATTACH DATABASE: KFS.SQLITE")
sqlite_cursor.execute(f"ATTACH DATABASE '{funcconf.drive_data_raw}Kfs/Kfs_curr.sqlite' AS 'KFSCURR'")
funcfile.writelog("%t ATTACH DATABASE: KFS_CURR.SQLITE")
sqlite_cursor.execute(f"ATTACH DATABASE '{funcconf.drive_data_raw}Kfs/Kfs_prev.sqlite' AS 'KFSPREV'")
funcfile.writelog("%t ATTACH DATABASE: KFS_PREV.SQLITE")
sqlite_cursor.execute(f"ATTACH DATABASE '{funcconf.drive_data_raw}Vss/Vss_curr.sqlite' AS 'VSSCURR'")
funcfile.writelog("%t ATTACH DATABASE: VSS_CURR.SQLITE")

"""*****************************************************************************
BEGIN OF SCRIPT
*****************************************************************************"""
if l_debug:
    print("BEGIN OF SCRIPT")
funcfile.writelog("BEGIN OF SCRIPT")

"""*****************************************************************************
TEST ACTIVE CIPC DIRECTOR ACTIVE VENDOR NO DECLARATION
*****************************************************************************"""

""" DESCRIPTION
"""

""" INDEX
"""

"""" TABLES USED IN TEST
"""

# DECLARE TEST VARIABLES
count_findings_after: int = 0
test_description = "Single supplier single approver"
test_file_name: str = "single_supplier_single_user"
test_file_prefix: str = "X001a"
test_finding: str = "SINGLE SUPPLIER SINGE USER"
test_report_file: str = "304_reported.txt"

# OBTAIN TEST RUN FLAG
if not functest.get_test_flag(sqlite_cursor, "KFS", f"TEST {test_finding}", "RUN"):

    if l_debug:
        print('TEST DISABLED')
    funcfile.writelog("TEST " + test_finding + " DISABLED")

else:

    # Open log
    if l_debug:
        print("TEST " + test_finding)
    funcfile.writelog("TEST " + test_finding)

    # Obtain a list of purchase and payment employees to exclude
    exclude_pp_employee = funcstat.stat_tuple(sqlite_cursor, "X000_PEOPLE", "employee_number", "organization = 'NWU PURCHASE AND PAYMENTS' Or organization = 'NWU FINANCIAL PLANNING'")
    if l_debug:
        print(exclude_pp_employee)

    # Fetch initial data from the master table
    if l_debug:
        print("Fetch initial data from the master table...")
    table_name = test_file_prefix + f"a_{test_file_name}"
    s_sql = f"CREATE TABLE {table_name} As " + f"""
    Select
        v.EDOC,
        v.VENDOR_ID,
        v.VENDOR_NAME,
        v.VENDOR_TYPE,        
        v.PMT_DT,
        v.NET_PMT_AMT,
        v.APPROVE_EMP_NO,
        v.APPROVE_EMP_NAME,
        v.APPROVE_DATE,
        p.oe_code,
        v.A_COUNT,
        v.ACC_COST_STRING,
        SubStr(v.ACC_COST_STRING, 4, 7) As ORG_ID,
        a.ORG_NM
    From
        KFSPREV.X001ac_Report_payments_approve v Left Join
        PEOPLE.X000_PEOPLE p On p.employee_number = v.APPROVE_EMP_NO Left Join
        KFS.X000_Account a On a.ACCOUNT_NBR = SubStr(v.ACC_COST_STRING, 4, 7) 
    Where
        v.VENDOR_TYPE In ('DV', 'PO') And
        v.PMT_STAT_CD = 'EXTR' And
        v.APPROVE_EMP_NO Not In {exclude_pp_employee} And
        SubStr(v.ACC_COST_STRING, 4, 7) <> ''
    ;"""
    if l_debug:
        # print(s_sql)
        pass
    sqlite_cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
    sqlite_cursor.execute(s_sql)
    sqlite_connection.commit()
    funcfile.writelog(f"%t BUILD TABLE: {table_name}")

    # Identify the approvers per account per vendor
    if l_debug:
        print("Fetch initial data from the master table...")
    table_name = test_file_prefix + f"b_{test_file_name}"
    s_sql = f"CREATE TABLE {table_name} As " + f"""
    Select
        v.VENDOR_ID,
        v.VENDOR_NAME,
        v.VENDOR_TYPE,
        v.ORG_ID,
        v.ORG_NM,
        v.APPROVE_EMP_NO,
        v.APPROVE_EMP_NAME
    From
        X001aa_single_supplier_single_user v
    Group By
        v.VENDOR_ID,
        v.VENDOR_NAME,
        v.ORG_ID,
        v.ORG_NM,
        v.APPROVE_EMP_NO,
        v.APPROVE_EMP_NAME
    ;"""
    if l_debug:
        # print(s_sql)
        pass
    sqlite_cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
    sqlite_cursor.execute(s_sql)
    sqlite_connection.commit()
    funcfile.writelog(f"%t BUILD TABLE: {table_name}")

    # Count the approvers per account per vendor
    if l_debug:
        print("Fetch initial data from the master table...")
    table_name = test_file_prefix + f"c_{test_file_name}"
    s_sql = f"CREATE TABLE {table_name} As " + f"""
    Select
        v.VENDOR_ID,
        v.VENDOR_NAME,
        v.VENDOR_TYPE,
        v.ORG_ID,
        v.ORG_NM,
        Count('count') As count_approver
    From
        X001ab_single_supplier_single_user v
    Group By
        v.VENDOR_ID,
        v.VENDOR_NAME,
        v.ORG_ID,
        v.ORG_NM
    ;"""
    if l_debug:
        # print(s_sql)
        pass
    sqlite_cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
    sqlite_cursor.execute(s_sql)
    sqlite_connection.commit()
    funcfile.writelog(f"%t BUILD TABLE: {table_name}")

    # Count the approvers per account per vendor
    if l_debug:
        print("Fetch initial data from the master table...")
    table_name = test_file_prefix + f"d_{test_file_name}"
    s_sql = f"CREATE TABLE {table_name} As " + f"""
    Select
        v.VENDOR_ID,
        v.VENDOR_NAME,
        v.VENDOR_TYPE,
        Count('count') As count_organization,
        Sum(v.count_approver) As count_approver
    From
        X001ac_single_supplier_single_user v
    Group By
        v.VENDOR_ID,
        v.VENDOR_NAME    ;"""
    if l_debug:
        # print(s_sql)
        pass
    sqlite_cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
    sqlite_cursor.execute(s_sql)
    sqlite_connection.commit()
    funcfile.writelog(f"%t BUILD TABLE: {table_name}")

    # Add the number of transactions and value
    if l_debug:
        print("Fetch initial data from the master table...")
    table_name = test_file_prefix + f"e_{test_file_name}"
    s_sql = f"CREATE TABLE {table_name} As " + f"""
    Select
        v.VENDOR_ID,
        v.VENDOR_NAME,
        v.VENDOR_TYPE,
        v.count_organization,
        v.count_approver,
        p.LAST_PMT_DT,
        p.TRAN_COUNT,
        p.NET_PMT_AMT
    From
        X001ad_single_supplier_single_user v Left Join
        KFSPREV.X002aa_Report_payments_summary p On p.VENDOR_ID = v.VENDOR_ID    
    ;"""
    if l_debug:
        # print(s_sql)
        pass
    sqlite_cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
    sqlite_cursor.execute(s_sql)
    sqlite_connection.commit()
    funcfile.writelog(f"%t BUILD TABLE: {table_name}")

    # Build a list where an employee approved a transaction while on leave
    if l_debug:
        print("Fetch initial data from the master table...")
    table_name = test_file_prefix + f"f_{test_file_name}"
    s_sql = f"CREATE TABLE {table_name} As " + f"""
    Select
        v.EDOC,
        v.VENDOR_ID,
        v.VENDOR_NAME,
        v.PMT_DT,
        v.NET_PMT_AMT,
        v.APPROVE_EMP_NO,
        v.APPROVE_EMP_NAME,
        v.APPROVE_DATE,
        v.oe_code,
        v.A_COUNT,
        v.ACC_COST_STRING,
        v.ORG_ID,
        v.ORG_NM,
        a.EMPLOYEE_NUMBER,
        a.DATE_START,
        a.DATE_END,
        a.ABSENCE_DAYS,
        a.LEAVE_TYPE,
        a.REASON_DESCRIP
    From
        X001aa_single_supplier_single_user v Left join
        LEAVE.X103_PER_ABSENCE_ATTENDANCES_CURR a On a.EMPLOYEE_NUMBER = v.APPROVE_EMP_NO And
            v.APPROVE_DATE BETWEEN a.DATE_START AND a.DATE_END
    ;"""
    if l_debug:
        # print(s_sql)
        pass
    sqlite_cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
    sqlite_cursor.execute(s_sql)
    sqlite_connection.commit()
    funcfile.writelog(f"%t BUILD TABLE: {table_name}")

    # Build a list where an employee approved a transaction while on leave
    if l_debug:
        print("Fetch initial data from the master table...")
    table_name = test_file_prefix + f"g_{test_file_name}"
    s_sql = f"CREATE TABLE {table_name} As " + f"""
    Select
        v.EDOC,
        v.VENDOR_ID,
        v.VENDOR_NAME,
        v.PMT_DT,
        v.NET_PMT_AMT,
        v.APPROVE_EMP_NO,
        v.APPROVE_EMP_NAME,
        v.APPROVE_DATE,
        v.oe_code,
        v.A_COUNT,
        v.ACC_COST_STRING,
        v.ORG_ID,
        v.ORG_NM,
        v.EMPLOYEE_NUMBER,
        v.DATE_START,
        v.DATE_END,
        Total(v.ABSENCE_DAYS) As Total_ABSENCE_DAYS,
        v.LEAVE_TYPE,
        v.REASON_DESCRIP
    From
        X001af_single_supplier_single_user v
    Group By
        v.EDOC,
        v.VENDOR_ID,
        v.APPROVE_EMP_NO,
        v.APPROVE_DATE,
        v.DATE_START,
        v.DATE_END,
        v.LEAVE_TYPE
    ;"""
    if l_debug:
        # print(s_sql)
        pass
    sqlite_cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
    sqlite_cursor.execute(s_sql)
    sqlite_connection.commit()
    funcfile.writelog(f"%t BUILD TABLE: {table_name}")

"""*****************************************************************************
END OF SCRIPT
*****************************************************************************"""
if l_debug:
    print("END OF SCRIPT")
funcfile.writelog("END OF SCRIPT")

# CLOSE THE DATABASE CONNECTION
sqlite_connection.close()

# CLOSE THE LOG WRITER
funcfile.writelog("----------------------------------------")
funcfile.writelog("COMPLETED: C002_PEOPLE_TEST_CONFLICT_DEV")
