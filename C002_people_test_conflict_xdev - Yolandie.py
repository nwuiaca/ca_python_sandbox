"""
Script to test PEOPLE conflict of interest
Created on: 8 Apr 2019
Modified on: 18 May 2021
Author: Albert J v Rensburg (NWU21162395)
"""

# IMPORT PYTHON MODULES
import csv
import sqlite3

# IMPORT OWN MODULES
from _my_modules import funcconf
from _my_modules import funccsv
from _my_modules import funcdatn
from _my_modules import funcfile
from _my_modules import funcsms
from _my_modules import funcsys
from _my_modules import functest

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
so_path: str = f"{funcconf.drive_data_raw}People_conflict/"  # Source database path
re_path: str = f"{funcconf.drive_data_results}People/" + funcdatn.get_current_year() + "/"  # Results path
ed_path: str = f"{funcconf.drive_system}_external_data/"  # external data path
so_file: str = "People_conflict.sqlite"  # Source database
s_sql: str = ""  # SQL statements
l_debug: bool = True
l_export: bool = False
l_mess: bool = False
l_mail: bool = False
l_record: bool = False

# OPEN THE SCRIPT LOG FILE
if l_debug:
    print("-----------------------------")
    print("C002_PEOPLE_TEST_CONFLICT_DEV")
    print("-----------------------------")
funcfile.writelog("Now")
funcfile.writelog("SCRIPT: C002_PEOPLE_TEST_CONFLICT_DEV")
funcfile.writelog("-------------------------------------")

if l_mess:
    funcsms.send_telegram('', 'administrator', 'Testing employee <b>conflict of interest</b>.')

"""*****************************************************************************
OPEN THE DATABASES
*****************************************************************************"""
if l_debug:
    print("OPEN THE DATABASES")
funcfile.writelog("OPEN THE DATABASES")

# OPEN THE WORKING DATABASE
with sqlite3.connect(so_path+so_file) as so_conn:
    so_curs = so_conn.cursor()
funcfile.writelog("OPEN DATABASE: " + so_file)

# ATTACH DATA SOURCES
so_curs.execute(f"ATTACH DATABASE '{funcconf.drive_data_raw}People/People.sqlite' AS 'PEOPLE'")
funcfile.writelog("%t ATTACH DATABASE: PEOPLE.SQLITE")
so_curs.execute(f"ATTACH DATABASE '{funcconf.drive_data_raw}People_payroll/People_payroll.sqlite' AS 'PAYROLL'")
funcfile.writelog("%t ATTACH DATABASE: PEOPLE.SQLITE")
so_curs.execute(f"ATTACH DATABASE '{funcconf.drive_data_raw}Kfs/Kfs.sqlite' AS 'KFS'")
funcfile.writelog("%t ATTACH DATABASE: KFS.SQLITE")
so_curs.execute(f"ATTACH DATABASE '{funcconf.drive_data_raw}Kfs/Kfs_curr.sqlite' AS 'KFSCURR'")
funcfile.writelog("%t ATTACH DATABASE: KFS_CURR.SQLITE")
so_curs.execute(f"ATTACH DATABASE '{funcconf.drive_data_raw}Vss/Vss_curr.sqlite' AS 'VSSCURR'")
funcfile.writelog("%t ATTACH DATABASE: VSS_CURR.SQLITE")

"""*****************************************************************************
BEGIN OF SCRIPT
*****************************************************************************"""
if l_debug:
    print("BEGIN OF SCRIPT")
funcfile.writelog("BEGIN OF SCRIPT")

"""*****************************************************************************
TEST CONFLICT PHONE NUMBER MASTER FILES
*****************************************************************************"""
if l_debug:
    print("TEST CONFLICT PHONE NUMBER MASTER FILES")
funcfile.writelog("TEST CONFLICT PHONE NUMBER MASTER FILES")

# BUILD AN EMPLOYEE PHONES MASTER TABLE
if l_debug:
    print("Obtain the employee phone numbers...")


# Obtain a list of exclude phone numbers
so_curs.execute('Select obj.LOOKUP_CODE From KFS.X000_Own_kfs_lookups obj Where obj.LOOKUP = "TEST CONFLICT PHONE NUMBER EXCLUDE PHONE"')
data = so_curs.fetchall()
exclude_phone: str = '['
for row in data:
    # if l_debug:
    #     print(row)
    for item in row:
        # if l_debug:
        #     print(item)
        exclude_phone += "'" + item + "',"
exclude_phone += ']'
exclude_phone = exclude_phone.replace(',]', ']')
# if l_debug:
#     print(exclude_phone)

# Create the database
so_curs.execute("DROP TABLE IF EXISTS X100_phone_emp")
so_curs.execute('CREATE TABLE X100_phone_emp (employee,name_address, phone)')

# Fetch the data from the original table
so_curs.execute('SELECT employee_number, name_address, phone_work, phone_mobile, phone_home FROM PEOPLE.X000_PEOPLE')
data = so_curs.fetchall()

# Merge the two columns and insert into the new table
phone = []
employee = ''
employee_name = ''
phone_test = ''
for row in data:
    # if l_debug:
    #     print(row)
    for item in row:
        # if l_debug:
        #         print(item)
        if employee == '':
            employee = item
        elif employee_name == '':
            employee_name = item
        else:
            if item:
                if item != phone_test and item not in exclude_phone:
                    so_curs.execute('INSERT INTO X100_phone_emp (employee, name_address, phone) VALUES (?,?,?)', (employee, employee_name, item))
                    phone_test = item
    employee = ''
    employee_name = ''
    phone = []

# Commit the changes and close the connection
so_conn.commit()

# Update the log file
funcfile.writelog("%t BUILD TABLE: X100_phone_emp")

# BUILD A LIST OF EXCLUDE OBJECTS
so_curs.execute('Select obj.LOOKUP_CODE From KFS.X000_Own_kfs_lookups obj Where obj.LOOKUP = "TEST CONFLICT PHONE NUMBER EXCLUDE OBJECT"')
data = so_curs.fetchall()
exclude_object: str = '('
for row in data:
    # if l_debug:
    #     print(row)
    for item in row:
        # if l_debug:
        #     print(item)
        exclude_object += "'" + item + "',"
exclude_object += ')'
exclude_object = exclude_object.replace(',)', ')')
# if l_debug:
#     print(exclude_object)

# OBTAIN PHONE TEST DATA FOR VENDORS
if l_debug:
    print("Obtain the vendor phone numbers...")
so_curs.execute("DROP TABLE IF EXISTS X100_phone_vend")
s_sql = "CREATE TABLE X100_phone_vend AS " + """
Select
    venc.VENDOR_ID As vendor_id,
    venc.VENDOR_NAME As vendor_name,
    vend.VNDR_URL_ADDR As vendor_regno,
    Count(venc.EDOC) As tran_count,
    Max(venc.PMT_DT) As last_pay_date,
    Total(venc.NET_PMT_AMT) As tran_total,
    vend.NUMBERS As vendor_numbers
From
    KFSCURR.X001ad_Report_payments_accroute venc Left join
    KFS.X000_Vendor vend On vend.VENDOR_ID = venc.vendor_id
Where
    venc.VENDOR_TYPE_CALC In ('DV','PO') And
    SubStr(venc.ACC_COST_STRING, -4) Not In %EXCLUDE_OBJECT%
Group By
    venc.VENDOR_ID
;"""
s_sql = s_sql.replace("%EXCLUDE_OBJECT%", exclude_object)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: X100_phone_vend")

"""*****************************************************************************
TEST CONFLICT PHONE NUMBER
*****************************************************************************"""

# FILES NEEDED
# X000_PEOPLE

# DEFAULT TRANSACTION OWNER PEOPLE

# DECLARE TEST VARIABLES
i_finding_before = 0
i_finding_after = 0
s_description = "Conflict phone number"
s_file_prefix: str = "X100c"
s_file_name: str = "conflict_phone_number"
s_finding: str = "CONFLICT PHONE NUMBER"
s_report_file: str = "001_reported.txt"

# UPDATE LOG FILE
if l_debug:
    print('TEST ' + s_finding)
funcfile.writelog("TEST " + s_finding)

# OBTAIN TEST RUN FLAG
if functest.get_test_flag(so_curs, "KFS", "TEST " + s_finding, "RUN") == "FALSE":

    if l_debug:
        print('TEST DISABLED')
    funcfile.writelog("TEST " + s_finding + " DISABLED")

else:

    # MERGE THE PEOPLE AND VENDOR PHONE NUMBERS
    if l_debug:
        print("Merge the employee end vendor phone numbers...")
    sr_file: str = s_file_prefix + "aa_" + s_file_name
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        emp.employee,
        emp.name_address,
        emp.phone,
        ven.vendor_id,
        ven.vendor_name,
        ven.vendor_regno,        
        ven.tran_count,
        ven.last_pay_date,
        ven.tran_total,
        ven.vendor_numbers,
        SubStr(emp.phone, -9) As test
    From
        X100_phone_emp emp Inner Join
        X100_phone_vend ven On ven.vendor_numbers Like ('%' || SubStr(emp.phone, -9) || '%')
    ;"""
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

so_conn.close()

"""*****************************************************************************
END OF SCRIPT
*****************************************************************************"""
if l_debug:
    print("END OF SCRIPT")
funcfile.writelog("END OF SCRIPT")

# CLOSE THE DATABASE CONNECTION
so_conn.close()

# CLOSE THE LOG WRITER
funcfile.writelog("----------------------------------------")
funcfile.writelog("COMPLETED: C002_PEOPLE_TEST_CONFLICT_DEV")
