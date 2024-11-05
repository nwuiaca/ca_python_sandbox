"""
SCRIPT TO BUILD PEOPLE LISTS
AUTHOR: Albert J v Rensburg (NWU:21162395)
CREATED: 12 APR 2018
MODIFIED: 5 APR 2020
"""

# IMPORT SYSTEM MODULES
import csv
import sqlite3

# IMPORT OWN MODULES
from _my_modules import funcconf
from _my_modules import funccsv
from _my_modules import funcdatn
from _my_modules import funcfile
from _my_modules import funcmail
from _my_modules import funcpayroll
from _my_modules import funcpeople
from _my_modules import funcsms
from _my_modules import funcsys

""" INDEX
ENVIRONMENT
OPEN THE DATABASES
BEGIN OF SCRIPT
End OF SCRIPT
"""

"""*****************************************************************************
ENVIRONMENT
*****************************************************************************"""

# SCRIPT LOG
funcfile.writelog("Now")
funcfile.writelog("SCRIPT: B001_PEOPLE_LISTS_DEV")
funcfile.writelog("-----------------------------")
print("---------------------")
print("B001_PEOPLE_LISTS_DEV")
print("---------------------")

# DECLARE VARIABLES
so_path = f"{funcconf.drive_data_raw}People/"  # Source database path
so_file = "People.sqlite"  # Source database
sr_file: str = ""  # Current sqlite table
re_path = f"{funcconf.drive_data_results}People/"  # Results path
l_debug: bool = True
l_export: bool = True
l_mail: bool = True
l_vacuum: bool = False

"""*****************************************************************************
OPEN THE DATABASES
*****************************************************************************"""
print("OPEN THE DATABASES")
funcfile.writelog("OPEN THE DATABASES")

# Open the SQLITE SOURCE file
with sqlite3.connect(so_path + so_file) as so_conn:
    so_curs = so_conn.cursor()
funcfile.writelog("OPEN DATABASE: " + so_file)

# ATTACH DATA SOURCES
so_curs.execute(f"ATTACH DATABASE '{funcconf.drive_data_raw}People_payroll/People_payroll.sqlite' AS 'PAYROLL'")
funcfile.writelog("%t ATTACH DATABASE: PEOPLE.SQLITE")

"""*****************************************************************************
BEGIN OF SCRIPT
*****************************************************************************"""
if l_debug:
    print("BEGIN OF SCRIPT")
funcfile.writelog("BEGIN OF SCRIPT")

# Build a people structure list
print("Build a people structure list...")
s_sql = "CREATE TABLE X000_PEOPLE_STRUCT AS " + """
Select
    peop1.employee_number,
    peop1.name_address || ' (' || peop1.preferred_name || ')' As employee,
    peop1.organization,
    peop1.position_name,
    peop1.grade_calc,
    peop2.name_address || ' (' || peop2.preferred_name || ')' As line2,
    peop3.name_address || ' (' || peop3.preferred_name || ')' As line3,
    peop4.name_address || ' (' || peop4.preferred_name || ')' As line4,
    ohead.name_address || ' (' || ohead.preferred_name || ')' As oehead,
    1 as customer
From
    X000_PEOPLE peop1 Inner Join
    X000_PEOPLE peop2 On peop2.employee_number = peop1.supervisor_number Inner Join
    X000_PEOPLE peop3 On peop3.employee_number = peop2.supervisor_number Inner Join
    X000_PEOPLE peop4 On peop4.employee_number = peop3.supervisor_number Inner Join
    X000_PEOPLE ohead On ohead.employee_number = peop1.oe_head_number
;"""
so_curs.execute("DROP TABLE IF EXISTS X000_PEOPLE_STRUCT")
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: X000_PEOPLE_STRUCT")

""" ****************************************************************************
BUILD CONTRACTS
*****************************************************************************"""
if l_debug:
    print("BUILD CONTRACTS")
funcfile.writelog("BUILD CONTRACTS")

# BUILD CONTRACTS MASTER
print("Build contracts master...")
sr_file = "X000_CONTRACT"
s_sql = "CREATE VIEW " + sr_file + " AS " + """
SELECT
    T.ASSIGNMENT_EXTRA_INFO_ID,
    T.ASSIGNMENT_ID,
    Cast(T.AEI_INFORMATION1 As Int) As CONTRACT_NUMBER,
    Upper(T.AEI_INFORMATION3) As PERSON_TYPE,
    Upper(T.AEI_INFORMATION15) As CONTRACT_CATEGORY,
    Upper(T.AEI_INFORMATION25) As CONTRACT_TYPE,
    Substr(Replace(T.AEI_INFORMATION4,'/','-'),1,10) As CONTRACT_FROM,
    --Substr(Replace(T.AEI_INFORMATION5,'/','-'),1,10) As CONTRACT_TO,
    Case
        When T.AEI_INFORMATION29 != '' Then Substr(Replace(T.AEI_INFORMATION29,'/','-'),1,10)
        Else Substr(Replace(T.AEI_INFORMATION5,'/','-'),1,10)
    End As CONTRACT_TO,    
    Substr(Replace(T.AEI_INFORMATION28,'/','-'),1,10) As AMEND_FROM,
    Substr(Replace(T.AEI_INFORMATION29,'/','-'),1,10) As AMEND_TO,
    Upper(T.AEI_INFORMATION22) As UNIT_CURRENCY,
    Cast(T.AEI_INFORMATION6 As Real) As CONTRACT_RATE,
    Upper(T.AEI_INFORMATION7) As CONTRACT_UNIT,
    Upper(T.AEI_INFORMATION19) As UNIT_DESCRIPTION,
    Cast(T.AEI_INFORMATION8 As Real) As CONTRACT_TOTAL_UNITS,
    Cast(T.AEI_INFORMATION9 As Real) As CONTRACT_TOTAL_AMOUNT,
    Cast(T.AEI_INFORMATION11 As Real) As HOURS_WORKED,
    Upper(T.AEI_INFORMATION21) As READY_TO_CLAIM,
    Cast(T.AEI_INFORMATION26 As Int) As MANAGER_ID,
    Upper(T.AEI_INFORMATION27) As POSITION_PERM,
    Upper(T.AEI_INFORMATION30) As LOCATION,    
    Cast(T.AEI_INFORMATION20 As Int) As ACCOUNT_ID,
    Cast(T.AEI_INFORMATION10 As Int) As ACCOUNT_COMBINATION_ID,
    Cast(T.AEI_INFORMATION12 As Int) As DEPARTMENT_ID,
    Cast(T.AEI_INFORMATION13 As Int) As UNKNOWN_ID,
    Cast(T.AEI_INFORMATION14 As Int) As SARS_REPORT_ID,
    Cast(T.AEI_INFORMATION16 As Int) As REMUNERATION_BEFORE_ID,
    Cast(T.AEI_INFORMATION17 As Int) As PROJECT_ID,
    Cast(T.AEI_INFORMATION18 As Int) As SOURCE_CONTRACT_ID,
    T.LAST_UPDATE_DATE,
    T.LAST_UPDATED_BY,
    T.LAST_UPDATE_LOGIN,
    T.CREATED_BY,
    T.CREATION_DATE    
FROM
    PER_ASSIGNMENT_EXTRA_INFO_TEMP T
ORDER BY
    ASSIGNMENT_ID,
    AEI_INFORMATION4            
;"""
so_curs.execute("DROP VIEW IF EXISTS " + sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD VIEW: " + sr_file)

# BUILD CONTRACTS MASTER
print("Build contracts current...")
sr_file = "X004_CONTRACT_CURR"
s_sql = "CREATE TABLE " + sr_file + " AS " + """
Select
    c.ASSIGNMENT_EXTRA_INFO_ID,
    c.ASSIGNMENT_ID,
    c.CONTRACT_NUMBER,
    c.PERSON_TYPE,
    c.CONTRACT_CATEGORY,
    c.CONTRACT_TYPE,
    c.CONTRACT_FROM,
    c.CONTRACT_TO,
    c.AMEND_FROM,
    c.AMEND_TO,
    c.CONTRACT_RATE,
    c.CONTRACT_UNIT,
    c.UNIT_DESCRIPTION,
    c.UNIT_CURRENCY,
    c.CONTRACT_TOTAL_UNITS,
    c.CONTRACT_TOTAL_AMOUNT,
    c.HOURS_WORKED,
    Cast(Round((Julianday(c.CONTRACT_TO) - Julianday(c.CONTRACT_FROM))/30.4167) As Int) As CONTRACT_MONTHS,
    Cast(Round(c.HOURS_WORKED/Round((Julianday(c.CONTRACT_TO) - Julianday(c.CONTRACT_FROM))/30.4167),1) As Real) As HOURS_MONTH,
    c.READY_TO_CLAIM,
    c.MANAGER_ID,
    c.POSITION_PERM,
    c.LOCATION,
    c.ACCOUNT_ID,
    c.ACCOUNT_COMBINATION_ID,
    c.DEPARTMENT_ID,
    c.UNKNOWN_ID,
    c.SARS_REPORT_ID,
    c.REMUNERATION_BEFORE_ID,
    c.PROJECT_ID,
    c.SOURCE_CONTRACT_ID,
    c.LAST_UPDATE_DATE,
    c.LAST_UPDATED_BY,
    c.LAST_UPDATE_LOGIN,
    c.CREATED_BY,
    c.CREATION_DATE
From
    X000_CONTRACT c
Where
    strftime('%Y-%m-%d', '%DATE%') between c.contract_from and c.contract_to
;"""
s_sql = s_sql.replace("%DATE%", funcdatn.get_today_date())
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: " + sr_file)

# BUILD CONTRACTS MASTER
print("Build contracts current...")
sr_file = "X004_CONTRACT_CURR_SUMM"
s_sql = "CREATE TABLE " + sr_file + " AS " + """
Select
    c.ASSIGNMENT_ID,
    Count(c.ASSIGNMENT_EXTRA_INFO_ID) As COUNT_ASSIGNMENT
From
    X004_CONTRACT_CURR c
Group By
    c.ASSIGNMENT_ID
;"""
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: " + sr_file)

print("Build position structure...")
sr_file = "X000_POSITIONS_STRUCT"
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
s_sql = "CREATE TABLE " + sr_file + " AS " + """
Select
    peop.max_persons,
    peop.position_id As position1,
    peop.position_name As position_name1,
    peop.employee_number As employee1,
    peop.name_full As name1,
    peop.employee_category As category1,
    peop.user_person_type As type1,
    peop2.max_persons max_persons2,
    post.POS02 As position2,
    peop2.position_name As position_name2,
    peop2.employee_number As employee2,
    peop2.name_full As name2,
    peop.supervisor_name,
    peop.oe_head_name_name
From
    X000_PEOPLE peop Left Join
    X000_POS_STRUCT_10 post On post.POS01 = peop.position_id Left Join
    X000_PEOPLE peop2 On peop2.position_id = post.POS02
Order By
    employee2,
    position2,
    position1
;"""
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: " + sr_file)

""" ****************************************************************************
End OF SCRIPT
*****************************************************************************"""
if l_debug:
    print("END OF SCRIPT")
funcfile.writelog("END OF SCRIPT")

# CLOSE THE DATABASE CONNECTION
so_conn.commit()
so_conn.close()

# CLOSE THE LOG WRITER
funcfile.writelog("--------------------------------")
funcfile.writelog("COMPLETED: B001_PEOPLE_LISTS_DEV")
