""" Script to test PEOPLE master file data development area
Created on: 1 Mar 2019
Author: Albert Janse van Rensburg (NWU21162395)
"""

# TODO This report need to be developed
#   Move result into the payroll sqlite database
#   Make it data driven and date driven

# IMPORT SYSTEM MODULES
import csv
import sqlite3

# OPEN OWN MODULES
from _my_modules import funcconf
from _my_modules import funccsv
from _my_modules import funcdatn
from _my_modules import funcfile
from _my_modules import funcsys

""" INDEX **********************************************************************
ENVIRONMENT
OPEN THE DATABASES
BEGIN OF SCRIPT
END OF SCRIPT
*****************************************************************************"""

"""*****************************************************************************
ENVIRONMENT
*****************************************************************************"""

# OPEN THE LOG
print("-------------------------------")
print("C001_PEOPLE_TEST_MASTERFILE_DEV")
print("-------------------------------")
funcfile.writelog("Now")
funcfile.writelog("SCRIPT: C001_PEOPLE_TEST_MASTERFILE_DEV")
funcfile.writelog("---------------------------------------")

# DECLARE VARIABLES
ed_path = f"{funcconf.drive_system}_external_data/"  # External data path
so_path = f"{funcconf.drive_data_raw}People/"  # Source database path
so_file = "People_test_masterfile.sqlite"  # Source database
re_path = f"{funcconf.drive_data_results}People/"  # Results path
l_export: bool = False
l_mail: bool = False
l_record: bool = False

"""*****************************************************************************
OPEN THE DATABASES
*****************************************************************************"""
print("OPEN THE DATABASES")
funcfile.writelog("OPEN THE DATABASES")

# OPEN THE WORKING DATABASE
with sqlite3.connect(so_path + so_file) as so_conn:
    so_curs = so_conn.cursor()
funcfile.writelog("OPEN DATABASE: " + so_file)

# ATTACH DATA SOURCES
so_curs.execute("ATTACH DATABASE '" + so_path + "People.sqlite' AS 'PEOPLE'")
funcfile.writelog("%t ATTACH DATABASE: PEOPLE.SQLITE")
so_curs.execute(f"ATTACH DATABASE '{funcconf.drive_data_raw}People_payroll/People_payroll.sqlite' AS 'PAYROLL'")
funcfile.writelog("%t ATTACH DATABASE: PEOPLE.SQLITE")

""" ****************************************************************************
BEGIN OF SCRIPT
*****************************************************************************"""
print("BEGIN OF SCRIPT")
funcfile.writelog("BEGIN OF SCRIPT")

# OBTAIN LIST OF LONG SERVICE AWARD DATES
# BUILD THE CURRENT ELEMENT LIST
print("Obtain long service awards...")
sr_file = "A001_test"
s_sql = "CREATE TABLE " + sr_file + " AS " + """
Select Distinct
    per.employee_number,
    per.full_name,
    peevf.EFFECTIVE_START_DATE As PEEVF_START,
    peevf.EFFECTIVE_END_DATE As PEEVF_END,
    Upper(petf.element_name) As ELEMENT_NAME,
    Upper(pivf.name) As INPUT_VALUE,
    Upper(peevf.screen_entry_value) As ENTRY_VALUE
From
    PAYROLL.PAY_ELEMENT_ENTRIES_F_CURR peef,
    PEOPLE.PER_ALL_PEOPLE_F per,
    PEOPLE.PER_ALL_ASSIGNMENTS_F paaf,
    PAYROLL.PAY_ELEMENT_TYPES_F petf,
    PAYROLL.PAY_ELEMENT_ENTRY_VALUES_F_CURR peevf,
    PAYROLL.PAY_INPUT_VALUES_F pivf
Where
    per.person_id = paaf.person_id And
    paaf.assignment_id = peef.assignment_id And
    peef.element_type_id = petf.element_type_id And
    peevf.element_entry_id = peef.element_entry_id And
    pivf.element_type_id = petf.element_type_id And
    pivf.input_value_id = peevf.input_value_id And
    paaf.primary_flag = 'Y' And
    peevf.screen_entry_value > 0 And
    --Upper(petf.element_name) Like 'NWU LONG SERVICE AWARD' And
    Upper(petf.element_name) Like 'NWU ALLOWANCE NRF' And
    Date('%TODAY%') Between peef.effective_start_date And peef.effective_end_date And
    Date('%TODAY%') Between per.effective_start_date And per.effective_end_date And
    Date('%TODAY%') Between paaf.effective_start_date And paaf.effective_end_date And
    Date('%TODAY%') Between petf.effective_start_date And petf.effective_end_date
Order By
    per.employee_number,
    peevf_start    
;"""
"""
    per.employee_number = '31228550'
"""
"""
    Upper(pivf.name) Like('%HOUR%')
"""
"""
    Upper(petf.element_name) Like 'NWU LONG SERVICE AWARD' And
    Date('%TODAY%') Between peef.effective_start_date And peef.effective_end_date And
    Date('%TODAY%') Between per.effective_start_date And per.effective_end_date And
    Date('%TODAY%') Between paaf.effective_start_date And paaf.effective_end_date And
    Date('%TODAY%') Between petf.effective_start_date And petf.effective_end_date And
"""
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
s_sql = s_sql.replace("%TODAY%", funcdatn.get_today_date())
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: " + sr_file)
print("Export findings...")
sr_filet = sr_file
sx_path = re_path + funcdatn.get_current_year() + "/"
sx_file = "People_payroll_hours_"
sx_filet = sx_file + funcdatn.get_today_date_file()
s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)
funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_file, s_head)
funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_filet, s_head)
funcfile.writelog("%t EXPORT DATA: " + sx_path + sx_file)

""" ****************************************************************************
END OF SCRIPT
*****************************************************************************"""
print("END OF SCRIPT")
funcfile.writelog("END OF SCRIPT")

# CLOSE THE WORKING DATABASE
so_conn.close()

# CLOSE THE LOG
funcfile.writelog("------------------------------------------")
funcfile.writelog("COMPLETED: C001_PEOPLE_TEST_MASTERFILE_DEV")
