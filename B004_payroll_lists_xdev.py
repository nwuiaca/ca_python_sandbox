"""
Script to build standard VSS lists
Created on: 01 Mar 2018
Copyright: Albert J v Rensburg
"""

# IMPORT PYTHON MODULES
import csv
import datetime
import sqlite3

# IMPORT OWN MODULES
from _my_modules import funcconf
from _my_modules import funcdatn
from _my_modules import funccsv
from _my_modules import funcfile
from _my_modules import funcpayroll
from _my_modules import funcsms
from _my_modules import funcsys
from _my_modules import funcoracle

# OPEN THE LOG FILE
funcfile.writelog("Now")
funcfile.writelog("SCRIPT: B004_PAYROLL_LISTS_DEV")
funcfile.writelog("------------------------------")
print("----------------------")
print("B004_PAYROLL_LISTS_DEV")
print("----------------------")

# Declare variables
so_path = f"{funcconf.drive_data_raw}People_payroll/"  # Source database path
so_file = "People_payroll.sqlite"  # Source database
re_path = f"{funcconf.drive_data_results}People/People/"  # Results
ed_path = f"{funcconf.drive_system}_external_data/"
l_debug = True

# OPEN THE SOURCE FILE
with sqlite3.connect(so_path + so_file) as so_conn:
    so_curs = so_conn.cursor()
funcfile.writelog("OPEN DATABASE: " + so_file)

# ATTACH DATA SOURCES
so_curs.execute(f"ATTACH DATABASE '{funcconf.drive_data_raw}People/People.sqlite' AS 'PEOPLE'")
funcfile.writelog("%t ATTACH DATABASE: PEOPLE.SQLITE")

# For which year
s_year: str = 'curr'
if s_year == 'curr':
    year_start: str = funcdatn.get_current_year_begin()
    year_end: str = funcdatn.get_current_year_end()
    calc_today = funcdatn.get_today_date()
    calc_monthend = funcdatn.get_previous_month_end()
    s_table_name: str = 'Payroll history curr'
elif s_year == 'prev':
    year_start: str = funcdatn.get_previous_year_begin()
    year_end: str = funcdatn.get_previous_year_end()
    calc_today = year_end
    calc_monthend = year_end
    s_table_name: str = 'Payroll history prev'
else:
    year_start: str = s_year + '-01-01'
    year_end: str = s_year + '-12-31'
    calc_today = year_end
    calc_monthend = year_end
    s_table_name: str = 'Payroll history ' + s_year

"""*****************************************************************************
BEGIN
*****************************************************************************"""

# Build the current element list *******************************************
print("Build the current element list...")
sr_file = "X000aa_element_type_list"
s_sql = "CREATE TABLE " + sr_file + " AS " + """
Select
    petf.ELEMENT_TYPE_ID,
    petf.EFFECTIVE_START_DATE,
    petf.EFFECTIVE_END_DATE,
    petf.ELEMENT_NAME,
    petf.REPORTING_NAME,
    petf.DESCRIPTION As ELEMENT_DESCRIPTION,
    petf.CLASSIFICATION_ID,
    pect.CLASSIFICATION_NAME,
    pect.DESCRIPTION As CLASSIFICATION_DESCRIPTION
From
    PAY_ELEMENT_TYPES_F petf Left Join
    PAY_ELEMENT_CLASSIFICATIONS_TL pect On pect.CLASSIFICATION_ID = petf.CLASSIFICATION_ID
;"""
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: " + sr_file)
if funcconf.l_mess_project:
    i = funcsys.tablerowcount(so_curs, sr_file)
    funcsms.send_telegram("", "administrator", "<b>" + str(i) + "</b> Elements")

# NOTE TO SELF
# The following code used in people test leave code invalid
# It show the relationship between the elements file
# Do not delete, but save somewhere else
# -----------------------------

# BUILD THE CURRENT ELEMENT LIST
print("Testing element list...")
sr_file = "A_test_element_list"
s_sql = "CREATE TABLE " + sr_file + " AS " + """
Select Distinct
    per.employee_number,
    per.full_name,
    Upper(petf.element_name) As ELEMENT_NAME,
    Upper(pivf.name) As INPUT_VALUE,
    peevf.screen_entry_value
From
    PAY_ELEMENT_ENTRIES_F_CURR peef,
    PEOPLE.PER_ALL_PEOPLE_F per,
    PEOPLE.PER_ALL_ASSIGNMENTS_F paaf,
    PAY_ELEMENT_TYPES_F petf,
    PAY_ELEMENT_ENTRY_VALUES_F_CURR peevf,
    PAY_INPUT_VALUES_F pivf
Where
    per.person_id = paaf.person_id And
    paaf.assignment_id = peef.assignment_id And
    peef.element_type_id = petf.element_type_id And
    peevf.element_entry_id = peef.element_entry_id And
    pivf.element_type_id = petf.element_type_id And
    pivf.input_value_id = peevf.input_value_id And
    Date('%TODAY%') Between peef.effective_start_date And peef.effective_end_date And
    Date('%TODAY%') Between per.effective_start_date And per.effective_end_date And
    Date('%TODAY%') Between paaf.effective_start_date And paaf.effective_end_date And
    Date('%TODAY%') Between petf.effective_start_date And petf.effective_end_date And
    paaf.primary_flag = 'Y' And
    peevf.screen_entry_value > 0
Order By
    per.employee_number    
;"""
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
s_sql = s_sql.replace("%TODAY%",funcdatn.get_today_date())
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: " + sr_file)

"""
Code from: http://josephbijoy.blogspot.com/search?q=peevf.screen_entry_value
Select
    per.employee_number,
    per.full_name,
    petf.element_name,
    peevf.screen_entry_value
From
    pay_element_entries_f peef,
    per_all_people_f per,
    per_all_assignments_f paaf,
    pay_element_types_f petf,
    pay_element_entry_values_f peevf,
    pay_input_values_f pivf
Where
    per.person_id = paaf.person_id And
    paaf.assignment_id = peef.assignment_id And
    peef.element_type_id = petf.element_type_id And
    peevf.element_entry_id = peef.element_entry_id And
    pivf.element_type_id = petf.element_type_id And
    pivf.input_value_id = peevf.input_value_id And
    paaf.primary_flag = 'Y' And
    To_Date('01-AUG-2013') Between peef.effective_start_date And peef.effective_end_date And
    To_Date('01-AUG-2013') Between per.effective_start_date And per.effective_end_date And
    To_Date('01-AUG-2013') Between paaf.effective_start_date And paaf.effective_end_date And
    To_Date('01-AUG-2013') Between petf.effective_start_date And petf.effective_end_date And
    Upper(petf.element_name) = 'NWU Long Service Award' And
    peevf.effective_start_date) = 'N' And
    peevf.screen_entry_value > 0 And
    Upper(pivf.name) = 'AMOUNT'
"""

# Extract the NWU TOTAL PACKAGE element for export *********************
print("Extract the nwu total package element...")
sr_file = "X001aa_element_package_curr"
s_sql = "CREATE TABLE " + sr_file + " AS " + """
SELECT
  X000aa_element_list_curr.ASSIGNMENT_ID,
  X000aa_element_list_curr.EFFECTIVE_START_DATE,
  X000aa_element_list_curr.INPUT_VALUE_ID,
  X000aa_element_list_curr.SCREEN_ENTRY_VALUE,
  X000aa_element_list_curr.ELEMENT_NAME,
  SUBSTR(PEOPLE.PER_ALL_ASSIGNMENTS_F.ASSIGNMENT_NUMBER,1,8) AS EMPL_NUMB
FROM
  X000aa_element_list_curr
  LEFT JOIN PEOPLE.PER_ALL_ASSIGNMENTS_F ON PEOPLE.PER_ALL_ASSIGNMENTS_F.ASSIGNMENT_ID = X000aa_element_list_curr.ASSIGNMENT_ID AND
    PEOPLE.PER_ALL_ASSIGNMENTS_F.EFFECTIVE_START_DATE <= Date('%TODAY%') AND
    PEOPLE.PER_ALL_ASSIGNMENTS_F.EFFECTIVE_END_DATE >= Date('%TODAY%')
WHERE
  X000aa_element_list_curr.INPUT_VALUE_ID = 6881 AND
  X000aa_element_list_curr.EFFECTIVE_START_DATE <= Date('%TODAY%') AND
  X000aa_element_list_curr.EFFECTIVE_END_DATE >= Date('%TODAY%')
;"""
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
s_sql = s_sql.replace("%TODAY%", funcdatn.get_today_date())
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: " + sr_file)

"""
# BUILD GROUP INSURANCE INFORMATION SPOUSE
s_date = funcdatn.get_today_date()
i_records = funcpayroll.payroll_element_screen_value(
    so_conn,
    'X000_NWU_ALLOWANCE_FUNCTIONAL',
    'nwu allowance functional',
    'amount',
    s_date)
if l_debug:
    print(i_records)
"""

"""
# BUILD GROUP INSURANCE INFORMATION SPOUSE
s_date = funcdatn.get_today_date()
i_records = funcpayroll.payroll_element_screen_value(
    so_conn,
    'X000_NWU_ALLOWANCE_FUNCTIONAL_TYPE',
    'nwu allowance functional',
    'allowance type',
    s_date)
if l_debug:
    print(i_records)
"""

"""
SQL Query to get payslip of an employee
  SELECT ppa.date_earned,
         per.full_name,
         per.employee_number,
         NVL (pet.reporting_name, pet.element_name),
         piv.NAME,
         prrv.result_value,
         ptp.period_name
    FROM pay_payroll_actions ppa,
         pay_assignment_actions pac,
         per_all_assignments_f ass,
         per_all_people_f per,
         pay_run_results prr,
         pay_element_types_f pet,
         pay_input_values_f piv,
         pay_run_result_values prrv,
         per_time_periods_v ptp
   WHERE     ppa.payroll_action_id = pac.payroll_action_id
         AND pac.assignment_id = ass.assignment_id
         AND ass.effective_end_date = TO_DATE ('12/31/4712', 'MM/DD/RRRR')
         AND ass.person_id = per.person_id
         AND per.effective_end_date = TO_DATE ('12/31/4712', 'MM/DD/RRRR')
         AND pac.assignment_action_id = prr.assignment_action_id
         AND prr.element_type_id = pet.element_type_id
         AND prr.run_result_id = prrv.run_result_id
         AND pet.element_type_id = piv.element_type_id
         AND piv.input_value_id = prrv.input_value_id
         AND ppa.time_period_id = ptp.time_period_id
         AND pet.element_name = 'Basic Salary'
         --     AND piv.NAME = 'Pay Value'
         AND per.employee_number = '91314'
         AND ptp.period_name LIKE '6 2008 Calendar Month'
ORDER BY 1;
"""

"""*************************************************************************
END
*************************************************************************"""

# CLOSE THE CONNECTION
so_conn.close()

# CLOSE THE LOG
funcfile.writelog("-----------------------------")
funcfile.writelog("COMPLETED: B003_VSS_LISTS_DEV")
