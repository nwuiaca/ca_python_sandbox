"""
Script to test PEOPLE conflict of interest
Created on: 8 Apr 2019
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
from _my_modules import funcstr
from _my_modules import funcstat

""" INDEX **********************************************************************
ENVIRONMENT
OPEN THE DATABASES
BEGIN OF SCRIPT
BUILD CONFLICT MASTER TABLES
BUILD ANNUAL TABLES
BUILD DASHBOARD TABLES
BANK NUMBER MASTER FILES
CHILD SUPPORT AND ADVANCES MASTER FILE
TEST EMPLOYEE VENDOR SHARE BANK ACCOUNT (V1.1.2)
TEST EMPLOYEE VENDOR SHARE EMAIL ADDRESS (V2.0.3)
TEST EMPLOYEE NO DECLARATION (V2.0.5)
TEST DECLARATION PENDING (V2.0.6)
TEST CONFLICTING TRANSACTIONS MASTER TABLES
TEST ACTIVE CIPC DIRECTOR ACTIVE VENDOR NO DECLARATION (V2.0.6)
TEST ACTIVE CIPC DIRECTOR LIST (V2.0.6)
END OF SCRIPT
*****************************************************************************"""


def people_test_conflict():
    """
    Script to test PEOPLE conflict of interest
    :return:
    """

    """*****************************************************************************
    ENVIRONMENT
    *****************************************************************************"""

    # DECLARE VARIABLES
    source_database_path: str = f"{funcconf.drive_data_raw}People_conflict/"  # Source database path
    source_database_name: str = "People_conflict.sqlite"  # Source database
    source_database: str = source_database_path + source_database_name
    external_data_path: str = f"{funcconf.drive_system}_external_data/"  # external data path
    results_path: str = f"{funcconf.drive_data_results}People/" + funcdatn.get_current_year() + "/"  # Results path
    l_debug: bool = False
    l_export: bool = False
    l_mail: bool = False
    l_mess: bool = True
    l_record: bool = True
    target_declare_date: str = funcdatn.get_current_year() + "-03-31"

    # OPEN THE SCRIPT LOG FILE
    if l_debug:
        print("-----------------------------")
        print("C002_PEOPLE_TEST_CONFLICT_DEV")
        print("-----------------------------")
    funcfile.writelog("Now")
    funcfile.writelog("SCRIPT: C002_PEOPLE_TEST_CONFLICT_DEV")
    funcfile.writelog("-------------------------------------")

    if l_mess:
        funcsms.send_telegram('', 'administrator', '<b>C002 People conflict interest tests</b>')

    """*****************************************************************************
    OPEN THE DATABASES
    *****************************************************************************"""
    if l_debug:
        print("OPEN THE DATABASES")
    funcfile.writelog("OPEN THE DATABASES")

    # OPEN THE WORKING DATABASE
    with sqlite3.connect(source_database_path+source_database_name) as sqlite_connection:
        sqlite_cursor = sqlite_connection.cursor()
    funcfile.writelog("OPEN DATABASE: " + source_database_name)

    # ATTACH DATA SOURCES
    sqlite_cursor.execute(f"ATTACH DATABASE '{funcconf.drive_data_raw}People/People.sqlite' AS 'PEOPLE'")
    funcfile.writelog("%t ATTACH DATABASE: PEOPLE.SQLITE")
    sqlite_cursor.execute(f"ATTACH DATABASE '{funcconf.drive_data_raw}People_payroll/People_payroll.sqlite' AS 'PAYROLL'")
    funcfile.writelog("%t ATTACH DATABASE: PEOPLE.SQLITE")
    sqlite_cursor.execute(f"ATTACH DATABASE '{funcconf.drive_data_raw}Kfs/Kfs.sqlite' AS 'KFS'")
    funcfile.writelog("%t ATTACH DATABASE: KFS.SQLITE")
    sqlite_cursor.execute(f"ATTACH DATABASE '{funcconf.drive_data_raw}Kfs/Kfs_curr.sqlite' AS 'KFSCURR'")
    funcfile.writelog("%t ATTACH DATABASE: KFS_CURR.SQLITE")
    sqlite_cursor.execute(f"ATTACH DATABASE '{funcconf.drive_data_raw}Vss/Vss_curr.sqlite' AS 'VSSCURR'")
    funcfile.writelog("%t ATTACH DATABASE: VSS_CURR.SQLITE")

    """ ****************************************************************************
    BEGIN OF SCRIPT
    *****************************************************************************"""
    if l_debug:
        print("BEGIN OF SCRIPT")
    funcfile.writelog("BEGIN OF SCRIPT")

    # Read the list person types to exclude used in various tests
    exclude_person_type = funcstat.stat_tuple(sqlite_cursor,
                                              "KFS.X000_Own_kfs_lookups",
                                              "LOOKUP_CODE",
                                              "LOOKUP='EXCLUDE CONFLICT PERSON TYPE'")
    if l_debug:
        print('List of person types to exclude:')
        print(exclude_person_type)

    """ ****************************************************************************
    BUILD CONFLICT MASTER TABLES
    *****************************************************************************"""
    if l_debug:
        print("CONFLICT MASTER TABLES")
    funcfile.writelog("CONFLICT MASTER TABLES")

    # BUILD DECLARATIONS MASTER TABLE
    if l_debug:
        print("Build declarations master table...")
    sr_file = "X000_declarations_all"
    s_sql = "CREATE TABLE "+sr_file+" AS " + """
    Select
        COI_DECLARATIONS.DECLARATION_ID,
        COI_DECLARATIONS.EMPLOYEE_NUMBER,
        COI_DECLARATIONS.DECLARATION_DATE,
        COI_DECLARATIONS.UNDERSTAND_POLICY_FLAG,
        COI_DECLARATIONS.INTEREST_TO_DECLARE_FLAG,
        COI_DECLARATIONS.FULL_DISCLOSURE_FLAG,
        PEOPLE.HR_LOOKUPS.MEANING AS STATUS,
        COI_DECLARATIONS.LINE_MANAGER,
        COI_DECLARATIONS.REJECTION_REASON,
        COI_DECLARATIONS.CREATION_DATE,
        COI_DECLARATIONS.AUDIT_USER,
        COI_DECLARATIONS.LAST_UPDATE_DATE,
        COI_DECLARATIONS.LAST_UPDATED_BY,
        COI_DECLARATIONS.EXTERNAL_REFERENCE,
        COI_DECLARATIONS.OBO_DATE,
        COI_DECLARATIONS.FORM_DATE,
        COI_DECLARATIONS.PRIVACY_FLAG
    From
      COI_DECLARATIONS
      Left Join PEOPLE.HR_LOOKUPS ON PEOPLE.HR_LOOKUPS.LOOKUP_CODE = COI_DECLARATIONS.STATUS_ID And
          HR_LOOKUPS.LOOKUP_TYPE = "NWU_COI_STATUS" And
          PEOPLE.HR_LOOKUPS.START_DATE_ACTIVE <= COI_DECLARATIONS.DECLARATION_DATE And
          PEOPLE.HR_LOOKUPS.END_DATE_ACTIVE >= COI_DECLARATIONS.DECLARATION_DATE
    Order By
      COI_DECLARATIONS.EMPLOYEE_NUMBER,
      COI_DECLARATIONS.DECLARATION_DATE
    ;"""
    sqlite_cursor.execute("DROP TABLE IF EXISTS "+sr_file)
    sqlite_cursor.execute(s_sql)
    sqlite_connection.commit()
    funcfile.writelog("%t BUILD TABLE: "+sr_file)

    # BUILD INTERESTS MASTER TABLE
    if l_debug:
        print("Build interests master table...")
    sr_file = "X000_interests_all"
    s_sql = "CREATE TABLE "+sr_file+" AS " + """
    Select
        COI_INTERESTS.INTEREST_ID,
        COI_INTERESTS.DECLARATION_ID,
        COI_DECLARATIONS.EMPLOYEE_NUMBER,
        COI_DECLARATIONS.DECLARATION_DATE,
        COI_INTERESTS.CONFLICT_TYPE_ID,
        HR_LOOKUPS2.MEANING AS CONFLICT_TYPE,
        COI_INTERESTS.INTEREST_TYPE_ID,
        HR_LOOKUPS1.MEANING AS INTEREST_TYPE,
        COI_INTERESTS.STATUS_ID,
        HR_LOOKUPS3.MEANING AS INTEREST_STATUS,
        COI_INTERESTS.PERC_SHARE_INTEREST,
        COI_INTERESTS.ENTITY_NAME,
        COI_INTERESTS.ENTITY_REGISTRATION_NUMBER,
        COI_INTERESTS.OFFICE_ADDRESS,
        COI_INTERESTS.DESCRIPTION,
        COI_INTERESTS.DIR_APPOINTMENT_DATE,
        COI_INTERESTS.LINE_MANAGER,
        COI_INTERESTS.NEXT_LINE_MANAGER,
        COI_INTERESTS.INDUSTRY_CLASS_ID,
        PEOPLE.HR_LOOKUPS.MEANING AS INDUSTRY_TYPE,
        COI_INTERESTS.TASK_PERF_AGREEMENT,
        COI_INTERESTS.MITIGATION_AGREEMENT,
        COI_INTERESTS.REJECTION_REASON,
        COI_INTERESTS.CREATION_DATE,
        COI_INTERESTS.AUDIT_USER,
        COI_INTERESTS.LAST_UPDATE_DATE,
        COI_INTERESTS.LAST_UPDATED_BY,
        COI_INTERESTS.EXTERNAL_REFERENCE,
        COI_INTERESTS.VENDOR_NUMBER,
        COI_INTERESTS.VENDOR_NAME,
        COI_INTERESTS.CC_NUMBER,
        COI_INTERESTS.VAT_NUMBER
    From
      COI_INTERESTS
      Left Join COI_DECLARATIONS ON COI_DECLARATIONS.DECLARATION_ID = COI_INTERESTS.DECLARATION_ID
      Left Join PEOPLE.HR_LOOKUPS ON PEOPLE.HR_LOOKUPS.LOOKUP_CODE = COI_INTERESTS.INTEREST_TYPE_ID AND
          HR_LOOKUPS.LOOKUP_TYPE = "NWU_COI_INDUSTRY_CLASS"
      Left Join PEOPLE.HR_LOOKUPS HR_LOOKUPS1 ON HR_LOOKUPS1.LOOKUP_CODE = COI_INTERESTS.INTEREST_TYPE_ID AND
          HR_LOOKUPS1.LOOKUP_TYPE = "NWU_COI_INTEREST_TYPES"
      Left Join PEOPLE.HR_LOOKUPS HR_LOOKUPS2 ON HR_LOOKUPS2.LOOKUP_CODE = COI_INTERESTS.CONFLICT_TYPE_ID AND
          HR_LOOKUPS2.LOOKUP_TYPE = "NWU_COI_CONFLICT_TYPE"
      Left Join PEOPLE.HR_LOOKUPS HR_LOOKUPS3 ON HR_LOOKUPS3.LOOKUP_CODE = COI_INTERESTS.STATUS_ID AND
          HR_LOOKUPS3.LOOKUP_TYPE = "NWU_COI_STATUS"
    ;"""
    sqlite_cursor.execute("DROP TABLE IF EXISTS "+sr_file)
    sqlite_cursor.execute(s_sql)
    sqlite_connection.commit()
    funcfile.writelog("%t BUILD TABLE: "+sr_file)

    """ ****************************************************************************
    BUILD ANNUAL TABLES
    *****************************************************************************"""
    if l_debug:
        print("CURRENT YEAR DECLARATIONS")
    funcfile.writelog("CURRENT YEAR DECLARATIONS")

    # BUILD CURRENT YEAR DECLARATIONS
    if l_debug:
        print("Build current declarations...")
    sr_file = "X001_declarations_curr"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        DECL.DECLARATION_ID,
        DECL.EMPLOYEE_NUMBER AS EMPLOYEE,
        PEOP.LAST_NAME AS EMP_SURNAME,
        PEOP.FULL_NAME AS EMP_FULL,
        ACTP.name_address AS EMP_NAME,
        ACTP.phone_work AS EMP_PHONE_WORK,
        ACTP.phone_work AS EMP_PHONE_HOME,    
        ACTP.phone_mobile AS EMP_PHONE_MOBILE,
        ACTP.address_sars AS EMP_ADD_SARS,
        ACTP.address_post AS EMP_ADD_POST,
        CASE
        WHEN ACTP.employee_number IS NULL THEN 'N'
        ELSE 'Y'
        END AS EMP_ACTIVE,    
        ACTP.email_address AS EMP_MAIL,
        ACTP.position_name EMP_POSITION,    
        DECL.DECLARATION_DATE,
        DECL.UNDERSTAND_POLICY_FLAG,
        DECL.INTEREST_TO_DECLARE_FLAG,
        DECL.FULL_DISCLOSURE_FLAG,
        DECL.STATUS,
        DECL.LINE_MANAGER,
        MANA.name_last AS MAN_SURNAME,
        MANA.name_address AS MAN_NAME,
        MANA.address_sars AS MAN_ADD_SARS,
        MANA.address_post AS MAN_ADD_POST,
        CASE
        WHEN MANA.employee_number IS NULL THEN 'N'
        ELSE 'Y'
        END AS MAN_ACTIVE,      
        MANA.email_address AS MAN_MAIL,
        MANA.position_name AS MAN_POSITION,    
        DECL.REJECTION_REASON,
        DECL.CREATION_DATE,
        DECL.AUDIT_USER,
        DECL.LAST_UPDATE_DATE,
        DECL.LAST_UPDATED_BY,
        DECL.EXTERNAL_REFERENCE,
        ACTP.supervisor_number AS SUPERVISOR,
        SUPE.name_last AS SUP_SURNAME,
        SUPE.name_address AS SUP_NAME,
        CASE
        WHEN SUPE.employee_number IS NULL THEN 'N'
        ELSE 'Y'
        END AS SUP_ACTIVE,      
        SUPE.email_address AS SUP_MAIL,
        SUPE.position_name AS SUP_POSITION
    From
        X000_declarations_all DECL Left Join
        PEOPLE.PER_ALL_PEOPLE_F PEOP ON PEOP.EMPLOYEE_NUMBER = DECL.EMPLOYEE_NUMBER
         AND DATE(DECL.LAST_UPDATE_DATE) BETWEEN PEOP.EFFECTIVE_START_DATE AND PEOP.EFFECTIVE_END_DATE Left Join 
        PEOPLE.X000_PEOPLE ACTP ON ACTP.employee_number = DECL.EMPLOYEE_NUMBER Left join
        PEOPLE.X000_PEOPLE MANA ON MANA.employee_number = DECL.LINE_MANAGER Left join
        PEOPLE.X000_PEOPLE SUPE ON SUPE.employee_number = ACTP.supervisor_number        
    Where
        DECL.DECLARATION_DATE >= Date("%CYEARB%") AND
        DECL.DECLARATION_DATE <= Date("%CYEARE%")
    Order By
        PEOP.FULL_NAME,
        DECL.LAST_UPDATE_DATE
    ;"""
    sqlite_cursor.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sqlp = s_sql
    s_sql = s_sql.replace("%CYEARB%", funcdatn.get_current_year_begin())
    s_sql = s_sql.replace("%CYEARE%", funcdatn.get_current_year_end())
    sqlite_cursor.execute(s_sql)
    sqlite_connection.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD PREVIOUS YEAR DECLARATIONS
    if l_debug:
        print("Build previous declarations...")
    sr_file = "X001_declarations_prev"
    s_sql = s_sqlp
    s_sql = s_sql.replace("X001_declarations_curr", sr_file)
    s_sql = s_sql.replace("X002_PEOPLE_CURR_YEAR", "X002_PEOPLE_PREV_YEAR")
    s_sql = s_sql.replace("%CYEARB%", funcdatn.get_previous_year_begin())
    s_sql = s_sql.replace("%CYEARE%", funcdatn.get_previous_year_end())
    sqlite_cursor.execute("DROP TABLE IF EXISTS "+sr_file)
    sqlite_cursor.execute(s_sql)
    sqlite_connection.commit()
    funcfile.writelog("%t BUILD TABLE: "+sr_file)

    # BUILD THE CURRENT YEAR INTERESTS
    if l_debug:
        print("Build current interests...")
    sr_file = "X002_interests_curr"
    s_sql = "CREATE TABLE "+sr_file+" AS " + """
    Select
        X000_interests_all.DECLARATION_ID,
        X000_interests_all.INTEREST_ID,
        X000_interests_all.EMPLOYEE_NUMBER,
        EMPLOYEE.LAST_NAME AS EMP_SURNAME,
        EMPLOYEE.NAME_ADDR AS EMP_NAME,
        EMPLOYEE.ADDRESS_SARS AS EMP_ADD_SARS,
        EMPLOYEE.ADDRESS_POST AS EMP_ADD_POST,
        EMPLOYEE.EMP_ACTIVE,    
        EMPLOYEE.EMAIL_ADDRESS AS EMP_MAIL,
        EMPLOYEE.POSITION_FULL AS EMP_POSITION,    
        X000_interests_all.DECLARATION_DATE,
        X000_interests_all.CONFLICT_TYPE,
        X000_interests_all.INTEREST_TYPE,
        X000_interests_all.PERC_SHARE_INTEREST,
        X000_interests_all.INDUSTRY_TYPE,
        X000_interests_all.ENTITY_NAME,
        X000_interests_all.ENTITY_REGISTRATION_NUMBER,
        X000_interests_all.OFFICE_ADDRESS,
        X000_interests_all.DESCRIPTION,
        X000_interests_all.INTEREST_STATUS,
        X000_interests_all.LINE_MANAGER,
        MANAGER.LAST_NAME AS MAN_SURNAME,
        MANAGER.NAME_ADDR AS MAN_NAME,
        MANAGER.ADDRESS_SARS AS MAN_ADD_SARS,
        MANAGER.ADDRESS_POST AS MAN_ADD_POST,
        MANAGER.EMP_ACTIVE AS MAN_ACTIVE,    
        MANAGER.EMAIL_ADDRESS AS MAN_MAIL,
        MANAGER.POSITION_FULL AS MAN_POSITION,    
        X000_interests_all.NEXT_LINE_MANAGER,
        NEXTMANAGER.LAST_NAME AS NMAN_SURNAME,
        NEXTMANAGER.NAME_ADDR AS NMAN_NAME,
        NEXTMANAGER.ADDRESS_SARS AS NMAN_ADD_SARS,
        NEXTMANAGER.ADDRESS_POST AS NMAN_ADD_POST,
        NEXTMANAGER.EMP_ACTIVE AS NMAN_ACTIVE,    
        NEXTMANAGER.EMAIL_ADDRESS AS NMAN_MAIL,
        NEXTMANAGER.POSITION_FULL AS NMAN_POSITION,    
        X000_interests_all.DIR_APPOINTMENT_DATE,
        X000_interests_all.TASK_PERF_AGREEMENT,
        X000_interests_all.MITIGATION_AGREEMENT,
        X000_interests_all.REJECTION_REASON,
        X000_interests_all.CREATION_DATE,
        X000_interests_all.AUDIT_USER,
        X000_interests_all.LAST_UPDATE_DATE,
        X000_interests_all.LAST_UPDATED_BY,
        X000_interests_all.EXTERNAL_REFERENCE,
        EMPLOYEE.SUPERVISOR,
        SUPERVISOR.LAST_NAME AS SUP_SURNAME,
        SUPERVISOR.NAME_ADDR AS SUP_NAME,
        SUPERVISOR.EMP_ACTIVE AS SUP_ACTIVE,    
        SUPERVISOR.EMAIL_ADDRESS AS SUP_MAIL,
        SUPERVISOR.POSITION_FULL AS SUP_POSITION
    From
        X001_declarations_curr
        Inner Join X000_interests_all ON X000_interests_all.DECLARATION_ID = X001_declarations_curr.DECLARATION_ID
        Left Join PEOPLE.X002_PEOPLE_CURR_YEAR EMPLOYEE ON EMPLOYEE.EMPLOYEE_NUMBER = X000_interests_all.EMPLOYEE_NUMBER
        Left Join PEOPLE.X002_PEOPLE_CURR_YEAR MANAGER ON MANAGER.EMPLOYEE_NUMBER = X000_interests_all.LINE_MANAGER
        Left Join PEOPLE.X002_PEOPLE_CURR_YEAR NEXTMANAGER ON NEXTMANAGER.EMPLOYEE_NUMBER = X000_interests_all.NEXT_LINE_MANAGER
        Left Join PEOPLE.X002_PEOPLE_CURR_YEAR SUPERVISOR ON SUPERVISOR.EMPLOYEE_NUMBER = EMPLOYEE.SUPERVISOR
    Where
        X001_declarations_curr.INTEREST_TO_DECLARE_FLAG == "Y"
    Order By
        EMPLOYEE.LAST_NAME,
        EMPLOYEE.NAME_ADDR,
        X000_interests_all.LAST_UPDATE_DATE
    """
    sqlite_cursor.execute("DROP TABLE IF EXISTS "+sr_file)
    s_sqlp = s_sql
    sqlite_cursor.execute(s_sql)
    funcfile.writelog("%t BUILD TABLE: "+sr_file)

    # BUILD PREVIOUS YEAR INTERESTS
    if l_debug:
        print("Build previous interests...")
    sr_file = "X002_interests_prev"
    s_sql = s_sqlp
    s_sql = s_sql.replace("X002_interests_curr", sr_file)
    s_sql = s_sql.replace("X001_declarations_curr", "X001_declarations_prev")
    s_sql = s_sql.replace("X002_PEOPLE_CURR_YEAR", "X002_PEOPLE_PREV_YEAR")
    sqlite_cursor.execute("DROP TABLE IF EXISTS "+sr_file)
    sqlite_cursor.execute(s_sql)
    sqlite_connection.commit()
    funcfile.writelog("%t BUILD TABLE: "+sr_file)

    """ ****************************************************************************
    BUILD DASHBOARD TABLES
    *****************************************************************************"""
    if l_debug:
        print("BUILD DASHBOARD TABLES")
    funcfile.writelog("BUILD DASHBOARD TABLES")

    # BUILD TABLE WITH PAYROLL FOREIGN PAYMENTS FOR THE PREVIOUS MONTH
    if l_debug:
        print("Obtain master list of foreign employee payments...")
    sr_file = "X003_foreign_payments"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        pfp.EMPLOYEE_NUMBER,
        Max(pfp.EFFECTIVE_DATE) As LAST_PAYMENT_DATE,
        Count(pfp.RUN_RESULT_ID) As COUNT_PAYMENTS
    From
        PAYROLL.X000aa_payroll_history_%PERIOD% pfp
    Where
        pfp.ELEMENT_NAME Like 'NWU Foreign Payment%' And
        pfp.EFFECTIVE_DATE Like '%PREVMONTH%%'
    Group By
        pfp.EMPLOYEE_NUMBER    
    ;"""
    sqlite_cursor.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = s_sql.replace("%PREVMONTH%", funcdatn.get_previous_month_end()[0:7])
    if funcdatn.get_previous_month_end()[0:4] == funcdatn.get_previous_year:
        s_sql = s_sql.replace("%PERIOD%", 'prev')
    else:
        s_sql = s_sql.replace("%PERIOD%", 'curr')
    if l_debug:
        print(s_sql)
    sqlite_cursor.execute(s_sql)
    funcfile.writelog("%t BUILD TABLE: " + sr_file)
    if l_debug:
        sqlite_connection.commit()

    # BUILD TABLE WITH UIF PAYMENTS FOR THE PREVIOUS MONTH
    if l_debug:
        print("Obtain master list of uif employee payments...")
    sr_file = "X003_uif_payments"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        pfp.EMPLOYEE_NUMBER,
        Max(pfp.EFFECTIVE_DATE) As LAST_PAYMENT_DATE,
        Count(pfp.RUN_RESULT_ID) As COUNT_PAYMENTS
    From
        PAYROLL.X000aa_payroll_history_%PERIOD% pfp
    Where
        pfp.ELEMENT_NAME Like 'ZA_UIF_Employer_Contribution%' And
        pfp.EFFECTIVE_DATE Like '%PREVMONTH%%'
    Group By
        pfp.EMPLOYEE_NUMBER    
    ;"""
    sqlite_cursor.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = s_sql.replace("%PREVMONTH%", funcdatn.get_previous_month_end()[0:7])
    if funcdatn.get_previous_month_end()[0:4] == funcdatn.get_previous_year:
        s_sql = s_sql.replace("%PERIOD%", 'prev')
    else:
        s_sql = s_sql.replace("%PERIOD%", 'curr')
    if l_debug:
        print(s_sql)
    sqlite_cursor.execute(s_sql)
    funcfile.writelog("%t BUILD TABLE: " + sr_file)
    if l_debug:
        sqlite_connection.commit()

    # BUILD CURRENT DECLARATION DASHBOARD PEOPLE
    if l_debug:
        print("Build current declaration dashboard people data...")
    sr_file = "X003aa_people_curr"
    s_sql = "CREATE TABLE " + sr_file + " AS " + f"""
    Select
        peop.employee_number AS EMPLOYEE,
        peop.name_address AS NAME,
        peop.gender AS GENDER,
        Upper(peop.race) As RACE,
        Upper(peop.corr_language) As LANGUAGE,
        Upper(peop.location) As LOCATION,
        Upper(peop.employee_category) As ACAD_SUPP,
        Upper(peop.faculty) As FACULTY,
        Upper(peop.division) As DIVISION,
        Upper(peop.assignment_category) As CATEGORY,
        Upper(peop.grade_calc) As POS_GRADE,
        Upper(peop.job_name) As JOB_NAME,
        Upper(peop.user_person_type) As PERSON_TYPE,
        CASE
            -- When fp.EMPLOYEE_NUMBER Is Not Null
            -- Then 'DECLARATION REQUIRED'
            When ui.EMPLOYEE_NUMBER Is Not Null
            Then 'DECLARATION REQUIRED'
            When Upper(peop.user_person_type) In {exclude_person_type}
            Then 'DECLARATION NOT REQUIRED'
            Else 'DECLARATION REQUIRED'
        END As STATUS,
        peop.employee_age As AGE,
        peop.supervisor_number AS SUPERVISOR,
        CASE
            When peop.date_started Is Null And peop.service_start_date Is Null Then peop.people_start_date
            When peop.date_started Is Null Then peop.service_start_date
            Else peop.date_started
        END As EMP_START,
        CASE
            When peop.service_end_date Is Not Null Then peop.service_end_date
            Else ''
        END As EMP_END,
        cont.CONTRACT_NUMBER,
        cont.PERSON_TYPE AS CONTRACT_PERSON_TYPE,
        cont.CONTRACT_CATEGORY,
        cont.CONTRACT_TYPE,
        Max(cont.CONTRACT_FROM) As CONTRACT_FROM,
        cont.CONTRACT_TO    
    From
        PEOPLE.X000_PEOPLE peop Left Join
        PEOPLE.X004_CONTRACT_CURR cont On cont.ASSIGNMENT_ID = peop.assignment_id Left Join
        X003_foreign_payments fp On fp.EMPLOYEE_NUMBER = peop.employee_number Left Join
        X003_uif_payments ui On ui.EMPLOYEE_NUMBER = peop.employee_number
    Group By
        peop.employee_number
    ;"""
    # Script if listed person types should be excluded from the data
    """
    Where
        Upper(peop.user_person_type) Not In {exclude_person_type}         
    """
    sqlite_cursor.execute("DROP TABLE IF EXISTS " + sr_file)
    sqlite_cursor.execute(s_sql)
    sqlite_connection.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD CURRENT DECLARATION DASHBOARD DECLARATION DATA
    if l_debug:
        print("Build current declaration dashboard unique declarations...")
    sr_file = "X003ab_declarations_curr"
    s_sql = "CREATE TABLE " + sr_file + " AS " + f"""
    Select
        DECLARE.DECLARATION_ID,
        DECLARE.EMPLOYEE,
        DECLARE.DECLARATION_DATE,
        Max(DECLARE.CREATION_DATE) As CREATION_DATE,
        Case
            When UNDERSTAND_POLICY_FLAG = 'N' Then 'NO'
            When UNDERSTAND_POLICY_FLAG = 'Y' Then 'YES'
            Else 'NA'
        End As UNDERSTAND_POLICY,
        Case
            When INTEREST_TO_DECLARE_FLAG = 'N' Then 'NO'
            When INTEREST_TO_DECLARE_FLAG = 'Y' Then 'YES'
            Else 'NA'
        End As INTERESTS_TO_DECLARE,
        Case
            When FULL_DISCLOSURE_FLAG = 'N' Then 'NO'
            When FULL_DISCLOSURE_FLAG = 'Y' Then 'YES'
            Else 'NA'
        End As FULL_DISCLOSURE,
        Upper(STATUS) AS DECLARE_STATUS,
        DECLARE.LINE_MANAGER,
        Upper(DECLARE.REJECTION_REASON) As REJECTION_REASON,
        Cast(Julianday('now') - Julianday(DECLARE.DECLARATION_DATE) As Int) As DECLARATION_AGE
    From
        X001_declarations_curr DECLARE
    Group By
        DECLARE.EMPLOYEE
    ;"""
    sqlite_cursor.execute("DROP TABLE IF EXISTS " + sr_file)
    sqlite_cursor.execute(s_sql)
    sqlite_connection.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD CURRENT DECLARATION DASHBOARD DATA
    if l_debug:
        print("Build current declaration dashboard data...")
    sr_file = "X003_dashboard_curr"
    s_sql = "CREATE TABLE " + sr_file + " AS " + f"""
    Select
        p.EMPLOYEE,
        p.NAME,
        p.GENDER,
        p.RACE,
        p.LANGUAGE,
        p.LOCATION,
        p.ACAD_SUPP,
        p.FACULTY,
        p.DIVISION,
        p.CATEGORY,
        p.POS_GRADE,
        p.JOB_NAME,
        p.PERSON_TYPE,
        p.STATUS,         
        p.AGE,
        p.SUPERVISOR,
        p.EMP_START,
        p.EMP_END,
        d.DECLARATION_ID,
        d.DECLARATION_DATE,
        d.CREATION_DATE,
        d.UNDERSTAND_POLICY,
        d.INTERESTS_TO_DECLARE,
        d.FULL_DISCLOSURE,
        d.DECLARE_STATUS,
        d.LINE_MANAGER,
        d.REJECTION_REASON,
        u.LAST_PAYMENT_DATE As UIF_PAY_DATE,
        u.COUNT_PAYMENTS As UIF_PAY_COUNT,
        f.LAST_PAYMENT_DATE As FOREIGN_PAY_DATE,
        f.COUNT_PAYMENTS As FOREIGN_PAY_COUNT,
        Case
            When d.DECLARE_STATUS IS NOT NULL Then d.DECLARE_STATUS
            Else 'NO DECLARATION'
        End As DECLARED,
        Cast(Julianday('%TODAY%') - Julianday(p.EMP_START) As Int) As DAYS_IN_SERVICE,
        CASE
            When p.EMP_END <> '' Then Cast(Julianday(p.EMP_END) - Julianday('%TODAY%') As Int)
            Else 999999
        END As DAYS_TO_END,
        Cast(Julianday('{target_declare_date}') - Julianday('now') As Int) As DAYS_TO_TARGET,    
        d.DECLARATION_AGE,
        p.CONTRACT_PERSON_TYPE,
        p.CONTRACT_CATEGORY,
        p.CONTRACT_TYPE
    From
        X003aa_people_curr p Left Join
        X003ab_declarations_curr d On d.EMPLOYEE = p.EMPLOYEE Left Join
        X003_uif_payments u On p.EMPLOYEE = u.EMPLOYEE_NUMBER Left Join
        X003_foreign_payments f On p.EMPLOYEE = f.EMPLOYEE_NUMBER
    ;"""
    sqlite_cursor.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = s_sql.replace("%TODAY%", funcdatn.get_today_date())
    sqlite_cursor.execute(s_sql)
    sqlite_connection.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD PREVIOUS DECLARATION DASHBOARD PEOPLE
    if l_debug:
        print("Build previous declaration dashboard people data...")
    sr_file = "X003aa_people_prev"
    s_sql = "CREATE TABLE " + sr_file + " AS " + f"""
    Select
        PERSON.EMPLOYEE_NUMBER AS EMPLOYEE,
        PERSON.NAME_ADDR AS NAME,
        Case
            When PERSON.SEX = 'F' Then 'FEMALE'
            Else 'MALE'
        End As GENDER,
        Upper(PERSON.RACE_DESC) As RACE,
        Upper(PERSON.LANG_DESC) As LANGUAGE,
        Upper(PERSON.LOCATION_DESCRIPTION) As LOCATION,
        Upper(PERSON.ACAD_SUPP) As ACAD_SUPP,
        Upper(PERSON.FACULTY) As FACULTY,
        Upper(PERSON.DIVISION) As DIVISION,
        PERSON.EMPLOYMENT_CATEGORY As CATEGORY,
        Upper(PERSON.GRADE_CALC) As POS_GRADE,
        Upper(PERSON.JOB_NAME) As JOB_NAME,
        Upper(PERSON.PERSON_TYPE) As PERSON_TYPE,
        CASE
            When Upper(PERSON.PERSON_TYPE) In {exclude_person_type}
            Then 'DECLARATION NOT REQUIRED'
            Else 'DECLARATION REQUIRED'
        END As STATUS,
        PERSON.AGE,
        PERSON.SUPERVISOR,
        PERSON.EMP_START
    From
        PEOPLE.X002_PEOPLE_PREV_YEAR PERSON
    Where
        PERSON.EMP_ACTIVE = 'Y'
    ;"""
    # Script if listed person types should be excluded from the data
    """
        And
        Upper(PEOPLE.user_person_type) Not In {exclude_person_type}         
    """
    sqlite_cursor.execute("DROP TABLE IF EXISTS " + sr_file)
    sqlite_cursor.execute(s_sql)
    sqlite_connection.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD PREVIOUS DECLARATION DASHBOARD DECLARATION DATA
    if l_debug:
        print("Build previous declaration dashboard unique declarations...")
    sr_file = "X003ab_declarations_prev"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        DECLARE.DECLARATION_ID,
        DECLARE.EMPLOYEE,
        DECLARE.DECLARATION_DATE,
        Max(DECLARE.CREATION_DATE) As CREATION_DATE,
        Case
            When UNDERSTAND_POLICY_FLAG = 'N' Then 'NO'
            When UNDERSTAND_POLICY_FLAG = 'Y' Then 'YES'
            Else 'NA'
        End As UNDERSTAND_POLICY,
        Case
            When INTEREST_TO_DECLARE_FLAG = 'N' Then 'NO'
            When INTEREST_TO_DECLARE_FLAG = 'Y' Then 'YES'
            Else 'NA'
        End As INTERESTS_TO_DECLARE,
        Case
            When FULL_DISCLOSURE_FLAG = 'N' Then 'NO'
            When FULL_DISCLOSURE_FLAG = 'Y' Then 'YES'
            Else 'NA'
        End As FULL_DISCLOSURE,
        Upper(STATUS) AS DECLARE_STATUS,
        DECLARE.LINE_MANAGER,
        Upper(DECLARE.REJECTION_REASON) As REJECTION_REASON
    From
        X001_declarations_prev DECLARE
    Group By
        DECLARE.EMPLOYEE
    ;"""
    sqlite_cursor.execute("DROP TABLE IF EXISTS " + sr_file)
    sqlite_cursor.execute(s_sql)
    sqlite_connection.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD PREVIOUS DECLARATION DASHBOARD DATA
    if l_debug:
        print("Build previous declaration dashboard data...")
    sr_file = "X003_dashboard_prev"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        *,
        Case
            When DECLARE.DECLARE_STATUS IS NOT NULL Then DECLARE.DECLARE_STATUS
            Else 'NO DECLARATION'
        End As DECLARED
    From
        X003aa_people_prev PEOPLE Left Join
        X003ab_declarations_prev DECLARE On DECLARE.EMPLOYEE = PEOPLE.EMPLOYEE
    Where
        PEOPLE.EMPLOYEE IS NOT NULL
    ;"""
    sqlite_cursor.execute("DROP TABLE IF EXISTS " + sr_file)
    sqlite_cursor.execute(s_sql)
    sqlite_connection.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # DELETE CALCULATION FILES
    sqlite_cursor.execute("DROP TABLE IF EXISTS X003aa_people_curr")
    sqlite_cursor.execute("DROP TABLE IF EXISTS X003ab_declarations_curr")
    sqlite_cursor.execute("DROP TABLE IF EXISTS X003aa_people_prev")
    sqlite_cursor.execute("DROP TABLE IF EXISTS X003ab_declarations_prev")

    """ ****************************************************************************
    BANK NUMBER MASTER FILES
    *****************************************************************************"""

    # BUILD TABLE WITH EMPLOYEE BANK ACCOUNT NUMBERS
    if l_debug:
        print("Obtain master list of all employees...")
    sr_file = "X100_bank_emp"
    s_sql = "CREATE TABLE "+sr_file+" AS " + """
    Select
        'NWU' AS ORG,
        Substr(LOCATION_DESCRIPTION,1,3) As LOC,
        PEOPLE.X002_PEOPLE_CURR.EMPLOYEE_NUMBER AS EMP,
        PEOPLE.X002_PEOPLE_CURR.ACC_NUMBER AS EMP_BANK
    From
        PEOPLE.X002_PEOPLE_CURR
    Where
        PEOPLE.X002_PEOPLE_CURR.ACC_NUMBER <> ''
    ;"""
    sqlite_cursor.execute("DROP TABLE IF EXISTS "+sr_file)
    sqlite_cursor.execute(s_sql)
    sqlite_connection.commit()
    funcfile.writelog("%t BUILD TABLE: "+sr_file)

    # BUILD TABLE WITH VENDOR BANK ACCOUNT NUMBERS
    if l_debug:
        print("Obtain master list of all vendors...")
    sr_file = "X100_bank_ven"
    s_sql = "CREATE TABLE "+sr_file+" AS " + """
    Select
        KFS.X000_Vendor.VENDOR_ID,
        KFS.X000_Vendor.VEND_BANK AS VENDOR_BANK,
        KFS.X000_Vendor.VNDR_NM
    From
        KFS.X000_Vendor
    Where
        KFS.X000_Vendor.VEND_BANK <> '' AND
        KFS.X000_Vendor.DOBJ_MAINT_CD_ACTV_IND = 'Y' AND
        Instr(KFS.X000_Vendor.VNDR_NM, "DT CARD") = 0         
    ;"""
    sqlite_cursor.execute("DROP TABLE IF EXISTS "+sr_file)
    sqlite_cursor.execute(s_sql)
    sqlite_connection.commit()
    funcfile.writelog("%t BUILD TABLE: "+sr_file)

    """ ****************************************************************************
    CHILD SUPPORT AND ADVANCES MASTER FILE
    *****************************************************************************"""

    # BUILD TABLE WITH CHILD SUPPORT AND ADVANCES FOR THE PREVIOUS MONTH
    if l_debug:
        print("Obtain master list of child support payments...")

    sr_file = "X100_child_support_from_payroll"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        pfp.EMPLOYEE_NUMBER,
        Max(pfp.EFFECTIVE_DATE) As LAST_PAYMENT_DATE,
        Count(pfp.RUN_RESULT_ID) As COUNT_PAYMENTS
    From
        PAYROLL.X000aa_payroll_history_%PERIOD% pfp
    Where
        (pfp.ELEMENT_NAME Like 'NWU Child Maintenance%' And
        pfp.EFFECTIVE_DATE Like '%PREVMONTH%%') Or
        (pfp.ELEMENT_NAME Like 'NWU Advance%' And
        pfp.EFFECTIVE_DATE Like '%PREVMONTH%%')
    Group By
        pfp.EMPLOYEE_NUMBER    
    ;"""
    sqlite_cursor.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = s_sql.replace("%PREVMONTH%", funcdatn.get_previous_month_end()[0:7])
    if funcdatn.get_previous_month_end()[0:4] == funcdatn.get_previous_year:
        s_sql = s_sql.replace("%PERIOD%", 'prev')
    else:
        s_sql = s_sql.replace("%PERIOD%", 'curr')
    if l_debug:
        print(s_sql)
    sqlite_cursor.execute(s_sql)
    funcfile.writelog("%t BUILD TABLE: " + sr_file)
    if l_debug:
        sqlite_connection.commit()

    # IDENTIFY CHILD SUPPORT VENDORS
    # Exclusions
    # NW.3G00111.9641 = Child support
    # NW.3G00111.7702 = Employee advance
    s_exclude = """(
    'NW.3G00111.9641',
    'NW.3G00111.7702'
    )"""
    if l_debug:
        print("Identify child support vendors...")
    sr_file = "X100_child_support_from_vendor"
    sqlite_cursor.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        acc.VENDOR_ID,
        acc.VENDOR_NAME,
        Count(acc.EDOC) As COUNT_TRANSACT
    From
        KFSCURR.X001ad_Report_payments_accroute acc
    Where
        acc.ACC_COST_STRING In %EXCLUDE%
    Group By
        acc.VENDOR_ID
    """
    sqlite_cursor.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = s_sql.replace("%EXCLUDE%", s_exclude)
    sqlite_cursor.execute(s_sql)
    sqlite_connection.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    """ ****************************************************************************
    TEST EMPLOYEE VENDOR SHARE BANK ACCOUNT
    *****************************************************************************"""
    if l_debug:
        print("TEST EMPLOYEE VENDOR COMMON BANK")
    funcfile.writelog("TEST EMPLOYEE VENDOR COMMON BANK")

    # DECLARE TEST VARIABLES
    i_coun: int = 0  # Number of new findings to report
    s_desc: str = "Employee vendor share bank acc"

    # BUILD TABLE WITH VENDOR BANK ACCOUNT NUMBERS
    if l_debug:
        print("Merge employees and vendors on bank account...")
    sr_file = "X100aa_bank_empven"
    sqlite_cursor.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        BANK.*,
        VEND.*   
    From
        X100_bank_emp BANK Inner Join
        X100_bank_ven VEND On VEND.VENDOR_BANK = BANK.EMP_BANK And
            Instr(VEND.VENDOR_ID, BANK.EMP) = 0
    ;"""
    sqlite_cursor.execute(s_sql)
    sqlite_connection.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD TABLE WITH VENDOR BANK ACCOUNT NUMBERS
    if l_debug:
        print("Compile list of shared bank accounts...")
    sr_file = "X100ab_bank_empven"
    sqlite_cursor.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        FINDING.ORG,
        FINDING.LOC,
        FINDING.EMP,
        FINDING.EMP_BANK,
        FINDING.VENDOR_ID,
        FINDING.VENDOR_BANK
    From
        X100aa_bank_empven FINDING
    ;"""
    sqlite_cursor.execute(s_sql)
    sqlite_connection.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # COUNT THE NUMBER OF FINDINGS
    i_find = funcsys.tablerowcount(sqlite_cursor, sr_file)
    if l_debug:
        print("*** Found " + str(i_find) + " exceptions ***")
    funcfile.writelog("%t FINDING: " + str(i_find) + " EMPL BANK conflict finding(s)")

    # GET PREVIOUS FINDINGS
    if i_find > 0:
        functest.get_previous_finding(sqlite_cursor, external_data_path, "002_reported.txt", "bank_share_emp_ven", "ITTTT")
        sqlite_connection.commit()

    # SET PREVIOUS FINDINGS
    if i_find > 0:
        functest.set_previous_finding(sqlite_cursor)
        sqlite_connection.commit()

    # ADD PREVIOUS FINDINGS
    sr_file = "X100ad_bank_addprev"
    sqlite_cursor.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_find > 0:
        if l_debug:
            print("Join previously reported to current findings...")
        s_sql = "CREATE TABLE " + sr_file + " AS" + """
        SELECT
            FIND.*,
            'bank_share_emp_ven' AS PROCESS,
            '%TODAY%' AS DATE_REPORTED,
            '%TODAYPLUS%' AS DATE_RETEST,
            PREV.PROCESS AS PREV_PROCESS,
            PREV.DATE_REPORTED AS PREV_DATE_REPORTED,
            PREV.DATE_RETEST AS PREV_DATE_RETEST,
            PREV.REMARK
        FROM
            X100ab_bank_empven FIND Left Join
            Z001ab_setprev PREV ON PREV.FIELD1 = FIND.EMP AND PREV.FIELD2 = FIND.EMP_BANK
        ;"""
        sqlite_cursor.execute("DROP TABLE IF EXISTS " + sr_file)
        s_sql = s_sql.replace("%TODAY%", funcdatn.get_today_date())
        s_sql = s_sql.replace("%TODAYPLUS%", funcdatn.get_current_month_end_next())
        sqlite_cursor.execute(s_sql)
        sqlite_connection.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD LIST TO UPDATE FINDINGS
    sr_file = "X100ae_bank_newprev"
    sqlite_cursor.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_find > 0:
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        SELECT
            PREV.PROCESS,
            PREV.EMP AS FIELD1,
            PREV.EMP_BANK AS FIELD2,
            '' AS FIELD3,
            '' AS FIELD4,
            '' AS FIELD5,
            PREV.DATE_REPORTED,
            PREV.DATE_RETEST,
            PREV.REMARK
        FROM
            X100ad_bank_addprev PREV
        WHERE
            PREV.PREV_PROCESS IS NULL Or
            PREV.DATE_REPORTED > PREV.PREV_DATE_RETEST And PREV.REMARK = ""
        ;"""
        sqlite_cursor.execute("DROP TABLE IF EXISTS " + sr_file)
        sqlite_cursor.execute(s_sql)
        sqlite_connection.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)
        # Export findings to previous reported file
        i_coun = funcsys.tablerowcount(sqlite_cursor, sr_file)
        if i_coun > 0:
            if l_debug:
                print("*** " + str(i_coun) + " Finding(s) to report ***")
            sr_filet = sr_file
            sx_path = external_data_path
            sx_file = "002_reported"
            # Read the header data
            s_head = funccsv.get_colnames_sqlite(sqlite_connection, sr_filet)
            # Write the data
            if l_record:
                funccsv.write_data(sqlite_connection, "main", sr_filet, sx_path, sx_file, s_head, "a", ".txt")
                funcfile.writelog("%t FINDING: " + str(i_coun) + " new finding(s) to export")
                funcfile.writelog("%t EXPORT DATA: " + sr_file)
            if l_mess:
                funcsms.send_telegram('', 'administrator', '<b>' + str(i_find) + '/' + str(i_coun) + '</b> ' + s_desc)
        else:
            if l_debug:
                print("*** No new findings to report ***")
            funcfile.writelog("%t FINDING: No new findings to export")

    # IMPORT OFFICERS FOR MAIL REPORTING PURPOSES
    if i_find > 0 and i_coun > 0:
        functest.get_officer(sqlite_cursor, "HR", "TEST_BANKACC_CONFLICT_VENDOR_OFFICER")
        sqlite_connection.commit()

    # IMPORT SUPERVISORS FOR MAIL REPORTING PURPOSES
    if i_find > 0 and i_coun > 0:
        functest.get_supervisor(sqlite_cursor, "HR", "TEST_BANKACC_CONFLICT_VENDOR_SUPERVISOR")
        sqlite_connection.commit()

    # ADD CONTACT DETAILS TO FINDINGS
    sr_file = "X100ah_bank_addempven"
    sqlite_cursor.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_find > 0 and i_coun > 0:
        if l_debug:
            print("Add details to findings...")
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            PREV.*,
            PERSON.NAME_ADDR AS EMP_NAME,
            PERSON.ACC_TYPE AS BANKACC_TYPE,
            PERSON.ACC_BRANCH AS BANKACC_BRANCH,
            PERSON.ACC_RELATION AS BANKACC_RELATION,
            PERSON.PERSON_TYPE,
            VENDOR.VNDR_NM AS VENDOR_NAME,
            DECLARE.DECLARATION_DATE AS DECLARE_DATE,
            DECLARE.STATUS AS DECLARE_STATUS,
            DECLARE.INTEREST_TO_DECLARE_FLAG AS DECLARE_INTEREST,
            PAYMENTS.LAST_PMT_DT AS PAY_DATE_LAST,
            PAYMENTS.TRAN_COUNT AS PAY_NO_TRAN,
            PAYMENTS.NET_PMT_AMT AS PAY_TOTAL_AMOUNT,
            CAMP_OFF.EMPLOYEE_NUMBER As CAMP_OFF_NUMB,
            CAMP_OFF.NAME_ADDR As CAMP_OFF_NAME,
            CAMP_OFF.EMAIL_ADDRESS As CAMP_OFF_MAIL,
            CAMP_SUP.EMPLOYEE_NUMBER As CAMP_SUP_NUMB,
            CAMP_SUP.NAME_ADDR As CAMP_SUP_NAME,
            CAMP_SUP.EMAIL_ADDRESS As CAMP_SUP_MAIL,
            ORG_OFF.EMPLOYEE_NUMBER As ORG_OFF_NUMB,
            ORG_OFF.NAME_ADDR As ORG_OFF_NAME,
            ORG_OFF.EMAIL_ADDRESS As ORG_OFF_MAIL,
            ORG_SUP.EMPLOYEE_NUMBER As ORG_SUP_NUMB,
            ORG_SUP.NAME_ADDR As ORG_SUP_NAME,
            ORG_SUP.EMAIL_ADDRESS As ORG_SUP_MAIL,
            AUD_OFF.EMPLOYEE_NUMBER As AUD_OFF_NUMB,
            AUD_OFF.NAME_ADDR As AUD_OFF_NAME,
            AUD_OFF.EMAIL_ADDRESS As AUD_OFF_MAIL,
            AUD_SUP.EMPLOYEE_NUMBER As AUD_SUP_NUMB,
            AUD_SUP.NAME_ADDR As AUD_SUP_NAME,
            AUD_SUP.EMAIL_ADDRESS As AUD_SUP_MAIL
        From
            X100ad_bank_addprev PREV
            Left Join PEOPLE.X002_PEOPLE_CURR PERSON On PERSON.EMPLOYEE_NUMBER = PREV.EMP
            Left Join KFS.X000_Vendor VENDOR On VENDOR.VENDOR_ID = PREV.VENDOR_ID
            Left Join KFSCURR.X002aa_Report_payments_summary PAYMENTS On PAYMENTS.VENDOR_ID = PREV.VENDOR_ID
            Left Join X001_declarations_curr DECLARE On DECLARE.EMPLOYEE = PREV.EMP
            Left Join Z001af_officer CAMP_OFF On CAMP_OFF.CAMPUS = PREV.LOC
            Left Join Z001af_officer ORG_OFF On ORG_OFF.CAMPUS = PREV.ORG
            Left Join Z001af_officer AUD_OFF On AUD_OFF.CAMPUS = 'AUD'
            Left Join Z001ag_supervisor CAMP_SUP On CAMP_SUP.CAMPUS = PREV.LOC
            Left Join Z001ag_supervisor ORG_SUP On ORG_SUP.CAMPUS = PREV.ORG
            Left Join Z001ag_supervisor AUD_SUP On AUD_SUP.CAMPUS = 'AUD'
        WHERE
            PREV.PREV_PROCESS IS NULL Or
            PREV.DATE_REPORTED > PREV.PREV_DATE_RETEST And PREV.REMARK = ""    
        ;"""
        sqlite_cursor.execute("DROP TABLE IF EXISTS " + sr_file)
        sqlite_cursor.execute(s_sql)
        sqlite_connection.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD THE FINAL TABLE FOR EXPORT AND REPORT
    sr_file = "X100ax_bank_emp_vend"
    sqlite_cursor.execute("DROP TABLE IF EXISTS " + sr_file)
    if l_debug:
        print("Build the final report")
    if i_find > 0 and i_coun > 0:
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            'EMPL VENDOR SAME BANK ACC' As AUDIT_FINDING,
            FIND.ORG AS ORGANIZATION,
            FIND.LOC AS LOCATION,
            FIND.EMP AS EMPLOYEE_NUMBER,
            FIND.EMP_NAME AS EMPLOYEE_NAME,
            FIND.PERSON_TYPE,
            FIND.EMP_BANK AS EMPLOYEE_BANK,
            FIND.BANKACC_TYPE,
            FIND.BANKACC_BRANCH,
            FIND.BANKACC_RELATION,
            FIND.DECLARE_DATE,
            FIND.DECLARE_STATUS,
            FIND.DECLARE_INTEREST,
            FIND.VENDOR_ID,
            FIND.VENDOR_NAME,
            FIND.VENDOR_BANK,
            FIND.PAY_DATE_LAST,
            FIND.PAY_NO_TRAN,
            FIND.PAY_TOTAL_AMOUNT,
            FIND.CAMP_OFF_NAME AS RESPONSIBLE_OFFICER,
            FIND.CAMP_OFF_NUMB AS RESPONSIBLE_OFFICER_NUMB,
            FIND.CAMP_OFF_MAIL AS RESPONSIBLE_OFFICER_MAIL,
            FIND.CAMP_SUP_NAME AS SUPERVISOR,
            FIND.CAMP_SUP_NUMB AS SUPERVISOR_NUMB,
            FIND.CAMP_SUP_MAIL AS SUPERVISOR_MAIL,
            FIND.ORG_OFF_NAME AS ORG_OFFICER,
            FIND.ORG_OFF_NUMB AS ORG_OFFICER_NUMB,
            FIND.ORG_OFF_MAIL AS ORG_OFFICER_MAIL,
            FIND.ORG_SUP_NAME AS ORG_SUPERVISOR,
            FIND.ORG_SUP_NUMB AS ORG_SUPERVISOR_NUMB,
            FIND.ORG_SUP_MAIL AS ORG_SUPERVISOR_MAIL,
            FIND.AUD_OFF_NAME AS AUDIT_OFFICER,
            FIND.AUD_OFF_NUMB AS AUDIT_OFFICER_NUMB,
            FIND.AUD_OFF_MAIL AS AUDIT_OFFICER_MAIL,
            FIND.AUD_SUP_NAME AS AUDIT_SUPERVISOR,
            FIND.AUD_SUP_NUMB AS AUDIT_SUPERVISOR_NUMB,
            FIND.AUD_SUP_MAIL AS AUDIT_SUPERVISOR_MAIL
        From
            X100ah_bank_addempven FIND
        ;"""
        sqlite_cursor.execute("DROP TABLE IF EXISTS " + sr_file)
        sqlite_cursor.execute(s_sql)
        sqlite_connection.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)
        # Export findings
        if l_export and funcsys.tablerowcount(sqlite_cursor, sr_file) > 0:
            if l_debug:
                print("Export findings...")
            sr_filet = sr_file
            sx_path = results_path
            sx_file = "People_test_100ax_bank_emp_vend_"
            sx_filet = sx_file + funcdatn.get_today_date_file()
            s_head = funccsv.get_colnames_sqlite(sqlite_connection, sr_filet)
            funccsv.write_data(sqlite_connection, "main", sr_filet, sx_path, sx_file, s_head)
            funccsv.write_data(sqlite_connection, "main", sr_filet, sx_path, sx_filet, s_head)
            funcfile.writelog("%t EXPORT DATA: " + sx_path + sx_file)
    else:
        s_sql = "CREATE TABLE " + sr_file + " (" + """
        BLANK TEXT
        );"""
        sqlite_cursor.execute("DROP TABLE IF EXISTS " + sr_file)
        sqlite_cursor.execute(s_sql)
        sqlite_connection.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    """*****************************************************************************
    TEST EMPLOYEE VENDOR SHARE EMAIL ADDRESS
    *****************************************************************************"""
    if l_debug:
        print("TEST EMPLOYEE VENDOR SHARE EMAIL ADDRESS")
    funcfile.writelog("TEST EMPLOYEE VENDOR SHARE EMAIL ADDRESS")

    # FILES NEEDED
    # X020bx_Student_master_sort

    # DECLARE TEST VARIABLES
    i_finding_after: int = 0
    s_description = "Employee vendor share email add"
    s_file_name: str = "employee_vendor_share_email"
    s_file_prefix: str = "X100b"
    s_finding: str = "VENDOR EMAIL INVALID"
    s_report_file: str = "002_reported.txt"

    # OBTAIN TEST DATA FOR CURRENT ACTIVE VENDORS - NEW VENDORS
    if l_debug:
        print("Obtain test data...")
    sr_file: str = s_file_prefix + "aaa_" + s_file_name
    sqlite_cursor.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        VEND.VENDOR_ID,
        Lower(VEND.VEND_MAIL) As VENDOR_MAIL
    From
        KFS.X000_Vendor VEND
    Where
        VEND.DOBJ_MAINT_CD_ACTV_IND = 'Y'
    ;"""
    sqlite_cursor.execute(s_sql)
    sqlite_connection.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # GET PREVIOUS VENDORS - NEW VENDORS
    sr_file: str = s_file_prefix + "aab_" + s_file_name
    sqlite_cursor.execute("DROP TABLE IF EXISTS " + sr_file)
    if l_debug:
        print("Import previous vendors...")
    sqlite_cursor.execute("CREATE TABLE " + sr_file + "(VENDOR_ID_PREV TEXT,VENDOR_MAIL_PREV TEXT)")
    co = open(external_data_path + "201_vendor_new.csv", "r")
    co_reader = csv.reader(co)
    # Read the COLUMN database data
    for row in co_reader:
        # Populate the column variables
        if row[0] == "VENDOR_ID":
            continue
        else:
            s_cols = "INSERT INTO " + sr_file + " VALUES('" + row[0] + "','" + row[1] + "')"
            sqlite_cursor.execute(s_cols)
    sqlite_connection.commit()
    # Close the imported data file
    co.close()
    funcfile.writelog("%t IMPORT TABLE: " + external_data_path + "201_vendor_new.csv (" + sr_file + ")")

    # EXPORT THE PREVIOUS VENDORS AS BACKUP - NEW VENDORS
    if l_record:
        if l_debug:
            print("Export previous vendor details...")
        sr_filet: str = s_file_prefix + "aab_" + s_file_name
        sx_path = external_data_path
        sx_file = "201_vendor_new_prev"
        s_head = funccsv.get_colnames_sqlite(sqlite_connection, sr_filet)
        funccsv.write_data(sqlite_connection, "main", sr_filet, sx_path, sx_file, s_head)
        funcfile.writelog("%t EXPORT DATA: " + sx_path + sx_file)

    # EXPORT THE CURRENT VENDORS - NEW VENDORS
    if l_record:
        if l_debug:
            print("Export current vendor details...")
        sr_filet: str = s_file_prefix + "aaa_" + s_file_name
        sx_path = external_data_path
        sx_file = "201_vendor_new"
        s_head = funccsv.get_colnames_sqlite(sqlite_connection, sr_filet)
        funccsv.write_data(sqlite_connection, "main", sr_filet, sx_path, sx_file, s_head)
        funcfile.writelog("%t EXPORT DATA: " + sx_path + sx_file)

    # COMBINE CURRENT AND PREVIOUS VENDORS  - NEW VENDORS WITH NWU.AC.ZA MAIL
    if l_debug:
        print("Combine current and previous vendors...")
    sr_file: str = s_file_prefix + "aac_" + s_file_name
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        'NEW VENDOR' As VENDOR_CATEGORY,
        NEW.VENDOR_ID,
        Lower(NEW.VENDOR_MAIL) As VENDOR_MAIL
    From
        X100baaa_employee_vendor_share_email NEW Left Join
        X100baab_employee_vendor_share_email OLD On OLD.VENDOR_ID_PREV = NEW.VENDOR_ID
    Where
        Lower(NEW.VENDOR_MAIL) Like ('%nwu.ac.za%') And
        OLD.VENDOR_ID_PREV Is Null
    """
    sqlite_cursor.execute("DROP TABLE IF EXISTS " + sr_file)
    sqlite_cursor.execute(s_sql)
    sqlite_connection.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # COMBINE CURRENT AND PREVIOUS VENDORS  - CURRENT VENDOR CHANGED TO NWU.AC.ZA
    if l_debug:
        print("Combine current and previous vendors...")
    sr_file: str = s_file_prefix + "ab_" + s_file_name
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        'CHANGED VENDOR' As VENDOR_CATEGORY,
        OLD.VENDOR_ID_PREV As VENDOR_ID,
        OLD.VENDOR_MAIL_PREV As VENDOR_MAIL_OLD,
        NEW.VENDOR_MAIL As VENDOR_MAIL_NEW,
        Case
            When OLD.VENDOR_MAIL_PREV Not Like('%nwu.ac.za%') And
                NEW.VENDOR_MAIL Not Like('%nwu.ac.za%')
                Then '1 NO NWU TO NO NWU'
            When OLD.VENDOR_MAIL_PREV Not Like('%nwu.ac.za%') And
                NEW.VENDOR_MAIL Like('%nwu.ac.za%')
                Then '2 NO NWU TO NWU'
            When OLD.VENDOR_MAIL_PREV Like('%nwu.ac.za%') And
                NEW.VENDOR_MAIL Not Like('%nwu.ac.za%')
                Then '3 NWU TO NO NWU'
            When Substr(OLD.VENDOR_ID_PREV, 1, 8) = Substr(NEW.VENDOR_MAIL, 1, 8) And
                NEW.VENDOR_MAIL Like('%nwu.ac.za%')
                Then '4 NWU TO NWU'     
            Else '0'
        End As TEST_TYPE 
    From
        X100baab_employee_vendor_share_email OLD Left Join
        X100baaa_employee_vendor_share_email NEW On NEW.VENDOR_ID = OLD.VENDOR_ID_PREV
    Where
        OLD.VENDOR_MAIL_PREV != NEW.VENDOR_MAIL
    """
    sqlite_cursor.execute("DROP TABLE IF EXISTS " + sr_file)
    sqlite_cursor.execute(s_sql)
    sqlite_connection.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # OBTAIN TEST DATA FOR CURRENT ACTIVE VENDORS - CATEGORY CURRENT ACTIVE VENDOR
    if l_debug:
        print("Obtain test data...")
    sr_file: str = s_file_prefix + "ac_" + s_file_name
    sqlite_cursor.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        'CURRENT VENDOR' As VENDOR_CATEGORY,
        VEND.VENDOR_ID,
        VEND.VNDR_NM As VENDOR_NAME,
        VEND.VNDR_TYP_CD As VENDOR_TYPE,
        SUMM.LAST_PMT_DT As LAST_PAYMENT_DATE,
        Cast(SUMM.NET_PMT_AMT As REAL) As PAYMENT_VALUE,
        Cast(SUMM.TRAN_COUNT As INT) As PAYMENT_TRANSACTION_COUNT,
        Lower(VEND.VEND_MAIL) As VENDOR_MAIL,
        Lower(VEND.EMAIL) As VENDOR_MAIL2,
        Lower(VEND.EMAIL_CONTACT) As VENDOR_MAIL_CONTACT
    From
        KFS.X000_Vendor VEND Left Join
        KFSCURR.X002aa_Report_payments_summary SUMM On SUMM.VENDOR_ID = VEND.VENDOR_ID Left Join
        PEOPLE.X002_PEOPLE_CURR_YEAR PEOP On PEOP.EMPLOYEE_NUMBER = Substr(VEND.VENDOR_ID,1,8) Left Join
        PEOPLE.X002_PEOPLE_PREV_YEAR PREP On PREP.EMPLOYEE_NUMBER = Substr(VEND.VENDOR_ID,1,8) Left Join
        VSSCURR.X001_STUDENT STUD On Cast(STUD.KSTUDBUSENTID As TEXT) = Substr(VEND.VENDOR_ID,1,8)
    Where
        Lower(VEND.VEND_MAIL) Like ('%nwu.ac.za%') And
        VEND.DOBJ_MAINT_CD_ACTV_IND = 'Y' And
        SUMM.VENDOR_ID Is Not Null And
        Cast(SUMM.TRAN_COUNT As INT) > 1 And
        PEOP.EMPLOYEE_NUMBER Is Null And
        PREP.EMPLOYEE_NUMBER Is Null And
        STUD.KSTUDBUSENTID Is Null And
        Substr(VEND.VENDOR_ID, 1, 8) != Substr(VEND.VEND_MAIL, 1, 8)
    ;"""
    # Substr(VEND.VENDOR_ID, 1, 8) != Substr(VEND.VEND_MAIL, 1, 8) And
    # PEOP.EMPLOYEE_NUMBER Is Null
    # PREP.EMPLOYEE_NUMBER Is Null
    sqlite_cursor.execute(s_sql)
    sqlite_connection.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # COMBINE VENDOR FINDINGS
    if l_debug:
        print("Create combined table...")
    sr_file: str = s_file_prefix + "ad_" + s_file_name
    sqlite_cursor.execute("DROP TABLE IF EXISTS " + sr_file)
    sqlite_cursor.execute(
        "CREATE TABLE " + sr_file + "(VENDOR_CATEGORY TEXT, VENDOR_ID TEXT, VENDOR_MAIL TEXT, VENDOR_MAIL_NEW TEXT)")
    # NEW VENDOR
    if l_debug:
        print("Add new vendors...")
    data_file: str = s_file_prefix + "aac_" + s_file_name
    s_sql = "INSERT INTO " + sr_file + \
            "(VENDOR_CATEGORY, VENDOR_ID, VENDOR_MAIL)" \
            " SELECT VENDOR_CATEGORY, VENDOR_ID, VENDOR_MAIL FROM " + \
            data_file + \
            ";"
    sqlite_cursor.execute(s_sql)
    # CHANGED VENDOR
    if l_debug:
        print("Add changed vendors...")
    data_file: str = s_file_prefix + "ab_" + s_file_name
    s_sql = "INSERT INTO " + sr_file + \
            "(VENDOR_CATEGORY, VENDOR_ID, VENDOR_MAIL, VENDOR_MAIL_NEW)" \
            " SELECT VENDOR_CATEGORY, VENDOR_ID, VENDOR_MAIL_OLD, VENDOR_MAIL_NEW FROM " + \
            data_file + \
            " WHERE TEST_TYPE Like('0%')" + \
            ";"
    sqlite_cursor.execute(s_sql)
    # CURRENT VENDOR VENDOR
    if l_debug:
        print("Add current vendors...")
    data_file: str = s_file_prefix + "ac_" + s_file_name
    s_sql = "INSERT INTO " + sr_file + \
            "(VENDOR_CATEGORY, VENDOR_ID, VENDOR_MAIL)" \
            " SELECT VENDOR_CATEGORY, VENDOR_ID, VENDOR_MAIL FROM " + \
            data_file + \
            ";"
    sqlite_cursor.execute(s_sql)
    sqlite_connection.commit()
    funcfile.writelog("%t COMBINE TABLE: " + sr_file)

    # SELECT TEST DATA
    if l_debug:
        print("Identify findings...")
    sr_file: str = s_file_prefix + "ae_" + s_file_name
    sqlite_cursor.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        COMB.*,
        VEND.*,
        CASE
            WHEN PEOP.EMPLOYEE_NUMBER Is Not Null THEN 'EMPLOYEE'
            WHEN STUD.KSTUDBUSENTID Is Not Null THEN 'STUDENT'
            ELSE 'VENDOR'
        END As VENDOR_CLASS    
    From
        %FILEP%%FILEN% COMB Left Join
        KFS.X000_Vendor VEND On VEND.VENDOR_ID = COMB.VENDOR_ID Left Join
        PEOPLE.X002_PEOPLE_CURR_YEAR PEOP On PEOP.EMPLOYEE_NUMBER = Substr(COMB.VENDOR_ID,1,8) Left Join
        VSSCURR.X001_STUDENT STUD On Cast(STUD.KSTUDBUSENTID As TEXT) = Substr(COMB.VENDOR_ID,1,8)        
    ;"""
    s_sql = s_sql.replace("%FILEP%", s_file_prefix)
    s_sql = s_sql.replace("%FILEN%", "ad_" + s_file_name)
    sqlite_cursor.execute(s_sql)
    sqlite_connection.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # SELECT TEST DATA
    if l_debug:
        print("Identify findings...")
    sr_file: str = s_file_prefix + "af_" + s_file_name
    sqlite_cursor.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        VEND.*,
        Cast(SUPP.COUNT_TRANSACT As Int) As CHILD_SUPPORT_TRAN
    From
        %FILEP%%FILEN% VEND Left Join
        X100_child_support_from_vendor SUPP On SUPP.VENDOR_ID = VEND.VENDOR_ID
    ;"""
    s_sql = s_sql.replace("%FILEP%", s_file_prefix)
    s_sql = s_sql.replace("%FILEN%", "ae_" + s_file_name)
    sqlite_cursor.execute(s_sql)
    sqlite_connection.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # SELECT TEST DATA
    if l_debug:
        print("Identify findings...")
    sr_file = s_file_prefix + "b_finding"
    sqlite_cursor.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        'NWU' As ORG,
        FIND.VENDOR_ID,
        FIND.VENDOR_MAIL,
        FIND.VENDOR_MAIL_NEW,        
        FIND.VNDR_TYP_CD As VENDOR_TYPE,
        FIND.VENDOR_CATEGORY,
        FIND.VENDOR_CLASS
    From
        %FILEP%%FILEN% FIND
    Where
        FIND.VENDOR_CLASS Like('VENDOR%') And
        FIND.CHILD_SUPPORT_TRAN Is null          
    Order by
        VENDOR_ID
    ;"""
    s_sql = s_sql.replace("%FILEP%", s_file_prefix)
    s_sql = s_sql.replace("%FILEN%", "af_" + s_file_name)
    sqlite_cursor.execute(s_sql)
    sqlite_connection.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # COUNT THE NUMBER OF FINDINGS
    i_finding_before: int = funcsys.tablerowcount(sqlite_cursor, sr_file)
    if l_debug:
        print("*** Found " + str(i_finding_before) + " exceptions ***")
    funcfile.writelog("%t FINDING: " + str(i_finding_before) + " " + s_finding + " finding(s)")

    # GET PREVIOUS FINDINGS
    if i_finding_before > 0:
        functest.get_previous_finding(sqlite_cursor, external_data_path, s_report_file, s_finding, "TTTTT")
        sqlite_connection.commit()

    # SET PREVIOUS FINDINGS
    if i_finding_before > 0:
        functest.set_previous_finding(sqlite_cursor)
        sqlite_connection.commit()

    # ADD PREVIOUS FINDINGS
    sr_file = s_file_prefix + "d_addprev"
    sqlite_cursor.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0:
        if l_debug:
            print("Join previously reported to current findings...")
        s_sql = "CREATE TABLE " + sr_file + " AS" + """
        Select
            FIND.*,
            Lower('%FINDING%') AS PROCESS,
            '%TODAY%' AS DATE_REPORTED,
            '%DAYS%' AS DATE_RETEST,
            PREV.PROCESS AS PREV_PROCESS,
            PREV.DATE_REPORTED AS PREV_DATE_REPORTED,
            PREV.DATE_RETEST AS PREV_DATE_RETEST,
            PREV.REMARK
        From
            %FILEP%b_finding FIND Left Join
            Z001ab_setprev PREV ON PREV.FIELD1 = FIND.VENDOR_ID And
                PREV.FIELD2 = FIND.VENDOR_MAIL
        ;"""
        s_sql = s_sql.replace("%FINDING%", s_finding)
        s_sql = s_sql.replace("%FILEP%", s_file_prefix)
        s_sql = s_sql.replace("%TODAY%", funcdatn.get_today_date())
        s_sql = s_sql.replace("%DAYS%", funcdatn.get_current_month_end_next())
        sqlite_cursor.execute(s_sql)
        sqlite_connection.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD LIST TO UPDATE FINDINGS
    sr_file = s_file_prefix + "e_newprev"
    sqlite_cursor.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0:
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            PREV.PROCESS,
            PREV.VENDOR_ID AS FIELD1,
            PREV.VENDOR_MAIL AS FIELD2,
            PREV.VENDOR_CATEGORY AS FIELD3,
            PREV.VENDOR_CLASS AS FIELD4,
            '' AS FIELD5,
            PREV.DATE_REPORTED,
            PREV.DATE_RETEST,
            PREV.REMARK
        From
            %FILEP%d_addprev PREV
        Where
            PREV.PREV_PROCESS Is Null Or
            PREV.DATE_REPORTED > PREV.PREV_DATE_RETEST And PREV.REMARK = ""        
        ;"""
        s_sql = s_sql.replace("%FILEP%", s_file_prefix)
        sqlite_cursor.execute(s_sql)
        sqlite_connection.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)
        # Export findings to previous reported file
        i_finding_after = funcsys.tablerowcount(sqlite_cursor, sr_file)
        if i_finding_after > 0:
            if l_debug:
                print("*** " + str(i_finding_after) + " Finding(s) to report ***")
            sx_path = external_data_path
            sx_file = s_report_file[:-4]
            # Read the header data
            s_head = funccsv.get_colnames_sqlite(sqlite_connection, sr_file)
            # Write the data
            if l_record:
                funccsv.write_data(sqlite_connection, "main", sr_file, sx_path, sx_file, s_head, "a", ".txt")
                funcfile.writelog("%t FINDING: " + str(i_finding_after) + " new finding(s) to export")
                funcfile.writelog("%t EXPORT DATA: " + sr_file)
            if l_mess:
                funcsms.send_telegram('', 'administrator', '<b>' + str(i_finding_before) + '/' + str(
                    i_finding_after) + '</b> ' + s_description)
        else:
            if l_debug:
                print("*** No new findings to report ***")
            funcfile.writelog("%t FINDING: No new findings to export")

    # IMPORT OFFICERS FOR MAIL REPORTING PURPOSES
    if i_finding_before > 0 and i_finding_after > 0:
        functest.get_officer(sqlite_cursor, "HR", "TEST " + s_finding + " OFFICER")
        sqlite_connection.commit()

    # IMPORT SUPERVISORS FOR MAIL REPORTING PURPOSES
    if i_finding_before > 0 and i_finding_after > 0:
        functest.get_supervisor(sqlite_cursor, "HR", "TEST " + s_finding + " SUPERVISOR")
        sqlite_connection.commit()

    # ADD CONTACT DETAILS TO FINDINGS
    sr_file = s_file_prefix + "h_detail"
    sqlite_cursor.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0 and i_finding_after > 0:
        if l_debug:
            print("Add contact details to findings...")
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            PREV.ORG,
            PREV.VENDOR_ID,
            FIND.VNDR_NM As VENDOR_NAME,
            PREV.VENDOR_TYPE,
            PREV.VENDOR_MAIL,
            PREV.VENDOR_MAIL_NEW,
            FIND.EMAIL As VENDOR_MAIL_ALT,
            FIND.EMAIL_CONTACT As VENDOR_MAIL_CON,
            FIND.VENDOR_CATEGORY,
            PREV.VENDOR_CLASS,
            SUMM.LAST_PMT_DT As PAY_DATE,
            SUMM.NET_PMT_AMT As PAY_AMOUNT,
            SUMM.TRAN_COUNT As COUNT_TRAN,
            CAMP_OFF.EMPLOYEE_NUMBER AS CAMP_OFF_NUMB,
            CAMP_OFF.NAME_ADDR AS CAMP_OFF_NAME,
            CASE
                WHEN  CAMP_OFF.EMPLOYEE_NUMBER != '' THEN CAMP_OFF.EMPLOYEE_NUMBER||'@nwu.ac.za'
                ELSE CAMP_OFF.EMAIL_ADDRESS
            END AS CAMP_OFF_MAIL,
            CAMP_OFF.EMAIL_ADDRESS AS CAMP_OFF_MAIL2,        
            CAMP_SUP.EMPLOYEE_NUMBER AS CAMP_SUP_NUMB,
            CAMP_SUP.NAME_ADDR AS CAMP_SUP_NAME,
            CASE
                WHEN CAMP_SUP.EMPLOYEE_NUMBER != '' THEN CAMP_SUP.EMPLOYEE_NUMBER||'@nwu.ac.za'
                ELSE CAMP_SUP.EMAIL_ADDRESS
            END AS CAMP_SUP_MAIL,
            CAMP_SUP.EMAIL_ADDRESS AS CAMP_SUP_MAIL2,
            ORG_OFF.EMPLOYEE_NUMBER AS ORG_OFF_NUMB,
            ORG_OFF.NAME_ADDR AS ORG_OFF_NAME,
            CASE
                WHEN ORG_OFF.EMPLOYEE_NUMBER != '' THEN ORG_OFF.EMPLOYEE_NUMBER||'@nwu.ac.za'
                ELSE ORG_OFF.EMAIL_ADDRESS
            END AS ORG_OFF_MAIL,
            ORG_OFF.EMAIL_ADDRESS AS ORG_OFF_MAIL2,
            ORG_SUP.EMPLOYEE_NUMBER AS ORG_SUP_NUMB,
            ORG_SUP.NAME_ADDR AS ORG_SUP_NAME,
            CASE
                WHEN ORG_SUP.EMPLOYEE_NUMBER != '' THEN ORG_SUP.EMPLOYEE_NUMBER||'@nwu.ac.za'
                ELSE ORG_SUP.EMAIL_ADDRESS
            END AS ORG_SUP_MAIL,
            ORG_SUP.EMAIL_ADDRESS AS ORG_SUP_MAIL2,
            AUD_OFF.EMPLOYEE_NUMBER As AUD_OFF_NUMB,
            AUD_OFF.NAME_ADDR As AUD_OFF_NAME,
            AUD_OFF.EMAIL_ADDRESS As AUD_OFF_MAIL,
            AUD_SUP.EMPLOYEE_NUMBER As AUD_SUP_NUMB,
            AUD_SUP.NAME_ADDR As AUD_SUP_NAME,
            AUD_SUP.EMAIL_ADDRESS As AUD_SUP_MAIL
        From
            %FILEP%d_addprev PREV Left Join
            %FILEP%%FILEN% FIND On FIND.VENDOR_ID = PREV.VENDOR_ID,
            Z001af_officer CAMP_OFF On CAMP_OFF.CAMPUS = PREV.VENDOR_TYPE Left Join
            Z001af_officer ORG_OFF On ORG_OFF.CAMPUS = PREV.ORG Left Join
            Z001af_officer AUD_OFF On AUD_OFF.CAMPUS = 'AUD' Left Join
            Z001ag_supervisor CAMP_SUP On CAMP_SUP.CAMPUS = PREV.VENDOR_TYPE Left Join
            Z001ag_supervisor ORG_SUP On ORG_SUP.CAMPUS = PREV.ORG Left Join
            Z001ag_supervisor AUD_SUP On AUD_SUP.CAMPUS = 'AUD' Left Join
            KFSCURR.X002aa_Report_payments_summary SUMM On SUMM.VENDOR_ID = PREV.VENDOR_ID
        Where
            PREV.PREV_PROCESS Is Null Or
            PREV.DATE_REPORTED > PREV.PREV_DATE_RETEST And PREV.REMARK = ""
        ;"""
        s_sql = s_sql.replace("%FILEP%", s_file_prefix)
        s_sql = s_sql.replace("%FILEN%", "ae_" + s_file_name)
        sqlite_cursor.execute(s_sql)
        sqlite_connection.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD THE FINAL TABLE FOR EXPORT AND REPORT
    sr_file = s_file_prefix + "x_" + s_file_name
    sqlite_cursor.execute("DROP TABLE IF EXISTS " + sr_file)
    if l_debug:
        print("Build the final report")
    if i_finding_before > 0 and i_finding_after > 0:
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            '%FIND%' As Audit_finding,
            FIND.VENDOR_CATEGORY As Vendor_category,
            FIND.VENDOR_CLASS As Vendor_class,
            FIND.VENDOR_TYPE As Vendor_type,
            FIND.VENDOR_ID As Vendor_id,
            FIND.VENDOR_NAME As Vendor_name,
            FIND.VENDOR_MAIL As Mail_address,
            FIND.VENDOR_MAIL_NEW As Mail_new,
            FIND.VENDOR_MAIL_ALT As Mail_alternate,
            FIND.VENDOR_MAIL_CON As Mail_contact,
            FIND.PAY_DATE As Last_transaction_date,
            FIND.PAY_AMOUNT As Total_amount,
            FIND.COUNT_TRAN As Transaction_count,
            FIND.ORG As Organization,
            FIND.CAMP_OFF_NAME AS Responsible_Officer,
            FIND.CAMP_OFF_NUMB AS Responsible_Officer_Numb,
            FIND.CAMP_OFF_MAIL AS Responsible_Officer_Mail,
            FIND.CAMP_SUP_NAME AS Supervisor,
            FIND.CAMP_SUP_NUMB AS Supervisor_Numb,
            FIND.CAMP_SUP_MAIL AS Supervisor_Mail,
            FIND.ORG_OFF_NAME AS Org_Officer,
            FIND.ORG_OFF_NUMB AS Org_Officer_Numb,
            FIND.ORG_OFF_MAIL AS Org_Officer_Mail,
            FIND.ORG_SUP_NAME AS Org_Supervisor,
            FIND.ORG_SUP_NUMB AS Org_Supervisor_Numb,
            FIND.ORG_SUP_MAIL AS Org_Supervisor_Mail,
            FIND.AUD_OFF_NAME AS Audit_Officer,
            FIND.AUD_OFF_NUMB AS Audit_Officer_Numb,
            FIND.AUD_OFF_MAIL AS Audit_Officer_Mail,
            FIND.AUD_SUP_NAME AS Audit_Supervisor,
            FIND.AUD_SUP_NUMB AS Audit_Supervisor_Numb,
            FIND.AUD_SUP_MAIL AS Audit_Supervisor_Mail
        From
            %FILEP%h_detail FIND
        Order By
            FIND.VENDOR_CATEGORY,
            FIND.VENDOR_CLASS,
            FIND.VENDOR_MAIL,
            FIND.VENDOR_NAME            
        ;"""
        s_sql = s_sql.replace("%FIND%", s_finding)
        s_sql = s_sql.replace("%FILEP%", s_file_prefix)
        sqlite_cursor.execute(s_sql)
        sqlite_connection.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)
        # Export findings
        if l_export and funcsys.tablerowcount(sqlite_cursor, sr_file) > 0:
            if l_debug:
                print("Export findings...")
            sx_path = results_path
            sx_file = s_file_prefix + "_" + s_finding.lower() + "_"
            sx_file_dated = sx_file + funcdatn.get_today_date_file()
            s_head = funccsv.get_colnames_sqlite(sqlite_connection, sr_file)
            funccsv.write_data(sqlite_connection, "main", sr_file, sx_path, sx_file, s_head)
            funccsv.write_data(sqlite_connection, "main", sr_file, sx_path, sx_file_dated, s_head)
            funcfile.writelog("%t EXPORT DATA: " + sx_path + sx_file)
    else:
        s_sql = "CREATE TABLE " + sr_file + " (" + """
        BLANK TEXT
        );"""
        sqlite_cursor.execute("DROP TABLE IF EXISTS " + sr_file)
        sqlite_cursor.execute(s_sql)
        sqlite_connection.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    """*****************************************************************************
    TEST EMPLOYEE NO DECLARATION
    *****************************************************************************"""

    """
    Test if employees declared conflict of interest.
        Request remediation from employee supervisor as per declaration.
    Test exclude:
        Person type:
        AD HOC APPOINTMENT
        COUNCIL MEMBER
        ADVISORY BOARD MEMBER
        EXTRAORDINARY APPOINTMENT
        If employed less than 31 days.                
    Created: 21 May 2021 (Albert J v Rensburg NWU:21162395)
    """

    # TABLES NEEDED
    # X003_dashboard_curr
    # PEOPLE.X002_PEOPLE_CURR

    # TEST WILL ONLY RUN FROM MAY TO NOVEMBER
    if funcdatn.get_current_month() in ("04","05", "06", "07", "08", "09", "10", "11"):

        # DECLARE TEST VARIABLES
        i_finding_after: int = 0
        s_description = "Employee did not declare interest"
        s_file_name: str = "employee_no_declaration"
        s_file_prefix: str = "X101a"
        s_finding: str = "EMPLOYEE NO DECLARATION"
        s_report_file: str = "002_reported.txt"

        # OBTAIN TEST RUN FLAG
        if functest.get_test_flag(sqlite_cursor, "HR", "TEST " + s_finding, "RUN") == "FALSE":

            if l_debug:
                print('TEST DISABLED')
            funcfile.writelog("TEST " + s_finding + " DISABLED")

        else:

            # OPEN LOG
            if l_debug:
                print("TEST " + s_finding)
            funcfile.writelog("TEST " + s_finding)

            # OBTAIN TEST DATA FOR EMPLOYEES
            # Add student assistants to test 2023-10-11
            # Exclude extraordinary appointments 2023-10-11 legal advice
            if l_debug:
                print("Obtain test data...")
            sr_file: str = s_file_prefix + "a_" + s_file_name
            sqlite_cursor.execute("DROP TABLE IF EXISTS " + sr_file)
            s_sql = "CREATE TABLE " + sr_file + " AS " + f"""
            Select
                d.EMPLOYEE,
                d.NAME,
                d.GENDER,
                d.RACE,
                d.LANGUAGE,
                d.LOCATION,
                d.ACAD_SUPP,
                d.FACULTY,
                d.DIVISION,
                d.CATEGORY,
                d.POS_GRADE,
                d.JOB_NAME,
                d.PERSON_TYPE,
                d.AGE,
                d.SUPERVISOR,
                d.EMP_START,
                d.UIF_PAY_DATE,
                d.FOREIGN_PAY_DATE,
                d.DECLARED,
                d.DAYS_IN_SERVICE,
                d.DAYS_TO_END
            From
                X003_dashboard_curr d
            Where
                d.JOB_NAME Not In ('COUNCELLOR') And
                d.DECLARED = 'NO DECLARATION' And
                d.STATUS = 'DECLARATION REQUIRED' And
                -- d.SUPERVISOR Is Not Null And
                d.DAYS_IN_SERVICE > 30 And
                d.DAYS_TO_END > 60
            ;"""
            sqlite_cursor.execute(s_sql)
            sqlite_connection.commit()
            funcfile.writelog("%t BUILD TABLE: " + sr_file)

            # SELECT TEST DATA
            if l_debug:
                print("Identify findings...")
            sr_file = s_file_prefix + "b_finding"
            sqlite_cursor.execute("DROP TABLE IF EXISTS " + sr_file)
            s_sql = "CREATE TABLE " + sr_file + " AS " + """
            Select
                'NWU' As ORG,
                FIND.LOCATION,
                FIND.SUPERVISOR,
                FIND.EMPLOYEE,
                FIND.CATEGORY,
                FIND.PERSON_TYPE
            From
                %FILEP%%FILEN% FIND
            Order By
                FIND.SUPERVISOR,
                FIND.EMPLOYEE    
            ;"""
            """
            Where
                (((FIND.CATEGORY Like 'PERM%')) Or
                ((FIND.PERSON_TYPE Like 'EX%') And
                (FIND.FOREIGN_PAY_DATE Like '%PREVMONTH%%'))
                ((FIND.CATEGORY Like 'TEMP%') And
                (FIND.UIF_PAY_DATE Like '%PREVMONTH%%')))
            """
            s_sql = s_sql.replace("%FILEP%", s_file_prefix)
            s_sql = s_sql.replace("%FILEN%", "a_" + s_file_name)
            s_sql = s_sql.replace("%PREVMONTH%", funcdatn.get_previous_month_end()[0:7])
            if l_debug:
                print(s_sql)
            sqlite_cursor.execute(s_sql)
            sqlite_connection.commit()
            funcfile.writelog("%t BUILD TABLE: " + sr_file)

            # COUNT THE NUMBER OF FINDINGS
            i_finding_before: int = funcsys.tablerowcount(sqlite_cursor, sr_file)
            if l_debug:
                print("*** Found " + str(i_finding_before) + " exceptions ***")
            funcfile.writelog("%t FINDING: " + str(i_finding_before) + " " + s_finding + " finding(s)")

            # GET PREVIOUS FINDINGS
            if i_finding_before > 0:
                functest.get_previous_finding(sqlite_cursor, external_data_path, s_report_file, s_finding, "TTTTT")
                sqlite_connection.commit()

            # SET PREVIOUS FINDINGS
            if i_finding_before > 0:
                functest.set_previous_finding(sqlite_cursor)
                sqlite_connection.commit()

            # ADD PREVIOUS FINDINGS
            sr_file = s_file_prefix + "d_addprev"
            sqlite_cursor.execute("DROP TABLE IF EXISTS " + sr_file)
            if i_finding_before > 0:
                if l_debug:
                    print("Join previously reported to current findings...")
                s_sql = "CREATE TABLE " + sr_file + " AS" + """
                Select
                    FIND.*,
                    Lower('%FINDING%') AS PROCESS,
                    '%TODAY%' AS DATE_REPORTED,
                    '%DAYS%' AS DATE_RETEST,
                    PREV.PROCESS AS PREV_PROCESS,
                    PREV.DATE_REPORTED AS PREV_DATE_REPORTED,
                    PREV.DATE_RETEST AS PREV_DATE_RETEST,
                    PREV.REMARK
                From
                    %FILEP%b_finding FIND Left Join
                    Z001ab_setprev PREV ON
                     PREV.FIELD1 = FIND.SUPERVISOR And
                     PREV.FIELD2 = FIND.EMPLOYEE
                ;"""
                s_sql = s_sql.replace("%FINDING%", s_finding)
                s_sql = s_sql.replace("%FILEP%", s_file_prefix)
                s_sql = s_sql.replace("%TODAY%", funcdatn.get_today_date())
                s_sql = s_sql.replace("%DAYS%", funcdatn.get_current_month_end_next())
                sqlite_cursor.execute(s_sql)
                sqlite_connection.commit()
                funcfile.writelog("%t BUILD TABLE: " + sr_file)

            # BUILD LIST TO UPDATE FINDINGS
            sr_file = s_file_prefix + "e_newprev"
            sqlite_cursor.execute("DROP TABLE IF EXISTS " + sr_file)
            if i_finding_before > 0:
                s_sql = "CREATE TABLE " + sr_file + " AS " + """
                Select
                    PREV.PROCESS,
                    PREV.SUPERVISOR AS FIELD1,
                    PREV.EMPLOYEE AS FIELD2,
                    PREV.CATEGORY AS FIELD3,
                    PREV.PERSON_TYPE AS FIELD4,
                    PREV.LOCATION AS FIELD5,
                    PREV.DATE_REPORTED,
                    PREV.DATE_RETEST,
                    PREV.REMARK
                From
                    %FILEP%d_addprev PREV
                Where
                    PREV.PREV_PROCESS Is Null Or
                    PREV.DATE_REPORTED > PREV.PREV_DATE_RETEST And PREV.REMARK = ""        
                ;"""
                s_sql = s_sql.replace("%FILEP%", s_file_prefix)
                sqlite_cursor.execute(s_sql)
                sqlite_connection.commit()
                funcfile.writelog("%t BUILD TABLE: " + sr_file)
                # Export findings to previous reported file
                i_finding_after = funcsys.tablerowcount(sqlite_cursor, sr_file)
                if i_finding_after > 0:
                    if l_debug:
                        print("*** " + str(i_finding_after) + " Finding(s) to report ***")
                    sx_path = external_data_path
                    sx_file = s_report_file[:-4]
                    # Read the header data
                    s_head = funccsv.get_colnames_sqlite(sqlite_connection, sr_file)
                    # Write the data
                    l_record_temporary: bool = True
                    if l_record and l_record_temporary:
                        funccsv.write_data(sqlite_connection, "main", sr_file, sx_path, sx_file, s_head, "a", ".txt")
                        funcfile.writelog("%t FINDING: " + str(i_finding_after) + " new finding(s) to export")
                        funcfile.writelog("%t EXPORT DATA: " + sr_file)
                    if l_mess:
                        funcsms.send_telegram('', 'administrator', '<b>' + str(i_finding_before) + '/' + str(
                            i_finding_after) + '</b> ' + s_description)
                else:
                    if l_debug:
                        print("*** No new findings to report ***")
                    funcfile.writelog("%t FINDING: No new findings to export")

            # IMPORT OFFICERS FOR MAIL REPORTING PURPOSES
            if i_finding_before > 0 and i_finding_after > 0:
                functest.get_officer(sqlite_cursor, "HR", "TEST " + s_finding + " OFFICER")
                sqlite_connection.commit()

            # IMPORT SUPERVISORS FOR MAIL REPORTING PURPOSES
            if i_finding_before > 0 and i_finding_after > 0:
                functest.get_supervisor(sqlite_cursor, "HR", "TEST " + s_finding + " SUPERVISOR")
                sqlite_connection.commit()

            # ADD CONTACT DETAILS TO FINDINGS
            sr_file = s_file_prefix + "h_detail"
            sqlite_cursor.execute("DROP TABLE IF EXISTS " + sr_file)
            if i_finding_before > 0 and i_finding_after > 0:
                if l_debug:
                    print("Add contact details to findings...")
                s_sql = "CREATE TABLE " + sr_file + " AS " + """
                Select
                    PREV.ORG,
                    PREV.LOCATION,
                    PREV.CATEGORY,
                    PREV.EMPLOYEE,
                    EMPL.NAME_ADDR As EMP_NAME,
                    EMPL.PERSON_TYPE As EMP_PERSON_TYPE,        
                    Upper(EMPL.POSITION_FULL) As EMP_POSITION,
                    EMPL.EMAIL_ADDRESS As EMP_MAIL1,
                    PREV.EMPLOYEE || '@nwu.ac.za' As EMP_MAIL2,
                    PREV.SUPERVISOR,
                    SUPE.NAME_ADDR As SUP_NAME,
                    SUPE.EMAIL_ADDRESS As SUP_MAIL1,
                    PREV.SUPERVISOR || '@nwu.ac.za' As SUP_MAIL2,
                    CAMP_OFF.EMPLOYEE_NUMBER AS CAMP_OFF_NUMB,
                    CAMP_OFF.NAME_ADDR AS CAMP_OFF_NAME,
                    CAMP_OFF.EMAIL_ADDRESS AS CAMP_OFF_MAIL1,        
                    CASE
                        WHEN  CAMP_OFF.EMPLOYEE_NUMBER != '' THEN CAMP_OFF.EMPLOYEE_NUMBER||'@nwu.ac.za'
                        ELSE CAMP_OFF.EMAIL_ADDRESS
                    END AS CAMP_OFF_MAIL2,
                    CAMP_SUP.EMPLOYEE_NUMBER AS CAMP_SUP_NUMB,
                    CAMP_SUP.NAME_ADDR AS CAMP_SUP_NAME,
                    CAMP_SUP.EMAIL_ADDRESS AS CAMP_SUP_MAIL1,
                    CASE
                        WHEN CAMP_SUP.EMPLOYEE_NUMBER != '' THEN CAMP_SUP.EMPLOYEE_NUMBER||'@nwu.ac.za'
                        ELSE CAMP_SUP.EMAIL_ADDRESS
                    END AS CAMP_SUP_MAIL2,
                    ORG_OFF.EMPLOYEE_NUMBER AS ORG_OFF_NUMB,
                    ORG_OFF.NAME_ADDR AS ORG_OFF_NAME,
                    ORG_OFF.EMAIL_ADDRESS AS ORG_OFF_MAIL1,
                    CASE
                        WHEN ORG_OFF.EMPLOYEE_NUMBER != '' THEN ORG_OFF.EMPLOYEE_NUMBER||'@nwu.ac.za'
                        ELSE ORG_OFF.EMAIL_ADDRESS
                    END AS ORG_OFF_MAIL2,
                    ORG_SUP.EMPLOYEE_NUMBER AS ORG_SUP_NUMB,
                    ORG_SUP.NAME_ADDR AS ORG_SUP_NAME,
                    ORG_SUP.EMAIL_ADDRESS AS ORG_SUP_MAIL1,
                    CASE
                        WHEN ORG_SUP.EMPLOYEE_NUMBER != '' THEN ORG_SUP.EMPLOYEE_NUMBER||'@nwu.ac.za'
                        ELSE ORG_SUP.EMAIL_ADDRESS
                    END AS ORG_SUP_MAIL2,
                    AUD_OFF.EMPLOYEE_NUMBER As AUD_OFF_NUMB,
                    AUD_OFF.NAME_ADDR As AUD_OFF_NAME,
                    AUD_OFF.EMAIL_ADDRESS As AUD_OFF_MAIL,
                    AUD_SUP.EMPLOYEE_NUMBER As AUD_SUP_NUMB,
                    AUD_SUP.NAME_ADDR As AUD_SUP_NAME,
                    AUD_SUP.EMAIL_ADDRESS As AUD_SUP_MAIL
                From
                    %FILEP%d_addprev PREV Left Join
                    PEOPLE.X002_PEOPLE_CURR EMPL On EMPL.EMPLOYEE_NUMBER = PREV.EMPLOYEE Left Join
                    PEOPLE.X002_PEOPLE_CURR SUPE On SUPE.EMPLOYEE_NUMBER = PREV.SUPERVISOR Left Join
                    Z001af_officer CAMP_OFF On CAMP_OFF.CAMPUS = PREV.CATEGORY Left Join
                    Z001af_officer ORG_OFF On ORG_OFF.CAMPUS = PREV.ORG Left Join
                    Z001af_officer AUD_OFF On AUD_OFF.CAMPUS = 'AUD' Left Join
                    Z001ag_supervisor CAMP_SUP On CAMP_SUP.CAMPUS = PREV.CATEGORY Left Join
                    Z001ag_supervisor ORG_SUP On ORG_SUP.CAMPUS = PREV.ORG Left Join
                    Z001ag_supervisor AUD_SUP On AUD_SUP.CAMPUS = 'AUD'
                Where
                    PREV.PREV_PROCESS Is Null Or
                    PREV.DATE_REPORTED > PREV.PREV_DATE_RETEST And PREV.REMARK = ""
                ;"""
                """
                    EMPL.NAME_ADDR,
                    EMPL.PERSON_TYPE,
                    Upper(EMPL.POSITION_FULL) As POSITION,
                    PREV.EMPLOYEE_NUMBER || '@nwu.ac.za' As EMAIL2,
                    EMPL.EMAIL_ADDRESS As EMAIL1,
                """
                s_sql = s_sql.replace("%FILEP%", s_file_prefix)
                s_sql = s_sql.replace("%FILEN%", "a_" + s_file_name)
                sqlite_cursor.execute(s_sql)
                sqlite_connection.commit()
                funcfile.writelog("%t BUILD TABLE: " + sr_file)

            # BUILD THE FINAL TABLE FOR EXPORT AND REPORT
            sr_file = s_file_prefix + "x_" + s_file_name
            sqlite_cursor.execute("DROP TABLE IF EXISTS " + sr_file)
            if l_debug:
                print("Build the final report")
            if i_finding_before > 0 and i_finding_after > 0:
                s_sql = "CREATE TABLE " + sr_file + " AS " + """
                Select
                    '%FIND%' As Audit_finding,
                    FIND.CATEGORY As Employee_category,
                    FIND.EMP_PERSON_TYPE As Employee_type,
                    FIND.EMPLOYEE As Employee_number,
                    FIND.EMP_NAME As Employee_name,
                    FIND.EMP_POSITION As Employee_position,
                    FIND.EMP_MAIL1 As Employee_mail_address,
                    FIND.EMP_MAIL2 As Employee_mail_alternate,
                    FIND.SUPERVISOR As Line_manager_number,
                    FIND.SUP_NAME As Line_manager_name,
                    FIND.SUP_MAIL1 As Line_manager_mail_address,
                    FIND.SUP_MAIL2 As Line_manager_mail_alternate,
                    FIND.ORG As Organization,
                    FIND.LOCATION As Campus,
                    FIND.CAMP_OFF_NAME AS Responsible_officer,
                    FIND.CAMP_OFF_NUMB AS Responsible_officer_numb,
                    FIND.CAMP_OFF_MAIL1 AS Responsible_officer_mail,
                    FIND.CAMP_SUP_NAME AS Resp_supervisor,
                    FIND.CAMP_SUP_NUMB AS Resp_supervisor_numb,
                    FIND.CAMP_SUP_MAIL1 AS Resp_supervisor_mail,
                    FIND.ORG_OFF_NAME AS Org_officer,
                    FIND.ORG_OFF_NUMB AS Org_officer_numb,
                    FIND.ORG_OFF_MAIL1 AS Org_officer_mail,
                    FIND.ORG_SUP_NAME AS Org_supervisor,
                    FIND.ORG_SUP_NUMB AS Org_supervisor_numb,
                    FIND.ORG_SUP_MAIL1 AS Org_Supervisor_mail,
                    FIND.AUD_OFF_NAME AS Audit_officer,
                    FIND.AUD_OFF_NUMB AS Audit_officer_numb,
                    FIND.AUD_OFF_MAIL AS Audit_officer_mail,
                    FIND.AUD_SUP_NAME AS Audit_supervisor,
                    FIND.AUD_SUP_NUMB AS Audit_supervisor_numb,
                    FIND.AUD_SUP_MAIL AS Audit_supervisor_mail
                From
                    %FILEP%h_detail FIND
                ;"""
                s_sql = s_sql.replace("%FIND%", s_finding)
                s_sql = s_sql.replace("%FILEP%", s_file_prefix)
                sqlite_cursor.execute(s_sql)
                sqlite_connection.commit()
                funcfile.writelog("%t BUILD TABLE: " + sr_file)
                # Export findings
                if l_export and funcsys.tablerowcount(sqlite_cursor, sr_file) > 0:
                    if l_debug:
                        print("Export findings...")
                    sx_path = results_path
                    sx_file = s_file_prefix + "_" + s_finding.lower() + "_"
                    sx_file_dated = sx_file + funcdatn.get_today_date_file()
                    s_head = funccsv.get_colnames_sqlite(sqlite_connection, sr_file)
                    funccsv.write_data(sqlite_connection, "main", sr_file, sx_path, sx_file, s_head)
                    funccsv.write_data(sqlite_connection, "main", sr_file, sx_path, sx_file_dated, s_head)
                    funcfile.writelog("%t EXPORT DATA: " + sx_path + sx_file)
            else:
                s_sql = "CREATE TABLE " + sr_file + " (" + """
                BLANK TEXT
                );"""
                sqlite_cursor.execute("DROP TABLE IF EXISTS " + sr_file)
                sqlite_cursor.execute(s_sql)
                sqlite_connection.commit()
                funcfile.writelog("%t BUILD TABLE: " + sr_file)

    """*****************************************************************************
    TEST DECLARATION PENDING
    *****************************************************************************"""

    """
    Test if employee declaration is pending.
        Request remediation from line manager.
    Test exclude:
        If pending less than 31 days.                
    Created: 26 May 2021 (Albert J v Rensburg NWU:21162395)
    """

    # TABLES NEEDED
    # X003_dashboard_curr
    # PEOPLE.X002_PEOPLE_CURR

    # TEST WILL ONLY RUN FROM MAY TO NOVEMBER
    if funcdatn.get_current_month() in ("03", "04", "05", "06", "07", "08", "09", "10", "11", "12"):

        # DECLARE TEST VARIABLES
        i_finding_after: int = 0
        s_description = "Employee declaration pending"
        s_file_name: str = "employee_declaration_pending"
        s_file_prefix: str = "X101b"
        s_finding: str = "EMPLOYEE DECLARATION PENDING"
        s_report_file: str = "002_reported.txt"

        # OBTAIN TEST RUN FLAG
        if not functest.get_test_flag(sqlite_cursor, "HR", "TEST " + s_finding, "RUN"):

            if l_debug:
                print('TEST DISABLED')
            funcfile.writelog("TEST " + s_finding + " DISABLED")

        else:

            # OPEN LOG
            if l_debug:
                print("TEST " + s_finding)
            funcfile.writelog("TEST " + s_finding)

            # OBTAIN TEST DATA FOR EMPLOYEES
            if l_debug:
                print("Obtain test data...")
            sr_file: str = s_file_prefix + "a_" + s_file_name
            sqlite_cursor.execute("DROP TABLE IF EXISTS " + sr_file)
            s_sql = "CREATE TABLE " + sr_file + " AS " + """
            Select
                'NWU' As ORG,
                Substr(a.LOCATION,1,3) As LOC,
                a.EMPLOYEE,
                a.CATEGORY,
                a.PERSON_TYPE,
                a.LINE_MANAGER As SUPERVISOR,
                a.DECLARED,
                Cast(Julianday('%TODAY%') - Julianday(a.DECLARATION_DATE) As Int) As DAYS_DECLARED
            From
                X003_dashboard_curr a
            ;"""
            s_sql = s_sql.replace("%TODAY%", funcdatn.get_today_date())
            sqlite_cursor.execute(s_sql)
            sqlite_connection.commit()
            funcfile.writelog("%t BUILD TABLE: " + sr_file)

            # SELECT TEST DATA
            if l_debug:
                print("Identify findings...")
            sr_file = s_file_prefix + "b_finding"
            sqlite_cursor.execute("DROP TABLE IF EXISTS " + sr_file)
            s_sql = "CREATE TABLE " + sr_file + " AS " + """
            Select
                FIND.ORG,
                FIND.LOC,
                FIND.SUPERVISOR,
                FIND.EMPLOYEE,
                FIND.CATEGORY
            From
                %FILEP%%FILEN% FIND
            Where
                FIND.DECLARED = 'PENDING' And
                FIND.SUPERVISOR Is Not Null And
                FIND.DAYS_DECLARED > 30
            Order By
                FIND.SUPERVISOR,
                FIND.EMPLOYEE    
            ;"""
            s_sql = s_sql.replace("%FILEP%", s_file_prefix)
            s_sql = s_sql.replace("%FILEN%", "a_" + s_file_name)
            sqlite_cursor.execute(s_sql)
            sqlite_connection.commit()
            funcfile.writelog("%t BUILD TABLE: " + sr_file)

            # COUNT THE NUMBER OF FINDINGS
            i_finding_before: int = funcsys.tablerowcount(sqlite_cursor, sr_file)
            if l_debug:
                print("*** Found " + str(i_finding_before) + " exceptions ***")
            funcfile.writelog("%t FINDING: " + str(i_finding_before) + " " + s_finding + " finding(s)")

            # GET PREVIOUS FINDINGS
            if i_finding_before > 0:
                functest.get_previous_finding(sqlite_cursor, external_data_path, s_report_file, s_finding, "TTTTT")
                sqlite_connection.commit()

            # SET PREVIOUS FINDINGS
            if i_finding_before > 0:
                functest.set_previous_finding(sqlite_cursor)
                sqlite_connection.commit()

            # ADD PREVIOUS FINDINGS
            sr_file = s_file_prefix + "d_addprev"
            sqlite_cursor.execute("DROP TABLE IF EXISTS " + sr_file)
            if i_finding_before > 0:
                if l_debug:
                    print("Join previously reported to current findings...")
                s_sql = "CREATE TABLE " + sr_file + " AS" + """
                Select
                    FIND.*,
                    Lower('%FINDING%') AS PROCESS,
                    '%TODAY%' AS DATE_REPORTED,
                    '%DAYS%' AS DATE_RETEST,
                    PREV.PROCESS AS PREV_PROCESS,
                    PREV.DATE_REPORTED AS PREV_DATE_REPORTED,
                    PREV.DATE_RETEST AS PREV_DATE_RETEST,
                    PREV.REMARK
                From
                    %FILEP%b_finding FIND Left Join
                    Z001ab_setprev PREV ON
                     PREV.FIELD1 = FIND.SUPERVISOR And
                     PREV.FIELD2 = FIND.EMPLOYEE
                ;"""
                s_sql = s_sql.replace("%FINDING%", s_finding)
                s_sql = s_sql.replace("%FILEP%", s_file_prefix)
                s_sql = s_sql.replace("%TODAY%", funcdatn.get_today_date())
                s_sql = s_sql.replace("%DAYS%", funcdatn.get_current_month_end_next())
                sqlite_cursor.execute(s_sql)
                sqlite_connection.commit()
                funcfile.writelog("%t BUILD TABLE: " + sr_file)

            # BUILD LIST TO UPDATE FINDINGS
            sr_file = s_file_prefix + "e_newprev"
            sqlite_cursor.execute("DROP TABLE IF EXISTS " + sr_file)
            if i_finding_before > 0:
                s_sql = "CREATE TABLE " + sr_file + " AS " + """
                Select
                    PREV.PROCESS,
                    PREV.SUPERVISOR AS FIELD1,
                    PREV.EMPLOYEE AS FIELD2,
                    '' AS FIELD3,
                    '' AS FIELD4,
                    '' AS FIELD5,
                    PREV.DATE_REPORTED,
                    PREV.DATE_RETEST,
                    PREV.REMARK
                From
                    %FILEP%d_addprev PREV
                Where
                    PREV.PREV_PROCESS Is Null Or
                    PREV.DATE_REPORTED > PREV.PREV_DATE_RETEST And PREV.REMARK = ""        
                ;"""
                s_sql = s_sql.replace("%FILEP%", s_file_prefix)
                sqlite_cursor.execute(s_sql)
                sqlite_connection.commit()
                funcfile.writelog("%t BUILD TABLE: " + sr_file)
                # Export findings to previous reported file
                i_finding_after = funcsys.tablerowcount(sqlite_cursor, sr_file)
                if i_finding_after > 0:
                    if l_debug:
                        print("*** " + str(i_finding_after) + " Finding(s) to report ***")
                    sx_path = external_data_path
                    sx_file = s_report_file[:-4]
                    # Read the header data
                    s_head = funccsv.get_colnames_sqlite(sqlite_connection, sr_file)
                    # Write the data
                    l_record_temporary: bool = True
                    if l_record and l_record_temporary:
                        funccsv.write_data(sqlite_connection, "main", sr_file, sx_path, sx_file, s_head, "a", ".txt")
                        funcfile.writelog("%t FINDING: " + str(i_finding_after) + " new finding(s) to export")
                        funcfile.writelog("%t EXPORT DATA: " + sr_file)
                    if l_mess:
                        funcsms.send_telegram('', 'administrator', '<b>' + str(i_finding_before) + '/' + str(
                            i_finding_after) + '</b> ' + s_description)
                else:
                    if l_debug:
                        print("*** No new findings to report ***")
                    funcfile.writelog("%t FINDING: No new findings to export")

            # IMPORT OFFICERS FOR MAIL REPORTING PURPOSES
            if i_finding_before > 0 and i_finding_after > 0:
                functest.get_officer(sqlite_cursor, "HR", "TEST " + s_finding + " OFFICER")
                sqlite_connection.commit()

            # IMPORT SUPERVISORS FOR MAIL REPORTING PURPOSES
            if i_finding_before > 0 and i_finding_after > 0:
                functest.get_supervisor(sqlite_cursor, "HR", "TEST " + s_finding + " SUPERVISOR")
                sqlite_connection.commit()

            # ADD CONTACT DETAILS TO FINDINGS
            sr_file = s_file_prefix + "h_detail"
            sqlite_cursor.execute("DROP TABLE IF EXISTS " + sr_file)
            if i_finding_before > 0 and i_finding_after > 0:
                if l_debug:
                    print("Add contact details to findings...")
                s_sql = "CREATE TABLE " + sr_file + " AS " + """
                Select
                    PREV.ORG,
                    PREV.LOC,
                    PREV.CATEGORY,
                    PREV.EMPLOYEE,
                    EMPL.NAME_ADDR As EMP_NAME,
                    EMPL.PERSON_TYPE As EMP_PERSON_TYPE,        
                    Upper(EMPL.POSITION_FULL) As EMP_POSITION,
                    EMPL.EMAIL_ADDRESS As EMP_MAIL1,
                    PREV.EMPLOYEE || '@nwu.ac.za' As EMP_MAIL2,
                    PREV.SUPERVISOR,
                    SUPE.NAME_ADDR As SUP_NAME,
                    SUPE.EMAIL_ADDRESS As SUP_MAIL1,
                    PREV.SUPERVISOR || '@nwu.ac.za' As SUP_MAIL2,
                    SUPE.SUPERVISOR As NEXT_SUPERVISOR,
                    NSUP.NAME_ADDR As NSUP_NAME,
                    NSUP.EMAIL_ADDRESS As NSUP_MAIL1,
                    SUPE.SUPERVISOR || '@nwu.ac.za' As NSUP_MAIL2,
                    CAMP_OFF.EMPLOYEE_NUMBER AS CAMP_OFF_NUMB,
                    CAMP_OFF.NAME_ADDR AS CAMP_OFF_NAME,
                    CAMP_OFF.EMAIL_ADDRESS AS CAMP_OFF_MAIL1,        
                    CASE
                        WHEN  CAMP_OFF.EMPLOYEE_NUMBER != '' THEN CAMP_OFF.EMPLOYEE_NUMBER||'@nwu.ac.za'
                        ELSE CAMP_OFF.EMAIL_ADDRESS
                    END AS CAMP_OFF_MAIL2,
                    CAMP_SUP.EMPLOYEE_NUMBER AS CAMP_SUP_NUMB,
                    CAMP_SUP.NAME_ADDR AS CAMP_SUP_NAME,
                    CAMP_SUP.EMAIL_ADDRESS AS CAMP_SUP_MAIL1,
                    CASE
                        WHEN CAMP_SUP.EMPLOYEE_NUMBER != '' THEN CAMP_SUP.EMPLOYEE_NUMBER||'@nwu.ac.za'
                        ELSE CAMP_SUP.EMAIL_ADDRESS
                    END AS CAMP_SUP_MAIL2,
                    ORG_OFF.EMPLOYEE_NUMBER AS ORG_OFF_NUMB,
                    ORG_OFF.NAME_ADDR AS ORG_OFF_NAME,
                    ORG_OFF.EMAIL_ADDRESS AS ORG_OFF_MAIL1,
                    CASE
                        WHEN ORG_OFF.EMPLOYEE_NUMBER != '' THEN ORG_OFF.EMPLOYEE_NUMBER||'@nwu.ac.za'
                        ELSE ORG_OFF.EMAIL_ADDRESS
                    END AS ORG_OFF_MAIL2,
                    ORG_SUP.EMPLOYEE_NUMBER AS ORG_SUP_NUMB,
                    ORG_SUP.NAME_ADDR AS ORG_SUP_NAME,
                    ORG_SUP.EMAIL_ADDRESS AS ORG_SUP_MAIL1,
                    CASE
                        WHEN ORG_SUP.EMPLOYEE_NUMBER != '' THEN ORG_SUP.EMPLOYEE_NUMBER||'@nwu.ac.za'
                        ELSE ORG_SUP.EMAIL_ADDRESS
                    END AS ORG_SUP_MAIL2,
                    AUD_OFF.EMPLOYEE_NUMBER As AUD_OFF_NUMB,
                    AUD_OFF.NAME_ADDR As AUD_OFF_NAME,
                    AUD_OFF.EMAIL_ADDRESS As AUD_OFF_MAIL,
                    AUD_SUP.EMPLOYEE_NUMBER As AUD_SUP_NUMB,
                    AUD_SUP.NAME_ADDR As AUD_SUP_NAME,
                    AUD_SUP.EMAIL_ADDRESS As AUD_SUP_MAIL
                From
                    %FILEP%d_addprev PREV Left Join
                    PEOPLE.X002_PEOPLE_CURR EMPL On EMPL.EMPLOYEE_NUMBER = PREV.EMPLOYEE Left Join
                    PEOPLE.X002_PEOPLE_CURR SUPE On SUPE.EMPLOYEE_NUMBER = PREV.SUPERVISOR Left Join
                    PEOPLE.X002_PEOPLE_CURR NSUP On NSUP.EMPLOYEE_NUMBER = SUPE.SUPERVISOR Left Join
                    Z001af_officer CAMP_OFF On CAMP_OFF.CAMPUS = PREV.CATEGORY Left Join
                    Z001af_officer ORG_OFF On ORG_OFF.CAMPUS = PREV.ORG Left Join
                    Z001af_officer AUD_OFF On AUD_OFF.CAMPUS = 'AUD' Left Join
                    Z001ag_supervisor CAMP_SUP On CAMP_SUP.CAMPUS = PREV.CATEGORY Left Join
                    Z001ag_supervisor ORG_SUP On ORG_SUP.CAMPUS = PREV.ORG Left Join
                    Z001ag_supervisor AUD_SUP On AUD_SUP.CAMPUS = 'AUD'
                Where
                    PREV.PREV_PROCESS Is Null Or
                    PREV.DATE_REPORTED > PREV.PREV_DATE_RETEST And PREV.REMARK = ""
                ;"""
                """
                    EMPL.NAME_ADDR,
                    EMPL.PERSON_TYPE,
                    Upper(EMPL.POSITION_FULL) As POSITION,
                    PREV.EMPLOYEE_NUMBER || '@nwu.ac.za' As EMAIL2,
                    EMPL.EMAIL_ADDRESS As EMAIL1,
                """
                s_sql = s_sql.replace("%FILEP%", s_file_prefix)
                s_sql = s_sql.replace("%FILEN%", "a_" + s_file_name)
                sqlite_cursor.execute(s_sql)
                sqlite_connection.commit()
                funcfile.writelog("%t BUILD TABLE: " + sr_file)

            # BUILD THE FINAL TABLE FOR EXPORT AND REPORT
            sr_file = s_file_prefix + "x_" + s_file_name
            sqlite_cursor.execute("DROP TABLE IF EXISTS " + sr_file)
            if l_debug:
                print("Build the final report")
            if i_finding_before > 0 and i_finding_after > 0:
                s_sql = "CREATE TABLE " + sr_file + " AS " + """
                Select
                    '%FIND%' As Audit_finding,
                    FIND.CATEGORY As Employee_category,
                    FIND.EMP_PERSON_TYPE As Employee_type,
                    FIND.EMPLOYEE As Employee_number,
                    FIND.EMP_NAME As Employee_name,
                    FIND.EMP_POSITION As Employee_position,
                    FIND.EMP_MAIL1 As Employee_mail_address,
                    FIND.EMP_MAIL2 As Employee_mail_alternate,
                    FIND.SUPERVISOR As Supervisor_number,
                    FIND.SUP_NAME As Supervisor_name,
                    FIND.SUP_MAIL1 As Supervisor_mail_address,
                    FIND.SUP_MAIL2 As Supervisor_mail_alternate,
                    FIND.NEXT_SUPERVISOR As Supervisor2_number,
                    FIND.NSUP_NAME As Supervisor2_name,
                    FIND.NSUP_MAIL1 As Supervisor2_mail_address,
                    FIND.NSUP_MAIL2 As Supervisor2_mail_alternate,
                    FIND.ORG As Organization,
                    FIND.LOC As Campus,
                    FIND.CAMP_OFF_NAME AS Responsible_officer,
                    FIND.CAMP_OFF_NUMB AS Responsible_officer_numb,
                    FIND.CAMP_OFF_MAIL1 AS Responsible_officer_mail,
                    FIND.CAMP_SUP_NAME AS Resp_supervisor,
                    FIND.CAMP_SUP_NUMB AS Resp_supervisor_numb,
                    FIND.CAMP_SUP_MAIL1 AS Resp_supervisor_mail,
                    FIND.ORG_OFF_NAME AS Org_officer,
                    FIND.ORG_OFF_NUMB AS Org_officer_numb,
                    FIND.ORG_OFF_MAIL1 AS Org_officer_mail,
                    FIND.ORG_SUP_NAME AS Org_supervisor,
                    FIND.ORG_SUP_NUMB AS Org_supervisor_numb,
                    FIND.ORG_SUP_MAIL1 AS Org_Supervisor_mail,
                    FIND.AUD_OFF_NAME AS Audit_officer,
                    FIND.AUD_OFF_NUMB AS Audit_officer_numb,
                    FIND.AUD_OFF_MAIL AS Audit_officer_mail,
                    FIND.AUD_SUP_NAME AS Audit_supervisor,
                    FIND.AUD_SUP_NUMB AS Audit_supervisor_numb,
                    FIND.AUD_SUP_MAIL AS Audit_supervisor_mail
                From
                    %FILEP%h_detail FIND
                ;"""
                s_sql = s_sql.replace("%FIND%", s_finding)
                s_sql = s_sql.replace("%FILEP%", s_file_prefix)
                sqlite_cursor.execute(s_sql)
                sqlite_connection.commit()
                funcfile.writelog("%t BUILD TABLE: " + sr_file)
                # Export findings
                if l_export and funcsys.tablerowcount(sqlite_cursor, sr_file) > 0:
                    if l_debug:
                        print("Export findings...")
                    sx_path = results_path
                    sx_file = s_file_prefix + "_" + s_finding.lower() + "_"
                    sx_file_dated = sx_file + funcdatn.get_today_date_file()
                    s_head = funccsv.get_colnames_sqlite(sqlite_connection, sr_file)
                    funccsv.write_data(sqlite_connection, "main", sr_file, sx_path, sx_file, s_head)
                    funccsv.write_data(sqlite_connection, "main", sr_file, sx_path, sx_file_dated, s_head)
                    funcfile.writelog("%t EXPORT DATA: " + sx_path + sx_file)
            else:
                s_sql = "CREATE TABLE " + sr_file + " (" + """
                BLANK TEXT
                );"""
                sqlite_cursor.execute("DROP TABLE IF EXISTS " + sr_file)
                sqlite_cursor.execute(s_sql)
                sqlite_connection.commit()
                funcfile.writelog("%t BUILD TABLE: " + sr_file)

    """*****************************************************************************
    TEST CONFLICTING TRANSACTIONS MASTER TABLES
    *****************************************************************************"""

    # DECLARE TEST VARIABLES
    test_file_prefix: str = "X200"

    # BUILD A TABLE WITH CLEANED UP VENDOR NAMES AND REGISTRATION NUMBERS
    if l_debug:
        print('BUILD A TABLE WITH CLEANED UP VENDOR NAMES AND REGISTRATION NUMBERS')

    # Read the list of words to exclude in the vendor names
    words_to_remove = funcstat.stat_list(sqlite_cursor,
                                         "KFS.X000_Own_kfs_lookups",
                                         "LOOKUP_CODE",
                                         "LOOKUP='EXCLUDE VENDOR WORD'")
    if l_debug:
        # print(words_to_remove)
        pass

    # Prepare the table to receive cleaned vendors
    table_name = test_file_prefix + "a_vendor_cleaned"
    sqlite_cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
    sqlite_cursor.execute(f"""
        CREATE TABLE {table_name} (
        vendor_id TEXT,
        vendor_name TEXT,
        vendor_regno TEXT
        )
    """)

    # Prepare the insert statement
    insert_stmt = f"INSERT INTO {table_name} (vendor_id, vendor_name, vendor_regno) VALUES (?, ?, ?)"

    # Execute the SQL query to fetch the data where vendor_type is 'PO' or 'DV'
    sqlite_cursor.execute("""
        SELECT VENDOR_ID,
        PAYEE_NAME,
        REG_NO
        FROM KFSCURR.X002aa_Report_payments_summary
        WHERE VENDOR_TYPE IN ("PO", "DV")
        ;""")
    vendors = sqlite_cursor.fetchall()

    # Accumulating vendors for bulk insert
    modified_vendors = []
    for vendor in vendors:
        payee_id, payee_name, vendor_reg_nr = vendor
        modified_payee_name = funcstr.clean_paragraph(payee_name, words_to_remove, 'b')
        modified_regno = funcstr.clean_paragraph(vendor_reg_nr, words_to_remove, 'n')
        modified_vendors.append((payee_id, modified_payee_name, modified_regno))

    # Bulk insert using executemany
    sqlite_cursor.executemany(insert_stmt, modified_vendors)
    sqlite_connection.commit()

    # Log the actions performed
    funcfile.writelog(f"%t BUILD & POPULATE TABLE: {table_name}")

    # VENDOR NAME AND REGISTRATION NUMBER COMPARISON
    if l_debug:
        print('VENDOR NAME AND REGISTRATION NUMBER COMPARISON')

    # Build table with directorship and vendor name and registration number comparison
    if l_debug:
        print("Build table with directorship and vendor name and registration number comparison...")
    table_name: str = test_file_prefix + "b_director_vendor_match"
    s_sql = f"CREATE TABLE {table_name} AS " + """
    Select
        d.nwu_number,
        d.company_name As company_name,
        d.registration_number As company_registration_number,
        v.vendor_id,
        v.vendor_name,
        Case
          When SubStr(d.registration_number, 1, 4) || SubStr(d.registration_number, 6, 6) = SubStr(v.vendor_regno, 1, 10)
          Then 1
          When d.company_name Like (v.vendor_name || '%')
          Then 3
          Else 2
        End As vendor_ratio,
        SubStr(d.registration_number, 1, 4) || SubStr(d.registration_number, 6, 6) As regno_director,
        SubStr(v.vendor_regno, 1, 10) As regno_vendor,
        Case
          When SubStr(d.registration_number, 1, 4) || SubStr(d.registration_number, 6, 6) = SubStr(v.vendor_regno, 1, 10)
          Then 1
          Else 0
        End As regno_ratio
    From
        X004x_searchworks_directors d,
        X200a_vendor_cleaned v
    Where
        (SubStr(d.registration_number, 1, 4) || SubStr(d.registration_number, 6, 6) = SubStr(v.vendor_regno, 1, 10)) Or
        (d.company_name Like (v.vendor_name || '%')) Or
        (v.vendor_name Like (d.company_name || '%'))    
    ;"""
    sqlite_cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
    sqlite_cursor.execute(s_sql)
    sqlite_connection.commit()
    funcfile.writelog(f"%t BUILD TABLE: {table_name}")

    # BUILD A TABLE WITH CLEANED UP EMPLOYEE INTERESTS
    if l_debug:
        print('BUILD A TABLE WITH CLEANED UP EMPLOYEE INTERESTS')

    # Read the list of words to exclude in the vendor names
    words_to_remove = funcstat.stat_list(sqlite_cursor,
                                         "KFS.X000_Own_kfs_lookups",
                                         "LOOKUP_CODE",
                                         "LOOKUP='EXCLUDE VENDOR WORD'")
    if l_debug:
        # print(words_to_remove)
        pass

    # Create SQLite table to receive cleaned vendors
    table_name = test_file_prefix + "c_interests_cleaned"
    sqlite_cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
    sqlite_cursor.execute(f"""
        CREATE TABLE {table_name} (
        declaration_id INT,
        interest_id INT,
        employee_number TEXT,
        entity_name TEXT,
        entity_registration_number TEXT        
        ) """)
    funcfile.writelog(f"%t BUILD TABLE: {table_name}")

    # Prepare the insert statement
    insert_stmt = f"INSERT INTO {table_name} (declaration_id, interest_id, employee_number, entity_name, entity_registration_number) VALUES (?, ?, ?, ?, ?)"

    # Execute the SQL query to fetch the data
    sqlite_cursor.execute("""
    Select
        i.DECLARATION_ID As declaration_id,
        i.INTEREST_ID As interest_id,
        i.EMPLOYEE_NUMBER As employee_number,
        i.ENTITY_NAME As entity_name,
        i.ENTITY_REGISTRATION_NUMBER As entity_registration_number,
        Max(i.DECLARATION_DATE) As declaration_date
    From
        X002_interests_curr i
    Where
        i.ENTITY_NAME <> '' And
        i.INTEREST_STATUS = 'Accepted'
    Group By
        i.DECLARATION_ID,
        i.INTEREST_ID
    """)
    # Fetch all the rows returned by the query
    interests = sqlite_cursor.fetchall()

    # Accumulating vendors for bulk insert
    modified_interests = []
    for interest in interests:
        declaration_id, interest_id, employee_number, entity_name, entity_registration_number, declaration_date = interest
        modified_entity_name = funcstr.clean_paragraph(entity_name, words_to_remove, 'b')
        modified_entity_registration_number = funcstr.clean_paragraph(entity_registration_number, words_to_remove, 'n')
        modified_interests.append(
            (declaration_id, interest_id, employee_number, modified_entity_name, modified_entity_registration_number))

    # Bulk insert using executemany
    sqlite_cursor.executemany(insert_stmt, modified_interests)
    sqlite_connection.commit()

    # Build table which compare conflicting transactions with declarations
    if l_debug:
        print("Build table which compare conflicting transactions with declarations...")
    table_name: str = test_file_prefix + "d_director_interest_match"
    s_sql = f"CREATE TABLE {table_name} As " + """
    Select
        v.nwu_number,
        v.company_name,
        v.company_registration_number,
        v.vendor_id,
        v.regno_director,
        v.vendor_ratio As vendor_match_type,
        Case
            When i.employee_number = v.nwu_number And SubStr(i.entity_registration_number, 1, 10) = v.regno_director
            Then 1
            When i.employee_number = v.nwu_number And i.entity_name Like (v.company_name || '%')
            Then 2
            Else 0        
        End As match_type,
        i.declaration_id,
        i.interest_id,
        i.entity_name,
        i.entity_registration_number
    From
        X200b_director_vendor_match v Left Join
        X200c_interests_cleaned i On (i.employee_number = v.nwu_number
                    And SubStr(i.entity_registration_number, 1, 10) = v.regno_director)
                Or (i.employee_number = v.nwu_number
                    And i.entity_name Like (v.company_name || '%'))    
    ;"""
    sqlite_cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
    sqlite_cursor.execute(s_sql)
    sqlite_connection.commit()
    funcfile.writelog(f"%t BUILD TABLE: {table_name}")

    # Add data from the people and vendor master tables
    if l_debug:
        print("Add some data needed to build the test parameters...")
    table_name: str = test_file_prefix + "e_master_table"
    s_sql = f"CREATE TABLE {table_name} As " + """
    Select
        i.nwu_number,
        p.name_address,
        i.company_name,
        i.company_registration_number,
        i.vendor_id,
        v.VNDR_NM As vendor_name,
        v.VNDR_TYP_CD As vendor_type,
        i.vendor_match_type,
        i.match_type,
        i.declaration_id,
        i.interest_id,
        i.entity_name,
        i.regno_director,        
        i.entity_registration_number,
        i.nwu_number || '-' || i.company_registration_number As exclude_combination          
    From
        X200d_director_interest_match i Left Join
        PEOPLE.X000_PEOPLE p On p.employee_number = i.nwu_number Left Join
        KFS.X000_Vendor v On v.VENDOR_ID = i.vendor_id
    ;"""
    sqlite_cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
    sqlite_cursor.execute(s_sql)
    sqlite_connection.commit()
    funcfile.writelog(f"%t BUILD TABLE: {table_name}")

    """*****************************************************************************
    TEST ACTIVE CIPC DIRECTOR ACTIVE VENDOR NO DECLARATION
    *****************************************************************************"""

    """ DESCRIPTION
    """

    """ INDEX
    """

    """" TABLES USED IN TEST
    """
    # TEST WILL ONLY RUN FROM MAY TO NOVEMBER
    if funcdatn.get_current_month() in ("03", "04", "05", "06", "07", "08", "09", "10", "11"):

        # DECLARE TEST VARIABLES
        count_findings_after: int = 0
        test_description = "Active CIPC director active vendor no declaration"
        test_file_name: str = "active_cipc_vendor_no_declaration"
        test_file_prefix: str = "X201a"
        test_finding: str = "ACTIVE CIPC DIRECTOR ACTIVE VENDOR NO DECLARATION"
        test_report_file: str = "002_reported.txt"

        # OBTAIN TEST RUN FLAG
        if not functest.get_test_flag(sqlite_cursor, "HR", f"TEST {test_finding}", "RUN"):

            if l_debug:
                print('TEST DISABLED')
            funcfile.writelog("TEST " + test_finding + " DISABLED")

        else:

            # OPEN LOG
            if l_debug:
                print("TEST " + test_finding)
            funcfile.writelog("TEST " + test_finding)

            # Fetch initial data from the master table
            if l_debug:
                print("Fetch initial data from the master table...")
            table_name = test_file_prefix + f"a_{test_file_name}"
            s_sql = f"CREATE TABLE {table_name} As " + """
            Select
                m.nwu_number,
                m.name_address,
                m.company_name,
                m.company_registration_number,
                m.vendor_id,
                m.vendor_name,
                m.vendor_type,
                m.vendor_match_type,
                Case
                    When m.vendor_match_type = 1 Then 'On registration number'
                    When m.vendor_match_type = 2 Then 'CIPC name in vendor'
                    When m.vendor_match_type = 3 Then 'Vendor name in CIPC'
                    else 'Unknown'
                End As vendor_match_description,
                m.match_type As interest_match_type,
                Case
                    When m.match_type = 1 Then 'On registration number'
                    When m.match_type = 2 Then 'CIPC name in interest'
                    else 'No match'
                End As interest_match_description,
                m.declaration_id,
                m.interest_id,
                m.entity_name,
                m.regno_director,
                m.entity_registration_number,
                m.exclude_combination
            From
                X200e_master_table m    
            ;"""
            if l_debug:
                # print(s_sql)
                pass
            sqlite_cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
            sqlite_cursor.execute(s_sql)
            sqlite_connection.commit()
            funcfile.writelog(f"%t BUILD TABLE: {table_name}")

            # Read the list employees and companies to exclude from the test
            exclude_employee_company = funcstat.stat_tuple(sqlite_cursor,
                                                           "KFS.X000_Own_kfs_lookups",
                                                           "LOOKUP_CODE",
                                                           "LOOKUP='EXCLUDE EMPLOYEE COMPANY 34(5)'")
            if l_debug:
                print('List of employees and companies to exclude:')
                print(exclude_employee_company)
                pass

            # Select the test data
            # Directorship data match with current active vendor but no declaration
            # Match types 0 = No match in declaration
            #             1 = Match on company registration number
            #             2 = Match on company name
            if l_debug:
                print("Identify findings...")
            table_name = test_file_prefix + "b_finding"
            s_sql = f"CREATE TABLE {table_name} As " + f"""
            Select
                'NWU' As org,
                f.vendor_type,
                f.nwu_number,
                f.name_address,
                f.company_registration_number,
                f.company_name,
                f.vendor_id,
                f.vendor_match_description
            From
                {test_file_prefix}a_{test_file_name} f
            Where
                f.interest_match_type = 0 And
                f.exclude_combination Not In {exclude_employee_company}
            ;"""
            if l_debug:
                # print(s_sql)
                pass
            sqlite_cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
            sqlite_cursor.execute(s_sql)
            sqlite_connection.commit()
            funcfile.writelog(f"%t BUILD TABLE: {table_name}")

            # Count the number of findings
            count_findings_before: int = funcsys.tablerowcount(sqlite_cursor, table_name)
            if l_debug:
                print("*** Found " + str(count_findings_before) + " exceptions ***")
            funcfile.writelog("%t FINDING: " + str(count_findings_before) + " " + test_finding + " finding(s)")

            # Get previous findings
            if count_findings_before > 0:
                functest.get_previous_finding(sqlite_cursor, external_data_path, test_report_file, test_finding, "TTTTT")
                sqlite_connection.commit()

            # Set previous findings
            if count_findings_before > 0:
                functest.set_previous_finding(sqlite_cursor)
                sqlite_connection.commit()

            # Add previous findings
            table_name = test_file_prefix + "d_addprev"
            sqlite_cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
            if count_findings_before > 0:
                if l_debug:
                    print("Join previously reported to current findings...")
                today = funcdatn.get_today_date()
                next_test_date = funcdatn.get_current_month_end_next()
                s_sql = f"CREATE TABLE {table_name} As" + f"""
                Select
                    f.*,
                    Lower('{test_finding}') AS PROCESS,
                    '{today}' AS DATE_REPORTED,
                    '{next_test_date}' AS DATE_RETEST,
                    p.PROCESS AS PREV_PROCESS,
                    p.DATE_REPORTED AS PREV_DATE_REPORTED,
                    p.DATE_RETEST AS PREV_DATE_RETEST,
                    p.REMARK
                From
                    {test_file_prefix}b_finding f Left Join
                    Z001ab_setprev p On
                    p.FIELD1 = f.nwu_number And
                    p.FIELD2 = f.company_registration_number And
                    p.FIELD3 = f.vendor_id
                ;"""
                if l_debug:
                    # print(s_sql)
                    pass
                sqlite_cursor.execute(s_sql)
                sqlite_connection.commit()
                funcfile.writelog(f"%t BUILD TABLE: {table_name}")

            # Build table to update findings
            table_name = test_file_prefix + "e_newprev"
            sqlite_cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
            if count_findings_before > 0:
                s_sql = f"CREATE TABLE {table_name} As " + f"""
                Select
                    p.PROCESS,
                    p.nwu_number AS FIELD1,
                    p.company_registration_number AS FIELD2,
                    p.vendor_id AS FIELD3,
                    p.name_address AS FIELD4,
                    p.company_name AS FIELD5,
                    p.DATE_REPORTED,
                    p.DATE_RETEST,
                    p.REMARK
                From
                    {test_file_prefix}d_addprev p
                Where
                    p.PREV_PROCESS Is Null Or
                    p.DATE_REPORTED > p.PREV_DATE_RETEST And p.REMARK = ""        
                ;"""
                sqlite_cursor.execute(s_sql)
                sqlite_connection.commit()
                funcfile.writelog(f"%t BUILD TABLE: {table_name}")
                # Export findings to previous reported file
                count_findings_after = funcsys.tablerowcount(sqlite_cursor, table_name)
                if count_findings_after > 0:
                    if l_debug:
                        print("*** " + str(count_findings_after) + " Finding(s) to report ***")
                    sx_path = external_data_path
                    sx_file = test_report_file[:-4]
                    # Read the header data
                    s_head = funccsv.get_colnames_sqlite(sqlite_connection, table_name)
                    # Write the data
                    l_record_temporary: bool = False
                    if l_record and l_record_temporary:
                        funccsv.write_data(sqlite_connection, "main", table_name, sx_path, sx_file, s_head, "a", ".txt")
                        funcfile.writelog("%t FINDING: " + str(count_findings_after) + " new finding(s) to export")
                        funcfile.writelog(f"%t EXPORT DATA: {table_name}")
                    if l_mess:
                        funcsms.send_telegram('', 'administrator', '<b>' + str(count_findings_before) + '/' + str(
                            count_findings_after) + '</b> ' + test_description)
                else:
                    if l_debug:
                        print("*** No new findings to report ***")
                    funcfile.writelog("%t FINDING: No new findings to export")

            # Import officers for reporting purposes
            if count_findings_before > 0 and count_findings_after > 0:
                functest.get_officer(sqlite_cursor, "HR", f"TEST {test_finding} OFFICER")
                sqlite_connection.commit()

            # Import supervisors for reporting purposes
            if count_findings_before > 0 and count_findings_after > 0:
                functest.get_supervisor(sqlite_cursor, "HR", f"TEST {test_finding} SUPERVISOR")
                sqlite_connection.commit()

            # Add contact and other details needed to findings
            table_name = test_file_prefix + "h_detail"
            sqlite_cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
            if count_findings_before > 0 and count_findings_after > 0:
                if l_debug:
                    print("Add contact details to findings...")
                s_sql = f"CREATE TABLE {table_name} As " + f"""
                Select
                    p.org,
                    p.vendor_type,
                    p.company_registration_number,
                    p.company_name,
                    d.company_status,
                    p.vendor_match_description,
                    p.vendor_id,
                    k.VENDOR_NAME As vendor_name,
                    k.TRAN_COUNT As transaction_count,
                    k.NET_PMT_AMT As total_payment_amount,
                    k.LAST_PMT_DT As last_payment_date,
                    -- Employee                
                    p.nwu_number As emp_number,
                    e.name_address As emp_name,
                    e.user_person_type As emp_person_type,
                    e.position_name As emp_position,
                    Lower(e.email_address) As emp_mail1,
                    p.nwu_number || '@nwu.ac.za' As emp_mail2,
                    -- Supervisor
                    e.supervisor_number As sup_number,
                    s.name_address As sup_name,
                    Lower(s.email_address) As sup_mail1,
                    e.supervisor_number || '@nwu.ac.za' As sup_mail2,
                    -- Next level supervisor
                    s.supervisor_number As sup2_number,
                    n.name_address As sup2_name,
                    Lower(n.email_address) As sup2_mail1,
                    s.supervisor_number || '@nwu.ac.za' As sup2_mail2,
                    -- Campus officer / responsible officer
                    oc.EMPLOYEE_NUMBER As campus_officer_number,
                    oc.NAME_ADDR As campus_officer_name,
                    oc.EMAIL_ADDRESS As campus_officer_mail1,        
                    Case
                        When  oc.EMPLOYEE_NUMBER != '' Then oc.EMPLOYEE_NUMBER||'@nwu.ac.za'
                        Else oc.EMAIL_ADDRESS
                    End As campus_officer_mail2,
                    -- Campus supervisor
                    sc.EMPLOYEE_NUMBER As campus_supervisor_number,
                    sc.NAME_ADDR As campus_supervisor_name,
                    sc.EMAIL_ADDRESS As campus_supervisor_mail1,        
                    Case
                        When sc.EMPLOYEE_NUMBER != '' Then sc.EMPLOYEE_NUMBER||'@nwu.ac.za'
                        Else sc.EMAIL_ADDRESS
                    End As campus_supervisor_mail2,
                    -- Organization officer
                    oo.EMPLOYEE_NUMBER As organization_officer_number,
                    oo.NAME_ADDR As organization_officer_name,
                    oo.EMAIL_ADDRESS As organization_officer_mail1,        
                    Case
                        When  oo.EMPLOYEE_NUMBER != '' Then oo.EMPLOYEE_NUMBER||'@nwu.ac.za'
                        Else oo.EMAIL_ADDRESS
                    End As organization_officer_mail2,
                    -- Campus supervisor
                    so.EMPLOYEE_NUMBER As organization_supervisor_number,
                    so.NAME_ADDR As organization_supervisor_name,
                    so.EMAIL_ADDRESS As organization_supervisor_mail1,        
                    Case
                        When so.EMPLOYEE_NUMBER != '' Then so.EMPLOYEE_NUMBER||'@nwu.ac.za'
                        Else so.EMAIL_ADDRESS
                    End As organization_supervisor_mail2,
                    -- Auditor
                    oa.EMPLOYEE_NUMBER As audit_officer_number,
                    oa.NAME_ADDR As audit_officer_name,
                    oa.EMAIL_ADDRESS As audit_officer_mail1,        
                    Case
                        When  oa.EMPLOYEE_NUMBER != '' Then oa.EMPLOYEE_NUMBER||'@nwu.ac.za'
                        Else oa.EMAIL_ADDRESS
                    End As audit_officer_mail2,
                    -- Audit supervisor
                    sa.EMPLOYEE_NUMBER As audit_supervisor_number,
                    sa.NAME_ADDR As audit_supervisor_name,
                    sa.EMAIL_ADDRESS As audit_supervisor_mail1,        
                    Case
                        When sa.EMPLOYEE_NUMBER != '' Then sa.EMPLOYEE_NUMBER||'@nwu.ac.za'
                        Else sa.EMAIL_ADDRESS
                    End As audit_supervisor_mail2
                From
                    {test_file_prefix}d_addprev p Left Join
                    X004x_searchworks_directors d On d.nwu_number = p.nwu_number And d.registration_number = p.company_registration_number Left Join
                    PEOPLE.X000_PEOPLE e On e.employee_number = p.nwu_number Left Join
                    PEOPLE.X000_PEOPLE s On s.employee_number = e.supervisor_number Left Join
                    PEOPLE.X000_PEOPLE n On n.employee_number = s.supervisor_number Left Join
                    KFSCURR.X002aa_Report_payments_summary k On k.vendor_id = p.vendor_id Left join
                    Z001af_officer oc On oc.CAMPUS = p.vendor_type Left Join
                    Z001af_officer oo On oo.CAMPUS = p.org Left Join
                    Z001af_officer oa On oa.CAMPUS = 'AUD' Left Join
                    Z001ag_supervisor sc On sc.CAMPUS = p.vendor_type Left Join
                    Z001ag_supervisor so On so.CAMPUS = p.org Left Join
                    Z001ag_supervisor sa On sa.CAMPUS = 'AUD'
                Where
                    p.PREV_PROCESS Is Null Or
                    p.DATE_REPORTED > p.PREV_DATE_RETEST And p.REMARK = ""
                ;"""
                sqlite_cursor.execute(s_sql)
                sqlite_connection.commit()
                funcfile.writelog(f"%t BUILD TABLE: {table_name}")

            # Build the final table for export and reporting
            table_name = test_file_prefix + "x_" + test_file_name
            sqlite_cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
            if l_debug:
                print("Build the final report")
            if count_findings_before > 0 and count_findings_after > 0:
                s_sql = f"CREATE TABLE {table_name} As " + f"""
                Select
                    '{test_finding}' As Audit_finding,
                    f.emp_number As Employee_nwu,
                    f.emp_name As Employee,
                    f.emp_person_type As Employee_person_type,
                    f.emp_position As Employee_position,
                    f.emp_mail1 As Employee_mail1,
                    f.emp_mail2 As Employee_mail2,
                    'Active' As CIPC_director,
                    f.company_registration_number As CIPC_regno,
                    f.company_name As CIPC_company,
                    f.company_status As CIPC_company_status,
                    f.org As Organization,
                    f.vendor_match_description As Vendor_match_by,
                    f.vendor_type As NWU_Vendor_type,
                    f.vendor_id As NWU_vendor_id,
                    f.vendor_name As NWU_vendor,
                    f.transaction_count As Transaction_count,
                    f.total_payment_amount As Transaction_total_amount,
                    f.last_payment_date As Transaction_last_date,
                    f.sup_name As Supervisor,
                    f.sup2_name As Supervisor_next,
                    f.campus_officer_name As Responsible_officer,
                    f.campus_supervisor_name As Responsible_supervisor,
                    f.organization_officer_name As Organization_officer,
                    f.organization_supervisor_name As Organization_supervisor,
                    f.audit_officer_name As Audit_officer,
                    f.audit_supervisor_name As Audit_supervisor,
                    f.sup_number As Supervisor_nwu,
                    f.sup_mail1 As Supervisor_mail1,
                    f.sup_mail2 As Supervisor_mail2,
                    f.sup2_number As Supervisor_next_nwu,
                    f.sup2_mail1 As Supervisor_next_mail1,
                    f.sup2_mail2 As Supervisor_next_mail2,
                    f.campus_officer_number As Responsible_officer_nwu,
                    f.campus_officer_mail1 As Responsible_officer_mail1,
                    f.campus_officer_mail2 As Responsible_officer_mail2,
                    f.campus_supervisor_number As Responsible_supervisor_nwu,
                    f.campus_supervisor_mail1 As Responsible_supervisor_mail1,
                    f.campus_supervisor_mail2 As Responsible_supervisor_mail2,
                    f.organization_officer_number As Organization_officer_nwu,
                    f.organization_officer_mail1 As Organization_officer_mail1,
                    f.organization_officer_mail2 As Organization_officer_mail2,
                    f.organization_supervisor_number As Organization_supervisor_nwu,
                    f.organization_supervisor_mail1 As Organization_supervisor_mail1,
                    f.organization_supervisor_mail2 As Organization_supervisor_mail2,
                    f.audit_officer_number As Audit_officer_nwu,
                    f.audit_officer_mail1 As Audit_officer_mail1,
                    f.audit_officer_mail2 As Audit_officer_mail2,
                    f.audit_supervisor_number As Audit_supervisor_nwu,
                    f.audit_supervisor_mail1 As Audit_supervisor_mail1,
                    f.audit_supervisor_mail2 As Audit_supervisor_mail2                
                From
                    {test_file_prefix}h_detail f
                ;"""
                sqlite_cursor.execute(s_sql)
                sqlite_connection.commit()
                funcfile.writelog(f"%t BUILD TABLE: {table_name}")
                # Export findings
                if l_export and funcsys.tablerowcount(sqlite_cursor, table_name) > 0:
                    print("Export findings...")
                    sx_path = results_path
                    sx_file = test_file_prefix + "_" + test_finding.lower() + "_"
                    sx_file_dated = sx_file + funcdatn.get_today_date_file()
                    s_head = funccsv.get_colnames_sqlite(sqlite_connection, table_name)
                    funccsv.write_data(sqlite_connection, "main", table_name, sx_path, sx_file, s_head)
                    funccsv.write_data(sqlite_connection, "main", table_name, sx_path, sx_file_dated, s_head)
                    funcfile.writelog(f"%t EXPORT DATA: {sx_path}{sx_file}")

            else:

                s_sql = f"CREATE TABLE {table_name} (" + """
                BLANK TEXT
                );"""
                sqlite_cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
                sqlite_cursor.execute(s_sql)
                sqlite_connection.commit()
                funcfile.writelog(f"%t BUILD TABLE: {table_name}")

    """*****************************************************************************
    TEST ACTIVE CIPC DIRECTOR LIST
    *****************************************************************************"""

    """ DESCRIPTION
    A test to distribute a list of all active CIPC directorships to employees FYI to take into account when
    declaring interests.
    """

    """ INDEX
    """

    """" TABLES USED IN TEST
    """

    # DECLARE TEST VARIABLES
    count_findings_after: int = 0
    test_description = "Active CIPC director list"
    test_file_name: str = "active_cipc_director_list"
    test_file_prefix: str = "X200a"
    test_finding: str = "ACTIVE CIPC DIRECTOR LIST"
    test_report_file: str = "002_reported.txt"

    # OBTAIN TEST RUN FLAG
    if not functest.get_test_flag(sqlite_cursor, "HR", f"TEST {test_finding}", "RUN"):

        if l_debug:
            print('TEST DISABLED')
        funcfile.writelog("TEST " + test_finding + " DISABLED")

    else:

        # OPEN LOG
        if l_debug:
            print("TEST " + test_finding)
        funcfile.writelog("TEST " + test_finding)

        # Fetch initial data from the master table
        if l_debug:
            print("Fetch initial data from the master table...")
        table_name = test_file_prefix + f"a_{test_file_name}"
        s_sql = f"CREATE TABLE {table_name} As " + """
        Select
            d.nwu_number,
            d.employee_name,
            d.national_identifier,
            d.user_person_type,
            d.position_name,
            d.date_submitted,
            d.import_date,
            d.registration_number,
            d.company_name,
            d.enterprise_type,
            d.company_status,
            d.history_date,
            d.business_start_date,
            d.directorship_status,
            d.directorship_start_date
        From
            X004x_searchworks_directors d
        ;"""
        if l_debug:
            # print(s_sql)
            pass
        sqlite_cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
        sqlite_cursor.execute(s_sql)
        sqlite_connection.commit()
        funcfile.writelog(f"%t BUILD TABLE: {table_name}")

        # Read the list employees and companies to exclude from the test
        # This lookup section moved to the B011 Searchworks script as from 2024-04-18

        # Select the test data
        # Directorship data match with current active vendor but no declaration
        # Match types 0 = No match in declaration
        #             1 = Match on company registration number
        #             2 = Match on company name
        if l_debug:
            print("Identify findings...")
        table_name = test_file_prefix + "b_finding"
        s_sql = f"CREATE TABLE {table_name} As " + f"""
        Select
            'NWU' As org,
            f.nwu_number,
            f.registration_number,
            f.employee_name,
            f.company_name
        From
            {test_file_prefix}a_{test_file_name} f
        ;"""
        if l_debug:
            # print(s_sql)
            pass
        sqlite_cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
        sqlite_cursor.execute(s_sql)
        sqlite_connection.commit()
        funcfile.writelog(f"%t BUILD TABLE: {table_name}")

        # Count the number of findings
        count_findings_before: int = funcsys.tablerowcount(sqlite_cursor, table_name)
        if l_debug:
            print("*** Found " + str(count_findings_before) + " exceptions ***")
        funcfile.writelog("%t FINDING: " + str(count_findings_before) + " " + test_finding + " finding(s)")

        # Get previous findings
        if count_findings_before > 0:
            functest.get_previous_finding(sqlite_cursor, external_data_path, test_report_file, test_finding, "TTTTT")
            sqlite_connection.commit()

        # Set previous findings
        if count_findings_before > 0:
            functest.set_previous_finding(sqlite_cursor)
            sqlite_connection.commit()

        # Add previous findings
        table_name = test_file_prefix + "d_addprev"
        sqlite_cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
        if count_findings_before > 0:
            if l_debug:
                print("Join previously reported to current findings...")
            today = funcdatn.get_today_date()
            next_test_date = funcdatn.get_current_year_end()
            s_sql = f"CREATE TABLE {table_name} As" + f"""
            Select
                f.*,
                Lower('{test_finding}') AS PROCESS,
                '{today}' AS DATE_REPORTED,
                '{next_test_date}' AS DATE_RETEST,
                p.PROCESS AS PREV_PROCESS,
                p.DATE_REPORTED AS PREV_DATE_REPORTED,
                p.DATE_RETEST AS PREV_DATE_RETEST,
                p.REMARK
            From
                {test_file_prefix}b_finding f Left Join
                Z001ab_setprev p On
                p.FIELD1 = f.nwu_number And
                p.FIELD2 = f.registration_number
            ;"""
            if l_debug:
                # print(s_sql)
                pass
            sqlite_cursor.execute(s_sql)
            sqlite_connection.commit()
            funcfile.writelog(f"%t BUILD TABLE: {table_name}")

        # Build table to update findings
        table_name = test_file_prefix + "e_newprev"
        sqlite_cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
        if count_findings_before > 0:
            s_sql = f"CREATE TABLE {table_name} As " + f"""
            Select
                p.PROCESS,
                p.nwu_number AS FIELD1,
                p.registration_number AS FIELD2,
                p.employee_name AS FIELD3,
                p.company_name AS FIELD4,
                '' AS FIELD5,
                p.DATE_REPORTED,
                p.DATE_RETEST,
                p.REMARK
            From
                {test_file_prefix}d_addprev p
            Where
                p.PREV_PROCESS Is Null Or
                p.DATE_REPORTED > p.PREV_DATE_RETEST And p.REMARK = ""        
            ;"""
            sqlite_cursor.execute(s_sql)
            sqlite_connection.commit()
            funcfile.writelog(f"%t BUILD TABLE: {table_name}")
            # Export findings to previous reported file
            count_findings_after = funcsys.tablerowcount(sqlite_cursor, table_name)
            if count_findings_after > 0:
                if l_debug:
                    print("*** " + str(count_findings_after) + " Finding(s) to report ***")
                sx_path = external_data_path
                sx_file = test_report_file[:-4]
                # Read the header data
                s_head = funccsv.get_colnames_sqlite(sqlite_connection, table_name)
                # Write the data
                l_record_temporary: bool = True
                if l_record and l_record_temporary:
                    funccsv.write_data(sqlite_connection, "main", table_name, sx_path, sx_file, s_head, "a", ".txt")
                    funcfile.writelog("%t FINDING: " + str(count_findings_after) + " new finding(s) to export")
                    funcfile.writelog(f"%t EXPORT DATA: {table_name}")
                if l_mess:
                    funcsms.send_telegram('', 'administrator', '<b>' + str(count_findings_before) + '/' + str(
                        count_findings_after) + '</b> ' + test_description)
            else:
                if l_debug:
                    print("*** No new findings to report ***")
                funcfile.writelog("%t FINDING: No new findings to export")

        # Import officers for reporting purposes
        if count_findings_before > 0 and count_findings_after > 0:
            functest.get_officer(sqlite_cursor, "HR", f"TEST {test_finding} OFFICER")
            sqlite_connection.commit()

        # Import supervisors for reporting purposes
        if count_findings_before > 0 and count_findings_after > 0:
            functest.get_supervisor(sqlite_cursor, "HR", f"TEST {test_finding} SUPERVISOR")
            sqlite_connection.commit()

        # Add contact and other details needed to findings
        table_name = test_file_prefix + "h_detail"
        sqlite_cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
        if count_findings_before > 0 and count_findings_after > 0:
            if l_debug:
                print("Add contact details to findings...")
            s_sql = f"CREATE TABLE {table_name} As " + f"""
            Select
                p.org,
                'OTHER' As vendor_type,
                p.nwu_number,
                p.employee_name,
                e.user_person_type As emp_person_type,
                e.position_name As emp_position,
                p.registration_number,
                p.company_name,
                d.company_status,
                Lower(e.email_address) As emp_mail1,
                p.nwu_number || '@nwu.ac.za' As emp_mail2,
                -- Supervisor
                e.supervisor_number As sup_number,
                s.name_address As sup_name,
                Lower(s.email_address) As sup_mail1,
                e.supervisor_number || '@nwu.ac.za' As sup_mail2,
                -- Next level supervisor
                s.supervisor_number As sup2_number,
                n.name_address As sup2_name,
                Lower(n.email_address) As sup2_mail1,
                s.supervisor_number || '@nwu.ac.za' As sup2_mail2,
                -- Campus officer / responsible officer
                oc.EMPLOYEE_NUMBER As campus_officer_number,
                oc.NAME_ADDR As campus_officer_name,
                oc.EMAIL_ADDRESS As campus_officer_mail1,        
                Case
                    When  oc.EMPLOYEE_NUMBER != '' Then oc.EMPLOYEE_NUMBER||'@nwu.ac.za'
                    Else oc.EMAIL_ADDRESS
                End As campus_officer_mail2,
                -- Campus supervisor
                sc.EMPLOYEE_NUMBER As campus_supervisor_number,
                sc.NAME_ADDR As campus_supervisor_name,
                sc.EMAIL_ADDRESS As campus_supervisor_mail1,        
                Case
                    When sc.EMPLOYEE_NUMBER != '' Then sc.EMPLOYEE_NUMBER||'@nwu.ac.za'
                    Else sc.EMAIL_ADDRESS
                End As campus_supervisor_mail2,
                -- Organization officer
                oo.EMPLOYEE_NUMBER As organization_officer_number,
                oo.NAME_ADDR As organization_officer_name,
                oo.EMAIL_ADDRESS As organization_officer_mail1,        
                Case
                    When  oo.EMPLOYEE_NUMBER != '' Then oo.EMPLOYEE_NUMBER||'@nwu.ac.za'
                    Else oo.EMAIL_ADDRESS
                End As organization_officer_mail2,
                -- Campus supervisor
                so.EMPLOYEE_NUMBER As organization_supervisor_number,
                so.NAME_ADDR As organization_supervisor_name,
                so.EMAIL_ADDRESS As organization_supervisor_mail1,        
                Case
                    When so.EMPLOYEE_NUMBER != '' Then so.EMPLOYEE_NUMBER||'@nwu.ac.za'
                    Else so.EMAIL_ADDRESS
                End As organization_supervisor_mail2,
                -- Auditor
                oa.EMPLOYEE_NUMBER As audit_officer_number,
                oa.NAME_ADDR As audit_officer_name,
                oa.EMAIL_ADDRESS As audit_officer_mail1,        
                Case
                    When  oa.EMPLOYEE_NUMBER != '' Then oa.EMPLOYEE_NUMBER||'@nwu.ac.za'
                    Else oa.EMAIL_ADDRESS
                End As audit_officer_mail2,
                -- Audit supervisor
                sa.EMPLOYEE_NUMBER As audit_supervisor_number,
                sa.NAME_ADDR As audit_supervisor_name,
                sa.EMAIL_ADDRESS As audit_supervisor_mail1,        
                Case
                    When sa.EMPLOYEE_NUMBER != '' Then sa.EMPLOYEE_NUMBER||'@nwu.ac.za'
                    Else sa.EMAIL_ADDRESS
                End As audit_supervisor_mail2
            From
                {test_file_prefix}d_addprev p Left Join
                X004x_searchworks_directors d On d.nwu_number = p.nwu_number And d.registration_number = p.registration_number Left Join
                PEOPLE.X000_PEOPLE e On e.employee_number = p.nwu_number Left Join
                PEOPLE.X000_PEOPLE s On s.employee_number = e.supervisor_number Left Join
                PEOPLE.X000_PEOPLE n On n.employee_number = s.supervisor_number Left Join
                Z001af_officer oc On oc.CAMPUS = 'OTHER' Left Join
                Z001af_officer oo On oo.CAMPUS = p.org Left Join
                Z001af_officer oa On oa.CAMPUS = 'AUD' Left Join
                Z001ag_supervisor sc On sc.CAMPUS = 'OTHER' Left Join
                Z001ag_supervisor so On so.CAMPUS = p.org Left Join
                Z001ag_supervisor sa On sa.CAMPUS = 'AUD'
            Where
                p.PREV_PROCESS Is Null Or
                p.DATE_REPORTED > p.PREV_DATE_RETEST And p.REMARK = ""
            ;"""
            sqlite_cursor.execute(s_sql)
            sqlite_connection.commit()
            funcfile.writelog(f"%t BUILD TABLE: {table_name}")

        # Build the final table for export and reporting
        table_name = test_file_prefix + "x_" + test_file_name
        sqlite_cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
        if l_debug:
            print("Build the final report")
        if count_findings_before > 0 and count_findings_after > 0:
            s_sql = f"CREATE TABLE {table_name} As " + f"""
            Select
                '{test_finding}' As Audit_finding,
                f.nwu_number || '-' || f.registration_number As Unique_id,
                f.nwu_number As Employee_nwu,
                f.employee_name As Employee,
                f.emp_person_type As Employee_person_type,
                f.emp_position As Employee_position,
                f.emp_mail1 As Employee_mail1,
                f.emp_mail2 As Employee_mail2,
                'Active' As CIPC_director,
                f.registration_number As CIPC_regno,
                f.company_name As CIPC_company,
                f.company_status As CIPC_company_status,
                f.org As Organization,
                f.sup_name As Supervisor,
                f.sup2_name As Supervisor_next,
                f.campus_officer_name As Responsible_officer,
                f.campus_supervisor_name As Responsible_supervisor,
                f.organization_officer_name As Organization_officer,
                f.organization_supervisor_name As Organization_supervisor,
                f.audit_officer_name As Audit_officer,
                f.audit_supervisor_name As Audit_supervisor,
                f.sup_number As Supervisor_nwu,
                f.sup_mail1 As Supervisor_mail1,
                f.sup_mail2 As Supervisor_mail2,
                f.sup2_number As Supervisor_next_nwu,
                f.sup2_mail1 As Supervisor_next_mail1,
                f.sup2_mail2 As Supervisor_next_mail2,
                f.campus_officer_number As Responsible_officer_nwu,
                f.campus_officer_mail1 As Responsible_officer_mail1,
                f.campus_officer_mail2 As Responsible_officer_mail2,
                f.campus_supervisor_number As Responsible_supervisor_nwu,
                f.campus_supervisor_mail1 As Responsible_supervisor_mail1,
                f.campus_supervisor_mail2 As Responsible_supervisor_mail2,
                f.organization_officer_number As Organization_officer_nwu,
                f.organization_officer_mail1 As Organization_officer_mail1,
                f.organization_officer_mail2 As Organization_officer_mail2,
                f.organization_supervisor_number As Organization_supervisor_nwu,
                f.organization_supervisor_mail1 As Organization_supervisor_mail1,
                f.organization_supervisor_mail2 As Organization_supervisor_mail2,
                f.audit_officer_number As Audit_officer_nwu,
                f.audit_officer_mail1 As Audit_officer_mail1,
                f.audit_officer_mail2 As Audit_officer_mail2,
                f.audit_supervisor_number As Audit_supervisor_nwu,
                f.audit_supervisor_mail1 As Audit_supervisor_mail1,
                f.audit_supervisor_mail2 As Audit_supervisor_mail2                
            From
                {test_file_prefix}h_detail f
            ;"""
            sqlite_cursor.execute(s_sql)
            sqlite_connection.commit()
            funcfile.writelog(f"%t BUILD TABLE: {table_name}")
            # Export findings
            if l_export and funcsys.tablerowcount(sqlite_cursor, table_name) > 0:
                print("Export findings...")
                sx_path = results_path
                sx_file = test_file_prefix + "_" + test_finding.lower() + "_"
                sx_file_dated = sx_file + funcdatn.get_today_date_file()
                s_head = funccsv.get_colnames_sqlite(sqlite_connection, table_name)
                funccsv.write_data(sqlite_connection, "main", table_name, sx_path, sx_file, s_head)
                funccsv.write_data(sqlite_connection, "main", table_name, sx_path, sx_file_dated, s_head)
                funcfile.writelog(f"%t EXPORT DATA: {sx_path}{sx_file}")

        else:

            s_sql = f"CREATE TABLE {table_name} (" + """
            BLANK TEXT
            );"""
            sqlite_cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
            sqlite_cursor.execute(s_sql)
            sqlite_connection.commit()
            funcfile.writelog(f"%t BUILD TABLE: {table_name}")

    """*****************************************************************************
    END OF SCRIPT
    *****************************************************************************"""
    print("END OF SCRIPT")
    funcfile.writelog("END OF SCRIPT")

    # CLOSE THE DATABASE CONNECTION
    sqlite_connection.close()

    # CLOSE THE LOG WRITER
    funcfile.writelog("----------------------------------------")
    funcfile.writelog("COMPLETED: C002_PEOPLE_TEST_CONFLICT_DEV")

    return


if __name__ == '__main__':
    try:
        people_test_conflict()
    except Exception as e:
        funcsys.ErrMessage(e)
