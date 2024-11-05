""" Script to prepare PEOPLE master file lists
Created on: 12 Jun 2019
Author: Albert Janse van Rensburg (NWU21162395)
"""

# IMPORT SYSTEM MODULES
import sqlite3

# OPEN OWN MODULES
from _my_modules import funcconf
from _my_modules import funcdatn
from _my_modules import funcfile
from _my_modules import funcsms
from _my_modules import funcsys


""" INDEX **********************************************************************
ENVIRONMENT
OPEN THE DATABASES
BEGIN OF SCRIPT
LIST ENTRY EXIT DATES MASTER
LIST AGE MASTER
END OF SCRIPT
*****************************************************************************"""


def people_list_masterfile():
    """
    Script to build PEOPLE master file lists
    :return: Nothing
    """

    """*****************************************************************************
    ENVIRONMENT
    *****************************************************************************"""

    # DECLARE VARIABLES
    so_path = f"{funcconf.drive_data_raw}People/"  # Source database path
    so_file = "People_list_masterfile.sqlite"  # Source database

    # OPEN THE LOG
    print("---------------------------")
    print("C003_PEOPLE_LIST_MASTERFILE")
    print("---------------------------")
    funcfile.writelog("Now")
    funcfile.writelog("SCRIPT: C003_PEOPLE_LIST_MASTERFILE")
    funcfile.writelog("-----------------------------------")

    # MESSAGE
    if funcconf.l_mess_project:
        funcsms.send_telegram("", "administrator", "<b>C003 People lists</b>")

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

    """ ****************************************************************************
    BEGIN OF SCRIPT
    *****************************************************************************"""
    print("BEGIN OF SCRIPT")
    funcfile.writelog("BEGIN OF SCRIPT")

    """ ****************************************************************************
    LIST ENTRY EXIT DATES MASTER
    *****************************************************************************"""
    print("ENTRY EXIT DATES MASTER")
    funcfile.writelog("ENTRY EXIT DATES MASTER")

    # BUILD PEOPLE START AND END DATE MASTER TABLE
    print("Build table for entry date analysis...")
    sr_file: str = "X001_People_start_end_master"
    s_sql = "CREATE TABLE " + sr_file + " As " + """
    Select
        PEOP.EMPLOYEE_NUMBER AS EMP,
        PEOP.NAME_LIST AS NAME,
        CASE
            WHEN PEOP.NATIONALITY_NAME = 'SOUTH AFRICA' THEN PEOP.NATIONALITY_NAME
            ELSE 'FOREIGN'
        END AS NATIONALITY,
        PEOP.SEX As GENDER,
        PEOP.RACE_DESC AS RACE,
        'START' As DATE_TYPE,
        PEOP.EMP_START As DATE,
        '' As LEAVING_REASON,
        '' As LEAVE_REASON_DESCRIP,
        PEOP.LOCATION_DESCRIPTION AS CAMPUS,
        PEOP.ACAD_SUPP,
        CASE
            WHEN PEOP.FACULTY <> '' THEN PEOP.FACULTY
            ELSE 'SUPPORT'
        END AS FACULTY,
        PEOP.EMPLOYMENT_CATEGORY AS PERM_TEMP,
        PEOP.DIVISION,
        PEOP.GRADE,
        Substr(PEOP.GRADE_CALC,1,3) As GRADE_CALC,
        PEOP.POSITION_NAME,
        PEOP.JOB_NAME,
        PEOP.PERSON_TYPE,
        Strftime('%m', PEOP.EMP_START) As MONTH,
        Cast(1 As Int) As COUNT
    From
        PEOPLE.X002_PEOPLE_CURR_YEAR PEOP
    Where
        Substr(PEOP.LEAVE_REASON_DESCRIP,1,6) <> 'AD HOC' And
        Substr(PEOP.PERSON_TYPE,1,6) <> 'AD HOC' And
        Strftime('%Y', PEOP.EMP_START) = '%CYEAR%'
    """
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = s_sql.replace("%CYEAR%", funcdatn.get_current_year())
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: X001_People_start_end_master")

    # BUILD PEOPLE START AND END DATE MASTER TABLE
    print("Insert into table exit date analysis...")
    sr_file: str = "X001_People_start_end_master"
    s_sql = "INSERT INTO " + sr_file + " " + """
    Select
        PEOP.EMPLOYEE_NUMBER AS EMP,
        PEOP.NAME_LIST AS NAME,
        CASE
            WHEN PEOP.NATIONALITY_NAME = 'SOUTH AFRICA' THEN PEOP.NATIONALITY_NAME
            ELSE 'FOREIGN'
        END AS NATIONALITY,        
        PEOP.SEX AS GENDER,
        PEOP.RACE_DESC AS RACE,
        'END' As DATE_TYPE,
        PEOP.EMP_END As DATE,
        PEOP.LEAVING_REASON,
        PEOP.LEAVE_REASON_DESCRIP,
        PEOP.LOCATION_DESCRIPTION AS CAMPUS,
        PEOP.ACAD_SUPP,
        CASE
            WHEN PEOP.FACULTY <> '' THEN PEOP.FACULTY
            ELSE 'SUPPORT'
        END AS FACULTY,
        PEOP.EMPLOYMENT_CATEGORY AS PERM_TEMP,
        PEOP.DIVISION,
        PEOP.GRADE,
        Substr(PEOP.GRADE_CALC,1,3) As GRADE_CALC,
        PEOP.POSITION_NAME,
        PEOP.JOB_NAME,
        PEOP.PERSON_TYPE,
        Strftime('%m', PEOP.EMP_END) As MONTH,
        Cast(-1 As Int) As COUNT
    From
        PEOPLE.X002_PEOPLE_CURR_YEAR PEOP
    Where
        Substr(PEOP.LEAVE_REASON_DESCRIP,1,6) <> 'AD HOC' And
        Substr(PEOP.PERSON_TYPE,1,6) <> 'AD HOC' And
        Strftime('%Y', PEOP.EMP_END) = '%CYEAR%' And
        PEOP.EMP_END < Date('%TODAY%')
    """
    s_sql = s_sql.replace("%CYEAR%", funcdatn.get_current_year())
    s_sql = s_sql.replace("%TODAY%", funcdatn.get_current_month_end())
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: X001_People_start_end_master")
    if funcconf.l_mess_project:
        i = funcsys.tablerowcount(so_curs, sr_file)
        funcsms.send_telegram("", "administrator", "<b>" + str(i) + "</b> Exits")

    """ ****************************************************************************
    LIST AGE MASTER
    *****************************************************************************"""
    print("AGE MASTER")
    funcfile.writelog("AGE MASTER")

    # BUILD PEOPLE START AND END DATE MASTER TABLE
    print("Build table for age analysis...")
    sr_file: str = "X002_People_age_master"
    s_sql = "CREATE TABLE " + sr_file + " As " + """
    Select
        PEOP.EMPLOYEE_NUMBER AS EMP,
        PEOP.NAME_ADDR AS NAME,
        PEOP.EMPLOYMENT_CATEGORY AS PERM_TEMP,
        PEOP.ACAD_SUPP,
        PEOP.SEX As GENDER,
        PEOP.RACE_DESC AS RACE,
        PEOP.PERSON_TYPE,
        PEOP.GRADE,
        Substr(PEOP.GRADE_CALC,1,3) As GRADE_CALC,
        CASE
            WHEN PEOP.NATIONALITY_NAME = 'SOUTH AFRICA' THEN PEOP.NATIONALITY_NAME
            ELSE 'FOREIGN'
        END AS NATIONALITY,
        PEOP.NATIONALITY_NAME,
        PEOP.LOCATION_DESCRIPTION AS CAMPUS,
        CASE
            WHEN PEOP.FACULTY <> '' THEN PEOP.FACULTY
            ELSE 'SUPPORT'
        END AS FACULTY,
        CASE
            WHEN PEOP.FACULTY <> '' THEN PEOP.FACULTY
            WHEN PEOP.DIVISION <> '' THEN PEOP.DIVISION
            ELSE 'SUPPORT'
        END AS WORKPLACE,
        PEOP.DIVISION,
        PEOP.ORG_NAME,
        PEOP.POSITION_NAME,
        PEOP.JOB_NAME,
        PEOP.DATE_OF_BIRTH,
        PEOP.AGE,
        CASE
            When PEOP.AGE >= 10 And PEOP.AGE <= 20 Then '00-20'
            When PEOP.AGE >= 21 And PEOP.AGE <= 25 Then '21-25'
            When PEOP.AGE >= 26 And PEOP.AGE <= 30 Then '26-30'
            When PEOP.AGE >= 31 And PEOP.AGE <= 35 Then '31-35'
            When PEOP.AGE >= 36 And PEOP.AGE <= 40 Then '36-40'
            When PEOP.AGE >= 41 And PEOP.AGE <= 45 Then '41-45'
            When PEOP.AGE >= 46 And PEOP.AGE <= 50 Then '46-50'
            When PEOP.AGE >= 51 And PEOP.AGE <= 55 Then '51-55'
            When PEOP.AGE >= 56 And PEOP.AGE <= 60 Then '56-60'
            When PEOP.AGE >= 61 And PEOP.AGE <= 65 Then '61-65'
            When PEOP.AGE >= 66 And PEOP.AGE <= 70 Then '66-70'
            Else '71-99'
        END As AGE_GROUP,
        Cast(1 As Int) As COUNT,
        PEOP.EMAIL_ADDRESS,
        CASE
            When PEOP.KNOWN_NAME = '' Then 'COLLEAGUE'
            Else Replace(Lower(PEOP.TITLE_FULL),'.','')||'  '||PEOP.KNOWN_NAME
        END AS KNOWN_NAME
    From
        PEOPLE.X002_PEOPLE_CURR PEOP
    Where
        Substr(PEOP.PERSON_TYPE,1,6) <> 'AD HOC'
    Order By
        PEOP.AGE Desc
    """
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: X002_People_age_master")
    if funcconf.l_mess_project:
        i = funcsys.tablerowcount(so_curs, sr_file)
        funcsms.send_telegram("", "administrator", "<b>" + str(i) + "</b> Age groups")

    """ ****************************************************************************
    END OF SCRIPT
    *****************************************************************************"""
    print("END OF SCRIPT")
    funcfile.writelog("END OF SCRIPT")

    # CLOSE THE WORKING DATABASE
    so_conn.close()

    # CLOSE THE LOG
    funcfile.writelog("--------------------------------------")
    funcfile.writelog("COMPLETED: C003_PEOPLE_LIST_MASTERFILE")

    return


if __name__ == '__main__':
    try:
        people_list_masterfile()
    except Exception as e:
        funcsys.ErrMessage(e, funcconf.l_mess_project, "C003_people_list_masterfile", "C003_people_list_masterfile")
