"""
Test history functions
Created on: 26 Jan 2020
Author: Albert J van Rensburg (NWU:21162395)
"""

# IMPORT SYSTEM FUNCTIONS
import csv

# IMPORT OWN MODULES
from _my_modules import funcfile
from _my_modules import funcsys

# VARIABLES
l_debug: bool = False

# INDEX
"""
get_previous_finding
set_previous_finding
get_officer
get_supervisor
get_test_flag
"""


def get_previous_finding(o_cursor, s_path, s_source, s_key, s_format="ITTTT"):
    """
    Function to fetch previous findings
    :param o_cursor: Database cursor
    :param s_path: Source file path
    :param s_source: Source file name
    :param s_key: Source file key
    :param s_format: Source file format
    :return: Number of records (findings)
    """

    if l_debug:
        print("Import previously reported findings...")

    # DECLARE / BUILD VARIABLES
    s_key = s_key.lower()
    s_formatt: str = "(PROCESS TEXT, "
    for i in range(5):
        if l_debug:
            print(s_format[i:i + 1])
        if s_format[i:i + 1] == "I":
            s_formatt += "FIELD" + str(i + 1) + " INT, "
        elif s_format[i:i + 1] == "R":
            s_formatt += "FIELD" + str(i + 1) + " REAL, "
        else:
            s_formatt += "FIELD" + str(i + 1) + " TEXT, "
    s_formatt += "DATE_REPORTED TEXT, DATE_RETEST TEXT, REMARK TEXT)"
    if l_debug:
        print(s_formatt)

    sr_file = "Z001aa_getprev"
    o_cursor.execute("DROP TABLE IF EXISTS  " + sr_file)
    o_cursor.execute("CREATE TABLE " + sr_file + s_formatt)
    co = open(s_path + s_source, "r")
    co_reader = csv.reader(co)
    # READ THE TEXT DATA INTO THE TABLE
    for row in co_reader:
        # Populate the column variables
        if row[0] == "PROCESS":
            continue
        elif row[0] != s_key:
            continue
        else:
            s_cols = "INSERT INTO " + sr_file + " VALUES('" \
                     + row[0] + "','" \
                     + row[1] + "','" \
                     + row[2] + "','" \
                     + row[3] + "','" \
                     + row[4] + "','" \
                     + row[5] + "','" \
                     + row[6] + "','" \
                     + row[7] + "','" \
                     + row[8] + "')"
        if l_debug:
            print(s_cols)
        o_cursor.execute(s_cols)
    # ClOSE THE SOURCE FILE
    co.close()
    funcfile.writelog("%t IMPORT TABLE: " + s_path + s_source + "(" + sr_file + ")")

    return funcsys.tablerowcount(o_cursor, sr_file)


def set_previous_finding(o_cursor):
    """
    Function order and set last finding
    :param o_cursor: Database cursor
    :return: Number of records
    """

    # SET PREVIOUS FINDINGS
    sr_file = "Z001ab_setprev"
    o_cursor.execute("DROP TABLE IF EXISTS " + sr_file)
    if l_debug:
        print("Obtain the latest previous finding...")
    s_sql = "Create Table " + sr_file + " As" + """
    Select
        GET.PROCESS,
        GET.FIELD1,
        GET.FIELD2,
        GET.FIELD3,
        GET.FIELD4,
        GET.FIELD5,
        Max(GET.DATE_REPORTED) As DATE_REPORTED,
        GET.DATE_RETEST,
        GET.REMARK
    From
        Z001aa_getprev GET
    Group By
        GET.FIELD1,
        GET.FIELD2,
        GET.FIELD3,
        GET.FIELD4,
        GET.FIELD5
    ;"""
    o_cursor.execute(s_sql)
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    return funcsys.tablerowcount(o_cursor, sr_file)


def get_officer(o_cursor, s_source="HR", s_key=""):
    """
    Function order and set last finding
    :param o_cursor: Database cursor
    :param s_source: Table to read lookups
    :param s_key: Officer search key
    :return: Number of records
    """

    # DECLARE / BUILD VARIABLES

    # SET PREVIOUS FINDINGS
    sr_file = "Z001af_officer"
    o_cursor.execute("DROP TABLE IF EXISTS " + sr_file)
    if l_debug:
        print("Import reporting officers for mail purposes...")
    s_sql = "Create Table " + sr_file + " As" + """
        Select
            LOOKUP.LOOKUP,
            Upper(LOOKUP.LOOKUP_CODE) AS CAMPUS,
            LOOKUP.LOOKUP_DESCRIPTION AS EMPLOYEE_NUMBER,
            PEOPLE.name_address AS NAME_ADDR,
            PEOPLE.EMAIL_ADDRESS
        FROM
            %TABLE% LOOKUP Left Join
            PEOPLE.X000_PEOPLE PEOPLE ON PEOPLE.employee_number = LOOKUP.LOOKUP_DESCRIPTION
        WHERE
            LOOKUP.LOOKUP = '%KEY%'
        ;"""
    s_sql = s_sql.replace("%KEY%", s_key)
    if s_source == "VSS":
        s_sql = s_sql.replace("%TABLE%", "VSS.X000_OWN_LOOKUPS")
    elif s_source == "IA":
        s_sql = s_sql.replace("%TABLE%", "X000_OWN_LOOKUPS")
    elif s_source == "KFS":
        s_sql = s_sql.replace("%TABLE%", "KFS.X000_OWN_KFS_LOOKUPS")
    else:
        s_sql = s_sql.replace("%TABLE%", "PEOPLE.X000_OWN_HR_LOOKUPS")
    o_cursor.execute(s_sql)
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    return funcsys.tablerowcount(o_cursor, sr_file)


def get_supervisor(o_cursor, s_source="HR", s_key=""):
    """
    Function order and set last finding
    :param o_cursor: Database cursor
    :param s_source: Table to read lookups
    :param s_key: Officer search key
    :return: Number of records
    """

    # DECLARE / BUILD VARIABLES

    # SET PREVIOUS FINDINGS
    sr_file = "Z001ag_supervisor"
    o_cursor.execute("DROP TABLE IF EXISTS " + sr_file)
    if l_debug:
        print("Import reporting supervisors for mail purposes...")
    s_sql = "Create Table " + sr_file + " As" + """
        Select
            LOOKUP.LOOKUP,
            Upper(LOOKUP.LOOKUP_CODE) AS CAMPUS,
            LOOKUP.LOOKUP_DESCRIPTION AS EMPLOYEE_NUMBER,
            PEOPLE.name_address AS NAME_ADDR,
            PEOPLE.EMAIL_ADDRESS
        FROM
            %TABLE% LOOKUP Left Join
            PEOPLE.X000_PEOPLE PEOPLE ON PEOPLE.employee_number = LOOKUP.LOOKUP_DESCRIPTION
        WHERE
            LOOKUP.LOOKUP = '%KEY%'
        ;"""
    s_sql = s_sql.replace("%KEY%", s_key)
    if s_source == "VSS":
        s_sql = s_sql.replace("%TABLE%", "VSS.X000_OWN_LOOKUPS")
    elif s_source == "IA":
        s_sql = s_sql.replace("%TABLE%", "X000_OWN_LOOKUPS")
    elif s_source == "KFS":
        s_sql = s_sql.replace("%TABLE%", "KFS.X000_OWN_KFS_LOOKUPS")
    else:
        s_sql = s_sql.replace("%TABLE%", "PEOPLE.X000_OWN_HR_LOOKUPS")
    o_cursor.execute(s_sql)
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    return funcsys.tablerowcount(o_cursor, sr_file)


def get_test_flag(o_cursor, s_source = "HR", s_key = "", s_code = ""):
    """
    Function to obtain a test flag.

    :param o_cursor: object: Database cursor
    :param s_source: str: Table to read lookups
    :param s_key: str: Test search key
    :param s_code: str: Test lookup code
    :return: str: Value of lookup description
    """

    # SET PREVIOUS FINDINGS
    if l_debug:
        print("Obtain a test flag flag...")
    s_sql = """
        Select
            l.LOOKUP_DESCRIPTION
        FROM
            %TABLE% l
        WHERE
            l.LOOKUP = '%KEY%' And
            l.LOOKUP_CODE = '%CODE%'
        ;"""
    s_sql = s_sql.replace("%KEY%", s_key)
    s_sql = s_sql.replace("%CODE%", s_code)
    if s_source == "VSS":
        s_sql = s_sql.replace("%TABLE%", "VSS.X000_OWN_LOOKUPS")
    elif s_source == "IA":
        s_sql = s_sql.replace("%TABLE%", "X000_OWN_LOOKUPS")
    elif s_source == "KFS":
        s_sql = s_sql.replace("%TABLE%", "KFS.X000_OWN_KFS_LOOKUPS")
    else:
        s_sql = s_sql.replace("%TABLE%", "PEOPLE.X000_OWN_HR_LOOKUPS")
    t_return = o_cursor.execute(s_sql).fetchone()
    if t_return:
        s_return = str(t_return[0])
    else:
        s_return = ""
    funcfile.writelog("%t OBTAIN TEST FLAG: " + s_key + " " + s_code + " " + s_return)

    return s_return
