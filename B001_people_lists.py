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
from _my_modules import funcpeople
from _my_modules import funcsms
from _my_modules import funcsys

""" INDEX
ENVIRONMENT
OPEN THE DATABASES
BEGIN OF SCRIPT
BUILD GRADES
BUILD POSITIONS
BUILD JOBS
BUILD ORGANIZATION
BUILD BANK ACCOUNTS
BUILD PERIODS OF SERVICE
BUILD ASSIGNMENTS
BUILD CONTRACTS
BUILD SECONDARY ASSIGNMENTS
BUILD CURRENT YEAR SECONDARY ASSIGNMENTS
BUILD ALL PEOPLE
BUILD PERSON TYPES
COUNT RECORDS
BUILD ADDRESSES AND PHONES
BUILD ASSIGNMENTS AND PEOPLE
BUILD LIST OF CURRENT PEOPLE
PEOPLE STRUCTURE LIST
PEOPLE ORGANIZATION STRUCTURE REF (Employee numbers of structure)
BUILD CURRENT SYSTEM USERS (X000_USER_CURR)
BUILD SPOUSES (X000_SPOUSE)
BUILD PEOPLE LEAVE
BUILD LEAVE REASONS
BUILD LEAVE TYPES
BUILD LEAVE LOG
"""


def people_lists():
    """
    Function to build standard PEOPLE tables from various people tables in oracle
    :return: Nothing
    """

    """*****************************************************************************
    ENVIRONMENT
    *****************************************************************************"""

    # DECLARE VARIABLES
    # l_mail: bool = False
    # l_mail: bool = funcconf.l_mail_project
    # l_mess: bool = False
    l_mess: bool = funcconf.l_mess_project
    so_path = f"{funcconf.drive_data_raw}People/"  # Source database path
    so_file = "People.sqlite"  # Source database
    # sr_file: str = ""  # Current sqlite table
    re_path = f"{funcconf.drive_data_results}People/"  # Results path
    l_debug: bool = False
    l_export: bool = True

    # SCRIPT LOG
    funcfile.writelog("Now")
    funcfile.writelog("SCRIPT: B001_PEOPLE_LISTS")
    funcfile.writelog("-------------------------")
    print("-----------------")
    print("B001_PEOPLE_LISTS")
    print("-----------------")

    # MESSAGE
    if l_mess:
        funcsms.send_telegram("", "administrator", "<b>B001 People master files</b>")

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

    """ ****************************************************************************
    BEGIN OF SCRIPT
    *****************************************************************************"""
    print("BEGIN OF SCRIPT")
    funcfile.writelog("BEGIN OF SCRIPT")

    """ ****************************************************************************
    BUILD GRADES
    *****************************************************************************"""
    print("BUILD GRADES")
    funcfile.writelog("BUILD GRADES")

    # BUILD GRADES
    print("Build grades...")
    s_sql = "CREATE TABLE X000_GRADES AS " + """
    SELECT
      GRADES.GRADE_ID,
      Substr(NAME,INSTR(NAME,'~')+1,60) As GRADE_NAME,
      GRADES.DATE_FROM,
      GRADES.DATE_TO,
      GRADES.GRADE_DEFINITION_ID,
      DEF.SEGMENT1 AS GRADE,
      GRADES.NAME AS GRADE_COMB,
      GRADES.CREATED_BY,
      GRADES.CREATION_DATE,
      GRADES.LAST_UPDATED_BY,
      GRADES.LAST_UPDATE_LOGIN,
      GRADES.SEQUENCE,
      DEF.SEGMENT2 AS GRADE_SEGMENT2,
      DEF.ID_FLEX_NUM,
      GRADES.BUSINESS_GROUP_ID,
      LOOKUP.LOOKUP_DESCRIPTION AS GRADE_CALC
    FROM
      PER_GRADES GRADES
      LEFT JOIN PER_GRADE_DEFINITIONS DEF ON DEF.GRADE_DEFINITION_ID = GRADES.GRADE_DEFINITION_ID
      LEFT JOIN X000_OWN_HR_LOOKUPS LOOKUP ON LOOKUP.LOOKUP_CODE = DEF.SEGMENT1 AND
        LOOKUP.LOOKUP = 'GRADE'  
    ORDER BY
      GRADES.GRADE_ID
    """
    so_curs.execute("DROP TABLE IF EXISTS X000_GRADES")
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: X000_GRADES")

    """ ****************************************************************************
    BUILD POSITIONS
    *****************************************************************************"""
    print("BUILD POSITIONS")
    funcfile.writelog("BUILD POSITIONS")

    print("Build positions...")
    sr_file = "X000_POSITIONS"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    SELECT
        POS.POSITION_ID,
        POS.EFFECTIVE_START_DATE,
        POS.EFFECTIVE_END_DATE,
        POS.DATE_EFFECTIVE,
        POS.DATE_END,
        POS.POSITION_DEFINITION_ID,
        PPD.SEGMENT1 AS POSITION,
        PPD.SEGMENT2 AS POSITION_NAME,
        POS.BUSINESS_GROUP_ID,
        POS.JOB_ID,
        POS.ORGANIZATION_ID,
        POS.LOCATION_ID,
        POS.PROBATION_PERIOD,
        POS.WORKING_HOURS,
        POS.STATUS,
        POS.ATTRIBUTE1,
        POS.ATTRIBUTE2,
        POS.ATTRIBUTE3,
        POS.ATTRIBUTE4,
        POS.ATTRIBUTE5,
        POS.ATTRIBUTE6,
        POS.ATTRIBUTE7,
        POS.ATTRIBUTE8,
        POS.ATTRIBUTE9,
        POS.ATTRIBUTE10,
        POS.ATTRIBUTE11,
        POS.ATTRIBUTE12,
        POS.ATTRIBUTE13,
        POS.ATTRIBUTE14,
        POS.ATTRIBUTE15,
        POS.ATTRIBUTE16,
        POS.ATTRIBUTE17,
        POS.ATTRIBUTE18,
        POS.ATTRIBUTE19,
        POS.ATTRIBUTE20,
        POS.ATTRIBUTE21,
        POS.ATTRIBUTE22,
        POS.ATTRIBUTE23,
        POS.ATTRIBUTE24,
        POS.ATTRIBUTE25,
        POS.ATTRIBUTE26,
        POS.ATTRIBUTE27,
        POS.ATTRIBUTE28,
        POS.ATTRIBUTE29,
        POS.ATTRIBUTE30,
        POS.CREATION_DATE,
        POS.CREATED_BY,
        POS.LAST_UPDATE_DATE,
        POS.LAST_UPDATED_BY,
        POS.LAST_UPDATE_LOGIN,
        PPD.ID_FLEX_NUM,
        PPD.SUMMARY_FLAG,
        PPD.ENABLED_FLAG,
        PPD.SEGMENT3 AS POS_DEF_SEGMENT3,
        PPD.SEGMENT4 AS POS_DEF_SEGMENT4,
        CASE
           WHEN PPD.SEGMENT4 = '1' THEN 'ACADEMIC'
           ELSE "SUPPORT"
        END AS ACAD_SUPP,
        PPS.PARENT_POSITION_ID,
        POS.MAX_PERSONS
    FROM
        HR_ALL_POSITIONS_F POS Left Join
        PER_POSITION_DEFINITIONS PPD On PPD.POSITION_DEFINITION_ID = POS.POSITION_DEFINITION_ID Left Join
        PER_POS_STRUCTURE_ELEMENTS_NWU PPS On PPS.SUBORDINATE_POSITION_ID = POS.POSITION_ID
    """
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD POSITION STRUCTURE STEP 1
    print("Build position structure step 1...")
    sr_file = "X000_POS_STRUCT_01"
    so_curs.execute("DROP VIEW IF EXISTS " + sr_file)
    s_sql = "CREATE VIEW " + sr_file + " AS " + """
    Select
        ppsen.POS_STRUCTURE_ELEMENT_ID,
        ppsen.SUBORDINATE_POSITION_ID As POS01,
        ppsen.PARENT_POSITION_ID As POS02
    From
        PER_POS_STRUCTURE_ELEMENTS_NWU ppsen
    ;"""
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD VIEW: " + sr_file)

    # BUILD POSITION STRUCTURE STEP 2
    print("Build position structure step 2...")
    sr_file = "X000_POS_STRUCT_02"
    so_curs.execute("DROP VIEW IF EXISTS " + sr_file)
    s_sql = "CREATE VIEW " + sr_file + " AS " + """
    Select
        x000p.*,
        ppsen.PARENT_POSITION_ID As POS03
    From
        X000_POS_STRUCT_01 x000p Left Join
        PER_POS_STRUCTURE_ELEMENTS_NWU ppsen On ppsen.SUBORDINATE_POSITION_ID = x000p.POS02
    ;"""
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD VIEW: " + sr_file)

    # BUILD POSITION STRUCTURE STEP 3
    print("Build position structure step 3...")
    sr_file = "X000_POS_STRUCT_03"
    so_curs.execute("DROP VIEW IF EXISTS " + sr_file)
    s_sql = "CREATE VIEW " + sr_file + " AS " + """
    Select
        x000p.*,
        ppsen.PARENT_POSITION_ID As POS04
    From
        X000_POS_STRUCT_02 x000p Left Join
        PER_POS_STRUCTURE_ELEMENTS_NWU ppsen On ppsen.SUBORDINATE_POSITION_ID = x000p.POS03
    ;"""
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD VIEW: " + sr_file)

    # BUILD POSITION STRUCTURE STEP 4
    print("Build position structure step 4...")
    sr_file = "X000_POS_STRUCT_04"
    so_curs.execute("DROP VIEW IF EXISTS " + sr_file)
    s_sql = "CREATE VIEW " + sr_file + " AS " + """
    Select
        x000p.*,
        ppsen.PARENT_POSITION_ID As POS05
    From
        X000_POS_STRUCT_03 x000p Left Join
        PER_POS_STRUCTURE_ELEMENTS_NWU ppsen On ppsen.SUBORDINATE_POSITION_ID = x000p.POS04
    ;"""
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD VIEW: " + sr_file)

    # BUILD POSITION STRUCTURE STEP 5
    print("Build position structure step 5...")
    sr_file = "X000_POS_STRUCT_05"
    so_curs.execute("DROP VIEW IF EXISTS " + sr_file)
    s_sql = "CREATE VIEW " + sr_file + " AS " + """
    Select
        x000p.*,
        ppsen.PARENT_POSITION_ID As POS06
    From
        X000_POS_STRUCT_04 x000p Left Join
        PER_POS_STRUCTURE_ELEMENTS_NWU ppsen On ppsen.SUBORDINATE_POSITION_ID = x000p.POS05
    ;"""
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD VIEW: " + sr_file)

    # BUILD POSITION STRUCTURE STEP 6
    print("Build position structure step 6...")
    sr_file = "X000_POS_STRUCT_06"
    so_curs.execute("DROP VIEW IF EXISTS " + sr_file)
    s_sql = "CREATE VIEW " + sr_file + " AS " + """
    Select
        x000p.*,
        ppsen.PARENT_POSITION_ID As POS07
    From
        X000_POS_STRUCT_05 x000p Left Join
        PER_POS_STRUCTURE_ELEMENTS_NWU ppsen On ppsen.SUBORDINATE_POSITION_ID = x000p.POS06
    ;"""
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD VIEW: " + sr_file)

    # BUILD POSITION STRUCTURE STEP 7
    print("Build position structure step 7...")
    sr_file = "X000_POS_STRUCT_07"
    so_curs.execute("DROP VIEW IF EXISTS " + sr_file)
    s_sql = "CREATE VIEW " + sr_file + " AS " + """
    Select
        x000p.*,
        ppsen.PARENT_POSITION_ID As POS08
    From
        X000_POS_STRUCT_06 x000p Left Join
        PER_POS_STRUCTURE_ELEMENTS_NWU ppsen On ppsen.SUBORDINATE_POSITION_ID = x000p.POS07
    ;"""
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD VIEW: " + sr_file)

    # BUILD POSITION STRUCTURE STEP 8
    print("Build position structure step 8...")
    sr_file = "X000_POS_STRUCT_08"
    so_curs.execute("DROP VIEW IF EXISTS " + sr_file)
    s_sql = "CREATE VIEW " + sr_file + " AS " + """
    Select
        x000p.*,
        ppsen.PARENT_POSITION_ID As POS09
    From
        X000_POS_STRUCT_07 x000p Left Join
        PER_POS_STRUCTURE_ELEMENTS_NWU ppsen On ppsen.SUBORDINATE_POSITION_ID = x000p.POS08
    ;"""
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD VIEW: " + sr_file)

    # BUILD POSITION STRUCTURE STEP 9
    print("Build position structure step 9...")
    sr_file = "X000_POS_STRUCT_09"
    so_curs.execute("DROP VIEW IF EXISTS " + sr_file)
    s_sql = "CREATE VIEW " + sr_file + " AS " + """
    Select
        x000p.*,
        ppsen.PARENT_POSITION_ID As POS10
    From
        X000_POS_STRUCT_08 x000p Left Join
        PER_POS_STRUCTURE_ELEMENTS_NWU ppsen On ppsen.SUBORDINATE_POSITION_ID = x000p.POS09
    ;"""
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD VIEW: " + sr_file)

    # BUILD POSITION STRUCTURE STEP 10
    print("Build position structure step 10...")
    sr_file = "X000_POS_STRUCT_10"
    so_curs.execute("DROP VIEW IF EXISTS " + sr_file)
    s_sql = "CREATE VIEW " + sr_file + " AS " + """
    Select
        x000p.*,
        ppsen.PARENT_POSITION_ID As POS11
    From
        X000_POS_STRUCT_09 x000p Left Join
        PER_POS_STRUCTURE_ELEMENTS_NWU ppsen On ppsen.SUBORDINATE_POSITION_ID = x000p.POS10
    ;"""
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD VIEW: " + sr_file)

    """ ****************************************************************************
    BUILD JOBS
    *****************************************************************************"""
    print("BUILD JOBS")
    funcfile.writelog("BUILD JOBS")

    print("Build jobs...")
    s_sql = "CREATE TABLE X000_JOBS AS " + """
    SELECT
      PER_JOBS.JOB_ID,
      PER_JOBS.DATE_FROM,
      PER_JOBS.DATE_TO,
      PER_JOBS.JOB_DEFINITION_ID,
      PER_JOB_DEFINITIONS.SEGMENT1 AS JOB_NAME,
      PER_JOBS.NAME AS JOB_COMB,
      PER_JOB_DEFINITIONS.SEGMENT2 AS JOB_SEGMENT,
      PER_JOBS.ATTRIBUTE1,
      PER_JOBS.CREATION_DATE,
      PER_JOBS.CREATED_BY,
      PER_JOBS.LAST_UPDATE_DATE,
      PER_JOBS.LAST_UPDATED_BY,
      PER_JOBS.LAST_UPDATE_LOGIN,
      PER_JOBS.JOB_GROUP_ID,
      PER_JOB_DEFINITIONS.ID_FLEX_NUM
    FROM
      PER_JOBS
      LEFT JOIN PER_JOB_DEFINITIONS ON PER_JOB_DEFINITIONS.JOB_DEFINITION_ID = PER_JOBS.JOB_DEFINITION_ID
    ORDER BY
      PER_JOBS.JOB_ID  
    """
    so_curs.execute("DROP TABLE IF EXISTS X000_JOBS")
    so_curs.execute(s_sql)
    so_conn.commit()

    funcfile.writelog("%t BUILD TABLE: X000_JOBS")

    # Calc grade name field
    if "JOB_SEGMENT_NAME" not in funccsv.get_colnames_sqlite(so_curs, "X000_JOBS"):
        so_curs.execute("ALTER TABLE X000_JOBS ADD JOB_SEGMENT_NAME TEXT;")
        so_curs.execute("UPDATE X000_JOBS SET JOB_SEGMENT_NAME = SUBSTR(JOB_COMB,INSTR(JOB_COMB,'~')+1,60);")
        so_conn.commit()
        funcfile.writelog("%t ADD COLUMN: JOB_SEGMENT_NAME")

    """ ****************************************************************************
    BUILD ORGANIZATION
    *****************************************************************************"""
    if l_debug:
        print("BUILD ORGANIZATION")
    funcfile.writelog("BUILD ORGANIZATION")

    if l_debug:
        print("Build organization...")
    sr_file = "X000_ORGANIZATION"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    SELECT
        ORG.ORGANIZATION_ID,
        ORG.LOCATION_ID,
        ORG.DATE_FROM,
        ORG.DATE_TO,
        ORG.TYPE AS ORG_TYPE,
        ORG.ATTRIBUTE_CATEGORY AS ORG_TYPE_DESC,
        ORG.ATTRIBUTE1,
        ORG.ATTRIBUTE2 AS ORG_NAAM,
        ORG.NAME AS OE_CODE,
        ORI.ORG_INFORMATION1 AS ORG_NAME,
        ORI.ORG_INFORMATION5 AS ORG_HEAD_PERSON_ID,
        ORG.CREATION_DATE,
        ORG.CREATED_BY,
        ORG.LAST_UPDATE_DATE,
        ORG.LAST_UPDATED_BY,
        ORG.LAST_UPDATE_LOGIN,
        ORG.BUSINESS_GROUP_ID,
        ORG.COST_ALLOCATION_KEYFLEX_ID,
        ORG.SOFT_CODING_KEYFLEX_ID,
        '' AS MAILTO
    FROM
        HR_ALL_ORGANIZATION_UNITS ORG LEFT JOIN
        HR_ORGANIZATION_INFORMATION ORI ON ORI.ORGANIZATION_ID = ORG.ORGANIZATION_ID AND
            ORI.ORG_INFORMATION_CONTEXT = 'NWU_ORG_INFO'
    """
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # Build organization structure step 1*******************************************
    print("Build organization structure step 1...")
    sr_file = "X000_ORG_STRUCT_1"
    s_sql = "CREATE VIEW " + sr_file + " AS " + """
    SELECT
      PER_ORG_STRUCTURE_ELEMENTS.ORG_STRUCTURE_ELEMENT_ID,
      PER_ORG_STRUCTURE_ELEMENTS.BUSINESS_GROUP_ID,
      PER_ORG_STRUCTURE_ELEMENTS.ORG_STRUCTURE_VERSION_ID,
      PER_ORG_STRUCTURE_ELEMENTS.ORGANIZATION_ID_CHILD AS ORG1,
      PER_ORG_STRUCTURE_ELEMENTS.ORGANIZATION_ID_PARENT AS ORG2
    FROM
      PER_ORG_STRUCTURE_ELEMENTS
    WHERE
      PER_ORG_STRUCTURE_ELEMENTS.ORG_STRUCTURE_VERSION_ID = 61
    ;"""
    so_curs.execute("DROP VIEW IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD VIEW: " + sr_file)

    # Build organization structure step 2*******************************************
    print("Build organization structure step 2...")
    sr_file = "X000_ORG_STRUCT_2"
    s_sql = "CREATE VIEW " + sr_file + " AS " + """
    SELECT
      X000_ORG_STRUCT_1.ORG_STRUCTURE_ELEMENT_ID,
      X000_ORG_STRUCT_1.BUSINESS_GROUP_ID,
      X000_ORG_STRUCT_1.ORG_STRUCTURE_VERSION_ID,
      X000_ORG_STRUCT_1.ORG1,
      X000_ORG_STRUCT_1.ORG2,
      PER_ORG_STRUCTURE_ELEMENTS.ORGANIZATION_ID_PARENT AS ORG3
    FROM
      X000_ORG_STRUCT_1
      LEFT JOIN PER_ORG_STRUCTURE_ELEMENTS ON PER_ORG_STRUCTURE_ELEMENTS.ORGANIZATION_ID_CHILD = X000_ORG_STRUCT_1.ORG2
    GROUP BY
      X000_ORG_STRUCT_1.ORG1
    ;"""
    so_curs.execute("DROP VIEW IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD VIEW: " + sr_file)

    # Build organization structure step 3*******************************************
    print("Build organization structure step 3...")
    sr_file = "X000_ORG_STRUCT_3"
    s_sql = "CREATE VIEW " + sr_file + " AS " + """
    SELECT
      X000_ORG_STRUCT_2.ORG_STRUCTURE_ELEMENT_ID,
      X000_ORG_STRUCT_2.BUSINESS_GROUP_ID,
      X000_ORG_STRUCT_2.ORG_STRUCTURE_VERSION_ID,
      X000_ORG_STRUCT_2.ORG1,
      X000_ORG_STRUCT_2.ORG2,
      X000_ORG_STRUCT_2.ORG3,
      PER_ORG_STRUCTURE_ELEMENTS.ORGANIZATION_ID_PARENT AS ORG4
    FROM
      X000_ORG_STRUCT_2
      LEFT JOIN PER_ORG_STRUCTURE_ELEMENTS ON PER_ORG_STRUCTURE_ELEMENTS.ORGANIZATION_ID_CHILD = X000_ORG_STRUCT_2.ORG3
    GROUP BY
      X000_ORG_STRUCT_2.ORG1  
    ;"""
    so_curs.execute("DROP VIEW IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD VIEW: " + sr_file)

    # Build organization structure step 4*******************************************
    print("Build organization structure step 4...")
    sr_file = "X000_ORG_STRUCT_4"
    s_sql = "CREATE VIEW " + sr_file + " AS " + """
    SELECT
      X000_ORG_STRUCT_3.ORG_STRUCTURE_ELEMENT_ID,
      X000_ORG_STRUCT_3.BUSINESS_GROUP_ID,
      X000_ORG_STRUCT_3.ORG_STRUCTURE_VERSION_ID,
      X000_ORG_STRUCT_3.ORG1,
      X000_ORG_STRUCT_3.ORG2,
      X000_ORG_STRUCT_3.ORG3,
      X000_ORG_STRUCT_3.ORG4,
      PER_ORG_STRUCTURE_ELEMENTS.ORGANIZATION_ID_PARENT AS ORG5
    FROM
      X000_ORG_STRUCT_3
      LEFT JOIN PER_ORG_STRUCTURE_ELEMENTS ON PER_ORG_STRUCTURE_ELEMENTS.ORGANIZATION_ID_CHILD = X000_ORG_STRUCT_3.ORG4
    GROUP BY
      X000_ORG_STRUCT_3.ORG1  
    ;"""
    so_curs.execute("DROP VIEW IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD VIEW: " + sr_file)

    # Build organization structure step 5*******************************************
    print("Build organization structure step 5...")
    sr_file = "X000_ORG_STRUCT_5"
    s_sql = "CREATE VIEW " + sr_file + " AS " + """
    SELECT
      X000_ORG_STRUCT_4.ORG_STRUCTURE_ELEMENT_ID,
      X000_ORG_STRUCT_4.BUSINESS_GROUP_ID,
      X000_ORG_STRUCT_4.ORG_STRUCTURE_VERSION_ID,
      X000_ORG_STRUCT_4.ORG1,
      X000_ORG_STRUCT_4.ORG2,
      X000_ORG_STRUCT_4.ORG3,
      X000_ORG_STRUCT_4.ORG4,
      X000_ORG_STRUCT_4.ORG5,  
      PER_ORG_STRUCTURE_ELEMENTS.ORGANIZATION_ID_PARENT AS ORG6
    FROM
      X000_ORG_STRUCT_4
      LEFT JOIN PER_ORG_STRUCTURE_ELEMENTS ON PER_ORG_STRUCTURE_ELEMENTS.ORGANIZATION_ID_CHILD = X000_ORG_STRUCT_4.ORG5
    GROUP BY
      X000_ORG_STRUCT_4.ORG1  
    ;"""
    so_curs.execute("DROP VIEW IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD VIEW: " + sr_file)

    # Build organization structure step 6*******************************************
    print("Build organization structure step 6...")
    sr_file = "X000_ORG_STRUCT_6"
    s_sql = "CREATE VIEW " + sr_file + " AS " + """
    SELECT
      X000_ORG_STRUCT_5.ORG_STRUCTURE_ELEMENT_ID,
      X000_ORG_STRUCT_5.BUSINESS_GROUP_ID,
      X000_ORG_STRUCT_5.ORG_STRUCTURE_VERSION_ID,
      X000_ORG_STRUCT_5.ORG1,
      X000_ORG_STRUCT_5.ORG2,
      X000_ORG_STRUCT_5.ORG3,
      X000_ORG_STRUCT_5.ORG4,
      X000_ORG_STRUCT_5.ORG5,
      X000_ORG_STRUCT_5.ORG6,    
      PER_ORG_STRUCTURE_ELEMENTS.ORGANIZATION_ID_PARENT AS ORG7
    FROM
      X000_ORG_STRUCT_5
      LEFT JOIN PER_ORG_STRUCTURE_ELEMENTS ON PER_ORG_STRUCTURE_ELEMENTS.ORGANIZATION_ID_CHILD = X000_ORG_STRUCT_5.ORG6
    GROUP BY
      X000_ORG_STRUCT_5.ORG1  
    ;"""
    so_curs.execute("DROP VIEW IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD VIEW: " + sr_file)

    # Build organization structure step 7*******************************************
    print("Build organization structure step 7...")
    sr_file = "X000_ORG_STRUCT_7"
    s_sql = "CREATE VIEW " + sr_file + " AS " + """
    SELECT
      X000_ORG_STRUCT_6.ORG_STRUCTURE_ELEMENT_ID,
      X000_ORG_STRUCT_6.BUSINESS_GROUP_ID,
      X000_ORG_STRUCT_6.ORG_STRUCTURE_VERSION_ID,
      X000_ORG_STRUCT_6.ORG1,
      X000_ORG_STRUCT_6.ORG2,
      X000_ORG_STRUCT_6.ORG3,
      X000_ORG_STRUCT_6.ORG4,
      X000_ORG_STRUCT_6.ORG5,  
      X000_ORG_STRUCT_6.ORG6,
      X000_ORG_STRUCT_6.ORG7,    
      PER_ORG_STRUCTURE_ELEMENTS.ORGANIZATION_ID_PARENT AS ORG8
    FROM
      X000_ORG_STRUCT_6
      LEFT JOIN PER_ORG_STRUCTURE_ELEMENTS ON PER_ORG_STRUCTURE_ELEMENTS.ORGANIZATION_ID_CHILD = X000_ORG_STRUCT_6.ORG7
    GROUP BY
      X000_ORG_STRUCT_6.ORG1  
    ;"""
    so_curs.execute("DROP VIEW IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD VIEW: " + sr_file)

    # Build organization structure *************************************************
    print("Build organization structure...")
    sr_file = "X000_ORGANIZATION_STRUCT"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    SELECT
      X000_ORG_STRUCT_7.ORG_STRUCTURE_ELEMENT_ID,
      X000_ORG_STRUCT_7.BUSINESS_GROUP_ID,
      X000_ORG_STRUCT_7.ORG_STRUCTURE_VERSION_ID,
      X000_ORG_STRUCT_7.ORG1,
      ORG1.ORG_TYPE AS ORG1_TYPE,
      ORG1.ORG_TYPE_DESC AS ORG1_TYPE_DESC,
      ORG1.OE_CODE AS ORG1_OE_CODE,
      ORG1.ORG_NAME AS ORG1_NAME,
      ORG1.ORG_NAAM AS ORG1_NAAM,
      X000_ORG_STRUCT_7.ORG2,
      ORG2.ORG_TYPE AS ORG2_TYPE,
      ORG2.ORG_TYPE_DESC AS ORG2_TYPE_DESC,
      ORG2.OE_CODE AS ORG2_OE_CODE,
      ORG2.ORG_NAME AS ORG2_NAME,
      ORG2.ORG_NAAM AS ORG2_NAAM,
      X000_ORG_STRUCT_7.ORG3,
      ORG3.ORG_TYPE AS ORG3_TYPE,
      ORG3.ORG_TYPE_DESC AS ORG3_TYPE_DESC,
      ORG3.OE_CODE AS ORG3_OE_CODE,
      ORG3.ORG_NAME AS ORG3_NAME,
      ORG3.ORG_NAAM AS ORG3_NAAM,
      X000_ORG_STRUCT_7.ORG4,
      ORG4.ORG_TYPE AS ORG4_TYPE,
      ORG4.ORG_TYPE_DESC AS ORG4_TYPE_DESC,
      ORG4.OE_CODE AS ORG4_OE_CODE,
      ORG4.ORG_NAME AS ORG4_NAME,
      ORG4.ORG_NAAM AS ORG4_NAAM,
      X000_ORG_STRUCT_7.ORG5,
      ORG5.ORG_TYPE AS ORG5_TYPE,
      ORG5.ORG_TYPE_DESC AS ORG5_TYPE_DESC,
      ORG5.OE_CODE AS ORG5_OE_CODE,
      ORG5.ORG_NAME AS ORG5_NAME,
      ORG5.ORG_NAAM AS ORG5_NAAM,
      X000_ORG_STRUCT_7.ORG6,
      ORG6.ORG_TYPE AS ORG6_TYPE,
      ORG6.ORG_TYPE_DESC AS ORG6_TYPE_DESC,
      ORG6.OE_CODE AS ORG6_OE_CODE,
      ORG6.ORG_NAME AS ORG6_NAME,
      ORG6.ORG_NAAM AS ORG6_NAAM,
      X000_ORG_STRUCT_7.ORG7,
      ORG7.ORG_TYPE AS ORG7_TYPE,
      ORG7.ORG_TYPE_DESC AS ORG7_TYPE_DESC,
      ORG7.OE_CODE AS ORG7_OE_CODE,
      ORG7.ORG_NAME AS ORG7_NAME,
      ORG7.ORG_NAAM AS ORG7_NAAM,
      X000_ORG_STRUCT_7.ORG8,
      ORG8.ORG_TYPE AS ORG8_TYPE,
      ORG8.ORG_TYPE_DESC AS ORG8_TYPE_DESC,
      ORG8.OE_CODE AS ORG8_OE_CODE,
      ORG8.ORG_NAME AS ORG8_NAME,
      ORG8.ORG_NAAM AS ORG8_NAAM
    FROM
      X000_ORG_STRUCT_7
      LEFT JOIN X000_ORGANIZATION ORG1 ON ORG1.ORGANIZATION_ID = X000_ORG_STRUCT_7.ORG1
      LEFT JOIN X000_ORGANIZATION ORG2 ON ORG2.ORGANIZATION_ID = X000_ORG_STRUCT_7.ORG2
      LEFT JOIN X000_ORGANIZATION ORG3 ON ORG3.ORGANIZATION_ID = X000_ORG_STRUCT_7.ORG3
      LEFT JOIN X000_ORGANIZATION ORG4 ON ORG4.ORGANIZATION_ID = X000_ORG_STRUCT_7.ORG4
      LEFT JOIN X000_ORGANIZATION ORG5 ON ORG5.ORGANIZATION_ID = X000_ORG_STRUCT_7.ORG5
      LEFT JOIN X000_ORGANIZATION ORG6 ON ORG6.ORGANIZATION_ID = X000_ORG_STRUCT_7.ORG6
      LEFT JOIN X000_ORGANIZATION ORG7 ON ORG7.ORGANIZATION_ID = X000_ORG_STRUCT_7.ORG7
      LEFT JOIN X000_ORGANIZATION ORG8 ON ORG8.ORGANIZATION_ID = X000_ORG_STRUCT_7.ORG8
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    if "FACULTY" not in funccsv.get_colnames_sqlite(so_curs, sr_file):
        so_curs.execute("ALTER TABLE " + sr_file + " ADD COLUMN FACULTY TEXT;")
        so_curs.execute("UPDATE " + sr_file + """
                        SET FACULTY = 
                        CASE
                           WHEN ORG1_TYPE = "FAC" THEN ORG1_NAME
                           WHEN ORG2_TYPE = "FAC" THEN ORG2_NAME
                           WHEN ORG3_TYPE = "FAC" THEN ORG3_NAME
                           WHEN ORG4_TYPE = "FAC" THEN ORG4_NAME
                           WHEN ORG5_TYPE = "FAC" THEN ORG5_NAME
                           WHEN ORG6_TYPE = "FAC" THEN ORG6_NAME
                           WHEN ORG7_TYPE = "FAC" THEN ORG7_NAME
                           WHEN ORG8_TYPE = "FAC" THEN ORG8_NAME
                           ELSE ""
                        END
                        ;""")
        so_conn.commit()
        funcfile.writelog("%t ADD COLUMN: FACULTY")

    if "DIVISION" not in funccsv.get_colnames_sqlite(so_curs, sr_file):
        so_curs.execute("ALTER TABLE " + sr_file + " ADD COLUMN DIVISION TEXT;")
        so_curs.execute("UPDATE " + sr_file + """
                        SET DIVISION = 
                        CASE
                           WHEN ORG1_TYPE = "DIV" THEN ORG1_NAME
                           WHEN ORG2_TYPE = "DIV" THEN ORG2_NAME
                           WHEN ORG3_TYPE = "DIV" THEN ORG3_NAME
                           WHEN ORG4_TYPE = "DIV" THEN ORG4_NAME
                           WHEN ORG5_TYPE = "DIV" THEN ORG5_NAME
                           WHEN ORG6_TYPE = "DIV" THEN ORG6_NAME
                           WHEN ORG7_TYPE = "DIV" THEN ORG7_NAME
                           WHEN ORG8_TYPE = "DIV" THEN ORG8_NAME
                           ELSE ""
                        END
                        ;""")
        so_conn.commit()
        funcfile.writelog("%t ADD COLUMN: DIVISION")

    """ ****************************************************************************
    BUILD BANK ACCOUNTS
    *****************************************************************************"""
    print("BUILD BANK ACCOUNTS")
    funcfile.writelog("BUILD BANK ACCOUNTS")

    # BUILD PERSONAL PAY BANK ACCOUNT LIST
    print("Build personal pay bank account number list...")
    sr_file = "X000_PAY_ACCOUNTS"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        PAYM.ASSIGNMENT_ID,
        PAYM.EFFECTIVE_START_DATE,
        PAYM.EFFECTIVE_END_DATE,
        PAYM.PERSONAL_PAYMENT_METHOD_ID,
        PAYM.BUSINESS_GROUP_ID,
        PAYM.ORG_PAYMENT_METHOD_ID,
        upper(PMET.ORG_PAYMENT_METHOD_NAME) As ORG_PAYMENT_METHOD_NAME,
        PAYM.PPM_INFORMATION_CATEGORY,
        PAYM.PPM_INFORMATION1,
        PAYM.CREATED_BY,
        PAYM.CREATION_DATE,
        PAYM.LAST_UPDATE_DATE,
        PAYM.LAST_UPDATED_BY,
        PAYM.EXTERNAL_ACCOUNT_ID,
        EXTA.TERRITORY_CODE,
        EXTA.SEGMENT1 As ACC_BRANCH,
        EXTA.SEGMENT2 As ACC_TYPE_CODE,
        Upper(HRTY.MEANING) As ACC_TYPE,
        EXTA.SEGMENT3 As ACC_NUMBER,
        EXTA.SEGMENT4 As ACC_HOLDER,
        EXTA.SEGMENT5 As ACC_UNKNOWN,
        EXTA.SEGMENT6 As ACC_RELATION_CODE,
        Upper(HRRE.MEANING) As ACC_RELATION
    From
        PAY_PERSONAL_PAYMENT_METHODS_F PAYM Inner Join
        PAY_EXTERNAL_ACCOUNTS EXTA On EXTA.EXTERNAL_ACCOUNT_ID = PAYM.EXTERNAL_ACCOUNT_ID Left Join
        HR_LOOKUPS HRTY On HRTY.LOOKUP_CODE = EXTA.SEGMENT2
                And HRTY.LOOKUP_TYPE = 'ZA_ACCOUNT_TYPE' Left Join
        HR_LOOKUPS HRRE On HRRE.LOOKUP_CODE = EXTA.SEGMENT6
                And HRRE.LOOKUP_TYPE = 'ZA_ACCOUNT_HOLDER_RELATION' Left Join
        PAY_ORG_PAYMENT_METHODS_F PMET On PMET.ORG_PAYMENT_METHOD_ID = PAYM.ORG_PAYMENT_METHOD_ID
                And PAYM.EFFECTIVE_END_DATE Between PMET.EFFECTIVE_START_DATE And PMET.EFFECTIVE_END_DATE
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD PERSONAL PAY BANK ACCOUNT LIST
    print("Build view personal pay bank account number list latest...")
    sr_file = "X000_PAY_ACCOUNTS_LATEST"
    s_sql = "CREATE VIEW " + sr_file + " AS " + """
    Select
        ppm.ASSIGNMENT_ID,
        ppm.PERSONAL_PAYMENT_METHOD_ID,
        ppm.EXTERNAL_ACCOUNT_ID,
        Max(ppm.EFFECTIVE_START_DATE) As EFFECTIVE_START_DATE,
        ppm.EFFECTIVE_END_DATE,
        ppm.ORG_PAYMENT_METHOD_ID,
        opm.ORG_PAYMENT_METHOD_NAME,
        ppm.PRIORITY As PRIORITY,
        ppm.PPM_INFORMATION1,
        ext.TERRITORY_CODE,
        ext.SEGMENT1 As ACC_BRANCH,
        ext.SEGMENT2 As ACC_TYPE_CODE,
        upper(hrty.MEANING) As ACC_TYPE,
        ext.SEGMENT3 As ACC_NUMBER,
        ext.SEGMENT4 As ACC_HOLDER,
        ext.SEGMENT5 As ACC_UNKNOWN,
        ext.SEGMENT6 As ACC_RELATION_CODE,
        upper(hrre.MEANING) As ACC_RELATION
    From
        PAY_PERSONAL_PAYMENT_METHODS_F ppm Inner Join
        PAY_ORG_PAYMENT_METHODS_F opm On opm.ORG_PAYMENT_METHOD_ID = ppm.ORG_PAYMENT_METHOD_ID Left Join
        PAY_EXTERNAL_ACCOUNTS ext On ext.EXTERNAL_ACCOUNT_ID = ppm.EXTERNAL_ACCOUNT_ID Left Join
        HR_LOOKUPS hrty On hrty.LOOKUP_CODE = ext.SEGMENT2 And hrty.LOOKUP_TYPE = 'ZA_ACCOUNT_TYPE' Left Join
        HR_LOOKUPS hrre On hrre.LOOKUP_CODE = ext.SEGMENT6 And hrre.LOOKUP_TYPE = 'ZA_ACCOUNT_HOLDER_RELATION'
    Where
        ppm.EFFECTIVE_START_DATE <= Date() And
        ppm.EFFECTIVE_END_DATE >= Date() And
        opm.EFFECTIVE_START_DATE <= Date() And
        opm.EFFECTIVE_END_DATE >= Date()
    Group By
        ppm.ASSIGNMENT_ID
    ;"""
    so_curs.execute("DROP VIEW IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD VIEW: " + sr_file)

    """ ****************************************************************************
    BUILD PERIODS OF SERVICE
    *****************************************************************************"""
    print("BUILD PERIODS OF SERVICE")
    funcfile.writelog("BUILD PERIODS OF SERVICE")

    # PER PERIODS OF SERVICE
    print("Build per periods of service...")
    s_sql = "CREATE VIEW X000_PER_PERIODS_OF_SERVICE AS " + """
    SELECT
      PER_PERIODS_OF_SERVICE.PERIOD_OF_SERVICE_ID,
      PER_PERIODS_OF_SERVICE.BUSINESS_GROUP_ID,
      PER_PERIODS_OF_SERVICE.PERSON_ID,
      PER_PERIODS_OF_SERVICE.DATE_START,
      PER_PERIODS_OF_SERVICE.ACCEPTED_TERMINATION_DATE,
      PER_PERIODS_OF_SERVICE.ACTUAL_TERMINATION_DATE,
      PER_PERIODS_OF_SERVICE.FINAL_PROCESS_DATE,
      PER_PERIODS_OF_SERVICE.LAST_STANDARD_PROCESS_DATE,
      PER_PERIODS_OF_SERVICE.LEAVING_REASON,
      HR_LOOKUPS.MEANING AS LEAVE_REASON_DESCRIP,
      PER_PERIODS_OF_SERVICE.LAST_UPDATE_DATE,
      PER_PERIODS_OF_SERVICE.LAST_UPDATED_BY,
      PER_PERIODS_OF_SERVICE.LAST_UPDATE_LOGIN,
      PER_PERIODS_OF_SERVICE.CREATED_BY,
      PER_PERIODS_OF_SERVICE.CREATION_DATE,
      PER_PERIODS_OF_SERVICE.COMMENTS
    FROM
      PER_PERIODS_OF_SERVICE
      LEFT JOIN HR_LOOKUPS ON HR_LOOKUPS.LOOKUP_CODE = PER_PERIODS_OF_SERVICE.LEAVING_REASON AND
       HR_LOOKUPS.LOOKUP_TYPE = 'LEAV_REAS'  
    """
    so_curs.execute("DROP VIEW IF EXISTS X000_PER_PERIODS_OF_SERVICE")
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD VIEW: X000_PER_PERIODS_OF_SERVICE")

    """ ****************************************************************************
    BUILD ASSIGNMENTS
    *****************************************************************************"""
    print("BUILD ASSIGNMENTS")
    funcfile.writelog("BUILD ASSIGNMENTS")

    # BUILD ASSIGNMENTS
    print("Build assignments...")
    s_sql = "CREATE VIEW X000_PER_ALL_ASSIGNMENTS AS " + """
    SELECT
      PER_ALL_ASSIGNMENTS_F.ASSIGNMENT_ID AS ASS_ID,
      PER_ALL_ASSIGNMENTS_F.ASSIGNMENT_NUMBER,
      PER_ALL_ASSIGNMENTS_F.PERSON_ID,
      PER_ALL_ASSIGNMENTS_F.SUPERVISOR_ID,
      PER_ALL_ASSIGNMENTS_F.EMPLOYMENT_CATEGORY,
      PER_ALL_ASSIGNMENTS_F.PRIMARY_FLAG,
      PER_ALL_ASSIGNMENTS_F.EFFECTIVE_START_DATE,
      PER_ALL_ASSIGNMENTS_F.EFFECTIVE_END_DATE,
      PER_ALL_ASSIGNMENTS_F.PERIOD_OF_SERVICE_ID,
      X000_PER_PERIODS_OF_SERVICE.DATE_START AS SERVICE_DATE_START,
      X000_PER_PERIODS_OF_SERVICE.ACTUAL_TERMINATION_DATE AS SERVICE_DATE_ACTUAL_TERMINATION,
      X000_PER_PERIODS_OF_SERVICE.LAST_STANDARD_PROCESS_DATE AS SERVICE_DATE_STD_PROCESS,
      X000_PER_PERIODS_OF_SERVICE.FINAL_PROCESS_DATE AS SERVICE_FINAL_PROCESS,
      X000_PER_PERIODS_OF_SERVICE.LEAVING_REASON,
      X000_PER_PERIODS_OF_SERVICE.LEAVE_REASON_DESCRIP,
      PER_ALL_ASSIGNMENTS_F.GRADE_ID,
      X000_GRADES.GRADE,
      X000_GRADES.GRADE_NAME,
      X000_GRADES.GRADE_CALC,  
      PER_ALL_ASSIGNMENTS_F.POSITION_ID,
      X000_POSITIONS.POSITION,
      X000_POSITIONS.POSITION_NAME,
      X000_POSITIONS.ACAD_SUPP,
      PER_ALL_ASSIGNMENTS_F.JOB_ID,
      X000_JOBS.JOB_NAME,
      X000_JOBS.JOB_SEGMENT_NAME,
      PER_ALL_ASSIGNMENTS_F.ASSIGNMENT_STATUS_TYPE_ID,
      PER_ALL_ASSIGNMENTS_F.LOCATION_ID,
      HR_LOCATIONS_ALL.LOCATION_CODE,
      HR_LOCATIONS_ALL.DESCRIPTION AS LOCATION_DESCRIPTION,
      PER_ALL_ASSIGNMENTS_F.ORGANIZATION_ID,
      X000_ORGANIZATION.ORG_TYPE,
      X000_ORGANIZATION.ORG_TYPE_DESC,
      X000_ORGANIZATION.OE_CODE,
      X000_ORGANIZATION.ORG_NAME,
      X000_ORGANIZATION.MAILTO,
      X000_ORGANIZATION_STRUCT.FACULTY,
      X000_ORGANIZATION_STRUCT.DIVISION,      
      PER_ALL_ASSIGNMENTS_F.ASS_ATTRIBUTE1 AS ASS_WEEK_LEN,
      PER_ALL_ASSIGNMENTS_F.ASS_ATTRIBUTE2,
      PER_ALL_ASSIGNMENTS_F.PEOPLE_GROUP_ID,
      PAY_PEOPLE_GROUPS.GROUP_NAME AS "LEAVE_CODE"
    FROM
      PER_ALL_ASSIGNMENTS_F
      LEFT JOIN X000_PER_PERIODS_OF_SERVICE ON X000_PER_PERIODS_OF_SERVICE.PERIOD_OF_SERVICE_ID =
        PER_ALL_ASSIGNMENTS_F.PERIOD_OF_SERVICE_ID
      LEFT JOIN X000_GRADES ON X000_GRADES.GRADE_ID = PER_ALL_ASSIGNMENTS_F.GRADE_ID
      LEFT JOIN X000_POSITIONS ON X000_POSITIONS.POSITION_ID = PER_ALL_ASSIGNMENTS_F.POSITION_ID
      LEFT JOIN X000_JOBS ON X000_JOBS.JOB_ID = PER_ALL_ASSIGNMENTS_F.JOB_ID
      LEFT JOIN HR_LOCATIONS_ALL ON HR_LOCATIONS_ALL.LOCATION_ID = PER_ALL_ASSIGNMENTS_F.LOCATION_ID
      LEFT JOIN X000_ORGANIZATION ON X000_ORGANIZATION.ORGANIZATION_ID = PER_ALL_ASSIGNMENTS_F.ORGANIZATION_ID
      LEFT JOIN X000_ORGANIZATION_STRUCT ON X000_ORGANIZATION_STRUCT.ORG1 = PER_ALL_ASSIGNMENTS_F.ORGANIZATION_ID
      LEFT JOIN PAY_PEOPLE_GROUPS ON PAY_PEOPLE_GROUPS.PEOPLE_GROUP_ID = PER_ALL_ASSIGNMENTS_F.PEOPLE_GROUP_ID
    ORDER BY
      ASSIGNMENT_NUMBER,
      PER_ALL_ASSIGNMENTS_F.EFFECTIVE_START_DATE
    """
    """
    WHERE
      (PER_ALL_ASSIGNMENTS_F.EFFECTIVE_END_DATE >= Date('%CYEARB%') AND
      PER_ALL_ASSIGNMENTS_F.EFFECTIVE_END_DATE <= Date('%CYEARE%')) OR
      (PER_ALL_ASSIGNMENTS_F.EFFECTIVE_START_DATE >= Date('%CYEARB%') AND
      PER_ALL_ASSIGNMENTS_F.EFFECTIVE_START_DATE <= Date('%CYEARE%')) OR
      (PER_ALL_ASSIGNMENTS_F.EFFECTIVE_END_DATE >= Date('%CYEARE%') AND
      PER_ALL_ASSIGNMENTS_F.EFFECTIVE_START_DATE <= Date('%CYEARB%'))
    """
    so_curs.execute("DROP VIEW IF EXISTS X000_ASSIGNMENTS")
    s_sql = s_sql.replace("%CYEARB%", funcdatn.get_current_year_begin())
    s_sql = s_sql.replace("%CYEARE%", funcdatn.get_current_year_end())
    so_curs.execute("DROP VIEW IF EXISTS X000_PER_ALL_ASSIGNMENTS")
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD VIEW: X000_PER_ALL_ASSIGNMENTS")

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

    """ ****************************************************************************
    BUILD SECONDARY ASSIGNMENTS
    *****************************************************************************"""
    print("BUILD SECONDARY ASSIGNMENTS")
    funcfile.writelog("BUILD SECONDARY ASSIGNMENTS")

    # BUILD SECONDARY ASSIGNMENTS
    print("Build secondary assignments...")
    sr_file = "X000_PER_ALL_ASSIGNMENTS_SECONDARY"
    s_sql = "CREATE VIEW " + sr_file + " AS " + """
    SELECT
        SEC.ASSIGNMENT_EXTRA_INFO_ID,
        SEC.ASSIGNMENT_ID,
        SEC.INFORMATION_TYPE,
        SEC.AEI_INFORMATION_CATEGORY,
        SEC.AEI_INFORMATION1 As SEC_POSITION,
        SEC.AEI_INFORMATION2 As SEC_ASS_ID,
        Upper(SEC.AEI_INFORMATION3) As SEC_BUDGET,
        SEC.AEI_INFORMATION4 As SEC_BUDGET_POSITION,
        Cast(SEC.AEI_INFORMATION5 as REAL) As SEC_BUDGET_VALUE,
        SEC.AEI_INFORMATION6 As SEC_YEAR,
        Cast(SEC.AEI_INFORMATION7 As REAL) As SEC_HOURS_FORECAST,
        SEC.AEI_INFORMATION8,
        SEC.AEI_INFORMATION9,
        SEC.AEI_INFORMATION10,
        SEC.AEI_INFORMATION11,
        Replace(SEC.AEI_INFORMATION12,'/','-') As SEC_DATE_FROM,
        Replace(SEC.AEI_INFORMATION13,'/','-') As SEC_DATE_TO,
        Upper(SEC.AEI_INFORMATION14) As SEC_TYPE,
        Cast(SEC.AEI_INFORMATION15 As REAL) As SEC_RATE,
        Upper(SEC.AEI_INFORMATION16) As SEC_UNIT,
        SEC.AEI_INFORMATION17 As SEC_APPROVER,
        Upper(SEC.AEI_INFORMATION18) As SEC_POSITION_NAME,
        SEC.AEI_INFORMATION19,
        Upper(SEC.AEI_INFORMATION20) As SEC_FULLPART_FLAG,
        SEC.AEI_INFORMATION21,
        SEC.AEI_INFORMATION22,
        SEC.AEI_INFORMATION23,
        SEC.AEI_INFORMATION24 As SEC_CHART,
        SEC.AEI_INFORMATION25 As SEC_ACCOUNT,
        SEC.AEI_INFORMATION26 As SEC_OBJECT,
        SEC.AEI_INFORMATION27,
        SEC.AEI_INFORMATION28,
        SEC.AEI_INFORMATION29,
        SEC.AEI_INFORMATION30,
        SEC.LAST_UPDATE_DATE,
        SEC.OBJECT_VERSION_NUMBER,
        SEC.LAST_UPDATED_BY,
        SEC.LAST_UPDATE_LOGIN,
        SEC.CREATED_BY,
        SEC.CREATION_DATE
    FROM
        PER_ASSIGNMENT_EXTRA_INFO_SEC SEC
    ORDER BY
        ASSIGNMENT_ID,
        AEI_INFORMATION12            
    ;"""
    so_curs.execute("DROP VIEW IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD VIEW: " + sr_file)

    """ ****************************************************************************
    BUILD CURRENT YEAR SECONDARY ASSIGNMENTS
    *****************************************************************************"""
    print("BUILD CURRENT YEAR SECONDARY ASSIGNMENTS")
    funcfile.writelog("BUILD CURRENT YEAR SECONDARY ASSIGNMENTS")

    # BUILD CURRENT YEAR SECONDARY ASSIGNMENTS
    print("Build current year secondary assignments...")
    sr_file = "X001_ASSIGNMENT_SEC_CURR_YEAR"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        SEC.ASSIGNMENT_EXTRA_INFO_ID,
        SEC.ASSIGNMENT_ID,
        SEC.SEC_POSITION,
        SEC.SEC_ASS_ID,
        SEC.SEC_BUDGET,
        SEC.SEC_BUDGET_POSITION,
        SEC.SEC_BUDGET_VALUE,
        SEC.SEC_YEAR,
        SEC.SEC_HOURS_FORECAST,
        SEC.SEC_DATE_FROM,
        SEC.SEC_DATE_TO,
        SEC.SEC_TYPE,
        SEC.SEC_RATE,
        SEC.SEC_UNIT,
        SEC.SEC_APPROVER,
        SEC.SEC_POSITION_NAME,
        SEC.SEC_FULLPART_FLAG,
        SEC.SEC_CHART,
        SEC.SEC_ACCOUNT,
        SEC.SEC_OBJECT,
        SEC.LAST_UPDATE_DATE,
        SEC.OBJECT_VERSION_NUMBER,
        SEC.LAST_UPDATED_BY,
        SEC.LAST_UPDATE_LOGIN,
        SEC.CREATED_BY,
        SEC.CREATION_DATE
    From
        X000_PER_ALL_ASSIGNMENTS_SECONDARY SEC
    Where
        (SEC.SEC_DATE_TO >= Date('%CYEARB%') AND
        SEC.SEC_DATE_TO <= Date('%CYEARE%')) OR
        (SEC.SEC_DATE_FROM >= Date('%CYEARB%') AND
        SEC.SEC_DATE_FROM <= Date('%CYEARE%')) OR
        (SEC.SEC_DATE_TO >= Date('%CYEARE%') AND
        SEC.SEC_DATE_FROM <= Date('%CYEARB%'))
    """
    s_sql = s_sql.replace("%CYEARB%", funcdatn.get_current_year_begin())
    s_sql = s_sql.replace("%CYEARE%", funcdatn.get_current_year_end())
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD VIEW: " + sr_file)

    """ ****************************************************************************
    BUILD ALL PEOPLE
    *****************************************************************************"""
    print("BUILD ALL PEOPLE")
    funcfile.writelog("BUILD ALL PEOPLE")

    # PER ALL PEOPLE
    print("Build per all people...")
    s_sql = "CREATE VIEW X000_PER_ALL_PEOPLE AS " + """
    SELECT
      PEOP.PERSON_ID,
      PEOP.PARTY_ID,
      PEOP.EMPLOYEE_NUMBER,
      PEOP.FULL_NAME,
      PEOP.DATE_OF_BIRTH,
      PEOP.SEX,
      PEOP.NATIONAL_IDENTIFIER,
      PEOP.EFFECTIVE_START_DATE,
      PEOP.EFFECTIVE_END_DATE,
      PEOP.EMAIL_ADDRESS,
      PEOP.TITLE,
      HR_LOOKUPS_TITLE.MEANING AS TITLE_FULL,
      PEOP.FIRST_NAME,
      PEOP.MIDDLE_NAMES,
      PEOP.LAST_NAME,
      PEOP.PERSON_TYPE_ID,
      PER_PERSON_TYPES.USER_PERSON_TYPE,
      PEOP.MARITAL_STATUS,
      PEOP.NATIONALITY,
      HR_LOOKUPS_NATIONALITY.MEANING AS NATIONALITY_NAME,
      PEOP.ATTRIBUTE1,
      PEOP.ATTRIBUTE2 AS INT_MAIL,
      PEOP.ATTRIBUTE3,
      PEOP.ATTRIBUTE4 AS KNOWN_NAME,
      PEOP.ATTRIBUTE5,
      PEOP.ATTRIBUTE6,
      PEOP.ATTRIBUTE7,
      PEOP.PER_INFORMATION_CATEGORY,
      PEOP.PER_INFORMATION1 AS TAX_NUMBER,
      PEOP.PER_INFORMATION2,
      PEOP.PER_INFORMATION3,
      PEOP.PER_INFORMATION4 AS RACE_CODE,
      HR_LOOKUPS_RACE.MEANING AS RACE_DESC,  
      PEOP.PER_INFORMATION5,
      PEOP.PER_INFORMATION6 AS LANG_CODE,
      HR_LOOKUPS_LANG.MEANING AS LANG_DESC,  
      PEOP.PER_INFORMATION7,
      PEOP.PER_INFORMATION8,
      PEOP.PER_INFORMATION9,
      PEOP.PER_INFORMATION10,
      PEOP.PER_INFORMATION11,
      PEOP.PER_INFORMATION12,
      PEOP.PER_INFORMATION13,
      PEOP.PER_INFORMATION14,
      PEOP.CREATED_BY,
      PEOP.CREATION_DATE,
      PEOP.LAST_UPDATE_DATE,
      PEOP.LAST_UPDATED_BY,
      PEOP.LAST_UPDATE_LOGIN,
      PEOP.ORIGINAL_DATE_OF_HIRE,
      PEOP.START_DATE,
      PEOP.DATE_OF_DEATH,
      PEOP.RECEIPT_OF_DEATH_CERT_DATE,
      PEOP.OBJECT_VERSION_NUMBER,
      PEOP.BUSINESS_GROUP_ID,
      PEOP.CURRENT_EMP_OR_APL_FLAG,
      PEOP.CURRENT_EMPLOYEE_FLAG,
      PEOP.DATE_EMPLOYEE_DATA_VERIFIED,
      PEOP.RESUME_EXISTS,
      PEOP.RESUME_LAST_UPDATED,
      PEOP.REGISTERED_DISABLED_FLAG,
      PEOP.SECOND_PASSPORT_EXISTS,
      PEOP.PREVIOUS_LAST_NAME
    FROM
      PER_ALL_PEOPLE_F PEOP Left Join
      PER_PERSON_TYPES ON PER_PERSON_TYPES.PERSON_TYPE_ID = PEOP.PERSON_TYPE_ID Left Join
      HR_LOOKUPS HR_LOOKUPS_NATIONALITY ON HR_LOOKUPS_NATIONALITY.LOOKUP_CODE = PEOP.NATIONALITY AND
        HR_LOOKUPS_NATIONALITY.LOOKUP_TYPE = 'NATIONALITY' Left Join  
      HR_LOOKUPS HR_LOOKUPS_TITLE ON HR_LOOKUPS_TITLE.LOOKUP_CODE = PEOP.TITLE AND
        HR_LOOKUPS_TITLE.LOOKUP_TYPE = 'TITLE' Left Join
      HR_LOOKUPS HR_LOOKUPS_RACE ON HR_LOOKUPS_RACE.LOOKUP_CODE = PEOP.PER_INFORMATION4 AND
        HR_LOOKUPS_RACE.LOOKUP_TYPE = 'ZA_RACE' Left Join
      HR_LOOKUPS HR_LOOKUPS_LANG ON HR_LOOKUPS_LANG.LOOKUP_CODE = PEOP.PER_INFORMATION6 AND
        HR_LOOKUPS_LANG.LOOKUP_TYPE = 'ZA_LANG_PREF'
    ORDER BY
      PEOP.EMPLOYEE_NUMBER,
      PEOP.EFFECTIVE_START_DATE
    """
    so_curs.execute("DROP VIEW IF EXISTS X000_PER_ALL_PEOPLE")
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD VIEW: X000_PER_ALL_PEOPLE")

    """ ****************************************************************************
    BUILD PERSON TYPES
    *****************************************************************************"""
    print("BUILD PERSON TYPES")
    funcfile.writelog("BUILD PERSON TYPES")

    # BUILD PERSON TYPES ***********************************************************

    print("Build person types...")
    sr_file = "X000_PER_PEOPLE_TYPES"
    s_sql = "CREATE VIEW " + sr_file + " AS " + """
    SELECT
      PER_PERSON_TYPE_USAGES_F.PERSON_TYPE_USAGE_ID,
      PER_PERSON_TYPE_USAGES_F.PERSON_ID,
      PER_PERSON_TYPE_USAGES_F.PERSON_TYPE_ID,
      PER_PERSON_TYPE_USAGES_F.EFFECTIVE_START_DATE,
      PER_PERSON_TYPE_USAGES_F.EFFECTIVE_END_DATE,
      PER_PERSON_TYPES.ACTIVE_FLAG,
      PER_PERSON_TYPES.DEFAULT_FLAG,
      PER_PERSON_TYPES.SYSTEM_PERSON_TYPE,
      PER_PERSON_TYPES.USER_PERSON_TYPE
    FROM
      PER_PERSON_TYPE_USAGES_F
      LEFT JOIN PER_PERSON_TYPES ON PER_PERSON_TYPES.PERSON_TYPE_ID = PER_PERSON_TYPE_USAGES_F.PERSON_TYPE_ID
    ;"""
    so_curs.execute("DROP VIEW IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD VIEW: " + sr_file)

    """ ****************************************************************************
    COUNT RECORDS
    *****************************************************************************"""
    print("COUNT RECORDS")
    funcfile.writelog("COUNT RECORDS")

    # ASSIGNMENTS
    print("Count assignments...")
    s_sql = "CREATE VIEW X000_COUNT_ASSIGNMENTS AS" + """
    SELECT
      PER_ALL_ASSIGNMENTS_F.PERSON_ID,
      Count(PER_ALL_ASSIGNMENTS_F.PRIMARY_FLAG) AS COUNT_ASS
    FROM
      PER_ALL_ASSIGNMENTS_F
    GROUP BY
      PER_ALL_ASSIGNMENTS_F.PERSON_ID
    """
    so_curs.execute("DROP VIEW IF EXISTS X000_COUNT_ASSIGNMENTS")
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD VIEW: X000_COUNT_ASSIGNMENTS")

    # PEOPLE
    print("Count people...")
    s_sql = "CREATE VIEW X000_COUNT_PEOPLE AS" + """
    SELECT
      PER_ALL_PEOPLE_F.PERSON_ID,
      Count(PER_ALL_PEOPLE_F.PARTY_ID) AS COUNT_PEO
    FROM
      PER_ALL_PEOPLE_F
    GROUP BY
      PER_ALL_PEOPLE_F.PERSON_ID
    """
    so_curs.execute("DROP VIEW IF EXISTS X000_COUNT_PEOPLE")
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD VIEW: X000_COUNT_PEOPLE")

    # PERIOD OF SERVICE
    print("Count periods of service...")
    s_sql = "CREATE VIEW X000_COUNT_PERIODOS AS" + """
    SELECT
      PER_PERIODS_OF_SERVICE.PERSON_ID,
      Count(PER_PERIODS_OF_SERVICE.BUSINESS_GROUP_ID) AS COUNT_POS
    FROM
      PER_PERIODS_OF_SERVICE
    GROUP BY
      PER_PERIODS_OF_SERVICE.PERSON_ID
    """
    so_curs.execute("DROP VIEW IF EXISTS X000_COUNT_PERIODOS")
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD VIEW: X000_COUNT_PERIODOS")

    # BUILD COUNTS
    print("Build counts...")
    s_sql = "CREATE VIEW X000_COUNTS AS" + """
    SELECT
      X000_COUNT_PEOPLE.PERSON_ID,
      X000_COUNT_PEOPLE.COUNT_PEO,
      X000_COUNT_PERIODOS.COUNT_POS,
      X000_COUNT_ASSIGNMENTS.COUNT_ASS
    FROM
      X000_COUNT_PEOPLE
      LEFT JOIN X000_COUNT_ASSIGNMENTS ON X000_COUNT_PEOPLE.PERSON_ID = X000_COUNT_ASSIGNMENTS.PERSON_ID
      LEFT JOIN X000_COUNT_PERIODOS ON X000_COUNT_PERIODOS.PERSON_ID = X000_COUNT_PEOPLE.PERSON_ID
    """
    so_curs.execute("DROP VIEW IF EXISTS X000_COUNTS")
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD VIEW: X000_COUNTS")

    """
    so_curs.execute("DROP VIEW IF EXISTS X000_COUNT_ASSIGNMENTS")
    funcfile.writelog("%t DROP TABLE: X000_COUNT_ASSIGNMENTS")
    so_curs.execute("DROP VIEW IF EXISTS X000_COUNT_PEOPLE")
    funcfile.writelog("%t DROP TABLE: X000_COUNT_PEOPLE")
    so_curs.execute("DROP VIEW IF EXISTS X000_COUNT_PERIODOS")
    funcfile.writelog("%t DROP TABLE: X000_COUNT_PERIODOS")
    """

    """ ****************************************************************************
    BUILD ADDRESSES AND PHONES
    *****************************************************************************"""
    print("BUILD ADDRESSES")
    funcfile.writelog("BUILD ADDRESSES")

    # 12 BUILD ADDRESSES
    print("Build addresses...")
    s_sql = "CREATE TABLE X000_ADDRESSES AS " + """
    Select
        ADDR.ADDRESS_ID,
        ADDR.PERSON_ID,
        ADDR.PARTY_ID,  
        ADDR.DATE_FROM,
        ADDR.DATE_TO,
        ADDR.STYLE,
        ADDR.ADDRESS_TYPE,
        ADDR.PRIMARY_FLAG,
        ADDR.ADDRESS_LINE1,
        ADDR.ADDRESS_LINE2,
        ADDR.ADDRESS_LINE3,
        ADDR.POSTAL_CODE,
        ADDR.TOWN_OR_CITY,
        ADDR.COUNTRY AS COUNTRY_CODE,
        COUN.MEANING AS COUNTRY_LOOKUP,
        Case
            When Length(COUN.MEANING) <> 0 Then Upper(COUN.MEANING)
            When Length(ADDR.COUNTRY) > 0 AND COUN.MEANING Is Null Then Upper(ADDR.COUNTRY)
            Else 'SOUTH AFRICA'
        End As COUNTRY_NAME,
        ADDR.REGION_1,
        ADDR.REGION_2,
        ADDR.REGION_3,
        ADDR.ADD_INFORMATION14,
        ADDR.ADD_INFORMATION15,
        ADDR.ADD_INFORMATION16,
        ADDR.ADD_INFORMATION17,
        ADDR.ADD_INFORMATION18,
        '' As ADDRESS_STYLE,
        '' As ADDRESS_SARS,
        '' As ADDRESS_HOME,
        '' As ADDRESS_POST,
        '' As ADDRESS_OTHE,
        ADDR.CREATION_DATE,
        ADDR.CREATED_BY,
        ADDR.LAST_UPDATED_BY,
        ADDR.LAST_UPDATE_DATE
    From
        PER_ADDRESSES ADDR Left Join
        HR_LOOKUPS COUN ON COUN.LOOKUP_CODE = ADDR.COUNTRY AND COUN.LOOKUP_TYPE = 'GHR_US_POSTAL_COUNTRY_CODE'
    """
    so_curs.execute("DROP TABLE IF EXISTS X000_ADDRESSES")
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: X000_ADDRESSES")

    # Calc ADDRESS_STYLE style field
    so_curs.execute("UPDATE X000_ADDRESSES " + """
    Set ADDRESS_STYLE = 
    Case
       When STYLE = "ZA_SARS" Then        UPPER( PRIMARY_FLAG ||'~'|| TRIM(ADDRESS_LINE1||' '||ADDRESS_LINE2)         ||'~'|| TRIM(ADDRESS_LINE3||' '||REGION_1)          ||'~'|| REGION_2      ||'~'|| TOWN_OR_CITY  ||'~'|| POSTAL_CODE ||'~'|| COUNTRY_NAME )
       When STYLE = "ZA_POST_STREET" Then UPPER( PRIMARY_FLAG ||'~'|| TRIM(ADD_INFORMATION15||' '||ADD_INFORMATION16) ||'~'|| TRIM(ADD_INFORMATION17||' '||ADDRESS_LINE1) ||'~'|| ADDRESS_LINE2 ||'~'|| ADDRESS_LINE3 ||'~'|| POSTAL_CODE ||'~'|| COUNTRY_NAME )
       When STYLE = "ZA_POST_POBOX"  Then UPPER( PRIMARY_FLAG ||'~'||                                                   '~'|| TRIM('PO BOX '||ADDRESS_LINE2)              ||'~'                 ||'~'|| ADDRESS_LINE3 ||'~'|| POSTAL_CODE ||'~'|| COUNTRY_NAME )
       When STYLE = "ZA_POST_PBAG"   Then UPPER( PRIMARY_FLAG ||'~'||                                                   '~'|| TRIM('P BAG '||ADDRESS_LINE2)               ||'~'                 ||'~'|| ADDRESS_LINE3 ||'~'|| POSTAL_CODE ||'~'|| COUNTRY_NAME )
       When STYLE = "ZA_POST_SSERV"  Then UPPER( PRIMARY_FLAG ||'~'|| REGION_3                                        ||'~'|| TRIM(ADDRESS_LINE1||' '||ADDRESS_LINE2)     ||'~'                 ||'~'|| ADDRESS_LINE3 ||'~'|| POSTAL_CODE ||'~'|| COUNTRY_NAME )
       Else                               UPPER( PRIMARY_FLAG ||'~'||                                                   '~'|| ADDRESS_LINE1                               ||'~'|| ADDRESS_LINE2 ||'~'|| ADDRESS_LINE3 ||'~'|| POSTAL_CODE ||'~'|| COUNTRY_NAME )
    End
    ;""")
    so_conn.commit()
    funcfile.writelog("%t ADD COLUMN: ADDRESS_STYLE")

    # Calc ADDRESS_SARS field
    so_curs.execute("UPDATE X000_ADDRESSES " + """
    SET ADDRESS_SARS = 
    Case
       When ADDRESS_TYPE = "ZA_RES" Then ADDRESS_STYLE
       Else ''
    End
    ;""")
    so_conn.commit()
    funcfile.writelog("%t ADD COLUMN: ADDRESS_SARS")

    # Calc ADDRESS_HOME field
    so_curs.execute("UPDATE X000_ADDRESSES " + """
    Set ADDRESS_HOME = 
    Case
        When ADDRESS_TYPE = "H" Then ADDRESS_STYLE
        Else ''
    End
    ;""")
    so_conn.commit()
    funcfile.writelog("%t ADD COLUMN: ADDRESS_HOME")

    # Calc ADDRESS_POST field
    so_curs.execute("UPDATE X000_ADDRESSES " + """
    SET ADDRESS_POST = 
    Case
        When ADDRESS_TYPE = "P" Then ADDRESS_STYLE
        Else ''
    End
    ;""")
    so_conn.commit()
    funcfile.writelog("%t ADD COLUMN: ADDRESS_POST")

    # Calc ADDRESS_OTHE field
    so_curs.execute("UPDATE X000_ADDRESSES " + """
    SET ADDRESS_OTHE = 
    Case
       When ADDRESS_TYPE = "H" Then ''
       When ADDRESS_TYPE = "P" Then ''
       When ADDRESS_TYPE = "ZA_RES" Then ''
       Else ADDRESS_STYLE
    End
    ;""")
    so_conn.commit()
    funcfile.writelog("%t ADD COLUMN: ADDRESS_OTHE")

    # 13 Build ADDRESS SARS ********************************************************

    print("Build sars addresses...")

    s_sql = "CREATE VIEW X000_ADDRESS_SARS AS " + """
    SELECT
      X000_ADDRESSES.ADDRESS_ID,
      X000_ADDRESSES.PERSON_ID,
      X000_ADDRESSES.DATE_FROM,
      X000_ADDRESSES.DATE_TO,
      X000_ADDRESSES.ADDRESS_SARS
    FROM
      X000_ADDRESSES
    WHERE
      LENGTH(X000_ADDRESSES.ADDRESS_SARS) > 0
    """
    so_curs.execute("DROP VIEW IF EXISTS X000_ADDRESS_SARS")
    so_curs.execute(s_sql)
    so_conn.commit()

    funcfile.writelog("%t BUILD VIEW: X000_ADDRESS_SARS")

    # 14 Build ADDRESS POST ********************************************************

    print("Build post addresses...")

    s_sql = "CREATE VIEW X000_ADDRESS_POST AS " + """
    SELECT
      X000_ADDRESSES.ADDRESS_ID,
      X000_ADDRESSES.PERSON_ID,
      X000_ADDRESSES.DATE_FROM,
      X000_ADDRESSES.DATE_TO,
      X000_ADDRESSES.ADDRESS_POST
    FROM
      X000_ADDRESSES
    WHERE
      LENGTH(X000_ADDRESSES.ADDRESS_POST) > 0
    """
    so_curs.execute("DROP VIEW IF EXISTS X000_ADDRESS_POST")
    so_curs.execute(s_sql)
    so_conn.commit()

    funcfile.writelog("%t BUILD VIEW: X000_ADDRESS_POST")

    # 15 Build ADDRESS HOME ********************************************************

    print("Build home addresses...")

    s_sql = "CREATE VIEW X000_ADDRESS_HOME AS " + """
    SELECT
      X000_ADDRESSES.ADDRESS_ID,
      X000_ADDRESSES.PERSON_ID,
      X000_ADDRESSES.DATE_FROM,
      X000_ADDRESSES.DATE_TO,
      X000_ADDRESSES.ADDRESS_HOME
    FROM
      X000_ADDRESSES
    WHERE
      LENGTH(X000_ADDRESSES.ADDRESS_HOME) > 0
    """
    so_curs.execute("DROP VIEW IF EXISTS X000_ADDRESS_HOME")
    so_curs.execute(s_sql)
    so_conn.commit()

    funcfile.writelog("%t BUILD VIEW: X000_ADDRESS_HOME")

    # 16 Build ADDRESS OTHE ********************************************************

    print("Build other addresses...")

    s_sql = "CREATE VIEW X000_ADDRESS_OTHE AS " + """
    SELECT
      X000_ADDRESSES.ADDRESS_ID,
      X000_ADDRESSES.PERSON_ID,
      X000_ADDRESSES.DATE_FROM,
      X000_ADDRESSES.DATE_TO,
      X000_ADDRESSES.ADDRESS_OTHE
    FROM
      X000_ADDRESSES
    WHERE
      LENGTH(X000_ADDRESSES.ADDRESS_OTHE) > 0
    """
    so_curs.execute("DROP VIEW IF EXISTS X000_ADDRESS_OTHE")
    so_curs.execute(s_sql)
    so_conn.commit()

    funcfile.writelog("%t BUILD VIEW: X000_ADDRESS_OTHE")

    """
    so_curs.execute("DROP TABLE IF EXISTS X000_ADDRESSES")
    funcfile.writelog("%t DROP TABLE: X000_ADDRESSES")
    """

    # 17 Build PHONE MOBILE CURR ***************************************************

    print("Build current mobile phones...")

    s_sql = "CREATE VIEW X000_PHONE_MOBI_CURR AS " + """
    SELECT
      X001_ASSIGNMENT_CURR.PERSON_ID,
      X001_ASSIGNMENT_CURR.DATE_EMP_LOOKUP,
      PER_PHONES.PHONE_ID,
      PER_PHONES.DATE_FROM,
      PER_PHONES.DATE_TO,
      PER_PHONES.PHONE_TYPE,
      PER_PHONES.PHONE_NUMBER AS PHONE_MOBI
    FROM
      PER_PHONES
      INNER JOIN X001_ASSIGNMENT_CURR ON PER_PHONES.PARENT_ID = X001_ASSIGNMENT_CURR.PERSON_ID AND
        PER_PHONES.DATE_FROM <= X001_ASSIGNMENT_CURR.DATE_EMP_LOOKUP AND
        PER_PHONES.DATE_TO >= X001_ASSIGNMENT_CURR.DATE_EMP_LOOKUP
    WHERE    
         PER_PHONES.PHONE_TYPE = 'M'
    ORDER BY
      X001_ASSIGNMENT_CURR.PERSON_ID,
      PER_PHONES.DATE_FROM
    """
    so_curs.execute("DROP VIEW IF EXISTS X000_PHONE_MOBI_CURR")
    so_curs.execute(s_sql)
    so_conn.commit()

    funcfile.writelog("%t BUILD VIEW: X000_PHONE_MOBI_CURR")

    # 18 Build PHONE MOBILE CURR LIST **********************************************

    print("Build current mobile phone list...")

    s_sql = "CREATE VIEW X000_PHONE_MOBI_CURR_LIST AS " + """
    SELECT
      X000_PHONE_MOBI_CURR.PERSON_ID,
      X000_PHONE_MOBI_CURR.DATE_EMP_LOOKUP,
      X000_PHONE_MOBI_CURR.PHONE_ID,
      MAX(X000_PHONE_MOBI_CURR.DATE_FROM) DATE_FROM,
      X000_PHONE_MOBI_CURR.DATE_TO,
      X000_PHONE_MOBI_CURR.PHONE_TYPE,
      X000_PHONE_MOBI_CURR.PHONE_MOBI
    FROM
      X000_PHONE_MOBI_CURR
    GROUP BY
      X000_PHONE_MOBI_CURR.PERSON_ID
    """
    so_curs.execute("DROP VIEW IF EXISTS X000_PHONE_MOBI_CURR_LIST")
    so_curs.execute(s_sql)
    so_conn.commit()

    funcfile.writelog("%t BUILD VIEW: X000_PHONE_MOBI_CURR_LIST")

    # 19 Build PHONE WORK CURR ***************************************************

    print("Build current work phones...")

    s_sql = "CREATE VIEW X000_PHONE_WORK_CURR AS " + """
    SELECT
      X001_ASSIGNMENT_CURR.PERSON_ID,
      X001_ASSIGNMENT_CURR.DATE_EMP_LOOKUP,
      PER_PHONES.PHONE_ID,
      PER_PHONES.DATE_FROM,
      PER_PHONES.DATE_TO,
      PER_PHONES.PHONE_TYPE,
      PER_PHONES.PHONE_NUMBER AS PHONE_WORK
    FROM
      PER_PHONES
      INNER JOIN X001_ASSIGNMENT_CURR ON PER_PHONES.PARENT_ID = X001_ASSIGNMENT_CURR.PERSON_ID AND
        PER_PHONES.DATE_FROM <= X001_ASSIGNMENT_CURR.DATE_EMP_LOOKUP AND
        PER_PHONES.DATE_TO >= X001_ASSIGNMENT_CURR.DATE_EMP_LOOKUP
    WHERE    
         PER_PHONES.PHONE_TYPE = 'W1'
    ORDER BY
      X001_ASSIGNMENT_CURR.PERSON_ID,
      PER_PHONES.DATE_FROM
    """
    so_curs.execute("DROP VIEW IF EXISTS X000_PHONE_WORK_CURR")
    so_curs.execute(s_sql)
    so_conn.commit()

    funcfile.writelog("%t BUILD VIEW: X000_PHONE_WORK_CURR")

    # 20 Build PHONE WORK CURR LIST **********************************************

    print("Build current work phone list...")

    s_sql = "CREATE VIEW X000_PHONE_WORK_CURR_LIST AS " + """
    SELECT
      X000_PHONE_WORK_CURR.PERSON_ID,
      X000_PHONE_WORK_CURR.DATE_EMP_LOOKUP,
      X000_PHONE_WORK_CURR.PHONE_ID,
      MAX(X000_PHONE_WORK_CURR.DATE_FROM) DATE_FROM,
      X000_PHONE_WORK_CURR.DATE_TO,
      X000_PHONE_WORK_CURR.PHONE_TYPE,
      X000_PHONE_WORK_CURR.PHONE_WORK
    FROM
      X000_PHONE_WORK_CURR
    GROUP BY
      X000_PHONE_WORK_CURR.PERSON_ID
    """
    so_curs.execute("DROP VIEW IF EXISTS X000_PHONE_WORK_CURR_LIST")
    so_curs.execute(s_sql)
    so_conn.commit()

    funcfile.writelog("%t BUILD VIEW: X000_PHONE_WORK_CURR_LIST")

    # 21 Build PHONE HOME CURR ***************************************************

    print("Build current home phones...")

    s_sql = "CREATE VIEW X000_PHONE_HOME_CURR AS " + """
    SELECT
      X001_ASSIGNMENT_CURR.PERSON_ID,
      X001_ASSIGNMENT_CURR.DATE_EMP_LOOKUP,
      PER_PHONES.PHONE_ID,
      PER_PHONES.DATE_FROM,
      PER_PHONES.DATE_TO,
      PER_PHONES.PHONE_TYPE,
      PER_PHONES.PHONE_NUMBER AS PHONE_HOME
    FROM
      PER_PHONES
      INNER JOIN X001_ASSIGNMENT_CURR ON PER_PHONES.PARENT_ID = X001_ASSIGNMENT_CURR.PERSON_ID AND
        PER_PHONES.DATE_FROM <= X001_ASSIGNMENT_CURR.DATE_EMP_LOOKUP AND
        PER_PHONES.DATE_TO >= X001_ASSIGNMENT_CURR.DATE_EMP_LOOKUP
    WHERE    
         PER_PHONES.PHONE_TYPE = 'H1'
    ORDER BY
      X001_ASSIGNMENT_CURR.PERSON_ID,
      PER_PHONES.DATE_FROM
    """
    so_curs.execute("DROP VIEW IF EXISTS X000_PHONE_HOME_CURR")
    so_curs.execute(s_sql)
    so_conn.commit()

    funcfile.writelog("%t BUILD VIEW: X000_PHONE_HOME_CURR")

    # 22 Build PHONE HOME CURR LIST **********************************************

    print("Build current home phone list...")

    s_sql = "CREATE VIEW X000_PHONE_HOME_CURR_LIST AS " + """
    SELECT
      X000_PHONE_HOME_CURR.PERSON_ID,
      X000_PHONE_HOME_CURR.DATE_EMP_LOOKUP,
      X000_PHONE_HOME_CURR.PHONE_ID,
      MAX(X000_PHONE_HOME_CURR.DATE_FROM) AS DATE_FROM,
      X000_PHONE_HOME_CURR.DATE_TO,
      X000_PHONE_HOME_CURR.PHONE_TYPE,
      X000_PHONE_HOME_CURR.PHONE_HOME
    FROM
      X000_PHONE_HOME_CURR
    GROUP BY
      X000_PHONE_HOME_CURR.PERSON_ID
    """
    so_curs.execute("DROP VIEW IF EXISTS X000_PHONE_HOME_CURR_LIST")
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD VIEW: X000_PHONE_HOME_CURR_LIST")

    print("Build view phone work latest...")
    sr_file = "X000_PHONE_WORK_LATEST"
    so_curs.execute("DROP VIEW IF EXISTS " + sr_file)
    s_sql = "CREATE VIEW " + sr_file + " AS " + """
    Select
        phon.PHONE_ID,
        phon.PARENT_ID,
        phon.PARENT_TABLE,
        phon.PHONE_TYPE,
        Max(phon.DATE_FROM) As DATE_FROM,
        phon.DATE_TO,
        phon.PHONE_NUMBER
    From
        PER_PHONES phon
    Where
        phon.PARENT_TABLE = 'PER_ALL_PEOPLE_F' And
        phon.PHONE_TYPE = 'W1' And
        phon.DATE_FROM <= date() And
        phon.DATE_TO >= date()
    Group By
        phon.PARENT_ID
    ;"""
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD VIEW: " + sr_file)

    print("Build view phone home latest...")
    sr_file = "X000_PHONE_HOME_LATEST"
    so_curs.execute("DROP VIEW IF EXISTS " + sr_file)
    s_sql = "CREATE VIEW " + sr_file + " AS " + """
    Select
        phon.PHONE_ID,
        phon.PARENT_ID,
        phon.PARENT_TABLE,
        phon.PHONE_TYPE,
        Max(phon.DATE_FROM) As DATE_FROM,
        phon.DATE_TO,
        phon.PHONE_NUMBER
    From
        PER_PHONES phon
    Where
        phon.PARENT_TABLE = 'PER_ALL_PEOPLE_F' And
        phon.PHONE_TYPE = 'H1' And
        phon.DATE_FROM <= date() And
        phon.DATE_TO >= date()
    Group By
        phon.PARENT_ID
    ;"""
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD VIEW: " + sr_file)

    print("Build view phone mobile latest...")
    sr_file = "X000_PHONE_MOBILE_LATEST"
    so_curs.execute("DROP VIEW IF EXISTS " + sr_file)
    s_sql = "CREATE VIEW " + sr_file + " AS " + """
    Select
        phon.PHONE_ID,
        phon.PARENT_ID,
        phon.PARENT_TABLE,
        phon.PHONE_TYPE,
        Max(phon.DATE_FROM) As DATE_FROM,
        phon.DATE_TO,
        phon.PHONE_NUMBER
    From
        PER_PHONES phon
    Where
        phon.PARENT_TABLE = 'PER_ALL_PEOPLE_F' And
        phon.PHONE_TYPE = 'M' And
        phon.DATE_FROM <= date() And
        phon.DATE_TO >= date()
    Group By
        phon.PARENT_ID
    ;"""
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD VIEW: " + sr_file)

    """ ****************************************************************************
    BUILD ASSIGNMENTS AND PEOPLE
    *****************************************************************************"""
    if l_debug:
        print("BUILD ASSIGNMENTS AND PEOPLE")
    funcfile.writelog("BUILD ASSIGNMENTS AND PEOPLE")

    # BUILD LIST OF CURRENT PEOPLE
    i_count = funcpeople.people_detail_list2(so_conn)
    # MESSAGE TO ADMIN
    if l_mess:
        # ACTIVE EMPLOYEES
        funcsms.send_telegram("", "administrator", "<b>" + str(i_count) + "</b> Active employees method 2")

    # BUILD LIST OF CURRENT PEOPLE
    i_count = funcpeople.people_detail_list(so_conn)
    if l_debug:
        print(i_count)

    # Build short list of employee contact details
    print("Build short list of employee contact details...")
    s_sql = "CREATE TABLE X000_PEOPLE_CONTACT AS " + """
    Select
        peop.employee_number,
        peop.name_address,
        lower(peop.email_address) As email_address12,
        peop.employee_number || '@nwu.ac.za' As email_address2
    From
        X000_PEOPLE peop
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS X000_PEOPLE_CONTACT")
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: X000_PEOPLE_CONTACT")

    if l_export:
        # EXPORT TABLE
        sr_file = "X000_PEOPLE"
        sx_path = re_path + funcdatn.get_current_year() + "/"
        sx_file = "People_000_all_"
        # sx_file_dated = sx_file + funcdatn.get_current_year()
        if l_debug:
            print("Export current people..." + sx_path + sx_file)
        # Read the header data
        s_head = funccsv.get_colnames_sqlite(so_conn, sr_file)
        # Write the data
        funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file, s_head)
        funcfile.writelog("%t EXPORT DATA: " + sx_path + sx_file)
        # Write the data dated
        # funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file_dated, s_head)
        # funcfile.writelog("%t EXPORT DATA: " + sx_path + sx_file_dated)
    if l_mess:
        # ACTIVE EMPLOYEES
        funcsms.send_telegram("", "administrator", "<b>" + str(i_count) + "</b> Active employees method 1")

    # Build current year assignment round 1 ******************************************
    funcpeople.assign01(so_conn, "X001_ASSIGNMENT_CURR_01", funcdatn.get_current_year_begin(), funcdatn.get_current_year_end(),
                        funcdatn.get_today_date(), "Build current year assignments 1...")
    # Build current year assignment round 2 ******************************************
    funcpeople.assign02(so_conn, "X001_ASSIGNMENT_CURR", "X001_ASSIGNMENT_CURR_01",
                        "Build current year assignments 2...")

    # Build previous year assignment round 1 ******************************************
    funcpeople.assign01(so_conn, "X001_ASSIGNMENT_PREV_01", funcdatn.get_previous_year_begin(), funcdatn.get_previous_year_end(),
                        funcdatn.get_previous_year_end(), "Build previous year assignments 1...")
    # Build previous year assignment round 2 ******************************************
    funcpeople.assign02(so_conn, "X001_ASSIGNMENT_PREV", "X001_ASSIGNMENT_PREV_01",
                        "Build previous year assignments 2...")

    # Build PEOPLE CURRENT ******************************************************
    i_count = funcpeople.people01(so_conn,
                                  "X002_PEOPLE_CURR",
                                  "X001_ASSIGNMENT_CURR",
                                  "CURR",
                                  "Build current people...",
                                  "Y")
    if l_mess:
        # ACTIVE EMPLOYEES
        funcsms.send_telegram("", "administrator", "<b>" + str(i_count) + "</b> Active employees method 3")

    # Build PEOPLE STRUCTURE LIST *********************************************
    print("Build a people structure list...")
    table_name = "X000_PEOPLE_STRUCT"
    s_sql = f"CREATE TABLE {table_name} AS " + """
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
    so_curs.execute(f"DROP TABLE IF EXISTS {table_name}")
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog(f"%t BUILD TABLE: {table_name}")
    i_count: int = funcsys.tablerowcount(so_curs, table_name)
    if l_mess:
        funcsms.send_telegram("", "administrator", "<b>" + str(i_count) + "</b> People structure records")

    # Build PEOPLE CURRENT ******************************************************
    funcpeople.people01(so_conn, "X002_PEOPLE_CURR_YEAR", "X001_ASSIGNMENT_CURR", "CURR",
                        "Build current year people ...", "N")

    # Build PEOPLE PREVIOUS YEAR ************************************************
    funcpeople.people01(so_conn, "X002_PEOPLE_PREV_YEAR", "X001_ASSIGNMENT_PREV", "CURR",
                        "Build previous year people...", "N")

    # Build PEOPLE ORGANIZATION STRUCTURE REF **********************************
    print("Build reference people organogram...")
    s_sql = "CREATE TABLE X003_PEOPLE_ORGA_REF AS " + """
    Select
        peop1.employee_number As employee_one,
        peop1.name_list As name_list_one,
        peop1.preferred_name As known_name_one,
        peop1.position_name As position_full_one,
        peop1.location As location_description_one,
        peop1.division As division_one,
        peop1.faculty As faculty_one,
        lower(peop1.email_address) As email_address_one,
        peop1.phone_work As phone_work_one,
        peop1.phone_mobile As phone_mobi_one,
        peop1.internal_box As phone_home_one,
        peop1.organization As org_name_one,
        peop1.grade_calc As grade_calc_one,
        peop2.employee_number As employee_two,
        peop2.name_list As name_list_two,
        peop2.preferred_name As known_name_two,
        peop2.position_name As position_full_two,
        peop2.location As location_description_two,
        peop2.division As division_two,
        peop2.faculty As faculty_two,
        lower(peop2.email_address) As email_address_two,
        peop2.phone_work As phone_work_two,
        peop2.phone_mobile As phone_mobi_two,
        peop2.internal_box As phone_home_two,
        peop3.employee_number As employee_three,
        peop3.name_list As name_list_three,
        peop3.preferred_name As known_name_three,
        peop3.position_name As position_full_three,
        peop3.location As location_description_three,
        peop3.division As division_three,
        peop3.faculty As faculty_three,
        lower(peop3.email_address) As email_address_three,
        peop3.phone_work As phone_work_three,
        peop3.phone_mobile As phone_mobi_three,
        peop3.internal_box As phone_home_three
    From
        X000_PEOPLE peop1 Left Join
        X000_PEOPLE peop2 On peop2.employee_number = peop1.supervisor_number Left Join
        X000_PEOPLE peop3 On peop3.employee_number = peop2.supervisor_number
    """
    so_curs.execute("DROP TABLE IF EXISTS X003_PEOPLE_ORGA_REF")
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: X003_PEOPLE_ORGA_REF")

    # 29 Build PEOPLE SUMMARY CURRENT **********************************************
    print("Build current people summary...")
    s_sql = "CREATE TABLE X003_PEOPLE_SUMM AS " + """
    SELECT
      X002_PEOPLE_CURR.EMPLOYEE_NUMBER,
      X002_PEOPLE_CURR.FULL_NAME,
      X002_PEOPLE_CURR.KNOWN_NAME,
      X002_PEOPLE_CURR.POSITION_FULL,
      X002_PEOPLE_CURR.SEX,
      X002_PEOPLE_CURR.DISABLED,
      X002_PEOPLE_CURR.PHONE_WORK,
      X002_PEOPLE_CURR.EMAIL_ADDRESS,
      X002_PEOPLE_CURR.INT_MAIL,
      X002_PEOPLE_CURR.LOCATION_DESCRIPTION,
      X002_PEOPLE_CURR.ORG_TYPE_DESC,
      X002_PEOPLE_CURR.OE_CODE,
      X002_PEOPLE_CURR.ORG_NAME,
      X002_PEOPLE_CURR.ACAD_SUPP,
      X002_PEOPLE_CURR.EMPLOYMENT_CATEGORY,
      X002_PEOPLE_CURR.GRADE,
      X002_PEOPLE_CURR.GRADE_CALC,
      X002_PEOPLE_CURR.GRADE_NAME,
      X002_PEOPLE_CURR.POSITION,
      X002_PEOPLE_CURR.POSITION_NAME,
      X002_PEOPLE_CURR.JOB_NAME,
      X002_PEOPLE_CURR.JOB_SEGMENT_NAME,
      X002_PEOPLE_CURR.SUPERVISOR
    FROM
      X002_PEOPLE_CURR
    ORDER BY
      X002_PEOPLE_CURR.ORG_NAME,
      X002_PEOPLE_CURR.GRADE_CALC,
      X002_PEOPLE_CURR.FULL_NAME
    """
    so_curs.execute("DROP TABLE IF EXISTS X003_PEOPLE_SUMM")
    so_curs.execute(s_sql)
    so_conn.commit()

    funcfile.writelog("%t BUILD TABLE: X003_PEOPLE_SUMM")

    # 30 Build PEOPLE ORGANIZATION MONTH *******************************************

    print("Build month people organogram...")

    s_sql = "CREATE TABLE X003_PEOPLE_ORGA AS " + """
    SELECT
      X002_PEOPLE_CURR.ASS_ID AS ass030_OraPosiId,
      X002_PEOPLE_CURR.EMPLOYEE_NUMBER AS per002_EmpNumb,
      X002_PEOPLE_CURR.TITLE_FULL AS per_013_PerTitl,
      X002_PEOPLE_CURR.FIRST_NAME AS per011_PerFirs,
      X002_PEOPLE_CURR.MIDDLE_NAMES AS per012_PerMidd,
      X002_PEOPLE_CURR.LAST_NAME AS per010_PerLast,
      X002_PEOPLE_CURR.PHONE_WORK AS pho021_PhoNumb,
      X002_PEOPLE_CURR.PHONE_MOBI AS pho021_PhoNumb2,
      X002_PEOPLE_CURR.INT_MAIL AS per036_PerBoxx,
      X002_PEOPLE_CURR.EMAIL_ADDRESS AS mai010_Emai,
      X002_PEOPLE_CURR.KNOWN_NAME AS mai011_Namc,
      X002_PEOPLE_CURR.INITIALS AS mai012_Init,
      X002_PEOPLE_CURR.IDNO AS bul005_Comb,
      X002_PEOPLE_CURR.JOB_NAME AS jod010_JodName,
      X002_PEOPLE_CURR.POSITION AS pod010_PodPosi,
      X002_PEOPLE_CURR.POSITION_NAME AS pod011_PodName,
      X002_PEOPLE_CURR.OE_CODE AS pos011_PosCost,
      X002_PEOPLE_CURR.ORG_NAME AS org011_OrgName,
      X002_PEOPLE_TWO.ASS_ID AS ass030_OraPosiId2,
      X002_PEOPLE_TWO.EMPLOYEE_NUMBER AS per002_EmpNumb2,
      X002_PEOPLE_TWO.TITLE_FULL AS per_013_PerTitl2,
      X002_PEOPLE_TWO.FIRST_NAME AS per011_PerFirs2,
      X002_PEOPLE_TWO.MIDDLE_NAMES AS per012_PerMidd2,
      X002_PEOPLE_TWO.LAST_NAME AS per010_PerLast2,
      X002_PEOPLE_TWO.PHONE_WORK AS pho021_PhoNumb3,
      X002_PEOPLE_TWO.PHONE_MOBI AS pho021_PhoNumb22,
      X002_PEOPLE_TWO.INT_MAIL AS per036_PerBoxx2,
      X002_PEOPLE_TWO.EMAIL_ADDRESS AS mai010_Emai2,
      X002_PEOPLE_TWO.KNOWN_NAME AS mai011_Namc2,
      X002_PEOPLE_TWO.INITIALS AS mai012_Init2,
      X002_PEOPLE_TWO.IDNO AS bul005_Comb2,
      X002_PEOPLE_TWO.JOB_NAME AS jod010_JodName2,
      X002_PEOPLE_TWO.POSITION AS pod010_PodPosi2,
      X002_PEOPLE_TWO.POSITION_NAME AS pod011_PodName2,
      X002_PEOPLE_TWO.OE_CODE AS pos011_PosCost2,
      X002_PEOPLE_TWO.ORG_NAME AS org011_OrgName2,
      X002_PEOPLE_THREE.ASS_ID AS ass030_OraPosiId3,
      X002_PEOPLE_THREE.EMPLOYEE_NUMBER AS per002_EmpNumb3,
      X002_PEOPLE_THREE.TITLE_FULL AS per_013_PerTitl3,
      X002_PEOPLE_THREE.FIRST_NAME AS per011_PerFirs3,
      X002_PEOPLE_THREE.MIDDLE_NAMES AS per012_PerMidd3,
      X002_PEOPLE_THREE.LAST_NAME AS per010_PerLast3,
      X002_PEOPLE_THREE.PHONE_WORK AS pho021_PhoNumb4,
      X002_PEOPLE_THREE.PHONE_MOBI AS pho021_PhoNumb23,
      X002_PEOPLE_THREE.INT_MAIL AS per036_PerBoxx3,
      X002_PEOPLE_THREE.EMAIL_ADDRESS AS mai010_Emai3,
      X002_PEOPLE_THREE.KNOWN_NAME AS mai011_Namc3,
      X002_PEOPLE_THREE.INITIALS AS mai012_Init3,
      X002_PEOPLE_THREE.IDNO AS bul005_Comb3,
      X002_PEOPLE_THREE.JOB_NAME AS jod010_JodName3,
      X002_PEOPLE_THREE.POSITION AS pod010_PodPosi3,
      X002_PEOPLE_THREE.POSITION_NAME AS pod011_PodName3,
      X002_PEOPLE_THREE.OE_CODE AS pos011_PosCost3,
      X002_PEOPLE_THREE.ORG_NAME AS org011_OrgName3,
      X002_PEOPLE_CURR.GRADE_CALC AS gra010_GraGras
    FROM
      X002_PEOPLE_CURR
      LEFT JOIN X002_PEOPLE_CURR X002_PEOPLE_TWO ON X002_PEOPLE_TWO.EMPLOYEE_NUMBER = X002_PEOPLE_CURR.SUPERVISOR
      LEFT JOIN X002_PEOPLE_CURR X002_PEOPLE_THREE ON X002_PEOPLE_THREE.EMPLOYEE_NUMBER = X002_PEOPLE_TWO.SUPERVISOR
    """
    so_curs.execute("DROP TABLE IF EXISTS X003_PEOPLE_ORGA")
    so_curs.execute(s_sql)
    so_conn.commit()

    funcfile.writelog("%t BUILD TABLE: X003_PEOPLE_ORGA")

    """ ****************************************************************************
    BUILD CURRENT SYSTEM USERS
    *****************************************************************************"""
    print("SYSTEM USERS")
    funcfile.writelog("SYSTEM USERS")

    # LOOKUP PARTY ID FOR USERS
    print("Lookup party id for users...")
    sr_file = "X000_USER_CURR_PARTY"
    s_sql = "CREATE VIEW " + sr_file + " AS " + """
    Select
        u.USER_ID,
        u.USER_NAME,
        u.PERSON_PARTY_ID,
        p.PARTY_ID
    From
        FND_USER u Inner Join
        X002_PEOPLE_CURR p On p.EMPLOYEE_NUMBER = u.USER_NAME
    Where
        u.PERSON_PARTY_ID = 0 And
        Cast(u.USER_NAME As Integer) > 0
    ;"""
    so_curs.execute("DROP VIEW IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD VIEW: " + sr_file)

    # LOOKUP PARTY ID FOR USERS
    print("Add party id for users...")
    sr_file = "FND_USER_PARTY"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        u.*,
        p.PARTY_ID As PEOPLE_PARTY_ID
    From
        FND_USER u Left Join
        X000_USER_CURR_PARTY p On p.USER_ID = u.USER_ID
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # CALCULATE USER FIELD
    print("Calculate party column...")
    so_curs.execute("UPDATE FND_USER_PARTY " + """
                    SET CALC_PARTY_ID =
                    CASE
                        WHEN PEOPLE_PARTY_ID <> '' THEN PEOPLE_PARTY_ID
                    ELSE
                        PERSON_PARTY_ID
                    END
                    ;""")
    so_conn.commit()
    funcfile.writelog("%t CALC COLUMN: CALC_PARTY_ID")

    # BUILD CURRENT USERS
    print("Build current users...")
    sr_file = "X000_USER_CURR"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        u.USER_ID,
        u.USER_NAME,
        p.EMPLOYEE_NUMBER,
        p.KNOWN_NAME,
        p.NAME_ADDR,
        p.EMAIL_ADDRESS,
        u.LAST_LOGON_DATE,
        p.ORG_NAME
    From
        FND_USER_PARTY u Inner Join
        X002_PEOPLE_CURR p On p.PARTY_ID = u.CALC_PARTY_ID
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # Delete some unncessary files *************************************************

    so_curs.execute("DROP VIEW IF EXISTS X000_PHONE_HOME_CURR")
    so_curs.execute("DROP VIEW IF EXISTS X000_PHONE_HOME_CURR_LIST")
    so_curs.execute("DROP VIEW IF EXISTS X000_PHONE_MOBI_CURR")
    so_curs.execute("DROP VIEW IF EXISTS X000_PHONE_MOBI_CURR_LIST")
    so_curs.execute("DROP VIEW IF EXISTS X000_PHONE_WORK_CURR")
    so_curs.execute("DROP VIEW IF EXISTS X000_PHONE_WORK_CURR_LIST")

    """ ****************************************************************************
    BUILD SPOUSES
    *****************************************************************************"""

    # BUILD SPOUSE MASTER
    print("Build spouse master file...")
    sr_file = "X000_SPOUSE"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        ppei.PERSON_EXTRA_INFO_ID As person_extra_info_id,
        ppei.PERSON_ID As person_id,
        ppei.PEI_INFORMATION10 As spouse_number,
        ppei.PEI_INFORMATION5 As spouse_title,
        ppei.PEI_INFORMATION4 As spouse_initials,
        ppei.PEI_INFORMATION3 As spouse_name_last,
        Trim(Replace(Substr(ppei.PEI_INFORMATION9,1,10),'/','-')) As spouse_date_of_birth,
        ppei.PEI_INFORMATION6 As spouse_national_identifier,
        ppei.PEI_INFORMATION7 As spouse_passport,
        Trim(Replace(Substr(ppei.PEI_INFORMATION1,1,10),'/','-')) As spouse_start_date,
        Case
            When Trim(Replace(Substr(ppei.PEI_INFORMATION2,1,10),'/','-')) <> '' Then Trim(Replace(Substr(ppei.PEI_INFORMATION2,1,10),'/','-'))  
            Else '4712-12-31'
        End As spouse_end_date,    
        Datetime(ppei.CREATION_DATE) As spouse_create_date,
        ppei.CREATED_BY As spouse_created_by,
        Datetime(ppei.LAST_UPDATE_DATE) As spouse_update_date,
        ppei.LAST_UPDATED_BY As spouse_updated_by,
        ppei.LAST_UPDATE_LOGIN As spouse_update_login
    From
        PER_PEOPLE_EXTRA_INFO ppei
    Where
        ppei.INFORMATION_TYPE = "NWU_SPOUSE"
    ;"""
    s_sql = s_sql.replace("%DATE%", funcdatn.get_today_date())
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD CURRENT SPOUSES
    print("Build current spouses...")
    sr_file = "X002_SPOUSE_CURR"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        papf.employee_number,
        ppei.person_extra_info_id,
        ppei.person_id,
        ppei.spouse_number,
        Case
            When spon.employee_number is not null Then 1
            Else 0
        End As spouse_active,
        ppei.spouse_title,
        ppei.spouse_initials,
        ppei.spouse_name_last,
        Trim(ppei.spouse_title || " " || ppei.spouse_initials || " " || ppei.spouse_name_last) As spouse_address,
        ppei.spouse_date_of_birth,
        ppei.spouse_national_identifier,
        ppei.spouse_passport,
        spon.user_person_type As spouse_person_type,
        spon.marital_status As spouse_marital_status,        
        ppei.spouse_start_date,
        ppei.spouse_end_date,
        ppei.spouse_create_date,
        ppei.spouse_created_by,
        Max(ppei.spouse_update_date) As spouse_update_date,
        ppei.spouse_updated_by,
        ppei.spouse_update_login,
        StrfTime('%Y', 'now') - StrfTime('%Y', ppei.spouse_date_of_birth) As spouse_age
    From
        X000_SPOUSE ppei Inner Join
        X000_PEOPLE papf On papf.person_id = ppei.person_id Left Join
        X000_PEOPLE spon On spon.employee_number = ppei.spouse_number
    Where
        StrfTime('%Y-%m-%d', 'now') Between ppei.spouse_start_date And ppei.spouse_end_date
    Group By
        papf.employee_number
    ;"""

    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # Close the connection *********************************************************
    so_conn.close()

    """ ****************************************************************************
    BUILD PEOPLE LEAVE
    *****************************************************************************"""
    print("BUILD PEOPLE LEAVE")
    funcfile.writelog("BUILD PEOPLE LEAVE")

    # PEOPLE_LEAVE Database ********************************************************

    # Declare variables
    so_path = f"{funcconf.drive_data_raw}People_leave/"  # Source database path
    so_file = "People_leave.sqlite"  # Source database

    # Open the SOURCE file
    with sqlite3.connect(so_path + so_file) as so_conn:
        so_curs = so_conn.cursor()
    funcfile.writelog("OPEN DATABASE: " + so_file)
    so_curs.execute(f"ATTACH DATABASE '{funcconf.drive_data_raw}PEOPLE/People.sqlite' AS 'PEOPLE'")
    funcfile.writelog("%t ATTACH DATABASE: PEOPLE.SQLITE")

    # TODO Delete after running on 20241119
    # so_curs.execute("DROP TABLE IF EXISTS X000_PEOPLE_STRUCT")
    # so_curs.execute("DROP TABLE IF EXISTS X103_PER_ABSENCE_ATTENDANCES")
    # so_curs.execute("DROP TABLE IF EXISTS X100_PER_ABSENCE_ATTENDANCES")
    # so_curs.execute("DROP TABLE IF EXISTS X101_PER_ABS_ATTENDANCE_REASONS")
    # so_curs.execute("DROP TABLE IF EXISTS X102_PER_ABSENCE_ATTENDANCE_TYPES")
    # so_curs.execute("DROP TABLE IF EXISTS X103_PER_ABSENCE_ATTENDANCES_CURR")
    # so_curs.execute("DROP TABLE IF EXISTS X103_PER_ABSENCE_ATTENDANCES_PREV")

    # BUILD LEAVE REASONS *********************************************************

    print("Build leave reasons...")
    table_name = "X000_Leave_reasons"
    s_sql = f"Create table {table_name} As " + """
    Select
        paar.ABS_ATTENDANCE_REASON_ID,
        paar.NAME As LEAVE_REASON,
        Upper(loor.MEANING) As LEAVE_REASON_DESCRIPTION,
        paar.ABSENCE_ATTENDANCE_TYPE_ID,
        Upper(paat.NAME) As LEAVE_TYPE,
        paat.ABSENCE_CATEGORY As LEAVE_CATEGORY,
        Upper(loot.MEANING) As LEAVE_CATEGORY_DESCRIPTION,
        paat.HOURS_OR_DAYS,
        paat.INCREASING_OR_DECREASING_FLAG,
        paat.INPUT_VALUE_ID,
        paar.CREATED_BY,
        paar.CREATION_DATE,
        paar.LAST_UPDATED_BY,
        paar.LAST_UPDATE_DATE,
        paar.LAST_UPDATE_LOGIN
    From
        PER_ABS_ATTENDANCE_REASONS paar Left Join
        PER_ABSENCE_ATTENDANCE_TYPES paat On paat.ABSENCE_ATTENDANCE_TYPE_ID = paar.ABSENCE_ATTENDANCE_TYPE_ID Left Join
        HR_LOOKUPS loor On loor.LOOKUP_CODE = paar.NAME
                And loor.LOOKUP_TYPE = 'ABSENCE_REASON' Left Join
        HR_LOOKUPS loot On loot.LOOKUP_CODE = paat.ABSENCE_CATEGORY
                And loot.LOOKUP_TYPE = 'ABSENCE_CATEGORY'
    Order By
        paar.ABS_ATTENDANCE_REASON_ID
    """
    so_curs.execute(f"DROP TABLE IF EXISTS {table_name}")
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog(f"%t BUILD TABLE: {table_name}")

    # BUILD LEAVE TYPES ***********************************************************

    print("Build leave types...")
    table_name = "X000_Leave_types"
    s_sql = f"Create table {table_name} As " + """
    Select
        paat.ABSENCE_ATTENDANCE_TYPE_ID,
        Upper(paat.NAME) As LEAVE_TYPE,
        paat.ABSENCE_CATEGORY As LEAVE_CATEGORY,
        Upper(loot.MEANING) As LEAVE_CATEGORY_DESCRIPTION,
        paat.HOURS_OR_DAYS,
        paat.INCREASING_OR_DECREASING_FLAG,
        paat.INPUT_VALUE_ID,
        paat.DATE_EFFECTIVE,
        paat.CREATED_BY,
        paat.CREATION_DATE,
        paat.LAST_UPDATED_BY,
        paat.LAST_UPDATE_DATE,
        paat.LAST_UPDATE_LOGIN
    From
        PER_ABSENCE_ATTENDANCE_TYPES paat Left Join
        HR_LOOKUPS loot On loot.LOOKUP_CODE = paat.ABSENCE_CATEGORY And loot.LOOKUP_TYPE = 'ABSENCE_CATEGORY'
    Order By
        paat.ABSENCE_ATTENDANCE_TYPE_ID
    """
    so_curs.execute(f"DROP TABLE IF EXISTS {table_name}")
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog(f"%t BUILD TABLE: {table_name}")

    # BUILD LEAVE LOG *************************************************************

    print("Build leave log...")
    table_name = "X000_Leave_log"
    s_sql = f"CREATE TABLE {table_name} AS " + """
    Select
        paal.ABSENCE_ATTENDANCE_ID,
        paal.PERSON_ID,
        paal.DATE_NOTIFICATION,
        paal.DATE_PROJECTED_START,
        paal.DATE_PROJECTED_END,
        paal.DATE_START,
        paal.DATE_END,
        paal.ABSENCE_DAYS,
        paal.ABSENCE_ATTENDANCE_TYPE_ID As LEAVE_TYPE_ID,
        paat.LEAVE_TYPE,
        paal.OCCURRENCE,
        paat.LEAVE_CATEGORY,
        paat.LEAVE_CATEGORY_DESCRIPTION,
        paal.ABS_ATTENDANCE_REASON_ID As LEAVE_REASON_ID,
        paar.LEAVE_REASON,
        paar.LEAVE_REASON_DESCRIPTION,
        paal.ATTRIBUTE5 As APPROVER_PERSON_ID,
        paal.AUTHORISING_PERSON_ID,
        paal.PROGRAM_APPLICATION_ID,
        paal.ATTRIBUTE1 As REFERENCE,
        paal.ATTRIBUTE2 As DATE_FORM,
        paal.ATTRIBUTE3 As DIY_REFERENCE,
        paal.ATTRIBUTE4 As DIY_COMMENT,
        paal.ATTRIBUTE6 As NEAR_PUBLIC_HOLIDAY,
        paal.ATTRIBUTE7 As NEAR_WEEKEND,
        paal.CREATED_BY,
        paal.CREATION_DATE,
        paal.LAST_UPDATED_BY As UPDATED_BY,
        paal.LAST_UPDATE_DATE As UPDATE_DATE,
        paal.LAST_UPDATE_LOGIN As UPDATE_LOGIN,
        paal.REASON_FOR_NOTIFICATION_DELAY,
        paal.ACCEPT_LATE_NOTIFICATION_FLAG,
        paal.OBJECT_VERSION_NUMBER
    From
        PER_ABSENCE_ATTENDANCES paal Left Join
        X000_Leave_types paat On paat.ABSENCE_ATTENDANCE_TYPE_ID = paal.ABSENCE_ATTENDANCE_TYPE_ID Left Join
        X000_Leave_reasons paar On paar.ABS_ATTENDANCE_REASON_ID = paal.ABS_ATTENDANCE_REASON_ID
    Order By
        paal.PERSON_ID,
        paal.CREATION_DATE
    """
    so_curs.execute(f"DROP TABLE IF EXISTS {table_name}")
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog(f"%t BUILD TABLE: {table_name}")

    """*****************************************************************************
    End OF SCRIPT
    *****************************************************************************"""
    print("END OF SCRIPT")
    funcfile.writelog("END OF SCRIPT")

    # COMMIT DATA
    so_conn.commit()

    # CLOSE THE DATABASE CONNECTION
    so_conn.close()

    # CLOSE THE LOG WRITER
    funcfile.writelog("----------------------------")
    funcfile.writelog("COMPLETED: B001_PEOPLE_LISTS")

    return


if __name__ == '__main__':
    try:
        people_lists()
    except Exception as e:
        funcsys.ErrMessage(e, False, "B001_people_lists", "B001_people_lists")
