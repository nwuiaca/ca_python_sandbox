"""
Script to build standard VSS lists
Created on: 01 Mar 2018
Copyright: Albert J v Rensburg
"""

# Import python modules
import sqlite3

# Import own modules
from _my_modules import funcconf
from _my_modules import funcdatn
from _my_modules import funccsv
from _my_modules import funcfile
from _my_modules import funcsms
from _my_modules import funcsys
from _my_modules import funcoracle


def payroll_lists(s_year: str = 'curr'):
    """
    Script to build payroll lists
    :param s_year: str: The financial period
    :return: Nothing
    """

    """ CONTENTS ***************************************************************
    PAYROLL RUN VALUES
    BUILD PAYROLL HISTORY
    ELEMENTS
    BALANCES
    SECONDARY ASSIGNMENTS
    *************************************************************************"""

    # Declare variables
    l_debug: bool = False
    l_export: bool = False
    so_path: str = f"{funcconf.drive_data_raw}People_payroll/"  # Source database path
    so_file: str = "People_payroll.sqlite"  # Source database
    re_path = f"{funcconf.drive_data_results}People/"  # Results
    # ed_path = "S:/_external_data/"

    funcfile.writelog("Now")
    funcfile.writelog("SCRIPT: B004_PAYROLL_LISTS")
    funcfile.writelog("--------------------------")
    if l_debug:
        print("------------------")
        print("B004_PAYROLL_LISTS")
        print("------------------")

    # MESSAGE
    if funcconf.l_mess_project:
        funcsms.send_telegram("", "administrator", "<b>B004 Payroll lists</b>")

    # Open the SOURCE file
    with sqlite3.connect(so_path+so_file) as so_conn:
        so_curs = so_conn.cursor()
    funcfile.writelog("OPEN DATABASE: " + so_file)

    """"
    # Create the connection
    so_conn = sqlite3.connect(so_path+so_file)
    
    # Create the cursor
    so_curs = con.cursor()
    
    # Provide the username and password
    username = input('Username: ')
    password = input('Password: ')
    
    # Execute the SQL query
    cursor.execute("SELECT username, password FROM users WHERE username=? AND password=?", (username, password))
    
    # Fetch the result
    result = cursor.fetchone()
    
    if result is None:
        print('Invalid username or password!')
    else:
        print('Login successful!')
    
    # Close the connection
    con.close()    
    """

    # Attach data sources
    so_curs.execute(f"ATTACH DATABASE '{funcconf.drive_data_raw}People/People.sqlite' AS 'PEOPLE'")
    funcfile.writelog("%t ATTACH DATABASE: PEOPLE.SQLITE")

    """*************************************************************************
    START
    *************************************************************************"""

    """*************************************************************************
    PAYROLL RUN VALUES
    *************************************************************************"""

    # For which year
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

    # Build the Oracle sql statement
    s_sql = """
    Select Distinct
        prr.RUN_RESULT_ID,
        pect.CLASSIFICATION_NAME,
        petf.ELEMENT_NAME,
        petf.REPORTING_NAME,
        ppa.EFFECTIVE_DATE,
        Cast(prrv.RESULT_VALUE As Decimal(13,2)) As RESULT_VALUE,
        paaf.LOCATION_ID,
        paaf.ORGANIZATION_ID,
        paaf.EMPLOYMENT_CATEGORY,
        paaf.POSITION_ID,
        paaf.EMPLOYEE_CATEGORY,
        paa.ASSIGNMENT_ID,
        papf.PERSON_ID,
        papf.EMPLOYEE_NUMBER
        --papf.EMPLOYEE_NUMBER,
        --papf.FULL_NAME    
        --piv.UOM,
        --piv.NAME,
        --petf.ELEMENT_TYPE_ID,
        --petf.EFFECTIVE_START_DATE,
        --petf.EFFECTIVE_END_DATE,
        --petf.DESCRIPTION As ELEMENT_DESCRIPTION,
        --pect.DESCRIPTION As CLASSIFICATION_DESCRIPTION,
        --paa.ASSIGNMENT_ACTION_ID,
        --paa.PAYROLL_ACTION_ID,
        --paa.PAYROLL_ACTION_ID#1,
        --paa.ACTION_STATUS,
        --ppa.PAYROLL_ACTION_ID As PAYROLL_ACTION_PPA,
        --ppa.ACTION_TYPE,
        --ppa.ACTION_SEQUENCE,
        --paaf.ASSIGNMENT_ID As ASSIGNMENT_PAAF,
        --paaf.EFFECTIVE_START_DATE As EFFECTIVE_START_PAAF,
        --paaf.EFFECTIVE_END_DATE As EFFECTIVE_END_PAAF,
        --papf.EFFECTIVE_START_DATE As EFFECTIVE_START_PAPF,
        --papf.EFFECTIVE_END_DATE As EFFECTIVE_END_PAPF,
    From
        HR.PAY_RUN_RESULTS prr,
        HR.PAY_RUN_RESULT_VALUES prrv,
        HR.PAY_INPUT_VALUES_F piv,
        HR.PAY_ELEMENT_TYPES_F petf,
        HR.PAY_ELEMENT_CLASSIFICATIONS_TL pect,
        HR.PAY_ASSIGNMENT_ACTIONS paa,
        APPS.PAY_PAYROLL_ACTIONS ppa,
        APPS.PER_ALL_ASSIGNMENTS_F paaf,
        HR.PER_ALL_PEOPLE_F papf  
    Where
        prr.RUN_RESULT_ID = prrv.RUN_RESULT_ID And
        piv.INPUT_VALUE_ID = prrv.INPUT_VALUE_ID And
        prrv.RESULT_VALUE != '0' And
        piv.UOM = 'M' And
        piv.NAME = 'Pay Value' And
        petf.ELEMENT_TYPE_ID = prr.ELEMENT_TYPE_ID And
        pect.CLASSIFICATION_ID = petf.CLASSIFICATION_ID And
        --pect.CLASSIFICATION_NAME In ('Normal Income', 'Allowances') And
        prr.ASSIGNMENT_ACTION_ID = paa.ASSIGNMENT_ACTION_ID And    
        paa.ASSIGNMENT_ID = paaf.ASSIGNMENT_ID And
        papf.PERSON_ID = paaf.PERSON_ID And
        paa.PAYROLL_ACTION_ID#1 = ppa.ACTION_SEQUENCE - 1 And
        paa.ACTION_STATUS = 'C' And
        ppa.ACTION_TYPE In ('R') And
        ppa.EFFECTIVE_DATE >= To_Date('%BEGIN%', 'YYYY-MM-DD') And
        ppa.EFFECTIVE_DATE <= To_Date('%END%', 'YYYY-MM-DD') And
        ppa.EFFECTIVE_DATE >= Trunc(paaf.EFFECTIVE_START_DATE) And
        ppa.EFFECTIVE_DATE <= Trunc(paaf.EFFECTIVE_END_DATE) And
        Trunc(paaf.EFFECTIVE_END_DATE) >= papf.EFFECTIVE_START_DATE And
        Trunc(paaf.EFFECTIVE_END_DATE) <= papf.EFFECTIVE_END_DATE
        --papf.EMPLOYEE_NUMBER = '21162395'
    Order by
        EMPLOYEE_NUMBER,
        EFFECTIVE_DATE,
        CLASSIFICATION_NAME,
        ELEMENT_NAME    
    """
    s_sql = s_sql.replace("%BEGIN%", year_start)
    s_sql = s_sql.replace("%END%", year_end)
    if l_debug:
        print('Run result SQL script')
        print(s_sql)

    # Execute the query
    try:
        funcoracle.oracle_sql_to_sqlite('People payroll', '000b_Table - oracle.csv', s_table_name, s_sql)
    except Exception as er:
        funcsys.ErrMessage(er)

    # BUILD PAYROLL HISTORY
    if l_debug:
        print("Build the payroll history with more people data...")
    sr_file = "X000aa_payroll_history_" + s_year
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        ph.RUN_RESULT_ID,
        ph.CLASSIFICATION_NAME,
        ph.ELEMENT_NAME,
        ph.REPORTING_NAME As PAYROLL_NAME,
        ph.EFFECTIVE_DATE,
        lo.DESCRIPTION As CAMPUS,
        og.DIVISION,
        og.FACULTY,
        og.ORG1_TYPE_DESC As ORGANIZATION_TYPE,
        og.ORG1_NAME As ORGANIZATION_NAME,
        lu.MEANING As ASS_CATEGORY,
        ph.POSITION_ID,
        po.POSITION,
        po.POSITION_NAME,
        ph.EMPLOYEE_CATEGORY As EMPLOYEE_CATEGORY_ASS,
        Case
            When ph.POSITION_ID = 0
            Then ph.EMPLOYEE_CATEGORY
            Else po.ACAD_SUPP
        End As EMPLOYEE_CATEGORY,
        ph.ASSIGNMENT_ID,
        ph.PERSON_ID,
        ph.EMPLOYEE_NUMBER,
        ph.RESULT_VALUE As PAYROLL_VALUE
        --ph.RUN_RESULT_ID,
        --ph.CLASSIFICATION_NAME,
        --ph.ELEMENT_NAME,
        --ph.REPORTING_NAME,
        --ph.EFFECTIVE_DATE,
        --ph.RESULT_VALUE,
        --ph.LOCATION_ID,
        --ph.ORGANIZATION_ID,
        --ph.EMPLOYMENT_CATEGORY,
        --ph.POSITION_ID,
        --ph.EMPLOYEE_CATEGORY,
        --ph.ASSIGNMENT_ID,
        --ph.PERSON_ID,
        --ph.EMPLOYEE_NUMBER
    From
        PAYROLL_HISTORY_%YEAR% ph Left Join
        PEOPLE.HR_LOCATIONS_ALL lo On lo.LOCATION_ID = ph.LOCATION_ID Left Join
        PEOPLE.X000_ORGANIZATION_STRUCT og On og.ORG1 = ph.ORGANIZATION_ID Left Join
        PEOPLE.X000_POSITIONS po On po.POSITION_ID = ph.POSITION_ID And
            ph.EFFECTIVE_DATE Between po.EFFECTIVE_START_DATE And EFFECTIVE_END_DATE Left Join
        HR_LOOKUPS lu On lu.LOOKUP_TYPE = 'EMP_CAT' And lu.LOOKUP_CODE = ph.EMPLOYMENT_CATEGORY
    ;"""
    s_sql = s_sql.replace("%YEAR%", s_year)
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    """*************************************************************************
    ELEMENTS
    *************************************************************************"""

    # Build the element list *******************************************
    if l_debug:
        print("Build the element list...")
    sr_file = "X000aa_element_list_" + s_year
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    SELECT
        PEE.ASSIGNMENT_ID,
        PEE.ELEMENT_ENTRY_ID,
        PEE.EFFECTIVE_START_DATE,
        PEE.EFFECTIVE_END_DATE,
        PEE.ELEMENT_LINK_ID,
        PEE.CREATOR_TYPE,
        PEE.ENTRY_TYPE,
        PEE.ELEMENT_TYPE_ID,
        PEV.ELEMENT_ENTRY_VALUE_ID,
        PEV.INPUT_VALUE_ID,
        PEV.SCREEN_ENTRY_VALUE,
        PET.ELEMENT_NAME,
        PET.REPORTING_NAME,
        PET.DESCRIPTION
    FROM
        PAY_ELEMENT_ENTRIES_F_%YEAR% PEE LEFT JOIN
        PAY_ELEMENT_ENTRY_VALUES_F_%YEAR% PEV ON PEV.ELEMENT_ENTRY_ID = PEE.ELEMENT_ENTRY_ID AND
            PEV.EFFECTIVE_START_DATE <= PEE.EFFECTIVE_START_DATE AND
            PEV.EFFECTIVE_END_DATE >= PEE.EFFECTIVE_START_DATE LEFT JOIN
        PAY_ELEMENT_TYPES_F PET ON PET.ELEMENT_TYPE_ID = PEE.ELEMENT_TYPE_ID AND
            PET.EFFECTIVE_START_DATE <= PEE.EFFECTIVE_START_DATE AND
            PET.EFFECTIVE_END_DATE >= PEE.EFFECTIVE_START_DATE
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS X000aa_element_list")
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = s_sql.replace("%YEAR%", s_year)
    if l_debug:
        print(s_sql)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)
    if funcconf.l_mess_project:
        i = funcsys.tablerowcount(so_curs, sr_file)
        funcsms.send_telegram("", "administrator", "<b>" + str(i) + "</b> Elements")

    # Extract the NWU TOTAL PACKAGE element for export *********************
    if l_debug:
        print("Extract the nwu total package element...")
    sr_file = "X001aa_element_package_" + s_year
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    SELECT
      el.ASSIGNMENT_ID,
      el.EFFECTIVE_START_DATE,
      el.INPUT_VALUE_ID,
      el.SCREEN_ENTRY_VALUE,
      el.ELEMENT_NAME,
      SUBSTR(ass.ASSIGNMENT_NUMBER,1,8) AS EMPL_NUMB
    FROM
      X000aa_element_list_%YEAR% el
      LEFT JOIN PEOPLE.PER_ALL_ASSIGNMENTS_F ass ON ass.ASSIGNMENT_ID = el.ASSIGNMENT_ID AND
        ass.EFFECTIVE_START_DATE <= Date('%TODAY%') AND
        ass.EFFECTIVE_END_DATE >= Date('%TODAY%')
    WHERE
      el.INPUT_VALUE_ID = 691 AND
      el.EFFECTIVE_START_DATE <= Date('%TODAY%') AND
      el.EFFECTIVE_END_DATE >= Date('%TODAY%')
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = s_sql.replace("%TODAY%", calc_today)
    s_sql = s_sql.replace("%YEAR%", s_year)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # Build the NWU TOTAL PACKAGE export file **************************************
    if l_debug:
        print("Build the nwu total package element export file...")
    sr_file = "X001ax_element_package_" + s_year
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    SELECT
      ep.EMPL_NUMB,
      ep.EFFECTIVE_START_DATE AS DATE,
      CAST(ep.SCREEN_ENTRY_VALUE AS REAL) AS PACKAGE
    FROM
      X001aa_element_package_%YEAR% ep
    ORDER BY
      EMPL_NUMB
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = s_sql.replace("%YEAR%", s_year)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)
    if l_export:
        # Export the data
        if l_debug:
            print("Export packages...")
        sr_filet = sr_file
        sx_path = re_path + s_year + "/"
        sx_file = "Payroll_001ax_package_"
        sx_filet = sx_file + funcdatn.get_current_month_end_file()
        s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)
        funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_file, s_head)
        funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_filet, s_head)
        funcfile.writelog("%t EXPORT DATA: "+sx_path+sx_file)

    """************************************************************************
    BALANCES
    ************************************************************************"""

    # BUILD THE PAY DEFINED BALANCES LIST
    if l_debug:
        print("Build defined balances list...")
    sr_file = "X000_PAY_DEFINED_BALANCES"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        db.DEFINED_BALANCE_ID,
        db.LEGISLATION_CODE,
        db.BALANCE_TYPE_ID,
        db.BALANCE_DIMENSION_ID,
        Max(db.OBJECT_VERSION_NUMBER) As Max_OBJECT_VERSION_NUMBER
    From
        PAY_DEFINED_BALANCES db
    Group By
        db.DEFINED_BALANCE_ID
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD THE PAY BALANCE TYPE LIST
    if l_debug:
        print("Build balance type list list...")
    sr_file = "X000_PAY_BALANCE_TYPE"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        bt.BALANCE_TYPE_ID,
        bt.BALANCE_NAME,
        bt.REPORTING_NAME,
        bt.BALANCE_CATEGORY_ID,
        bt.BALANCE_UOM,
        Max(bt.OBJECT_VERSION_NUMBER) As Max_OBJECT_VERSION_NUMBER
    From
        PAY_BALANCE_TYPES bt
    Group By
        bt.BALANCE_TYPE_ID
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # Build the balances list ******************************************************
    if l_debug:
        print("Build the balances list...")
    sr_file = "X000aa_balance_list_" + s_year
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        rb.RUN_BALANCE_ID,
        rb.ASSIGNMENT_ID,
        rb.EFFECTIVE_DATE,
        Upper(bt.BALANCE_NAME) As BALANCE_NAME,
        Upper(bt.REPORTING_NAME) As REPORTING_NAME,
        rb.BALANCE_VALUE,
        db.DEFINED_BALANCE_ID,
        bt.BALANCE_TYPE_ID
    From
        PAY_RUN_BALANCES_%YEAR% rb Left Join
        X000_PAY_DEFINED_BALANCES db On db.DEFINED_BALANCE_ID = rb.DEFINED_BALANCE_ID Left Join
        X000_PAY_BALANCE_TYPE bt On bt.BALANCE_TYPE_ID = db.BALANCE_TYPE_ID    
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = s_sql.replace("%YEAR%", s_year)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)
    if funcconf.l_mess_project:
        i = funcsys.tablerowcount(so_curs, sr_file)
        funcsms.send_telegram("", "administrator", "<b>" + str(i) + "</b> Balances")

    # Extract the NWU INCOME PER MONTH balance for export **************************
    if l_debug:
        print("Extract the nwu total income balance...")
    sr_file = "X002aa_balance_totalincome_" + s_year
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    SELECT
      bl.ASSIGNMENT_ID,
      bl.EFFECTIVE_DATE,
      bl.DEFINED_BALANCE_ID,
      bl.BALANCE_VALUE,
      bl.BALANCE_NAME,
      bl.REPORTING_NAME,
      SUBSTR(ass.ASSIGNMENT_NUMBER,1,8) AS EMPL_NUMB
    FROM
      X000aa_balance_list_%YEAR% bl
      LEFT JOIN PEOPLE.PER_ALL_ASSIGNMENTS_F ass ON ass.ASSIGNMENT_ID = bl.ASSIGNMENT_ID AND
        ass.EFFECTIVE_START_DATE <= Date('%PMONTHEND%') AND
        ass.EFFECTIVE_END_DATE >= Date('%PMONTHEND%')
    WHERE
      bl.DEFINED_BALANCE_ID = 16264 AND
      bl.EFFECTIVE_DATE = Date('%PMONTHEND%')
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = s_sql.replace("%PMONTHEND%", calc_monthend)
    s_sql = s_sql.replace("%YEAR%", s_year)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # Build the NWU TOTAL INCOME export file ***************************************
    if l_debug:
        print("Build the nwu total income balance export file...")
    sr_file = "X002ax_balance_totalincome_" + s_year
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    SELECT
      bt.EMPL_NUMB,
      bt.EFFECTIVE_DATE AS DATE,
      CAST(bt.BALANCE_VALUE AS REAL) AS INCOME
    FROM
      X002aa_balance_totalincome_%YEAR% bt
    ORDER BY
      EMPL_NUMB
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = s_sql.replace("%YEAR%", s_year)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)
    if l_export:
        # Export the data
        if l_debug:
            print("Export incomes...")
        sr_filet = sr_file
        sx_path = re_path + s_year + "/"
        sx_file = "Payroll_002ax_income_total_"
        sx_filet = sx_file + funcdatn.get_previous_month_end_file()
        s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)
        funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_file, s_head)
        funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_filet, s_head)
        funcfile.writelog("%t EXPORT DATA: "+sx_path+sx_file)

    """*************************************************************************
    SECONDARY ASSIGNMENTS
    *************************************************************************"""

    if l_debug:
        print("Build previous secondary assignments...")
    sr_file = "X000aa_sec_assignment_" + s_year
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    SELECT
      ae.ASSIGNMENT_EXTRA_INFO_ID,
      ae.ASSIGNMENT_ID,
      ae.AEI_INFORMATION1,
      ae.AEI_INFORMATION2,
      ae.AEI_INFORMATION3,
      ae.AEI_INFORMATION4,
      ae.AEI_INFORMATION5,
      ae.AEI_INFORMATION6,
      ae.AEI_INFORMATION7,
      ae.AEI_INFORMATION8,
      ae.AEI_INFORMATION9,
      ae.AEI_INFORMATION10,
      ae.AEI_INFORMATION11,
      ae.AEI_INFORMATION12 AS DATE_FROM,
      ae.AEI_INFORMATION13 AS DATE_TO,
      ae.AEI_INFORMATION14,
      ae.AEI_INFORMATION15,
      ae.AEI_INFORMATION16,
      ae.AEI_INFORMATION17,
      ae.AEI_INFORMATION18,
      ae.AEI_INFORMATION19,
      ae.AEI_INFORMATION20,
      ae.AEI_INFORMATION21,
      ae.AEI_INFORMATION22,
      ae.AEI_INFORMATION23,
      ae.AEI_INFORMATION24,
      ae.AEI_INFORMATION25,
      ae.AEI_INFORMATION26,
      ae.AEI_INFORMATION27,
      ae.AEI_INFORMATION28,
      ae.AEI_INFORMATION29,
      ae.AEI_INFORMATION30,
      ae.LAST_UPDATE_DATE,
      ae.LAST_UPDATED_BY,
      ae.LAST_UPDATE_LOGIN,
      ae.CREATED_BY,
      ae.CREATION_DATE
    FROM
      PEOPLE.PER_ASSIGNMENT_EXTRA_INFO_SEC ae
    WHERE
      DATE_FROM >= Date('%YEARS%') AND
      DATE_TO >= Date('%YEARE%')
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = s_sql.replace("%YEARS%", year_start)
    s_sql = s_sql.replace("%YEARE%", year_end)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    """*****************************************************************************
    End OF SCRIPT
    *****************************************************************************"""
    if l_debug:
        print("END OF SCRIPT")
    funcfile.writelog("END OF SCRIPT")

    # COMMIT DATA
    so_conn.commit()

    # CLOSE THE DATABASE CONNECTION
    so_conn.close()

    # CLOSE THE LOG WRITER
    funcfile.writelog("-----------------------------")
    funcfile.writelog("COMPLETED: B004_PAYROLL_LISTS")

    return


if __name__ == '__main__':
    try:
        payroll_lists('curr')
    except Exception as e:
        funcsys.ErrMessage(e, funcconf.l_mess_project, "B004_payroll_lists", "B004_payroll_lists")
