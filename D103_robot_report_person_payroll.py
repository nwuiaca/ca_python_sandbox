"""
PREPARE AND SEND PEOPLE REPORTS VIA EMAIL FROM THE NWUIACA SYSTEM
PERSON (EMPLOYEE) PAYROLL REPORT
Script: D103_robot_report_person_payroll.py
Author: Albert B Janse van Rensburg (NWU:21162395)
Created: 3 April 2023
"""

# IMPORT OWN MODULES
from _my_modules import funcconf
from _my_modules import funcsys

# INDEX
"""
ENVIRONMENT
OPEN THE DATABASES
BEGIN OF SCRIPT
BUILD THE PAYROLL REPORT
END OF SCRIPT
"""

# VARIABLES
s_function: str = "D103_robot_report_person_payroll"


def robot_report_person_payroll(s_nwu: str = "", s_name: str = "", s_mail: str = ""):
    """
    REPORT EMPLOYEE PERSON PAYROLL

    :param s_nwu: NWU Number
    :param s_name: The name of the requester / recipient
    :param s_mail: The requester mail address
    :return: str: Info in message format
    """

    # IMPORT PYTHON MODULES
    import sqlite3
    from datetime import datetime

    # IMPORT OWN MODULES
    from _my_modules import funccsv
    from _my_modules import funcdatn
    from _my_modules import funcfile
    from _my_modules import funcmail
    from _my_modules import funcsms
    from _my_modules import funcstat
    from _my_modules import funcoracle

    # DECLARE VARIABLES
    l_debug: bool = False

    """*************************************************************************
    ENVIRONMENT
    *************************************************************************"""
    if l_debug:
        print("ENVIRONMENT")

    # DECLARE VARIABLES
    s_description: str = "Payroll report"
    so_path: str = f"{funcconf.drive_data_raw}People_payroll/"  # Source database path
    so_file: str = "People_payroll.sqlite"  # Source database
    re_path: str = f"{funcconf.drive_data_results}People/" + funcdatn.get_current_year() + "/"  # Results
    l_mess: bool = funcconf.l_mess_project
    l_mess: bool = False
    l_mailed: bool = False

    # LOG
    if l_debug:
        print(s_function.upper())
    funcfile.writelog("Now")
    funcfile.writelog("SCRIPT: " + s_function.upper())
    funcfile.writelog("-" * len("script: "+s_function))
    funcfile.writelog("%t " + s_description + " for " + s_nwu + " requested by " + s_name)

    # MESSAGE
    if l_mess:
        funcsms.send_telegram("", "administrator", "<b>" + s_function.upper() + "</b>")

    """*****************************************************************************
    OPEN THE DATABASES
    *****************************************************************************"""
    funcfile.writelog("OPEN THE DATABASES")
    if l_debug:
        print("OPEN THE DATABASES")

    # OPEN THE WORKING DATABASE
    with sqlite3.connect(so_path+so_file) as so_conn:
        so_curs = so_conn.cursor()
    funcfile.writelog("OPEN DATABASE: " + so_file)

    # ATTACH DATA SOURCES
    so_curs.execute(f"ATTACH DATABASE '{funcconf.drive_data_raw}People/People.sqlite' AS 'PEOPLE'")
    funcfile.writelog("%t ATTACH DATABASE: PEOPLE.SQLITE")

    # OBTAIN THE NAME OF THE PERSON
    s_lookup_name = funcfile.get_field_value(so_curs,
                                             "PEOPLE.X000_PEOPLE",
                                             "name_address||' ('||preferred_name||')' ",
                                             "employee_number = '" + s_nwu + "'")
    if l_debug:
        print("FIELD LOOKUP: " + s_lookup_name)
    print(s_description + " for " + s_nwu + " requested by " + s_name)

    s_message: str = s_description + " for <b>" + s_lookup_name + '(' + s_nwu + ")</b>"

    """*************************************************************************
    PAYROLL RUN VALUES
    *************************************************************************"""

    # For which year
    year_start: str = '2000-01-01'
    year_end: str = funcdatn.get_current_year() + '-12-31'
    calc_today: str = year_end
    calc_month_end: str = year_end
    s_table_name: str = 'Payroll history report'

    funcfile.writelog("%t ORACLE TABLE: " + s_table_name)
    if l_debug:
        print("Obtain the raw payroll data from Oracle...")

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
        Trunc(paaf.EFFECTIVE_END_DATE) <= papf.EFFECTIVE_END_DATE And
        papf.EMPLOYEE_NUMBER = '%EMP%'
    Order by
        EMPLOYEE_NUMBER,
        EFFECTIVE_DATE,
        CLASSIFICATION_NAME,
        ELEMENT_NAME    
    """
    s_sql = s_sql.replace("%BEGIN%", year_start)
    s_sql = s_sql.replace("%END%", year_end)
    s_sql = s_sql.replace("%EMP%", s_nwu)
    # if l_debug:
        # print('Run result SQL script')
        # print(s_sql)

    # Execute the query
    try:
        funcoracle.oracle_sql_to_sqlite('People payroll', '000b_Table - oracle.csv', s_table_name, s_sql)
        funcfile.writelog("%t BUILD ORACLE TABLE: " + s_table_name)
    except Exception as er:
        funcsys.ErrMessage(er)

    # BUILD PAYROLL HISTORY
    if l_debug:
        print("Build the payroll history with more people data...")
    sr_file = "X000aa_payroll_history_report"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        ph.EMPLOYEE_NUMBER,
        ph.CLASSIFICATION_NAME,
        ph.ELEMENT_NAME,
        ph.REPORTING_NAME As PAYROLL_NAME,
        ph.EFFECTIVE_DATE,
        ph.RESULT_VALUE As PAYROLL_VALUE,
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
        ph.RUN_RESULT_ID,
        ph.ASSIGNMENT_ID,
        ph.PERSON_ID
    From
        PAYROLL_HISTORY_REPORT ph Left Join
        PEOPLE.HR_LOCATIONS_ALL lo On lo.LOCATION_ID = ph.LOCATION_ID Left Join
        PEOPLE.X000_ORGANIZATION_STRUCT og On og.ORG1 = ph.ORGANIZATION_ID Left Join
        PEOPLE.X000_POSITIONS po On po.POSITION_ID = ph.POSITION_ID And
            ph.EFFECTIVE_DATE Between po.EFFECTIVE_START_DATE And EFFECTIVE_END_DATE Left Join
        HR_LOOKUPS lu On lu.LOOKUP_TYPE = 'EMP_CAT' And lu.LOOKUP_CODE = ph.EMPLOYMENT_CATEGORY
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # RECORDS FOUND
    i_count: int = funcsys.tablerowcount(so_curs, sr_file)
    if i_count > 0:

        # BUILD THE MESSAGE
        s_message += '\n\n'
        s_message += 'Records: ' + str(i_count)

        # EXPORT RECORDS
        if l_debug:
            print("Export report...")
        sx_path = re_path
        sx_file = sr_file + '_' + s_nwu + "_"
        sx_file_dated = sx_file + funcdatn.get_today_date_file()
        s_head = funccsv.get_colnames_sqlite(so_conn, sr_file)
        # funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file, s_head)
        funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file_dated, s_head)
        funcfile.writelog("%t EXPORT DATA: " + sx_path + sx_file_dated)

        # MAIL THE REPORT
        s_report = "All payroll elements included!"
        if s_name != "" and s_mail != "":
            l_mailed = True
            funcfile.writelog("%t Payroll report mailed to " + s_mail)
            if l_debug:
                print("Send the report...")
            s_body: str = "Attached please find payroll report for " + s_lookup_name + " (" + s_nwu + ")."
            s_body += "\n\r"
            s_body += s_report
            funcmail.send(s_name,
                          s_mail,
                          "E",
                          s_description + " for " + s_nwu,
                          s_body,
                          re_path,
                          sx_file_dated + ".csv")

        # DELETE THE MAILED FILE
        if funcfile.file_delete(sx_path, sx_file_dated + '.csv'):
            funcfile.writelog("%t Payroll temporary file deleted")
            if l_debug:
                print("Payroll temporary file deleted...")

    else:
        s_message += "\n\n"
        s_message += "No payroll records found."
        if l_debug:
            print('No records found...')

    # POPULATE THE RETURN MESSAGE
    if l_mailed:
        s_message += "\n\n"
        s_message += "Report was mailed to " + s_mail

    s_return_message = s_message

    """*****************************************************************************
    END OF SCRIPT
    *****************************************************************************"""
    funcfile.writelog("END OF SCRIPT")
    if l_debug:
        print("END OF SCRIPT")

    # CLOSE THE LOG WRITER
    funcfile.writelog("-" * len("completed: "+s_function))
    funcfile.writelog("COMPLETED: " + s_function.upper())

    return s_return_message[0:4096]


if __name__ == '__main__':
    try:
        s_return = robot_report_person_payroll("21162395", "Albert", "21162395@nwu.ac.za")
        print("RETURN: " + s_return)
        print("LENGTH: " + str(len(s_return)))
    except Exception as e:
        funcsys.ErrMessage(e, funcconf.l_mess_project, s_function, s_function)
