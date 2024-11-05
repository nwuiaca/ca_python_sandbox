"""
PAYROLL Functions
Created on: 18 Jun 2021
Author: Albert J v Rensburg (NWU21162395)
"""

# INDEX
"""
Function to build table of PAYROLL ELEMENT
"""


def payroll_element_screen_value(
        so_conn,
        s_table: str = '',
        s_element: str = '',
        s_option: str = '',
        s_date: str = ''
        ) -> int:
    """
    Function to build table of PAYROLL ELEMENT

    :param so_conn: object: Table connection object
    :param s_table: str: Table name to create
    :param s_element: str: Element name (lower case)
    :param s_option: str: Element input value (lower case)
    :param s_date: str: Element date
    :return: int: Table row count
    """

    # IMPORT SYSTEM MODULES
    # import csv
    # import sqlite3

    # OPEN OWN MODULES
    # from _my_modules import funccsv
    from _my_modules import funcconf
    from _my_modules import funcdatn
    from _my_modules import funcfile
    from _my_modules import funcsys

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
    i_return: int = 0
    l_debug: bool = False
    ed_path = f"{funcconf.drive_system}_external_data/"  # External data path
    re_path = f"{funcconf.drive_data_results}People/"  # Results path
    l_export: bool = False
    l_mail: bool = False

    # OPEN THE LOG
    if l_debug:
        print("----------------------------------------------")
        print("FUNCTION PAYROLL: PAYROLL_ELEMENT_SCREEN_VALUE")
        print("----------------------------------------------")
    funcfile.writelog("Now")
    funcfile.writelog("FUNCTION PAYROLL: PAYROLL_ELEMENT_SCREEN_VALUE")
    funcfile.writelog("----------------------------------------------")

    """*****************************************************************************
    OPEN THE DATABASES
    *****************************************************************************"""
    if l_debug:
        print("OPEN THE DATABASES")
    funcfile.writelog("OPEN THE DATABASES")

    # OPEN THE WORKING DATABASE
    so_curs = so_conn.cursor()

    """ ****************************************************************************
    BEGIN OF SCRIPT
    *****************************************************************************"""
    if l_debug:
        print("BEGIN OF SCRIPT")
    funcfile.writelog("BEGIN OF SCRIPT")

    # NO FILE NAME - TEST
    if s_table == '':
        s_table = 'A000_test'

    # NO ELEMENT NAME OR INPUT VALUE
    if s_element == '' and s_option == '':
        sr_file = s_table
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        return i_return

    # ELEMENT
    if s_element != '':
        s_element = "And Lower(petf.element_name) Like '%" + s_element + "%'"

    # OPTION
    if s_option != '':
        s_option = "And Lower(pivf.name) Like '%" + s_option + "%'"

    # NO DATE - TODAY
    if s_date == '':
        s_date = funcdatn.get_today_date()

    # BUILD THE ELEMENT LIST
    if l_debug:
        print("Obtain " + s_element + " - " + s_option)
    sr_file = s_table
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        papf.employee_number,
        --papf.full_name,
        Max(peevf.EFFECTIVE_START_DATE) EFFECTIVE_START_DATE,
        peevf.EFFECTIVE_END_DATE,
        Upper(petf.element_name) As ELEMENT_NAME,
        Upper(pivf.name) As ELEMENT_OPTION,
        Upper(peevf.screen_entry_value) As ELEMENT_VALUE
    From
        PER_ALL_PEOPLE_F papf,
        PER_ALL_ASSIGNMENTS_F paaf,
        PAY_ELEMENT_ENTRIES_F_CURR peef,        
        PAY_ELEMENT_TYPES_F petf,
        PAY_ELEMENT_ENTRY_VALUES_F_CURR peevf,
        PAY_INPUT_VALUES_F pivf
    Where
        papf.person_id = paaf.person_id And
        '%DATE%' between papf.effective_start_date And papf.effective_end_date And
        paaf.assignment_id = peef.assignment_id And
        paaf.primary_flag = 'Y' And        
        paaf.assignment_status_type_id in (1) and        
        '%DATE%' between paaf.effective_start_date And paaf.effective_end_date And
        paaf.effective_end_date between papf.effective_start_date and papf.effective_end_date and                
        peef.element_type_id = petf.element_type_id And
        '%DATE%' between peef.effective_start_date And peef.effective_end_date And        
        peevf.element_entry_id = peef.element_entry_id And
        pivf.element_type_id = petf.element_type_id And
        pivf.input_value_id = peevf.input_value_id And
        --peevf.screen_entry_value > 0 And
        '%DATE%' between petf.effective_start_date And petf.effective_end_date
        %ELEMENT%
        %OPTION%
    Group by
        papf.employee_number   
    Order By
        papf.employee_number,
        peevf.effective_start_date    
    ;"""
    s_sql = s_sql.replace("%DATE%", s_date)
    s_sql = s_sql.replace("%ELEMENT%", s_element)
    s_sql = s_sql.replace("%OPTION%", s_option)
    # print(s_sql)
    so_curs.execute(s_sql)
    so_conn.commit()
    i_return = funcsys.tablerowcount(so_curs, s_table)
    funcfile.writelog("%t BUILD TABLE: " + sr_file + ' (' + str(i_return) + ' RECORDS)')

    """ ****************************************************************************
    END OF SCRIPT
    *****************************************************************************"""
    if l_debug:
        print("END OF SCRIPT")
    funcfile.writelog("END OF SCRIPT")

    # CLOSE THE WORKING DATABASE
    # so_conn.close()

    # CLOSE THE LOG
    funcfile.writelog("---------------------------------------")
    funcfile.writelog("COMPLETED: PAYROLL_ELEMENT_SCREEN_VALUE")

    return i_return
