"""
PEOPLE list report and export
Created: 21 June 2021
Author: Albert J van Rensburg (NWU:21162395)
"""

# IMPORT PYTHON MODULES
import sqlite3

# IMPORT OWN MODULES
from _my_modules import funcconf
from _my_modules import funccsv
from _my_modules import funcdatn
from _my_modules import funcfile
# from _my_modules import funcstat
from _my_modules import funcsys
from _my_modules import funcsms
# from _my_modules import functest
from _my_modules import funcpeople

# INDEX
"""
ENVIRONMENT
OPEN THE DATABASES
TEMPORARY AREA
BEGIN OF SCRIPT
END OF SCRIPT
"""

# SCRIPT WIDE VARIABLES
s_function: str = "Report_people_list"


def report_people_list(
        s_file_name: str = '',
        s_date: str = '',
        s_assign: str = '',
        s_category: str = '',
        s_division: str = '',
        s_faculty: str = ''
        ) -> int:
    """
    Function to list and export people.

    param: s_date: str People active on which date
    param: s_assign: str Assignment category (x)all (p)ermanent (t)emporary
    param: s_category: str Employee category (x)all (a)cademic (s)upport
    param: s_faculty: str Faculty (ec)onomic (ed)ucation (en)gineering (he)ealth (hu)manities (la)w (na)tural (th)eology
    param: s_division: str Division
    return: int: Table row count
    """

    """*****************************************************************************
    ENVIRONMENT
    *****************************************************************************"""

    # FUNCTION WIDE VARIABLES
    i_return: int = 0
    ed_path: str = f"{funcconf.drive_system}_external_data/"  # External data path
    re_path: str = f"{funcconf.drive_data_results}People/"
    so_path: str = f"{funcconf.drive_data_raw}People/"  # Source database path
    so_file: str = "People.sqlite"
    l_debug: bool = False
    l_mail: bool = funcconf.l_mail_project
    l_mail: bool = False
    l_mess: bool = funcconf.l_mess_project
    l_mess: bool = True
    l_export: bool = True

    # LOG
    funcfile.writelog("Now")
    funcfile.writelog("SCRIPT: " + s_function.upper())
    funcfile.writelog("-" * len("script: "+s_function))
    if l_debug:
        print(s_function.upper())

    # MESSAGE
    if l_mess:
        funcsms.send_telegram("", "administrator", "<b>" + s_function + "</b>")

    """************************************************************************
    OPEN THE DATABASES
    ************************************************************************"""
    funcfile.writelog("OPEN THE DATABASES")
    if l_debug:
        print("OPEN THE DATABASES")

    # OPEN SQLITE SOURCE table
    if l_debug:
        print("Open sqlite database...")
    with sqlite3.connect(so_path + so_file) as so_conn:
        so_curs = so_conn.cursor()
    funcfile.writelog("OPEN DATABASE: " + so_file)

    # ATTACH DATA SOURCES
    so_curs.execute(f"ATTACH DATABASE '{funcconf.drive_data_raw}People_payroll/People_payroll.sqlite' AS 'PAYROLL'")
    funcfile.writelog("%t ATTACH DATABASE: PEOPLE.SQLITE")

    """************************************************************************
    TEMPORARY AREA
    ************************************************************************"""
    funcfile.writelog("TEMPORARY AREA")
    if l_debug:
        print("TEMPORARY AREA")

    """************************************************************************
    BEGIN OF SCRIPT
    ************************************************************************"""
    funcfile.writelog("BEGIN OF SCRIPT")
    if l_debug:
        print("BEGIN OF SCRIPT")

    # ASK THE QUESTIONS
    if s_file_name == '':
        s_file_name = 'X000_PEOPLE_LIST'

    # PEOPLE ON WHICH DATE
    if s_date == '':
        print()
        s_date = input("People on which date? (yyyy-mm-dd) ")
        if s_date == '':
            s_date = funcdatn.get_today_date()

    # ASSIGNMENT CATEGORY
    if s_assign == '':
        print()
        s_assign = input("Assignment category? (x)all (p)ermanent (t)emporary ")

    # EMPLOYEE CATEGORY
    if s_category == '':
        print()
        s_category = input("Employee category? (x)all (a)cademic (s)upport ")

    # DIVISION
    if s_division == '':
        print()
        s_division = input("Division? ")

    # FACULTY
    if s_faculty == '':
        print()
        print('Faculty  (ec)onomic management sciences')
        print('         (ed)ucation')
        print('         (en)gineering')
        print('         (he)alth sciences')
        print('         (hu)manities')
        print('         (la)w')
        print('         (na)tural agricultural sciences')
        print('         (th)eology')
        s_faculty = input("Faculty? ")

    # DISPLAY THE INPUT
    if l_debug:
        print('')
        print('VALUES')
        print('          File name: ', s_file_name)
        print('               Date: ', s_date)
        print('Assignment category: ', s_assign)
        print('  Employee category: ', s_category)
        print('           Division: ', s_division)
        print('            Faculty: ', s_faculty)

    # BUILD THE SELECTED VALUE
    s_selected: str = ''
    if s_assign == 'p':
        s_selected += " and p.assignment_category like('PERMANENT')"
    if s_assign == 't':
        s_selected += " and p.assignment_category like('TEMPORARY')"
    if s_category == 'a':
        s_selected += " and p.employee_category like('ACADEMIC')"
    if s_category == 's':
        s_selected += " and p.employee_category like('SUPPORT')"
    if s_division != '':
        s_selected += " and p.division like('%" + s_division.upper() + "%')"
    if s_faculty != '':
        s_selected += " and p.faculty like('%" + s_faculty.upper() + "%')"

    # BUILD LIST OF DATED PEOPLE
    print('')
    print('Build dated list of people...')
    i_count = funcpeople.people_detail_list(so_conn, s_file_name, s_date)
    if l_debug:
        print(i_count, ' records')
        print('')

    print("Build selected list of people...")
    sr_file = s_file_name + '_SELECTED'
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        p.*
    From
        %FILE% p
    Where
        p.employee_number Is Not Null
        %SELECTION%        
    ;"""
    s_sql = s_sql.replace("%FILE%", s_file_name)
    s_sql = s_sql.replace("%SELECTION%", s_selected)
    # print(s_sql)
    so_curs.execute(s_sql)
    so_conn.commit()
    i_return = funcsys.tablerowcount(so_curs, sr_file)
    funcfile.writelog("%t BUILD TABLE: " + sr_file + ' (' + str(i_return) + ' RECORDS)')

    if l_export:
        # EXPORT TABLE
        sr_file = s_file_name + '_SELECTED'
        sx_path = re_path + funcdatn.get_current_year() + "/"
        sx_file = "People_000_all_selected_"
        sx_file_dated = sx_file + s_date.replace('-', '') + '_' + funcdatn.get_today_date_file()
        if l_debug:
            print("Export selected people..." + sx_path + sx_file)
        # Read the header data
        s_head = funccsv.get_colnames_sqlite(so_conn, sr_file)
        # Write the data
        # funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file, s_head)
        # funcfile.writelog("%t EXPORT DATA: " + sx_path + sx_file)
        # Write the data dated
        funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file_dated, s_head)
        funcfile.writelog("%t EXPORT DATA: " + sx_path + sx_file_dated)

    # MESSAGE
    if l_mess:
        funcsms.send_telegram("", "administrator", " " + str(i_return) + " records selected")

    """************************************************************************
    END OF SCRIPT
    ************************************************************************"""
    funcfile.writelog("END OF SCRIPT")
    if l_debug:
        print("END OF SCRIPT")

    # CLOSE THE DATABASE CONNECTION
    so_conn.commit()
    so_conn.close()

    # CLOSE THE LOG WRITER
    funcfile.writelog("-" * len("completed: "+s_function))
    funcfile.writelog("COMPLETED: " + s_function.upper())

    return i_return


if __name__ == '__main__':
    try:
        report_people_list()
    except Exception as e:
        funcsys.ErrMessage(e)
