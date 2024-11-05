"""
SCRIPT TO BACKUP PEOPLE TO NWUIA WEB DATA
Script: A007_backup_people.py
Author: Albert B Janse van Rensburg (NWU:21162395)
Created: 30 May 2023
"""

# IMPORT PYTHON MODULES
import sqlite3

# IMPORT OWN MODULES
from _my_modules import funcconf
from _my_modules import funcdatn
from _my_modules import funcsys
from _my_modules import funcconf
from _my_modules import funcfile
from _my_modules import funcmysql
from _my_modules import funcsms

# INDEX OF FUNCTIONS
"""
ia_mysql_import = Function to import the mysql data
"""

# SCRIPT WIDE VARIABLES
s_function: str = "A007 BACKUP PEOPLE"


def ia_mysql_backup_people(s_source_database: str = "Web_ia_nwu", s_target_database: str = "Web_nwu_ia"):
    """
    Script to import ia web data from mysql to sqlite
    :param s_source_database: str: The MySQL database to import data from
    :return: Nothing
    """

    """*****************************************************************************
    ENVIRONMENT
    *****************************************************************************"""

    # FUNCTION WIDE VARIABLES
    l_return: bool = True
    l_debug: bool = False  # Display debug messages
    # l_mess: bool = funcconf.l_mess_project  # Send messages
    l_mess: bool = True  # Send messages
    # l_mail: bool = funcconf.l_mail_project
    l_mail: bool = False
    # so_path: str = f"{funcconf.drive_data_raw}Internal_audit/"
    # so_file: str = "Web_ia_nwu.sqlite"
    target_table: str = "ia_people"

    # IF SOURCE OR TARGET EMPTY RETURN FALSE AND DO NOTHING

    s_target_schema: str = ""
    if s_target_database == "Web_nwu_ia":
        s_target_schema = "nwuiaeapciy_db1"
    elif s_target_database == "Web_ia_nwu":
        s_target_schema = "Ia_nwu"
    elif s_target_database == "Web_ia_joomla":
        s_target_schema = "Ia_joomla"
    elif s_target_database == "Mysql_ia_server":
        s_target_schema = "nwuiaca"
    else:
        l_return = False

    # RUN THE IMPORT
    if l_return:

        """****************************************************************************
        BEGIN OF SCRIPT
        ****************************************************************************"""

        # SCRIPT LOG
        funcfile.writelog("Now")
        funcfile.writelog("SCRIPT: " + s_function.upper())
        funcfile.writelog("----------------------")
        if l_debug:
            print("--------------")
            print(s_function.upper())
            print("--------------")

        # MESSAGE
        if l_mess:
            funcsms.send_telegram("", "administrator", "<b>" + s_function + "</b>")

        # SET A TABLE AND RECORD COUNTER
        i_table_counter: int = 0
        i_record_counter: int = 0

        """****************************************************************************
        OPEN THE DATABASES
        ****************************************************************************"""

        if l_debug:
            print("OPEN THE SOURCE AND TARGET DATABASES")

        # OPEN THE MYSQL TARGET DATABASE
        if l_debug:
            print("Open mysql target database...")
        ms_to_connection = funcmysql.mysql_open(s_target_database)
        ms_to_cursor = ms_to_connection.cursor()
        funcfile.writelog("%t OPEN MYSQL TARGET DATABASE: " + s_target_database)

        # OPEN THE SOURCE PEOPLE DATABASE
        if l_debug:
            print("OPEN THE PEOPLE DATABASES")
        so_path = f"{funcconf.drive_data_raw}People/"  # Source database path
        so_file = "People.sqlite"  # Source database
        with sqlite3.connect(so_path + so_file) as so_conn:
            so_curs = so_conn.cursor()
        funcfile.writelog("OPEN SQLITE PEOPLE DATABASE: " + so_file)

        # OBTAIN THE SQLITE TABLE COLUMN NAMES IN TEXT FORMAT
        s_source_struct = funcmysql.convert_struct_sqlite_mysql_text(so_curs, "X000_PEOPLE")
        if l_debug:
            print(s_source_struct)
        s_source_column = funcmysql.get_colnames_sqlite_text(so_curs, "X000_PEOPLE", "")
        s_source_column = "(" + s_source_column.rstrip(", ") + ")"
        if l_debug:
            print(s_source_column)
        l_source_type = funcmysql.convert_coltypes_sqlite_mysql_list(so_curs, "X000_PEOPLE")
        if l_debug:
            print(l_source_type)

        # BUILD THE TARGET TABLE AFTER DELETING THE OLD TABLE
        if l_debug:
            print("Drop target table...")
        ms_to_cursor.execute(f"DROP TABLE IF EXISTS `{target_table}`;")
        ms_to_cursor.execute(f'CREATE TABLE `{target_table}` ( ' + s_source_struct + ' ) ENGINE = InnoDB;')
        i_table_counter = i_table_counter + 1

        # LOOP THE DATA SOURCE PER ROW
        if l_debug:
            print(f"Insert target {target_table}")
        so_curs.execute("SELECT * FROM X000_PEOPLE")
        rows = so_curs.fetchall()
        i_tota = 0
        i_coun = 0
        for row in rows:
            if l_debug:
                print(row)
            s_data = funcmysql.convert_sqlite_mysql(row, l_source_type, 0, 0)
            if l_debug:
                print(s_data)
            s_sql = f"INSERT IGNORE INTO `{target_table}` " + s_source_column + " VALUES " + s_data + ";"
            ms_to_cursor.execute(s_sql)
            i_record_counter = i_record_counter + 1
            i_tota = i_tota + 1
            i_coun = i_coun + 1
            if i_coun == 100:
                if l_debug:
                    print(i_record_counter)
                ms_to_connection.commit()
                i_coun = 0

        # Add table indexes
        '''
        ms_to_connection.commit()
        s_sql = f"ALTER TABLE `{s_target_database}`.`{target_table}` ADD INDEX `index_employee_number` (`employee_number`(8));"
        ms_to_cursor.execute(s_sql)
        s_sql = f"ALTER TABLE `{s_target_database}`.`{target_table}` ADD INDEX `index_name_list` (`name_list`(30));"
        ms_to_cursor.execute(s_sql)
        '''

        # MESSAGE
        if l_mess:
            funcsms.send_telegram("", "administrator", "<b>" + str(i_table_counter) + "</b> tables backup")
            funcsms.send_telegram("", "administrator", "<b>" + str(i_record_counter) + "</b> records backup")

        """************************************************************************
        END OF SCRIPT
        ************************************************************************"""
        funcfile.writelog("END OF SCRIPT")
        if l_debug:
            print("END OF SCRIPT")

    return l_return


if __name__ == '__main__':
    try:
        ia_mysql_backup_people()
    except Exception as e:
        funcsys.ErrMessage(e)
