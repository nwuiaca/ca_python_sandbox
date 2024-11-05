"""
Script to backup directors to NWUIA Web
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
s_function: str = "A008 BACKUP DIRECTOR"


def ia_backup_director(s_source_table: str = "X004x_searchworks_directors", s_target_database: str = "Web_nwu_ia"):
    """
    Script to import ia web data from mysql to sqlite
    :param s_source_table: str: The SQLite table to import data from
    :param s_target_database: str: The MySQL database to export data to
    :return: Nothing
    """

    """*****************************************************************************
    ENVIRONMENT
    *****************************************************************************"""

    # FUNCTION WIDE VARIABLES
    l_return: bool = True
    l_debug: bool = False  # Display debug messages
    l_mess: bool = funcconf.l_mess_project  # Send messages
    # l_mess: bool = True  # Send messages
    target_table: str = "ia_director"

    # IF SOURCE OR TARGET EMPTY RETURN FALSE AND DO NOTHING
    database_name: str = ""
    if s_target_database == "Web_nwu_ia":
        database_name = "nwuiaeapciy_db1"
    elif s_target_database == "Web_ia_nwu":
        database_name: str = ""
    elif s_target_database == "Web_ia_joomla":
        database_name: str = ""
    elif s_target_database == "Mysql_ia_server":
        database_name: str = ""
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
        so_path = f"{funcconf.drive_data_raw}People_conflict/"  # Source database path W:\People_conflict\People_conflict.sqlite
        so_file = "People_conflict.sqlite"  # Source database
        with sqlite3.connect(so_path + so_file) as so_conn:
            so_curs = so_conn.cursor()
        funcfile.writelog("OPEN SQLITE PEOPLE DATABASE: " + so_file)

        # OBTAIN THE SQLITE TABLE COLUMN NAMES IN TEXT FORMAT
        s_source_struct = funcmysql.convert_struct_sqlite_mysql_text(so_curs, s_source_table)
        if l_debug:
            print(s_source_struct)
        s_source_column = funcmysql.get_colnames_sqlite_text(so_curs, s_source_table, "")
        s_source_column = "(" + s_source_column.rstrip(", ") + ")"
        if l_debug:
            print(s_source_column)
        l_source_type = funcmysql.convert_coltypes_sqlite_mysql_list(so_curs, s_source_table)
        if l_debug:
            print(l_source_type)

        # BUILD THE TARGET TABLE AFTER DELETING THE OLD TABLE
        if l_debug:
            print("Drop target table...")
        ms_to_cursor.execute(f"DROP TABLE IF EXISTS `{target_table}`;")
        ms_to_cursor.execute(f"CREATE TABLE `{target_table}` ( " + s_source_struct + " ) ENGINE = InnoDB;")
        i_table_counter = i_table_counter + 1

        # LOOP THE DATA SOURCE PER ROW
        if l_debug:
            print(f"Insert target {target_table}")
        so_curs.execute("SELECT * FROM " + s_source_table)
        rows = so_curs.fetchall()
        i_total = 0
        i_counter = 0
        for row in rows:
            if l_debug:
                print(row)
            s_data = funcmysql.convert_sqlite_mysql(row, l_source_type, 0, 0)
            if l_debug:
                print(s_data)
            s_sql = f"INSERT IGNORE INTO `{target_table}` " + s_source_column + " VALUES " + s_data + ";"
            ms_to_cursor.execute(s_sql)
            i_record_counter = i_record_counter + 1
            i_total += 1
            i_counter += 1
            if i_counter == 100:
                if l_debug:
                    print(i_record_counter)
                ms_to_connection.commit()
                i_counter = 0

        # Add table indexes
        s_sql = f"ALTER TABLE `{database_name}`.`{target_table}` ADD INDEX `index_nwu_number` (`nwu_number`(8));"
        ms_to_cursor.execute(s_sql)
        s_sql = f"ALTER TABLE `{database_name}`.`{target_table}` ADD INDEX `index_national_identifier` (`national_identifier`(13));"
        ms_to_cursor.execute(s_sql)
        s_sql = f"ALTER TABLE `{database_name}`.`{target_table}` ADD INDEX `registration_number` (`registration_number`(14));"
        ms_to_cursor.execute(s_sql)
        ms_to_connection.commit()

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
        ia_backup_director()
    except Exception as e:
        funcsys.ErrMessage(e)
