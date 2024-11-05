"""
SCRIPT TO CONVERT MYSQL TO SQLITE
Script: A001_mysql_sqlite.py
Author: Albert B Janse van Rensburg (NWU:21162395)
Created: 20 November 2022
"""

# IMPORT PYTHON MODULES
import sqlite3

# IMPORT OWN MODULES
from _my_modules import funcdatn
from _my_modules import funcsys
from _my_modules import funcconf
from _my_modules import funcfile
from _my_modules import funcmysql
from _my_modules import funcsms

# INDEX OF FUNCTIONS
"""
"""


def mysql_to_sqlite(s_source_database: str = "", s_target_location = "", s_target_database: str = ""):
    """
    Script to convert mysql to sqlite
    :param s_source_database: str: The MySQL source database
    :param s_target_location: str: The SQLite target database location
    :param s_target_database: str: The SQLite target database
    :return: Nothing
    """

    """*****************************************************************************
    ENVIRONMENT
    *****************************************************************************"""

    # FUNCTION WIDE VARIABLES
    s_function: str = "A001 MySql to SQLite"
    l_return: bool = True
    l_debug: bool = True  # Display debug messages
    # l_mess: bool = funcconf.l_mess_project  # Send messages
    l_mess: bool = True  # Send messages
    # l_mail: bool = funcconf.l_mail_project
    l_mail: bool = False
    so_path: str = s_target_location
    so_file: str = s_target_database + ".sqlite"

    # IF SOURCE OR TARGET EMPTY RETURN FALSE AND DO NOTHING
    s_source_schema: str = ""
    if s_source_database == "Web_ia_nwu":
        s_source_schema = "Ia_nwu"
    elif s_source_database == "Web_ia_joomla":
        s_source_schema = "Ia_joomla"
    elif s_source_database == "Mysql_ia_server":
        s_source_schema = "nwuiaca"
    elif s_source_database == "Web_rensburg":
        s_source_schema = "joomla"
    elif s_source_database == "Web_tax_admin":
        s_source_schema = "tax_admin"
    elif s_source_database == "Web_tax_joomla":
        s_source_schema = "tax_joomla"
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
        funcfile.writelog("--------" + "-"*len(s_function))
        if l_debug:
            print("-"*len(s_function))
            print(s_function)
            print("-"*len(s_function))

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

        # OPEN SQLITE TARGET DATABASE
        if l_debug:
            print("Open sqlite target database...")
        with sqlite3.connect(so_path + so_file) as so_conn:
            so_curs = so_conn.cursor()
        funcfile.writelog("%t OPEN SQLITE TARGET DATABASE: " + so_file)

        # OPEN THE MYSQL SOURCE DATABASE
        if l_debug:
            print("Open mysql source database...")
        ms_from_connection = funcmysql.mysql_open(s_source_database)
        ms_from_cursor = ms_from_connection.cursor()
        funcfile.writelog("%t OPEN MYSQL SOURCE DATABASE: " + s_source_database)

        # OBTAIN THE TABLE NAMES FROM THE SCHEMA
        if l_debug:
            print("OBTAIN TABLE NAMES")

        """****************************************************************************
        DO FOR EACH TABLE
        ****************************************************************************"""

        for row in ms_from_cursor.execute(
                "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '" + s_source_schema + "';"
        ).fetchall():

            if l_debug:
                print(row[0])

            s_table = row[0]

            # SKIP CERTAIN TABLES
            # ONLY GET THE RENSBURG TABLES
            # if s_table[:2] != "re":
            #     continue

            # UPDATE THE TABLE COUNTER
            i_table_counter = i_table_counter + 1

            if l_debug:
                print("IMPORT " + s_table.upper())
            funcfile.writelog("%t IMPORT " + s_table.upper())

            # OBTAIN THE SOURCE MYSQL TABLE STRUCTURE
            if l_debug:
                print("Build source mysql table structure for " + s_table + "...")
            s_source_struct = funcmysql.get_struct_mysql_text(ms_from_cursor, s_source_schema, s_table)
            # print(s_source_struct)

            # OBTAIN THE MYSQL TABLE COLUMN NAMES IN TUPLE FORMAT
            """if l_debug:
                print("Build source mysql table column name tuple for " + s_table + "...")
            s_from_names = funcmysql.get_colnames_mysql_text(ms_from_cursor, s_source_schema, s_table)
            """

            # OBTAIN THE MYSQL TABLE COLUMN TYPES IN LIST FORMAT
            if l_debug:
                print("Build source mysql table column type list for " + s_table + "...")
            a_from_types = funcmysql.get_coltypes_mysql_list(ms_from_cursor, s_source_schema, s_table)

            # BUILD THE TARGET TABLE AFTER DELETING THE OLD TABLE
            if l_debug:
                print("Build the target table...")
            sr_file = s_table
            so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
            so_curs.execute(
                "CREATE TABLE IF NOT EXISTS `" +
                s_table + "` " +
                s_source_struct +
                ";"
            )

            # LOOP THE DATA SOURCE PER ROW
            if l_debug:
                print("Insert target " + s_table)
            ms_from_cursor.execute("SELECT * FROM " + s_table)
            rows = ms_from_cursor.fetchall()
            i_total = 0
            i_counter = 0
            for o_records in rows:

                # UPDATE THE TABLE COUNTER
                i_record_counter = i_record_counter + 1

                s_data = funcmysql.convert_mysql_sqlite(o_records, a_from_types)
                # print(s_data)
                s_sql = "INSERT INTO " + s_table + " VALUES" + s_data + ";"
                so_curs.execute(s_sql)
                # print(s_sql)
                # break
                i_total = i_total + 1
                i_counter = i_counter + 1
                if i_counter == 10:
                    so_conn.commit()
                    i_counter = 0
            so_conn.commit()
        # MESSAGE
        if l_mess:
            funcsms.send_telegram("", "administrator", "<b>" + str(i_table_counter) + "</b> tables imported")
            funcsms.send_telegram("", "administrator", "<b>" + str(i_record_counter) + "</b> records imported")

        """************************************************************************
        END OF SCRIPT
        ************************************************************************"""
        funcfile.writelog("END OF SCRIPT")
        if l_debug:
            print("END OF SCRIPT")

    return l_return


if __name__ == '__main__':
    from _my_modules import funcconf
    try:
        # mysql_to_sqlite("Web_tax_admin", f"{funcconf.drive_data_raw}Rensburg/", "Web_tax_admin")
        mysql_to_sqlite("Web_tax_joomla", f"{funcconf.drive_data_raw}Rensburg/", "Web_tax_joomla")
    except Exception as e:
        funcsys.ErrMessage(e)
