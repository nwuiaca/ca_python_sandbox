"""
SCRIPT TO IMPORT IA WEB DATA FROM MYSQL TO SQLITE
Script: B009_ia_lists.py
Author: Albert B Janse van Rensburg (NWU:21162395)
Created: 21 October 2022
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
ia_mysql_import = Function to import the mysql data
"""


def rensburg_import(s_source_database: str = "Web_rensburg"):
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
    l_debug: bool = True  # Display debug messages
    # l_mess: bool = funcconf.l_mess_project  # Send messages
    l_mess: bool = True  # Send messages
    # l_mail: bool = funcconf.l_mail_project
    l_mail: bool = False
    so_path: str = f"{funcconf.drive_data_raw}Rensburg/"
    so_file: str = "Web_rensburg_2022.sqlite"

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
    else:
        l_return = False

    # RUN THE IMPORT
    if l_return:

        """****************************************************************************
        BEGIN OF SCRIPT
        ****************************************************************************"""

        # SCRIPT LOG
        funcfile.writelog("Now")
        funcfile.writelog("SCRIPT: B010_RENSBURG_IMPORT")
        funcfile.writelog("----------------------------")
        if l_debug:
            print("--------------------")
            print("B010_RENSBURG_IMPORT")
            print("--------------------")

        # MESSAGE
        if l_mess:
            funcsms.send_telegram("", "administrator", "<b>B010 Rensburg import</b>")

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
            if s_table[:2] != "re":
                continue

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
    try:
        rensburg_import()
    except Exception as e:
        funcsys.ErrMessage(e)
