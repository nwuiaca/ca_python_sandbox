"""
SCRIPT TO BACKUP IA WEB DATA TO NWUIA WEB DATA
Script: A006_backup_ia.py
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
s_function: str = "A006 BACKUP IA"


def ia_mysql_backup(s_source_database: str = "Web_ia_nwu", s_target_database: str = "Web_nwu_ia"):
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

    # IF SOURCE OR TARGET EMPTY RETURN FALSE AND DO NOTHING

    s_source_schema: str = ""
    if s_source_database == "Web_nwu_ia":
        s_source_schema = "nwuiaeapciy_db1"
    elif s_source_database == "Web_ia_nwu":
        s_source_schema = "Ia_nwu"
    elif s_source_database == "Web_ia_joomla":
        s_source_schema = "Ia_joomla"
    elif s_source_database == "Mysql_ia_server":
        s_source_schema = "nwuiaca"
    else:
        l_return = False

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

            # if l_debug:
                # print(row[0])

            s_table = row[0]

            # SKIP CERTAIN TABLES
            if s_table == "ia_assignment_test":
                continue
            if s_table == "ia_finding_attach":
                continue
            if s_table == "ia_finding_5":
                continue
            if s_table == "ia_finding_6":
                continue
            if s_table == "ia_people":
                continue
            if s_table == "ia_people_struct":
                continue

            # UPDATE THE TABLE COUNTER
            i_table_counter = i_table_counter + 1

            if l_debug:
                print("BACKUP " + s_table.upper())
            funcfile.writelog("%t BACKUP " + s_table.upper())

            # OBTAIN THE SOURCE MYSQL TABLE STRUCTURE
            if l_debug:
                print("Build source mysql table structure for " + s_table + "...")
            s_source_struct = funcmysql.get_struct_mysql_text(ms_from_cursor, s_source_schema, s_table)

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
                print("Truncate target table...")
            sr_file = s_table
            ms_to_cursor.execute("TRUNCATE TABLE " + sr_file + ";")
            """
            so_curs.execute(
                "CREATE TABLE IF NOT EXISTS `" +
                s_table + "` " +
                s_source_struct +
                ";"
            )
            """

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

                s_data = funcmysql.convert_mysql_mysql(o_records, a_from_types)
                # if l_debug:
                    # print(s_data)
                s_sql = "INSERT INTO " + s_table + " VALUES" + s_data + ";"
                ms_to_cursor.execute(s_sql)
                # print(s_sql)
                # break
                i_total = i_total + 1
                i_counter = i_counter + 1
                if i_counter == 10:
                    ms_to_connection.commit()
                    i_counter = 0
            ms_to_connection.commit()

        # MESSAGE
        if l_mess:
            funcsms.send_telegram("", "administrator", "<b>" + str(i_table_counter) + "</b> tables backup")
            funcsms.send_telegram("", "administrator", "<b>" + str(i_record_counter) + "</b> records backup")

        # UPDATE SOME FIELD VALUES NEEDED IN NEW DATABASE
        # Update the assignments
        s_sql = "UPDATE ia_assignment SET ia_assi_formedit=13,ia_assi_formdelete=14,ia_assi_formview=23;"
        ms_to_cursor.execute(s_sql)
        s_sql = "UPDATE ia_finding SET ia_find_formedit=15,ia_find_formdelete=16,ia_find_formview=31;"
        ms_to_cursor.execute(s_sql)
        s_sql = "UPDATE ia_finding_remediation SET ia_findreme_formedit=21,ia_findreme_formdelete=5,ia_findreme_formview=33,ia_findreme_formclient=22,ia_findreme_formtransfer=23;"
        ms_to_cursor.execute(s_sql)
        ms_to_connection.commit()
        ms_to_connection.close()
        ms_from_connection.close()


        """************************************************************************
        END OF SCRIPT
        ************************************************************************"""
        funcfile.writelog("END OF SCRIPT")
        if l_debug:
            print("END OF SCRIPT")

    return l_return


if __name__ == '__main__':
    try:
        ia_mysql_backup()
    except Exception as e:
        funcsys.ErrMessage(e)
