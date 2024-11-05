"""
SCRIPT TO CREATE MYSQL BACKUPS
Author: AB Janse van Rensburg (NWU:21162395)
Created: 22 Oct 2020
"""

# IMPORT OWN MODULES
from _my_modules import funcsys
from _my_modules import funcconf
from _my_modules import funcfile
from _my_modules import funcmysql
from _my_modules import funcsms

# Index of functions
"""
mysql_backup = Function to copy table data from one mysql database to another
"""


def mysql_backup(s_source_database: str = "Web_ia_nwu", s_target_database: str = "Mysql_ia_server"):
    """
    """

    # Index of steps
    """
    VARIABLES
    BEGIN OF SCRIPT
    OPEN THE DATABASES
    THE BACKUP
    END OF SCRIPT
    """

    # VARIABLES
    l_return: bool = True
    l_debug: bool = False  # Display debug messages
    l_mess: bool = funcconf.l_mess_project  # Send messages
    # l_mess: bool = False  # Send messages

    # IF SOURCE OR TARGET EMPTY RETURN FALSE AND DO NOTHING
    s_source_schema: str = ""
    if s_source_database == "Web_ia_nwu":
        s_source_schema = "Ia_nwu"
    elif s_source_database == "Web_ia_joomla":
        s_source_schema = "Ia_joomla"
    elif s_source_database == "Mysql_ia_server":
        s_source_schema = "nwuiaca"
    else:
        l_return = False

    s_target_schema: str = ""
    if s_target_database == "Mysql_ia_server":
        s_target_schema = "nwuiaca"
    else:
        l_return = False

    # RUN THE BACKUP
    if l_return:

        """****************************************************************************
        BEGIN OF SCRIPT
        ****************************************************************************"""

        # SCRIPT LOG
        funcfile.writelog("Now")
        funcfile.writelog("SCRIPT: B008_MYSQL_BACKUP")
        funcfile.writelog("-------------------------")
        if l_debug:
            print("-----------------")
            print("B008_MYSQL_BACKUP")
            print("-----------------")

        # MESSAGE
        if l_mess:
            funcsms.send_telegram("", "administrator", "<b>B008 Mysql backup</b>")

        """****************************************************************************
        OPEN THE DATABASES
        ****************************************************************************"""

        if l_debug:
            print("OPEN THE MYSQL DATABASES")
        funcfile.writelog("OPEN THE MYSQL DATABASES")

        # OPEN THE SOURCE FILE
        ms_from_connection = funcmysql.mysql_open(s_source_database)
        ms_from_cursor = ms_from_connection.cursor()
        funcfile.writelog("%t OPEN MYSQL DATABASE: " + s_source_database)

        # OPEN THE TARGET
        ms_to_connection = funcmysql.mysql_open(s_target_database)
        ms_to_cursor = ms_to_connection.cursor()
        funcfile.writelog("%t OPEN MYSQL DATABASE: " + s_target_database)

        # Obtain the table names from the schema and backup each
        if l_debug:
            print("OBTAIN TABLE NAMES")

        for row in ms_from_cursor.execute(
                "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '" +
                s_source_schema + "';"
                ).fetchall():
            
            if l_debug:
                print(row[0])

            """****************************************************************************
            THE BACKUP
            ****************************************************************************"""
            
            s_table = row[0]
    
            if l_debug:
                print("BACKUP " + s_table.upper())
            funcfile.writelog("%t BACKUP " + s_table.upper())
    
            # TEMPORARY DROP TABLE WHILE DEVELOPMENT
            """
            if l_debug:
                print("Drop table " + s_table + "...")
            ms_to_cursor.execute("DROP TABLE IF EXISTS " + s_table + ";")
            """
    
            # Obtain the source mysql table structure
            if l_debug:
                print("Build source mysql table structure for " + s_table + "...")
            s_source_struct = funcmysql.get_struct_mysql_text(ms_from_cursor, s_source_schema, s_table)
    
            # Obtain the target mysql table structure
            if l_debug:
                print("Build target mysql table structure for " + s_table + "...")
            s_target_struct = funcmysql.get_struct_mysql_text(ms_to_cursor, s_target_schema, s_table)
    
            # Obtain the mysql table column names in tuple format
            if l_debug:
                print("Build source mysql table column name tuple for " + s_table + "...")
            s_from_names = funcmysql.get_colnames_mysql_text(ms_from_cursor, s_source_schema, s_table)
    
            # Obtain the mysql table column types in list format
            if l_debug:
                print("Build source mysql table column type list for " + s_table + "...")
            a_from_types = funcmysql.get_coltypes_mysql_list(ms_from_cursor, s_source_schema, s_table)
    
            # Recreate the target table
            if s_source_struct != s_target_struct:
    
                if l_debug:
                    print("Recreate the " + s_table + " table...")
                funcfile.writelog("%t CREATE MYSQL TABLE: " + s_table)
    
                ms_to_cursor.execute(
                    "DROP TABLE IF EXISTS " +
                    s_table + ";"
                    )
    
                ms_to_cursor.execute(
                    "CREATE TABLE IF NOT EXISTS `" +
                    s_table + "` " +
                    s_source_struct +
                    " ENGINE=InnoDB DEFAULT CHARSET=utf8;"
                    )
    
            else:
    
                if l_debug:
                    print("Truncate " + s_table + "...")
                funcfile.writelog("%t TRUNCATE MYSQL TABLE: " + s_table)
                ms_to_cursor.execute(
                    "TRUNCATE `" +
                    s_target_schema + "`.`" +
                    s_table + "`;"
                    )
    
            # Loop the source data per row
            if l_debug:
                print("Insert mysql " + s_table)
            ms_from_cursor.execute("SELECT * FROM " + s_table)
            rows = ms_from_cursor.fetchall()
            i_total = 0
            i_counter = 0
            for o_records in rows:
                s_data = funcmysql.convert_mysql_mysql(o_records, a_from_types)
                # print(s_data)
                s_sql = "INSERT IGNORE INTO `" + s_table + "` " + s_from_names + " VALUES " + s_data + ";"
                ms_to_cursor.execute(s_sql)
                i_total = i_total + 1
                i_counter = i_counter + 1
                if i_counter == 10:
                    ms_to_connection.commit()
                    i_counter = 0
    
            # Close the ROW Connection
            ms_to_connection.commit()
            print("Inserted " + str(i_total) + " mysql " + s_table + "...")
            funcfile.writelog("%t BACKUP MYSQL: " + str(i_total) + " records (" + s_table + ")")
            if l_mess:
                funcsms.send_telegram("", "administrator", "<b> " + str(i_total) + "</b> " + s_table + " backup")

        """****************************************************************************
        END OF SCRIPT
        ****************************************************************************"""

        if l_debug:
            print("END OF SCRIPT")

        # CLOSE MYSQL DATABASES
        ms_from_connection.close()
        ms_to_connection.close()

        # CLOSE THE LOG WRITER
        funcfile.writelog("----------------------------")
        funcfile.writelog("COMPLETED: B008_MYSQL_BACKUP")

    return l_return


if __name__ == '__main__':
    try:
        mysql_backup()
    except Exception as e:
        funcsys.ErrMessage(e)
