"""
SCRIPT TO IMPORT IA WEB DATA FROM MYSQL TO SQLITE
Script: A004_import_ia.py
Author: Albert B Janse van Rensburg (NWU:21162395)
Created: 21 October 2022
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
s_function: str = "A004 IMPORT IA"


def ia_mysql_import(source_database: str = "Web_nwu_ia", target_database: str = f'{funcconf.drive_data_raw}Internal_audit/Web_ia_nwu_test.sqlite'):
    """
    Script to import ia web data from mysql to sqlite
    :param source_database: str: The MySQL database to import data from
    :param target_database: str: The SQLite target database
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

    # IF SOURCE OR TARGET EMPTY RETURN FALSE AND DO NOTHING
    s_source_schema: str = ""
    if source_database == "Web_nwu_ia":
        s_source_schema = "nwuiaeapciy_db1"
    elif source_database == "Web_ia_nwu":
        s_source_schema = "Ia_nwu"
    elif source_database == "Web_ia_joomla":
        s_source_schema = "Ia_joomla"
    elif source_database == "Mysql_ia_server":
        s_source_schema = "nwuiaca"
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
            # Function
            print("--------------")
            print(s_function.upper())
            print("--------------")

        # MESSAGE
        if l_mess:
            funcsms.send_telegram("", "administrator", "<b>" + s_function + "</b>")

        # SET A TABLE AND RECORD COUNTER
        table_counter: int = 0
        column_counter: int = 0
        record_counter: int = 0

        """****************************************************************************
        OPEN THE DATABASES
        ****************************************************************************"""

        if l_debug:
            print("OPEN THE SOURCE AND TARGET DATABASES")

        # OPEN THE MYSQL SOURCE DATABASE
        if l_debug:
            # Open the source database
            print("Open mysql source database...")
        ms_from_connection = funcmysql.mysql_open(source_database)
        mysql_cursor = ms_from_connection.cursor()
        funcfile.writelog("%t OPEN MYSQL SOURCE DATABASE: " + source_database)

        # OPEN SQLITE TARGET DATABASE
        if l_debug:
            # Open the target database
            print("Open sqlite target database...")
        with sqlite3.connect(target_database) as sqlite_conn:
            sqlite_cursor = sqlite_conn.cursor()
        funcfile.writelog("%t OPEN SQLITE TARGET DATABASE: " + target_database)

        # Get table names from MySQL
        if l_debug:
            print("OBTAIN TABLE NAMES")
        mysql_cursor.execute("SHOW TABLES;")
        tables = [table[0] for table in mysql_cursor.fetchall()]
        if l_debug:
            print('MySQL Tables names:')
            print(tables)

        """****************************************************************************
        DO FOR EACH TABLE
        ****************************************************************************"""

        # Copy tables from MySQL to SQLite
        for table in tables:

            if l_debug:
                print(table)

            # Skip identified tables
            if table == "ia_people":
                continue
            if table == "ia_director":
                continue
            if table == "def_town":
                continue
            if table == "def_country":
                continue
            if table[:3] == "jm4":
                continue

            # Count the number of tables
            table_counter += 1

            # Get column names and types from MySQL table
            mysql_cursor.execute(f"DESCRIBE {table};")
            mysql_columns = [column[0] for column in mysql_cursor.fetchall()]
            column_counter += len(mysql_columns)
            if l_debug:
                print('MySQL Column names:')
                print(mysql_columns)
            # Store the fetched results to avoid calling fetchall() again
            mysql_cursor.execute(f"DESCRIBE {table};")
            mysql_types = [column[1] for column in mysql_cursor.fetchall()]
            if l_debug:
                print('MySQL Column types:')
                print(mysql_types)

            # Convert MySQL types into SQLite types
            converted_mysql_types = []
            for i in range(len(mysql_types)):
                '''
                if l_debug:
                    # The first three letters of the MySQL column type
                    print(mysql_types[i][:3])
                '''
                if mysql_types[i][:3] in ['int']:
                    # Integer
                    converted_mysql_types.append('INTEGER')
                elif mysql_types[i][:3] in ['dec']:
                    # Float
                    converted_mysql_types.append('REAL')
                else:
                    converted_mysql_types.append('TEXT')
            if l_debug:
                print('SQLite column types')
                print(converted_mysql_types)

            # Build the CREATE TABLE statement for SQLite
            create_table_sql = f"CREATE TABLE {table} ("
            for i in range(len(mysql_columns)):
                column_name = mysql_columns[i]
                column_type = converted_mysql_types[i]
                create_table_sql += f"{column_name} {column_type}, "
            create_table_sql = create_table_sql[:-2] + ");"
            if l_debug:
                print('SQLite script to create the new table:')
                print(create_table_sql)

            # Create SQLite table with the same columns and types
            sqlite_cursor.execute("DROP TABLE IF EXISTS " + table)
            sqlite_cursor.execute(create_table_sql)
            sqlite_conn.commit()

            # Fetch all data from MySQL table
            mysql_cursor.execute(f"SELECT * FROM {table};")
            mysql_data = mysql_cursor.fetchall()
            record_counter += len(mysql_data)

            # Insert data into SQLite table
            for row in mysql_data:
                '''
                if l_debug:
                    print('The MySQL row to be converted and inserted:')
                    print(row)
                '''
                converted_mysql_data = []
                for i in range(len(row)):
                    '''
                    if l_debug:
                        print('The data item and data item type:')
                        print(row[i])
                        print(type(row[i]))
                    '''
                    if row[i] is None:
                        converted_mysql_data.append('')
                    elif mysql_types[i][:3] in ['int']:
                        converted_mysql_data.append(row[i])
                    elif mysql_types[i][:3] in ['dec']:
                        converted_mysql_data.append(float(row[i]))
                    else:
                        converted_mysql_data.append(str(row[i]))
                '''
                if l_debug:
                    print('The converted SQLite row to be inserted:')
                    print(tuple(converted_mysql_data))
                '''

                '''
                if l_debug:
                    print('The SQLite script to indert the new row:')
                    print(f"INSERT INTO {table} VALUES ({', '.join(['?' for _ in mysql_columns])});", tuple(converted_mysql_data))
                '''
                sqlite_cursor.execute(f"INSERT INTO {table} VALUES ({', '.join(['?' for _ in mysql_columns])});", tuple(converted_mysql_data))

        # MESSAGE
        if l_mess:
            funcsms.send_telegram("", "administrator", "<b>" + str(table_counter) + "</b> tables imported")
            funcsms.send_telegram("", "administrator", "<b>" + str(column_counter) + "</b> columns imported")
            funcsms.send_telegram("", "administrator", "<b>" + str(record_counter) + "</b> records imported")

        """************************************************************************
        END OF SCRIPT
        ************************************************************************"""
        funcfile.writelog("END OF SCRIPT")
        if l_debug:
            print("END OF SCRIPT")

    return l_return


if __name__ == '__main__':
    try:
        ia_mysql_import()
    except Exception as e:
        funcsys.ErrMessage(e)
