""" ORACLE TO SQLITE
Script to read data from an Oracle database and put the data received in an SQLite database.
Created by: Albert J van Rensburg (NWU:21162395)
Created on: 2024-05-17
"""

""" INDEX
ENVIRONMENT
DETERMINE THE ORACLE DATABASE TO BE OPENED
OPEN THE RELATED SQLITE AND ORACLE DATABASE
FETCH THE ORACLE AND SQLITE TABLE NAMES
FETCH THE COLUMN NAMES AND TYPES
BUILD THE ORACLE SQL QUERY
BUILD THE SQLITE TABLE
QUERY AND FETCH DATA FROM ORACLE
SAVE THE FETCHED DATA IN SQLITE
"""


def oracle_to_sqlite_new(
        table_file: str = "000b_Table - temp.csv",
        database_file: str = "000a_Database.csv",
        column_file: str = "000c_Column_new.csv"):
    """
    Script to import data from oracle.
    :param table_file: The file holding the tables list
    :param database_file: The file holding the database list
    :param column_file: The file holding the columns list
    :return: l_return
    """

    """************************************************************************
    ENVIRONMENT
    ************************************************************************"""

    # Import python modules
    import csv
    import oracledb
    import sqlite3
    import re
    from datetime import datetime

    # Import own modules
    from _my_modules import funcfile
    from _my_modules import funcconf
    from _my_modules import funcdatn
    from _my_modules import funcsms
    from _my_modules import funcsys
    from _my_modules import funcstr
    from _my_modules import funcsys

    # Declare variables
    l_return: bool = True
    l_debug: bool = False
    l_mess: bool = funcconf.l_mess_project
    # l_mess = False
    script_name: str = "A001_oracle_to_sqlite_new"
    script_path: str = funcconf.drive_system

    # Open the log
    s_message = f"SCRIPT: {script_name}"
    if l_debug:
        print(f"{'-' * len(s_message)}\n{s_message}\n{'-' * len(s_message)}")
    funcfile.writelog("Now")
    funcfile.writelog(f"{s_message}\n{'-' * len(s_message)}")

    """************************************************************************
    DETERMINE THE ORACLE DATABASE TO BE OPENED
    ************************************************************************"""

    # Build a unique SQLite table list
    table_db = open(script_path + table_file, newline=None)
    table_db_reader = csv.reader(table_db)
    sqlite_database_to_build_list = []
    for row in table_db_reader:
        if row[1] == "DATABASE":
            continue
        elif row[7] == "X":
            continue
        elif row[1] in sqlite_database_to_build_list:
            continue
        elif row[7] == "":
            sqlite_database_to_build_list.append(row[1])
        elif row[7] == funcdatn.get_today_name():
            sqlite_database_to_build_list.append(row[1])
        elif row[7] == funcdatn.get_today_day_strip():
            sqlite_database_to_build_list.append(row[1])
    if not sqlite_database_to_build_list:
        table_db.close()
        l_return = False
        return l_return
    if l_debug:
        print(sqlite_database_to_build_list)
    table_db.close()

    message_inserted = ""
    if l_mess:
        funcsms.send_telegram('', 'administrator', f'<b>A001 Oracle to sqlite new</b>')

    """************************************************************************
    OPEN THE RELATED SQLITE AND ORACLE DATABASE
    ************************************************************************"""

    # Do for each SQLite table identified - Get the Oracle opening parameters
    for sqlite in sqlite_database_to_build_list:
        if l_debug:
            print(sqlite)

        # Find the SQLite table in the database list
        database_db = open(script_path + database_file, newline=None)
        database_db_reader = csv.reader(database_db)
        for oracle in database_db_reader:
            if oracle[4] != sqlite:
                continue
            else:
                # Open the identified Oracle database
                if l_debug:
                    print("--------")
                    print("DATABASE: " + oracle[0])
                    print("--------")
                oracle_source_name = oracle[0]  # Oracle database description
                oracle_source_dsn = oracle[1]  # Oracle database name
                oracle_source_usr = oracle[2]
                oracle_source_pwd = oracle[3]
                oracle_source_dnq = oracle[8]
                sqlite_destination_file = oracle[4]  # Sqlite destination database name
                sqlite_destination_path = oracle[5]  # Sqlite destination path
                funcfile.writelog(f"%t OPEN ORACLE DATABASE: {oracle_source_dsn}")
                funcfile.writelog(f"%t CREATE SQLITE DATABASE: {sqlite_destination_path}{sqlite_destination_file}")

        # MESSAGE TO ADMIN
        if l_mess:
            funcsms.send_telegram('', 'administrator', f'{oracle_source_name}: {sqlite_destination_file}')

        if not oracle:
            database_db.close()
            l_return = False
            return l_return

        # Close the database db
        database_db.close()
        
        # Open the relevant SQLite database
        sql_connection = sqlite3.connect(sqlite_destination_path + sqlite_destination_file)
        sql_cursor = sql_connection.cursor()

        # Open the relevant Oracle database
        # Establish the database connection
        oracle_connection = oracledb.connect(user=oracle_source_usr, password=oracle_source_pwd, dsn=oracle_source_dnq)
        oracle_cursor = oracle_connection.cursor()

        """************************************************************************
        FETCH THE ORACLE AND SQLITE TABLE NAMES
        ************************************************************************"""

        # Get the oracle table name to be fetched
        table_db = open(script_path + table_file, newline=None)
        table_db_reader = csv.reader(table_db)
        for table in table_db_reader:
            l_run_data = False
            if table[1] == "DATABASE":
                continue
            elif table[7] == "X":
                continue
            elif table[7] == "":
                l_run_data = True
            elif table[7] == funcdatn.get_today_name():
                l_run_data = True
            elif table[7] == funcdatn.get_today_day_strip():
                l_run_data = True
            else:
                l_run_data = False

            # Fetch the data
            if l_run_data and sqlite_destination_file == table[1]:

                # Build the Oracle SQL string
                if l_debug:
                    print("Oracle schema and table:")
                    if table[2] != "":
                        print(table[2]+'.'+table[3])
                    else:
                        print(table[3])
                table_description = table[0]  # Table description
                sqlite_database_name = table[1]  # Sqlite table name
                oracle_table_schema = table[2]  # Oracle table schema
                oracle_table_name = table[3]  # Oracle table name
                oracle_where_clause = table[4]  # Oracle SQL where clause
                oracle_table_order = table[5]  # Oracle SQL sort clause
                sqlite_table_alt_name = table[6]  # Oracle table alternative name
                table_run_schedule = table[7]  # Table schedule
                if sqlite_table_alt_name != "":
                    message_inserted += sqlite_table_alt_name.lower()
                    funcfile.writelog(f"%t CREATE TABLE: {oracle_table_schema}.{oracle_table_name} alias {sqlite_table_alt_name}")
                else:
                    message_inserted += oracle_table_name.lower()
                    funcfile.writelog(f"%t CREATE TABLE: {oracle_table_schema}.{oracle_table_name}")

                """************************************************************************
                FETCH THE COLUMN NAMES AND TYPES
                ************************************************************************"""

                # Get the column name from the columns database
                column_db = open(script_path + column_file, newline=None)
                column_db_reader = csv.reader(column_db)
                oracle_column_list = ""
                oracle_database_list = []
                oracle_column_type_list = []
                sqlite_database_list = []
                sqlite_database_list_add = []
                for column in column_db_reader:
                    if column[0] == "DESC":
                        continue
                    elif oracle_source_dsn == column[1] and oracle_table_name == column[2]:
                        # These columns belong to the table

                        # Build the sqlite column list
                        column_name = column[4]  # Column name
                        column_alternate_name = column[8]  # Column alternate name
                        column_type = column[5]  # Column type
                        column_length = column[6]  # Column width
                        column_decimal = column[7]  # Column decimals

                        # Build the list of oracle column names
                        oracle_database_list.append(column_name)

                        # Convert the Oracle column types to SQLite column types
                        column_type_new = ""
                        if column_type == "REAL":
                            column_type_new = "REAL"
                        elif column_type == "NUMBER" and column_decimal != "0":
                            column_type_new = "REAL"
                        elif column_type == "BINARY_FLOAT":
                            column_type_new = "REAL"
                        elif column_type == "BINARY_DOUBLE":
                            column_type_new = "REAL"
                        elif column_type == "FLOAT":
                            column_type_new = "REAL"
                        elif column_type == "NUMBER":
                            column_type_new = "INTEGER"
                        elif column_type == "INTEGER":
                            column_type_new = "INTEGER"
                        elif column_type == "PLS_INTEGER":
                            column_type_new = "INTEGER"
                        elif column_type == "BINARY_INTEGER":
                            column_type_new = "INTEGER"
                        elif column_type == "BLOB":
                            column_type_new = "BLOB"
                        elif column_type == "RAW":
                            column_type_new = "BLOB"
                        elif column_type == "LONG RAW":
                            column_type_new = "BLOB"
                        else:
                            column_type_new = "TEXT"

                        # Identify start date columns for later processing
                        start_date_list = ['STARTDATE', 'STARTDATETIME', 'KSTARTDATETIME', 'DATE_FROM', 'START_DATE_ACTIVE']
                        end_date_list = ['ENDDATE', 'ENDDATETIME', 'DATE_TO', 'END_DATE_ACTIVE']
                        if column_name in start_date_list:
                            date_indicator = "B"
                        elif column_name in end_date_list:
                            date_indicator = "E"
                        else:
                            date_indicator = ""

                        # Build the Oracle column type list
                        oracle_column_type_list.append(column_type)

                        # Replace column name with alternate column name
                        if column_alternate_name != "":
                            if "%CYEAR%" in column_alternate_name:
                                column_alternate_name = column_alternate_name.replace("%CYEAR%", funcdatn.get_current_year())
                            if "%CMONTH%" in column_alternate_name:
                                column_alternate_name = column_alternate_name.replace("%CMONTH%", funcdatn.get_current_month())
                            if "%PYEAR%" in column_alternate_name:
                                column_alternate_name = column_alternate_name.replace("%PYEAR%", funcdatn.get_previous_year())
                            if "%PMONTH%" in column_alternate_name:
                                column_alternate_name = column_alternate_name.replace("%PMONTH%", funcdatn.get_previous_month())
                            sqlite_database_list.append((column_alternate_name, column_type_new))
                            sqlite_database_list_add.append(date_indicator)
                        else:
                            sqlite_database_list.append((column_name, column_type_new))
                            sqlite_database_list_add.append(date_indicator)

                if not oracle_database_list:
                    # No columns found, select all columns
                    # oracle_column_list = "*"

                    # SQL to get table parameters (column names and data types)
                    sql_table = f"""
                    Select
                        TABLE_NAME,
                        COLUMN_ID,
                        COLUMN_NAME,
                        DATA_TYPE,
                        DATA_LENGTH,
                        DEFAULT_LENGTH,
                        OWNER,
                        NULLABLE,
                        DATA_DEFAULT
                    From
                        ALL_TAB_COLUMNS
                    Where
                        --OWNER = '{oracle_table_schema}' And
                        TABLE_NAME = '{oracle_table_name}'                    
                    Order by
                        OWNER,
                        TABLE_NAME,
                        COLUMN_ID
                    """

                    if l_debug:
                        print("Oracle sql query if no pre-defined columns exist:")
                        print(sql_table)

                    # Execute the SQL query
                    oracle_database_list = []
                    sqlite_database_list = []
                    sqlite_database_list_add = []
                    for column in oracle_cursor.execute(sql_table):
                        # Build the list of oracle column names

                        # Build the sqlite column list
                        column_name = column[2]  # Column name
                        column_alternate_name = ""  # Column alternate name
                        column_type = column[3]  # Column type
                        column_length = column[4]  # Column width
                        column_decimal = "0"  # column[7]  # Column decimals

                        # Build the list of oracle column names
                        oracle_database_list.append(column_name)

                        # Convert the Oracle column types to SQLite column types
                        column_type_new = ""
                        if column_type == "REAL":
                            column_type_new = "REAL"
                        elif column_type == "NUMBER" and column_decimal != "0":
                            column_type_new = "REAL"
                        elif column_type == "BINARY_FLOAT":
                            column_type_new = "REAL"
                        elif column_type == "BINARY_DOUBLE":
                            column_type_new = "REAL"
                        elif column_type == "FLOAT":
                            column_type_new = "REAL"
                        elif column_type == "NUMBER":
                            column_type_new = "INTEGER"
                        elif column_type == "INTEGER":
                            column_type_new = "INTEGER"
                        elif column_type == "PLS_INTEGER":
                            column_type_new = "INTEGER"
                        elif column_type == "BINARY_INTEGER":
                            column_type_new = "INTEGER"
                        elif column_type == "BLOB":
                            column_type_new = "BLOB"
                        elif column_type == "RAW":
                            column_type_new = "BLOB"
                        elif column_type == "LONG RAW":
                            column_type_new = "BLOB"
                        else:
                            column_type_new = "TEXT"

                        # Identify start date columns for later processing
                        start_date_list = ['STARTDATE', 'STARTDATETIME', 'KSTARTDATETIME', 'DATE_FROM', 'START_DATE_ACTIVE']
                        end_date_list = ['ENDDATE', 'ENDDATETIME', 'DATE_TO', 'END_DATE_ACTIVE']
                        if column_name in start_date_list:
                            date_indicator = "B"
                        elif column_name in end_date_list:
                            date_indicator = "E"
                        else:
                            date_indicator = ""

                        # Build the Oracle column type list
                        oracle_column_type_list.append(column_type)

                        # Replace column name with alternate column name
                        sqlite_database_list.append((column_name.replace("#", ""), column_type_new))
                        sqlite_database_list_add.append(date_indicator)

                        sqlite_column_list = ",".join([f"{name} {dtype}" for name, dtype in sqlite_database_list])

                    # Convert list to a string separated by commas
                    oracle_column_list = ','.join(oracle_database_list)
                    sqlite_column_list = ",".join([f"{name} {dtype}" for name, dtype in sqlite_database_list])

                    i_counter = len(oracle_column_list)
                    funcfile.writelog(f"DEFINED COLUMN COUNT: {i_counter}")

                else:

                    # Convert list to a string separated by commas
                    oracle_column_list = ','.join(oracle_database_list)
                    sqlite_column_list = ",".join([f"{name} {dtype}" for name, dtype in sqlite_database_list])

                    i_counter = len(oracle_column_list)
                    funcfile.writelog(f"DEFINED COLUMN COUNT: {i_counter}")

                if l_debug:
                    print("Oracle database list:")
                    print(oracle_database_list)
                    print("Oracle column list:")
                    print(oracle_column_list)
                    print("Oracle column type list:")
                    print(oracle_column_type_list)
                    print("Sqlite database list:")
                    print(sqlite_database_list)
                    print("Sqlite additional database list:")
                    print(sqlite_database_list_add)
                    print("Sqlite column list:")
                    print(sqlite_column_list)

                # Close the column database
                column_db.close()

                """************************************************************************
                BUILD THE ORACLE SQL QUERY
                ************************************************************************"""

                # Build the SQL sting
                if oracle_table_schema != "":
                    sql = f"Select {oracle_column_list} From {oracle_table_schema}.{oracle_table_name}"
                else:
                    sql = f"Select {oracle_column_list} From {oracle_table_name}"
                if oracle_where_clause != "":
                    if "%CYEAR%" in oracle_where_clause:
                        oracle_where_clause = oracle_where_clause.replace("%CYEAR%", funcdatn.get_current_year())
                    if "%CYEARB%" in oracle_where_clause:
                        oracle_where_clause = oracle_where_clause.replace("%CYEARB%", funcdatn.get_current_year_begin())
                    if "%CYEARE%" in oracle_where_clause:
                        oracle_where_clause = oracle_where_clause.replace("%CYEARE%", funcdatn.get_current_year_end())
                    if "%CMONTHB%" in oracle_where_clause:
                        oracle_where_clause = oracle_where_clause.replace("%CMONTHB%", funcdatn.get_current_month_begin())
                    if "%CMONTHE%" in oracle_where_clause:
                        oracle_where_clause = oracle_where_clause.replace("%CMONTHE%", funcdatn.get_current_month_end())
                    if "%PYEAR%" in oracle_where_clause:
                        oracle_where_clause = oracle_where_clause.replace("%PYEAR%", funcdatn.get_previous_year())
                    if "%PMONTHB%" in oracle_where_clause:
                        oracle_where_clause = oracle_where_clause.replace("%PMONTHB%", funcdatn.get_previous_month_begin())
                    if "%PMONTHE%" in oracle_where_clause:
                        oracle_where_clause = oracle_where_clause.replace("%PMONTHE%", funcdatn.get_previous_month_end())
                    if "%PYEARB%" in oracle_where_clause:
                        oracle_where_clause = oracle_where_clause.replace("%PYEARB%", funcdatn.get_previous_year_begin())
                    if "%PYEARE%" in oracle_where_clause:
                        oracle_where_clause = oracle_where_clause.replace("%PYEARE%", funcdatn.get_previous_year_end())
                    sql += f" {oracle_where_clause}"
                if oracle_table_order != "":
                    sql += f" Order by {oracle_table_order}"
                if l_debug:
                    # sql += " FETCH FIRST 5 ROWS ONLY"
                    print("Oracle sql to obtain the data:")
                    print(sql)

                """************************************************************************
                BUILD THE SQLITE TABLE
                ************************************************************************"""

                if sqlite_table_alt_name != "":
                    if "%CYEAR%" in sqlite_table_alt_name:
                        sqlite_table_alt_name = sqlite_table_alt_name.replace("%CYEAR%", funcdatn.get_current_year())
                    if "%CMONTH%" in sqlite_table_alt_name:
                        sqlite_table_alt_name = sqlite_table_alt_name.replace("%CMONTH%", funcdatn.get_current_month())
                    if "%PYEAR%" in sqlite_table_alt_name:
                        sqlite_table_alt_name = sqlite_table_alt_name.replace("%PYEAR%", funcdatn.get_previous_year())
                    if "%PMONTH%" in sqlite_table_alt_name:
                        sqlite_table_alt_name = sqlite_table_alt_name.replace("%PMONTH%", funcdatn.get_previous_month())
                    sql_drop = f"DROP TABLE IF EXISTS {sqlite_table_alt_name}"
                    sql_create = f"CREATE TABLE {sqlite_table_alt_name} ({sqlite_column_list})"
                else:
                    sql_drop = f"DROP TABLE IF EXISTS {oracle_table_name}"
                    sql_create = f"CREATE TABLE {oracle_table_name} ({sqlite_column_list})"

                # Create the destination SQLite
                if l_debug:
                    print("Sqlite sql to drop the table:")
                    print(sql_drop)
                sql_cursor.execute(sql_drop)
                if l_debug:
                    print("Sqlite sql to build the new table:")
                    print(sql_create)
                sql_cursor.execute(sql_create)
                sql_connection.commit()

                """************************************************************************
                QUERY AND FETCH DATA FROM ORACLE
                ************************************************************************"""

                # Query the database in chunks to save memory
                for result in funcsys.ResultIter(oracle_cursor.execute(sql)):
                    if l_debug:
                        print("Oracle query result:")
                        print(result)
                    new_values = []
                    # Use enumerate to iterate over each element in the original tuple and keep track of the index
                    for index, item in enumerate(result):

                        raw_item = str(item)
                        if raw_item == "" or raw_item in ("None", "0", ".0", "0.0"):
                            cleaned_item = ""
                        else:
                            # Create a regular expression pattern that matches the unwanted patterns
                            pattern = re.compile(r"[,'\"<>]")
                            # Use a single re.sub call to replace all patterns with an empty string
                            cleaned_item = re.sub(pattern, '', raw_item)

                        if oracle_column_type_list[index] == "DATE":
                            if cleaned_item == "":
                                # Default values for start (B) and end (E) dates
                                if sqlite_database_list_add[index] == "B":
                                    new_values.append("0001-01-01")
                                elif sqlite_database_list_add[index] == "E":
                                    new_values.append("2099-12-31")
                                else:
                                    new_values.append(cleaned_item)
                            else:
                                new_values.append(cleaned_item[0:10])
                        elif oracle_column_type_list[index] == "DATETIME":
                            if cleaned_item == "":
                                # Default values for start (B) and end (E) dates
                                if sqlite_database_list_add[index] == "B":
                                    new_values.append("0001-01-01 00:00:00")
                                elif sqlite_database_list_add[index] == "E":
                                    new_values.append("2099-12-31 23:59:59")
                                else:
                                    new_values.append(cleaned_item)
                            else:
                                new_values.append(cleaned_item[0:19])
                        elif oracle_column_type_list[index] == "NUMBER":
                            if cleaned_item == "":
                                new_values.append(cleaned_item)
                            else:
                                new_values.append(item)
                        elif oracle_column_type_list[index] == "REAL":
                            if cleaned_item == "":
                                new_values.append(cleaned_item)
                            else:
                                new_values.append(item)
                        else:
                            new_values.append(cleaned_item)

                    # Convert the new list back to a tuple
                    new_tuple = tuple(new_values)
                    if l_debug:
                        print("Sqlite data to be inserted:")
                        print(new_tuple)

                    """************************************************************************
                    SAVE THE FETCHED DATA IN SQLITE
                    ************************************************************************"""

                    try:
                        if sqlite_table_alt_name != "":
                            sql_insert = f"INSERT INTO {sqlite_table_alt_name} VALUES{new_tuple}"
                        else:
                            sql_insert = f"INSERT INTO {oracle_table_name} VALUES{new_tuple}"
                        if l_debug:
                            print("Sqlite sql to insert the results:")
                            print(sql_insert)
                        sql_cursor.execute(sql_insert)
                    except Exception as e:
                        funcfile.writelog(f"ERROR: {sql_insert}")
                        # funcsys.ErrMessage(e)

                # Commit the data
                sql_connection.commit()

                # Count the number of record submitted
                if sqlite_table_alt_name != "":
                    sql_cursor.execute(f"SELECT COUNT(*) FROM {sqlite_table_alt_name}")
                else:
                    sql_cursor.execute(f"SELECT COUNT(*) FROM {oracle_table_name}")
                i_counter = sql_cursor.fetchone()[0]
                message_inserted += f" {i_counter}\n"
                funcfile.writelog(f"RECORDS INSERTED: {i_counter}")

        if l_mess:
            funcsms.send_telegram('', 'administrator', message_inserted)
            message_inserted = ""

        # Close the table db
        table_db.close()

        # Close the Oracle connection
        oracle_connection.close()

        # Close the SQLite connection
        sql_connection.commit()
        sql_connection.close()

    # Return
    return l_return


if __name__ == '__main__':
    from _my_modules import funcsys
    try:
        print(oracle_to_sqlite_new())
        # print(oracle_to_sqlite_new("000b_Table - kfs.csv"))
        # print(oracle_to_sqlite_new("000b_Table - people.csv"))
        # print(oracle_to_sqlite_new("000b_Table - vss.csv"))
    except Exception as e:
        funcsys.ErrMessage(e)
