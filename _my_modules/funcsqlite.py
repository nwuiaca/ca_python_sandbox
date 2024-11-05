"""
Functions for various SQLite database and table actions.
Created on: 18 October 2023
Author: Albert B Janse van Rensburg (NWU:21162395)
"""

# IMPORT PYTHON MODULES
import sqlite3
import csv

# INDEX TO FUNCTIONS
"""
check_table_exists = Check if a table exist in a database connection.
get_column_names = Get a list of table column names. List format.
table_row_count = Number of rows in a table. 
"""


def check_table_exists(db_connection, table_name):
    """
    Test to see if a table exist or not.
    :param db_connection: Database connection
    :param table_name: Table to count
    :rtype: boolean
    :return: True or False
    """
    cursor = db_connection.cursor()
    cursor.execute(f'''
        SELECT COUNT(*)
        FROM sqlite_master
        WHERE type='table' AND name='{table_name}';
    ''')

    # If the count is 1, then table exists
    if cursor.fetchone()[0] == 1:
        return True

    return False


def get_column_names(db_cursor, table_name):
    """
    Build a list of table names.
    :param db_cursor: Database cursor
    :param table_name: Table to count
    :rtype: list
    :return: List of table columns
    """
    db_cursor.execute(f"PRAGMA table_info({table_name})")
    columns = [column[1] for column in db_cursor.fetchall()]
    return columns


def table_row_count(db_cursor, table_name):
    """
    Count the number of rows in a table.
    :param db_cursor: Database cursor
    :param table_name: Table to count
    :rtype: int
    :return: Number of rows
    """
    db_cursor.execute("SELECT COUNT(*) FROM " + table_name)
    x = db_cursor.fetchone()
    return int(x[0])


def sqlite_to_csv(db_cursor, table_name, csv_file):
    """
    Write all the data in an SQLite table to a csv file.
    :param db_cursor: Database cursor
    :param table_name: Table to read
    :param csv_file: Csv file to write
    :rtype: none
    :return: None
    """

    # Execute a query to get all data from the table
    db_cursor.execute(f"SELECT * FROM {table_name}")
    rows = db_cursor.fetchall()

    # Get column names from the cursor description
    column_names = [description[0] for description in db_cursor.description]

    # Write data to the csv file
    with open(csv_file, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(column_names)  # write header
        writer.writerows(rows)  # write data
