"""
Statistical functions
Created on: 29 Aug 2019
Author: Albert J van Rensburg (NWU:21162395)
"""

# IMPORT SYSTEM OBJECTS
import statistics


def stat_mode(sqlite_cursor, source_table, source_column, where_condition=""):
    """
    Function to return the statistic mode for a column
    :param sqlite_cursor: Database cursor
    :param source_table: Table in database
    :param source_column: Column to calculate
    :param where_condition: Where clause
    :return: Mode statistic
    """

    # Convert the table column into a list and the calculate the mode
    s_sql = "SELECT " + source_column
    s_sql += " FROM " + source_table
    s_sql += " WHERE " + source_column + " Is Not Null"
    if where_condition != "":
        s_sql += " And " + where_condition
    s_sql += " ;"
    # print(s_sql)
    a_list = []
    for row in sqlite_cursor.execute(s_sql).fetchall():
        a_list.append(row[0])
    return statistics.mode(a_list)


def stat_pstdev(sqlite_cursor, source_table, source_column, where_condition=""):
    """
    Function to return the statistic mode for a column
    :param sqlite_cursor: Database cursor
    :param source_table: Table in database
    :param source_column: Column to calculate
    :param where_condition: Where clause
    :return: Population standard deviation statistic
    """

    # Convert the table column into a list and the calculate the mode
    s_sql = "SELECT " + source_column
    s_sql += " FROM " + source_table
    s_sql += " WHERE " + source_column + " Is Not Null"
    if where_condition != "":
        s_sql += " And " + where_condition
    s_sql += " ;"
    # print(s_sql)
    a_list = []
    for row in sqlite_cursor.execute(s_sql).fetchall():
        a_list.append(row[0])
    return statistics.pstdev(a_list)


def stat_highest_value(sqlite_cursor, source_table, source_column, where_condition=""):
    """
    Function to return the statistic highest value for a column
    :param sqlite_cursor: Database cursor
    :param source_table: Table in database
    :param source_column: Column to calculate
    :param where_condition: Where clause
    :return: Mode statistic
    """

    # Convert the table column into a list and the calculate the mode
    s_sql = "SELECT " + source_column
    s_sql += " FROM " + source_table
    s_sql += " WHERE " + source_column + " Is Not Null"
    if where_condition != "":
        s_sql += " And " + where_condition
    s_sql += " ORDER BY " + source_column + " DESC "
    s_sql += " ;"
    # print(s_sql)
    row = sqlite_cursor.execute(s_sql).fetchone()
    return row[0]


def stat_list(sqlite_cursor, source_table, source_column, where_condition=""):
    """
    Function to return a list of non-null values for a given column from the specified table.
    :param sqlite_cursor: Database cursor
    :param source_table: Table in database
    :param source_column: Column to return
    :param where_condition: Where clause
    :return: List of non-null values
    """

    # Build the SQL query
    s_sql = f"SELECT {source_column} FROM {source_table} WHERE {source_column} IS NOT NULL"
    if where_condition:
        s_sql += f" AND ({where_condition})"

    # Execute the query and return the results directly as a list
    sqlite_cursor.execute(s_sql)
    return [row[0] for row in sqlite_cursor.fetchall()]


def stat_list_reverse(sqlite_cursor, source_table, source_column, where_condition=""):
    """
    Function to return a list of non-null values for a given column from the specified table.
    :param sqlite_cursor: Database cursor
    :param source_table: Table in database
    :param source_column: Column to return
    :param where_condition: Where clause
    :return: List of non-null values
    """

    # Build the SQL query
    s_sql = f"SELECT {source_column} FROM {source_table} WHERE {source_column} IS NOT NULL"
    if where_condition:
        s_sql += f" AND ({where_condition})"
    s_sql += f" ORDER BY {source_column} DESC"

    # Execute the query and return the results directly as a list
    sqlite_cursor.execute(s_sql)
    return [row[0] for row in sqlite_cursor.fetchall()]


def stat_tuple(sqlite_cursor, source_table, source_column, where_condition=""):
    """
    Function to return the tuple of non-null values for a column
    :param sqlite_cursor: Database cursor
    :param source_table: Table in database
    :param source_column: Column to return
    :param where_condition: Where clause
    :return: Tuple
    """

    s_sql = f"SELECT {source_column} FROM {source_table} WHERE {source_column} IS NOT NULL"
    if where_condition:
        s_sql += f" AND ({where_condition})"
    s_sql += ";"

    # Execute the query and directly convert the result to a tuple
    sqlite_cursor.execute(s_sql)
    return tuple(row[0] for row in sqlite_cursor.fetchall())
