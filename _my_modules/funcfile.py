"""
Functions to handle file functions
Author: AB Janse v Rensburg
Create: 24 Jan 2018
"""

# IMPORT SYSTEM OBJECTS
import os
import logging
import datetime

# IMPORT OWN MODULES
from _my_modules import funcconf
from _my_modules import funcsys

# FUNCTIONS INDEX
"""
writelog - Function to create log file.
file_delete - Function to delete a file. 
get_field_value - Function to obtain a field value from any table.
"""


def writelog(s_entry: str = '\n',
             s_path: str = f'{funcconf.drive_system}logs/',
             s_file: str = f'Python_log_{datetime.datetime.now().strftime("%Y%m%d")}.txt',
             s_mode: str = 'a'):
    """
    Function to create log file

    :param s_entry: Log file entry
    :param s_path: Log file path
    :param s_file: Log file name
    :param s_mode: File mode
    :return: Nothing
    """

    # Declare variables
    s_project: str = f'FUNCFILE: {s_file}'

    try:
        with open(os.path.join(s_path, s_file), s_mode, newline='', encoding="utf-8") as fl:
            # File opened for writing. Write the log
            if s_entry == "Now":
                fl.write("----------------\n")
                s_entry = datetime.datetime.now().strftime("%Y-%m-%d %H:%M") + "\n"
            elif "%t" in s_entry:
                s_entry = s_entry.replace("%t", datetime.datetime.now().strftime("%H:%M:%S")) + "\n"
            else:
                s_entry += "\n"
            fl.write(s_entry)
            return True
    except IOError as err:
        logging.error(f"NWUIACA:Fail:{s_project} - {err}")
        return False


def file_delete(s_path: str = "", s_file: str = "", debug: bool = False):
    """
    Function to delete a file.

    :param s_path: Log file path
    :param s_file: Log file name
    :param debug: Whether to print debug information
    :return: bool: True - deleted or False
    """

    # Declare variables
    file_path = os.path.join(s_path, s_file)

    if debug:
        print('File to delete: ' + file_path)

    try:
        os.remove(file_path)
        return True
    except FileNotFoundError:
        return False


def get_field_value(o_cursor, s_table='', s_field='', s_where=''):
    """
    Function to obtain a field value from any table.

    :param o_cursor: object: Database cursor
    :param s_table: str: Table to query
    :param s_field: str: Field value to return
    :param s_where: str: Where clause
    :return: str: Value of lookup field
    """

    # IMPORT OWN MODULES
    from _my_modules import funcfile

    # DECLARE VARIABLES
    l_debug: bool = False

    # SET PREVIOUS FINDINGS
    if l_debug:
        print("Field value lookup...")

    s_sql = """
        Select
            t.%FIELD%
        FROM
            %TABLE% t
        WHERE
            t.%WHERE%
        ;"""
    s_sql = s_sql.replace("%TABLE%", s_table)
    s_sql = s_sql.replace("%FIELD%", s_field)
    s_sql = s_sql.replace("%WHERE%", s_where)

    if l_debug:
        print(s_sql)

    t_return = o_cursor.execute(s_sql).fetchone()

    if l_debug:
        print(t_return)

    if t_return is not None:
        s_return = str(t_return[0])
    else:
        s_return = "UNKNOWN "
    funcfile.writelog("%t OBTAIN TABLE FIELD VALUE: " + s_table + " " + s_field + " " + s_where)

    return s_return


def writelog_old(s_entry: str = '\n',
                 s_path: str = f'{funcconf.drive_system}Logs/',
                 s_file: str = 'Python_log_' + datetime.datetime.now().strftime('%Y%m%d') + '.txt',
                 s_mode: str = 'a'):
    """
    Function to create log file
    :param s_entry: Log file entry
    :param s_path: Log file path
    :param s_file: Log file name
    :param s_mode: File mode
    :return: Nothing
    """

    # DECLARE VARIABLES
    s_project: str = 'FUNCFILE: ' + s_file

    try:

        with open(s_path + s_file, s_mode, newline='', encoding="utf-8") as fl:
            # file opened for writing. write to it here
            # Write the log
            if s_entry == "Now":
                fl.write("----------------\n")
                s_entry = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")+"\n"
            elif "%t" in s_entry:
                s_entry = s_entry.replace("%t", datetime.datetime.now().strftime("%H:%M:%S"))
                s_entry += "\n"
            else:
                s_entry += "\n"
            fl.write(s_entry)
            fl.close()
            pass
            l_success = True

    except Exception as err:

        l_success = False
        funcsys.ErrMessage(err, funcconf.l_mail_project,
                           "NWUIACA:Fail:" + s_project,
                           "NWUIACA: Fail: " + s_project)

    return l_success


def file_delete_old(s_path: str = "", s_file: str = ""):
    """
    Function to delete a file.

    :param s_path: Log file path
    :param s_file: Log file name
    :return: bool: True - deleted or False
    """

    # DECLARE VARIABLES
    l_debug: bool = False
    l_return: bool = False

    if l_debug:
        print('File to delete: ' + s_path + s_file)

    if os.path.exists(s_path + s_file):
        os.remove(s_path + s_file)
        l_return = True

    return l_return
