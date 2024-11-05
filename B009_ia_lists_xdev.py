"""
Script to build internal audit lists
Created on 24 Oct 2022
Author: Albert J v Rensburg (NWU:21162395)
"""

# IMPORT PYTHON MODULES
import sqlite3
import csv

# IMPORT OWN MODULES
from _my_modules import funcconf
from _my_modules import funcdatn
from _my_modules import funcfile
from _my_modules import funcstat
from _my_modules import funcsys
from _my_modules import funcsms

# SCRIPT WIDE VARIABLES
s_function: str = "B009_ia_lists"


def ia_lists(s_period: str = "curr"):
    """
    Script to build INTERNAL AUDIT lists
    :param s_period: str: The audit season
    :return: Nothing
    """

    """*****************************************************************************
    ENVIRONMENT
    *****************************************************************************"""

    # FUNCTION WIDE VARIABLES
    if s_period == "prev" and funcdatn.get_current_month() >= "10":
        s_year: str = str(int(funcdatn.get_previous_year()) + 1)
    elif s_period == "prev":
        s_year: str = funcdatn.get_previous_year()
    elif s_period == "curr" and funcdatn.get_current_month() >= "10":
        s_year: str = str(int(funcdatn.get_current_year())+1)
    elif s_period == "curr":
        s_year: str = funcdatn.get_current_year()
    else:
        s_year: str = s_period
    s_from: str = str(int(s_year) - 1) + '-10-01'
    s_to: str = s_year + '-09-30'

    # ed_path: str = "S:/_external_data/"  # External data path
    # re_path: str = f"{funcconf.drive_data_results}Internal_audit/" + s_year
    so_path: str = f"{funcconf.drive_data_raw}Internal_audit/"  # Source database path
    so_file: str = "Web_ia_nwu.sqlite"
    l_debug: bool = True
    # l_mail: bool = funcconf.l_mail_project
    # l_mail: bool = True
    # l_mess: bool = funcconf.l_mess_project
    l_mess: bool = False
    # l_record: bool = False
    # l_export: bool = False
    s_madelein: str = "11987774@nwu.ac.za"
    s_shahed: str = "10933107@nwu.ac.za"
    s_nicolene: str = "12119180@nwu.ac.za"

    # LOG
    funcfile.writelog("Now")
    funcfile.writelog("SCRIPT: " + s_function.upper())
    funcfile.writelog("-" * len("script: "+s_function))
    if l_debug:
        print(s_function.upper())

    # MESSAGE
    if l_mess:
        funcsms.send_telegram("", "administrator", "<b>" + s_function + "</b>")

    """************************************************************************
    OPEN THE DATABASES
    ************************************************************************"""
    funcfile.writelog("OPEN THE DATABASES")
    if l_debug:
        print("OPEN THE DATABASES")

    # OPEN SQLITE SOURCE table
    if l_debug:
        print("Open sqlite database...")
    with sqlite3.connect(so_path + so_file) as so_conn:
        so_curs = so_conn.cursor()
    funcfile.writelog("OPEN DATABASE: " + so_file)

    """************************************************************************
    TEMPORARY AREA
    ************************************************************************"""
    funcfile.writelog("TEMPORARY AREA")
    if l_debug:
        print("TEMPORARY AREA")

    """************************************************************************
    BEGIN OF SCRIPT
    ************************************************************************"""
    funcfile.writelog("BEGIN OF SCRIPT")
    if l_debug:
        print("BEGIN OF SCRIPT")

    """************************************************************************
    END OF SCRIPT
    ************************************************************************"""
    funcfile.writelog("END OF SCRIPT")
    if l_debug:
        print("END OF SCRIPT")

    # CLOSE THE DATABASE CONNECTION
    so_conn.commit()
    so_conn.close()

    # CLOSE THE LOG WRITER
    funcfile.writelog("-" * len("completed: "+s_function))
    funcfile.writelog("COMPLETED: " + s_function.upper())

    return


if __name__ == '__main__':
    try:
        ia_lists('curr')
    except Exception as e:
        funcsys.ErrMessage(e)
