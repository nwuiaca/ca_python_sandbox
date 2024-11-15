"""
SCRIPT TO IMPORT LOOKUP TABLES
Script: A005_import_lookup_tables
Author: Albert B Janse van Rensburg (NWU:21162395)
Created: 18 November 2022
"""

# IMPORT PYTHON MODULES
import csv
import sqlite3

# IMPORT OWN MODULES
from _my_modules import funcconf
from _my_modules import funcfile
from _my_modules import funcsms
from _my_modules import funcsys

# INDEX
"""
ENVIRONMENT
BEGIN OF SCRIPT
IMPORT OWN LOOKUPS HR
IMPORT OWN LOOKUPS KFS
IMPORT OWN LOOKUPS VSS
END OF SCRIPT
"""

# SCRIPT WIDE VARIABLES
s_function: str = "A005 Import lookup tables"


def lookup_import(s_table: str = 'all'):
    """
    Script to import lookup tables
    :return: Nothing
    """

    """*****************************************************************************
    ENVIRONMENT
    *****************************************************************************"""

    # FUNCTION WIDE VARIABLES
    l_return: bool = True
    l_debug: bool = False  # Display debug messages
    l_mess: bool = funcconf.l_mess_project  # Send messages
    # l_mess: bool = False  # Send messages
    ed_path = f"{funcconf.drive_system}_external_data/"  # external data path

    # RUN THE IMPORT
    if l_return:

        """****************************************************************************
        BEGIN OF SCRIPT
        ****************************************************************************"""

        # SCRIPT LOG
        funcfile.writelog("Now")
        funcfile.writelog("SCRIPT: " + s_function.upper())
        funcfile.writelog("---------------------------------")
        if l_debug:
            print("-------------------------")
            print(s_function.upper())
            print("-------------------------")

        # MESSAGE
        if l_mess:
            funcsms.send_telegram("", "administrator", "<b>" + s_function + "</b>")

        """*****************************************************************************
        IMPORT OWN LOOKUPS HR
        *****************************************************************************"""
        if s_table in ('all', 'hr'):
            if l_debug:
                print("Import HR own lookups...")
            # DECLARE VARIABLES
            so_path = f"{funcconf.drive_data_raw}People/"  # Source database path
            so_file = "People.sqlite"  # Source database
            tb_name = "X000_OWN_HR_LOOKUPS"
            funcfile.writelog("%t IMPORT TABLE: " + tb_name)
            # OPEN THE WORKING DATABASE
            with sqlite3.connect(so_path + so_file) as so_conn:
                so_curs = so_conn.cursor()
            funcfile.writelog("%t OPEN DATABASE: " + so_file)
            so_curs.execute("DROP TABLE IF EXISTS " + tb_name)
            so_curs.execute("CREATE TABLE " + tb_name + "(LOOKUP TEXT,LOOKUP_CODE TEXT,LOOKUP_DESCRIPTION TEXT)")
            co = open(ed_path + "001_own_hr_lookups.csv", newline=None)
            co_reader = csv.reader(co)
            for row in co_reader:
                if row[0] == "LOOKUP":
                    continue
                else:
                    s_cols = "INSERT INTO " + tb_name + " VALUES('" + row[0] + "','" + row[1] + "','" + row[2] + "')"
                    so_curs.execute(s_cols)
            # CLOSE THE TARGET DATABASE
            so_conn.commit()
            i_record_counter = funcsys.tablerowcount(so_curs, tb_name)
            so_conn.close()
            # CLOSE THE SOURCE DATABASE
            co.close()
            funcfile.writelog("%t " + str(i_record_counter) + " Records imported.")
            # MESSAGE
            if l_debug:
                print(str(i_record_counter) + " Records imported.")
            if l_mess:
                funcsms.send_telegram("", "administrator", "<b>" + str(i_record_counter) + "</b> HR records imported")

        """*****************************************************************************
        IMPORT OWN LOOKUPS KFS
        *****************************************************************************"""

        # INTO KFS.SQLITE
        if s_table in ('all', 'kfs'):
            if l_debug:
                print("Import KFS own lookups...")
            # DECLARE VARIABLES
            so_path = f"{funcconf.drive_data_raw}Kfs/"  # Source database path
            so_file = "Kfs.sqlite"  # Source database
            tb_name = "X000_Own_kfs_lookups"
            funcfile.writelog("%t IMPORT TABLE: " + tb_name)
            # OPEN THE WORKING DATABASE
            with sqlite3.connect(so_path + so_file) as so_conn:
                so_curs = so_conn.cursor()
            funcfile.writelog("%t OPEN DATABASE: " + so_file)
            so_curs.execute("DROP TABLE IF EXISTS " + tb_name)
            so_curs.execute("CREATE TABLE " + tb_name + "(LOOKUP TEXT,LOOKUP_CODE TEXT,LOOKUP_DESCRIPTION TEXT)")
            co = open(ed_path + "001_own_kfs_lookups.csv", newline=None)
            co_reader = csv.reader(co)
            for row in co_reader:
                if row[0] == "LOOKUP":
                    continue
                else:
                    s_cols = "INSERT INTO " + tb_name + " VALUES('" + row[0] + "','" + row[1] + "','" + row[2] + "')"
                    so_curs.execute(s_cols)
            # CLOSE THE TARGET DATABASE
            so_conn.commit()
            i_record_counter = funcsys.tablerowcount(so_curs, tb_name)
            so_conn.close()
            # CLOSE THE SOURCE DATABASE
            co.close()
            funcfile.writelog("%t " + str(i_record_counter) + " Records imported.")
            # MESSAGE
            if l_debug:
                print(str(i_record_counter) + " Records imported.")
            if l_mess:
                funcsms.send_telegram("", "administrator", "<b>" + str(i_record_counter) + "</b> KFS records imported")

        # INTO KFS_TEST_GL_TRANSACTION.SQLITE
        if s_table in ('all', 'kfs'):
            if l_debug:
                print("Import KFS own lookups...")
            # DECLARE VARIABLES
            so_path = f"{funcconf.drive_data_raw}Kfs/"  # Source database path
            so_file = "Kfs_test_gl_transaction.sqlite"  # Source database
            tb_name = "X000_Own_kfs_lookups"
            funcfile.writelog("%t IMPORT TABLE: " + tb_name)
            # OPEN THE WORKING DATABASE
            with sqlite3.connect(so_path + so_file) as so_conn:
                so_curs = so_conn.cursor()
            funcfile.writelog("%t OPEN DATABASE: " + so_file)
            so_curs.execute("DROP TABLE IF EXISTS " + tb_name)
            so_curs.execute("CREATE TABLE " + tb_name + "(LOOKUP TEXT,LOOKUP_CODE TEXT,LOOKUP_DESCRIPTION TEXT)")
            co = open(ed_path + "001_own_kfs_lookups.csv", newline=None)
            co_reader = csv.reader(co)
            for row in co_reader:
                if row[0] == "LOOKUP":
                    continue
                else:
                    s_cols = "INSERT INTO " + tb_name + " VALUES('" + row[0] + "','" + row[1] + "','" + row[2] + "')"
                    so_curs.execute(s_cols)
            # CLOSE THE TARGET DATABASE
            so_conn.commit()
            i_record_counter = funcsys.tablerowcount(so_curs, tb_name)
            so_conn.close()
            # CLOSE THE SOURCE DATABASE
            co.close()
            funcfile.writelog("%t " + str(i_record_counter) + " Records imported.")
            # MESSAGE
            if l_debug:
                print(str(i_record_counter) + " Records imported.")
            if l_mess:
                funcsms.send_telegram("", "administrator", "<b>" + str(i_record_counter) + "</b> KFS records imported")

        """*****************************************************************************
        IMPORT OWN LOOKUPS VSS
        *****************************************************************************"""
        if s_table in ('all', 'vss'):
            if l_debug:
                print("Import VSS own lookups...")
            # DECLARE VARIABLES
            so_path = f"{funcconf.drive_data_raw}Vss/"  # Source database path
            so_file = "Vss.sqlite"  # Source database
            tb_name = "X000_Own_lookups"
            funcfile.writelog("%t IMPORT TABLE: " + tb_name)
            # OPEN THE WORKING DATABASE
            with sqlite3.connect(so_path + so_file) as so_conn:
                so_curs = so_conn.cursor()
            funcfile.writelog("%t OPEN DATABASE: " + so_file)
            so_curs.execute("DROP TABLE IF EXISTS " + tb_name)
            so_curs.execute("CREATE TABLE " + tb_name + "(LOOKUP TEXT,LOOKUP_CODE TEXT,LOOKUP_DESCRIPTION TEXT)")
            co = open(ed_path + "001_own_vss_lookups.csv", newline=None)
            co_reader = csv.reader(co)
            for row in co_reader:
                if row[0] == "LOOKUP":
                    continue
                else:
                    s_cols = "INSERT INTO " + tb_name + " VALUES('" + row[0] + "','" + row[1] + "','" + row[2] + "')"
                    so_curs.execute(s_cols)
            # CLOSE THE TARGET DATABASE
            so_conn.commit()
            i_record_counter = funcsys.tablerowcount(so_curs, tb_name)
            so_conn.close()
            # CLOSE THE SOURCE DATABASE
            co.close()
            funcfile.writelog("%t " + str(i_record_counter) + " Records imported.")
            # MESSAGE
            if l_debug:
                print(str(i_record_counter) + " Records imported.")
            if l_mess:
                funcsms.send_telegram("", "administrator", "<b>" + str(i_record_counter) + "</b> VSS records imported")

        """*****************************************************************************
        IMPORT OWN LOOKUPS IA
        *****************************************************************************"""
        if s_table in ('all', 'ia'):
            if l_debug:
                print("Import IA own lookups...")
            # DECLARE VARIABLES
            so_path = f"{funcconf.drive_data_raw}Internal_audit/"  # Source database path
            so_file = "Web_ia_nwu.sqlite"  # Source database
            tb_name = "X000_Own_lookups"
            funcfile.writelog("%t IMPORT TABLE: " + tb_name)
            # OPEN THE WORKING DATABASE
            with sqlite3.connect(so_path + so_file) as so_conn:
                so_curs = so_conn.cursor()
            funcfile.writelog("%t OPEN DATABASE: " + so_file)
            so_curs.execute("DROP TABLE IF EXISTS " + tb_name)
            so_curs.execute("CREATE TABLE " + tb_name + "(LOOKUP TEXT,LOOKUP_CODE TEXT,LOOKUP_DESCRIPTION TEXT)")
            co = open(ed_path + "001_own_ia_lookups.csv", newline=None)
            co_reader = csv.reader(co)
            for row in co_reader:
                if row[0] == "LOOKUP":
                    continue
                else:
                    s_cols = "INSERT INTO " + tb_name + " VALUES('" + row[0] + "','" + row[1] + "','" + row[2] + "')"
                    so_curs.execute(s_cols)
            # CLOSE THE TARGET DATABASE
            so_conn.commit()
            i_record_counter = funcsys.tablerowcount(so_curs, tb_name)
            so_conn.close()
            # CLOSE THE SOURCE DATABASE
            co.close()
            funcfile.writelog("%t " + str(i_record_counter) + " Records imported.")
            # MESSAGE
            if l_debug:
                print(str(i_record_counter) + " Records imported.")
            if l_mess:
                funcsms.send_telegram("", "administrator", "<b>" + str(i_record_counter) + "</b> IA records imported")

        """************************************************************************
        END OF SCRIPT
        ************************************************************************"""
        funcfile.writelog("END OF SCRIPT")
        if l_debug:
            print("END OF SCRIPT")

    return l_return


if __name__ == '__main__':
    try:
        lookup_import('hr')
    except Exception as e:
        funcsys.ErrMessage(e)
