"""
SCRIPT TO IMPORT IA WEB DATA FROM MYSQL TO SQLITE
Script: A004_import_ia.py
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

# INDEX AND INDEX OF FUNCTIONS
"""
ia_mysql_import = Function to import the mysql data
Calculate the finding audit risk score
Calculate the finding client risk score
Calculate the assignment overall audit risk score
Calculate the assignment overall audit risk score client
"""

# SCRIPT WIDE VARIABLES
s_function: str = "A004 IMPORT IA"


def ia_mysql_update(s_source_database: str = "Web_nwu_ia"):
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
    so_path: str = f"{funcconf.drive_data_raw}Internal_audit/"
    so_file: str = "Web_ia_nwu.sqlite"

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
        THE CALCULATIONS
        ****************************************************************************"""

        # Display the calculated values as an example
        if l_debug:

            sql = """
            Select
                find.ia_find_auto,
                cast(rate.ia_findrate_impact as integer)As ia_findrate_impact,
                cast(hood.ia_findlike_value as integer) As ia_findlike_value,
                cast(cont.ia_findcont_value as decimal(6,2)) As ia_findcont_value,
                case
                  when cast(cont.ia_findcont_value as decimal(6,2)) > 0.90 then cast(rate.ia_findrate_impact as integer) * (cast(hood.ia_findlike_value as integer) * 0.10)
                  else cast(rate.ia_findrate_impact as integer) * cast(hood.ia_findlike_value as integer) * (1 - cast(cont.ia_findcont_value as decimal(6,2)))
                end as result
            From
                ia_finding find Inner Join
                ia_finding_rate rate On rate.ia_findrate_auto = find.ia_findrate_auto Inner Join
                ia_finding_likelihood hood On hood.ia_findlike_auto = find.ia_findlike_auto Inner Join
                ia_finding_control cont On cont.ia_findcont_auto = find.ia_findcont_auto        
            ;"""
            for row in ms_from_cursor.execute(sql).fetchall():

                if l_debug:
                    print(row[0])
                    print(row[4])

        # Calculate the finding audit risk score
        try:
            sql = """
            Update ia_finding find
            Join ia_finding_rate rate On rate.ia_findrate_auto = find.ia_findrate_auto
            Join ia_finding_likelihood hood On hood.ia_findlike_auto = find.ia_findlike_auto
            Join ia_finding_control cont On cont.ia_findcont_auto = find.ia_findcont_auto
            Set find.ia_find_riskscore = cast(rate.ia_findrate_impact as integer) * 
                                         cast(hood.ia_findlike_value as integer) * 
                                         CASE
                                             WHEN cast(cont.ia_findcont_value as decimal(6,2)) > 0.90 
                                             THEN 0.10
                                             ELSE (1 - cast(cont.ia_findcont_value as decimal(6,2)))
                                         END;
            """
            ms_from_cursor.execute(sql)
            # Commit the transaction if the command was successful
            ms_from_connection.commit()

            # Get the number of affected rows
            affected_rows = ms_from_cursor.rowcount

            # Check if the rows were affected
            if affected_rows != -1:
                print(f"Command executed successfully, {affected_rows} rows affected.")
            else:
                print("Command executed successfully, but the number of affected rows could not be determined.")

        except Exception as e:
            # Rollback the transaction in case of error
            ms_from_connection.rollback()
            print(f"An error occurred: {e}")

        # Calculate the finding client risk score
        try:
            sql = """
            Update ia_finding find
            Join ia_finding_rate rate On rate.ia_findrate_auto = find.ia_findrate_auto_client
            Join ia_finding_likelihood hood On hood.ia_findlike_auto = find.ia_findlike_auto_client
            Join ia_finding_control cont On cont.ia_findcont_auto = find.ia_findcont_auto_client
            Set find.ia_find_riskscore_client = cast(rate.ia_findrate_impact as integer) * 
                                         cast(hood.ia_findlike_value as integer) * 
                                         CASE
                                             WHEN cast(cont.ia_findcont_value as decimal(6,2)) > 0.90 
                                             THEN 0.10
                                             ELSE (1 - cast(cont.ia_findcont_value as decimal(6,2)))
                                         END;
            """
            ms_from_cursor.execute(sql)
            # Commit the transaction if the command was successful
            ms_from_connection.commit()

            # Get the number of affected rows
            affected_rows = ms_from_cursor.rowcount

            # Check if the rows were affected
            if affected_rows != -1:
                print(f"Command executed successfully, {affected_rows} rows affected.")
            else:
                print("Command executed successfully, but the number of affected rows could not be determined.")

        except Exception as e:
            # Rollback the transaction in case of error
            ms_from_connection.rollback()
            print(f"An error occurred: {e}")

        # Calculate the assignment overall audit risk score
        try:

            sql = """
            UPDATE ia_assignment assi
            JOIN (
                SELECT find.ia_assi_auto, AVG(find.ia_find_riskscore) AS avg_riskscore
                FROM ia_finding find Inner Join
                ia_assignment assi On assi.ia_assi_auto = find.ia_assi_auto Left Join
                ia_finding_status fist On fist.ia_findstat_auto = find.ia_findstat_auto Left Join
                jm4_users us On us.id = assi.ia_user_sysid Left Join
                ia_finding_rate rate On rate.ia_findrate_auto = find.ia_findrate_auto Left Join
                ia_finding_control cont On cont.ia_findcont_auto = find.ia_findcont_auto Left Join
                ia_finding_likelihood likh On likh.ia_findlike_auto = find.ia_findlike_auto Left Join
                ia_finding_audit aust On aust.ia_findaud_auto = find.ia_findaud_auto Left Join
                ia_assignment_category cate On cate.ia_assicate_auto = assi.ia_assicate_auto Left Join
                ia_assignment_type type On type.ia_assitype_auto = assi.ia_assitype_auto
                WHERE cate.ia_assicate_private = '0' and 
                type.ia_assitype_private = '0' and 
                find.ia_find_private = '0' and 
                find.ia_find_appendix = 0 and 
                fist.ia_findstat_private = '0' and
                find.ia_find_riskscore > 0
                GROUP BY ia_assi_auto
            ) find_avg ON find_avg.ia_assi_auto = assi.ia_assi_auto
            SET assi.ia_assi_riskscore = find_avg.avg_riskscore;
            """
            ms_from_cursor.execute(sql)
            # Commit the transaction if the command was successful
            ms_from_connection.commit()

            # Get the number of affected rows
            affected_rows = ms_from_cursor.rowcount

            # Check if the rows were affected
            if affected_rows != -1:
                print(f"Command executed successfully, {affected_rows} rows affected.")
            else:
                print("Command executed successfully, but the number of affected rows could not be determined.")

        except Exception as e:
            # Rollback the transaction in case of error
            ms_from_connection.rollback()
            print(f"An error occurred: {e}")

        # Calculate the assignment overall audit risk score client
        try:

            sql = """
            UPDATE ia_assignment assi
            JOIN (
                SELECT find.ia_assi_auto, AVG(find.ia_find_riskscore_client) AS avg_riskscore
                FROM ia_finding find Inner Join
                ia_assignment assi On assi.ia_assi_auto = find.ia_assi_auto Left Join
                ia_finding_status fist On fist.ia_findstat_auto = find.ia_findstat_auto Left Join
                jm4_users us On us.id = assi.ia_user_sysid Left Join
                ia_finding_rate rate On rate.ia_findrate_auto = find.ia_findrate_auto Left Join
                ia_finding_control cont On cont.ia_findcont_auto = find.ia_findcont_auto Left Join
                ia_finding_likelihood likh On likh.ia_findlike_auto = find.ia_findlike_auto Left Join
                ia_finding_audit aust On aust.ia_findaud_auto = find.ia_findaud_auto Left Join
                ia_assignment_category cate On cate.ia_assicate_auto = assi.ia_assicate_auto Left Join
                ia_assignment_type type On type.ia_assitype_auto = assi.ia_assitype_auto
                WHERE cate.ia_assicate_private = '0' and 
                type.ia_assitype_private = '0' and 
                find.ia_find_private = '0' and 
                find.ia_find_appendix = 0 and 
                fist.ia_findstat_private = '0' and
                find.ia_find_riskscore_client > 0
                GROUP BY ia_assi_auto
            ) find_avg ON find_avg.ia_assi_auto = assi.ia_assi_auto
            SET assi.ia_assi_riskscore_client = find_avg.avg_riskscore;
            """
            ms_from_cursor.execute(sql)
            # Commit the transaction if the command was successful
            ms_from_connection.commit()

            # Get the number of affected rows
            affected_rows = ms_from_cursor.rowcount

            # Check if the rows were affected
            if affected_rows != -1:
                print(f"Command executed successfully, {affected_rows} rows affected.")
            else:
                print("Command executed successfully, but the number of affected rows could not be determined.")

        except Exception as e:
            # Rollback the transaction in case of error
            ms_from_connection.rollback()
            print(f"An error occurred: {e}")


        """************************************************************************
        END OF SCRIPT
        ************************************************************************"""
        funcfile.writelog("END OF SCRIPT")
        if l_debug:
            print("END OF SCRIPT")

    return l_return


if __name__ == '__main__':
    try:
        ia_mysql_update()
    except Exception as e:
        funcsys.ErrMessage(e)
