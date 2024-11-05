"""
Script to prepare, export and mail Searchworks requests
Created on: 21 September 2023
Author: Yolandie Koekemoer (NWU:12788074)
Author: Albert J van Rensburg (NWU:21162395)
"""

# IMPORT PYTHON MODULES
import datetime
import sys
import sqlite3
import csv
import shutil
import os
import pandas as pd

# IMPORT OWN MODULES
# sys.path.append('_my_modules')
from _my_modules import funcconf
from _my_modules import funcdatn
from _my_modules import funcsys
from _my_modules import funcfile
from _my_modules import funcmail
from _my_modules import funcsms
from _my_modules import funcsqlite
from _my_modules import funcstat
import A008_backup_director

# INDEX
"""
ENVIRONMENT
OPEN THE DATABASES
TEMPORARY AREA
BEGIN OF SCRIPT
BUILD TABLE WITH PREVIOUS SUBMITTED EMPLOYEES
READ THE WATCHLIST DATA
BUILD NEW SUBMISSIONS IN SQLITE
BUILD THE EMAIL
IMPORT SEARCHWORKS CIPC RESULTS IF A FILE WITH DATA EXISTS
NO SEARCHWORKS DIRECTORS IMPORTED
END OF SCRIPT
"""

# LOGIC
"""

"""

# SCRIPT WIDE VARIABLES
s_function: str = "B011_searchworks"


def searchworks_submit(i_day_to_run: int = 6, l_override_date: bool = False):
    """****************************************************************************
    ENVIRONMENT
    Format can be different and not exact.
    <Tab> to shift in
    <Shift><Tab> to shift out
    ****************************************************************************"""

    # DECLARE VARIABLES
    l_debug: bool = False  # Enable the display of on-screen debug messages.
    l_mess: bool = funcconf.l_mess_project  # Enable / disable the robot communicator message function.
    # l_mess: bool = False  # Enable / disable the robot communicator message function.
    so_path: str = f"{funcconf.drive_data_raw}People_conflict/"  # Source database path
    so_file: str = "People_conflict.sqlite"  # Source database
    sw_path: str = f"{funcconf.drive_system}searchworks/"
    csv_master_name: str = "01_master_submit.csv"
    csv_send_name: str = "02_nwu_submit_" + funcdatn.get_today_date_file() + ".csv"
    csv_watchlist_name: str = "03_searchworks_watchlist.csv"
    csv_watchlist_new: str = "03_searchworks_watchlist_" + funcdatn.get_today_date_file() + ".csv"
    xls_results: str = "NWU CIPC Results.xlsx"
    csv_results: str = "04_cipc_results_" + funcdatn.get_today_date_file() + ".csv"
    csv_history: str = "05_cipc_history_" + funcdatn.get_today_date_file() + ".csv"

    # LOG
    funcfile.writelog("Now")
    funcfile.writelog("SCRIPT: " + s_function.upper())
    funcfile.writelog("-" * len("script: " + s_function))
    if l_debug:
        print(s_function.upper())

    # MESSAGE
    if l_mess:
        funcsms.send_telegram("", "administrator", "<b>" + s_function + "</b>")

    """************************************************************************
    OPEN THE DATABASES
    ************************************************************************"""
    if l_debug:
        print("OPEN THE DATABASES")
    funcfile.writelog("OPEN THE DATABASES")

    # OPEN THE WORKING DATABASE
    funcfile.writelog("OPEN DATABASE: " + so_file)
    with sqlite3.connect(so_path + so_file) as sqlite_connection:
        sqlite_cursor = sqlite_connection.cursor()

    # ATTACH DATA SOURCES
    funcfile.writelog("%t ATTACH DATABASE: People.sqlite")
    sqlite_cursor.execute(f"ATTACH DATABASE '{funcconf.drive_data_raw}People/People.sqlite' AS 'PEOPLE'")
    sqlite_cursor.execute(f"ATTACH DATABASE '{funcconf.drive_data_raw}Kfs/Kfs.sqlite' AS 'KFS'")
    funcfile.writelog("%t ATTACH DATABASE: KFS.SQLITE")

    """************************************************************************
    TEMPORARY AREA
    ************************************************************************"""
    if l_debug:
        print("TEMPORARY AREA")
    funcfile.writelog("TEMPORARY AREA")

    """************************************************************************
    BEGIN OF SCRIPT
    ************************************************************************"""
    if l_debug:
        print("BEGIN OF SCRIPT")
    funcfile.writelog("BEGIN OF SCRIPT")

    # SCRIPT MUST ONLY RUN ON 2ND OF EACH MONTH

    # Get the current date and days
    today = datetime.date.today()
    if l_debug:
        print("Today: ")
        print(today)
    current_day = today.day
    current_weekday = today.weekday()
    b_run_script: bool = False

    # Determine if the code should run or not
    for i in range(3):
        if current_day == i_day_to_run and current_weekday < 5:
            b_run_script = True
            break
        elif current_day == i_day_to_run + i and current_weekday == 0:
            b_run_script = True
            break

    # Override the run date
    if l_override_date:
        b_run_script = True

    # Run the script
    if b_run_script:

        # IDENTIFY NEW EMPLOYEES WHO SHOULD BE SUBMITTED

        # Create SQLite table to receive previously submitted employees.
        sr_file = "X004a_submitted_to_searchworks"
        s_sql = "CREATE TABLE " + sr_file + """
        (
        nwu_number TEXT,
        employee_name TEXT,
        national_identifier TEXT,
        ni_type TEXT,
        date_submitted TEXT
        )
        """
        sqlite_cursor.execute("DROP TABLE IF EXISTS " + sr_file)
        sqlite_cursor.execute(s_sql)
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

        # Read csv data previously submitted and populate SQLite table.
        with open(sw_path + csv_master_name, "r") as file:
            csv_reader = csv.DictReader(file)
            for row in csv_reader:
                sqlite_cursor.execute(
                    "INSERT INTO " + sr_file + " VALUES (:nwu_number,"
                                               " :employee_name,"
                                               " :national_identifier,"
                                               " :ni_type,"
                                               " :date_submitted)",
                    row)
        sqlite_connection.commit()
        funcfile.writelog("%t IMPORT CSV: " + sw_path + csv_master_name)

        # Build a new SQLite table with all current employees, and join the previously submitted employees.
        sr_file = "X004b_employee_submitted"
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            p.employee_number,
            p.name_address,
            Case
                When p.national_identifier == '' And p.passport = '' Then p.permit
                When p.national_identifier == '' Then p.passport
                Else p.national_identifier
            End As national_identifier,
            Case
                When p.national_identifier == '' And p.passport = '' Then 'WP'
                When p.national_identifier == '' Then 'PP'
                Else 'ID'
            End As ni_type,
            s.date_submitted
        From
            PEOPLE.X000_PEOPLE p Left Join
            X004a_submitted_to_searchworks s On s.nwu_number = p.employee_number
        Where
            (p.assignment_category Like('PERMANENT%')) Or
            (p.user_person_type Like('COUNCIL%'))     
        """
        sqlite_cursor.execute("DROP TABLE IF EXISTS " + sr_file)
        sqlite_cursor.execute(s_sql)
        sqlite_connection.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

        # IMPORT WATCHLIST ID NUMBERS RECEIVED FROM SEARCHWORKS TO FORM PART OF THE NEW SUBMISSION

        # Create SQLite table to receive watchlist id numbers.
        sr_file = "X004d_searchworks_watchlist"
        s_sql = "CREATE TABLE " + sr_file + """
        (
        national_identifier TEXT
        )
        """
        sqlite_cursor.execute("DROP TABLE IF EXISTS " + sr_file)
        sqlite_cursor.execute(s_sql)
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

        # Read watchlist csv data and populate SQLite table.
        with open(sw_path + csv_watchlist_name, 'r') as file:
            csv_reader = csv.DictReader(file)
            for row in csv_reader:
                sqlite_cursor.execute(
                    "INSERT INTO " + sr_file + " VALUES (:national_identifier)",
                    row)
        sqlite_connection.commit()
        funcfile.writelog("%t IMPORT CSV: " + sw_path + csv_watchlist_name)

        # Count the number of lines in the watchlist.
        line_count_watchlist: int = 0
        with open(sw_path + csv_watchlist_name, 'r') as file:
            csv_reader = csv.reader(file)
            line_count_watchlist = sum(1 for row in csv_reader) - 1
            if l_debug:
                print('Watchlist items:')
                print(line_count_watchlist)

        # Add the list of watchlist id's to the employee SQLite table above containing all employees. Join the
        # watchlist data on national_identifier.
        sr_file = "X004b_employee_submitted_watchlist"
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            s.employee_number,
            s.name_address,
            s.national_identifier,
            s.ni_type,
            s.date_submitted,
            w.national_identifier As national_identifier_watchlist
        From
            X004b_employee_submitted s Left Join
            X004d_searchworks_watchlist w On w.national_identifier = s.national_identifier
        Group By
            s.national_identifier    
        """
        sqlite_cursor.execute("DROP TABLE IF EXISTS " + sr_file)
        sqlite_cursor.execute(s_sql)
        sqlite_connection.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

        # Make a copy of the raw watchlist data submitted for future reference.
        send_file_size: int = os.path.getsize(sw_path + csv_watchlist_name)
        # Debug
        if l_debug:
            print("Watchlist file size: ")
            print(send_file_size)

        # Only create a copy of the watchlist file if not empty
        if send_file_size > 24:

            # Make a copy of the temporary watchlist file.
            shutil.copy2(sw_path + csv_watchlist_name, sw_path + csv_watchlist_new)

            if not l_debug:
                # Delete the watchlist file.
                os.remove(sw_path + csv_watchlist_name)
                # Create a blank watchlist file size = 24.
                header = ['national_identifier']
                data = ['0']
                with open(sw_path + csv_watchlist_name, 'w', newline='') as f:
                    writer = csv.writer(f)
                    writer.writerow(header)
                    writer.writerow(data)

        # BUILD NEW SUBMISSIONS IN SQLITE

        # Build the final SQLite table containing all the new submissions.
        sr_file = "X004c_new_submissions"
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            s.employee_number,
            s.name_address,
            s.national_identifier,
            Case
            When s.national_identifier_watchlist Is Not Null Then s.ni_type || 'W'
            Else s.ni_type   
            End As ni_type,
            CURRENT_DATE As date_submitted
        From
            X004b_employee_submitted_watchlist s
        Where
            (s.date_submitted Is Null) Or
            (s.national_identifier_watchlist Is Not Null)
        """
        sqlite_cursor.execute("DROP TABLE IF EXISTS " + sr_file)
        sqlite_cursor.execute(s_sql)
        sqlite_connection.commit()
        line_count_total: int = funcsqlite.table_row_count(sqlite_cursor, sr_file)
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

        # Only build new files if records found
        if line_count_total > 0:

            # Calculate the number of new submissions.
            line_count_new: int = line_count_total - line_count_watchlist

            # Communicate the new submission count
            if l_mess:
                s_desc = "New submissions"
                funcsms.send_telegram('', 'administrator', '<b>' + str(line_count_new) + '</b> ' + s_desc)

            # Open the new submissions SQLite table.
            sr_file = "X004c_new_submissions"
            s_sql = """SELECT
            employee_number,
            name_address,
            national_identifier,
            ni_type,
            date_submitted
            """ + " FROM " + sr_file
            sqlite_cursor.execute(s_sql)
            data = sqlite_cursor.fetchall()
            funcfile.writelog("%t READ DATA: " + sr_file)

            # Create and export the new submissions to a csv file to send via email.
            funcfile.file_delete(sw_path, csv_send_name)
            with open(sw_path + csv_send_name, 'w', newline='') as csv_file:
                writer = csv.writer(csv_file)
                header = [('nwu_number', 'employee_name', 'national_identifier', 'ni_type', 'date_submitted')]
                # Write data to the CSV file
                writer.writerows(header)
                writer.writerows(data)
            funcfile.writelog("%t BUILD CSV: " + csv_send_name + " (" + str(line_count_total) + " new records)")

            # Communicate the new submission count
            if l_mess:
                s_desc = "Watchlist submissions"
                funcsms.send_telegram('', 'administrator', '<b>' + str(line_count_watchlist) + '</b> ' + s_desc)

            # Update the master submission csv file with the new submissions.
            if not l_debug:
                with open(sw_path + csv_master_name, 'a', newline='') as csv_file:
                    writer = csv.writer(csv_file)
                    # Write data to the CSV file
                    writer.writerows(data)
                funcfile.writelog("%t UPDATE CSV: " + csv_master_name)

            # Communicate the total submission count
            if l_mess:
                s_desc = "Total submission"
                funcsms.send_telegram('', 'administrator', '<b>' + str(line_count_total) + '</b> ' + s_desc)

            # BUILD THE EMAIL

            # The body
            mail_subject: str = "NWU Employee data for company search and ID watchlist."
            mail_body: str = """
            Attached please find a list of updated employees for which we require a company search and upload to the ID watchlist.
            Please prepare and forward the quotation to us please.
            """

            # Send the mail
            if not l_debug:
                funcmail.send_mail('b011_searchworks_submit', mail_subject, mail_body)

        else:

            # No new records found
            funcfile.writelog("%t NO NEW EMPLOYEES FOUND TO SUBMIT TO SEARCHWORKS")

            # Communicate no submissions
            if l_mess:
                s_desc = "No submissions"
                funcsms.send_telegram('', 'administrator', s_desc)

    # IMPORT SEARCHWORKS CIPC RESULTS IF A FILE WITH DATA EXISTS
    # Test for file name NWU CIPC Results.xlsx.
    # File should contain sheet named Directorships.

    if os.path.isfile(sw_path + xls_results):

        # Read the Excel file.
        excel_file = pd.read_excel(sw_path + xls_results, sheet_name='Directorships')
        funcfile.writelog("%t IMPORT EXCEL: " + sw_path + xls_results)

        # Rename the columns by replacing spaces with underscores and removing ':'.
        excel_file.columns = excel_file.columns.str.replace(' ', '_').str.replace(':', '').str.lower()
        excel_file.replace("'", '', regex=True, inplace=True)
        excel_file.replace('"', '', regex=True, inplace=True)
        excel_file.replace('É', 'E', regex=True, inplace=True)
        funcfile.writelog("%t CLEAN DATA: " + sw_path + csv_results)

        # Count the number of records
        line_count_results: int = len(excel_file)

        # Write to csv.
        excel_file.to_csv(sw_path + csv_results, index=False)
        funcfile.writelog("%t BUILD CSV: " + sw_path + csv_results)

        # Communicate the total submission count
        if l_debug:
            print('Results imported:')
            print(line_count_results)
        if l_mess:
            s_desc = "Results imported"
            funcsms.send_telegram('', 'administrator', '<b>' + str(line_count_results) + '</b> ' + s_desc)

        # Create SQLite table to receive searchworks results.
        sr_file = "X004e_searchworks_results_import"
        s_sql = "CREATE TABLE " + sr_file + """
        (
        nwu_number TEXT,
        employee_name TEXT,
        national_identifier TEXT,
        ni_type TEXT,
        date_submitted TEXT,
        id_number TEXT,
        surname TEXT,
        full_name TEXT,
        date_of_birth TEXT,
        registration_number TEXT,
        company_name TEXT,
        enterprise_type TEXT,
        company_status TEXT,
        history_date TEXT,
        business_start_date TEXT,
        directorship_status TEXT,
        directorship_start_date TEXT,
        directorship_end_date TEXT,
        directorship_type TEXT,
        directorship_interest TEXT,
        nationality TEXT
        )
        """
        sqlite_cursor.execute("DROP TABLE IF EXISTS " + sr_file)
        sqlite_cursor.execute(s_sql)
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

        # Read csv Searchworks results into SQLite.
        with open(sw_path + csv_results, "r") as file:
            csv_reader = csv.DictReader(file)
            for row in csv_reader:
                sqlite_cursor.execute(
                    "INSERT INTO " + sr_file + " VALUES (:nwu_number,"
                                               " :employee_name,"
                                               " :national_identifier,"
                                               " :ni_type,"
                                               " :date_submitted,"
                                               " :idnum,"
                                               " :surname,"
                                               " :full_name,"
                                               " :dob,"
                                               " :regnum,"
                                               " :companyname,"
                                               " :enterprisetype,"
                                               " :companystatus,"
                                               " :historydate,"
                                               " :businessstartdate,"
                                               " :directorship_status,"
                                               " :directorship_startdate,"
                                               " :directorship_enddate,"
                                               " :directorship_type,"
                                               " :directorship_interest,"
                                               " :nationality)", row)

        sqlite_connection.commit()
        funcfile.writelog("%t IMPORT CSV: " + sw_path + csv_results)

        # Create SQLite table to join with people
        sr_file = "X004f_searchworks_results_people"
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            i.nwu_number,
            i.employee_name,
            i.national_identifier,
            i.ni_type,
            i.date_submitted,
            i.registration_number,
            i.company_name,
            i.enterprise_type,
            i.company_status,
            i.history_date,
            i.business_start_date,
            i.directorship_status,
            Case
                When i.directorship_start_date = '' Then i.business_start_date  
                Else i.directorship_start_date
            End As directorship_start_date,
            i.directorship_end_date,
            i.directorship_type,
            i.directorship_interest,
            p.employee_number,
            p.user_person_type,
            p.position_name,
            i.nwu_number || '-' || i.registration_number As exclude_combination,
            Date('now') As import_date
            --'2023-09-26' As import_date
        From
            X004e_searchworks_results_import i Left Join
            PEOPLE.X000_PEOPLE As p On p.employee_number = i.nwu_number
        """
        sqlite_cursor.execute("DROP TABLE IF EXISTS " + sr_file)
        sqlite_cursor.execute(s_sql)
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

        # Create or Update the history table with the new results
        sr_file = "X004g_searchworks_results_history"
        # TODO Comment the following line when results should be added to the current file
        # sqlite_cursor.execute("DROP TABLE IF EXISTS " + sr_file)
        if not funcsqlite.check_table_exists(sqlite_connection, sr_file):

            # Create SQLite history file if it does not exist
            s_sql = "CREATE TABLE IF NOT EXISTS " + sr_file + " AS " + """
            Select
                p.*
            From
                X004f_searchworks_results_people p
            """
            sqlite_cursor.execute(s_sql)
            funcfile.writelog("%t BUILD TABLE: " + sr_file)

        else:

            # Add the current results to the history

            source_table = 'X004f_searchworks_results_people'
            # Get the column names from the source table
            columns = funcsqlite.get_column_names(sqlite_cursor, source_table)
            # Create a string for the column names
            column_str = ', '.join(columns)
            # Create a string for the placeholders
            placeholder_str = ', '.join('?' * len(columns))
            # Get the data from the source table
            sqlite_cursor.execute(f"SELECT {column_str} FROM {source_table}")
            data = sqlite_cursor.fetchall()
            # Insert the data into the target table
            sqlite_cursor.executemany(f"INSERT INTO {sr_file} ({column_str}) VALUES ({placeholder_str})", data)

        # Write all the history to a csv file
        funcsqlite.sqlite_to_csv(sqlite_cursor, sr_file, sw_path + csv_history)

        # Build the final SQLite table containing all the current directorship data.
        sr_file = "X004h_searchworks_directors_all"
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            r.nwu_number,
            r.employee_name,
            r.national_identifier,
            r.user_person_type,
            r.position_name,
            Max(r.date_submitted) As date_submitted,
            r.import_date,
            r.registration_number,
            r.company_name,
            r.enterprise_type,
            r.company_status,
            r.history_date,
            r.business_start_date,
            r.directorship_status,
            r.directorship_start_date,
            r.directorship_end_date,
            r.directorship_type,
            r.directorship_interest,
            '1' As customer
        From
            X004g_searchworks_results_history r
        Where
            r.employee_number Is Not Null
        Group By
            r.national_identifier,
            r.registration_number
        Order By
            r.nwu_number,
            r.directorship_start_date        
        """
        sqlite_cursor.execute("DROP TABLE IF EXISTS " + sr_file)
        sqlite_cursor.execute(s_sql)
        sqlite_connection.commit()
        line_count_director: int = funcsqlite.table_row_count(sqlite_cursor, sr_file)
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

        # Communicate the total director count
        if l_mess:
            s_desc = "Directors"
            funcsms.send_telegram('', 'administrator', '<b>' + str(line_count_director) + '</b> ' + s_desc)

        # Read the list employees and companies to exclude from the test
        exclude_employee_company = funcstat.stat_tuple(sqlite_cursor,
                                                       "KFS.X000_Own_kfs_lookups",
                                                       "LOOKUP_CODE",
                                                       "LOOKUP='EXCLUDE EMPLOYEE COMPANY LIST'")
        if l_debug:
            print('List of employees and companies to exclude:')
            print(exclude_employee_company)
            pass

        # Build the final SQLite table containing all the current directorship data.
        sr_file = "X004x_searchworks_directors"
        s_sql = f"CREATE TABLE {sr_file} AS " + f"""
        Select
            r.nwu_number,
            r.employee_name,
            r.national_identifier,
            r.user_person_type,
            r.position_name,
            Max(r.date_submitted) As date_submitted,
            r.import_date,
            r.registration_number,
            r.company_name,
            r.enterprise_type,
            r.company_status,
            r.history_date,
            r.business_start_date,
            r.directorship_status,
            r.directorship_start_date,
            r.directorship_end_date,
            r.directorship_type,
            r.directorship_interest,
            '1' As customer
        From
            X004g_searchworks_results_history r
        Where
            r.employee_number Is Not Null and
            r.directorship_status = 'ACTIVE' and
            r.company_status Not In ('Conversion CO/CC or CC/CO') and
            r.exclude_combination Not In {exclude_employee_company}
        Group By
            r.national_identifier,
            r.registration_number
        Order By
            r.nwu_number,
            r.directorship_start_date        
        """
        sqlite_cursor.execute("DROP TABLE IF EXISTS " + sr_file)
        sqlite_cursor.execute(s_sql)
        sqlite_connection.commit()
        line_count_director: int = funcsqlite.table_row_count(sqlite_cursor, sr_file)
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

        # Communicate the total director count
        if l_mess:
            s_desc = "Active directors"
            funcsms.send_telegram('', 'administrator', '<b>' + str(line_count_director) + '</b> ' + s_desc)

        # Delete the imported Excel file.
        if not l_debug:
            os.remove(sw_path + xls_results)

        # BACKUP THE DIRECTORS TO THE NWUIA WEB APP
        try:
            A008_backup_director.ia_backup_director()
        except Exception as e:
            funcsys.ErrMessage(e, funcconf.l_mess_project, s_function, s_function)

    else:

        # NO SEARCHWORKS DIRECTORS IMPORTED

        # No new records found
        funcfile.writelog("%t NO SEARCHWORKS DIRECTORS IMPORTED")

        # Read the list employees and companies to exclude from the test
        exclude_employee_company = funcstat.stat_tuple(sqlite_cursor,
                                                       "KFS.X000_Own_kfs_lookups",
                                                       "LOOKUP_CODE",
                                                       "LOOKUP='EXCLUDE EMPLOYEE COMPANY LIST'")
        if l_debug:
            print('List of employees and companies to exclude:')
            print(exclude_employee_company)
            pass

        # Build the final SQLite table containing all the current directorship data.
        sr_file = "X004x_searchworks_directors"
        s_sql = f"CREATE TABLE {sr_file} AS " + f"""
        Select
            r.nwu_number,
            r.employee_name,
            r.national_identifier,
            r.user_person_type,
            r.position_name,
            Max(r.date_submitted) As date_submitted,
            r.import_date,
            r.registration_number,
            r.company_name,
            r.enterprise_type,
            r.company_status,
            r.history_date,
            r.business_start_date,
            r.directorship_status,
            r.directorship_start_date,
            r.directorship_end_date,
            r.directorship_type,
            r.directorship_interest,
            '1' As customer
        From
            X004g_searchworks_results_history r
        Where
            r.employee_number Is Not Null and
            r.directorship_status = 'ACTIVE' and
            r.company_status Not In ('Conversion CO/CC or CC/CO') and
            r.exclude_combination Not In {exclude_employee_company}
        Group By
            r.national_identifier,
            r.registration_number
        Order By
            r.nwu_number,
            r.directorship_start_date        
        """
        sqlite_cursor.execute("DROP TABLE IF EXISTS " + sr_file)
        sqlite_cursor.execute(s_sql)
        sqlite_connection.commit()
        line_count_director: int = funcsqlite.table_row_count(sqlite_cursor, sr_file)
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

        # Communicate the total director count
        if l_mess:
            s_desc = "Active directors"
            funcsms.send_telegram('', 'administrator', '<b>' + str(line_count_director) + '</b> ' + s_desc)

    """************************************************************************
    END OF SCRIPT
    ************************************************************************"""
    if l_debug:
        print("END OF SCRIPT")
    funcfile.writelog("END OF SCRIPT")

    # Commit the changes and close the connection
    sqlite_connection.commit()
    sqlite_connection.close()

    return


if __name__ == '__main__':
    try:
        searchworks_submit()
    except Exception as e:
        funcsys.ErrMessage(e, funcconf.l_mess_project, s_function, s_function)
