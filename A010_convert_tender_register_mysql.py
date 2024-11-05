"""
SCRIPT TO IMPORT TENDER REGISTERS TO MYSQL
Script: A010_convert_tender_register_mysql.py
Author: Albert B Janse van Rensburg (NWU:21162395)
Created: 4 Sep 2024
"""

# IMPORT PYTHON MODULES
import os
import pandas as pd

# IMPORT OWN MODULES
from _my_modules import funcconf
from _my_modules import funcdatn
from _my_modules import funcsys
from _my_modules import funcconf
from _my_modules import funcfile
from _my_modules import funcmysql
from _my_modules import funcsms
from _my_modules import funcstr

# INDEX OF FUNCTIONS
"""
ia_mysql_import_tender = Function to import the tender register
ENVIRONMENT
BEGIN OF SCRIPT
OPEN THE DATABASES
"""

# SCRIPT WIDE VARIABLES
s_function: str = "A010 IMPORT TENDER REGISTER"


def ia_mysql_import_tender(s_year: str = "2024", s_target_database: str = "Web_nwu_ia"):
    """
    Script to import tender register from excel to mysql
    :param s_year: str: The tender register year
    :param s_target_database: str: The MYSQL database to export data to
    :return: Nothing
    """

    """*****************************************************************************
    ENVIRONMENT
    *****************************************************************************"""

    # FUNCTION WIDE VARIABLES
    l_return: bool = True
    l_debug: bool = True  # Display debug messages
    # l_mess: bool = funcconf.l_mess_project  # Send messages
    l_mess: bool = False  # Send messages
    # l_mail: bool = funcconf.l_mail_project
    l_mail: bool = False
    # so_path: str = f"{funcconf.drive_data_raw}Internal_audit/"
    # so_file: str = "Web_ia_nwu.sqlite"
    target_table: str = "ia_tender"

    # IF SOURCE OR TARGET EMPTY RETURN FALSE AND DO NOTHING

    s_target_schema: str = ""
    if s_target_database == "Web_nwu_ia":
        s_target_schema = "nwuiaeapciy_db1"
    elif s_target_database == "Web_ia_nwu":
        s_target_schema = "Ia_nwu"
    elif s_target_database == "Web_ia_joomla":
        s_target_schema = "Ia_joomla"
    elif s_target_database == "Mysql_ia_server":
        s_target_schema = "nwuiaca"
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

        # OPEN THE TENDER REGISTER
        if l_debug:
            print("OPEN THE TENDER REGISTER")
        so_path = f"Q:/Tenders {s_year}/"  # Source database path
        so_file = "Tender register merge spreadsheet.xlsx"  # Source database

        if os.path.isfile(so_path + so_file):

            # Read the Excel file.
            excel_file = pd.read_excel(so_path + so_file, sheet_name='Tender')
            funcfile.writelog("%t IMPORT EXCEL: " + so_path + so_file)

            # Count the number of records
            line_count_results: int = len(excel_file)

            # OPEN THE MYSQL TARGET DATABASE
            if l_debug:
                print("Open mysql target database...")
            funcfile.writelog("%t OPEN MYSQL TARGET DATABASE: " + s_target_database)
            ms_to_connection = funcmysql.mysql_open(s_target_database)
            ms_to_cursor = ms_to_connection.cursor()
            funcfile.writelog("%t OPEN MYSQL TARGET DATABASE: " + s_target_database)

            # Delete all current data in the table
            if s_year == "2020":
                delete_query = f"TRUNCATE TABLE ia_tender"  # or "DELETE FROM your_table_name"
                ms_to_cursor.execute(delete_query)
                print("Existing records deleted successfully.")

            # Iterate over the DataFrame rows
            for index, row in excel_file.iterrows():

                if l_debug:
                    print(row['Tender'])

                # Calculate conversion values

                # Unique has value
                hash_value = funcstr.generate_md5_hash(row['Tender'] + str(index))

                # Tender sequence number
                sequence_number = funcstr.clean_paragraph(row['Tender'], [s_year], 'n')
                sequence_number_int = int(sequence_number)

                # Standardised tender description
                convert_description = row['Description'].title()
                convert_description = convert_description.replace(' At ', ' at ')
                convert_description = convert_description.replace(' Of ', ' of ')
                convert_description = convert_description.replace(' An ', ' an ')
                convert_description = convert_description.replace(' In ', ' in ')
                convert_description = convert_description.replace(' To ', ' to ')
                convert_description = convert_description.replace(' And ', ' and ')
                convert_description = convert_description.replace(' For ', ' for ')
                convert_description = convert_description.replace(' The ', ' the ')
                utf8_bytes = convert_description.encode('utf-8')
                decoded_string = utf8_bytes.decode('utf-8')

                # Tender site
                if row['Campus'][0] == 'M':
                    site_code = 6
                elif row['Campus'][0] == 'P':
                    site_code = 7
                elif row['Campus'][0] == 'V':
                    site_code = 8
                else:
                    site_code = 5

                # Date opened
                if s_year < '2024':
                    date_opened = row['Date closing']
                elif row['Date closing'].strftime("%Y-%m-%d") < funcdatn.get_today_date():
                    date_opened = row['Date closing']
                else:
                    date_opened = row['Date opened']

                # date_opened = row['Date opened']

                # Tender category
                if row['Type'] == 'Closed and invited':
                    category_code = 13
                    # Priority status
                    # Category status
                    if s_year < '2024':
                        priority_status = 9
                        category_status = 103
                    elif row['Date closing'].strftime("%Y-%m-%d") < funcdatn.get_today_date():
                        priority_status = 9
                        category_status = 103
                    else:
                        priority_status = 0
                        category_status = 102
                elif row['Type'] == 'Open invite in media':
                    category_code = 14
                    # Priority status
                    # Category status
                    if s_year < '2024':
                        priority_status = 9
                        category_status = 105
                    elif row['Date closing'].strftime("%Y-%m-%d") < funcdatn.get_today_date():
                        priority_status = 9
                        category_status = 105
                    else:
                        priority_status = 0
                        category_status = 104
                elif row['Type'] == 'Tender on behalf invited':
                    category_code = 15
                    # Priority status
                    # Category status
                    if s_year < '2024':
                        priority_status = 9
                        category_status = 109
                    elif row['Date closing'].strftime("%Y-%m-%d") < funcdatn.get_today_date():
                        priority_status = 9
                        category_status = 109
                    else:
                        priority_status = 0
                        category_status = 108
                elif row['Type'] == 'Tender on behalf advertisement':
                    category_code = 16
                    # Priority status
                    # Category status
                    if s_year < '2024':
                        priority_status = 9
                        category_status = 107
                        date_opened = row['Date closing']
                    elif row['Date closing'].strftime("%Y-%m-%d") < funcdatn.get_today_date():
                        priority_status = 9
                        category_status = 107
                    else:
                        priority_status = 0
                        category_status = 106
                else:
                    category_code = 18
                    # Priority status
                    # Category status
                    if s_year < '2024':
                        priority_status = 9
                        category_status = 111
                    elif row['Date closing'].strftime("%Y-%m-%d") < funcdatn.get_today_date():
                        priority_status = 9
                        category_status = 111
                    else:
                        priority_status = 0
                        category_status = 110

                # Tender owner
                owner_name = row['OwnerName']
                if type(owner_name) is str:
                    owner_name = owner_name.upper()
                else:
                    owner_name = ''

                # Audit name
                audit_name = row['AudiName']
                if type(audit_name) is str:
                    audit_name = audit_name.upper()
                else:
                    audit_name = ''

                # Tender officer
                user_name = row['ReprName']
                if type(user_name) is str:
                    user_name = user_name.upper()
                else:
                    user_name = ''

                if 'ETIENNE' in user_name:
                    user_id = 906
                elif 'RIAAN' in user_name:
                    user_id = 907
                elif 'FRIKKIE' in user_name:
                    user_id = 908
                elif 'HELEEN' in user_name:
                    user_id = 909
                elif 'MABASA' in user_name:
                    user_id = 910
                elif 'SIYANDA' in user_name:
                    user_id = 911
                elif 'LWANDILE' in user_name:
                    user_id = 912
                elif 'NKANYISO' in user_name:
                    user_id = 913
                elif 'PHETHANI' in user_name:
                    user_id = 914
                elif 'TEBOGO' in user_name:
                    user_id = 915
                elif 'COLLEEN' in user_name:
                    user_id = 916
                else:
                    user_id = 0

                # Assuming your table has columns: col1, col2, col3
                sql_query = """
                INSERT INTO ia_tender (
                ia_tend_token,
                ia_user_sysid,
                ia_tend_customer,
                ia_tend_status,
                ia_assisite_auto,
                ia_assicate_auto,
                ia_assistat_auto,
                ia_tend_year,
                ia_tend_sequence,
                ia_tend_number,
                ia_tend_description,
                ia_tend_owner_name,
                ia_tend_representative_name,
                ia_tend_auditor_name,
                ia_tend_datesubmit,
                ia_tend_dateclose,
                ia_tend_dateopen,
                ia_tend_dateexpect,
                ia_tend_valueexpect,
                ia_tend_division
                ) VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                )"""
                try:
                    ms_to_cursor.execute(sql_query, (
                        hash_value,
                        user_id,
                        32,
                        priority_status,
                        site_code,
                        category_code,
                        category_status,
                        s_year,
                        sequence_number_int,
                        row['Tender'],
                        decoded_string,
                        owner_name,
                        user_name,
                        audit_name,
                        row['Date registered'],
                        row['Date closing'],
                        date_opened,
                        row['Date expected'],
                        row['Expected value'],
                        row['DiviName']
                    ))
                except:
                    print('error')
                finally:
                    print('done')

            # Commit the transaction
            ms_to_connection.commit()

            print("Records inserted successfully.")

            # Communicate the total submission count
            if l_debug:
                print('Results imported:')
                print(line_count_results)
            if l_mess:
                s_desc = "Results imported"
                funcsms.send_telegram('', 'administrator', '<b>' + str(line_count_results) + '</b> ' + s_desc)

        # MESSAGE
        if l_mess:
            funcsms.send_telegram("", "administrator", "<b>" + str(i_table_counter) + "</b> tables backup")
            funcsms.send_telegram("", "administrator", "<b>" + str(i_record_counter) + "</b> records backup")

        """************************************************************************
        END OF SCRIPT
        ************************************************************************"""
        funcfile.writelog("END OF SCRIPT")
        if l_debug:
            print("END OF SCRIPT")

    return l_return


if __name__ == '__main__':
    try:
        ia_mysql_import_tender('2020')
        ia_mysql_import_tender('2021')
        ia_mysql_import_tender('2022')
        ia_mysql_import_tender('2023')
        ia_mysql_import_tender('2024')
    except Exception as e:
        funcsys.ErrMessage(e)
