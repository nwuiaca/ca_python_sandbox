"""
Script to test PEOPLE conflict of interest
Created on: 8 Apr 2019
Modified on: 18 May 2021
Author: Albert J v Rensburg (NWU21162395)
"""

# IMPORT PYTHON MODULES
import csv
import sqlite3
from fuzzywuzzy import fuzz

# IMPORT OWN MODULES
from _my_modules import funcconf
from _my_modules import funccsv
from _my_modules import funcdatn
from _my_modules import funcfile
from _my_modules import funcsms
from _my_modules import funcsys
from _my_modules import functest
from _my_modules import funcstr

# INDEX
"""
ENVIRONMENT
OPEN THE DATABASES
BEGIN OF SCRIPT
END OF SCRIPT
"""

"""*****************************************************************************
ENVIRONMENT
*****************************************************************************"""

# DECLARE VARIABLES
so_path: str = f"{funcconf.drive_data_raw}People_conflict/"  # Source database path
re_path: str = f"{funcconf.drive_data_results}Kfs/" + funcdatn.get_current_year() + "/"  # Results path
ed_path: str = f"{funcconf.drive_system}_external_data/"  # external data path
so_file: str = "People_conflict.sqlite"  # Source database
s_sql: str = ""  # SQL statements
l_debug: bool = True
l_export: bool = False
l_mess: bool = False
l_mail: bool = False
l_record: bool = False

# OPEN THE SCRIPT LOG FILE
if l_debug:
    print("-----------------------------")
    print("C002_PEOPLE_TEST_CONFLICT_DEV")
    print("-----------------------------")
funcfile.writelog("Now")
funcfile.writelog("SCRIPT: C002_PEOPLE_TEST_CONFLICT_DEV")
funcfile.writelog("-------------------------------------")

if l_mess:
    funcsms.send_telegram('', 'administrator', 'Testing employee <b>conflict of interest</b>.')

"""*****************************************************************************
OPEN THE DATABASES
*****************************************************************************"""
if l_debug:
    print("OPEN THE DATABASES")
funcfile.writelog("OPEN THE DATABASES")

# OPEN THE WORKING DATABASE
with sqlite3.connect(so_path+so_file) as so_conn:
    so_curs = so_conn.cursor()
funcfile.writelog("OPEN DATABASE: " + so_file)

# ATTACH DATA SOURCES
so_curs.execute(f"ATTACH DATABASE '{funcconf.drive_data_raw}People/People.sqlite' AS 'PEOPLE'")
funcfile.writelog("%t ATTACH DATABASE: PEOPLE.SQLITE")
so_curs.execute(f"ATTACH DATABASE '{funcconf.drive_data_raw}People_payroll/People_payroll.sqlite' AS 'PAYROLL'")
funcfile.writelog("%t ATTACH DATABASE: PAYROLL.SQLITE")
so_curs.execute(f"ATTACH DATABASE '{funcconf.drive_data_raw}Kfs/Kfs.sqlite' AS 'KFS'")
funcfile.writelog("%t ATTACH DATABASE: KFS.SQLITE")
so_curs.execute(f"ATTACH DATABASE '{funcconf.drive_data_raw}Kfs/Kfs_curr.sqlite' AS 'KFSCURR'")
funcfile.writelog("%t ATTACH DATABASE: KFS_CURR.SQLITE")
so_curs.execute(f"ATTACH DATABASE '{funcconf.drive_data_raw}Vss/Vss_curr.sqlite' AS 'VSSCURR'")
funcfile.writelog("%t ATTACH DATABASE: VSS_CURR.SQLITE")

"""*****************************************************************************
BEGIN OF SCRIPT
*****************************************************************************"""
if l_debug:
    print("BEGIN OF SCRIPT")
funcfile.writelog("BEGIN OF SCRIPT")

"""*****************************************************************************
TEST CONFLICTING TRANSACTIONS
*****************************************************************************"""

"""
Test if employees declared conflict of interest.
    Request remediation from employee supervisor as per declaration.
Test exclude:
Created: 21 May 2021 (Albert J v Rensburg NWU:21162395)
"""

""" INDEX
BUILD A CSV FILE WITH CLEANED UP VENDOR NAMES AND REGISTRATION NUMBERS
VENDOR NAME AND REGISTRATION NUMBER COMPARISON METHOD 1 USING THE LEVENSHTEIN METHODS
VENDOR NAME AND REGISTRATION NUMBER COMPARISON METHOD 2 USING NORMAL COMPARISON METHODS
"""

# TABLES NEEDED

# DECLARE TEST VARIABLES
i_finding_after: int = 0
s_description = "Employee conflict transaction"
s_file_name: str = "employee_conflict_transaction"
s_file_prefix: str = "X200a"
s_finding: str = "EMPLOYEE CONFLICT TRANSACTION"
s_report_file: str = "002_reported.txt"

# OBTAIN TEST RUN FLAG
if functest.get_test_flag(so_curs, "HR", "TEST " + s_finding, "RUN") == "FALSE":

    if l_debug:
        print('TEST DISABLED')
    funcfile.writelog("TEST " + s_finding + " DISABLED")

else:

    # OPEN LOG
    if l_debug:
        print("TEST " + s_finding)
    funcfile.writelog("TEST " + s_finding)

    # BUILD A CSV FILE WITH CLEANED UP VENDOR NAMES AND REGISTRATION NUMBERS

    # Read the list of words to exclude in the vendor names
    # Create an empty list to store the lookup codes
    words_to_remove = []
    # Read the csv file
    file_path = ed_path + '001_own_kfs_lookups.csv'
    lookup_column_name = 'LOOKUP'
    lookup_code_column_name = 'LOOKUP_CODE'
    with open(file_path, 'r') as file:
        # Create a csv reader object
        csv_reader = csv.DictReader(file)
        # Iterate over each row in the csv file
        for row in csv_reader:
            # Filter rows based on the LOOKUP column value
            if row[lookup_column_name] == 'EXCLUDE VENDOR WORD':
                # Append the LOOKUP_CODE value to the lookup_codes list
                words_to_remove.append(row[lookup_code_column_name])
    # Print the lookup codes
    if l_debug:
        print(words_to_remove)
    # Build a list of cleaned vendor names and registration numbers
    # Execute the SQL query to fetch the data
    so_curs.execute('Select p.VENDOR_ID, p.PAYEE_NAME, p.REG_NO '
                    'From KFSCURR.X002aa_Report_payments_summary p '
                    'Where p.VENDOR_TYPE In ("PO", "DV")')
    # Fetch all the rows returned by the query
    rows = so_curs.fetchall()
    # Modify the data before writing to CSV
    modified_rows = []
    for row in rows:
        payee_id, payee_name, vendor_reg_nr = row
        # Clean the vendor name
        modified_payee_name = funcstr.clean_paragraph(payee_name, words_to_remove, 'b')
        # Clean the vendor registration number
        modified_regno = funcstr.clean_paragraph(vendor_reg_nr, words_to_remove, 'n')
        modified_rows.append((payee_id, modified_payee_name, modified_regno))
    # Write the data into the CSV file
    output_file = re_path + 'X200a_vendors_cleaned.csv'
    with open(output_file, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["vendor_id", "vendor_name", "vendor_regno"])  # Write column headers
        writer.writerows(modified_rows)  # Write the data rows

    # Create SQLite table to receive cleaned vendors
    sr_file: str = s_file_prefix + "a_" + s_file_name
    s_sql = "CREATE TABLE " + sr_file + """
    (
    vendor_id TEXT,
    vendor_name TEXT,
    vendor_regno TEXT
    )
    """
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # Read csv data previously submitted and populate SQLite table.
    with open(output_file, "r") as file:
        csv_reader = csv.DictReader(file)
        for row in csv_reader:
            so_curs.execute(
                "INSERT INTO " + sr_file + " VALUES (:vendor_id,"
                                           " :vendor_name,"
                                           " :vendor_regno)",
                row)
    so_conn.commit()
    funcfile.writelog("%t IMPORT CSV: " + output_file)

    # VENDOR NAME AND REGISTRATION NUMBER COMPARISON METHOD 1 USING THE LEVENSHTEIN METHODS

    '''
    To join tables with potentially different values in the vendor_name field, you can use string similarity algorithms 
    to match similar values. One widely used algorithm for this purpose is the Levenshtein distance. 
    1. Ratio:
       - The `fuzz.ratio()` method calculates the simple ratio between two strings. It compares the strings character
        by character and returns a similarity score as a percentage, ranging from 0 to 100. The ratio is calculated
         using the formula: `(2 * matches) / (length of string1 + length of string2)`. This method is useful for basic
          string comparison.
    2. Partial Ratio:
       - The `fuzz.partial_ratio()` method computes the ratio between the best matching substring of two strings.
        It is helpful when you need to match partial strings or when the order of characters is different in the two
         strings being compared. It returns a similarity score based on the ratio of the size of the matching substring
          to the size of the longer string.
    3. Token Sort Ratio:
       - The `fuzz.token_sort_ratio()` method compares two strings by tokenizing them into words and sorting them
        alphabetically. It then calculates the ratio between the sorted token lists of the two strings.
         This method can be useful when the strings need to be matched irrespective of their word order.
    4. Partial Token Sort Ratio:
       - The `fuzz.partial_token_sort_ratio()` method is a combination of the `partial_ratio()` and
        `token_sort_ratio()` methods. It tokenizes the strings and sorts the tokens, but only compares the best
         matching tokens from the sorted lists. It then calculates the ratio based on the best matching token subsets
          of the two strings.
    '''

    # Fetch data from table1
    so_curs.execute("""Select
                            d.nwu_number,
                            d.company_name,
                            substr(d.registration_number,1,4)||substr(d.registration_number,6,6)
                        From
                            X004x_searchworks_directors d""")
    table1_data = so_curs.fetchall()

    # Fetch data from table2
    so_curs.execute("""Select
                            v.vendor_id,
                            v.vendor_name,
                            substr(v.vendor_regno,1,10)
                        From
                            X200aa_employee_conflict_transaction v""")
    table2_data = so_curs.fetchall()

    # Prepare a list to store the results
    results = []

    # Iterate over each vendor in table1
    for record_t1 in table1_data:
        vendor_best_match: str = ''
        vendor_best_similarity_ratio = 0
        regno_best_match: str = ''
        regno_best_similarity_ratio = 0

        # Compare the vendor from table1 with each vendor in table2
        new_vendor_id = ''
        new_regno = ''
        for record_t2 in table2_data:

            # Fuzz methods to match vendor name
            vendor_similarity_ratio = fuzz.token_set_ratio(record_t1[1], record_t2[1])
            # Update the best match if the similarity ratio is higher
            if vendor_similarity_ratio >= vendor_best_similarity_ratio:
                vendor_best_similarity_ratio = vendor_similarity_ratio
                vendor_best_match = record_t2[1]
                new_vendor_id = record_t2[0]

            # Fuzz methods to match vendor registration number
            regno_similarity_ratio = fuzz.ratio(record_t1[2], record_t2[2])
            # Update the best match if the similarity ratio is higher
            if regno_similarity_ratio > regno_best_similarity_ratio:
                regno_best_similarity_ratio = regno_similarity_ratio
                regno_best_match = record_t2[2]
                new_regno = record_t1[2]

            # Ascii sum method
            '''
            record1 = record_t1[1]
            record2 = record_t2[1]
            i_sum1 = sum(ord(char) for char in record1)
            i_sum2 = sum(ord(char) for char in record2)
            vendor_similarity_ratio = int((i_sum1/i_sum2)*100)
            '''

        # Store the result in the list
        results.append({
            'nwu_number': record_t1[0],
            'company_name': record_t1[1],
            'vendor_id': new_vendor_id,
            'vendor_name': vendor_best_match,
            'vendor_ratio': vendor_best_similarity_ratio,
            'regno_director': new_regno,
            'regno_vendor': regno_best_match,
            'regno_ratio': regno_best_similarity_ratio
        })

    if l_debug:
        print(results)

    # Create SQLite table to receive previously submitted employees.
    sr_file: str = s_file_prefix + "b_" + s_file_name
    s_sql = "CREATE TABLE " + sr_file + """
    (
    nwu_number TEXT,
    company_name TEXT,
    vendor_id TEXT,
    vendor_name TEXT,
    vendor_ratio INT,
    regno_director TEXT,
    regno_vendor TEXT,
    regno_ratio INT
    )
    """
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # Insert results into the table
    for result in results:
        so_curs.execute('INSERT INTO ' + sr_file + ' (nwu_number, company_name, vendor_id, vendor_name, vendor_ratio, regno_director, regno_vendor, regno_ratio) VALUES (?, ?, ?, ?, ?, ?, ?, ?)', (result['nwu_number'], result['company_name'], result['vendor_id'], result['vendor_name'], result['vendor_ratio'], result['regno_director'], result['regno_vendor'], result['regno_ratio']))
        so_conn.commit()

    # VENDOR NAME AND REGISTRATION NUMBER COMPARISON METHOD 2 USING NORMAL COMPARISON METHODS

    # BUILD DECLARATIONS MASTER TABLE
    print("Build declarations master table...")
    sr_file: str = s_file_prefix + "c_" + s_file_name
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        d.nwu_number,
        d.company_name As company_name,
        v.vendor_id,
        v.vendor_name,
        Case
          When SubStr(d.registration_number, 1, 4) || SubStr(d.registration_number, 6, 6) = SubStr(v.vendor_regno, 1, 10)
          Then 0
          When d.company_name Like (v.vendor_name || '%')
          Then 2
          Else 1
        End As vendor_ratio,
        SubStr(d.registration_number, 1, 4) || SubStr(d.registration_number, 6, 6) As regno_director,
        SubStr(v.vendor_regno, 1, 10) As regno_vendor,
        Case
          When SubStr(d.registration_number, 1, 4) || SubStr(d.registration_number, 6, 6) = SubStr(v.vendor_regno, 1, 10)
          Then 1
          Else 0
        End As regno_ratio
    From
        X004x_searchworks_directors d,
        X200aa_employee_conflict_transaction v
    Where
        (SubStr(d.registration_number, 1, 4) || SubStr(d.registration_number, 6, 6) = SubStr(v.vendor_regno, 1, 10)) Or
        (d.company_name Like (v.vendor_name || '%')) Or
        (v.vendor_name Like (d.company_name || '%'))    
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD EITHER COMPANY MASTER FILE OR TRANSACTION MASTER FILE ???

"""*****************************************************************************
END OF SCRIPT
*****************************************************************************"""
if l_debug:
    print("END OF SCRIPT")
funcfile.writelog("END OF SCRIPT")

# CLOSE THE DATABASE CONNECTION
so_conn.close()

# CLOSE THE LOG WRITER
funcfile.writelog("----------------------------------------")
funcfile.writelog("COMPLETED: C002_PEOPLE_TEST_CONFLICT_DEV")

"""*****************************************************************************
SAMPLE CODE
*****************************************************************************"""

'''
Words Yolandie identified to be removed for vendor name comparisons 20231109
["LTD", "(PTY)", "(CRED)", "(DV)", "AD HOC NO PO","*", "(SOLE)","ADHOC", "(Pty)", "Ltd", "BPK", "Bpk", "EDMS", "Edms", " CC", " cc", "T/A","NO PO", "()", "AD HOC", " SOC", " CR ", "BIB"]
'''

'''
# SCRIPT TO BUILD A CSV OF WORDS THAT OCCUR IN A SQLITE TABLE COLUMNS LIKE VENDOR NAME
# Execute the SQL query to fetch the ID column from the VENDOR table
so_curs.execute('Select v.VNDR_NM From KFS.X000_Vendor v Where v.VNDR_TYP_CD In ("PO", "DV") And v.DOBJ_MAINT_CD_ACTV_IND = "Y"')
so_curs.execute('Select p.PAYEE_ID, p.PAYEE_NAME, p.VENDOR_REG_NR From KFSCURR.X001aa_Report_payments p Where p.VENDOR_TYPE_CALC In ("PO", "DV")')
# Fetch all the rows returned by the query
rows = so_curs.fetchall()
# Open the csv file in write mode
with open('words.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    for row in rows:
        word_list = funcstr.build_word_list(row[0])
        print(word_list)
        # Write each word in a new line
        for word in word_list:
            writer.writerow([word])
'''
