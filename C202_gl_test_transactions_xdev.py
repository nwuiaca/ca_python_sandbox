"""
SCRIPT TO TEST GL TRANSACTIONS
Created: 2 Jul 2019
Author: Albert J v Rensburg (NWU:21162395)
"""

# IMPORT PYTHON MODULES
import csv
import sqlite3

# IMPORT OWN MODULES
from _my_modules import funcconf
from _my_modules import funcfile
from _my_modules import funccsv
from _my_modules import funcdatn
from _my_modules import funcmail
from _my_modules import funcsms
from _my_modules import funcsys
from _my_modules import functest
from _my_modules import funcstat
from _my_modules import funcstr

""" INDEX **********************************************************************
ENVIRONMENT
OPEN THE DATABASES
BEGIN OF SCRIPT
END OF SCRIPT
*****************************************************************************"""


def test_transactions():
    """
    Script to test GL transactions
    :return: int
    """

    """*****************************************************************************
    ENVIRONMENT
    *****************************************************************************"""

    # DECLARE VARIABLES
    so_path = f"{funcconf.drive_data_raw}Kfs/"  # Source database path
    so_file = "Kfs_test_gl_transaction.sqlite"  # Source database
    ed_path = f"{funcconf.drive_system}_external_data/"  # External data path
    re_path = f"{funcconf.drive_data_results}Kfs/"  # Results path
    l_debug: bool = True
    l_export = False
    l_mess: bool = funcconf.l_mess_project
    # l_mess: bool = False
    l_record = True

    # OPEN THE SCRIPT LOG FILE
    if l_debug:
        print("-------------------------")
        print("C202_GL_TEST_TRANSACTIONS")
        print("-------------------------")
    funcfile.writelog("Now")
    funcfile.writelog("SCRIPT: C202_GL_TEST_TRANSACTIONS")
    funcfile.writelog("---------------------------------")

    # MESSAGE
    if l_mess:
        funcsms.send_telegram("", "administrator", "<b>C202 Kfs gl transaction tests</b>")

    """*****************************************************************************
    OPEN THE DATABASES
    *****************************************************************************"""
    if l_debug:
        print("OPEN THE DATABASES")
    funcfile.writelog("OPEN THE DATABASES")

    # OPEN THE WORKING DATABASE
    with sqlite3.connect(so_path+so_file) as sqlite_connection:
        sqlite_cursor = sqlite_connection.cursor()
    funcfile.writelog("OPEN DATABASE: " + so_file)

    # ATTACH DATA SOURCES
    sqlite_cursor.execute(f"ATTACH DATABASE '{funcconf.drive_data_raw}Kfs/Kfs.sqlite' AS 'KFS'")
    funcfile.writelog("%t ATTACH DATABASE: KFS.SQLITE")
    sqlite_cursor.execute(f"ATTACH DATABASE '{funcconf.drive_data_raw}Kfs/Kfs_curr.sqlite' AS 'KFSCURR'")
    funcfile.writelog("%t ATTACH DATABASE: KFS_CURR.SQLITE")
    sqlite_cursor.execute(f"ATTACH DATABASE '{funcconf.drive_data_raw}Kfs/Kfs_prev.sqlite' AS 'KFSPREV'")
    funcfile.writelog("%t ATTACH DATABASE: KFS_PREV.SQLITE")
    sqlite_cursor.execute(f"ATTACH DATABASE '{funcconf.drive_data_raw}Kfs/Kfs_2022.sqlite' AS 'KFS2022'")
    funcfile.writelog("%t ATTACH DATABASE: KFS_2022.SQLITE")
    sqlite_cursor.execute(f"ATTACH DATABASE '{funcconf.drive_data_raw}People/People.sqlite' AS 'PEOPLE'")
    funcfile.writelog("%t ATTACH DATABASE: PEOPLE.SQLITE")
    sqlite_cursor.execute(f"ATTACH DATABASE '{funcconf.drive_data_raw}Vss/Vss_curr.sqlite' AS 'VSSCURR'")
    funcfile.writelog("%t ATTACH DATABASE: VSS_CURR.SQLITE")
    sqlite_cursor.execute(f"ATTACH DATABASE '{funcconf.drive_data_raw}Vss/Vss_prev.sqlite' AS 'VSSPREV'")
    funcfile.writelog("%t ATTACH DATABASE: VSS_PREV.SQLITE")

    """ ****************************************************************************
    BEGIN OF SCRIPT
    *****************************************************************************"""
    if l_debug:
        print("BEGIN OF SCRIPT")
    funcfile.writelog("BEGIN OF SCRIPT")

    """ ****************************************************************************
    DONOR GL TRANSACTION IDENTIFICATION
    *****************************************************************************"""

    if l_debug:
        print("DONOR GL TRANSACTION IDENTIFICATION")
    funcfile.writelog("DONOR GL TRANSACTION IDENTIFICATION")

    # Extract gl transactions as needed
    if l_debug:
        print('Obtain the 2022 transactions')
    sr_file: str = "X003aa_donor_transaction"
    s_sql = f"CREATE TABLE {sr_file} AS " + f"""
    Select
        gl.UNIV_FISCAL_YR,
        gl.UNIV_FISCAL_PRD_CD,
        gl.CALC_COST_STRING,
        gl.ORG_NM,
        gl.ACCOUNT_NM,
        gl.FIN_OBJ_CD_NM,
        gl.TRANSACTION_DT,
        gl.FDOC_NBR,
        gl.CALC_AMOUNT,
        Upper(gl.TRN_LDGR_ENTR_DESC) As TRN_LDGR_ENTR_DESC,
        '' As GL_DESCRIPTION,
        Cast('0' As Int) As RECEIPT,
        Cast('0' As Int) As RECEIPT1,
        Cast('0' As Int) As RECEIPT2,        
        gl.ACCT_TYP_NM,
        gl.TRN_POST_DT,
        gl."TIMESTAMP",
        gl.FIN_COA_CD,
        gl.ACCOUNT_NBR,
        gl.FIN_OBJECT_CD,
        gl.FIN_BALANCE_TYP_CD,
        gl.FIN_OBJ_TYP_CD,
        gl.FDOC_TYP_CD,
        gl.FS_ORIGIN_CD,
        Upper(gl.FS_DATABASE_DESC) As FS_DATABASE_DESC,
        gl.TRN_ENTR_SEQ_NBR
    From
        KFS2022.X000_GL_trans gl
    Where
        (gl.FIN_OBJECT_CD In ('6251', '5754', '5752') And
            gl.FIN_OBJ_TYP_CD = 'IN' And
            gl.CALC_AMOUNT < 0 And
            gl.TRANSACTION_DT >= '2022-06-01') Or
        (gl.FIN_OBJECT_CD In ('7502', '7503') And
            gl.CALC_AMOUNT < 0 And
            gl.FDOC_TYP_CD = 'APP' And
            gl.TRANSACTION_DT >= '2022-06-01')
    ;"""
    sqlite_cursor.execute("DROP TABLE IF EXISTS " + sr_file)
    sqlite_cursor.execute(s_sql)
    sqlite_connection.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # Add the 2023 data
    if l_debug:
        print("Add the 2023 transactions")
    s_sql = f'''
    Insert Into {sr_file} (
        UNIV_FISCAL_YR,
        UNIV_FISCAL_PRD_CD,
        CALC_COST_STRING,
        ORG_NM,
        ACCOUNT_NM,
        FIN_OBJ_CD_NM,
        TRANSACTION_DT,
        FDOC_NBR,
        CALC_AMOUNT,
        TRN_LDGR_ENTR_DESC,
        GL_DESCRIPTION,
        RECEIPT,
        RECEIPT1,
        RECEIPT2,        
        ACCT_TYP_NM,
        TRN_POST_DT,
        "TIMESTAMP",
        FIN_COA_CD,
        ACCOUNT_NBR,
        FIN_OBJECT_CD,
        FIN_BALANCE_TYP_CD,
        FIN_OBJ_TYP_CD,
        FDOC_TYP_CD,
        FS_ORIGIN_CD,
        FS_DATABASE_DESC,
        TRN_ENTR_SEQ_NBR
    )
    Select
        gl.UNIV_FISCAL_YR,
        gl.UNIV_FISCAL_PRD_CD,
        gl.CALC_COST_STRING,
        gl.ORG_NM,
        gl.ACCOUNT_NM,
        gl.FIN_OBJ_CD_NM,
        gl.TRANSACTION_DT,
        gl.FDOC_NBR,
        gl.CALC_AMOUNT,
        Upper(gl.TRN_LDGR_ENTR_DESC) As TRN_LDGR_ENTR_DESC,
        '' As GL_DESCRIPTION,
        Cast('0' As Int) As RECEIPT,
        Cast('0' As Int) As RECEIPT1,
        Cast('0' As Int) As RECEIPT2,        
        gl.ACCT_TYP_NM,
        gl.TRN_POST_DT,
        gl."TIMESTAMP",
        gl.FIN_COA_CD,
        gl.ACCOUNT_NBR,
        gl.FIN_OBJECT_CD,
        gl.FIN_BALANCE_TYP_CD,
        gl.FIN_OBJ_TYP_CD,
        gl.FDOC_TYP_CD,
        gl.FS_ORIGIN_CD,
        Upper(gl.FS_DATABASE_DESC) As FS_DATABASE_DESC,
        gl.TRN_ENTR_SEQ_NBR
    From
        KFSPREV.X000_GL_trans gl
    Where
        (gl.FIN_OBJECT_CD In ('6251', '5754', '5752') And
            gl.FIN_OBJ_TYP_CD = 'IN' And
            gl.CALC_AMOUNT < 0) Or
        (gl.FIN_OBJECT_CD In ('7502', '7503') And
            gl.CALC_AMOUNT < 0 And
            gl.FDOC_TYP_CD = 'APP')
    '''
    sqlite_cursor.execute(s_sql)
    sqlite_connection.commit()


    # Add the 2024 data
    if l_debug:
        print("Add the 2024 transactions")
    s_sql = f'''
    Insert Into {sr_file} (
        UNIV_FISCAL_YR,
        UNIV_FISCAL_PRD_CD,
        CALC_COST_STRING,
        ORG_NM,
        ACCOUNT_NM,
        FIN_OBJ_CD_NM,
        TRANSACTION_DT,
        FDOC_NBR,
        CALC_AMOUNT,
        TRN_LDGR_ENTR_DESC,
        GL_DESCRIPTION,
        RECEIPT,
        RECEIPT1,
        RECEIPT2,        
        ACCT_TYP_NM,
        TRN_POST_DT,
        "TIMESTAMP",
        FIN_COA_CD,
        ACCOUNT_NBR,
        FIN_OBJECT_CD,
        FIN_BALANCE_TYP_CD,
        FIN_OBJ_TYP_CD,
        FDOC_TYP_CD,
        FS_ORIGIN_CD,
        FS_DATABASE_DESC,
        TRN_ENTR_SEQ_NBR
    )
    Select
        gl.UNIV_FISCAL_YR,
        gl.UNIV_FISCAL_PRD_CD,
        gl.CALC_COST_STRING,
        gl.ORG_NM,
        gl.ACCOUNT_NM,
        gl.FIN_OBJ_CD_NM,
        gl.TRANSACTION_DT,
        gl.FDOC_NBR,
        gl.CALC_AMOUNT,
        Upper(gl.TRN_LDGR_ENTR_DESC) As TRN_LDGR_ENTR_DESC,
        '' As GL_DESCRIPTION,
        Cast('0' As Int) As RECEIPT,
        Cast('0' As Int) As RECEIPT1,
        Cast('0' As Int) As RECEIPT2,        
        gl.ACCT_TYP_NM,
        gl.TRN_POST_DT,
        gl."TIMESTAMP",
        gl.FIN_COA_CD,
        gl.ACCOUNT_NBR,
        gl.FIN_OBJECT_CD,
        gl.FIN_BALANCE_TYP_CD,
        gl.FIN_OBJ_TYP_CD,
        gl.FDOC_TYP_CD,
        gl.FS_ORIGIN_CD,
        Upper(gl.FS_DATABASE_DESC) As FS_DATABASE_DESC,
        gl.TRN_ENTR_SEQ_NBR
    From
        KFSCURR.X000_GL_trans gl
    Where
        (gl.FIN_OBJECT_CD In ('6251', '5754', '5752') And
            gl.FIN_OBJ_TYP_CD = 'IN' And
            gl.CALC_AMOUNT < 0 And
            gl.TRANSACTION_DT <= '2024-05-31') Or
        (gl.FIN_OBJECT_CD In ('7502', '7503') And
            gl.CALC_AMOUNT < 0 And
            gl.FDOC_TYP_CD = 'APP' And
            gl.TRANSACTION_DT <= '2024-05-31')
    '''
    sqlite_cursor.execute(s_sql)
    sqlite_connection.commit()

    # Extract the first receipt number from the gl description
    if l_debug:
        print('Extract the first receipt number')
    s_sql = f"""
    UPDATE {sr_file}
    SET RECEIPT1 = CAST(
                        SUBSTR(TRN_LDGR_ENTR_DESC, 
                            INSTR(TRN_LDGR_ENTR_DESC, ' ') + 1, 
                            INSTR(SUBSTR(TRN_LDGR_ENTR_DESC, INSTR(TRN_LDGR_ENTR_DESC, ' ') + 1), ' ')) 
                       AS INTEGER)
    WHERE FS_ORIGIN_CD = '09'
    """
    # Execute the SQL statement
    sqlite_cursor.execute(s_sql)

    # Extract the second receipt number from the gl description
    if l_debug:
        print('Extract the second receipt number')
    s_sql = f"""
    UPDATE {sr_file}
    SET RECEIPT2 = CAST(
                        SUBSTR(TRN_LDGR_ENTR_DESC,
                               INSTR(TRN_LDGR_ENTR_DESC, '(') + 1,
                               INSTR(TRN_LDGR_ENTR_DESC, ')') - INSTR(TRN_LDGR_ENTR_DESC, '(') - 1) 
                       AS INTEGER)
    WHERE TRN_LDGR_ENTR_DESC Like ('VSS RECEIPT%')
    """
    # Execute the SQL statement
    sqlite_cursor.execute(s_sql)

    # Build one receipt number column
    if l_debug:
        print('Combine the receipt numbers')
    s_sql = f"""
    UPDATE {sr_file}
    SET RECEIPT =
    Case
     When RECEIPT1 > 0 Then RECEIPT1
     When RECEIPT2 > 0 Then RECEIPT2
     Else 0
    End
    """
    # Execute the SQL statement
    sqlite_cursor.execute(s_sql)
    sqlite_connection.commit()

    # Add receipt detail to the gl transactions
    if l_debug:
        print('Add the VSS receipt detail to the gl transactions')
    sr_file: str = "X003ab_donor_receipt"
    s_sql = "CREATE TABLE " + sr_file + " AS " + f"""
    Select
        gl.UNIV_FISCAL_YR,
        gl.UNIV_FISCAL_PRD_CD,
        gl.CALC_COST_STRING,
        gl.ORG_NM,
        gl.ACCOUNT_NM,
        gl.FIN_OBJ_CD_NM,
        gl.TRANSACTION_DT,
        gl.FDOC_NBR,
        gl.CALC_AMOUNT,
        gl.TRN_LDGR_ENTR_DESC,
        gl.GL_DESCRIPTION,
        gl.RECEIPT,
        gl.RECEIPT1,
        gl.RECEIPT2,
        gl.ACCT_TYP_NM,
        gl.TRN_POST_DT,
        gl."TIMESTAMP",
        gl.FIN_COA_CD,
        gl.ACCOUNT_NBR,
        gl.FIN_OBJECT_CD,
        gl.FIN_BALANCE_TYP_CD,
        gl.FIN_OBJ_TYP_CD,
        gl.FDOC_TYP_CD,
        gl.FS_ORIGIN_CD,
        gl.FS_DATABASE_DESC,
        gl.TRN_ENTR_SEQ_NBR,
        rc.RECEIPTDATE,
        rc.KRECEIPTID,
        Upper(rc.NAME) As NAME,
        Cast('0' As Int) As EXCLUDE,
        '' As DONOR_ID,
        gl.FDOC_NBR ||'-'|| gl.RECEIPT || Cast(gl.CALC_AMOUNT As TEXT) As UNIQUE_ID              
    From
        X003aa_donor_transaction gl Left Join
        VSSCURR.RECEIPT rc On rc.RECEIPTNO = gl.RECEIPT
            And Date(rc.RECEIPTDATE) = Date(gl.TRANSACTION_DT)        
    ;"""
    sqlite_cursor.execute("DROP TABLE IF EXISTS " + sr_file)
    sqlite_cursor.execute(s_sql)
    sqlite_connection.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # Read the list of words to exclude in the vendor names
    words_to_remove = funcstat.stat_list(sqlite_cursor,
                                           "KFS.X000_Own_kfs_lookups",
                                           "LOOKUP_CODE",
                                           "LOOKUP='EXCLUDE GL DONOR WORD'")

    if l_debug:
        print("WORDS TO EXCLUDE:")
        print(words_to_remove)
        pass

    # CLEAN THE GL DESCRIPTION COLUMN

    # Select the dirty data from the table
    sqlite_cursor.execute(f"Select rowid, TRN_LDGR_ENTR_DESC From {sr_file}")
    rows = sqlite_cursor.fetchall()

    # Clean the data and update the clean_data column
    for row in rows:
        row_id = row[0]
        dirty_data = row[1]
        clean_data = funcstr.clean_paragraph(dirty_data, words_to_remove, 'a')

        # Update the clean_data column with the cleaned data
        sqlite_cursor.execute(f"UPDATE {sr_file} SET GL_DESCRIPTION = ? WHERE rowid = ?", (clean_data, row_id))

    # Commit the changes and close the connection
    sqlite_connection.commit()

    # IDENTIFY DONORS

    # Read the list of donors to identify
    records_to_identify = funcstat.stat_list_reverse(sqlite_cursor,
                                             "KFS.X000_Own_kfs_lookups",
                                             "LOOKUP_CODE",
                                             "LOOKUP='IDENTIFY GL DONOR RECORD'")

    if l_debug:
        print("DONORS TO INCLUDE:")
        print(records_to_identify)
        pass

    # Construct the SQL query with NOT LIKE for each word
    query_parts = [f'GL_DESCRIPTION LIKE "%{word}%"' for word in records_to_identify]
    inclusion_criteria = ' Or '.join(query_parts)

    # Query to fetch all the descriptions from the general ledger
    s_sql = f"SELECT rowid, GL_DESCRIPTION FROM {sr_file} WHERE {inclusion_criteria}"
    sqlite_cursor.execute(s_sql)

    # Fetch all the rows in the general ledger
    rows = sqlite_cursor.fetchall()

    # Iterate over each row and check if any company name is contained in the description
    # matches = []
    for row in rows:
        record_id = row[0]
        gl_description = row[1]
        for company_name in records_to_identify:
            if company_name.upper() in gl_description:  # Case-insensitive match
                if l_debug:
                    # print(company_name)
                    # print(gl_description)
                    pass
                # matches.append((row[0], company_name))
                # Update the clean_data column with the cleaned data
                sqlite_cursor.execute(f"UPDATE {sr_file} SET EXCLUDE = 1, DONOR_ID = ? WHERE rowid = ?", (company_name, record_id))
                break  # Stop looking for other company names if a match is found

    """
    if l_debug:
        # print(matches)
        pass
    """

    # Commit the changes and close the connection
    sqlite_connection.commit()

    # EXCLUDE DONORS

    # Read the list of words to exclude in the vendor names
    records_to_remove = funcstat.stat_list_reverse(sqlite_cursor,
                                           "KFS.X000_Own_kfs_lookups",
                                           "LOOKUP_CODE",
                                           "LOOKUP='EXCLUDE GL DONOR RECORD'")

    if l_debug:
        print("WORDS TO EXCLUDE:")
        print(records_to_remove)
        pass

    # Construct the SQL query with NOT LIKE for each word
    query_parts = [f'GL_DESCRIPTION LIKE "%{word}%"' for word in records_to_remove]
    # Test on receipt name too
    # query_parts = [f'GL_DESCRIPTION LIKE "%{word}%" Or NAME LIKE "%{word}%"' for word in records_to_remove]
    exclusion_criteria = ' Or '.join(query_parts)

    if l_debug:
        print("EXCLUSION CRITERIA:")
        print(exclusion_criteria)
        pass

    # Update the table with excluded records
    if l_debug:
        print('Mark records for exclusion')
    s_sql = f"""
    UPDATE {sr_file}
    SET EXCLUDE = 
    Case
        When {exclusion_criteria} Then 2
        Else 0
    End
    Where
        EXCLUDE = 0
    """
    # Execute the SQL statement
    sqlite_cursor.execute(s_sql)
    sqlite_connection.commit()

    # Extract the identified donors

    # Add receipt detail to the gl transactions
    if l_debug:
        print('Add the VSS receipt detail to the gl transactions')
    sr_file: str = "X003ac_donor_all_transaction"
    sqlite_cursor.execute("DROP TABLE IF EXISTS " + sr_file)
    sr_file: str = "X003ax_donor_all_transaction"
    s_sql = "CREATE TABLE " + sr_file + " AS " + f"""
    Select
        don.FDOC_NBR,
        don.UNIV_FISCAL_YR As YEAR,
        don.UNIV_FISCAL_PRD_CD As MONTH,
        don.TRANSACTION_DT As DATE,
        don.CALC_AMOUNT As AMOUNT,
        don.TRN_LDGR_ENTR_DESC As DESCRIPTION_RAW,
        don.GL_DESCRIPTION As DESCRIPTION_CLEANED,
        don."EXCLUDE" As STATUS_ID,
        Case
            When don."EXCLUDE" = 1 Then "DONOR IDENTIFIED"
            When don."EXCLUDE" = 2 Then "EXCLUDED FROM DONOR IDENTIFICATION"
            Else "DONOR TO BE IDENTIFIED IN GL TRANSACTION"
        END As STATUS,
        don.DONOR_ID,
        own.LOOKUP_DESCRIPTION As DONOR,
        don.CALC_COST_STRING As COST_STRING,
        don.ORG_NM As ORG_NAME,
        don.ACCOUNT_NM As ACCOUNT_NAME,
        don.FIN_OBJ_CD_NM As OBJECT_NAME,
        don.ACCT_TYP_NM As ACCOUNT_TYPE_NAME,
        don.TRN_POST_DT As DATE_POSTED,
        don."TIMESTAMP" As TIME_POSTED,
        don.FIN_BALANCE_TYP_CD,
        don.FIN_OBJ_TYP_CD,
        don.FDOC_TYP_CD,
        don.FS_ORIGIN_CD,
        don.FS_DATABASE_DESC,
        don.RECEIPT,
        don.KRECEIPTID,
        don.RECEIPTDATE,
        don.NAME
    From
        X003ab_donor_receipt don Left Join
        X000_Own_kfs_lookups own On own.LOOKUP_CODE = don.DONOR_ID And own.LOOKUP = "IDENTIFY GL DONOR RECORD"
    Order By
        don.TRANSACTION_DT,
        don.FDOC_NBR
    ;"""
    sqlite_cursor.execute("DROP TABLE IF EXISTS " + sr_file)
    sqlite_cursor.execute(s_sql)
    sqlite_connection.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    """ ****************************************************************************
    END OF SCRIPT
    *****************************************************************************"""
    if l_debug:
        print("END OF SCRIPT")
    funcfile.writelog("END OF SCRIPT")

    # CLOSE THE DATABASE CONNECTION
    sqlite_connection.commit()
    sqlite_connection.close()

    # CLOSE THE LOG WRITER
    funcfile.writelog("------------------------------------")
    funcfile.writelog("COMPLETED: C202_GL_TEST_TRANSACTIONS")

    return


if __name__ == '__main__':
    try:
        test_transactions()
    except Exception as e:
        funcsys.ErrMessage(e, funcconf.l_mess_project, "C202_gl_test_transactions", "C202_gl_test_transactions")
