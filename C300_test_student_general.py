""" C300_TEST_STUDENT_GENERAL **************************************************
*** Script to vss student general items
*** Albert J van Rensburg (21162395)
*** 25 Jun 2018
*****************************************************************************"""

# Import python module
import csv
import sqlite3

# Import own modules
from _my_modules import funcconf
from _my_modules import funccsv
from _my_modules import funcdatn
from _my_modules import funcfile
from _my_modules import funcmail
from _my_modules import funcsms
from _my_modules import funcsys

"""*************************************************************************
***
*** CREATE STUDENT ID NUMBER LISTS
***
*** Import vss transactions from VSS.SQLITE
*** Import vss party data from from VSS.SQLITE
*** Import previously reported ID numbers
*** Join the vss tran and party data
*** Join the previously reported id number list
***   Add prev reported columns
*** Export the ytd ID list to report
*** Export the current ID list to report
*** Add new ID number list to prev reported
***
*************************************************************************"""


def test_student_general():
    """
    SCRIPT TO TEST STUDENT MASTER FILE
    :return: Nothing
    """

    """*****************************************************************************
    ENVIRONMENT
    *****************************************************************************"""

    # Declare variables
    so_path = f"{funcconf.drive_data_raw}Vss_general/"  # Source database path
    re_path = f"{funcconf.drive_data_results}Vss/"  # Results
    ed_path = f"{funcconf.drive_system}_external_data/"
    so_file = "Vss_general.sqlite"  # Source database
    s_sql = ""  # SQL statements
    l_mail = True
    l_export = True

    # OPEN THE LOG
    funcfile.writelog("Now")
    funcfile.writelog("SCRIPT: C300_TEST_STUDENT_GENERAL")
    funcfile.writelog("---------------------------------")
    print("-------------------------")
    print("C300_TEST_STUDENT_GENERAL")
    print("-------------------------")

    # MESSAGE
    if funcconf.l_mess_project:
        funcsms.send_telegram("", "administrator", "<b>C300 Student master file tests</b>")

    """*****************************************************************************
    OPEN THE DATABASES
    *****************************************************************************"""
    print("OPEN THE DATABASES")
    funcfile.writelog("OPEN THE DATABASES")

    # Open the SOURCE file
    with sqlite3.connect(so_path+so_file) as so_conn:
        so_curs = so_conn.cursor()
    funcfile.writelog("OPEN DATABASE: " + so_file)

    # ATTACH VSS DATABASE
    print("Attach vss database...")
    so_curs.execute(f"ATTACH DATABASE '{funcconf.drive_data_raw}Vss/Vss.sqlite' AS 'VSS'")
    funcfile.writelog("%t ATTACH DATABASE: Vss.sqlite")
    so_curs.execute(f"ATTACH DATABASE '{funcconf.drive_data_raw}Vss/Vss_curr.sqlite' AS 'VSSCURR'")
    funcfile.writelog("%t ATTACH DATABASE: Vss_curr.sqlite")
    so_curs.execute(f"ATTACH DATABASE '{funcconf.drive_data_raw}Vss/Vss_prev.sqlite' AS 'VSSPREV'")
    funcfile.writelog("%t ATTACH DATABASE: Vss_prev.sqlite")

    """ ****************************************************************************
    BEGIN OF SCRIPT
    *****************************************************************************"""
    print("BEGIN OF SCRIPT")
    funcfile.writelog("BEGIN OF SCRIPT")
    funcfile.writelog("%t ---------- STUDENT ID NUMBER LIST ----------")

    # Import vss transactions from VSS.SQLITE *********************************
    print("IDNo list import vss transactions from VSS.SQLITE...")
    sr_file = "X001aa_impo_vsstran"
    s_sql = "CREATE TABLE "+sr_file+" AS " + """
    SELECT
        TRAN.FBUSENTID AS STUDENT,
        TRAN.FDEBTCOLLECTIONSITE AS CAMPUS,
        SUBSTR(TRAN.TRANSDATE,1,4) AS YEAR
    FROM
        VSSCURR.X010_Studytrans TRAN
    GROUP BY
        TRAN.FBUSENTID
    ORDER BY
        TRAN.FBUSENTID
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: "+sr_file)

    # Import previously reported findings ******************************************
    print("IDNo list import previously reported findings...")
    tb_name = "X001ac_impo_reported"
    so_curs.execute("DROP TABLE IF EXISTS " + tb_name)
    so_curs.execute("CREATE TABLE " + tb_name + "(PROCESS TEXT,FIELD1 INT,FIELD2 TEXT,FIELD3 TEXT,FIELD4 TEXT,FIELD5 TEXT,DATE_REPORTED TEXT,DATE_RETEST TEXT)")
    co = open(ed_path + "300_reported.txt", newline=None)
    co_reader = csv.reader(co)
    # Read the COLUMN database data
    for row in co_reader:
        # Populate the column variables
        if row[0] == "PROCESS":
            continue
        elif row[0] != "idno_list":
            continue
        else:
            s_cols = "INSERT INTO " + tb_name + " VALUES('" + row[0] + "','" + row[1] + "','" + row[2] + "','" + row[3] + "','" + row[4] + "','" + row[5] + "','" + row[6] + "','" + row[7] + "')"
            so_curs.execute(s_cols)
    so_conn.commit()
    # Close the imported data file
    co.close()
    funcfile.writelog("%t IMPORT TABLE: " + ed_path + "300_reported.txt (" + tb_name + ")" )

    # Join the tran and party data *********************************************
    print("IDNo list join the vss tran and party data...")
    sr_file = "X001ba_join_tran_vss"
    s_sql = "CREATE TABLE "+sr_file+" AS " + """
    SELECT
        VSS.X000_party.IDNO,
        TRAN.STUDENT,
        VSS.X000_Party.FULL_NAME AS NAME,
        TRAN.YEAR,
        TRAN.CAMPUS,
        Trim(VSS.X000_Party.FIRSTNAMES) AS FIRSTNAME,
        VSS.X000_Party.INITIALS,
        VSS.X000_Party.SURNAME,
        VSS.X000_Party.TITLE,
        VSS.X000_Party.DATEOFBIRTH,
        VSS.X000_Party.GENDER,
        VSS.X000_Party.NATIONALITY,
        VSS.X000_Party.POPULATION,
        VSS.X000_Party.RACE,
        VSS.X000_Party.FAUDITUSERCODE AS PARTY_AUDITDATETIME,
        VSS.X000_Party.AUDITDATETIME AS PARTY_AUDITUSERCODE
    FROM
        X001aa_impo_vsstran TRAN Inner Join
        VSS.X000_Party ON VSS.X000_Party.KBUSINESSENTITYID = TRAN.STUDENT AND
        Length(Trim(VSS.X000_Party.IDNO)) = 13
    ORDER BY
        TRAN.STUDENT
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: "+sr_file)

    # Add previous reported ID list to current ID list *****************************
    print("IDNo list join the previously reported id number list...")
    sr_file = "X001ca_join_prev_reported"
    s_sql = "CREATE TABLE " + sr_file + " AS" + """
    SELECT
        TRAN.IDNO,
        TRAN.STUDENT,
        TRAN.NAME,
        TRAN.YEAR,
        TRAN.CAMPUS,
        TRAN.FIRSTNAME,
        TRAN.INITIALS,
        TRAN.SURNAME,
        TRAN.TITLE,
        TRAN.DATEOFBIRTH,
        TRAN.GENDER,
        TRAN.NATIONALITY,
        TRAN.POPULATION,
        TRAN.RACE,
        TRAN.PARTY_AUDITDATETIME,
        TRAN.PARTY_AUDITUSERCODE,
        IMPO.PROCESS AS PREV_PROCESS,
        IMPO.DATE_REPORTED AS PREV_DATE_REPORTED,
        IMPO.DATE_RETEST AS PREV_DATE_RETEST
    FROM
        X001ba_join_tran_vss TRAN Left Join
        X001ac_impo_reported IMPO ON IMPO.FIELD1 = TRAN.STUDENT AND IMPO.DATE_RETEST >= Date('%TODAY%')
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = s_sql.replace("%TODAY%",funcdatn.get_today_date())
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)
    # Add columns used to export new ID list
    print("IDNo list add prev reported columns...")
    so_curs.execute("ALTER TABLE "+sr_file+" ADD COLUMN PROCESS TEXT;")
    so_curs.execute("ALTER TABLE "+sr_file+" ADD COLUMN FIELD2 TEXT;")
    so_curs.execute("ALTER TABLE "+sr_file+" ADD COLUMN DATE_REPORTED TEXT;")
    so_curs.execute("ALTER TABLE "+sr_file+" ADD COLUMN DATE_RETEST TEXT;")
    so_curs.execute("UPDATE "+sr_file+" SET PROCESS = 'idno_list'")
    s_sql = "UPDATE "+sr_file+" SET DATE_REPORTED = '%TODAY%'"
    s_sql = s_sql.replace("%TODAY%",funcdatn.get_today_date())
    so_curs.execute(s_sql)
    s_sql = "UPDATE "+sr_file+" SET DATE_RETEST = '%NYEARB%'"
    s_sql = s_sql.replace("%NYEARB%",funcdatn.get_next_year_begin())
    so_curs.execute(s_sql)
    so_conn.commit()

    # Build the final ytd ID list report table *****************************************
    print("IDNo list build the ytd ID list to export...")
    sr_file = "X001da_report_idlist"
    s_sql = "CREATE TABLE " + sr_file + " AS" + """
    SELECT
        PREV.IDNO,
        PREV.STUDENT,
        PREV.NAME,
        PREV.YEAR,
        PREV.CAMPUS,
        PREV.FIRSTNAME,
        PREV.INITIALS,
        PREV.SURNAME,
        PREV.TITLE,
        PREV.DATEOFBIRTH,
        PREV.GENDER,
        PREV.NATIONALITY,
        PREV.POPULATION,
        PREV.RACE
    FROM
        X001ca_join_prev_reported PREV
    ORDER BY
        PREV.STUDENT
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)
    # MESSAGE
    if funcconf.l_mess_project:
        i = funcsys.tablerowcount(so_curs, sr_file)
        funcsms.send_telegram("", "administrator", "<b> " + str(i) + "</b> " + " Student id numbers")
    # Export the data
    if l_export == True and funcsys.tablerowcount(so_curs,sr_file) > 0:
        print("IDNo list export ytd ID list...")
        sr_filet = sr_file
        sx_path = re_path + funcdatn.get_current_year() + "/"
        sx_file = "Idno_001a_list_ytd_"
        sx_filet = sx_file + funcdatn.get_previous_month_end_file()
        # Read the header data
        s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)
        # Write the data
        funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_file, s_head)
        #funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_filet, s_head)

    # Build the final ytd ID list report table *****************************************
    print("IDNo list build the current ID list to export...")
    sr_file = "X001da_report_idlist"
    s_sql = "CREATE TABLE " + sr_file + " AS" + """
    SELECT
        PREV.IDNO,
        PREV.STUDENT,
        PREV.NAME,
        PREV.YEAR,
        PREV.CAMPUS,
        PREV.FIRSTNAME,
        PREV.INITIALS,
        PREV.SURNAME,
        PREV.TITLE,
        PREV.DATEOFBIRTH,
        PREV.GENDER,
        PREV.NATIONALITY,
        PREV.POPULATION,
        PREV.RACE
    FROM
        X001ca_join_prev_reported PREV
    WHERE
        StrfTime('%m', PREV.PREV_DATE_REPORTED) = StrfTime('%m', 'now') OR
        StrfTime('%m', PREV.DATE_REPORTED) = StrfTime('%m', 'now') AND PREV.PREV_PROCESS IS NULL
    ORDER BY
      PREV.STUDENT
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)
    # Export the data
    if l_export == True and funcsys.tablerowcount(so_curs,sr_file) > 0:
        print("IDNo list export current ID lists...")
        sr_filet = sr_file
        sx_path = re_path + funcdatn.get_current_year() + "/"
        sx_file = "Idno_001b_list_curr_"
        sx_filet = sx_file + funcdatn.get_current_month()
        # Read the header data
        s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)
        # Write the data
        funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_file, s_head)
        funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_filet, s_head)

    # Build new ID list file to export to external previous reported file **********
    print("IDNo list add new id number list to previous reported...")
    sr_file = "X001ea_prev_reported"
    s_sql = "CREATE TABLE "+sr_file+" AS " + """
    SELECT
        PREV.PROCESS,
        PREV.STUDENT AS FIELD1,
        PREV.FIELD2,
        PREV.FIELD2 AS FIELD3,
        PREV.FIELD2 AS FIELD4,
        PREV.FIELD2 AS FIELD5,
        PREV.DATE_REPORTED,
        PREV.DATE_RETEST
    FROM
        X001ca_join_prev_reported PREV
    WHERE
        PREV.PREV_PROCESS IS NULL
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: "+sr_file)
    # Export the new ID list to previous reported file
    if funcsys.tablerowcount(so_curs,sr_file) > 0:
        print("IDNo list export the new data to previously reported file...")
        sr_filet = sr_file
        sx_path = ed_path
        sx_file = "300_reported"
        # Read the header data
        s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)
        # Write the data
        funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_file, s_head,"a",".txt")
        funcfile.writelog("%t EXPORT DATA: "+sr_file)
    else:
        print("IDNo list no new data to previously reported file...")
        funcfile.writelog("%t EXPORT DATA: No new data to export")

    if l_mail:
        funcmail.send_mail("vss_list_idno_ytd")

    if l_mail:
        funcmail.send_mail("vss_list_idno_curr")

    # MESSAGE
    # if funcconf.l_mess_project:
    #    funcsms.send_telegram("", "administrator", "<b>VSS STUDENT</b> master file tests end.")

    """*****************************************************************************
    End OF SCRIPT
    *****************************************************************************"""
    print("END OF SCRIPT")
    funcfile.writelog("END OF SCRIPT")

    # COMMIT DATA
    so_conn.commit()

    # CLOSE THE DATABASE CONNECTION
    so_conn.close()

    # CLOSE THE LOG WRITER
    funcfile.writelog("------------------------------------")
    funcfile.writelog("COMPLETED: C300_TEST_STUDENT_GENERAL")

    return


if __name__ == '__main__':
    try:
        test_student_general()
    except Exception as e:
        funcsys.ErrMessage(e, funcconf.l_mess_project, "C300_test_student_general", "C300_test_student_general")
