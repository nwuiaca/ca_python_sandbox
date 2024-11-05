"""
Script to build kfs creditor payment tests
Created on: 16 Apr 2019
Author: Albert J v Rensburg (NWU21162395)
"""

# IMPORT PYTHON MODULES
import csv
import sqlite3
import datetime

# IMPORT OWN MODULES
from _my_modules import funcconf
from _my_modules import funcfile
from _my_modules import funccsv
from _my_modules import funcdatn
from _my_modules import funcsms
from _my_modules import funcstat
from _my_modules import funcsys
from _my_modules import functest

"""*****************************************************************************
ENVIRONMENT
*****************************************************************************"""

# DECLARE VARIABLES
l_debug: bool = True
so_path: str = f"{funcconf.drive_data_raw}Kfs/"  # Source database path
so_file: str = "Kfs_test_creditor.sqlite"  # Source database
re_path: str = f"{funcconf.drive_data_results}Kfs/"  # Results path
ed_path: str = f"{funcconf.drive_system}_external_data/"  # External data path
l_export: bool = False
# l_mail: bool = funcconf.l_mail_project
l_mail: bool = False
# l_mess: bool = funcconf.l_mess_project
l_mess: bool = False
l_record: bool = True

# OPEN THE SCRIPT LOG FILE
funcfile.writelog("Now")
funcfile.writelog("SCRIPT: C201_CREDITOR_TEST_DEV")
funcfile.writelog("------------------------------")
if l_debug:
    print("----------------------")
    print("C201_CREDITOR_TEST_DEV")
    print("----------------------")

"""*****************************************************************************
OPEN THE DATABASES
*****************************************************************************"""
funcfile.writelog("OPEN THE DATABASES")
if l_debug:
    print("OPEN THE DATABASES")

# OPEN THE WORKING DATABASE
with sqlite3.connect(so_path + so_file) as so_conn:
    so_curs = so_conn.cursor()
funcfile.writelog("OPEN DATABASE: " + so_file)

# ATTACH DATA SOURCES
so_curs.execute(f"ATTACH DATABASE '{funcconf.drive_data_raw}Kfs/Kfs.sqlite' AS 'KFS'")
funcfile.writelog("%t ATTACH DATABASE: KFS.SQLITE")
so_curs.execute(f"ATTACH DATABASE '{funcconf.drive_data_raw}Kfs/Kfs_curr.sqlite' AS 'KFSCURR'")
funcfile.writelog("%t ATTACH DATABASE: KFS_CURR.SQLITE")
so_curs.execute(f"ATTACH DATABASE '{funcconf.drive_data_raw}Kfs/Kfs_prev.sqlite' AS 'KFSPREV'")
funcfile.writelog("%t ATTACH DATABASE: KFS_PREV.SQLITE")
so_curs.execute(f"ATTACH DATABASE '{funcconf.drive_data_raw}People/People.sqlite' AS 'PEOPLE'")
funcfile.writelog("%t ATTACH DATABASE: PEOPLE.SQLITE")

""" ****************************************************************************
BEGIN OF SCRIPT
*****************************************************************************"""
funcfile.writelog("BEGIN OF SCRIPT")
if l_debug:
    print("BEGIN OF SCRIPT")

""" ****************************************************************************
SPOUSE INSURANCE MASTER FILES
*****************************************************************************"""

# IMPORT SPOUSE BENCHMARK
sr_file = "X003_spouse_matrix"
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
if l_debug:
    print("Import spouse matrix...")
so_curs.execute(
    "CREATE TABLE " + sr_file + "(PERSON_TYPE, MARITAL_STATUS, TEST1)")
s_cols = ""
co = open(ed_path + "001_employee_marital_status.csv", "r")
co_reader = csv.reader(co)
# Read the COLUMN database data
for row in co_reader:
    # Populate the column variables
    if row[0] == "PERSON_TYPE":
        continue
    else:
        s_cols = "INSERT INTO " + sr_file + " VALUES('" + row[0] + "','" + row[1] + "','" + row[2] + "')"
        so_curs.execute(s_cols)
so_conn.commit()
# Close the impoted data file
co.close()
funcfile.writelog("%t IMPORT TABLE: " + ed_path + "001_employee_marital_status.csv (" + sr_file + ")")

# OBTAIN MASTER DATA
# if test <> '1' then employee does not form part of the test
# if married = '1' then married for all married person types
if l_debug:
    print("Obtain employee and spouse data...")
sr_file: str = 'X003_people_spouse_all'
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
s_sql = "Create Table " + sr_file + " As " + """
Select
    p.employee_number,
    p.person_id,
    p.name_address,
    p.user_person_type,
    cast(t.TEST1 As Int) As test,
    p.marital_status,
    cast(m.TEST1 As Int) As married,
    p.date_started,
    cast(i.ELEMENT_VALUE As Int) As spouse_insurance_status,
    cast(s.spouse_age As Int) As spouse_age,
    s.person_extra_info_id,
    s.spouse_number,
    s.spouse_active,        
    s.spouse_address,
    s.spouse_date_of_birth,
    s.spouse_national_identifier,
    s.spouse_passport,
    s.spouse_start_date,
    s.spouse_end_date,
    s.spouse_create_date,
    s.spouse_created_by,
    s.spouse_update_date,
    s.spouse_updated_by,
    s.spouse_update_login
From
    PEOPLE.X000_PEOPLE p Left Join
    PEOPLE.X000_GROUPINSURANCE_SPOUSE i On i.EMPLOYEE_NUMBER = p.employee_number Left Join
    PEOPLE.X002_SPOUSE_CURR s On s.employee_number = p.employee_number Left Join
    X003_spouse_matrix t On t.PERSON_TYPE = p.user_person_type Left Join
    X003_spouse_matrix m On m.MARITAL_STATUS = p.marital_status
Group By
    p.employee_number    
;"""
so_curs.execute(s_sql)
funcfile.writelog("%t BUILD TABLE: " + sr_file)
if l_debug:
    so_conn.commit()

"""*****************************************************************************
TEST SPOUSE APPROVE PAYMENT
*****************************************************************************"""

# FILES NEEDED
# X003_people_spuse_all

# DECLARE TEST VARIABLES
i_finding_before = 0
i_finding_after = 0
s_description = "Spouse approve payment"
s_file_prefix: str = "X003c"
s_file_name: str = "spouse_approve_payment"
s_finding: str = "SPOUSE APPROVE PAYMENT"
s_report_file: str = "201_reported.txt"

# OBTAIN TEST RUN FLAG
if functest.get_test_flag(so_curs, "KFS", "TEST " + s_finding, "RUN") == "FALSE":

    if l_debug:
        print('TEST DISABLED')
    funcfile.writelog("TEST " + s_finding + " DISABLED")

else:

    # LOG
    funcfile.writelog("TEST " + s_finding)
    if l_debug:
        print("TEST " + s_finding)

    # OBTAIN MASTER DATA
    if l_debug:
        print("Obtain master data...")
    sr_file: str = s_file_prefix + "a_" + s_file_name
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = "Create Table " + sr_file + " As " + """
        Select
            p.*,
            s.married as m1,
            s.employee_number as em1,
            s.spouse_number as sp1,
            r.married as m2,
            r.employee_number as em2,
            r.spouse_number as sp2
        From
            KFSCURR.X001ac_Report_payments_approve p Left Join
            X003_people_spouse_all s On s.employee_number = Substr(p.VENDOR_ID, 1, 8) And s.married = '1' Left Join
            X003_people_spouse_all r On r.spouse_number = Substr(p.VENDOR_ID, 1, 8) And s.married = '1'
        Where
            p.APPROVE_EMP_NO = s.spouse_number Or
            p.APPROVE_EMP_NO = r.employee_number
        Order By
            p.APPROVE_DATE
    ;"""
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD TABLE: " + sr_file)
    if l_debug:
        so_conn.commit()

    # IDENTIFY FINDINGS
    if l_debug:
        print("Identify findings...")
    sr_file = s_file_prefix + "b_finding"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        'NWU' As ORG,
        'OTHER' As DOC_TYPE,
        FIND.EDOC,
        FIND.VENDOR_ID,
        FIND.PAYEE_NAME,
        FIND.APPROVE_EMP_NAME,
        FIND.APPROVE_DATE,
        FIND.NET_PMT_AMT,
        FIND.ACC_DESC        
    From
        %FILEP%%FILEN% FIND
    ;"""
    s_sql = s_sql.replace("%FILEP%", s_file_prefix)
    s_sql = s_sql.replace("%FILEN%", "a_" + s_file_name)
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD TABLE: " + sr_file)
    if l_debug:
        so_conn.commit()

    # COUNT THE NUMBER OF FINDINGS
    if l_debug:
        print("Count the number of findings...")
    i_finding_before: int = funcsys.tablerowcount(so_curs, sr_file)
    funcfile.writelog("%t FINDING: " + str(i_finding_before) + " " + s_finding + " finding(s)")
    if l_debug:
        print("*** Found " + str(i_finding_before) + " exceptions ***")

    # GET PREVIOUS FINDINGS
    if i_finding_before > 0:
        functest.get_previous_finding(so_curs, ed_path, s_report_file, s_finding, "TTTTR")
        if l_debug:
            so_conn.commit()

    # SET PREVIOUS FINDINGS
    if i_finding_before > 0:
        functest.set_previous_finding(so_curs)
        if l_debug:
            so_conn.commit()

    # ADD PREVIOUS FINDINGS
    sr_file = s_file_prefix + "d_addprev"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0:
        if l_debug:
            print("Join previously reported to current findings...")
        s_sql = "CREATE TABLE " + sr_file + " AS" + """
        Select
            FIND.*,
            Lower('%FINDING%') AS PROCESS,
            '%TODAY%' AS DATE_REPORTED,
            '%DATETEST%' AS DATE_RETEST,
            PREV.PROCESS AS PREV_PROCESS,
            PREV.DATE_REPORTED AS PREV_DATE_REPORTED,
            PREV.DATE_RETEST AS PREV_DATE_RETEST,
            PREV.REMARK
        From
            %FILEP%b_finding FIND Left Join
            Z001ab_setprev PREV ON PREV.FIELD1 = FIND.EDOC
        ;"""
        s_sql = s_sql.replace("%FINDING%", s_finding)
        s_sql = s_sql.replace("%FILEP%", s_file_prefix)
        s_sql = s_sql.replace("%TODAY%", funcdatn.get_today_date())
        s_sql = s_sql.replace("%DATETEST%", funcdatn.get_current_year_end())
        so_curs.execute(s_sql)
        funcfile.writelog("%t BUILD TABLE: " + sr_file)
        if l_debug:
            so_conn.commit()

    # BUILD LIST TO UPDATE FINDINGS
    sr_file = s_file_prefix + "e_newprev"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0:
        if l_debug:
            print("Build list to update findings...")
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            PREV.PROCESS,
            PREV.EDOC AS FIELD1,
            PREV.VENDOR_ID AS FIELD2,
            PREV.PAYEE_NAME AS FIELD3,
            PREV.APPROVE_EMP_NAME AS FIELD4,
            PREV.NET_PMT_AMT AS FIELD5,
            PREV.DATE_REPORTED,
            PREV.DATE_RETEST,
            PREV.REMARK
        From
            %FILEP%d_addprev PREV
        Where
            PREV.PREV_PROCESS Is Null Or
            PREV.DATE_REPORTED > PREV.PREV_DATE_RETEST And PREV.REMARK = ""        
        ;"""
        s_sql = s_sql.replace("%FILEP%", s_file_prefix)
        so_curs.execute(s_sql)
        funcfile.writelog("%t BUILD TABLE: " + sr_file)
        if l_debug:
            so_conn.commit()
        # Export findings to previous reported file
        i_finding_after = funcsys.tablerowcount(so_curs, sr_file)
        if i_finding_after > 0:
            if l_debug:
                print("*** " + str(i_finding_after) + " Finding(s) to report ***")
            sx_path = ed_path
            sx_file = s_report_file[:-4]
            # Read the header data
            s_head = funccsv.get_colnames_sqlite(so_conn, sr_file)
            # Write the data
            if l_record:
                funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file, s_head, "a", ".txt")
                funcfile.writelog("%t FINDING: " + str(i_finding_after) + " new finding(s) to export")
                funcfile.writelog("%t EXPORT DATA: " + sr_file)
            if l_mess:
                funcsms.send_telegram('', 'administrator', '<b>' + str(i_finding_before) + '/' + str(
                    i_finding_after) + '</b> ' + s_description)
        else:
            funcfile.writelog("%t FINDING: No new findings to export")
            if l_debug:
                print("*** No new findings to report ***")

    # IMPORT OFFICERS FOR MAIL REPORTING PURPOSES
    if i_finding_before > 0 and i_finding_after > 0:
        functest.get_officer(so_curs, "KFS", "TEST " + s_finding + " OFFICER")
        so_conn.commit()

    # IMPORT SUPERVISORS FOR MAIL REPORTING PURPOSES
    if i_finding_before > 0 and i_finding_after > 0:
        functest.get_supervisor(so_curs, "KFS", "TEST " + s_finding + " SUPERVISOR")
        so_conn.commit()

    # ADD CONTACT DETAILS TO FINDINGS
    sr_file = s_file_prefix + "h_detail"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0 and i_finding_after > 0:
        if l_debug:
            print("Add contact details to findings...")
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            PREV.ORG,
            PREV.DOC_TYPE,
            PREV.EDOC,
            PREV.VENDOR_ID,
            PREV.PAYEE_NAME,
            PREV.APPROVE_EMP_NAME,
            PREV.APPROVE_DATE,
            PREV.NET_PMT_AMT,
            PREV.ACC_DESC,
            CAMP_OFF.EMPLOYEE_NUMBER AS CAMP_OFF_NUMB,
            CAMP_OFF.NAME_ADDR AS CAMP_OFF_NAME,
            CAMP_OFF.EMAIL_ADDRESS AS CAMP_OFF_MAIL1,        
            CASE
                WHEN  CAMP_OFF.EMPLOYEE_NUMBER != '' THEN CAMP_OFF.EMPLOYEE_NUMBER||'@nwu.ac.za'
                ELSE CAMP_OFF.EMAIL_ADDRESS
            END AS CAMP_OFF_MAIL2,
            CAMP_SUP.EMPLOYEE_NUMBER AS CAMP_SUP_NUMB,
            CAMP_SUP.NAME_ADDR AS CAMP_SUP_NAME,
            CAMP_SUP.EMAIL_ADDRESS AS CAMP_SUP_MAIL1,
            CASE
                WHEN CAMP_SUP.EMPLOYEE_NUMBER != '' THEN CAMP_SUP.EMPLOYEE_NUMBER||'@nwu.ac.za'
                ELSE CAMP_SUP.EMAIL_ADDRESS
            END AS CAMP_SUP_MAIL2,
            ORG_OFF.EMPLOYEE_NUMBER AS ORG_OFF_NUMB,
            ORG_OFF.NAME_ADDR AS ORG_OFF_NAME,
            ORG_OFF.EMAIL_ADDRESS AS ORG_OFF_MAIL1,
            CASE
                WHEN ORG_OFF.EMPLOYEE_NUMBER != '' THEN ORG_OFF.EMPLOYEE_NUMBER||'@nwu.ac.za'
                ELSE ORG_OFF.EMAIL_ADDRESS
            END AS ORG_OFF_MAIL2,
            ORG_SUP.EMPLOYEE_NUMBER AS ORG_SUP_NUMB,
            ORG_SUP.NAME_ADDR AS ORG_SUP_NAME,
            ORG_SUP.EMAIL_ADDRESS AS ORG_SUP_MAIL1,
            CASE
                WHEN ORG_SUP.EMPLOYEE_NUMBER != '' THEN ORG_SUP.EMPLOYEE_NUMBER||'@nwu.ac.za'
                ELSE ORG_SUP.EMAIL_ADDRESS
            END AS ORG_SUP_MAIL2,
            AUD_OFF.EMPLOYEE_NUMBER As AUD_OFF_NUMB,
            AUD_OFF.NAME_ADDR As AUD_OFF_NAME,
            AUD_OFF.EMAIL_ADDRESS As AUD_OFF_MAIL,
            AUD_SUP.EMPLOYEE_NUMBER As AUD_SUP_NUMB,
            AUD_SUP.NAME_ADDR As AUD_SUP_NAME,
            AUD_SUP.EMAIL_ADDRESS As AUD_SUP_MAIL
        From
            %FILEP%d_addprev PREV Left Join
            Z001af_officer CAMP_OFF On CAMP_OFF.CAMPUS = 'OTHER' Left Join
            Z001af_officer ORG_OFF On ORG_OFF.CAMPUS = 'NWU' Left Join
            Z001af_officer AUD_OFF On AUD_OFF.CAMPUS = 'AUD' Left Join
            Z001ag_supervisor CAMP_SUP On CAMP_SUP.CAMPUS = 'OTHER' Left Join
            Z001ag_supervisor ORG_SUP On ORG_SUP.CAMPUS = 'NWU' Left Join
            Z001ag_supervisor AUD_SUP On AUD_SUP.CAMPUS = 'AUD'                    
        Where
            PREV.PREV_PROCESS Is Null Or
            PREV.DATE_REPORTED > PREV.PREV_DATE_RETEST And PREV.REMARK = ""
        ;"""
        s_sql = s_sql.replace("%FILEP%", s_file_prefix)
        s_sql = s_sql.replace("%FILEN%", s_file_name)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD THE FINAL TABLE FOR EXPORT AND REPORT
    sr_file = s_file_prefix + "x_" + s_file_name
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0 and i_finding_after > 0:
        # NOTE
        # Remember to put the fields in the order to be displayed in the email to the client
        if l_debug:
            print("Build the final report")
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            '%FIND%' As Audit_finding,
            FIND.ORG As Organization,
            FIND.EDOC As Edoc,
            FIND.DOC_TYPE As Document_type,
            FIND.VENDOR_ID As Vendor_id,
            FIND.PAYEE_NAME As Vendor_name,
            FIND.APPROVE_EMP_NAME As Approver_name,
            FIND.APPROVE_DATE As Approve_date,
            FIND.NET_PMT_AMT As Amount,
            FIND.ACC_DESC As Description,
            FIND.CAMP_OFF_NAME AS Responsible_officer,
            FIND.CAMP_OFF_NUMB AS Responsible_officer_numb,
            FIND.CAMP_OFF_MAIL1 AS Responsible_officer_mail,
            FIND.CAMP_OFF_MAIL2 AS Responsible_officer_mail_alt,
            FIND.CAMP_SUP_NAME AS Supervisor,
            FIND.CAMP_SUP_NUMB AS Supervisor_numb,
            FIND.CAMP_SUP_MAIL1 AS Supervisor_mail,
            FIND.ORG_OFF_NAME AS Org_officer,
            FIND.ORG_OFF_NUMB AS Org_officer_numb,
            FIND.ORG_OFF_MAIL1 AS Org_officer_mail,
            FIND.ORG_SUP_NAME AS Org_supervisor,
            FIND.ORG_SUP_NUMB AS Org_supervisor_numb,
            FIND.ORG_SUP_MAIL1 AS Org_supervisor_mail,
            FIND.AUD_OFF_NAME AS Audit_officer,
            FIND.AUD_OFF_NUMB AS Audit_officer_numb,
            FIND.AUD_OFF_MAIL AS Audit_officer_mail,
            FIND.AUD_SUP_NAME AS Audit_supervisor,
            FIND.AUD_SUP_NUMB AS Audit_supervisor_numb,
            FIND.AUD_SUP_MAIL AS Audit_supervisor_mail
        From
            %FILEP%h_detail FIND
        ;"""
        s_sql = s_sql.replace("%FIND%", s_finding)
        s_sql = s_sql.replace("%FILEP%", s_file_prefix)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)
        # Export findings
        if l_export and funcsys.tablerowcount(so_curs, sr_file) > 0:
            if l_debug:
                print("Export findings...")
            sx_path = re_path + funcdatn.get_current_year() + "/"
            sx_file = s_file_prefix + "_" + s_finding.lower() + "_"
            sx_file_dated = sx_file + funcdatn.get_today_date_file()
            s_head = funccsv.get_colnames_sqlite(so_conn, sr_file)
            funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file, s_head)
            funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file_dated, s_head)
            funcfile.writelog("%t EXPORT DATA: " + sx_path + sx_file)
    else:
        s_sql = "CREATE TABLE " + sr_file + " (" + """
        BLANK TEXT
        );"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

"""*****************************************************************************
END OF SCRIPT
*****************************************************************************"""
funcfile.writelog("END OF SCRIPT")
if l_debug:
    print("END OF SCRIPT")

# CLOSE THE DATABASE CONNECTION
so_conn.close()

# CLOSE THE LOG WRITER
funcfile.writelog("---------------------------------")
funcfile.writelog("COMPLETED: C201_CREDITOR_TEST_DEV")
