""" Script to run MYSQL web lists
Created: 28 May 2019
Author: Albert J v Rensburg (NWU21162395)
"""

# Import python modules
# import sys
import sqlite3

# Import own modules
from _my_modules import funcconf
from _my_modules import funcfile
from _my_modules import funcmysql
from _my_modules import funcsms
from _my_modules import funcsys

""" INDEX **********************************************************************
ENVIRONMENT
OPEN THE MYSQL DATABASES
BEGIN OF SCRIPT
OPEN THE PEOPLE DATABASES
EXPORT CURRENT PEOPLE
EXPORT CURRENT PEOPLE STRUCTURE
OPEN THE KFS VSS STUDDEB DATABASES
EXPORT STUD DEBTOR MONTHLY BALANCES

END OF SCRIPT
*****************************************************************************"""


def mysql_lists(s_database):
    """
    Function to populate the mysql databases
    :param s_database:
    :return:
    """

    """*****************************************************************************
    ENVIRONMENT
    *****************************************************************************"""

    # Declare variables
    s_schema: str = ""
    if s_database == "Web_nwu_ia":
        s_schema = "nwuiaeapciy_db1"
    elif s_database == "Web_ia_nwu":
        s_schema = "Ia_nwu"
    elif s_database == "Web_ia_joomla":
        s_schema = "Ia_joomla"
    elif s_database == "Mysql_ia_server":
        s_schema = "nwuiaca"
    # l_export = True
    # l_mail = True
    # l_vacuum = False

    # Script log file
    funcfile.writelog("Now")
    funcfile.writelog("SCRIPT: B005_MYSQL_LISTS")
    funcfile.writelog("-------------------------")
    print("-----------------")
    print("B005_MYSQL_LISTS")
    print("-----------------")

    # MESSAGE
    if funcconf.l_mess_project:
        funcsms.send_telegram("", "administrator", "<b>B005 Mysql server (" + s_schema + ")</b>")

    """*****************************************************************************
    OPEN THE MYSQL DATABASES
    *****************************************************************************"""
    print("OPEN THE MYSQL DATABASES")
    funcfile.writelog("OPEN THE MYSQL DATABASES")

    # Open the MYSQL DESTINATION table
    ms_cnxn = funcmysql.mysql_open(s_database)
    ms_curs = ms_cnxn.cursor()
    funcfile.writelog("%t OPEN MYSQL DATABASE: " + s_database)

    """ ****************************************************************************
    BEGIN OF SCRIPT
    *****************************************************************************"""
    print("BEGIN OF SCRIPT")
    funcfile.writelog("BEGIN OF SCRIPT")

    """*****************************************************************************
    OPEN THE PEOPLE DATABASES
    *****************************************************************************"""
    print("OPEN THE PEOPLE DATABASES")
    funcfile.writelog("OPEN THE PEOPLE DATABASES")

    # Open the SQLITE SOURCE file
    so_path = f"{funcconf.drive_data_raw}People/"  # Source database path
    so_file = "People.sqlite"  # Source database
    with sqlite3.connect(so_path+so_file) as so_conn:
        so_curs = so_conn.cursor()
    funcfile.writelog("OPEN DATABASE: " + so_file)

    """*****************************************************************************
    EXPORT CURRENT PEOPLE
    *****************************************************************************"""
    print("EXPORT CURRENT PEOPLE")
    funcfile.writelog("EXPORT CURRENT PEOPLE")

    # EXPORT CURRENT PEOPLE TO MYSQL
    print("Build mysql current people...")
    ms_curs.execute("DROP TABLE IF EXISTS ia_people")
    funcfile.writelog("%t DROP MYSQL TABLE: PEOPLE (ia_people)")
    # ia_find_1_auto INT(11) NOT NULL AUTO_INCREMENT,
    s_sql = """
    CREATE TABLE IF NOT EXISTS ia_people (
    ia_find_auto INT(11) NOT NULL,
    people_employee_number VARCHAR(20),
    people_ass_id INT(11),
    people_person_id INT(11),
    people_ass_numb VARCHAR(30),
    people_party_id INT(11),
    people_full_name VARCHAR(150),
    people_name_list VARCHAR(150),
    people_name_addr VARCHAR(150),    
    people_known_name VARCHAR(150),
    people_position_full VARCHAR(150),
    people_date_of_birth DATE,
    people_nationality VARCHAR(30),
    people_nationality_name VARCHAR(150),
    people_idno VARCHAR(150),
    people_passport VARCHAR(150),
    people_permit VARCHAR(150),
    people_permit_expire DATE,
    people_tax_number VARCHAR(150),
    people_sex VARCHAR(30),
    people_marital_status VARCHAR(30),
    people_disabled VARCHAR(30),
    people_race_code VARCHAR(30),
    people_race_desc VARCHAR(150),
    people_lang_code VARCHAR(30),
    people_lang_desc VARCHAR(150),
    people_int_mail VARCHAR(30),
    people_email_address VARCHAR(150),
    people_curr_empl_flag VARCHAR(1),
    people_user_person_type VARCHAR(30),
    people_ass_start DATE,
    people_ass_end DATE,
    people_emp_start DATE,
    people_emp_end DATE,
    people_leaving_reason VARCHAR(30),
    people_leave_reason_descrip VARCHAR(150),
    people_location_description VARCHAR(150),
    people_org_type_desc VARCHAR(150),
    people_oe_code VARCHAR(30),
    people_org_name VARCHAR(150),
    people_primary_flag VARCHAR(1),
    people_acad_supp VARCHAR(30),
    people_faculty VARCHAR(150),
    people_division VARCHAR(150),
    people_employment_category VARCHAR(150),
    people_ass_week_len INT(2),
    people_leave_code VARCHAR(30),
    people_grade VARCHAR(30),
    people_grade_name VARCHAR(150),
    people_grade_calc VARCHAR(30),
    people_position VARCHAR(30),
    people_position_name VARCHAR(150),
    people_job_name VARCHAR(150),
    people_job_segment_name VARCHAR(150),
    people_supervisor INT(11),
    people_title_full VARCHAR(30),
    people_first_name VARCHAR(150),
    people_middle_names VARCHAR(150),
    people_last_name VARCHAR(150),
    people_phone_work VARCHAR(30),
    people_phone_mobi VARCHAR(30),
    people_phone_home VARCHAR(30),
    people_address_sars VARCHAR(250),
    people_address_post VARCHAR(250),
    people_address_home VARCHAR(250),
    people_address_othe VARCHAR(250),
    people_count_pos INT(11),
    people_count_ass INT(11),
    people_count_peo INT(11),
    people_date_ass_lookup DATE,
    people_ass_active VARCHAR(1),
    people_date_emp_lookup DATE,
    people_emp_active VARCHAR(1),
    people_mailto VARCHAR(150),
    people_proposed_salary_n DECIMAL(12,2),
    people_person_type VARCHAR(150),
    people_acc_type VARCHAR(30),
    people_acc_branch VARCHAR(30),
    people_acc_number VARCHAR(30),
    people_acc_sars VARCHAR(1),
    people_acc_relation VARCHAR(30),
    people_sec_fullpart_flag VARCHAR(1),
    people_initials VARCHAR(30),
    people_age INT(3),
    people_month INT(2),
    people_day INT(2),
    PRIMARY KEY (people_employee_number),
    INDEX fb_order_people_full_name_INDEX (people_full_name),
    INDEX fb_order_people_known_name_INDEX (people_known_name)
    )
    ENGINE = InnoDB
    CHARSET=utf8mb4
    COLLATE utf8mb4_unicode_ci
    COMMENT = 'Table to store detailed people data'
    """ + ";"
    ms_curs.execute(s_sql)
    funcfile.writelog("%t CREATE MYSQL TABLE: PEOPLE (ia_people)")

    # Obtain the new mysql table column types
    print("Build mysql current people columns types...")
    funcfile.writelog("GET COLUMN TYPE: People")
    a_cols = funcmysql.get_coltypes_mysql_list(ms_curs, s_schema, "ia_people")
    # print(a_cols)

    # Open the SOURCE file to obtain column headings
    print("Build mysql current people columns...")
    funcfile.writelog("GET COLUMN NAME: People")
    s_head = funcmysql.get_colnames_sqlite_text(so_curs, "X002_PEOPLE_CURR", "people_")
    s_head = "(`ia_find_auto`, " + s_head.rstrip(", ") + ")"
    # print(s_head)

    # Open the SOURCE file to obtain the data
    print("Insert mysql current people...")
    with sqlite3.connect(so_path+so_file) as rs_conn:
        rs_conn.row_factory = sqlite3.Row
    rs_curs = rs_conn.cursor()
    rs_curs.execute("SELECT * FROM X002_PEOPLE_CURR")
    rows = rs_curs.fetchall()
    i_tota = 0
    i_coun = 0
    for row in rows:
        s_data = funcmysql.convert_sqlite_mysql(row, a_cols, 1, 1)
        # print(s_data)
        s_sql = "INSERT IGNORE INTO `ia_people` " + s_head + " VALUES " + s_data + ";"
        ms_curs.execute(s_sql)
        i_tota = i_tota + 1
        i_coun = i_coun + 1
        if i_coun == 100:
            ms_cnxn.commit()
            i_coun = 0
            
    # Close the ROW Connection
    ms_cnxn.commit()
    rs_conn.close()
    print("Inserted " + str(i_tota) + " mysql current people...")
    funcfile.writelog("%t POPULATE MYSQL: " + str(i_tota) + " PEOPLE CURRENT rows (ia_people)")
    if funcconf.l_mess_project:
        funcsms.send_telegram("", "administrator", "<b> " + str(i_tota) + "</b> People upload")

    # Update MYSQL PEOPLE TO WEB FINDING mail trigger ******************************
    # if s_database == "Web_ia_nwu":
        # print("Update mysql current people mail trigger...")
        # s_sql = """
        # UPDATE `ia_finding` SET
        # `ia_find_updated` = '1',
        # `ia_find_r1_send` = '0',
        # `ia_find_updatedate` = now()
        # WHERE `ia_finding`.`ia_find_auto` = 1
        # """ + ";"
        # ms_curs.execute(s_sql)
        # ms_cnxn.commit()
        # funcfile.writelog("%t UPDATE MYSQL TRIGGER: FINDING 1 (people current)")

    # Update MYSQL PEOPLE TO WEB FINDING mail trigger ******************************
    # if s_database == "Web_ia_nwu":
        #     print("Update mysql current people summary mail trigger...")
        # s_sql = """
        # UPDATE `ia_finding` SET
        # `ia_find_updated` = '1',
        # `ia_find_r1_send` = '0',
        # `ia_find_updatedate` = now()
        # WHERE `ia_finding`.`ia_find_auto` = 2
        # """ + ";"
        # ms_curs.execute(s_sql)
        # ms_cnxn.commit()
        # funcfile.writelog("%t UPDATE MYSQL TRIGGER: FINDING 2 (people current summary)")

    # Update MYSQL PEOPLE TO WEB FINDING mail trigger ******************************
    # if s_database == "Web_ia_nwu":
        # print("Update mysql current people birthday mail trigger...")
        # s_sql = """
        # UPDATE `ia_finding` SET
        # `ia_find_updated` = '1',
        # `ia_find_r1_send` = '0',
        # `ia_find_updatedate` = now()
        # WHERE `ia_finding`.`ia_find_auto` = 3
        # """ + ";"
        # ms_curs.execute(s_sql)
        # ms_cnxn.commit()
        # funcfile.writelog("%t UPDATE MYSQL TRIGGER: FINDING 3 (people current birthdays)")

    """*****************************************************************************
    EXPORT CURRENT PEOPLE STRUCTURE
    *****************************************************************************"""
    print("EXPORT CURRENT PEOPLE STRUCTURE")
    funcfile.writelog("EXPORT CURRENT PEOPLE STRUCTURE")

    # EXPORT CURRENT PEOPLE STRUCTURE
    print("Build mysql current people structure...")
    ms_curs.execute("DROP TABLE IF EXISTS ia_people_struct")
    funcfile.writelog("%t DROP MYSQL TABLE: PEOPLE_STRUCT (ia_people_struct)")
    # ia_find_1_auto INT(11) NOT NULL AUTO_INCREMENT,
    s_sql = """
    CREATE TABLE IF NOT EXISTS ia_people_struct (
    ia_find_auto INT(11) NOT NULL,
    struct_employee_one VARCHAR(20),
    struct_name_list_one VARCHAR(150),
    struct_known_name_one VARCHAR(150),
    struct_position_full_one VARCHAR(150),
    struct_location_description_one VARCHAR(150),
    struct_division_one VARCHAR(150),
    struct_faculty_one VARCHAR(150),
    struct_email_address_one VARCHAR(150),
    struct_phone_work_one VARCHAR(30),
    struct_phone_mobi_one VARCHAR(30),
    struct_phone_home_one VARCHAR(30),
    struct_org_name_one VARCHAR(150),
    struct_grade_calc_one VARCHAR(150),
    struct_employee_two VARCHAR(20),
    struct_name_list_two VARCHAR(150),
    struct_known_name_two VARCHAR(150),
    struct_position_full_two VARCHAR(150),
    struct_location_description_two VARCHAR(150),
    struct_division_two VARCHAR(150),
    struct_faculty_two VARCHAR(150),
    struct_email_address_two VARCHAR(150),
    struct_phone_work_two VARCHAR(30),
    struct_phone_mobi_two VARCHAR(30),
    struct_phone_home_two VARCHAR(30),
    struct_employee_three VARCHAR(20),
    struct_name_list_three VARCHAR(150),
    struct_known_name_three VARCHAR(150),
    struct_position_full_three VARCHAR(150),
    struct_location_description_three VARCHAR(150),
    struct_division_three VARCHAR(150),
    struct_faculty_three VARCHAR(150),
    struct_email_address_three VARCHAR(150),
    struct_phone_work_three VARCHAR(30),
    struct_phone_mobi_three VARCHAR(30),
    struct_phone_home_three VARCHAR(30),
    PRIMARY KEY (struct_employee_one)
    )
    ENGINE = InnoDB
    CHARSET=utf8mb4
    COLLATE utf8mb4_unicode_ci
    COMMENT = 'Table to store detailed people structure data'
    """ + ";"
    ms_curs.execute(s_sql)
    funcfile.writelog("%t CREATE MYSQL TABLE: PEOPLE_STRUCT (ia_people_struct)")
    # Obtain the new mysql table column types
    print("Build mysql current people structure columns types...")
    funcfile.writelog("GET COLUMN TYPE: People")
    a_cols = funcmysql.get_coltypes_mysql_list(ms_curs, s_schema, "ia_people_struct")
    # print(a_cols)
    # Open the SOURCE file to obtain column headings
    print("Build mysql current people structure columns...")
    funcfile.writelog("GET COLUMN NAME: People org structure")
    s_head = funcmysql.get_colnames_sqlite_text(so_curs, "X003_PEOPLE_ORGA_REF", "struct_")
    s_head = "(`ia_find_auto`, " + s_head.rstrip(", ") + ")"
    # print(s_head)
    # Open the SOURCE file to obtain the data
    print("Insert mysql current people structure...")
    with sqlite3.connect(so_path+so_file) as rs_conn:
        rs_conn.row_factory = sqlite3.Row
    rs_curs = rs_conn.cursor()
    rs_curs.execute("SELECT * FROM X003_PEOPLE_ORGA_REF")
    rows = rs_curs.fetchall()
    i_tota = 0
    i_coun = 0
    for row in rows:
        s_data = funcmysql.convert_sqlite_mysql(row, a_cols, 4, 1)
        s_sql = "INSERT IGNORE INTO `ia_people_struct` " + s_head + " VALUES " + s_data + ";"
        ms_curs.execute(s_sql)
        i_tota = i_tota + 1
        i_coun = i_coun + 1
        if i_coun == 100:
            # print(i_coun)
            ms_cnxn.commit()
            i_coun = 0
    # Close the ROW Connection
    ms_cnxn.commit()
    rs_conn.close()
    print("Inserted " + str(i_tota) + " mysql current people...")
    funcfile.writelog("%t POPULATE MYSQL: " + str(i_tota) + " PEOPLE STRUCTURE rows (ia_people_struct)")

    # Update MYSQL PEOPLE TO WEB FINDING mail trigger ******************************
    # if s_database == "Web_ia_nwu":
    #    print("Update mysql current people hierarchy mail trigger...")
    #    s_sql = """
    #    UPDATE `ia_finding` SET
    #    `ia_find_updated` = '1',
    #    `ia_find_r1_send` = '0',
    #    `ia_find_updatedate` = now()
    #    WHERE `ia_finding`.`ia_find_auto` = 4
    #    """ + ";"
    #    ms_curs.execute(s_sql)
    #    ms_cnxn.commit()
    #    funcfile.writelog("%t UPDATE MYSQL TRIGGER: FINDING 4 (people current hierarchy)")

    # CLOSE PEOPLE DATABASE
    so_conn.commit()
    so_conn.close()

    """*****************************************************************************
    OPEN THE KFS VSS STUDDEB DATABASES
    *****************************************************************************"""
    print("OPEN THE PEOPLE DATABASES")
    funcfile.writelog("OPEN THE PEOPLE DATABASES")

    # Open the SQLITE SOURCE file
    so_path = f"{funcconf.drive_data_raw}Kfs_vss_studdeb/"  # Source database path
    so_file = "Kfs_vss_studdeb.sqlite"  # Source database
    with sqlite3.connect(so_path+so_file) as so_conn:
        so_curs = so_conn.cursor()
    funcfile.writelog("OPEN DATABASE: " + so_file)

    """*****************************************************************************
    EXPORT STUD DEBTOR MONTHLY BALANCES
    *****************************************************************************"""
    print("EXPORT STUD DEBTOR MONTHLY BALANCES")
    funcfile.writelog("EXPORT STUD DEBTOR MONTHLY BALANCES")

    # Create MYSQL VSS GL MONTHLY BALANCES TO WEB table ****************************
    print("Build mysql vss gl monthly balances...")
    ms_curs.execute("DROP TABLE IF EXISTS ia_finding_5")
    funcfile.writelog("%t DROP MYSQL TABLE: ia_finding_5")
    s_sql = """
    CREATE TABLE IF NOT EXISTS ia_finding_5 (
    ia_find_auto INT(11),
    ia_find5_auto INT(11) AUTO_INCREMENT,
    ia_find5_campus VARCHAR(20),
    ia_find5_month VARCHAR(2),
    ia_find5_current VARCHAR(1),
    ia_find5_vss_tran_dt DECIMAL(20,2),
    ia_find5_vss_tran_ct DECIMAL(20,2),
    ia_find5_vss_tran DECIMAL(20,2),
    ia_find5_vss_runbal DECIMAL(20,2),
    ia_find5_gl_tran DECIMAL(20,2),
    ia_find5_gl_runbal DECIMAL(20,2),
    ia_find5_diff DECIMAL(20,2),
    ia_find5_move DECIMAL(20,2),
    ia_find5_officer_camp VARCHAR(10),
    ia_find5_officer_name_camp VARCHAR(50),
    ia_find5_officer_mail_camp VARCHAR(100),
    ia_find5_officer_org VARCHAR(10),
    ia_find5_officer_name_org VARCHAR(50),
    ia_find5_officer_mail_org VARCHAR(100),
    ia_find5_supervisor_camp VARCHAR(10),
    ia_find5_supervisor_name_camp VARCHAR(50),
    ia_find5_supervisor_mail_camp VARCHAR(100),
    ia_find5_supervisor_org VARCHAR(10),
    ia_find5_supervisor_name_org VARCHAR(50),
    ia_find5_supervisor_mail_org VARCHAR(100),
    PRIMARY KEY (ia_find5_auto),
    INDEX fb_order_ia_find5_campus_INDEX (ia_find5_campus),
    INDEX fb_order_ia_find5_month_INDEX (ia_find5_month)
    )
    ENGINE = InnoDB
    CHARSET=utf8mb4
    COLLATE utf8mb4_unicode_ci
    COMMENT = 'Table to store vss and gl monthly balances'
    """ + ";"
    ms_curs.execute(s_sql)
    funcfile.writelog("%t CREATE MYSQL TABLE: ia_finding_5 (vss gl monthly balances per campus per month)")
    # Obtain the new mysql table column types
    print("Build mysql current people structure columns types...")
    funcfile.writelog("GET COLUMN TYPE: People")
    a_cols = funcmysql.get_coltypes_mysql_list(ms_curs, s_schema, "ia_finding_5")
    # print(a_cols)
    # Open the SOURCE file to obtain column headings
    print("Build mysql vss gl monthly balance columns...")
    funcfile.writelog("GET COLUMN NAME: ia_finding_5")
    s_head = funcmysql.get_colnames_sqlite_text(so_curs, "X002ex_vss_gl_balance_month", "ia_find5_")
    s_head = "(`ia_find_auto`, " + s_head.rstrip(", ") + ")"
    # print(s_head)
    # Open the SOURCE file to obtain the data
    print("Insert mysql vss gl monthly balance data...")
    with sqlite3.connect(so_path+so_file) as rs_conn:
        rs_conn.row_factory = sqlite3.Row
    rs_curs = rs_conn.cursor()
    rs_curs.execute("SELECT * FROM X002ex_vss_gl_balance_month")
    rows = rs_curs.fetchall()
    i_tota = 0
    i_coun = 0
    for row in rows:
        s_data = funcmysql.convert_sqlite_mysql(row, a_cols, 5, 2)
        s_sql = "INSERT IGNORE INTO `ia_finding_5` " + s_head + " VALUES " + s_data + ";"
        ms_curs.execute(s_sql)
        i_tota = i_tota + 1
        i_coun = i_coun + 1
        if i_coun == 100:
            ms_cnxn.commit()
            i_coun = 0
    ms_cnxn.commit()
    print("Inserted " + str(i_tota) + " rows...")
    funcfile.writelog("%t POPULATE MYSQL: " + str(i_tota) + " STUD DEBT MONTHLY BAL rows (ia_finding_5)")
    if funcconf.l_mess_project:
        funcsms.send_telegram("", "administrator", "<b> " + str(i_tota) + "</b> Student debtor balance upload")

    """*****************************************************************************
    EXPORT STUD DEBTOR COMPARISON CAMPUS MONTH SUMMARY
    *****************************************************************************"""
    print("EXPORT STUD DEBTOR COMPARISON CAMPUS MONTH SUMMARY")
    funcfile.writelog("EXPORT STUD DEBTOR COMPARISON CAMPUS MONTH SUMMARY")

    # EXPORT MYSQL VSS GL COMPARISON PER CAMPUS PER MONTH TO WEB
    print("Build mysql vss gl comparison campus month...")
    ms_curs.execute("DROP TABLE IF EXISTS ia_finding_6")
    funcfile.writelog("%t DROP MYSQL TABLE: ia_finding_6")
    s_sql = """
    CREATE TABLE IF NOT EXISTS ia_finding_6 (
    ia_find_auto INT(11),
    ia_find6_auto INT(11) AUTO_INCREMENT,
    ia_find6_campus VARCHAR(20),
    ia_find6_month VARCHAR(2),
    ia_find6_trancode VARCHAR(5),
    ia_find6_vss_description VARCHAR(150),
    ia_find6_vss_amount DECIMAL(20,2),
    ia_find6_gl_description VARCHAR(150),
    ia_find6_gl_amount DECIMAL(20,2),
    ia_find6_diff DECIMAL(20,2),
    ia_find6_matched VARCHAR(2),
    ia_find6_period VARCHAR(7),
    ia_find6_current VARCHAR(1),
    PRIMARY KEY (ia_find6_auto),
    INDEX fb_order_ia_find6_campus_INDEX (ia_find6_campus),
    INDEX fb_order_ia_find6_month_INDEX (ia_find6_month)
    )
    ENGINE = InnoDB
    CHARSET=utf8mb4
    COLLATE utf8mb4_unicode_ci
    COMMENT = 'Table to store vss and gl monthly comparisons'
    """ + ";"
    ms_curs.execute(s_sql)
    funcfile.writelog("%t CREATE MYSQL TABLE: ia_finding_6 (vss gl comparison per campus per month)")
    # Obtain the new mysql table column types
    print("Build mysql current people structure columns types...")
    funcfile.writelog("GET COLUMN TYPE: People")
    a_cols = funcmysql.get_coltypes_mysql_list(ms_curs, s_schema, "ia_finding_6")
    # print(a_cols)
    # Open the SOURCE file to obtain column headings
    print("Build mysql vss gl comparison columns...")
    funcfile.writelog("GET COLUMN NAME: ia_finding_6")
    s_head = funcmysql.get_colnames_sqlite_text(so_curs, "X003ax_vss_gl_join", "ia_find6_")
    s_head = "(ia_find_auto, " + s_head.rstrip(", ") + ")"
    # print(s_head)
    # Open the SOURCE file to obtain the data
    print("Insert mysql vss gl comparison data...")
    with sqlite3.connect(so_path+so_file) as rs_conn:
        rs_conn.row_factory = sqlite3.Row
    rs_curs = rs_conn.cursor()
    rs_curs.execute("SELECT * FROM X003ax_vss_gl_join")
    rows = rs_curs.fetchall()
    i_tota = 0
    i_coun = 0
    for row in rows:
        s_data = funcmysql.convert_sqlite_mysql(row, a_cols, 6, 2)
        s_sql = "INSERT IGNORE INTO `ia_finding_6` " + s_head + " VALUES " + s_data + ";"
        ms_curs.execute(s_sql)
        i_tota = i_tota + 1
        i_coun = i_coun + 1
        if i_coun == 100:
            ms_cnxn.commit()
            i_coun = 0
    ms_cnxn.commit()
    print("Inserted " + str(i_tota) + " rows...")
    funcfile.writelog("%t POPULATE MYSQL: " + str(i_tota) + " STUD DEBT COMPARISON rows (ia_finding_6)")

    # CLOSE PEOPLE DATABASE
    so_conn.commit()
    so_conn.close()

    """*****************************************************************************
    END OF SCRIPT
    *****************************************************************************"""
    print("END OF SCRIPT")
    funcfile.writelog("END OF SCRIPT")

    # CLOSE MYSQL DATABASES
    ms_cnxn.commit()
    ms_cnxn.close()

    # CLOSE THE LOG WRITER
    funcfile.writelog("---------------------------")
    funcfile.writelog("COMPLETED: B005_MYSQL_LISTS")

    return


if __name__ == '__main__':
    try:
        mysql_lists("Web_ia_nwu")
        # mysql_lists(("Mysql_ia_server"))
    except Exception as e:
        funcsys.ErrMessage(e, funcconf.l_mess_project, "B005_mysql_lists", "B005_mysql_lists")
