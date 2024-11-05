"""
Script to test STUDENT FEES
Created on: 28 Aug 2019
Author: Albert J v Rensburg (NWU21162395)
"""

# IMPORT PYTHON MODULES
import sqlite3
import csv
import zipfile

# IMPORT OWN MODULES
from _my_modules import funcconf
from _my_modules import funccsv
from _my_modules import funcdatn
from _my_modules import funcfile
from _my_modules import funcstat
from _my_modules import funcsys
from _my_modules import funcsms
from _my_modules import functest
from _my_modules import funcmail

""" INDEX **********************************************************************
ENVIRONMENT
OPEN THE DATABASES
TEMPORARY AREA
BEGIN OF SCRIPT
END OF SCRIPT
*****************************************************************************"""


def student_fee(s_period: str = "curr"):
    """
    Script to test STUDENT FEE INCOME
    :param s_period: str: The financial period
    :return: Nothing
    """

    """*****************************************************************************
    ENVIRONMENT
    *****************************************************************************"""

    # DECLARE VARIABLES
    if s_period == "prev":
        s_year = funcdatn.get_previous_year()
    else:
        s_year = funcdatn.get_current_year()

    ed_path = f"{funcconf.drive_system}_external_data/"  # External data path
    so_path = f"{funcconf.drive_data_raw}Vss_fee/"  # Source database path
    re_path = f"{funcconf.drive_data_results}Vss/" + s_year

    if s_period == "2019":
        f_reg_fee = 1830.00
        d_sem1_con = "2019-03-05"
        d_sem1_dis = "2019-03-05"
        d_sem2_con = "2019-08-09"
        d_sem2_dis = "2019-08-09"
        d_test_overcharge = "2019-07-15"  # Only month and day used
        so_file = "Vss_test_fee_2019.sqlite"  # Source database
        s_reg_trancode: str = "095"
        s_qual_trancode: str = "004"
        s_modu_trancode: str = "004"
        s_burs_trancode: str = "042z052z381z500"
        # Find these id's from Sqlite->Sqlite_vss_test_fee->Q021aa_qual_nofee_loaded
        s_mba: str = "71500z2381692z2381690z665559"  # Exclude these FQUALLEVELAPID
        s_mpa: str = "665566z618161z618167z618169"  # Exclude these FQUALLEVELAPID
        s_aud: str = "71839z71840z71841z71842z71820z71821z71822z1085390"  # Exclude these FQUALLEVELAPID
        l_record: bool = False
        l_export: bool = True
    elif s_period == "2020":
        f_reg_fee = 1930.00
        d_sem1_con = "2020-02-21"
        d_sem1_dis = "2020-03-09"
        d_sem2_con = "2020-09-04"
        d_sem2_dis = "2020-09-04"
        d_test_overcharge = "2020-07-15"  # Only month and day used
        so_file = "Vss_test_fee_2020.sqlite"  # Source database
        s_reg_trancode: str = "095"
        s_qual_trancode: str = "004"
        s_modu_trancode: str = "004"
        s_burs_trancode: str = "042z052z381z500"
        # Find these id's from Sqlite->Sqlite_vss_test_fee->Q021aa_qual_nofee_loaded
        s_mba: str = "71500z2381692z2381690z665559"  # Exclude these FQUALLEVELAPID
        s_mpa: str = "665566z618161z618167z618169"  # Exclude these FQUALLEVELAPID
        s_aud: str = "71839z71840z71841z71842z71820z71821z71822z1085390"  # Exclude these FQUALLEVELAPID
        l_record: bool = False
        l_export: bool = True
    elif s_period == "2021":
        f_reg_fee = 2020.00
        d_sem1_con = "2021-04-09"
        d_sem1_dis = "2021-04-09"
        d_sem2_con = "2021-09-04"
        d_sem2_dis = "2021-09-04"
        d_test_overcharge = "2021-07-15"  # Only month and day used
        so_file = "Vss_test_fee_2021.sqlite"  # Source database
        s_reg_trancode: str = "095"
        s_qual_trancode: str = "004"
        s_modu_trancode: str = "004"
        s_burs_trancode: str = "042z052z381z500"
        # Find these id's from Sqlite->Sqlite_vss_test_fee->Q021aa_qual_nofee_loaded
        s_mba: str = "71500z2381692z2381690z665559"  # Exclude these FQUALLEVELAPID
        s_mpa: str = "665566z618161z618167z618169"  # Exclude these FQUALLEVELAPID
        s_aud: str = "71839z71840z71841z71842z71820z71821z71822z1085390"  # Exclude these FQUALLEVELAPID
        l_record: bool = False
        l_export: bool = True
    elif s_period == "prev":
        f_reg_fee = 2110.00
        d_sem1_con = "2022-03-11"
        d_sem1_dis = "2022-03-11"
        d_sem2_con = "2022-08-05"
        d_sem2_dis = "2022-08-05"
        d_test_overcharge = "2022-07-15"  # Only month and day used
        so_file = "Vss_test_fee_prev.sqlite"  # Source database
        s_reg_trancode: str = "095"
        s_qual_trancode: str = "004"
        s_modu_trancode: str = "004"
        s_burs_trancode: str = "042z052z381z500"
        # Find these id's from Sqlite->Sqlite_vss_test_fee->Q021aa_qual_nofee_loaded
        s_mba: str = "71500z2381692z2381690z665559"  # Exclude these FQUALLEVELAPID
        s_mpa: str = "665566z618161z618167z618169"  # Exclude these FQUALLEVELAPID
        s_aud: str = "71839z71840z71841z71842z71820z71821z71822z1085390"  # Exclude these FQUALLEVELAPID
        l_record: bool = False
        l_export: bool = True
    else:
        f_reg_fee = 2220.00
        d_sem1_con = "2023-03-03"
        d_sem1_dis = "2023-03-03"
        d_sem2_con = "2023-08-04"
        d_sem2_dis = "2023-08-04"
        d_test_overcharge = "2023-07-15"  # Only month and day used
        so_file = "Vss_test_fee.sqlite"  # Source database
        s_reg_trancode: str = "095"
        s_qual_trancode: str = "004"
        s_modu_trancode: str = "004"
        s_burs_trancode: str = "042z052z381z500"
        # Find these id's from Sqlite->Sqlite_vss_test_fee->Q021aa_qual_nofee_loaded
        s_mba: str = "71500z2381692z2381690z665559"  # Exclude these FQUALLEVELAPID
        s_mpa: str = "665566z618161z618167z618169"  # Exclude these FQUALLEVELAPID
        s_aud: str = "71839z71840z71841z71842z71820z71821z71822z1085390"  # Exclude these FQUALLEVELAPID
        l_record: bool = True
        l_export: bool = True

    l_debug: bool = True
    # l_mail: bool = funcconf.l_mail_project
    l_mail: bool = False
    # l_mess: bool = funcconf.l_mess_project
    l_mess: bool = False
    s_desc: str = ""

    # SCRIPT LOG FILE
    if l_debug:
        print("---------------------")
        print("C302_TEST_STUDENT_FEE")
        print("---------------------")
    funcfile.writelog("Now")
    funcfile.writelog("SCRIPT: C302_TEST_STUDENT_FEE")
    funcfile.writelog("-----------------------------")

    # MESSAGE
    if l_mess:
        funcsms.send_telegram('', 'administrator', '<b>C302 Student fee income tests</b>')

    """*****************************************************************************
    OPEN THE DATABASES
    *****************************************************************************"""
    if l_debug:
        print("OPEN THE DATABASES")
    funcfile.writelog("OPEN THE DATABASES")

    # OPEN SQLITE SOURCE table
    if l_debug:
        print("Open sqlite database...")
    with sqlite3.connect(so_path + so_file) as so_conn:
        so_curs = so_conn.cursor()
    funcfile.writelog("OPEN DATABASE: " + so_file)

    # ATTACH VSS DATABASE
    if l_debug:
        print("Attach vss database...")
    so_curs.execute(f"ATTACH DATABASE '{funcconf.drive_data_raw}Vss/Vss.sqlite' AS 'VSS'")
    funcfile.writelog("%t ATTACH DATABASE: Vss.sqlite")
    so_curs.execute(f"ATTACH DATABASE '{funcconf.drive_data_raw}Vss/Vss_curr.sqlite' AS 'VSSCURR'")
    funcfile.writelog("%t ATTACH DATABASE: Vss_curr.sqlite")
    so_curs.execute(f"ATTACH DATABASE '{funcconf.drive_data_raw}Vss/Vss_prev.sqlite' AS 'VSSPREV'")
    funcfile.writelog("%t ATTACH DATABASE: Vss_prev.sqlite")
    so_curs.execute(f"ATTACH DATABASE '{funcconf.drive_data_raw}People/People.sqlite' AS 'PEOPLE'")
    funcfile.writelog("%t ATTACH DATABASE: People.sqlite")

    """ ****************************************************************************
    TEMPORARY AREA
    *****************************************************************************"""
    if l_debug:
        print("TEMPORARY AREA")
    funcfile.writelog("TEMPORARY AREA")

    """
    import zipfile

    file_name = 'example.zip'  # Name of zip file to be created

    # List of files to be zipped 
    files_to_zip = ['example1.txt', 'example2.txt']

    archive = zipfile.ZipFile(file_name, 'w')

    # Adding files to zip
    for file in files_to_zip:
        archive.write(file, compress_type=zipfile.ZIP_DEFLATED)

        # Closing file
    archive.close()
    """

    """ ****************************************************************************
    BEGIN OF SCRIPT
    *****************************************************************************"""
    if l_debug:
        print("BEGIN OF SCRIPT")
    funcfile.writelog("BEGIN OF SCRIPT")


    # JOIN STUDENTS AND TRANSACTIONS
    if l_debug:
        print("Join students and transactions...")
    sr_file = "X020bx_Student_master_sort"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        Case
            When HALF.FEE_COUNT_HALF = 2 Then 1
            When STUD.CONV_IND != '' And STUD.FEE_MODE = STUD.FEE_QUAL_TOTAL Then 1
            When STUD.CONV_IND != '' And STUD.FEE_LEVIED > 0 And STUD.FEE_LEVIED = Round(STUD.FEE_QUAL_TOTAL/2,2) Then 1 
            When STUD.CONV_IND != '' And STUD.FEE_LEVIED > 0 And STUD.FEE_QUAL_TOTAL >= STUD.FEE_LEVIED Then 1
            When STUD.CONV_IND != '' And FEE_QUAL_TOTAL > 0 And FEE_SHOULD_BE Like('%HALF%') And FEE_MODE_HALF <= FEE_QUAL_TOTAL Then 1 
            When STUD.CONV_IND != '' And FEE_QUAL_TOTAL > 0 And FEE_SHOULD_BE Like('%FULL%') And FEE_MODE <= FEE_QUAL_TOTAL Then 1
            Else VALID
        End As VALID,
        STUD.CAMPUS,
        STUD.KSTUDBUSENTID,
        STUD.FEE_LEVIED,
        STUD.FEE_MODE,
        STUD.FEE_MODE_HALF,
        STUD.CONV_IND,
        STUD.FEE_QUAL_TOTAL,
        HALF.FEE_COUNT_HALF,
        STUD.FEE_SHOULD_BE,
        STUD.FEE_LEVIED_TYPE,
        STUD.QUALIFICATION,
        STUD.QUALIFICATION_NAME,
        STUD.QUAL_TYPE,
        STUD.QUAL_TYPE_FEE,
        STUD.ACTIVE_IND,
        STUD.LEVY_CATEGORY,
        STUD.PRESENT_CAT,
        STUD.ENROL_CAT,
        STUD.ENTRY_LEVEL,
        STUD.STATUS_FINAL,
        STUD.FSITE,
        STUD.ORGUNIT_NAME,
        STUD.DATEQUALLEVELSTARTED,
        STUD.STARTDATE,
        STUD.DATEENROL,
        STUD.DISCONTINUEDATE,
        STUD.DAYS_REG,
        STUD.DISCDATE_CALC,
        STUD.RESULTPASSDATE,
        STUD.DAYS_PASS,
        STUD.RESULTISSUEDATE,
        STUD.RESULT,
        STUD.CEREMONYDATETIME,
        STUD.CEREMONY,
        STUD.ISHEMISSUBSIDY,
        STUD.ISMAINQUALLEVEL,
        STUD.ENROLACADEMICYEAR,
        STUD.ENROLHISTORYYEAR,
        STUD.CALCHISTORYYEAR,
        STUD.FEEHISTORYYEAR,        
        STUD.SEM0,
        STUD.SEM1,
        STUD.SEM2,
        STUD.SEM7,
        STUD.SEM8,
        STUD.SEM9,
        STUD.REG_FEE_TYPE,
        STUD.FEE_BURS,
        STUD.MIN,
        STUD.MIN_UNIT,
        STUD.MAX,
        STUD.MAX_UNIT,
        STUD.CERT_TYPE,
        STUD.LEVY_TYPE,
        STUD.BLACKLIST,
        STUD.LONG,
        STUD.DISCONTINUE_REAS,
        STUD.POSTPONE_REAS,
        STUD.FBUSINESSENTITYID,
        STUD.ORGUNIT_TYPE,
        STUD.ISCONDITIONALREG,
        STUD.MARKSFINALISEDDATE,
        STUD.EXAMSUBMINIMUM,
        STUD.ISCUMLAUDE,
        STUD.ISPOSSIBLEGRADUATE,
        STUD.FACCEPTANCETESTCODEID,
        STUD.FENROLMENTPRESENTATIONID,
        STUD.FQUALLEVELAPID,
        STUD.FPROGRAMAPID,
        STUD.TRAN_COUNT,
        STUD.FUSERBUSINESSENTITYID,
        STUD.NAME_ADDR,
        STUD.FAUDITUSERCODE,
        STUD.SYSTEM_DESC
    From
        X020ba_Student_master STUD Left Join
        X020bb_Student_multiple_half HALF On HALF.KSTUDBUSENTID = STUD.KSTUDBUSENTID
    Order By
        STUD.FEE_LEVIED_TYPE,
        STUD.FEE_SHOULD_BE
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)
    if l_export and funcsys.tablerowcount(so_curs, sr_file) > 0:
        if l_debug:
            print("Export findings...")
        sx_path = re_path + "/"
        sx_file = "Student_fee_test_020bx_qual_fee_studentlist_"
        sx_file_dated = sx_file + funcdatn.get_today_date_file()
        s_head = funccsv.get_colnames_sqlite(so_conn, sr_file)
        funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file, s_head)
        # funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file_dated, s_head)
        funcfile.writelog("%t EXPORT DATA: " + sx_path + sx_file)

        # Zip the file
        zip_file_name = sx_path + 'Student_fee_test_020bx_qual_fee_studentlist_.zip'  # Name of zip file to be created
        # files_to_zip = ['example1.txt', 'example2.txt']  # List of files to be zipped
        files_to_zip = [sx_path + 'Student_fee_test_020bx_qual_fee_studentlist_.csv']  # List of files to be zipped
        archive = zipfile.ZipFile(zip_file_name, 'w')
        # Adding files to zip
        for file in files_to_zip:
            archive.write(file, compress_type=zipfile.ZIP_DEFLATED)
        archive.close()  # Closing file
        funcfile.writelog("%t COMPRESS DATA: " + zip_file_name)

    if l_mail:
        funcmail.send_mail('vss_list_020bx_studentlist')

    # MESSAGE
    # if l_mess:
    #     funcsms.send_telegram('', 'administrator', '<b>STUDENT FEE</b> income tests end.')

    """ ****************************************************************************
    END OF SCRIPT
    *****************************************************************************"""
    if l_debug:
        print("END OF SCRIPT")
    funcfile.writelog("END OF SCRIPT")

    # CLOSE THE DATABASE CONNECTION
    so_conn.commit()
    so_conn.close()

    # CLOSE THE LOG WRITER *********************************************************
    funcfile.writelog("--------------------------------")
    funcfile.writelog("COMPLETED: C302_TEST_STUDENT_FEE")

    return


if __name__ == '__main__':
    try:
        student_fee("curr")
    except Exception as e:
        funcsys.ErrMessage(e)
