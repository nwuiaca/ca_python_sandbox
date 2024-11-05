# IMPORT SYSTEM MODULES

# IMPORT OWN MODULES
from _my_modules import funcfile
from _my_modules import funcmail
from _my_modules import funcdatn
from _my_modules import funcsms
from _my_modules import funcsys

""" SCRIPT TO RUN ON A SCHEDULED TIME FOR ALL PYTHON SCRIPTS *******************
Author: Albert J van Rensburg (NWU21162395)
****************************************************************************"""

""" INDEX *********************************************************************
ENVIRONMENT

* ORACLE TO SQLITE (A001_oracle_to_sqlite) "TueWedThuFriSat"
* LOGS (A002_log) "MonTueWedThuFriSatSun" 

* PEOPLE LISTS (import B001_people_lists) "TueWedThuFriSat"
* KFS LISTS (import B002_kfs_lists) "TueWedThuFriSat"
* KFS GL AND PAYMENT LISTS (B006_kfs_period_list) "TueWedThuFriSat"
* VSS LISTS (B003_vss_lists) "TueWedThuFriSat"
* VSS PERIOD LISTS (B007_vss_period_list) "TueWedThuFriSat"

* PEOPLE LIST MASTERFILE (C003_people_list_masterfile) "MonTueWedThuFri"
* PEOPLE MASTER FILE TESTS (C001_people_test_masterfile) "MonTueWedThuFri"
* PEOPLE CONFLICT TESTS (C002_people_test_conflict) "MonTueWedThuFri"

* KFS CREDITOR PAYMENT TESTS (C201_creditor_test_payments) "MonTueWedThuFri" 
* KFS GL TEST TRANSACTIONS TESTS (C202_gl_test_transactions) "MonTueWedThuFri"
* KFS VSS STUDENT DEBTOR RECONCILIATION AND TESTS (C200_report_studdeb_recon) "MonTueWedThuFri"

* VSS STUDENT MASTER FILE TESTS (C300_test_student_general) 
* VSS STUDENT FEE TESTS AND REPORTS (C302_test_student_fee) "MonTueWedThuFri"
* VSS STUDENT DEFERMENT MASTER FILE (C301_report_student_deferment) "MonTueWedThuFri"

* MYSQL LISTS WEB SERVER (B005_mysql_lists) "TueWedThuFriSat"
* MYSQL LISTS ACL SERVER (B005_mysql_lists) "TueWedThuFriSat"
****************************************************************************"""

"""****************************************************************************
ENVIRONMENT
****************************************************************************"""

# DECLARE VARIABLES
l_mail = True
l_mess = True

# MESSAGES TO ADMIN
if l_mail:
    funcmail.send_mail('std_success_nwu', 'Python:Success:Start_runall', 'NWUIAPython: Success: Start runall')
if l_mess:
    funcsms.send_telegram('', 'administrator', 'Downloading <b>kfs</b> data from oracle and <b>running</b> tests.')

# OPEN THE SCRIPT LOG FILE
print("------------")
print("A000_RUN_ALL")
print("------------")
funcfile.writelog("Now")
funcfile.writelog("SCRIPT: A000_RUN_ALL")
funcfile.writelog("--------------------")

"""****************************************************************************
ORACLE TO SQLITE
****************************************************************************"""

if funcdatn.get_today_name() in "TueWedThuFriSat":
    import A001_oracle_to_sqlite
    try:
        A001_oracle_to_sqlite.oracle_to_sqlite()
        if l_mail:
            funcmail.send_mail('std_success_nwu', 'NWUIAPython:Success:A001_oracle_to_sqlite',
                          'NWUIAPython: Success: A001_oracle_to_sqlite')
    except Exception as e:
        funcsys.ErrMessage(e, l_mail, 'NWUIAPython:Fail:A001_oracle_to_sqlite',
                           'NWUIAPython: Fail: A001_oracle_to_sqlite')
else:
    print("ORACLE to SQLITE do not run on Sundays and Mondays")
    funcfile.writelog("SCRIPT: A001_ORACLE_TO_SQLITE: DO NOT RUN ON SUNDAYS AND MONDAYS")

"""****************************************************************************
LOGS
****************************************************************************"""

if funcdatn.get_today_name() in "MonTueWedThuFriSatSun":
    import A002_log
    try:
        A002_log.log_capture(funcdatn.get_yesterday_date(), True)
        if l_mail:
            funcmail.send_mail('std_success_nwu', 'NWUIAPython:Success:A002_log',
                          'NWUIAPython: Success: A002_log')
    except Exception as e:
        funcsys.ErrMessage(e, l_mail, 'NWUIAPython:Fail:A002_log',
                           'NWUIAPython: Fail: A002_log')
else:
    print("LOGS run every day")
    funcfile.writelog("SCRIPT: A002_LOG: RUN EVERY DAY")

"""****************************************************************************
PEOPLE LISTS
****************************************************************************"""

if funcdatn.get_today_name() in "TueWedThuFriSat":
    import B001_people_lists
    try:
        B001_people_lists.people_lists()
        if l_mail:
            funcmail.send_mail('std_success_nwu', 'NWUIAPython:Success:B001_people_lists',
                          'NWUIAPython: Success: B001_people_lists')
    except Exception as e:
        funcsys.ErrMessage(e, l_mail, 'NWUIAPython:Fail:B001_people_lists',
                           'NWUIAPython: Fail: B001_people_lists')
else:
    print("PEOPLE lists do not run on Sundays and Mondays")
    funcfile.writelog("SCRIPT: B001_PEOPLE_LISTS: DO NOT RUN ON SUNDAYS AND MONDAYS")

"""****************************************************************************
KFS LISTS
****************************************************************************"""

if funcdatn.get_today_name() in "TueWedThuFriSat":
    import B002_kfs_lists
    try:
        B002_kfs_lists.kfs_lists()
        if l_mail:
            funcmail.send_mail('std_success_nwu', 'NWUIAPython:Success:B002_kfs_lists',
                          'NWUIAPython: Success: B002_kfs_lists')
    except Exception as e:
        funcsys.ErrMessage(e, l_mail, 'NWUIAPython:Fail:B002_kfs_lists',
                           'NWUIAPython: Fail: B002_kfs_lists')
else:
    print("KFS lists do not run on Sundays and Mondays")
    funcfile.writelog("SCRIPT: B002_KFS_LISTS: DO NOT RUN ON SUNDAYS AND MONDAYS")

"""****************************************************************************
KFS GL AND PAYMENT LISTS
****************************************************************************"""

# CURRENT YEAR
if funcdatn.get_today_name() in "TueWedThuFriSat":
    import B006_kfs_period_list
    try:
        B006_kfs_period_list.kfs_period_list("curr")
        if l_mail:
            funcmail.send_mail('std_success_nwu', 'NWUIAPython:Success:B006_kfs_period_list',
                          'NWUIAPython: Success: B006_kfs_period_list')
    except Exception as e:
        funcsys.ErrMessage(e, l_mail, 'NWUIAPython:Fail:B006_kfs_period_list',
                           'NWUIAPython: Fail: B006_kfs_period_list')
else:
    print("KFS GL and Payment lists do not run on Sundays and Mondays")
    funcfile.writelog("SCRIPT: B006_KFS_PERIOD_LIST: DO NOT RUN ON SUNDAYS AND MONDAYS")

# PREVIOUS YEAR
if funcdatn.get_today_name() in "TueWedThuFriSat":
    import B006_kfs_period_list
    try:
        B006_kfs_period_list.kfs_period_list("prev")
        if l_mail:
            funcmail.send_mail('std_success_nwu', 'NWUIAPython:Success:B006_kfs_period_list_prev',
                          'NWUIAPython: Success: B006_kfs_period_list_prev')
    except Exception as e:
        funcsys.ErrMessage(e, l_mail, 'NWUIAPython:Fail:B006_kfs_period_list_prev',
                           'NWUIAPython: Fail: B006_kfs_period_list_prev')
else:
    print("KFS GL and Payment lists do not run on Sundays and Mondays")
    funcfile.writelog("SCRIPT: B006_KFS_PERIOD_LIST_PREV: DO NOT RUN ON SUNDAYS AND MONDAYS")

"""****************************************************************************
VSS LISTS
****************************************************************************"""

if funcdatn.get_today_name() in "TueWedThuFriSat":
    import B003_vss_lists
    try:
        B003_vss_lists.vss_lists()
        if l_mail:
            funcmail.send_mail('std_success_nwu', 'NWUIAPython:Success:B003_vss_lists',
                          'NWUIAPython: Success: B003_vss_lists')
    except Exception as e:
        funcsys.ErrMessage(e, l_mail, 'NWUIAPython:Fail:B003_vss_lists',
                           'NWUIAPython: Fail: B003_vss_lists')
else:
    print("VSS lists do not run on Sundays and Mondays")
    funcfile.writelog("SCRIPT: B003_VSS_LISTS: DO NOT RUN ON SUNDAYS AND MONDAYS")

"""****************************************************************************
VSS PERIOD LISTS
****************************************************************************"""

if funcdatn.get_today_name() in "TueWedThuFriSat":
    import B007_vss_period_list
    try:
        B007_vss_period_list.vss_period_list("curr")
        if l_mail:
            funcmail.send_mail('std_success_nwu', 'NWUIAPython:Success:B007_vss_period_list',
                          'NWUIAPython: Success: B007_vss_period_list')
    except Exception as e:
        funcsys.ErrMessage(e, l_mail, 'NWUIAPython:Fail:B007_vss_period_list',
                           'NWUIAPython: Fail: B007_vss_period_list')
else:
    print("VSS PERIOD LISTS do not run on Sundays and Mondays")
    funcfile.writelog("SCRIPT: B007_VSS_PERIOD_LISTS: DO NOT RUN ON SUNDAYS AND MONDAYS")

""" ***************************************************************************
PEOPLE LIST MASTERFILE
****************************************************************************"""

if funcdatn.get_today_name() in "MonTueWedThuFri":
    import C003_people_list_masterfile
    try:
        C003_people_list_masterfile.people_list_masterfile()
        if l_mail:
            funcmail.send_mail('std_success_nwu', 'NWUIAPython:Success:C003_people_list_masterfile',
                          'NWUIAPython: Success: C003_people_list_masterfile')
    except Exception as e:
        funcsys.ErrMessage(e, l_mail, 'NWUIAPython:Fail:C003_people_list_masterfile',
                           'NWUIAPython: Fail: C003_people_list_masterfile')
else:
    print("PEOPLE MASTERFILE LISTS do not run on Saturdays and Sundays")
    funcfile.writelog("SCRIPT: C003_PEOPLE_LIST_MASTERFILE_RUN: DO NOT RUN ON SATURDAYS AND SUNDAYS")

"""****************************************************************************
PEOPLE MASTER FILE TESTS
****************************************************************************"""

if funcdatn.get_today_name() in "MonTueWedThuFri":
    import C001_people_test_masterfile
    try:
        C001_people_test_masterfile.people_test_masterfile()
        if l_mail:
            funcmail.send_mail('std_success_nwu', 'NWUIAPython:Success:C001_people_test_masterfile',
                          'NWUIAPython: Success: C001_people_test_masterfile')
    except Exception as e:
        funcsys.ErrMessage(e, l_mail, 'NWUIAPython:Fail:C001_people_test_masterfile',
                           'NWUIAPython: Fail: C001_people_test_masterfile')
else:
    print("PEOPLE MASTER FILE TESTS do not run on Saturdays and Sundays")
    funcfile.writelog("SCRIPT: C001_PEOPLE_TEST_MASTERFILE: DO NOT RUN ON SATURDAYS AND SUNDAYS")

"""****************************************************************************
PEOPLE CONFLICT TESTS
****************************************************************************"""

if funcdatn.get_today_name() in "MonTueWedThuFri":
    import C002_people_test_conflict
    try:
        C002_people_test_conflict.people_test_conflict()
        if l_mail:
            funcmail.send_mail('std_success_nwu', 'NWUIAPython:Success:C002_people_test_conflict',
                          'NWUIAPython: Success: C002_people_test_conflict')
    except Exception as e:
        funcsys.ErrMessage(e, l_mail, 'NWUIAPython:Fail:C002_people_test_conflict',
                           'NWUIAPython: Fail: C002_people_test_conflict')
else:
    print("PEOPLE CONFLICT TESTS do not run on Saturdays and Sundays")
    funcfile.writelog("SCRIPT: C001_PEOPLE_TEST_CONFLICT: DO NOT RUN ON SATURDAYS AND SUNDAYS")

"""****************************************************************************
KFS CREDITOR PAYMENT TESTS
****************************************************************************"""

if funcdatn.get_today_name() in "MonTueWedThuFri":
    import C201_creditor_test_payments
    try:
        C201_creditor_test_payments.creditor_test_payments()
        if l_mail:
            funcmail.send_mail('std_success_nwu', 'NWUIAPython:Success:C201_creditor_test_payments',
                          'NWUIAPython: Success: C201_creditor_test_payments')
    except Exception as e:
        funcsys.ErrMessage(e, l_mail, 'NWUIAPython:Fail:C201_creditor_test_payments',
                           'NWUIAPython: Fail: C201_creditor_test_payments')
else:
    print("KFS CREDITOR PAYMENT TESTS do not run on Saturdays and Sundays")
    funcfile.writelog("SCRIPT: C201_CREDITOR_TEST_PAYMENTS: DO NOT RUN ON SATURDAYS AND SUNDAYS")

"""****************************************************************************
KFS GL TEST TRANSACTIONS TESTS
****************************************************************************"""

if funcdatn.get_today_name() in "MonTueWedThuFri":
    import C202_gl_test_transactions
    try:
        C202_gl_test_transactions.gl_test_transactions()
        if l_mail:
            funcmail.send_mail('std_success_nwu', 'NWUIAPython:Success:C202_gl_test_transactions', 'NWUIAPython: Success: C202_gl_test_transactions')
    except Exception as e:
        funcsys.ErrMessage(e, l_mail, 'NWUIAPython:Fail:C202_gl_test_transactions', 'NWUIAPython: Fail: C202_gl_test_transactions')
else:
    print("GL TEST TRANSACTIONS TESTS do not run on Saturdays and Sundays")
    funcfile.writelog("SCRIPT: C202_gl_test_transactions: DO NOT RUN ON SATURDAYS AND SUNDAYS")

"""****************************************************************************
KFS VSS STUDENT DEBTOR RECONCILIATION AND TESTS
****************************************************************************"""

if funcdatn.get_today_name() in "MonTueWedThuFri":
    import C200_report_studdeb_recon
    try:
        C200_report_studdeb_recon.report_studdeb_recon('48501952.09', '-12454680.98', '49976048.39', "curr")
        # 2019 balances
        # C200_report_studdeb_recon.report_studdeb_recon('66561452.48','-18340951.06','39482933.18')
        if l_mail:
            funcmail.send_mail('std_success_nwu', 'NWUIAPython:Success:C200_report_studdeb_recon',
                          'NWUIAPython: Success: C200_report_studdeb_recon')
    except Exception as e:
        funcsys.ErrMessage(e, l_mail, 'NWUIAPython:Fail:C200_report_studdeb_recon',
                           'NWUIAPython: Fail: C200_report_studdeb_recon')
else:
    print("C200_REPORT_STUDDEB_RECON only run on 1-4 and 10th of the month")
    funcfile.writelog("SCRIPT: C200_REPORT_STUDDEB_RECON: ONLY RUN ON 1-4 AND 10TH OF THE MONTH")

"""****************************************************************************
VSS STUDENT MASTER FILE TESTS
****************************************************************************"""

if funcdatn.get_today_day() in "01z13":
    import C300_test_student_general
    try:
        C300_test_student_general.test_student_general()
        if l_mail:
            funcmail.send_mail('std_success_nwu', 'NWUIAPython:Success:C300_test_student_general',
                          'NWUIAPython: Success: C300_test_student_general')
    except Exception as e:
        funcsys.ErrMessage(e, l_mail, 'NWUIAPython:Fail:C300_test_student_general',
                           'NWUIAPython: Fail: C300_test_student_general')
else:
    print("C300_TEST_STUDENT_GENERAL only run on 1st of the month")
    funcfile.writelog("SCRIPT: C300_TEST_STUDENT_GENERAL: ONLY RUN ON 1ST OF THE MONTH")


"""****************************************************************************
VSS STUDENT FEE TESTS AND REPORTS
****************************************************************************"""

if funcdatn.get_today_name() in "MonTueWedThuFri":
    import C302_test_student_fee
    try:
        C302_test_student_fee.student_fee()
        if l_mail:
            funcmail.send_mail('std_success_nwu', 'NWUIAPython:Success:C302_test_student_fee',
                          'NWUIAPython: Success: C302_test_student_fee')
    except Exception as e:
        funcsys.ErrMessage(e, l_mail, 'NWUIAPython:Fail:C302_test_student_fee',
                           'NWUIAPython: Fail: C302_test_student_fee')
else:
    print("VSS STUDENT FEE TESTS do not run on Saturdays and Sundays")
    funcfile.writelog("SCRIPT: C302_TEST_STUDENT_FEE: DO NOT RUN ON SATURDAYS AND SUNDAYS")

"""****************************************************************************
VSS STUDENT DEFERMENT MASTER FILE
****************************************************************************"""

if funcdatn.get_today_name() in "MonTueWedThuFri":
    import C301_report_student_deferment
    try:
        C301_report_student_deferment.studdeb_deferments('curr', funcdatn.get_current_year())
        if l_mail:
            funcmail.send_mail('std_success_nwu', 'NWUIAPython:Success:C301_report_student_deferment',
                          'NWUIAPython: Success: C301_report_student_deferment')
    except Exception as e:
        funcsys.ErrMessage(e, l_mail, 'NWUIAPython:Fail:C301_report_student_deferment',
                           'NWUIAPython: Fail: C301_report_student_deferment')
else:
    print("VSS STUDENT DEFERMENT MASTER FILE do not run on Saturdays and Sundays")
    funcfile.writelog("SCRIPT: C301_REPORT_STUDENT_DEFERMENT_RUN: DO NOT RUN ON SATURDAYS AND SUNDAYS")

"""****************************************************************************
MYSQL LISTS WEB SERVER
****************************************************************************"""

if funcdatn.get_today_name() in "TueWedThuFriSat":
    import B005_mysql_lists
    try:
        B005_mysql_lists.mysql_lists("Web_ia_nwu")
        if l_mail:
            funcmail.send_mail('std_success_nwu', 'NWUIAPython:Success:B005_mysql_lists(Web_ia_nwu)',
                          'NWUIAPython: Success: B005_mysql_lists (Web_ia_nwu)')
    except Exception as e:
        funcsys.ErrMessage(e, l_mail, 'NWUIAPython:Fail:B005_mysql_lists(Web_ia_nwu)',
                           'NWUIAPython: Fail: B005_mysql_lists (Web_ia_nwu)')
else:
    print("MYSQL lists do not run on Sundays and Mondays")
    funcfile.writelog("SCRIPT: B005_MYSQL_LISTS (Web_ia_nwu): DO NOT RUN ON SUNDAYS AND MONDAYS")

"""****************************************************************************
MYSQL LISTS ACL SERVER
****************************************************************************"""

if funcdatn.get_today_name() in "TueWedThuFriSat":
    import B005_mysql_lists
    try:
        B005_mysql_lists.mysql_lists("Mysql_ia_server")
        if l_mail:
            funcmail.send_mail('std_success_nwu', 'NWUIAPython:Success:B005_mysql_lists(Mysql_ia_server)',
                          'NWUIAPython: Success: B005_mysql_lists (Mysql_ia_server)')
    except Exception as e:
        funcsys.ErrMessage(e, l_mail, 'NWUIAPython:Fail:B005_mysql_lists(Mysql_ia_server)',
                           'NWUIAPython: Fail: B005_mysql_lists (Mysql_ia_server)')
else:
    print("MYSQL lists do not run on Sundays and Mondays")
    funcfile.writelog("SCRIPT: B005_MYSQL_LISTS (Mysql_ia_server): DO NOT RUN ON SUNDAYS AND MONDAYS")

""" ****************************************************************************
END OF SCRIPT
*****************************************************************************"""

# CLOSE THE LOG WRITER
funcfile.writelog("Now")
funcfile.writelog("COMPLETED: A000_RUN_ALL")
funcfile.writelog("-----------------------")

# SEND MAIL TO INDICATE THE SUCCESSFUL COMPLETION OF ALL PYTHON SCRIPTS
if l_mail:
    funcmail.send_mail("python_log")
if l_mess:
    funcsms.send_telegram('', 'administrator', 'Completed downloading and running all tests.')