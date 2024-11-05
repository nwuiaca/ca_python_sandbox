"""
SCRIPT TO RUN OTHER SCRIPTS
Author: Albert J van Rensburg (NWU:21162395)
Created: 13 Apr 2020
"""

# IMPORT PYTHON PACKAGES

# IMPORT OWN MODULES
from _my_modules import funcdatn
from _my_modules import funcconf
from _my_modules import funcmail
from _my_modules import funcsys

"""
INDEX

GENERAL
UPDATE LOG (A002_log)
VACUUM TEST FINDING TABLES (A003_table_vacuum)
BACKUP MYSQL (B008_mysql_backup) DISCONTINUED 20221118
IMPORT LOOKUP TABLES (A005_import_lookup_tables)
IMPORT TEMP (A001_oracle_to_sqlite(temp))
SEARCHWORKS SUBMISSION (B011_searchworks)

PEOPLE
IMPORT TEMP (A001_oracle_to_sqlite(people))
PEOPLE MASTER LISTS (B001_people_lists)
PEOPLE LISTS (C003_people_list_masterfile)
PEOPLE TEST MASTER FILE (C001_people_test_masterfile)
PEOPLE TEST CONFLICT (C002_people_test_conflict)
PEOPLE PAYROLL LISTS (B004_payroll_lists)

VSS
IMPORT VSS (A001_oracle_to_sqlite(vss))
VSS LISTS (B003_vss_lists)
VSS PERIOD LIST (B007_vss_period_list)
VSS STUDENT DEFERMENT MASTER LISTS (C301_report_student_deferment)
VSS STUDENT DEBTOR RECON (C200_report_studdeb_recon)
VSS STUDENT MASTER FILE TESTS (C300_test_student_general)
VSS STUDENT FEE TESTS AND REPORTS (C302_test_student_fee)

KFS
IMPORT TEMP (A001_oracle_to_sqlite(kfs))

"""


def run_scripts(s_script: str = "a003", s_parameter1: str = "", s_parameter2: str = ""):
    """
    SCRIPT TO RUN OTHER SCRIPTS
    IT WILL USE GLOBAL l_run VARIABLES
    :param s_script: Script to run
    :param s_parameter1: Script first parameter
    :param s_parameter2: Script second parameter
    :return: bool: Successful or not
    """

    # VARIABLES
    l_return = False
    l_debug = True

    # DEBUG
    if l_debug:
        print("SCRIPT: " + s_script)
        print("PARAM1: " + s_parameter1)
        print("PARAM2: " + s_parameter2)

    # SWITCH ON
    funcconf.l_run_system = True
    if s_script in "all|data|kfs":
        funcconf.l_run_kfs_test = True
    elif s_script in "all|data|people":
        funcconf.l_run_people_test = True
    elif s_script in "all|data|vss":
        funcconf.l_run_vss_test = True

    # GENERAL GROUP ****************************************************************

    # UPDATE LOG
    s_project: str = "A002_log"
    if s_script in "a002|all|general":
        import A002_log
        try:
            if s_script == 'a002' and s_parameter1[0:1] == 'y':
                A002_log.log_capture(funcdatn.get_yesterday_date(), True)
            elif s_script == 'all':
                A002_log.log_capture(funcdatn.get_yesterday_date(), True)
            elif s_script == 'general':
                A002_log.log_capture(funcdatn.get_yesterday_date(), True)
            else:
                A002_log.log_capture(funcdatn.get_today_date(), False)
            l_return = True
            if funcconf.l_mail_project:
                funcmail.send_mail('std_success_nwu',
                              'NWUIACA:Success:' + s_project,
                              'NWUIACA: Success: ' + s_project)
        except Exception as err:
            l_return = False
            funcsys.ErrMessage(err, funcconf.l_mail_project,
                               'NWUIACA:Fail:' + s_project,
                               'NWUIACA: Fail: ' + s_project)

    # VACUUM TEST FINDING TABLES
    s_project: str = "A003_table_vacuum"
    if s_script in "a003|all|general":
        import A003_table_vacuum
        try:
            A003_table_vacuum.table_vacuum()
            l_return = True
            if funcconf.l_mail_project:
                funcmail.send_mail('std_success_nwu',
                              'NWUIACA:Success:' + s_project,
                              'NWUIACA: Success: ' + s_project)
        except Exception as err:
            l_return = False
            funcsys.ErrMessage(err, funcconf.l_mail_project,
                               'NWUIACA:Fail:' + s_project,
                               'NWUIACA: Fail: ' + s_project)

    # IMPORT LOOKUP TABLES
    s_project: str = "A005_import_lookup_tables"
    if s_script in "a005|all|general":
        if s_parameter1 in "all|hr|kfs|vss":
            import A005_import_lookup_tables
            try:
                A005_import_lookup_tables.lookup_import(s_parameter1)
                l_return = True
                if funcconf.l_mail_project:
                    funcmail.send_mail('std_success_nwu',
                                  'NWUIACA:Success:' + s_project,
                                  'NWUIACA: Success: ' + s_project)
            except Exception as err:
                l_return = False
                funcsys.ErrMessage(err, funcconf.l_mail_project,
                                   'NWUIACA:Fail:' + s_project,
                                   'NWUIACA: Fail: ' + s_project)

    # BACKUP MYSQL - DISCONTINUED 20221118
    """
    s_project: str = "B008_mysql_backup"
    if s_script in "b008|all|general":
        import B008_mysql_backup
        try:
            l_return = B008_mysql_backup.mysql_backup()
            if l_return:
                if funcconf.l_mail_project:
                    funcmail.send_mail('std_success_nwu',
                                  'NWUIACA:Success:' + s_project,
                                  'NWUIACA: Success: ' + s_project)
            else:
                if funcconf.l_mail_project:
                    funcmail.send_mail('std_success_nwu',
                                  'NWUIACA:Fail:' + s_project,
                                  'NWUIACA: Fail: ' + s_project)
        except Exception as err:
            l_return = False
            funcsys.ErrMessage(err, funcconf.l_mail_project,
                               'NWUIACA:Fail:' + s_project,
                               'NWUIACA: Fail: ' + s_project)
    """

    # IMPORT TEMP
    s_project: str = "A001_oracle_to_sqlite(temp)"
    if s_script in "a001":
        if s_script == "a001" and s_parameter1[0:1] == "t":
            import A001_oracle_to_sqlite
            try:
                A001_oracle_to_sqlite.oracle_to_sqlite()
                l_return = True
                if funcconf.l_mail_project:
                    funcmail.send_mail('std_success_nwu',
                                  'NWUIACA:Success:' + s_project,
                                  'NWUIACA: Success: ' + s_project)
            except Exception as err:
                l_return = False
                funcsys.ErrMessage(err, funcconf.l_mail_project,
                                   "NWUIACA:Fail:" + s_project,
                                   "NWUIACA: Fail: " + s_project)

    # SEARCHWORKS SUBMISSION **********************************************
    s_project: str = "B011_searchworks"
    if s_script in "b011":
        if s_parameter1[0:1] == "1":
            import B011_searchworks
            try:
                B011_searchworks.searchworks_submit(0, True)
                l_return = True
            except Exception as e:
                l_return = False
                funcsys.ErrMessage(e)

    # PEOPLE GROUP *****************************************************************

    # IMPORT PEOPLE
    s_project: str = "A001_oracle_to_sqlite(people)"
    if s_script in "a001|all|data|people":
        if (s_script == "a001" and s_parameter1 == "people") or (s_script in "all|data|people"):
            import A001_oracle_to_sqlite
            try:
                A001_oracle_to_sqlite.oracle_to_sqlite("000b_Table - people.csv", "PEOPLE")
                l_return = True
                if funcconf.l_mail_project:
                    funcmail.send_mail('std_success_nwu',
                                  'NWUIACA:Success:' + s_project,
                                  'NWUIACA: Success: ' + s_project)
            except Exception as err:
                l_return = False
                funcconf.l_run_people_test = False
                funcsys.ErrMessage(err, funcconf.l_mail_project,
                                   "NWUIACA:Fail:" + s_project,
                                   "NWUIACA: Fail: " + s_project)

    # PEOPLE MASTER LISTS
    s_project: str = "B001_people_lists"
    if funcconf.l_run_people_test:
        if s_script in "b001|all|people":
            # IMPORT THE PEOPLE MASTER TABLE
            import B001_people_lists
            try:
                B001_people_lists.people_lists()
                l_return = True
                if funcconf.l_mail_project:
                    funcmail.send_mail('std_success_nwu',
                                  'NWUIACA:Success:' + s_project,
                                  'NWUIACA: Success: ' + s_project)
            except Exception as err:
                l_return = False
                funcconf.l_run_people = False
                funcsys.ErrMessage(err, funcconf.l_mail_project,
                                   "NWUIACA:Fail:" + s_project,
                                   "NWUIACA: Fail: " + s_project)

    # PEOPLE LISTS
    s_project: str = "C003_people_list_masterfile"
    if funcconf.l_run_people_test:
        if s_script in "c003|all|people":
            import C003_people_list_masterfile
            try:
                C003_people_list_masterfile.people_list_masterfile()
                l_return = True
                if funcconf.l_mail_project:
                    funcmail.send_mail('std_success_nwu',
                                  'NWUIACA:Success:' + s_project,
                                  'NWUIACA: Success: ' + s_project)
            except Exception as err:
                l_return = False
                funcconf.l_run_people = False
                funcsys.ErrMessage(err, funcconf.l_mail_project,
                                   "NWUIACA:Fail:" + s_project,
                                   "NWUIACA: Fail: " + s_project)

    # PEOPLE TEST MASTER FILE
    s_project: str = "C001_people_test_masterfile"
    if funcconf.l_run_people_test:
        if s_script in "c001|all|people":
            # IMPORT THE LOOKUP TABLE
            import A005_import_lookup_tables
            try:
                A005_import_lookup_tables.lookup_import('hr')
                l_return = True
                if funcconf.l_mail_project:
                    funcmail.send_mail('std_success_nwu',
                                  'NWUIACA:Success:' + s_project,
                                  'NWUIACA: Success: ' + s_project)
            except Exception as err:
                l_return = False
                funcsys.ErrMessage(err, funcconf.l_mail_project,
                                   'NWUIACA:Fail:' + s_project,
                                   'NWUIACA: Fail: ' + s_project)
            # DO THE MASTER FILE TESTS
            import C001_people_test_masterfile
            try:
                C001_people_test_masterfile.people_test_masterfile()
                l_return = True
                if funcconf.l_mail_project:
                    funcmail.send_mail('std_success_nwu',
                                  'NWUIACA:Success:' + s_project,
                                  'NWUIACA: Success: ' + s_project)
            except Exception as err:
                l_return = False
                funcconf.l_run_people = False
                funcsys.ErrMessage(err, funcconf.l_mail_project,
                                   "NWUIACA:Fail:" + s_project,
                                   "NWUIACA: Fail: " + s_project)

    # PEOPLE TEST CONFLICT
    s_project: str = "C002_people_test_conflict"
    if funcconf.l_run_people_test:
        if s_script in "c002|all|people":
            # IMPORT THE LOOKUP TABLE
            import A005_import_lookup_tables
            try:
                A005_import_lookup_tables.lookup_import('hr')
                l_return = True
                if funcconf.l_mail_project:
                    funcmail.send_mail('std_success_nwu',
                                  'NWUIACA:Success:' + s_project,
                                  'NWUIACA: Success: ' + s_project)
            except Exception as err:
                l_return = False
                funcsys.ErrMessage(err, funcconf.l_mail_project,
                                   'NWUIACA:Fail:' + s_project,
                                   'NWUIACA: Fail: ' + s_project)
            # DO THE CONFLICT TESTS
            import C002_people_test_conflict
            try:
                C002_people_test_conflict.people_test_conflict()
                l_return = True
                if funcconf.l_mail_project:
                    funcmail.send_mail('std_success_nwu',
                                  'NWUIACA:Success:' + s_project,
                                  'NWUIACA: Success: ' + s_project)
            except Exception as err:
                l_return = False
                funcconf.l_run_people = False
                funcsys.ErrMessage(err, funcconf.l_mail_project,
                                   "NWUIACA:Fail:" + s_project,
                                   "NWUIACA: Fail: " + s_project)

    # PEOPLE PAYROLL LISTS
    if s_script in "b004|all|people":
        import B004_payroll_lists
        s_project: str = "B004_payroll_lists"
        try:
            B004_payroll_lists.payroll_lists()
            l_return = True
            if funcconf.l_mail_project:
                funcmail.send_mail('std_success_nwu',
                              'NWUIACA:Success:' + s_project,
                              'NWUIACA: Success: ' + s_project)
        except Exception as err:
            l_return = False
            funcsys.ErrMessage(err, funcconf.l_mail_project,
                               'NWUIACA:Fail:' + s_project,
                               'NWUIACA: Fail: ' + s_project)

    # VSS GROUP ********************************************************************

    # IMPORT VSS
    s_project: str = "A001_oracle_to_sqlite(vss)"
    if s_script in "a001|all|data|vss":
        if (s_script == "a001" and s_parameter1 == "vss") or (s_script in "all|data|vss"):
            import A001_oracle_to_sqlite
            try:
                A001_oracle_to_sqlite.oracle_to_sqlite("000b_Table - vss.csv", "VSS")
                l_return = True
                if funcconf.l_mail_project:
                    funcmail.send_mail('std_success_nwu',
                                  'NWUIACA:Success:' + s_project,
                                  'NWUIACA: Success: ' + s_project)
            except Exception as err:
                l_return = False
                funcconf.l_run_vss_test = False
                funcsys.ErrMessage(err, funcconf.l_mail_project,
                                   "NWUIACA:Fail:" + s_project,
                                   "NWUIACA: Fail: " + s_project)

    # VSS LISTS
    s_project: str = "B003_vss_lists"
    if funcconf.l_run_vss_test:
        if s_script in "b003|all|vss":
            import B003_vss_lists
            try:
                B003_vss_lists.vss_lists()
                l_return = True
                if funcconf.l_mail_project:
                    funcmail.send_mail('std_success_nwu',
                                  'NWUIACA:Success:' + s_project,
                                  'NWUIACA: Success: ' + s_project)
            except Exception as err:
                l_return = False
                funcconf.l_run_vss = False
                funcsys.ErrMessage(err, funcconf.l_mail_project,
                                   "NWUIACA:Fail:" + s_project,
                                   "NWUIACA: Fail: " + s_project)

    # VSS PERIOD LISTS
    s_project: str = "B007_vss_period_list"
    if funcconf.l_run_vss_test:
        if s_script in "b007|all|vss":
            if s_script != "b007":
                s_parameter1 = "curr"
            if (s_parameter1 in "curr|prev") or (2015 <= int(s_parameter1) <= int(funcdatn.get_current_year())-2):
                import B007_vss_period_list
                try:
                    B007_vss_period_list.vss_period_list(s_parameter1)
                    l_return = True
                    if funcconf.l_mail_project:
                        funcmail.send_mail('std_success_nwu',
                                      'NWUIACA:Success:' + s_project,
                                      'NWUIACA: Success: ' + s_project)
                except Exception as err:
                    l_return = False
                    funcconf.l_run_vss = False
                    funcsys.ErrMessage(err, funcconf.l_mail_project,
                                       "NWUIACA:Fail:" + s_project,
                                       "NWUIACA: Fail: " + s_project)

    # VSS STUDENT DEFERMENT MASTER LISTS
    s_project: str = "C301_report_student_deferment"
    if funcconf.l_run_vss_test:
        if s_script in "c301|all|vss":
            if s_script != "c301":
                s_parameter1 = "curr"
            if (s_parameter1 in "curr|prev") or (2015 <= int(s_parameter1) <= int(funcdatn.get_current_year())-2):
                import C301_report_student_deferment
                try:
                    C301_report_student_deferment.studdeb_deferments(s_parameter1)
                    l_return = True
                    if funcconf.l_mail_project:
                        funcmail.send_mail('std_success_nwu',
                                      'NWUIACA:Success:' + s_project,
                                      'NWUIACA: Success: ' + s_project)
                except Exception as err:
                    l_return = False
                    funcconf.l_run_vss = False
                    funcsys.ErrMessage(err, funcconf.l_mail_project,
                                       "NWUIACA:Fail:" + s_project,
                                       "NWUIACA: Fail: " + s_project)

    #
    # VSS STUDENT DEFERMENT MASTER LISTS
    s_project: str = "C301_report_student_deferment"
    if funcconf.l_run_vss_test:
        if s_script in "c301|all|vss":
            if s_script != "c301":
                s_parameter1 = "curr"
            if (s_parameter1 in "curr|prev") or (2015 <= int(s_parameter1) <= int(funcdatn.get_current_year())-2):
                import C301_report_student_deferment
                try:
                    C301_report_student_deferment.studdeb_deferments(s_parameter1)
                    l_return = True
                    if funcconf.l_mail_project:
                        funcmail.send_mail('std_success_nwu',
                                      'NWUIACA:Success:' + s_project,
                                      'NWUIACA: Success: ' + s_project)
                except Exception as err:
                    l_return = False
                    funcconf.l_run_vss = False
                    funcsys.ErrMessage(err, funcconf.l_mail_project,
                                       "NWUIACA:Fail:" + s_project,
                                       "NWUIACA: Fail: " + s_project)

    # VSS STUDENT DEFERMENT MASTER LISTS
    s_project: str = "C301_report_student_deferment"
    if funcconf.l_run_vss_test:
        if s_script in "c301|all|vss":
            if s_script != "c301":
                s_parameter1 = "curr"
            if (s_parameter1 in "curr|prev") or (2015 <= int(s_parameter1) <= int(funcdatn.get_current_year())-2):
                import C301_report_student_deferment
                try:
                    C301_report_student_deferment.studdeb_deferments(s_parameter1)
                    l_return = True
                    if funcconf.l_mail_project:
                        funcmail.send_mail('std_success_nwu',
                                      'NWUIACA:Success:' + s_project,
                                      'NWUIACA: Success: ' + s_project)
                except Exception as err:
                    l_return = False
                    funcconf.l_run_vss = False
                    funcsys.ErrMessage(err, funcconf.l_mail_project,
                                       "NWUIACA:Fail:" + s_project,
                                       "NWUIACA: Fail: " + s_project)

    # VSS STUDENT DEBTOR RECON
    s_project: str = "C200_report_studdeb_recon"
    if funcconf.l_run_vss_test:
        if s_script in "c200|all|vss":
            if s_script != "c200":
                s_parameter1 = "curr"
                # No open balances
                r_maf = 0
                r_pot = 0
                r_vaa = 0
                # 2022 balances
                # r_maf = 40960505.33
                # r_pot = 6573550.30
                # r_vaa = 29005168.76
                # 2021 balances
                # r_maf = 65676774.13
                # r_pot = 61655697.80
                # r_vaa = 41648563.00
                # 2020 balances
                # r_maf = 48501952.09
                # r_pot = -12454680.98
                # r_vaa = 49976048.39
                # 2019 balances
                # r_maf = 66561452.48
                # r_pot = -18340951.06
                # r_vaa = 39482933.18
            else:
                r_maf = 0
                r_pot = 0
                r_vaa = 0
            if s_parameter1 in "curr|prev|2022|2021|2020":
                # IMPORT THE LOOKUP TABLE
                import A005_import_lookup_tables
                try:
                    A005_import_lookup_tables.lookup_import('vss')
                    l_return = True
                    if funcconf.l_mail_project:
                        funcmail.send_mail('std_success_nwu',
                                      'NWUIACA:Success:' + s_project,
                                      'NWUIACA: Success: ' + s_project)
                except Exception as err:
                    l_return = False
                    funcsys.ErrMessage(err, funcconf.l_mail_project,
                                       'NWUIACA:Fail:' + s_project,
                                       'NWUIACA: Fail: ' + s_project)
                # DO THE RECON
                import C200_report_studdeb_recon
                try:
                    C200_report_studdeb_recon.report_studdeb_recon(r_maf, r_pot, r_vaa, s_parameter1)
                    l_return = True
                    if funcconf.l_mail_project:
                        funcmail.send_mail('std_success_nwu',
                                      'NWUIACA:Success:' + s_project,
                                      'NWUIACA: Success: ' + s_project)
                except Exception as err:
                    l_return = False
                    funcconf.l_run_vss = False
                    funcsys.ErrMessage(err, funcconf.l_mail_project,
                                       "NWUIACA:Fail:" + s_project,
                                       "NWUIACA: Fail: " + s_project)

    # VSS STUDENT MASTER FILE TESTS
    s_project: str = "C300_test_student_general"
    if funcconf.l_run_vss_test:
        if s_script in "c300|all|vss":
            # IMPORT THE LOOKUP TABLE
            import A005_import_lookup_tables
            try:
                A005_import_lookup_tables.lookup_import('vss')
                l_return = True
                if funcconf.l_mail_project:
                    funcmail.send_mail('std_success_nwu',
                                  'NWUIACA:Success:' + s_project,
                                  'NWUIACA: Success: ' + s_project)
            except Exception as err:
                l_return = False
                funcsys.ErrMessage(err, funcconf.l_mail_project,
                                   'NWUIACA:Fail:' + s_project,
                                   'NWUIACA: Fail: ' + s_project)
            # DO THE MASTER FILE TESTS
            import C300_test_student_general
            try:
                C300_test_student_general.test_student_general()
                l_return = True
                if funcconf.l_mail_project:
                    funcmail.send_mail('std_success_nwu',
                                  'NWUIACA:Success:' + s_project,
                                  'NWUIACA: Success: ' + s_project)
            except Exception as err:
                l_return = False
                funcconf.l_run_vss = False
                funcsys.ErrMessage(err, funcconf.l_mail_project,
                                   "NWUIACA:Fail:" + s_project,
                                   "NWUIACA: Fail: " + s_project)

    # VSS STUDENT FEE TESTS AND REPORTS
    s_project: str = "C302_test_student_fee"
    if funcconf.l_run_vss_test:
        if s_script in "c302|all|vss":
            if s_script != "c302":
                s_parameter1 = "curr"
            if s_parameter1 in "curr|prev":
                # IMPORT THE LOOKUP TABLE
                import A005_import_lookup_tables
                try:
                    A005_import_lookup_tables.lookup_import('vss')
                    l_return = True
                    if funcconf.l_mail_project:
                        funcmail.send_mail('std_success_nwu',
                                      'NWUIACA:Success:' + s_project,
                                      'NWUIACA: Success: ' + s_project)
                except Exception as err:
                    l_return = False
                    funcsys.ErrMessage(err, funcconf.l_mail_project,
                                       'NWUIACA:Fail:' + s_project,
                                       'NWUIACA: Fail: ' + s_project)
                # DO THE TESTS
                import C302_test_student_fee
                try:
                    C302_test_student_fee.student_fee(s_parameter1)
                    l_return = True
                    if funcconf.l_mail_project:
                        funcmail.send_mail('std_success_nwu',
                                      'NWUIACA:Success:' + s_project,
                                      'NWUIACA: Success: ' + s_project)
                except Exception as err:
                    l_return = False
                    funcconf.l_run_vss = False
                    funcsys.ErrMessage(err, funcconf.l_mail_project,
                                       "NWUIACA:Fail:" + s_project,
                                       "NWUIACA: Fail: " + s_project)

    # KFS GROUP ********************************************************************

    # IMPORT KFS
    s_project: str = "A001_oracle_to_sqlite(kfs)"
    if (s_script == "a001" and s_parameter1 == "kfs") or (s_script in "all|data|kfs"):
        import A001_oracle_to_sqlite
        try:
            A001_oracle_to_sqlite.oracle_to_sqlite("000b_Table - kfs.csv", "KFS")
            l_return = True
            if funcconf.l_mail_project:
                funcmail.send_mail('std_success_nwu',
                              'NWUIACA:Success:' + s_project,
                              'NWUIACA: Success: ' + s_project)
        except Exception as err:
            l_return = False
            funcconf.l_run_kfs_test = False
            funcsys.ErrMessage(err, funcconf.l_mail_project,
                               "NWUIACA:Fail:" + s_project,
                               "NWUIACA: Fail: " + s_project)

    # KFS PERIOD LISTS
    s_project: str = "B006_kfs_period_list"
    if funcconf.l_run_kfs_test:
        if s_script in "b006|all|kfs":
            if s_script != "b006":
                s_parameter1 = "curr"
            if (s_parameter1 in "curr|prev") or (2015 <= int(s_parameter1) <= int(funcdatn.get_current_year())-2):
                import B006_kfs_period_list
                try:
                    B006_kfs_period_list.kfs_period_list(s_parameter1)
                    l_return = True
                    if funcconf.l_mail_project:
                        funcmail.send_mail('std_success_nwu',
                                      'NWUIACA:Success:' + s_project,
                                      'NWUIACA: Success: ' + s_project)
                except Exception as err:
                    l_return = False
                    funcconf.l_run_kfs = False
                    funcsys.ErrMessage(err, funcconf.l_mail_project,
                                       "NWUIACA:Fail:" + s_project,
                                       "NWUIACA: Fail: " + s_project)

    return l_return


if __name__ == '__main__':
    try:
        run_scripts()
    except Exception as e:
        funcsys.ErrMessage(e)


