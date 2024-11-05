"""
SCHEDULER GROUP 7 FUNCTIONS
Auther: Albert Janse van Rensburg (NWU:21162395)
Create date: 25 Sep 2023
"""

# IMPORT OWN FUNCTIONS
from _my_modules import funclogg
from _my_modules import funcconf
from _my_modules import funcsms
from _my_modules import funcfile
from _my_modules import funcdatn
from _my_modules import funcmail
from _my_modules import funcsys
import A002_log
import C001_people_test_masterfile
import B011_searchworks
import C002_people_test_conflict
import C200_report_studdeb_recon
import C300_test_student_general
import C302_test_student_fee
import C201_creditor_test_payments
import C202_gl_test_transactions
import C303_test_student_bursary


def group7_functions():
    """
    Script to run workday dawn functions
    :return: Nothing
    """

    # Run only if the run system switch is on
    if funcconf.l_run_system:

        # UPDATE THE LOG
        funcfile.writelog("Now")
        funcfile.writelog("SCRIPT: RUN GROUP7 DAWN FUNCTIONS")
        funcfile.writelog("---------------------------------")

        # MESSAGE TO ADMIN
        if funcconf.l_mess_project:
            funcsms.send_telegram('', 'administrator', 'Group 7 (dawn) schedule start.')

        # UPDATE LOG ***************************************************
        s_function: str = "A002_log"
        if funcdatn.get_today_name() in "MonTueWedThuFriSatSun":
            try:
                A002_log.log_capture(funcdatn.get_yesterday_date(), True)
                # pass
            except Exception as e:
                funcsys.ErrMessage(e)

        # PEOPLE TEST MASTER FILE **************************************
        s_function: str = "C001_people_test_masterfile"
        if funcconf.l_run_people_test:
            if funcdatn.get_today_name() in "MonTueWedThuFri":
                try:
                    C001_people_test_masterfile.people_test_masterfile()
                except Exception as e:
                    funcsys.ErrMessage(e)

        # SEARCHWORKS SUBMISSION **********************************************
        s_function: str = "B011_searchworks"
        if funcconf.l_run_people_test:
            if funcdatn.get_today_name() in "MonTueWedThuFri":
                try:
                    B011_searchworks.searchworks_submit()
                except Exception as e:
                    funcsys.ErrMessage(e)

        # PEOPLE TEST CONFLICT *****************************************
        s_function: str = "C002_people_test_conflict"
        if funcconf.l_run_people_test:
            if funcdatn.get_today_name() in "MonTueWedThuFri":
                try:
                    C002_people_test_conflict.people_test_conflict()
                except Exception as e:
                    funcsys.ErrMessage(e)

        # VSS STUDENT DEBTOR RECON *************************************
        s_function: str = "C200_report_studdeb_recon"
        if funcconf.l_run_people_test and funcconf.l_run_kfs_test and funcconf.l_run_vss_test:
            if funcdatn.get_today_name() in "MonTueWedThuFri":
                try:
                    # C200_report_studdeb_recon.report_studdeb_recon()
                    C200_report_studdeb_recon.report_studdeb_recon(168408077.92, 147644293.06, 76500858.10, "curr")
                except Exception as e:
                    funcsys.ErrMessage(e)

        # VSS STUDENT MASTER FILE TESTS ********************************
        s_function: str = "C300_test_student_general"
        if funcconf.l_run_vss_test:
            if funcdatn.get_today_date() in "01z13":
                try:
                    C300_test_student_general.test_student_general()
                except Exception as e:
                    funcsys.ErrMessage(e)

        # VSS STUDENT FEE TESTS AND REPORTS ****************************
        s_function: str = "C302_test_student_fee"
        if funcconf.l_run_vss_test:
            if funcdatn.get_today_name() in "MonTueWedThuFri":
                try:
                    C302_test_student_fee.student_fee()
                except Exception as e:
                    funcsys.ErrMessage(e)

        # KFS CREDITOR PAYMENT TESTS ***********************************
        s_function: str = "C201_creditor_test_payments"
        if funcconf.l_run_kfs_test:
            if funcdatn.get_today_name() in "MonTueWedThuFri":
                try:
                    C201_creditor_test_payments.creditor_test_payments()
                except Exception as e:
                    funcsys.ErrMessage(e)

        # KFS GL TRANSACTION TESTS *************************************
        s_function: str = "C202_gl_test_transactions"
        if funcconf.l_run_kfs_test:
            if funcdatn.get_today_name() in "MonTueWedThuFri":
                try:
                    C202_gl_test_transactions.gl_test_transactions()
                except Exception as e:
                    funcsys.ErrMessage(e)

        # VSS STUDENT BURSARY TESTS *******************************
        s_function: str = "C303_test_student_bursary(curr)"
        if funcconf.l_run_vss_test:
            if funcdatn.get_today_name() in "MonTueWedThuFri":
                try:
                    C303_test_student_bursary.student_bursary("curr")
                except Exception as e:
                    funcsys.ErrMessage(e)

        # MESSAGE TO ADMIN
        if funcconf.l_mess_project:
            funcsms.send_telegram('', 'administrator', 'Group7 (dawn) schedule end.')

        """********************************************************************
        GROUP7 SCHEDULE END
        ********************************************************************"""
        funcfile.writelog("SCRIPT: END GROUP7 DAWN FUNCTIONS")

    return


if __name__ == '__main__':
    group7_functions()
