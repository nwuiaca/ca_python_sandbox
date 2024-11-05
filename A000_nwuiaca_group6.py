"""
SCHEDULER GROUP 6 FUNCTIONS
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
import A001_oracle_to_sqlite  # Import oracle data to various sqlite tables
import A001_oracle_to_sqlite_xdev  # Import oracle data to various sqlite tables
import B002_kfs_lists  # KFS master lists
import B006_kfs_period_list  # KFS general ledger and payments
# import B005_mysql_lists  # Copy current nwu employees to web (ia-nwu) mysql


def group6_functions():
    """
    Script to run workday midnight functions
    :return: Nothing
    """

    # Run only if the run system switch is on
    if funcconf.l_run_system:

        # UPDATE THE LOG
        funcfile.writelog("Now")
        funcfile.writelog("SCRIPT: RUN GROUP6 MIDNIGHT FUNCTIONS")
        funcfile.writelog("-------------------------------------")

        # MESSAGE TO ADMIN
        if funcconf.l_mess_project:
            funcsms.send_telegram('', 'administrator', 'Group 6 (midnight) schedule start.')

        # IMPORT KFS **********************************************************
        s_function: str = "A001_oracle_to_sqlite_xdev(kfs)"
        if funcconf.l_run_kfs_test:
            if funcdatn.get_today_name() in "TueWedThuFriSat":
                try:
                    A001_oracle_to_sqlite.oracle_to_sqlite("000b_Table - kfs.csv", "KFS")
                    # A001_oracle_to_sqlite_xdev.oracle_to_sqlite_new("000b_Table - kfs.csv", "000a_Database.csv", "000c_Column_new.csv")
                except Exception as e:
                    funcsys.ErrMessage(e)
                    # DISABLE KFS TESTS
                    funcconf.l_run_kfs_test = False

        # KFS LISTS ***********************************************************
        s_function: str = "B002_kfs_lists"
        if funcconf.l_run_kfs_test:
            if funcdatn.get_today_name() in "TueWedThuFriSat":
                try:
                    B002_kfs_lists.kfs_lists()
                except Exception as e:
                    funcsys.ErrMessage(e)
                    # DISABLE PEOPLE TESTS
                    funcconf.l_run_kfs = False

        # KFS PERIOD LISTS CURR ***********************************************
        s_function: str = "B006_kfs_period_list(curr)"
        if funcconf.l_run_kfs_test:
            if funcdatn.get_today_name() in "TueWedThuFriSat":
                try:
                    B006_kfs_period_list.kfs_period_list("curr")
                except Exception as e:
                    funcsys.ErrMessage(e)
                    # DISABLE PEOPLE TESTS
                    funcconf.l_run_kfs = False

        """
        # KFS PERIOD LISTS PREV ***********************************************
        s_function: str = "B006_kfs_period_list(prev)"
        if funcconf.l_run_kfs_test:
            if funcdatn.get_today_name() in "TueWedThuFriSat":
                try:
                    B006_kfs_period_list.kfs_period_list("prev")
                except Exception as e:
                    funcsys.ErrMessage(e)
                    # DISABLE PEOPLE TESTS
                    funcconf.l_run_kfs = False
        """

        # MYSQL UPDATE WEB IA NWU *********************************************
        # Depracated 2023-11-04 due to move to new web server
        '''
        s_function: str = "B005_mysql_lists(web)"
        if funcconf.l_run_people_test:
            if funcdatn.get_today_name() in "TueWedThuFriSat":
                try:
                    B005_mysql_lists.mysql_lists("Web_ia_nwu")
                except Exception as e:
                    funcsys.ErrMessage(e)

        # MESSAGE TO ADMIN
        if funcconf.l_mess_project:
            funcsms.send_telegram('', 'administrator', 'Group6 (midnight) schedule end.')
        '''

        """********************************************************************
        GROUP6 SCHEDULE END
        ********************************************************************"""
        funcfile.writelog("SCRIPT: END GROUP6 MIDNIGHT FUNCTIONS")

    return


if __name__ == '__main__':
    group6_functions()
