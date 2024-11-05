"""
SCHEDULER GROUP 5 FUNCTIONS
Auther: Albert Janse van Rensburg (NWU:21162395)
Create date: 25 Sep 2023
"""

# Import own functions
from _my_modules import funclogg
from _my_modules import funcconf
from _my_modules import funcsms
from _my_modules import funcfile
from _my_modules import funcdatn
from _my_modules import funcmail
from _my_modules import funcsys
import A001_oracle_to_sqlite  # Import oracle data to various sqlite tables
import A001_oracle_to_sqlite_xdev
import B001_people_lists  # People master lists
import C003_people_list_masterfile  # People lists for graphs in Highbond
import A007_backup_people  # Copy current nwu employees to web (nwu-ia) mysql
import A009_backup_people_structure  # Copy people structure to web (nwu-ia) mysql
import B004_payroll_lists  # Payroll master lists
import B003_vss_lists  # VSS master lists
import B007_vss_period_list  # VSS transaction lists
import C301_report_student_deferment  # VSS student deferment lists


def group5_functions():
    """
    Script to run workday late evening functions
    :return: Nothing
    """

    # Run only if the run system switch is on
    if funcconf.l_run_system:

        # UPDATE THE LOG
        funcfile.writelog("Now")
        funcfile.writelog("SCRIPT: RUN GROUP5 LATE EVENING FUNCTIONS")
        funcfile.writelog("-----------------------------------------")

        # MESSAGE TO ADMIN
        if funcconf.l_mess_project:
            funcsms.send_telegram('', 'administrator', 'Group 5 (late evening) schedule start.')

        # IMPORT PEOPLE *******************************************************
        s_function: str = "A001_oracle_to_sqlite(people)"
        if funcconf.l_run_people_test:
            if funcdatn.get_today_name() in "MonTueWedThuFri":
                try:
                    A001_oracle_to_sqlite.oracle_to_sqlite("000b_Table - people.csv", "PEOPLE")
                    # A001_oracle_to_sqlite_xdev.oracle_to_sqlite_new("000b_Table - people.csv", "000a_Database.csv", "000c_Column_new.csv")
                except Exception as e:
                    funcsys.ErrMessage(e)
                    # DISABLE PEOPLE TESTS
                    funcconf.l_run_people_test = False

        # PEOPLE LISTS ********************************************************
        s_function: str = "B001_people_lists"
        if funcconf.l_run_people_test:
            if funcdatn.get_today_name() in "MonTueWedThuFri":
                try:
                    B001_people_lists.people_lists()
                except Exception as e:
                    funcsys.ErrMessage(e)
                    # DISABLE PEOPLE TESTS
                    funcconf.l_run_people_test = False

        # PEOPLE LIST MASTER FILE *********************************************
        s_function: str = "C003_people_list_masterfile"
        if funcconf.l_run_people_test:
            if funcdatn.get_today_name() in "MonTueWedThuFri":
                try:
                    C003_people_list_masterfile.people_list_masterfile()
                except Exception as e:
                    funcsys.ErrMessage(e)

        # PEOPLE BACKUP TO NEW WEB SERVER *************************************
        s_function: str = "A007_backup_people"
        if funcconf.l_run_people_test:
            if funcdatn.get_today_name() in "MonTueWedThuFri":
                try:
                    A007_backup_people.ia_mysql_backup_people()
                except Exception as e:
                    funcsys.ErrMessage(e)

        # PEOPLE STRUCTURE BACKUP TO NEW WEB SERVER ****************************
        s_function: str = "A009_backup_people_structure"
        if funcconf.l_run_people_test:
            if funcdatn.get_today_name() in "MonTueWedThuFri":
                try:
                    A009_backup_people_structure.ia_mysql_backup_people_structure()
                except Exception as e:
                    funcsys.ErrMessage(e)

        # PEOPLE PAYROLL LISTS ************************************************
        s_function: str = "B004_payroll_lists"
        if funcconf.l_run_people_test:
            if funcdatn.get_today_name() in "MonTueWedThuFri":
                try:
                    B004_payroll_lists.payroll_lists()
                except Exception as e:
                    funcsys.ErrMessage(e)

        # IMPORT VSS **********************************************************
        s_function: str = "A001_oracle_to_sqlite_xdev(vss)"
        if funcconf.l_run_vss_test:
            if funcdatn.get_today_name() in "MonTueWedThuFri":
                try:
                    A001_oracle_to_sqlite.oracle_to_sqlite("000b_Table - vss.csv", "VSS")
                    # A001_oracle_to_sqlite_xdev.oracle_to_sqlite_new("000b_Table - vss.csv", "000a_Database.csv", "000c_Column_new.csv")
                except Exception as e:
                    funcsys.ErrMessage(e)
                    # DISABLE PEOPLE TESTS
                    funcconf.l_run_vss_test = False

        # VSS LISTS ***********************************************************
        s_function: str = "B003_vss_lists"
        if funcconf.l_run_vss_test:
            if funcdatn.get_today_name() in "MonTueWedThuFri":
                try:
                    B003_vss_lists.vss_lists()
                except Exception as e:
                    funcsys.ErrMessage(e)
                    # DISABLE VSS TESTS
                    funcconf.l_run_vss_test = False

        # VSS PERIOD LIST CURR ************************************************
        s_function: str = "B007_vss_period_list(curr)"
        if funcconf.l_run_vss_test:
            if funcdatn.get_today_name() in "MonTueWedThuFri":
                try:
                    B007_vss_period_list.vss_period_list("curr")
                except Exception as e:
                    funcsys.ErrMessage(e)
                    # DISABLE VSS TESTS
                    funcconf.l_run_vss_test = False

        # VSS STUDENT DEFERMENT MASTER FILE ***********************************
        s_function: str = "C301_report_student_deferment"
        if funcconf.l_run_vss_test:
            if funcdatn.get_today_name() in "MonTueWedThuFri":
                try:
                    C301_report_student_deferment.studdeb_deferments()
                except Exception as e:
                    funcsys.ErrMessage(e)

        # MESSAGE TO ADMIN
        if funcconf.l_mess_project:
            funcsms.send_telegram('', 'administrator', 'Group5 (late evening) schedule end.')

        """********************************************************************
        GROUP5 SCHEDULE END
        ********************************************************************"""
        funcfile.writelog("SCRIPT: END GROUP5 LATE EVENING FUNCTIONS")

    return


if __name__ == '__main__':
    group5_functions()
