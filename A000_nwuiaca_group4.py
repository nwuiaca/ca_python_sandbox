"""
SCHEDULER GROUP 4 FUNCTIONS
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
import A004_import_ia  # Copy web (ia-nwu) mysql data to sqlite
import B009_ia_lists  # Build IA project lists for export to Highbond
# import A006_backup_ia  # Copy web (ia-nwu) mysql data to web (nwu-ia) mysql
import A005_import_lookup_tables  # Import own lookup tables used in all audit test functions
# import A001_oracle_to_sqlite  # Import oracle data to various sqlite tables


def group4_functions():
    """
    Script to run workday early evening functions
    :return: Nothing
    """

    # Run only if the run system switch is on
    if funcconf.l_run_system:

        # Start the log
        funcfile.writelog("Now")
        funcfile.writelog("SCRIPT: RUN GROUP4 EARLY EVENING FUNCTIONS")
        funcfile.writelog("------------------------------------------")

        # Message to admin
        if funcconf.l_mess_project:
            funcsms.send_telegram('', 'administrator', 'Group 4 (early evening) schedule start.')

        # Reset all the tests
        funcconf.l_run_kfs_test = True
        funcconf.l_run_people_test = True
        funcconf.l_run_vss_test = True

        # IMPORT INTERNAL AUDIT ***********************************************
        s_function: str = "A004_import_ia"
        try:
            A004_import_ia.ia_mysql_import()
        except Exception as e:
            funcsys.ErrMessage(e)

        # INTERNAL AUDIT LISTS CURRENT YEAR ***********************************
        s_function: str = "B009_ia_lists"
        try:
            B009_ia_lists.ia_lists("curr")
        except Exception as e:
            funcsys.ErrMessage(e)

        # INTERNAL AUDIT LISTS PREVIOUS YEAR **********************************
        s_function: str = "B009_ia_lists"
        try:
            B009_ia_lists.ia_lists("prev")
        except Exception as e:
            funcsys.ErrMessage(e)

        # INTERNAL AUDIT LISTS BACKUP TO NEW WEB SERVER ***********************
        # Depractaed 2023-11-04 due to move to new web server
        '''
        s_function: str = "A006_backup_ia"
        try:
            A006_backup_ia.ia_mysql_backup()
        except Exception as e:
            funcsys.ErrMessage(e)
        '''

        # IMPORT LOOKUP TABLES ************************************************
        s_function: str = "A005_import_lookup_tables"
        try:
            A005_import_lookup_tables.lookup_import()
        except Exception as e:
            funcsys.ErrMessage(e)
            # DISABLE TESTS
            funcconf.l_run_people_test = False
            funcconf.l_run_kfs_test = False
            funcconf.l_run_vss_test = False

        # Message to admin
        if funcconf.l_mess_project:
            funcsms.send_telegram('', 'administrator', 'Group 4 (early evening) schedule end.')

        """********************************************************************
        GROUP4 SCHEDULE END
        ********************************************************************"""
        funcfile.writelog("%t SCRIPT: END GROUP4 EARLY EVENING FUNCTIONS")

    return


if __name__ == '__main__':
    group4_functions()
