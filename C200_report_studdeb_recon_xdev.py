# Import python modules
import csv
import sqlite3

# Import own modules
from _my_modules import funcconf
from _my_modules import funcdatn
from _my_modules import funccsv
from _my_modules import funcfile
from _my_modules import funcsys
from _my_modules import funcmysql
from _my_modules import funcsms
from _my_modules import functest


def report_studdeb_recon(dopenmaf: float = 0, dopenpot: float = 0, dopenvaa: float = 0, s_period: str = "curr"):
    """
    STUDENT DEBTOR RECONCILIATIONS
    :param dopenmaf: int: Mafiking Campus opening balance
    :param dopenpot: int: Potchefstroom opening balance
    :param dopenvaa: int: Vaal Campus opening balance
    :param s_period: str: Period indication curr, prev or year
    :return: Null
    """

    """ PARAMETERS *************************************************************
    dopenmaf = GL Opening balances for Mahikeng campus
    dopenpot = GL Opening balances for Potchefstroom campus
    dopenvaa = GL Opening balances for Vanderbijlpark campus
    Notes:
    1. When new financial year start, GL does not contain opening balances.
       Opening balances are the inserted manually here, until the are inserted
       into the GL by journal, usually at the end of March.
    *************************************************************************"""

    """*************************************************************************
    ENVIRONMENT
    *************************************************************************"""

    # DECLARE VARIABLES

    l_debug: bool = True
    so_path: str = f"{funcconf.drive_data_raw}Kfs_vss_studdeb/"  # Source database path
    if s_period == "curr":
        s_year: str = funcdatn.get_current_year()
        s_prev_year = "prev"
        so_file: str = "Kfs_vss_studdeb.sqlite"  # Source database
    elif s_period == "prev":
        s_year = funcdatn.get_previous_year()
        s_prev_year = str(int(funcdatn.get_previous_year()) - 1)
        so_file = "Kfs_vss_studdeb_prev.sqlite"  # Source database
    else:
        s_year = s_period
        s_prev_year = str(int(s_period) - 1)
        so_file = "Kfs_vss_studdeb_" + s_year + ".sqlite"  # Source database
    re_path = f"{funcconf.drive_data_results}Debtorstud/"  # Results
    ed_path = f"{funcconf.drive_system}_external_data/"  # External data
    l_mess: bool = funcconf.l_mess_project
    # l_mess: bool = True
    l_export = True
    l_record = True
    s_burs_code = '042z052z381z500'  # Current bursary transaction codes

    # Open the script log file ******************************************************
    print("-------------------------")
    print("C200_REPORT_STUDDEB_RECON")
    print("-------------------------")
    print("ENVIRONMENT")
    funcfile.writelog("Now")
    funcfile.writelog("SCRIPT: C200_REPORT_STUDDEB_RECON")
    funcfile.writelog("---------------------------------")
    funcfile.writelog("ENVIRONMENT")

    # MESSAGE
    if l_mess:
        funcsms.send_telegram('', 'administrator', '<b>C200 Student debtor reconciliations</b>')

    """*************************************************************************
    OPEN DATABASES
    *************************************************************************"""
    print("OPEN DATABASES")
    funcfile.writelog("OPEN DATABASES")

    # Open the SOURCE file
    with sqlite3.connect(so_path + so_file) as so_conn:
        so_curs = so_conn.cursor()
    funcfile.writelog("OPEN DATABASE: " + so_file)

    # Attach data sources
    so_curs.execute(f"ATTACH DATABASE '{funcconf.drive_data_raw}People/People.sqlite' AS 'PEOPLE'")
    funcfile.writelog("%t ATTACH DATABASE: PEOPLE.SQLITE")
    so_curs.execute(f"ATTACH DATABASE '{funcconf.drive_data_raw}Vss/Vss.sqlite' AS 'VSS'")
    funcfile.writelog("%t ATTACH DATABASE: VSS.SQLITE")
    so_curs.execute(f"ATTACH DATABASE '{funcconf.drive_data_raw}Vss/Vss_{s_prev_year}.sqlite' AS 'VSSOLDD'")
    funcfile.writelog(f"%t ATTACH DATABASE: VSS_{s_prev_year}.SQLITE")
    if s_period == "curr":
        so_curs.execute(f"ATTACH DATABASE '{funcconf.drive_data_raw}Kfs/Kfs_curr.sqlite' AS 'KFSTRAN'")
        funcfile.writelog("%t ATTACH DATABASE: KFS_CURR.SQLITE")
        so_curs.execute(f"ATTACH DATABASE '{funcconf.drive_data_raw}Vss/Vss_curr.sqlite' AS 'VSSTRAN'")
        funcfile.writelog("%t ATTACH DATABASE: VSS_CURR.SQLITE")
    elif s_period == "prev":
        so_curs.execute(f"ATTACH DATABASE '{funcconf.drive_data_raw}Kfs/Kfs_prev.sqlite' AS 'KFSTRAN'")
        funcfile.writelog("%t ATTACH DATABASE: KFS_PREV.SQLITE")
        so_curs.execute(f"ATTACH DATABASE '{funcconf.drive_data_raw}Vss/Vss_prev.sqlite' AS 'VSSTRAN'")
        funcfile.writelog("%t ATTACH DATABASE: VSS_PREV.SQLITE")
    else:
        so_curs.execute(f"ATTACH DATABASE '{funcconf.drive_data_raw}Kfs/Kfs_{s_year}.sqlite' AS 'KFSTRAN'")
        funcfile.writelog(f"%t ATTACH DATABASE: KFS_{s_year}.SQLITE")
        so_curs.execute(f"ATTACH DATABASE '{funcconf.drive_data_raw}Vss/Vss_{s_year}.sqlite' AS 'VSSTRAN'")
        funcfile.writelog(f"%t ATTACH DATABASE: VSS_{s_year}.SQLITE")

    """*****************************************************************************
    BEGIN OF SCRIPT
    *****************************************************************************"""

    # Extract vss transactions from VSS.SQLITE *********************************
    if l_debug:
        print("Import vss transactions from VSS.SQLITE...")
    sr_file = "X002aa_vss_tranlist"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """

    ;"""
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: "+sr_file)

    # MESSAGE
    # if l_mess:
    #     funcsms.send_telegram('', 'administrator', 'Finished building <b>student debtor</b> reconciliations.')

    """*****************************************************************************
    END OF SCRIPT
    *****************************************************************************"""

    print("END OF SCRIPT")
    funcfile.writelog("END OF SCRIPT")

    # COMMIT DATA
    so_conn.commit()

    # CLOSE THE DATABASE CONNECTION
    so_conn.close()

    # CLOSE THE LOG WRITER
    funcfile.writelog("------------------------------------")
    funcfile.writelog("COMPLETED: C200_REPORT_STUDDEB_RECON")

    return


if __name__ == '__main__':
    try:
        # Current year own gl opening balances
        report_studdeb_recon(0,0,0,"curr")
        # 2024 balances real values
        # report_studdeb_recon(168408077.92, 147644293.06, 76500858.10, "curr")
        # Previous year own gl opening balances
        # report_studdeb_recon(0,0,0,"prev")
        # 2023 balances real values
        # report_studdeb_recon(43861754.51, 19675773.32, 14658226.87, "curr")
        # 2022 balances journal test values
        # report_studdeb_recon(40961071.35, 6594337.25, 28983815.79, "curr")
        # 2022 balances real values
        # report_studdeb_recon(40960505.33, 6573550.30, 29005168.76, "curr")
        # 2021 balances
        # report_studdeb_recon(65676774.13, 61655697.80, 41648563.00, "curr")
        # 2020 balances
        # report_studdeb_recon(48501952.09, -12454680.98, 49976048.39, "prev")
        # 2019 balances
        # report_studdeb_recon(66561452.48,-18340951.06,39482933.18, "prev")
    except Exception as e:
        funcsys.ErrMessage(e, funcconf.l_mess_project, "B003_vss_lists", "B003_vss_lists")
