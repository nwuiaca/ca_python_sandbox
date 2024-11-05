"""
SCRIPT TO BUILD STUDENT DEFERMENT MASTER FILES
Author: Albert J v Rensburg (NWU21162395)
Created: 19 MAR 2018
Edited: 10 Apr 2020
"""

# IMPORT PYTHON MODULES
import csv
import sqlite3

# IMPORT OWN MODULES
from _my_modules import funcconf
from _my_modules import funcdatn
from _my_modules import funccsv
from _my_modules import funcfile
from _my_modules import funcsms
from _my_modules import funcsys

""" INDEX **********************************************************************
ENVIRONMENT
OPEN THE DATABASES
TEMPORARY AREA
BEGIN OF SCRIPT
BUILD DEFERMENTS
OBTAIN STUDENTS
OBTAIN STUDENT TRANSACTIONS AND CALCULATE BALANCES
ADD BALANCES TO STUDENTS
CALCULATE DEFERMENT STATUS
END OF SCRIPT
*****************************************************************************"""


def studdeb_deferments(s_period="curr"):
    """
    Script to build standard STUDENT DEBTOR DEFERMENT lists
    :type s_period: str: The financial period (curr, prev or year)
    :return: Nothing
    """

    # TODO Convert student deferments to vss_deferments_yyyy.sqlite from vss_deferment.sqlite

    """*****************************************************************************
    ENVIRONMENT
    *****************************************************************************"""

    # DECLARE VARIABLES
    l_debug: bool = True
    so_path = f"{funcconf.drive_data_raw}Vss_deferment/"  # Source database path
    if s_period == "curr":
        s_year = funcdatn.get_current_year()
        #so_file = "Vss_deferment_curr.sqlite"  # Source database
        so_file = "Vss_deferment.sqlite"  # Source database
    elif s_period == "prev":
        s_year = funcdatn.get_previous_year()
        #so_file = "Vss_deferment_prev.sqlite"  # Source database
        so_file = "Vss_deferment.sqlite"  # Source database
    else:
        s_year = s_period
        # so_file = "Vss_deferment_" + s_year + ".sqlite"  # Source database
        so_file = "Vss_deferment.sqlite"  # Source database
    ed_path = f"{funcconf.drive_system}_external_data/"  # External data path
    l_export: bool = True
    l_mail = False

    # SCRIPT LOG FILE
    funcfile.writelog("Now")
    funcfile.writelog("SCRIPT: REPORT_VSS_DEFERMENTS")
    funcfile.writelog("-----------------------------")
    print("---------------------")
    print("REPORT_VSS_DEFERMENTS")
    print("---------------------")

    # MESSAGE
    if funcconf.l_mess_project:
        funcsms.send_telegram("", "administrator", "<b>C301 Student " + s_year + " deferments</b>")

    """*****************************************************************************
    OPEN THE DATABASES
    *****************************************************************************"""
    print("OPEN THE DATABASES")
    funcfile.writelog("OPEN THE DATABASES")

    # OPEN SQLITE SOURCE table
    print("Open sqlite database...")
    with sqlite3.connect(so_path+so_file) as so_conn:
        so_curs = so_conn.cursor()
    funcfile.writelog("OPEN DATABASE: " + so_file)

    # ATTACH VSS DATABASE
    print("Attach vss database...")
    so_curs.execute(f"ATTACH DATABASE '{funcconf.drive_data_raw}Vss/Vss.sqlite' AS 'VSS'")
    funcfile.writelog("%t ATTACH DATABASE: Vss.sqlite")
    so_curs.execute(f"ATTACH DATABASE '{funcconf.drive_data_raw}Vss/Vss_curr.sqlite' AS 'VSSCURR'")
    funcfile.writelog("%t ATTACH DATABASE: Vss_curr.sqlite")
    so_curs.execute(f"ATTACH DATABASE '{funcconf.drive_data_raw}Vss/Vss_prev.sqlite' AS 'VSSPREV'")
    funcfile.writelog("%t ATTACH DATABASE: Vss_prev.sqlite")

    """ ****************************************************************************
    TEMPORARY AREA
    *****************************************************************************"""
    print("TEMPORARY AREA")
    funcfile.writelog("TEMPORARY AREA")

    so_curs.execute("DROP TABLE IF EXISTS X000_Deferments_curr")
    so_curs.execute("DROP TABLE IF EXISTS X000_Deferments_prev")
    so_curs.execute("DROP TABLE IF EXISTS X000_Students_curr")
    so_curs.execute("DROP TABLE IF EXISTS X000_Tran_balopen_curr")
    so_curs.execute("DROP TABLE IF EXISTS X000_Tran_feereg_curr")
    so_curs.execute("DROP TABLE IF EXISTS X001_DEFERMENTS_CURR")
    so_curs.execute("DROP TABLE IF EXISTS X001_DEFERMENTS_PREV")

    """ ****************************************************************************
    BEGIN OF SCRIPT
    *****************************************************************************"""
    print("BEGIN OF SCRIPT")
    funcfile.writelog("BEGIN OF SCRIPT")      

    """ ****************************************************************************
    BUILD DEFERMENTS
    *****************************************************************************"""
    print("BUILD DEFERMENTS")
    funcfile.writelog("BUILD DEFERMENTS")      

    # ADD DESCRIPTIONS TO DEFERMENTS
    print("Build deferments...")
    sr_file = "X000_Deferments"
    s_sql = "CREATE TABLE " + sr_file + " AS" + """
    SELECT
      DEFER.KACCDEFERMENTID,
      DEFER.FACCID,
      STUDACC.FBUSENTID,
      DEFER.DATEARRANGED,
      USER.FUSERBUSINESSENTITYID,
      DEFER.STARTDATE,
      DEFER.ENDDATE,
      DEFER.TOTALAMOUNT,
      SUBACC.CODESHORTDESCRIPTION AS SUBACCOUNTTYPE,
      TYPE.CODESHORTDESCRIPTION AS DEFERMENTTYPE,
      REAS.CODESHORTDESCRIPTION AS DEFERMENTREASON,
      DEFER.NOTE,
      DEFER.FAUDITUSERCODE,
      DEFER.AUDITDATETIME,
      DEFER.FAUDITSYSTEMFUNCTIONID
    FROM
      ACCDEFERMENT DEFER
      LEFT JOIN VSS.CODEDESCRIPTION SUBACC ON SUBACC.KCODEDESCID = DEFER.FSUBACCTYPECODEID
      LEFT JOIN VSS.CODEDESCRIPTION TYPE ON TYPE.KCODEDESCID = DEFER.FDEFERMENTTYPECODEID
      LEFT JOIN VSS.CODEDESCRIPTION REAS ON REAS.KCODEDESCID = DEFER.FDEFERMENTREASONCODEID
      LEFT JOIN VSS.STUDACC STUDACC ON STUDACC.KACCID = DEFER.FACCID
      LEFT JOIN VSS.SYSTEMUSER USER ON USER.KUSERCODE = DEFER.FAUDITUSERCODE
    WHERE
      SUBACC.KSYSTEMLANGUAGECODEID = 3 AND
      TYPE.KSYSTEMLANGUAGECODEID = 3 AND
      REAS.KSYSTEMLANGUAGECODEID = 3
    ORDER BY
      STUDACC.FBUSENTID,
      DEFER.AUDITDATETIME
    """
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD THE PERIOD DEFERMENT LIST
    print("Select the deferment period...")
    sr_file = "X000_Deferments_select"
    s_sql = "CREATE TABLE " + sr_file + " AS" + """
    SELECT
      DEFER.KACCDEFERMENTID,
      DEFER.FBUSENTID AS 'STUDENT',
      SITE.FDEBTCOLLECTIONSITE AS 'CAMPUS',
      DEFER.DATEARRANGED,
      DEFER.FUSERBUSINESSENTITYID AS 'EMPLOYEE',
      DEFER.STARTDATE AS 'DATESTART',
      DEFER.ENDDATE AS 'DATEEND',
      DEFER.TOTALAMOUNT,
      DEFER.SUBACCOUNTTYPE,
      DEFER.DEFERMENTTYPE,
      DEFER.DEFERMENTREASON,
      DEFER.NOTE,
      DEFER.FAUDITUSERCODE,
      DEFER.AUDITDATETIME,
      DEFER.FAUDITSYSTEMFUNCTIONID,
      SITE.FADMISSIONSITE,
      SITE.FMAINQUALSITE
    FROM
      X000_DEFERMENTS DEFER
      LEFT JOIN STUDENTSITE SITE ON SITE.KSTUDENTBUSENTID = DEFER.FBUSENTID
    WHERE
      SITE.KSTARTDATETIME <= DEFER.DATEARRANGED AND
      SITE.ENDDATETIME > DEFER.DATEARRANGED AND
      DEFER.STARTDATE >= Date('%YEARB%') AND
      DEFER.ENDDATE <= Date('%YEARE%')
    """
    if s_period == "curr":
        s_sql = s_sql.replace("%YEARB%", funcdatn.get_current_year_begin())
        s_sql = s_sql.replace("%YEARE%", funcdatn.get_current_year_end())
    elif s_period == "prev":
        s_sql = s_sql.replace("%YEARB%", funcdatn.get_previous_year_begin())
        s_sql = s_sql.replace("%YEARE%", funcdatn.get_previous_year_end())
    else:
        s_sql = s_sql.replace("%YEARB%", s_year + "-01-01")
        s_sql = s_sql.replace("%YEARE%", s_year + "-12-31")
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD TABLE: " + sr_file)
    # MESSAGE
    if funcconf.l_mess_project:
        i = funcsys.tablerowcount(so_curs, sr_file)
        funcsms.send_telegram("", "administrator", "<b>" + str(i) + "</b> Deferments")
    # Export the declaration data
    if l_export:
        if s_period == "curr":
            sx_path = f"{funcconf.drive_data_results}Debtorstud/" + funcdatn.get_current_year() + "/"
        elif s_period == "prev":
            sx_path = f"{funcconf.drive_data_results}Debtorstud/" + funcdatn.get_previous_year() + "/"
        else:
            sx_path = f"{funcconf.drive_data_results}Debtorstud/" + s_year + "/"
        sx_file = "Deferment_000_list_"
        print("Export data..." + sx_path + sx_file)
        s_head = funccsv.get_colnames_sqlite(so_conn, sr_file)
        funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file, s_head)
        funcfile.writelog("%t EXPORT DATA: " + sx_path + sx_file)

    """ ****************************************************************************
    OBTAIN STUDENTS
    *****************************************************************************"""
    print("OBTAIN STUDENTS")
    funcfile.writelog("OBTAIN STUDENTS")      

    # OBTAIN THE LIST STUDENTS
    print("Obtain the registered students...")
    sr_file = "X000_Students"
    s_sql = "CREATE TABLE " + sr_file + " AS" + """
    SELECT
      STUD.*,
      CASE
          WHEN DATEENROL < STARTDATE THEN STARTDATE
          ELSE DATEENROL
      END AS DATEENROL_CALC
    FROM
      %VSS%.X001_Student STUD
    WHERE
      UPPER(STUD.QUAL_TYPE) Not Like '%SHORT COURSE%' AND
      STUD.ISMAINQUALLEVEL = 1 AND
      UPPER(STUD.ACTIVE_IND) = 'ACTIVE'
    """
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if s_period == "prev":
        s_sql = s_sql.replace("%VSS%", "VSSPREV")
    else:
        s_sql = s_sql.replace("%VSS%", "VSSCURR")
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    """ ****************************************************************************
    OBTAIN STUDENT TRANSACTIONS AND CALCULATE BALANCES
    *****************************************************************************"""
    print("OBTAIN STUDENT TRANSACTIONS")
    funcfile.writelog("OBTAIN STUDENT TRANSACTIONS")      

    # OBTAIN STUDENT ACCOUNT TRANSACTIONS
    print("Import student transactions...")
    sr_file = "X000_Transaction"
    s_sql = "CREATE TABLE " + sr_file + " AS" + """
    Select
      TRAN.FBUSENTID As STUDENT,
      CASE
        WHEN TRAN.FDEBTCOLLECTIONSITE = '-9' THEN 'MAFIKENG'
        WHEN TRAN.FDEBTCOLLECTIONSITE = '-2' THEN 'VAAL TRIANGLE'
        ELSE 'POTCHEFSTROOM'
      END AS CAMPUS,
      TRAN.TRANSDATE,
      TRAN.TRANSDATETIME,
      CASE
        WHEN SUBSTR(TRAN.TRANSDATE,6,5)='01-01' AND INSTR('001z031z061',TRAN.TRANSCODE)>0 THEN '00'
        WHEN strftime('%Y',TRAN.TRANSDATE)>strftime('%Y',TRAN.POSTDATEDTRANSDATE) THEN strftime('%m',TRAN.TRANSDATE)
        ELSE strftime('%m',TRAN.TRANSDATE)
      END AS MONTH,
      TRAN.TRANSCODE,
      TRAN.AMOUNT,
      CASE
        WHEN TRAN.AMOUNT > 0 THEN TRAN.AMOUNT
        ELSE 0.00
      END AS AMOUNT_DT,
      CASE
        WHEN TRAN.AMOUNT < 0 THEN TRAN.AMOUNT
        ELSE 0.00
      END AS AMOUNT_CR,
      TRAN.DESCRIPTION_E As TRANSDESC
    FROM
      %VSS%.X010_Studytrans TRAN
    WHERE
      TRAN.TRANSCODE <> ''
    """
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if s_period == "prev":
        s_sql = s_sql.replace("%VSS%", "VSSPREV")
    else:
        s_sql = s_sql.replace("%VSS%", "VSSCURR")
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # CALCULATE THE STUDENT ACCOUNT OPENING BALANCE
    print("Calculate the account opening balance...")
    sr_file = "X001aa_Trans_balopen"
    s_sql = "CREATE VIEW " + sr_file + " AS" + """
    SELECT
      TRAN.STUDENT,
      CAST(ROUND(TOTAL(TRAN.AMOUNT),2) AS REAL) AS BAL_OPEN
    FROM
      X000_Transaction TRAN
    WHERE
      TRAN.MONTH = '00'
    GROUP BY
      TRAN.STUDENT
    """
    so_curs.execute("DROP VIEW IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD VIEW: " + sr_file)

    # CALCULATE THE REGISTRATION FEES LEVIED
    print("Calculate the registration fee transactions...")
    sr_file = "X001ab_Trans_feereg"
    s_sql = "CREATE VIEW " + sr_file + " AS" + """
    SELECT
      TRAN.STUDENT,
      CAST(ROUND(TOTAL(TRAN.AMOUNT),2) AS REAL) AS FEE_REG
    FROM
      X000_Transaction TRAN
    WHERE
      TRAN.TRANSCODE = "002" Or
      TRAN.TRANSCODE = "095"
    GROUP BY
      TRAN.STUDENT
    """
    so_curs.execute("DROP VIEW IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD VIEW: " + sr_file)

    # ADD THE REGISTRATION DATE TO THE LIST OF TRANSACTIONS
    print("Add the registration date to the list of transactions...")
    sr_file = "X001ac_Trans_addreg"
    s_sql = "CREATE VIEW " + sr_file + " AS" + """
    SELECT
      TRAN.*,
      STUD.DATEENROL_CALC
    FROM
      X000_Transaction TRAN
      INNER JOIN X000_Students STUD ON STUD.KSTUDBUSENTID = TRAN.STUDENT
    """
    so_curs.execute("DROP VIEW IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD VIEW: " + sr_file)

    # CALCULATE THE STUDENT ACCOUNT BALANCE ON REGISTRATION DATE
    print("Calculate the account balance on registration date...")
    sr_file = "X001ad_Trans_balreg"
    s_sql = "CREATE VIEW " + sr_file + " AS" + """
    SELECT
      TRAN.STUDENT,
      CAST(ROUND(TOTAL(TRAN.AMOUNT),2) AS REAL) AS BAL_REG
    FROM
      X001ac_Trans_addreg TRAN
    WHERE
      TRAN.TRANSDATE <= TRAN.DATEENROL_CALC
    GROUP BY
      TRAN.STUDENT
    """
    so_curs.execute("DROP VIEW IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD VIEW: " + sr_file)

    # CALCULATE THE STUDENT ACCOUNT CREDIT TRANSACTIONS BEFORE REGISTRATION
    print("Calculate the credits after registration date...")
    sr_file = "X001ae_Trans_crebefreg"
    s_sql = "CREATE VIEW " + sr_file + " AS" + """
    SELECT
      TRAN.STUDENT,
      CAST(ROUND(TOTAL(TRAN.AMOUNT_CR),2) AS REAL) AS CRE_REG_BEFORE
    FROM
      X001ac_Trans_addreg TRAN
    WHERE
      TRAN.MONTH <> '00' AND
      TRAN.TRANSDATE <= TRAN.DATEENROL_CALC
    GROUP BY
      TRAN.STUDENT
    """
    so_curs.execute("DROP VIEW IF EXISTS " + sr_file)
    so_curs.execute("DROP VIEW IF EXISTS X001ae_Trans_crereg")
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD VIEW: " + sr_file)

    # CALCULATE THE STUDENT ACCOUNT CREDIT TRANSACTIONS AFTER REGISTRATION
    print("Calculate the credits after registration date...")
    sr_file = "X001af_Trans_creaftreg"
    s_sql = "CREATE VIEW " + sr_file + " AS" + """
    SELECT
      TRAN.STUDENT,
      CAST(ROUND(TOTAL(TRAN.AMOUNT_CR),2) AS REAL) AS CRE_REG_AFTER
    FROM
      X001ac_Trans_addreg TRAN
    WHERE
      TRAN.MONTH <> '00' AND
      TRAN.TRANSDATE > TRAN.DATEENROL_CALC
    GROUP BY
      TRAN.STUDENT
    """
    so_curs.execute("DROP VIEW IF EXISTS " + sr_file)
    so_curs.execute("DROP VIEW IF EXISTS X001ae_Trans_crereg")
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD VIEW: " + sr_file)

    # CALCULATE THE STUDENT ACCOUNT BALANCE
    print("Calculate the account balance...")
    sr_file = "X001ag_Trans_balance"
    s_sql = "CREATE VIEW " + sr_file + " AS" + """
    SELECT
      TRAN.STUDENT,
      CAST(ROUND(TOTAL(TRAN.AMOUNT),2) AS REAL) AS BAL_CUR
    FROM
      X000_Transaction TRAN
    GROUP BY
      TRAN.STUDENT
    """
    so_curs.execute("DROP VIEW IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD VIEW: " + sr_file)

    # CALCULATE THE DEFERMENT DATE
    print("Calculate the deferment date per student...")
    sr_file = "X002aa_Defer_date"
    s_sql = "CREATE VIEW " + sr_file + " AS" + """
    Select
        DEFER.STUDENT,
        DEFER.DATEEND
    From
        X000_Deferments_select DEFER
    Group By
        DEFER.STUDENT
    Order By
        DEFER.DATEEND
    """
    so_curs.execute("DROP VIEW IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD VIEW: " + sr_file)

    # CALCULATE THE STUDENT ACCOUNT CREDIT TRANSACTIONS BEFORE DEFERMENT DATE
    print("Calculate the credits up to deferment date...")
    sr_file = "X002ab_Trans_crebefdef"
    s_sql = "CREATE VIEW " + sr_file + " AS" + """
    Select
        TRAN.STUDENT,
        Cast(Round(Total(TRAN.AMOUNT_CR),2) As REAL) As CRE_DEF_BEFORE
    From
        X000_Transaction TRAN Inner Join
        X002aa_Defer_date DDATE On DDATE.STUDENT = TRAN.STUDENT
    Where
        TRAN.MONTH <> '00' And
        TRAN.TRANSDATE <= DDATE.DATEEND
    Group By
        TRAN.STUDENT
    """
    so_curs.execute("DROP VIEW IF EXISTS " + sr_file)
    so_curs.execute("DROP VIEW IF EXISTS X001ae_Trans_crereg")
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD VIEW: " + sr_file)

    """ ****************************************************************************
    ADD BALANCES TO STUDENTS
    *****************************************************************************"""
    print("ADD BALANCES TO STUDENTS")
    funcfile.writelog("ADD BALANCES TO STUDENTS")      

    # ADD THE BALANCES TO THE LIST OF REGISTERED STUDENTS
    print("Add the calculated balances to the students list...")
    sr_file = "X001aa_Students"
    s_sql = "CREATE TABLE " + sr_file + " AS" + """
    Select
      STUD.*,
      BOPEN.BAL_OPEN,
      CBREG.CRE_REG_BEFORE,
      CAST(0 AS REAL) AS BAL_REG_CALC,
      BREG.BAL_REG,
      CAREG.CRE_REG_AFTER,
      CAST(0 AS REAL) AS BAL_CRE_CALC,
      BAL.BAL_CUR,
      FEE.FEE_REG,
      CBDEF.CRE_DEF_BEFORE,
      CAST(0 AS REAL) AS BAL_DEF_CALC
    From
      X000_Students STUD Left Join
      X001aa_Trans_balopen BOPEN ON BOPEN.STUDENT = STUD.KSTUDBUSENTID Left Join
      X001ad_Trans_balreg BREG ON BREG.STUDENT = STUD.KSTUDBUSENTID Left Join
      X001ae_Trans_crebefreg CBREG ON CBREG.STUDENT = STUD.KSTUDBUSENTID Left Join
      X001af_Trans_creaftreg CAREG ON CAREG.STUDENT = STUD.KSTUDBUSENTID Left Join
      X001ab_Trans_feereg FEE ON FEE.STUDENT = STUD.KSTUDBUSENTID Left Join
      X001ag_Trans_balance BAL ON BAL.STUDENT = STUD.KSTUDBUSENTID Left Join
      X002ab_Trans_crebefdef CBDEF ON CBDEF.STUDENT = STUD.KSTUDBUSENTID  
    """
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD TABLE: " + sr_file)
    # Calc balance after credits up to registration
    print("Add column bal_reg_calc...")
    so_curs.execute("UPDATE " + sr_file + """
                    SET BAL_REG_CALC =
                    CASE
                        WHEN TYPEOF(BAL_OPEN) = "null" AND TYPEOF(CRE_REG_BEFORE) = "null" THEN 0
                        WHEN TYPEOF(BAL_OPEN) = "null" THEN CRE_REG_BEFORE
                        WHEN TYPEOF(CRE_REG_BEFORE) = "null"  THEN BAL_OPEN
                        ELSE BAL_OPEN + CRE_REG_BEFORE
                    END
                    ;""")
    so_conn.commit()
    funcfile.writelog("%t ADD COLUMN: bal_reg_calc")
    # Calc balance including all credits
    print("Add column bal_cre_calc...")
    so_curs.execute("UPDATE " + sr_file + """
                    SET BAL_CRE_CALC =
                    CASE
                        WHEN TYPEOF(CRE_REG_AFTER) = "null"  THEN BAL_REG_CALC
                        ELSE BAL_REG_CALC + CRE_REG_AFTER
                    END
                    ;""")
    so_conn.commit()
    funcfile.writelog("%t ADD COLUMN: bal_cre_calc")
    # Calc balance after credits up to registration
    print("Add column bal_def_calc...")
    so_curs.execute("UPDATE " + sr_file + """
                    SET BAL_DEF_CALC =
                    CASE
                        WHEN TYPEOF(BAL_OPEN) = "null" AND TYPEOF(CRE_DEF_BEFORE) = "null" THEN BAL_CRE_CALC
                        WHEN TYPEOF(BAL_OPEN) = "null" THEN CRE_DEF_BEFORE
                        WHEN TYPEOF(CRE_DEF_BEFORE) = "null"  THEN BAL_CRE_CALC
                        ELSE BAL_CRE_CALC
                    END
                    ;""")
    so_conn.commit()
    funcfile.writelog("%t ADD COLUMN: bal_def_calc")

    # CALCULATE THE STUDENT ACCOUNT CREDIT TRANSACTIONS BEFORE REGISTRATION
    print("Join students and deferments...")
    sr_file = "X001ab_Students_deferment"
    s_sql = "CREATE TABLE " + sr_file + " AS" + """
    Select
        X001aa_Students.*,
        X000_Deferments_select.*
    From
        X001aa_Students Left Join
        X000_Deferments_select On X000_Deferments_select.STUDENT = X001aa_Students.KSTUDBUSENTID
    """
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD VIEW: " + sr_file)

    """ ****************************************************************************
    CALCULATE DEFERMENT STATUS
    *****************************************************************************"""
    print("CALCULATE DEFERMENT STATUS")
    funcfile.writelog("CALCULATE DEFERMENT STATUS") 

    # CALCULATE THE DEFERMENT TYPE
    print("Calculate the deferment type...")
    so_curs.execute("ALTER TABLE "+sr_file+" ADD COLUMN DEFER_TYPE INT;")
    s_sql = "UPDATE " + sr_file + """
    SET DEFER_TYPE =
    CASE
        WHEN BAL_REG_CALC <= 0 THEN 0
        WHEN BAL_REG_CALC > 0 And BAL_REG_CALC <= 1000 THEN 1
        WHEN BAL_REG_CALC > 1000 And BAL_DEF_CALC <= 0 THEN 2
        WHEN BAL_REG_CALC > 1000 And STUDENT IS NULL THEN 3
        WHEN BAL_REG_CALC > 1000 And DATEEND = '%YEARE%' THEN 6
        WHEN BAL_REG_CALC > 1000 And DATEEND >= '%TODAY%' THEN 5
        WHEN BAL_REG_CALC > 1000 And BAL_DEF_CALC > 0 THEN 4
        WHEN BAL_REG_CALC > 1000 THEN 7
        ELSE 8
    END;"""
    if s_period == "curr":
        s_sql = s_sql.replace("%YEARE%", funcdatn.get_current_year_end())
    elif s_period == "prev":
        s_sql = s_sql.replace("%YEARE%", funcdatn.get_previous_year_end())
    else:
        s_sql = s_sql.replace("%YEARE%", s_year + "-12-31")
    s_sql = s_sql.replace("%TODAY%", funcdatn.get_today_date())
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t ADD COLUMN: DEFER_TYPE")

    # CALCULATE THE DEFERMENT TYPE
    print("Calculate the deferment type description...")
    so_curs.execute("ALTER TABLE "+sr_file+" ADD COLUMN DEFER_TYPE_DESC TEXT;")
    s_sql = "UPDATE " + sr_file + """
    SET DEFER_TYPE_DESC =
    CASE
        WHEN DEFER_TYPE = 0 THEN 'CREDIT ACCOUNT WITH REGISTRATION'
        WHEN DEFER_TYPE = 1 THEN 'ACCOUNT LESS THAN R1000 WITH REGISTRATION'
        WHEN DEFER_TYPE = 2 THEN 'ACCOUNT SETTLED ON AGREEMENT DATE'
        WHEN DEFER_TYPE = 3 THEN 'REGISTERED WITHOUT AGREEMENT'
        WHEN DEFER_TYPE = 4 THEN 'ACCOUNT IN ARREARS ON AGREEMENT DATE'
        WHEN DEFER_TYPE = 5 THEN 'FUTURE AGREEMENT DATE'
        WHEN DEFER_TYPE = 6 THEN 'FULL YEAR DEFERMENT'
        ELSE 'OTHER'
    END;"""
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t ADD COLUMN: DEFER_TYPE_DESC")

    """ ****************************************************************************
    BUILD THE FINAL DEFERMENTS
    *****************************************************************************"""
    print("BUILD THE FINAL DEFERMENTS")
    funcfile.writelog("BUILD THE FINAL DEFERMENTS") 

    # FINAL DEFERMENTS TABLE
    print("Build the final deferments table...")
    sr_file = "X001ax_Deferments_final_"+s_period
    s_sql = "CREATE TABLE " + sr_file + " AS" + """
    Select
        Case
            When DEFER.FSITEORGUNITNUMBER = -9 Then 'MAFIKENG'
            When DEFER.FSITEORGUNITNUMBER = -2 Then 'VAAL TRIANGLE'
            Else 'POTCHEFSTROOM'
        End As CAMPUS,
        DEFER.KSTUDBUSENTID As STUDENT_VSS,
        DEFER.DATEQUALLEVELSTARTED As DATEQUALSTART,
        DEFER.DATEENROL_CALC AS DATEENROL,
        DEFER.DEFER_TYPE,
        DEFER.DEFER_TYPE_DESC,
        DEFER.BAL_OPEN,
        DEFER.CRE_REG_BEFORE,
        DEFER.BAL_REG_CALC,
        DEFER.CRE_DEF_BEFORE,
        DEFER.BAL_DEF_CALC,
        DEFER.CRE_REG_AFTER,
        DEFER.BAL_CRE_CALC,
        DEFER.BAL_CUR,
        Upper(DEFER.QUALIFICATIONCODE || ' ' || DEFER.QUALIFICATIONFIELDOFSTUDY || ' ' || DEFER.QUALIFICATIONLEVEL) As
        QUALIFICATION,
        Upper(DEFER.QUAL_TYPE) As QUAL_TYPE,
        DEFER.ISMAINQUALLEVEL As MAIN_IND,
        DEFER.ENROLACADEMICYEAR As YEAR_ACAD,
        DEFER.ENROLHISTORYYEAR As YEAR_HIST,
        Upper(DEFER.ENTRY_LEVEL) As ENTRY_LEVEL,
        Upper(DEFER.ENROL_CAT) As ENROL_CAT,
        Upper(DEFER.PRESENT_CAT) As PRESENT_CAT,
        Upper(DEFER.STATUS_FINAL) As STATUS_FINAL,
        Upper(DEFER.LEVY_CATEGORY) As LEVY_CATEGORY,
        Upper(DEFER.ORGUNIT_NAME) As ORGUNIT_NAME,
        DEFER.FSITEORGUNITNUMBER As CAMPUS_CODE,
        DEFER.STUDENT As STUDENT_DEF,
        DEFER.DATEARRANGED,
        DEFER.DATESTART,
        DEFER.DATEEND,
        DEFER.TOTALAMOUNT,
        Upper(DEFER.SUBACCOUNTTYPE) As SUBACCOUNTTYPE,
        Upper(DEFER.DEFERMENTTYPE) As DEFERMENTTYPE,
        Upper(DEFER.DEFERMENTREASON) As DEFERMENTREASON,
        Upper(DEFER.NOTE) As NOTE,
        DEFER.FADMISSIONSITE,
        DEFER.FMAINQUALSITE,
        DEFER.EMPLOYEE,
        DEFER.FAUDITUSERCODE,
        DEFER.AUDITDATETIME,
        DEFER.DISCONTINUEDATE,
        Upper(DEFER.DISCONTINUE_REAS) As DISCONTINUE_REAS
    From
        X001ab_Students_deferment DEFER
    Order By
        DEFER.FSITEORGUNITNUMBER,
        DEFER.DEFER_TYPE,    
        BAL_CUR
    """
    """ DEFER COLUMNS AVAILABLE BUT NOT USED ***********************************
        DEFER.DATEENROL,
        DEFER.STARTDATE,
        DEFER.ENDDATE,
        Upper(DEFER.ACTIVE_IND) As ACTIVE_IND,
        DEFER.BAL_REG,        
    *************************************************************************"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD TABLE: " + sr_file)
    # Export the declaration data
    if l_export:
        if s_period == "curr":
            sx_path = f"{funcconf.drive_data_results}Debtorstud/" + funcdatn.get_current_year() + "/"
        elif s_period == "prev":
            sx_path = f"{funcconf.drive_data_results}Debtorstud/" + funcdatn.get_previous_year() + "/"
        else:
            sx_path = f"{funcconf.drive_data_results}Debtorstud/" + s_year + "/"
        sx_file = "Deferment_001_student_"
        print("Export data..." + sx_path + sx_file)
        s_head = funccsv.get_colnames_sqlite(so_conn, sr_file)
        funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file, s_head)
        funcfile.writelog("%t EXPORT DATA: " + sx_path + sx_file)

    # SUMMARIZE
    print("Summarize registrations with accounts...")
    sr_file = "X001ac_Students_deferment_summ_"+s_period
    s_sql = "CREATE TABLE " + sr_file + " AS" + """
    Select
        '%YEAR%' As YEAR,
        DEFER.CAMPUS,
        DEFER.DEFER_TYPE,
        DEFER.DEFER_TYPE_DESC,
        Cast(Count(DEFER.STUDENT_VSS) As INT) As STUD_COUNT,
        Cast(Round(Total(DEFER.BAL_REG_CALC),2) As REAL) As BAL_REG_DATE,
        Cast(Round(Sum(DEFER.BAL_DEF_CALC),2) As REAL) As BAL_DEF_DATE,
        Cast(Round(Sum(DEFER.BAL_CUR),2) As REAL) As BAL_CUR
    From
        X001ax_Deferments_final_%PERIOD% DEFER
    Group By
        DEFER.DEFER_TYPE,
        DEFER.DEFER_TYPE_DESC,
        DEFER.CAMPUS
    Order By
        DEFER.CAMPUS,
        DEFER.DEFER_TYPE
    """
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = s_sql.replace("%PERIOD%", s_period)
    s_sql = s_sql.replace("%YEAR%", s_year)
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD TABLE: " + sr_file)
    # Export the summary
    if l_export:
        if s_period == "curr":
            sx_path = f"{funcconf.drive_data_results}Debtorstud/" + funcdatn.get_current_year() + "/"
        elif s_period == "prev":
            sx_path = f"{funcconf.drive_data_results}Debtorstud/" + funcdatn.get_previous_year() + "/"
        else:
            sx_path = f"{funcconf.drive_data_results}Debtorstud/" + s_year + "/"
        sx_file = "Deferment_001_summary_"
        print("Export data..." + sx_path + sx_file)
        s_head = funccsv.get_colnames_sqlite(so_conn, sr_file)
        funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file, s_head)
        funcfile.writelog("%t EXPORT DATA: " + sx_path + sx_file)

    # CREATE DUPLICATE SUMMARY FILE TO RECEIVE SUMMARY FOR PREVIOUS YEARS
    print("Summ previous years...")
    sr_file = "X001ad_Deferment_summ"
    s_sql = "Create Table " + sr_file + " As " + """
    Select
        *
    From
        X001ac_Students_deferment_summ_curr
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # GET PREVIOUS YEAR SUMMARIES
    sr_file = "X001ad_Deferment_summ"
    print("Import previous deferment summaries...")
    co = open(ed_path + "301_Deferment_summ.csv", "r")
    co_reader = csv.reader(co)
    # Read the COLUMN database data
    for row in co_reader:
        # Populate the column variables
        if row[0] == "YEAR":
            continue
        else:
            s_cols = "INSERT INTO " + sr_file + " VALUES('" + row[0] + "','" + row[1] + "','" + row[2] + "','" + row[3] + "','" + row[4] + "','" + row[5] + "','" + row[6] + "','" + row[7] + "')"
            so_curs.execute(s_cols)
    so_conn.commit()
    # Close the impoted data file
    co.close()
    funcfile.writelog("%t IMPORT TABLE: " + ed_path + "301_Deferment_summ.csv (" + sr_file + ")")

    # CREATE DUPLICATE SUMMARY FILE TO RECEIVE SUMMARY FOR PREVIOUS YEARS
    print("Sort previous years...")
    sr_file = "X001ae_Deferment_summ_sort"
    s_sql = "Create Table " + sr_file + " As " + """
    Select
        *
    From
        X001ad_Deferment_summ
    Order By
        DEFER_TYPE,
        CAMPUS,
        YEAR Desc        
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    """ ****************************************************************************
    END OF SCRIPT
    *****************************************************************************"""
    print("END OF SCRIPT")
    funcfile.writelog("END OF SCRIPT")

    # CLOSE THE DATABASE CONNECTION
    so_conn.commit()
    so_conn.close()

    # CLOSE THE LOG WRITER *********************************************************
    funcfile.writelog("--------------------------------")
    funcfile.writelog("COMPLETED: REPORT_VSS_DEFERMENTS")

    return


if __name__ == '__main__':
    try:
        studdeb_deferments()
    except Exception as e:
        funcsys.ErrMessage(e, funcconf.l_mess_project, "C301_report_student_deferment", "C301_report_student_deferment")
