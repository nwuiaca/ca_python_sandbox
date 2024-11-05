"""
Script to build standard VSS lists
Created on: 01 Mar 2018
Copyright: Albert J v Rensburg
"""

# IMPORT PYTHON MODULES
import csv
import sqlite3

# IMPORT OWN MODULES
from _my_modules import funcconf
from _my_modules import funcdatn
from _my_modules import funcfile
from _my_modules import funcsms
from _my_modules import funcsys

""" INDEX **********************************************************************
ENVIRONMENT
BUILD QUALIFICATION MASTER LIST
BUILD MODULES
BUILD PROGRAMS
BUILD BURSARIES
STUDENT ACCOUNT TRANSACTIONS
*****************************************************************************"""


def vss_lists():
    """
    Function to build vss master lists
    :return: Nothing
    """

    """*************************************************************************
    ENVIRONMENT
    *************************************************************************"""
    print("ENVIRONMENT")
    funcfile.writelog("ENVIRONMENT")

    # DECLARE VARIABLES
    so_path: str = f"{funcconf.drive_data_raw}Vss/"  # Source database path
    so_file: str = "Vss.sqlite"  # Source database
    re_path: str = f"{funcconf.drive_data_results}Vss/"  # Results
    ed_path: str = f"{funcconf.drive_system}_external_data/"  # External data location
    s_sql: str = ""  # SQL statements
    l_export: bool = False  # Export files

    # LOG
    print("--------------")
    print("B003_VSS_LISTS")
    print("--------------")
    funcfile.writelog("Now")
    funcfile.writelog("SCRIPT: B003_VSS_LISTS")
    funcfile.writelog("----------------------")
    ilog_severity = 1

    # MESSAGE
    if funcconf.l_mess_project:
        funcsms.send_telegram("", "administrator", "<b>B003 VSS master lists</b>")

    # OPEN DATABASE
    with sqlite3.connect(so_path+so_file) as so_conn:
        so_curs = so_conn.cursor()
    funcfile.writelog("OPEN DATABASE: " + so_file)

    # BUILD CODE DESCRIPTIONS
    print("Build code descriptions...")
    s_sql = "CREATE TABLE X000_Codedescription AS " + """
    SELECT
        CODE.KCODEDESCID,
        CODE.CODELONGDESCRIPTION AS LANK,
        CODE.CODESHORTDESCRIPTION AS KORT,
        LONG.CODELONGDESCRIPTION AS LONG,
        LONG.CODESHORTDESCRIPTION AS SHORT
    FROM
        CODEDESCRIPTION CODE Inner Join
        CODEDESCRIPTION LONG ON LONG.KCODEDESCID = CODE.KCODEDESCID
    WHERE
        CODE.KSYSTEMLANGUAGECODEID = 2 AND
        LONG.KSYSTEMLANGUAGECODEID = 3
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS X000_Codedescription")
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: X000_Codedescription")

    # CREATE SYSTEM FUNCTION MASTER TABLE
    # NOTE - Only records where name purpose is General (Exclude others like web, heading etc.)
    sr_file = "X000_Gradceremony"
    print("Create graduation ceremony master...")
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        GRAD.KGRADUATIONCEREMONYID,
        GRAD.FBUSENTID,
        GRAD.FGRADUATIONCEREMONYCODEID,
        Upper(CERE.LONG) As CEREMONY,
        GRAD.FGRADUATIONCEREMONYTYPECODEID,
        Upper(TYPE.LONG) As CEREMONY_TYPE,
        GRAD.CEREMONYDATETIME,
        GRAD.SESSIONNUMBER,
        GRAD.FRESERVATIONID,
        GRAD.ISDEFAULTCEREMONY,
        GRAD.LOCKSTAMP,
        GRAD.AUDITDATETIME,
        GRAD.FAUDITSYSTEMFUNCTIONID,
        GRAD.FAUDITUSERCODE
    From
        GRADUATIONCEREMONY GRAD Left Join
        X000_Codedescription CERE On CERE.KCODEDESCID = GRAD.FGRADUATIONCEREMONYCODEID Left Join
        X000_Codedescription TYPE On TYPE.KCODEDESCID = GRAD.FGRADUATIONCEREMONYTYPECODEID
    Order By
        KGRADUATIONCEREMONYID
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD ORGANIZATION UNIT NAME
    print("Build org unit name...")
    s_sql = "CREATE VIEW X000_Orgunitname AS " + """
    SELECT
        NAME.KORGUNITNUMBER,
        NAME.KSTARTDATE,
        NAME.ENDDATE,
        NAME.SHORTNAME AS KORT,
        NAME.LONGNAME AS LANK,
        ENGL.SHORTNAME AS SHORT,
        ENGL.LONGNAME AS LONG
    FROM
        ORGUNITNAME NAME Left Join
        ORGUNITNAME ENGL ON ENGL.KORGUNITNUMBER = NAME.KORGUNITNUMBER AND
            ENGL.KSTARTDATE = NAME.KSTARTDATE
    WHERE
        NAME.KSYSTEMLANGUAGECODEID = 2 AND
        ENGL.KSYSTEMLANGUAGECODEID = 3
    ;"""
    so_curs.execute("DROP VIEW IF EXISTS X000_Orgunitname")
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD VIEW: X000_Orgunitname")

    # BUILD ORGANIZATION UNIT
    print("Build org unit...")
    s_sql = "CREATE VIEW X000_Orgunit AS " + """
    SELECT
        ORGU.KORGUNITNUMBER,
        ORGU.STARTDATE,
        ORGU.ENDDATE,
        ORGU.FORGUNITTYPECODEID,
        ORGU.FORGUNITTYPECODE,
        ORGU.ISSITE,
        ORGU.LOCKSTAMP,
        ORGU.AUDITDATETIME,
        ORGU.FAUDITSYSTEMFUNCTIONID,
        ORGU.FAUDITUSERCODE,
        NAME.KSTARTDATE,
        NAME.ENDDATE AS ENDDATE1,
        NAME.KORT,
        NAME.LANK,
        NAME.SHORT,
        NAME.LONG,
        DESC.LONG AS UNIT_TYPE
    FROM
        ORGUNIT ORGU Left Join
        X000_Orgunitname NAME ON NAME.KORGUNITNUMBER = ORGU.KORGUNITNUMBER Left Join
        X000_Codedescription DESC ON DESC.KCODEDESCID = ORGU.FORGUNITTYPECODEID
    WHERE
        NAME.KSTARTDATE <= ORGU.STARTDATE AND
        NAME.ENDDATE >= ORGU.STARTDATE
    ORDER BY
        ORGU.KORGUNITNUMBER
    """
    so_curs.execute("DROP VIEW IF EXISTS X000_Orgunit")
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD VIEW: X000_Orgunit")

    # BUILD ORGANIZATION UNIT INSTANCE
    print("Build org unit instance...")
    s_sql = "CREATE TABLE X000_Orgunitinstance AS " + """
    SELECT
        ORGI.KBUSINESSENTITYID,
        ORGI.FORGUNITNUMBER,  
        ORGU.UNIT_TYPE AS ORGUNIT_TYPE,
        ORGU.LONG AS ORGUNIT_NAME,
        ORGI.FSITEORGUNITNUMBER,
        ORGI.STARTDATE,  
        ORGI.ENDDATE,
        ORGI.FMANAGERTYPECODEID,
        MANT.LONG AS MANAGER_TYPE,
        ORGI.PLANNEDRESTRUCTUREDATE,
        ORGI.FMANAGERTYPECODE,
        ORGI.LOCKSTAMP,
        ORGI.AUDITDATETIME,
        ORGI.FAUDITSYSTEMFUNCTIONID,
        ORGI.FAUDITUSERCODE,
        ORGU.ISSITE,
        ORGU.KORT,
        ORGU.LANK,
        ORGU.SHORT,
        ORGI.FNEWBUSINESSENTITYID  
    FROM
        ORGUNITINSTANCE ORGI Left Join
        X000_Orgunit ORGU ON ORGU.KORGUNITNUMBER = ORGI.FORGUNITNUMBER Left Join
        X000_Codedescription MANT ON MANT.KCODEDESCID = ORGI.FMANAGERTYPECODEID
    ORDER BY
        ORGI.KBUSINESSENTITYID
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS X000_Orgunitinstance")
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: X000_Orgunitinstance")

    # CREATE SYSTEM FUNCTION MASTER TABLE
    # NOTE - Only records where name purpose is General (Exclude others like web, heading etc.)
    sr_file = "X000_Systemfunction"
    print("Create system function master...")
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        SYSF.KSYSTEMFUNCTIONID,
        Upper(NAME.NAME) As SYSTEM_NAME,
        Upper(NAME.DESCRIPTION) As SYSTEM_DESC,
        Upper(PURC.LONG) As SYSTEM_PURPOSE,
        Upper(ACTC.LONG) As SYSTEM_ACTION,
        Upper(NAPC.LONG) As SYSTEM_NAME_PURPOSE,
        SYSF.FBUSINESSAREAID,
        SYSF.FSYSTEMFUNCTIONHIERARCHYID,
        SYSF.FBUSINESSCLASSID,
        SYSF.ISSYSFUNCTIONSTOPINPRODUCTION,
        SYSF.ISONLYASSIGNTOAPPROVEDROLE,
        SYSF.REPORTPHYSICALNAME,
        SYSF.SYSTEMFUNCTIONTYPE,
        SYSF.ISALWAYSINCACHE,
        SYSF.CLASSNAME,
        SYSF.FSYSFUNCPURPOSECODEID,
        SYSF.FGROUPASSOCSYSFUNCID,
        SYSF.SEQNO,
        SYSF.ISADDTOACTIVATIONLOG,
        SYSF.WINDOWCLASSNAME,
        SYSF.FACTIONCODEID,
        SYSF.LOCKSTAMP,
        SYSF.AUDITDATETIME,
        SYSF.FAUDITSYSTEMFUNCTIONID,
        SYSF.FAUDITUSERCODE,
        SYSF.ISUPDATEDINTHISVERSION,
        SYSF.ISDISPLAYHISTORYRECORDS,
        SYSF.ISAFTERCMDSEPARATOR
    From
        SYSTEMFUNCTION SYSF Left Join
        X000_Codedescription PURC On PURC.KCODEDESCID = SYSF.FSYSFUNCPURPOSECODEID Left Join
        X000_Codedescription ACTC On ACTC.KCODEDESCID = SYSF.FACTIONCODEID Left Join
        SYSTEMFUNCTIONNAME NAME On NAME.KSYSTEMFUNCTIONID = SYSF.KSYSTEMFUNCTIONID Left Join
        X000_Codedescription NAPC On NAPC.KCODEDESCID = NAME.KNAMEPURPOSECODEID
    Where
        NAME.KSYSTEMLANGUAGECODE = 3 And
        Upper(NAPC.LONG) = 'GENERAL'
    Order By
        SYSF.KSYSTEMFUNCTIONID
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD STUDENT RESULTS
    funcfile.writelog("STUDENT RESULTS")
    print("Build student results...")
    sr_file = "X000_Student_qualfos_result"
    s_sql = "CREATE VIEW " + sr_file + " AS " + """
    Select
        RESU.KBUSINESSENTITYID,
        RESU.KACADEMICPROGRAMID,
        RESU.KQUALFOSRESULTCODEID,
        Upper(REAS.LONG) AS RESULT,
        RESU.KRESULTYYYYMM,
        RESU.KSTUDQUALFOSRESULTID,
        RESU.FGRADUATIONCEREMONYID,
        RESU.FPOSTPONEMENTCODEID,
        Upper(POST.LONG) AS POSTPONE_REAS,
        RESU.RESULTISSUEDATE,
        RESU.DISCONTINUEDATE,
        RESU.FDISCONTINUECODEID,
        Upper(DISC.LONG) AS DISCONTINUE_REAS,
        RESU.RESULTPASSDATE,
        RESU.FLANGUAGECODEID,
        RESU.ISSUESURNAME,
        RESU.CERTIFICATESEQNUMBER,
        RESU.AVGMARKACHIEVED,
        RESU.PROCESSSEQNUMBER,
        RESU.FRECEIPTID,
        RESU.FRECEIPTLINEID,
        RESU.ISINABSENTIA,
        RESU.FPROGRAMAPID,
        RESU.FISSUETYPECODEID,
        Upper(TYPE.LONG) AS ISSUE_TYPE,
        RESU.DATEPRINTED,
        RESU.LOCKSTAMP,
        RESU.AUDITDATETIME,
        RESU.FAUDITSYSTEMFUNCTIONID,
        RESU.FAUDITUSERCODE,
        RESU.FAPPROVEDBYCODEID,
        RESU.FAPPROVEDBYUSERCODE,
        RESU.DATERESULTAPPROVED,
        RESU.FENROLMENTPRESENTATIONID,
        RESU.CERTDISPATCHDATE,
        RESU.CERTDISPATCHREFNO,
        RESU.ISSUEFIRSTNAMES
    From
        STUDQUALFOSRESULT RESU
        LEFT JOIN X000_Codedescription REAS ON REAS.KCODEDESCID = RESU.KQUALFOSRESULTCODEID
        LEFT JOIN X000_Codedescription POST ON POST.KCODEDESCID = RESU.FPOSTPONEMENTCODEID
        LEFT JOIN X000_Codedescription DISC ON DISC.KCODEDESCID = RESU.FDISCONTINUECODEID
        LEFT JOIN X000_Codedescription TYPE ON TYPE.KCODEDESCID = RESU.FISSUETYPECODEID
    Order By
        RESU.KBUSINESSENTITYID,
        RESU.AUDITDATETIME DESC
    ;"""
    so_curs.execute("DROP VIEW IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD VIEW: " + sr_file)

    # BUILD ACADEMIC PROGRAM NAME
    print("Build academic program name 1 ...")
    sr_file = "X000ba_Academicprog_shortname"
    s_sql = "CREATE VIEW " + sr_file + " AS " + """
    Select
        PROG.KACADEMICPROGRAMNAMEID,
        PROG.FACADEMICPROGRAMID,
        PROG.FNAMEPURPOSECODEID,
        Upper(PURP.LONG) As PURPOSE,
        PROG.FSYSTEMLANGUAGECODEID,
        Upper(LANG.LONG) As LANGUAGE,
        PROG.STARTDATE,
        PROG.ENDDATE,
        PROG.SHORTDESCRIPTION,
        PROG.WFSHORTDESC,
        PROG.LOCKSTAMP,
        PROG.AUDITDATETIME,
        PROG.FAUDITSYSTEMFUNCTIONID,
        PROG.FAUDITUSERCODE
    From
        ACADEMICPROGRAMSHORTNAME PROG Left Join
        X000_Codedescription PURP On PURP.KCODEDESCID = PROG.FNAMEPURPOSECODEID Left Join
        X000_Codedescription LANG On LANG.KCODEDESCID = PROG.FSYSTEMLANGUAGECODEID
    Order By
        PROG.FACADEMICPROGRAMID,
        LANGUAGE,
        PURPOSE,
        PROG.STARTDATE
    ;"""
    so_curs.execute("DROP VIEW IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD VIEW: " + sr_file)

    # BUILD ACADEMIC PROGRAM NAME SUMMARY
    print("Build academic program name summary 2...")
    sr_file = "X000bb_Academicprog_summary"
    s_sql = "CREATE VIEW " + sr_file + " AS " + """
    Select
        PROG.FNAMEPURPOSECODEID,
        PROG.PURPOSE,
        Count(PROG.KACADEMICPROGRAMNAMEID) As PURPOSE_COUNT
    From
        X000ba_Academicprog_shortname PROG
    Where
        PROG.FSYSTEMLANGUAGECODEID = 3
    Group By
        PROG.FNAMEPURPOSECODEID
    ;"""
    so_curs.execute("DROP VIEW IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD VIEW: " + sr_file)

    # QUALIFICATION, MODULE, PROGRAM MASTER
    print("Build present enrol master...")
    sr_file = "X000aa_QMP_Master"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        PRES.KENROLMENTPRESENTATIONID,
        PRES.FQUALPRESENTINGOUID,
        PRES.FMODULEPRESENTINGOUID,
        PRES.FPROGRAMPRESENTINGOUID,
        PRES.FPROGRAMAPID,
        PRES.FENROLMENTCATEGORYCODEID,
        Upper(ENRO.LONG) As ENROL_CATEGORY,
        PRES.FPRESENTATIONCATEGORYCODEID,
        Upper(PRES.LONG) As PRESENT_CATEGORY,
        PRES.MAXNOOFSTUDENTS,
        PRES.MINNOOFSTUDENTS,
        PRES.ISVERIFICATIONREQUIRED,
        PRES.EXAMSUBMINIMUM,
        PRES.STARTDATE AS MASTER_STARTDATE,
        PRES.ENDDATE AS MASTER_ENDDATE,
        PRES.AUDITDATETIME MASTER_AUDITDATETIME,
        PRES.FAUDITSYSTEMFUNCTIONID AS MASTER_SYSID,
        PRES.FAUDITUSERCODE AS MASTER_USERCODE
    From
        PRESENTOUENROLPRESENTCAT PRES Left Join
        X000_Codedescription ENRO ON ENRO.KCODEDESCID = PRES.FENROLMENTCATEGORYCODEID Left Join 
        X000_Codedescription PRES ON PRES.KCODEDESCID = PRES.FPRESENTATIONCATEGORYCODEID 
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    """*************************************************************************
    BUILD QUALIFICATION MASTER LIST
    *************************************************************************"""
    print("BUILD QUALIFICATION MASTER LIST")
    funcfile.writelog("BUILD QUALIFICATION MASTER LIST")

    # BUILD QUALIFICATION STEP 1 - QUALIFICATION LEVEL
    print("Build qualification level (step 1)...")
    sr_file = "X001aa_Qual_level"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        QUAL.KACADEMICPROGRAMID,
        QUAL.STARTDATE,
        QUAL.ENDDATE,
        QUAL.QUALIFICATIONLEVEL,
        Upper(FINA.LONG) AS FINAL_STATUS,
        Upper(LEVY.LONG) AS LEVY_CATEGORY,
        QUAL.FFIELDOFSTUDYAPID,
        QUAL.FFINALSTATUSCODEID,
        QUAL.FLEVYCATEGORYCODEID,
        QUAL.LOCKSTAMP,
        QUAL.AUDITDATETIME,
        QUAL.FAUDITSYSTEMFUNCTIONID,
        QUAL.FAUDITUSERCODE,
        QUAL.PHASEOUTDATE
    From
        QUALIFICATIONLEVEL QUAL Left Join
        X000_Codedescription FINA ON FINA.KCODEDESCID = QUAL.FFINALSTATUSCODEID Left Join
        X000_Codedescription LEVY ON LEVY.KCODEDESCID = QUAL.FLEVYCATEGORYCODEID
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD QUALIFICATION STEP 2 - QUALIFICATION LEVEL
    print("Build qualification presentation (step 2)...")
    sr_file = "X001ab_Qual_level_present"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        MAST.KENROLMENTPRESENTATIONID,
        LEVE.QUALIFICATIONLEVEL,
        PRES.FQUALLEVELAPID,
        MAST.FENROLMENTCATEGORYCODEID,
        MAST.ENROL_CATEGORY,
        MAST.FPRESENTATIONCATEGORYCODEID,
        MAST.PRESENT_CATEGORY,
        LEVE.FINAL_STATUS,
        LEVE.LEVY_CATEGORY,
        LEVE.FFIELDOFSTUDYAPID,
        MAST.FMODULEPRESENTINGOUID,
        MODU.KMODPERIODENROLPRESID,
        MAST.FPROGRAMAPID,
        PRES.FBUSINESSENTITYID,
        ORGA.FSITEORGUNITNUMBER As SITEID,
        Upper(SITE.LONG) As CAMPUS,
        Upper(ORGA.ORGUNIT_TYPE) As ORGUNIT_TYPE,
        Upper(ORGA.ORGUNIT_NAME) As ORGUNIT_NAME,
        Upper(ORGA.MANAGER_TYPE) As ORGUNIT_MANAGER,
        MAST.MAXNOOFSTUDENTS,
        MAST.MINNOOFSTUDENTS,
        PRES.NUMBEROFSTUDENTS,
        MAST.ISVERIFICATIONREQUIRED,
        MAST.EXAMSUBMINIMUM,
        MAST.FQUALPRESENTINGOUID,
        MAST.MASTER_STARTDATE,
        MAST.MASTER_ENDDATE,
        MAST.MASTER_AUDITDATETIME,
        MAST.MASTER_SYSID,
        MAST.MASTER_USERCODE,
        PRES.KPRESENTINGOUID,
        PRES.STARTDATE As PRESENT_STARTDATE,
        PRES.ENDDATE As PRESENT_ENDDATE,
        PRES.AUDITDATETIME As PRESENT_AUDITDATETIME,
        PRES.FAUDITSYSTEMFUNCTIONID AS PRESENT_SYSID,
        PRES.FAUDITUSERCODE As PRESENT_USERCODE,
        LEVE.KACADEMICPROGRAMID As LEVEL_KACADEMICPROGRAMID,
        LEVE.STARTDATE As LEVEL_STARTDATE,
        LEVE.ENDDATE As LEVEL_ENDDATE,
        LEVE.PHASEOUTDATE As LEVEL_PHASEOUTDATE,    
        LEVE.AUDITDATETIME As LEVEL_AUDITDATETIME,
        LEVE.FAUDITSYSTEMFUNCTIONID As LEVEL_SYSID,
        LEVE.FAUDITUSERCODE As LEVEL_USERCODE
    From
        X000aa_QMP_Master MAST Inner Join
        QUALLEVELPRESENTINGOU PRES On PRES.KPRESENTINGOUID = MAST.FQUALPRESENTINGOUID Left Join
        X000_Orgunitinstance ORGA On ORGA.KBUSINESSENTITYID = PRES.FBUSINESSENTITYID Left Join
        X000_Orgunit SITE On SITE.KORGUNITNUMBER = ORGA.FSITEORGUNITNUMBER Left Join
        X001aa_Qual_level LEVE On LEVE.KACADEMICPROGRAMID = PRES.FQUALLEVELAPID Left Join
        MODPERIODPRESOUENROLPRESCAT MODU On MODU.KMODPRESENTOUENROLPRESID = MAST.FMODULEPRESENTINGOUID  
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD QUALIFICATION STEP 3 - FIELD OF STUDY
    print("Build qualification field of study (step 3)...")
    sr_file = "X001ac_Qual_fieldofstudy"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        QUAL.KENROLMENTPRESENTATIONID,
        FOFS.QUALIFICATIONFIELDOFSTUDY,
        QUAL.QUALIFICATIONLEVEL,
        QUAL.FQUALLEVELAPID,
        QUAL.FENROLMENTCATEGORYCODEID,
        QUAL.ENROL_CATEGORY,
        QUAL.FPRESENTATIONCATEGORYCODEID,
        QUAL.PRESENT_CATEGORY,
        QUAL.FINAL_STATUS,
        QUAL.LEVY_CATEGORY,
        Upper(SELE.LONG) As FOS_SELECTION,
        QUAL.FFIELDOFSTUDYAPID,
        FOFS.FQUALIFICATIONAPID,
        QUAL.FPROGRAMAPID,
        QUAL.FBUSINESSENTITYID,
        QUAL.SITEID,
        QUAL.CAMPUS,
        QUAL.ORGUNIT_TYPE,
        QUAL.ORGUNIT_NAME,
        QUAL.ORGUNIT_MANAGER,
        QUAL.MAXNOOFSTUDENTS,
        QUAL.MINNOOFSTUDENTS,
        QUAL.NUMBEROFSTUDENTS,
        QUAL.ISVERIFICATIONREQUIRED,
        QUAL.EXAMSUBMINIMUM,
        QUAL.FQUALPRESENTINGOUID,
        QUAL.MASTER_STARTDATE,
        QUAL.MASTER_ENDDATE,
        QUAL.MASTER_AUDITDATETIME,
        QUAL.MASTER_SYSID,
        QUAL.MASTER_USERCODE,
        QUAL.KPRESENTINGOUID,
        QUAL.PRESENT_STARTDATE,
        QUAL.PRESENT_ENDDATE,
        QUAL.PRESENT_AUDITDATETIME,
        QUAL.PRESENT_SYSID,
        QUAL.PRESENT_USERCODE,
        QUAL.LEVEL_KACADEMICPROGRAMID,
        QUAL.LEVEL_STARTDATE,
        QUAL.LEVEL_ENDDATE,
        QUAL.LEVEL_PHASEOUTDATE,
        QUAL.LEVEL_AUDITDATETIME,
        QUAL.LEVEL_SYSID,
        QUAL.LEVEL_USERCODE,
        FOFS.KACADEMICPROGRAMID As FOS_KACADEMICPROGRAMID,
        FOFS.STARTDATE As FOS_STARTDATE,
        FOFS.ENDDATE As FOS_ENDDATE,
        FOFS.AUDITDATETIME As FOS_AUDITDATETIME,
        FOFS.FAUDITSYSTEMFUNCTIONID As FOS_SYSID,
        FOFS.FAUDITUSERCODE As FOS_USERCODE
    From
        X001ab_Qual_level_present QUAL Left Join
        FIELDOFSTUDY FOFS On FOFS.KACADEMICPROGRAMID = QUAL.FFIELDOFSTUDYAPID Left Join
        X000_Codedescription SELE On SELE.KCODEDESCID = FOFS.FSELECTIONCODEID
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD QUALIFICATION STEP 4 - QUALIFICATION
    print("Build qualification (step 4)...")
    sr_file = "X001ad_Qual"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        QUAL.KACADEMICPROGRAMID,
        QUAL.STARTDATE,
        QUAL.ENDDATE,
        QUAL.QUALIFICATIONCODE,
        Upper(QUALTYPE.LONG) AS QUALIFICATION_TYPE,
        Cast(MINDUR.LANK As REAL) AS MIN,
        Upper(MINUNI.LONG) AS MIN_UNIT,
        Cast(MAXDUR.LONG As REAL) AS MAX,
        Upper(MAXUNI.LONG) AS MAX_UNIT,
        Upper(CERTTYPE.LONG) AS CERT_TYPE,
        Upper(LEVY.LONG) AS LEVY_TYPE,
        QUAL.ISVATAPPLICABLE,
        QUAL.ISPRESENTEDBEFOREAPPROVAL,
        QUAL.ISDIRECTED,
        QUAL.AUDITDATETIME,
        QUAL.FAUDITSYSTEMFUNCTIONID,
        QUAL.FAUDITUSERCODE
    From
        QUALIFICATION QUAL Left Join
        X000_Codedescription MINDUR ON MINDUR.KCODEDESCID = QUAL.FMINDURATIONCODEID Left Join
        X000_Codedescription MINUNI ON MINUNI.KCODEDESCID = QUAL.FMINDURPERIODUNITCODEID Left Join
        X000_Codedescription MAXDUR ON MAXDUR.KCODEDESCID = QUAL.FMAXDURATIONCODEID Left Join
        X000_Codedescription MAXUNI ON MAXUNI.KCODEDESCID = QUAL.FMAXDURPERIODUNITCODEID Left Join
        X000_Codedescription QUALTYPE ON QUALTYPE.KCODEDESCID = QUAL.FQUALIFICATIONTYPECODEID Left Join
        X000_Codedescription CERTTYPE ON CERTTYPE.KCODEDESCID = QUAL.FCERTIFICATETYPECODEID Left Join
        X000_Codedescription LEVY ON LEVY.KCODEDESCID = QUAL.FLEVYLEVELCODEID
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD QUALIFICATION STEP 5 - ADD QUALIFICATION
    print("Build qualification final (step 5)...")
    sr_file = "X001ae_Qual_final"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        QUAL.KENROLMENTPRESENTATIONID,
        Upper(NAME.SHORTDESCRIPTION) As QUALIFICATION_NAME,
        Trim(QAUD.QUALIFICATIONCODE)||' '||
            Trim(QUAL.QUALIFICATIONFIELDOFSTUDY)||' '||
            Trim(QUAL.QUALIFICATIONLEVEL) As QUALIFICATION,
        QAUD.QUALIFICATIONCODE,
        QUAL.QUALIFICATIONFIELDOFSTUDY,
        QUAL.QUALIFICATIONLEVEL,
        QAUD.QUALIFICATION_TYPE,
        QUAL.FQUALLEVELAPID,
        QUAL.FENROLMENTCATEGORYCODEID,
        QUAL.ENROL_CATEGORY,
        QUAL.FPRESENTATIONCATEGORYCODEID,
        QUAL.PRESENT_CATEGORY,
        QUAL.FINAL_STATUS,
        QUAL.LEVY_CATEGORY,
        QAUD.CERT_TYPE,
        QAUD.LEVY_TYPE,
        QUAL.FOS_SELECTION,
        QUAL.FPROGRAMAPID,
        QUAL.FBUSINESSENTITYID,
        QUAL.SITEID,
        QUAL.CAMPUS,
        QUAL.ORGUNIT_TYPE,
        QUAL.ORGUNIT_NAME,
        QUAL.ORGUNIT_MANAGER,
        QAUD.MIN,
        QAUD.MIN_UNIT,
        QAUD.MAX,
        QAUD.MAX_UNIT,
        QUAL.MAXNOOFSTUDENTS,
        QUAL.MINNOOFSTUDENTS,
        QUAL.NUMBEROFSTUDENTS,
        QUAL.ISVERIFICATIONREQUIRED,
        QUAL.EXAMSUBMINIMUM,
        QAUD.ISVATAPPLICABLE,
        QAUD.ISPRESENTEDBEFOREAPPROVAL,
        QAUD.ISDIRECTED,
        QUAL.FQUALPRESENTINGOUID,
        QUAL.MASTER_STARTDATE,
        QUAL.MASTER_ENDDATE,
        QUAL.MASTER_AUDITDATETIME,
        QUAL.MASTER_SYSID,
        QUAL.MASTER_USERCODE,
        QUAL.KPRESENTINGOUID,
        QUAL.PRESENT_STARTDATE,
        QUAL.PRESENT_ENDDATE,
        QUAL.PRESENT_AUDITDATETIME,
        QUAL.PRESENT_SYSID,
        QUAL.PRESENT_USERCODE,
        QUAL.LEVEL_KACADEMICPROGRAMID,
        QUAL.LEVEL_STARTDATE,
        QUAL.LEVEL_ENDDATE,
        QUAL.LEVEL_PHASEOUTDATE,
        QUAL.LEVEL_AUDITDATETIME,
        QUAL.LEVEL_SYSID,
        QUAL.LEVEL_USERCODE,
        QUAL.FOS_KACADEMICPROGRAMID,
        QUAL.FOS_STARTDATE,
        QUAL.FOS_ENDDATE,
        QUAL.FOS_AUDITDATETIME,
        QUAL.FOS_SYSID,
        QUAL.FOS_USERCODE,
        QAUD.KACADEMICPROGRAMID As QUAL_KACADEMICPROGRAMID,
        QAUD.STARTDATE As QUAL_STARTDATE,
        QAUD.ENDDATE As QUAL_ENDDATE,
        QAUD.AUDITDATETIME As QUAL_AUDITDATETIME,
        QAUD.FAUDITSYSTEMFUNCTIONID As QUAL_SYSID,
        QAUD.FAUDITUSERCODE As QUAL_USERCODE
    From
        X001ac_Qual_fieldofstudy QUAL Left Join
        X001ad_Qual QAUD On QAUD.KACADEMICPROGRAMID = QUAL.FQUALIFICATIONAPID Left Join
        ACADEMICPROGRAMSHORTNAME NAME On NAME.FACADEMICPROGRAMID = QUAL.LEVEL_KACADEMICPROGRAMID And
            NAME.STARTDATE <= QUAL.LEVEL_STARTDATE And
            NAME.ENDDATE >= QUAL.LEVEL_ENDDATE And
            NAME.FSYSTEMLANGUAGECODEID = 3 And
            NAME.FNAMEPURPOSECODEID = 7294
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD QUALIFICATION FINAL
    print("Build qualification final...")
    sr_file = "X000_Qualifications"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        QUAL.KENROLMENTPRESENTATIONID,
        QUAL.QUALIFICATION,
        QUAL.QUALIFICATION_NAME,
        QUAL.QUALIFICATION_TYPE,
        QUAL.FQUALLEVELAPID,
        QUAL.FENROLMENTCATEGORYCODEID,
        QUAL.ENROL_CATEGORY,
        QUAL.FPRESENTATIONCATEGORYCODEID,
        QUAL.PRESENT_CATEGORY,
        QUAL.FINAL_STATUS,
        QUAL.LEVY_CATEGORY,
        QUAL.CERT_TYPE,
        QUAL.LEVY_TYPE,
        QUAL.FOS_SELECTION,
        QUAL.FPROGRAMAPID,
        QUAL.FBUSINESSENTITYID,
        QUAL.SITEID,
        QUAL.CAMPUS,
        QUAL.ORGUNIT_TYPE,
        QUAL.ORGUNIT_NAME,
        QUAL.ORGUNIT_MANAGER,
        QUAL.QUALIFICATIONCODE,
        QUAL.QUALIFICATIONFIELDOFSTUDY,
        QUAL.QUALIFICATIONLEVEL,
        QUAL.MIN,
        QUAL.MIN_UNIT,
        QUAL.MAX,
        QUAL.MAX_UNIT,
        QUAL.MAXNOOFSTUDENTS,
        QUAL.MINNOOFSTUDENTS,
        QUAL.NUMBEROFSTUDENTS,
        QUAL.ISVERIFICATIONREQUIRED,
        QUAL.EXAMSUBMINIMUM,
        QUAL.ISVATAPPLICABLE,
        QUAL.ISPRESENTEDBEFOREAPPROVAL,
        QUAL.ISDIRECTED,
        QUAL.FQUALPRESENTINGOUID,
        QUAL.MASTER_STARTDATE,
        QUAL.MASTER_ENDDATE,
        QUAL.MASTER_AUDITDATETIME,
        QUAL.MASTER_SYSID,
        QUAL.MASTER_USERCODE,
        QUAL.KPRESENTINGOUID,
        QUAL.PRESENT_STARTDATE,
        QUAL.PRESENT_ENDDATE,
        QUAL.PRESENT_AUDITDATETIME,
        QUAL.PRESENT_SYSID,
        QUAL.PRESENT_USERCODE,
        QUAL.LEVEL_KACADEMICPROGRAMID,
        QUAL.LEVEL_STARTDATE,
        QUAL.LEVEL_ENDDATE,
        QUAL.LEVEL_PHASEOUTDATE,
        QUAL.LEVEL_AUDITDATETIME,
        QUAL.LEVEL_SYSID,
        QUAL.LEVEL_USERCODE,
        QUAL.FOS_KACADEMICPROGRAMID,
        QUAL.FOS_STARTDATE,
        QUAL.FOS_ENDDATE,
        QUAL.FOS_AUDITDATETIME,
        QUAL.FOS_SYSID,
        QUAL.FOS_USERCODE,
        QUAL.QUAL_KACADEMICPROGRAMID,
        QUAL.QUAL_STARTDATE,
        QUAL.QUAL_ENDDATE,
        QUAL.QUAL_AUDITDATETIME,
        QUAL.QUAL_SYSID,
        QUAL.QUAL_USERCODE
    From
        X001ae_Qual_final QUAL
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # VACUUM TEMP DEVELOPMENT FILES
    sr_file = "X001aa_Qual_level"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    sr_file = "X001ab_Qual_level_present"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    sr_file = "X001ac_Qual_fieldofstudy"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    sr_file = "X001ad_Qual"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    sr_file = "X001ae_Qual_final"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_conn.commit()

    """*************************************************************************
    BUILD MODULES
    *************************************************************************"""
    print("BUILD MODULES")
    funcfile.writelog("BUILD MODULES")

    # BUILD MODULES STEP 1 - MODULE PRESENT
    print("Build module present (step 1)...")
    sr_file = "X002aa_Modu_present"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        MAST.KENROLMENTPRESENTATIONID,
        PRES.FMODULEAPID,
        MAST.FENROLMENTCATEGORYCODEID,
        MAST.ENROL_CATEGORY,
        MAST.FPRESENTATIONCATEGORYCODEID,
        MAST.PRESENT_CATEGORY,
        PRES.FCOURSEGROUPCODEID,
        Upper(GROU.LONG) As COURSEGROUP,
        PRES.FBUSINESSENTITYID,
        ORGA.FSITEORGUNITNUMBER As SITEID,
        Upper(SITE.LONG) As CAMPUS,
        Upper(ORGA.ORGUNIT_TYPE) As ORGUNIT_TYPE,
        Upper(ORGA.ORGUNIT_NAME) As ORGUNIT_NAME,
        Upper(ORGA.MANAGER_TYPE) As ORGUNIT_MANAGER,    
        MAST.EXAMSUBMINIMUM,
        PRES.ISEXAMMODULE,
        MAST.MASTER_STARTDATE,
        MAST.MASTER_ENDDATE,
        MAST.MASTER_AUDITDATETIME,
        MAST.MASTER_SYSID,
        MAST.MASTER_USERCODE,
        PRES.KPRESENTINGOUID As PRESENT_KPRESENTINGOUID,
        PRES.STARTDATE As PRESENT_STARTDATE,
        PRES.ENDDATE As PRESENT_ENDDATE,
        PRES.AUDITDATETIME As PRESENT_AUDITDATETIME,
        PRES.FAUDITSYSTEMFUNCTIONID As PRESENT_SYSID,
        PRES.FAUDITUSERCODE As PRESENT_USERCODE
    From
        X000aa_QMP_Master MAST Inner Join
        MODULEPRESENTINGOU PRES On PRES.KPRESENTINGOUID = MAST.FMODULEPRESENTINGOUID Left Join
        X000_Codedescription GROU On GROU.KCODEDESCID = PRES.FCOURSEGROUPCODEID Left Join
        X000_Orgunitinstance ORGA On ORGA.KBUSINESSENTITYID = PRES.FBUSINESSENTITYID Left Join
        X000_Orgunit SITE On SITE.KORGUNITNUMBER = ORGA.FSITEORGUNITNUMBER    
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD MODULES STEP 2 - MODULE
    print("Build module (step 2)...")
    sr_file = "X002ab_Modu_module"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        PRES.KENROLMENTPRESENTATIONID,
        MODU.COURSEMODULE,
        MODU.FCOURSELEVELAPID,
        PRES.FENROLMENTCATEGORYCODEID,
        PRES.ENROL_CATEGORY,
        PRES.FPRESENTATIONCATEGORYCODEID,
        PRES.PRESENT_CATEGORY,
        PRES.FCOURSEGROUPCODEID,
        PRES.COURSEGROUP,
        PRES.FBUSINESSENTITYID,
        PRES.SITEID,
        PRES.CAMPUS,
        PRES.ORGUNIT_TYPE,
        PRES.ORGUNIT_NAME,
        PRES.ORGUNIT_MANAGER,
        PRES.EXAMSUBMINIMUM,
        PRES.ISEXAMMODULE,
        MODU.ISRESEARCHMODULE,
        PRES.MASTER_STARTDATE,
        PRES.MASTER_ENDDATE,
        PRES.MASTER_AUDITDATETIME,
        PRES.MASTER_SYSID,
        PRES.MASTER_USERCODE,
        PRES.PRESENT_KPRESENTINGOUID,
        PRES.PRESENT_STARTDATE,
        PRES.PRESENT_ENDDATE,
        PRES.PRESENT_AUDITDATETIME,
        PRES.PRESENT_SYSID,
        PRES.PRESENT_USERCODE,
        PRES.FMODULEAPID,
        MODU.KACADEMICPROGRAMID As MODU_KACADEMICPROGRAMID,
        MODU.STARTDATE As MODU_STARTDATE,
        MODU.ENDDATE As MODU_ENDDATE,
        MODU.AUDITDATETIME,
        MODU.FAUDITSYSTEMFUNCTIONID As MODU_AUDITDATETIME,
        MODU.FAUDITUSERCODE As MODU_USERCODE
    From
        X002aa_Modu_present PRES Inner Join
        MODULE MODU On MODU.KACADEMICPROGRAMID = PRES.FMODULEAPID
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD MODULES STEP 3 - COURSE LEVEL
    print("Build course level (step 3)...")
    sr_file = "X002ac_Modu_courselevel"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        MODU.KENROLMENTPRESENTATIONID,
        MODU.COURSEMODULE,
        COUL.COURSELEVEL,
        COUL.FCOURSEAPID,
        MODU.FENROLMENTCATEGORYCODEID,
        MODU.ENROL_CATEGORY,
        MODU.FPRESENTATIONCATEGORYCODEID,
        MODU.PRESENT_CATEGORY,
        MODU.FCOURSEGROUPCODEID,
        MODU.COURSEGROUP,
        MODU.FBUSINESSENTITYID,
        MODU.SITEID,
        MODU.CAMPUS,
        MODU.ORGUNIT_TYPE,
        MODU.ORGUNIT_NAME,
        MODU.ORGUNIT_MANAGER,
        MODU.EXAMSUBMINIMUM,
        MODU.ISEXAMMODULE,
        MODU.ISRESEARCHMODULE,
        MODU.MASTER_STARTDATE,
        MODU.MASTER_ENDDATE,
        MODU.MASTER_AUDITDATETIME,
        MODU.MASTER_SYSID,
        MODU.MASTER_USERCODE,
        MODU.PRESENT_KPRESENTINGOUID,
        MODU.PRESENT_STARTDATE,
        MODU.PRESENT_ENDDATE,
        MODU.PRESENT_AUDITDATETIME,
        MODU.PRESENT_SYSID,
        MODU.PRESENT_USERCODE,
        MODU.FMODULEAPID,
        MODU.MODU_KACADEMICPROGRAMID,
        MODU.MODU_STARTDATE,
        MODU.MODU_ENDDATE,
        MODU.AUDITDATETIME,
        MODU.MODU_AUDITDATETIME,
        MODU.MODU_USERCODE,
        MODU.FCOURSELEVELAPID,
        COUL.KACADEMICPROGRAMID As COUL_KACADEMICPROGRAMID,
        COUL.AUDITDATETIME As COUL_AUDITDATETIME,
        COUL.FAUDITSYSTEMFUNCTIONID As COUL_SYSID,
        COUL.FAUDITUSERCODE As COUL_USERCODE
    From
        X002ab_Modu_module MODU Left Join
        COURSELEVEL COUL On COUL.KACADEMICPROGRAMID = MODU.FCOURSELEVELAPID
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD MODULES STEP 4 - COURSE
    print("Build course (step 4)...")
    sr_file = "X002ad_Modu_course"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        COUL.KENROLMENTPRESENTATIONID,
        Upper(NAME.SHORTDESCRIPTION) As MODULE_NAME,
        COUR.COURSECODE,
        COUL.COURSELEVEL,
        COUL.COURSEMODULE,
        COUL.FENROLMENTCATEGORYCODEID,
        COUL.ENROL_CATEGORY,
        COUL.FPRESENTATIONCATEGORYCODEID,
        COUL.PRESENT_CATEGORY,
        COUL.FCOURSEGROUPCODEID,
        COUL.COURSEGROUP,
        COUL.FBUSINESSENTITYID,
        COUL.SITEID,
        COUL.CAMPUS,
        COUL.ORGUNIT_TYPE,
        COUL.ORGUNIT_NAME,
        COUL.ORGUNIT_MANAGER,
        COUL.EXAMSUBMINIMUM,
        COUL.ISEXAMMODULE,
        COUL.ISRESEARCHMODULE,
        COUL.MASTER_STARTDATE,
        COUL.MASTER_ENDDATE,
        COUL.MASTER_AUDITDATETIME,
        COUL.MASTER_SYSID,
        COUL.MASTER_USERCODE,
        COUL.PRESENT_KPRESENTINGOUID,
        COUL.PRESENT_STARTDATE,
        COUL.PRESENT_ENDDATE,
        COUL.PRESENT_AUDITDATETIME,
        COUL.PRESENT_SYSID,
        COUL.PRESENT_USERCODE,
        COUL.FMODULEAPID,
        COUL.MODU_KACADEMICPROGRAMID,
        COUL.MODU_STARTDATE,
        COUL.MODU_ENDDATE,
        COUL.AUDITDATETIME,
        COUL.MODU_AUDITDATETIME,
        COUL.MODU_USERCODE,
        COUL.FCOURSELEVELAPID,
        COUL.COUL_KACADEMICPROGRAMID,
        COUL.COUL_AUDITDATETIME,
        COUL.COUL_SYSID,
        COUL.COUL_USERCODE,
        COUL.FCOURSEAPID,
        COUR.KACADEMICPROGRAMID As COUR_KACADEMICPROGRAMID,
        COUR.AUDITDATETIME As COUR_AUDITDATETIME,
        COUR.FAUDITSYSTEMFUNCTIONID As COUR_SYSID,
        COUR.FAUDITUSERCODE As COUR_USERCODE
    From
        X002ac_Modu_courselevel COUL Left Join
        COURSE COUR On COUR.KACADEMICPROGRAMID = COUL.FCOURSEAPID Left Join
        ACADEMICPROGRAMSHORTNAME NAME On NAME.FACADEMICPROGRAMID = MODU_KACADEMICPROGRAMID And
            NAME.STARTDATE <= COUL.MODU_STARTDATE And
            NAME.ENDDATE >= COUL.MODU_ENDDATE And
            NAME.FSYSTEMLANGUAGECODEID = 3 And
            NAME.FNAMEPURPOSECODEID = 7294
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD MODULES STEP 5 - FINAL
    print("Build modules (step 5)...")
    sr_file = "X000_Modules"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        MODU.KENROLMENTPRESENTATIONID,
        MODU.MODULE_NAME,
        MODU.COURSECODE||' '||MODU.COURSELEVEL||' '||MODU.COURSEMODULE As MODULE,
        MODU.COURSECODE,
        MODU.COURSELEVEL,
        MODU.COURSEMODULE,
        Substr(MODU.COURSEMODULE,1,1) As COURSESEMESTER,
        MODU.FENROLMENTCATEGORYCODEID,
        MODU.ENROL_CATEGORY,
        MODU.FPRESENTATIONCATEGORYCODEID,
        MODU.PRESENT_CATEGORY,
        MODU.FCOURSEGROUPCODEID,
        MODU.COURSEGROUP,
        MODU.FBUSINESSENTITYID,
        MODU.SITEID,
        MODU.CAMPUS,
        MODU.ORGUNIT_TYPE,
        MODU.ORGUNIT_NAME,
        MODU.ORGUNIT_MANAGER,
        MODU.EXAMSUBMINIMUM,
        MODU.ISEXAMMODULE,
        MODU.ISRESEARCHMODULE,
        MODU.MASTER_STARTDATE,
        MODU.MASTER_ENDDATE,
        MODU.MASTER_AUDITDATETIME,
        MODU.MASTER_SYSID,
        MODU.MASTER_USERCODE,
        MODU.PRESENT_KPRESENTINGOUID,
        MODU.PRESENT_STARTDATE,
        MODU.PRESENT_ENDDATE,
        MODU.PRESENT_AUDITDATETIME,
        MODU.PRESENT_SYSID,
        MODU.PRESENT_USERCODE,
        MODU.FMODULEAPID,
        MODU.MODU_KACADEMICPROGRAMID,
        MODU.MODU_STARTDATE,
        MODU.MODU_ENDDATE,
        MODU.AUDITDATETIME,
        MODU.MODU_AUDITDATETIME,
        MODU.MODU_USERCODE,
        MODU.FCOURSELEVELAPID,
        MODU.COUL_KACADEMICPROGRAMID,
        MODU.COUL_AUDITDATETIME,
        MODU.COUL_SYSID,
        MODU.COUL_USERCODE,
        MODU.FCOURSEAPID,
        MODU.COUR_KACADEMICPROGRAMID,
        MODU.COUR_AUDITDATETIME,
        MODU.COUR_SYSID,
        MODU.COUR_USERCODE
    From
        X002ad_Modu_course MODU
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # VACUUM TEMP DEVELOPMENT FILES
    sr_file = "X002aa_Modu_present"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    sr_file = "X002ab_Modu_module"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    sr_file = "X002ac_Modu_courselevel"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    sr_file = "X002ad_Modu_course"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)

    """*************************************************************************
    BUILD PROGRAMS
    *************************************************************************"""
    print("BUILD PROGRAMS")
    funcfile.writelog("BUILD PROGRAMS")

    """*************************************************************************
    BUILD BURSARIES
    *************************************************************************"""
    print("BUILD BURSARIES")
    funcfile.writelog("BUILD BURSARIES")

    funcfile.writelog("STUDENT BURSARIES")

    print("Build bursaries...")

    s_sql = "CREATE VIEW X004aa_Bursaries AS " + """
    SELECT
      FINAID.KFINAIDID,
      FINAID.FFINAIDINSTBUSENTID,
      FINAID.FINAIDCODE,
      FINAIDNAME.FINAIDNAME,
      FINAIDNAAM.FINAIDNAME AS FINAIDNAAM,
      FINAID.FTYPECODEID,
      X000_Codedescription.LONG AS TYPE_E,
      X000_Codedescription.LANK AS TYPE_A,
      FINAID.FFINAIDCATCODEID,
      X000_CODEDESC_FINAIDCATE.LONG AS BURS_CATE_E,
      X000_CODEDESC_FINAIDCATE.LANK AS BURS_CATE_A,
      FINAID.ISAUTOAPPL,
      FINAID.ISWWWAPPLALLOWED,
      FINAID.FINAIDYEARS,
      FINAID.STARTDATE,
      FINAID.ENDDATE,
      FINAID.FFUNDTYPECODEID,
      X000_Codedesc_fundtype.LONG AS FUND_TYPE_E,
      X000_Codedesc_fundtype.LANK AS FUND_TYPE_A,
      FINAID.FSTUDYTYPECODEID,
      X000_Codedesc_studytype.LONG AS STUDY_TYPE_E,
      X000_Codedesc_studytype.LANK AS STUDY_TYPE_A,
      FINAID.FPROCESSID,
      FINAID.AUDITDATETIME,
      FINAID.FAUDITUSERCODE,
      FINAID.FAUDITSYSTEMFUNCTIONID
    FROM
      FINAID
      LEFT JOIN X000_Codedescription X000_CODEDESC_FINAIDCATE ON X000_CODEDESC_FINAIDCATE.KCODEDESCID =
        FINAID.FFINAIDCATCODEID
      LEFT JOIN FINAIDNAME ON FINAIDNAME.FFINAIDID = FINAID.KFINAIDID AND FINAIDNAME.KSYSTEMLANGUAGECODEID = '3'
      LEFT JOIN FINAIDNAME FINAIDNAAM ON FINAIDNAAM.FFINAIDID = FINAID.KFINAIDID AND FINAIDNAAM.KSYSTEMLANGUAGECODEID = '2'
      LEFT JOIN X000_Codedescription ON X000_Codedescription.KCODEDESCID = FINAID.FTYPECODEID
      LEFT JOIN X000_Codedescription X000_Codedesc_fundtype ON X000_Codedesc_fundtype.KCODEDESCID = FINAID.FFUNDTYPECODEID
      LEFT JOIN X000_Codedescription X000_Codedesc_studytype ON X000_Codedesc_studytype.KCODEDESCID =
        FINAID.FSTUDYTYPECODEID
    """
    so_curs.execute("DROP VIEW IF EXISTS X004aa_Bursaries")
    so_curs.execute(s_sql)
    so_conn.commit()

    funcfile.writelog("%t BUILD VIEW: X004aa_Bursaries")

    # Build bursary site *******************************************************

    print("Build bursary sites...")

    s_sql = "CREATE VIEW X004ab_Bursary_site AS " + """
    SELECT
      FINAIDSITE.KFINAIDSITEID,
      FINAIDSITE.FFINAIDID,
      FINAIDSITE.FSITEORGUNITNUMBER,
      X004aa_Bursaries.FFINAIDINSTBUSENTID,
      X004aa_Bursaries.FINAIDCODE,
      X004aa_Bursaries.FINAIDNAME,
      X004aa_Bursaries.FINAIDNAAM,
      X004aa_Bursaries.FTYPECODEID,
      X004aa_Bursaries.TYPE_E,
      X004aa_Bursaries.TYPE_A,
      X004aa_Bursaries.FFINAIDCATCODEID,
      X004aa_Bursaries.BURS_CATE_E,
      X004aa_Bursaries.BURS_CATE_A,
      X004aa_Bursaries.ISAUTOAPPL,
      X004aa_Bursaries.ISWWWAPPLALLOWED,
      X004aa_Bursaries.FINAIDYEARS,
      X004aa_Bursaries.FFUNDTYPECODEID,
      X004aa_Bursaries.FUND_TYPE_E,
      X004aa_Bursaries.FUND_TYPE_A,
      X004aa_Bursaries.FSTUDYTYPECODEID,
      X004aa_Bursaries.STUDY_TYPE_E,
      X004aa_Bursaries.STUDY_TYPE_A,
      FINAIDSITE.CC,
      FINAIDSITE.ACC,
      FINAIDSITE.LOANTYPECODE,
      FINAIDSITE.STARTDATE,
      FINAIDSITE.ENDDATE,
      FINAIDSITE.FCOAID,
      FINAIDSITE.AUDITDATETIME,
      FINAIDSITE.FAUDITSYSTEMFUNCTIONID,
      FINAIDSITE.FAUDITUSERCODE
    FROM
      FINAIDSITE
      LEFT JOIN X004aa_Bursaries ON X004aa_Bursaries.KFINAIDID = FINAIDSITE.FFINAIDID"""
    so_curs.execute("DROP VIEW IF EXISTS X004ab_Bursary_site")
    so_curs.execute(s_sql)
    so_conn.commit()

    funcfile.writelog("%t BUILD VIEW: X004ab_Bursary_site")

    # Build bursary master table ***********************************************

    print("Build bursary master table...")

    s_sql = "CREATE TABLE X004_Bursaries AS " + """
    SELECT
      b.KFINAIDSITEID,
      b.FFINAIDID,
      b.FSITEORGUNITNUMBER,
      b.FFINAIDINSTBUSENTID,
      b.FINAIDCODE,
      Case
        When Trim(b.FINAIDNAME) = '' and Trim(b.FINAIDNAAM) <> '' Then b.FINAIDNAAM
        Else b.FINAIDNAME
      End As FINAIDNAME,
      Case
        When Trim(b.FINAIDNAAM) = '' and Trim(b.FINAIDNAME) <> '' Then b.FINAIDNAME
        Else b.FINAIDNAAM
      End As FINAIDNAAM,
      Upper(b.TYPE_E) As TYPE_E,
      Upper(b.BURS_CATE_E) As BURS_CATE_E,
      b.ISAUTOAPPL,
      b.ISWWWAPPLALLOWED,
      b.FINAIDYEARS,
      Upper(b.FUND_TYPE_E) As FUND_TYPE_E,
      Upper(b.STUDY_TYPE_E) As STUDY_TYPE_E,
      b.CC,
      b.ACC,
      b.LOANTYPECODE,
      b.STARTDATE,
      b.ENDDATE,
      b.FCOAID
    FROM
      X004ab_Bursary_site b
    ORDER BY
      b.KFINAIDSITEID
    """
    so_curs.execute("DROP TABLE IF EXISTS X000_Bursaries")
    so_curs.execute("DROP TABLE IF EXISTS X004_Bursaries")    
    so_curs.execute(s_sql)
    so_conn.commit()

    funcfile.writelog("%t BUILD TABLE: X004_Bursaries")

    """ PARTY STUDENT BIO INFORMATION ******************************************
    *** Build party external references like ID and PASSPORT numbers
    *** Extract ID number list for today
    *** Build the student bio PARTY file
    *************************************************************************"""

    funcfile.writelog("STUDENT BIO INFORMATION")

    # Build student party external reference file **********************************
    print("Build student party external ref file...")
    sr_file = "X005aa_Party_extref"
    s_sql = "CREATE VIEW "+sr_file+" AS " + """
    SELECT
      PARTYEXTERNALREFERENCE.KBUSINESSENTITYID,
      PARTYEXTERNALREFERENCE.KEXTERNALREFERENCECODEID,
      X000_Codedesc_partyextref.LONG,
      X000_Codedesc_partyextref.LANK,
      PARTYEXTERNALREFERENCE.EXTERNALREFERENCENUMBER,
      PARTYEXTERNALREFERENCE.STARTDATE,
      PARTYEXTERNALREFERENCE.ENDDATE,
      PARTYEXTERNALREFERENCE.REFERENCECODE,
      PARTYEXTERNALREFERENCE.OTHERDESCRIPTION,
      PARTYEXTERNALREFERENCE.LOCKSTAMP,
      PARTYEXTERNALREFERENCE.AUDITDATETIME,
      PARTYEXTERNALREFERENCE.FAUDITSYSTEMFUNCTIONID,
      PARTYEXTERNALREFERENCE.FAUDITUSERCODE
    FROM
      PARTYEXTERNALREFERENCE
      LEFT JOIN X000_Codedescription X000_Codedesc_partyextref ON X000_Codedesc_partyextref.KCODEDESCID =
        PARTYEXTERNALREFERENCE.KEXTERNALREFERENCECODEID
    ;"""
    so_curs.execute("DROP VIEW IF EXISTS "+sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD VIEW: "+sr_file)

    # BUILD CURRENT STUDENT ID NUMBER TABLE
    print("Build current student party id numbers...")
    sr_file = "X005ab_Party_idno_curr"
    s_sql = "CREATE VIEW "+sr_file+" AS " + """
    SELECT DISTINCT
      X005aa_Party_extref.KBUSINESSENTITYID,
      X005aa_Party_extref.KEXTERNALREFERENCECODEID,
      X005aa_Party_extref.LONG,
      X005aa_Party_extref.LANK,
      X005aa_Party_extref.EXTERNALREFERENCENUMBER
    FROM
      X005aa_Party_extref
    WHERE
      X005aa_Party_extref.STARTDATE <= Date('%TODAY%') AND X005aa_Party_extref.ENDDATE >= Date('%TODAY%') AND
      X005aa_Party_extref.KEXTERNALREFERENCECODEID = '6525'
    ;"""
    so_curs.execute("DROP VIEW IF EXISTS "+sr_file)
    s_sql = s_sql.replace("%TODAY%", funcdatn.get_today_date())
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD VIEW: "+sr_file)

    # BUILD CURRENT STUDENT PASSPORT TABLE
    print("Build current student passport table...")
    sr_file = "X005ac_Party_pass_curr"
    s_sql = "CREATE VIEW "+sr_file+" AS " + """
    SELECT DISTINCT
      X005aa_Party_extref.KBUSINESSENTITYID,
      X005aa_Party_extref.KEXTERNALREFERENCECODEID,
      X005aa_Party_extref.LONG,
      X005aa_Party_extref.LANK,
      X005aa_Party_extref.EXTERNALREFERENCENUMBER
    FROM
      X005aa_Party_extref
    WHERE
      X005aa_Party_extref.STARTDATE <= Date('%TODAY%') AND X005aa_Party_extref.ENDDATE >= Date('%TODAY%') AND
      X005aa_Party_extref.KEXTERNALREFERENCECODEID = '6526'
    ;"""
    so_curs.execute("DROP VIEW IF EXISTS "+sr_file)
    s_sql = s_sql.replace("%TODAY%", funcdatn.get_today_date())
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD VIEW: "+sr_file)

    # BUILD CURRENT STUDENT PASSPORT FILE
    print("Build current student study permit table...")
    sr_file = "X005ad_Party_perm_curr"
    s_sql = "CREATE VIEW "+sr_file+" AS " + """
    SELECT DISTINCT
      X005aa_Party_extref.KBUSINESSENTITYID,
      X005aa_Party_extref.KEXTERNALREFERENCECODEID,
      X005aa_Party_extref.LONG,
      X005aa_Party_extref.LANK,
      X005aa_Party_extref.EXTERNALREFERENCENUMBER
    FROM
      X005aa_Party_extref
    WHERE
      X005aa_Party_extref.STARTDATE <= Date('%TODAY%') AND X005aa_Party_extref.ENDDATE >= Date('%TODAY%') AND
      X005aa_Party_extref.KEXTERNALREFERENCECODEID = '9690'
    ;"""
    so_curs.execute("DROP VIEW IF EXISTS "+sr_file)
    s_sql = s_sql.replace("%TODAY%", funcdatn.get_today_date())
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD VIEW: "+sr_file)

    # TODO Convert fields to uppercase

    # BUILD STUDENT PARTY FILE
    print("Build party file...")
    sr_file = "X000_Party"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    SELECT
      PARTY.KBUSINESSENTITYID,
      PARTY.PARTYTYPE,
      PARTY.NAME,
      CASE
        WHEN PARTY.PARTYTYPE = 2 THEN UPPER(PARTY.NAME)
        ELSE UPPER(PARTY.SURNAME)
      END AS SURNAME,
      Upper(PARTY.INITIALS) As INITIALS,
      Upper(PARTY.FIRSTNAMES) As FIRSTNAMES,
      CASE
        WHEN PARTY.PARTYTYPE = 2 AND PARTY.CONTACTPERSONNAME LIKE('KRED%') THEN ""
        WHEN PARTY.PARTYTYPE = 2 AND PARTY.CONTACTPERSONNAME LIKE(' %') THEN ""        
        WHEN PARTY.PARTYTYPE = 2 THEN UPPER(PARTY.CONTACTPERSONNAME)
        ELSE UPPER(PARTY.NICKNAME)
      END AS NICKNAME,
      Upper(PARTY.MAIDENNAME) As MAIDENNAME,
      PARTY.DATEOFBIRTH,
      ID.EXTERNALREFERENCENUMBER AS IDNO,
      Upper(PASSPORT.EXTERNALREFERENCENUMBER) As PASSPORT,
      Upper(SPERMIT.EXTERNALREFERENCENUMBER) AS STUDYPERMIT,
      PARTY.FTITLECODEID,
      X000_Codedesc_title.LONG AS TITLE,
      X000_Codedesc_title.LANK AS TITEL,
      PARTY.FGENDERCODEID,
      PARTY.FGENDERCODE,
      X000_Codedesc_gender.LONG AS GENDER,
      X000_Codedesc_gender.LANK AS GESLAG,
      PARTY.FNATIONALITYCODEID,
      X000_Codedesc_nationality.LONG AS NATIONALITY,
      X000_Codedesc_nationality.LANK AS NASIONALITEIT,
      PARTY.FPOPULATIONGROUPCODEID,
      X000_Codedesc_population.LONG AS POPULATION,
      X000_Codedesc_population.LANK AS POPULASIE,
      PARTY.FRACECODEID,
      X000_Codedesc_race.LONG AS RACE,
      X000_Codedesc_race.LANK AS RAS,
      PARTY.ISFOREIGN,
      PARTY.CONTACTPERSONNAME,
      PARTY.FRELIGIOUSAFFILIATIONCODEID,
      PARTY.FPREFERREDCORRCODEID,
      PARTY.FPREFACCCORRCODEID,
      PARTY.LOCKSTAMP,
      PARTY.AUDITDATETIME,
      PARTY.FAUDITSYSTEMFUNCTIONID,
      PARTY.FAUDITUSERCODE,
      CASE
        WHEN PARTY.PARTYTYPE = 2 THEN UPPER(PARTY.NAME)
        ELSE Upper(Trim(SURNAME))||' '||Replace(Upper(Trim(INITIALS)),' ','')
      END AS SURN_INIT,
      CASE
        WHEN PARTY.PARTYTYPE = 2 THEN UPPER(PARTY.NAME)
        ELSE Upper(Trim(SURNAME))||' ('||Replace(Upper(Trim(INITIALS)),' ','')||') '||Upper(Trim(FIRSTNAMES))
      END AS FULL_NAME
    FROM
      PARTY
      LEFT JOIN X000_Codedescription X000_Codedesc_title ON X000_Codedesc_title.KCODEDESCID = PARTY.FTITLECODEID
      LEFT JOIN X000_Codedescription X000_Codedesc_gender ON X000_Codedesc_gender.KCODEDESCID = PARTY.FGENDERCODEID
      LEFT JOIN X000_Codedescription X000_Codedesc_nationality ON X000_Codedesc_nationality.KCODEDESCID =
        PARTY.FNATIONALITYCODEID
      LEFT JOIN X000_Codedescription X000_Codedesc_population ON X000_Codedesc_population.KCODEDESCID =
        PARTY.FPOPULATIONGROUPCODEID
      LEFT JOIN X000_Codedescription X000_Codedesc_race ON X000_Codedesc_race.KCODEDESCID = PARTY.FRACECODEID
      LEFT JOIN X005ab_Party_idno_curr ID ON ID.KBUSINESSENTITYID = PARTY.KBUSINESSENTITYID
      LEFT JOIN X005ac_Party_pass_curr PASSPORT ON PASSPORT.KBUSINESSENTITYID = PARTY.KBUSINESSENTITYID
      LEFT JOIN X005ad_Party_perm_curr SPERMIT ON SPERMIT.KBUSINESSENTITYID = PARTY.KBUSINESSENTITYID
    ORDER BY
      PARTY.KBUSINESSENTITYID
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    """*************************************************************************
    STUDENT ACCOUNT TRANSACTIONS
    *************************************************************************"""

    # BUILD TRANSACTION MASTER
    funcfile.writelog("STUDENT TRANSACTIONS")
    sr_file = "X000_Transmaster"
    print("Build transaction master...")
    s_sql = "CREATE VIEW " + sr_file + " AS " + """
    Select
        TMAS.KTRANSMASTERID,
        TMAS.TRANSCODE,
        TMAS.FBUSAREAID,
        Upper(DESE.DESCRIP) AS DESCRIPTION_E,
        Upper(DESA.DESCRIP) AS DESCRIPTION_A,
        TMAS.FENROLCHECKCODEID,
        Upper(ENRO.LONG) AS ENROL_CHECK_E,
        Upper(ENRO.LANK) AS ENROL_CHECK_A,
        TMAS.FSUBACCTYPECODEID,
        Upper(SUBA.LONG) AS SYBACCTYPE_E,
        Upper(SUBA.LANK) AS SUBACCTYPE_A,
        TMAS.FAGEANALYSISCTCODEID,
        Upper(AGEA.LONG) AS AGEANAL_E,
        Upper(AGEA.LANK) AS AGEANAL_A,
        TMAS.FGENERALLEDGERTYPECODEID,
        Upper(TYPE.LONG) AS GLTYPE_E,
        Upper(TYPE.LANK) AS GLTYPE_A,
        TMAS.FTRANSGROUPCODEID,
        Upper(TRAG.LONG) AS TRANGROUP_E,
        Upper(TRAG.LANK) AS TRANGROUP_A,
        TMAS.STARTDATE,
        TMAS.ENDDATE,
        TMAS.FREBATETRANSTYPECODEID,
        TMAS.FINSTALLMENTCODEID,
        TMAS.FSUBSYSTRANSCODEID,
        TMAS.ISPERMITTEDTOCREATEMANUALLY,
        TMAS.ISSUMMARISED,
        TMAS.ISCONSOLIDATIONNEEDED,
        TMAS.ISEXTERNALTRANS,
        TMAS.ISONLYDEBITSSHOWN,
        TMAS.ISDEBTEXCLUDED,
        TMAS.ISMISCELLANEOUS,
        TMAS.LOCKSTAMP,
        TMAS.AUDITDATETIME,
        TMAS.FAUDITSYSTEMFUNCTIONID,
        TMAS.FAUDITUSERCODE,
        TMAS.ISMAF,
        TMAS.ISNONREGSTUDENTALLOWED
    From
        TRANSMASTER TMAS Left Join
        TRANSMASTERDESC DESA ON DESA.KTRANSMASTERID = TMAS.KTRANSMASTERID AND DESA.KSYSLANGUAGECODEID = '2' Left Join
        TRANSMASTERDESC DESE ON DESE.KTRANSMASTERID = TMAS.KTRANSMASTERID AND DESE.KSYSLANGUAGECODEID = '3' Left Join
        X000_Codedescription ENRO ON ENRO.KCODEDESCID = TMAS.FENROLCHECKCODEID Left Join
        X000_Codedescription SUBA ON SUBA.KCODEDESCID = TMAS.FSUBACCTYPECODEID Left Join
        X000_Codedescription AGEA ON AGEA.KCODEDESCID = TMAS.FAGEANALYSISCTCODEID Left Join
        X000_Codedescription TYPE ON TYPE.KCODEDESCID = TMAS.FGENERALLEDGERTYPECODEID Left Join
        X000_Codedescription TRAG ON TRAG.KCODEDESCID = TMAS.FTRANSGROUPCODEID
    """
    so_curs.execute("DROP VIEW IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD VIEW: X000_Transmaster")

    # MESSAGE
    if funcconf.l_mess_project:
        sr_file = "X000_Qualifications"
        i = funcsys.tablerowcount(so_curs, sr_file)
        funcsms.send_telegram("", "administrator", "<b>" + str(i) + "</b> Qualifications")
        sr_file = "X000_Modules"
        i = funcsys.tablerowcount(so_curs, sr_file)
        funcsms.send_telegram("", "administrator", "<b>" + str(i) + "</b> Modules")

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
    funcfile.writelog("-------------------------")
    funcfile.writelog("COMPLETED: B003_VSS_LISTS")

    return


if __name__ == '__main__':
    try:
        vss_lists()
    except Exception as e:
        funcsys.ErrMessage(e, funcconf.l_mess_project, "B003_vss_lists", "B003_vss_lists")
