"""
Script to build internal audit lists
Created on 24 Oct 2022
Author: Albert J v Rensburg (NWU:21162395)
"""

# IMPORT PYTHON MODULES
import sqlite3
import csv

# IMPORT OWN MODULES
from _my_modules import funcconf
from _my_modules import funcdatn
from _my_modules import funcfile
from _my_modules import funcstat
from _my_modules import funcsys
from _my_modules import funcsms

# INDEX
"""
ENVIRONMENT
OPEN THE DATABASES
TEMPORARY AREA
BEGIN OF SCRIPT
OBTAIN LIST OF ASSIGNMENTS
OBTAIN LIST OF FINDINGS
OBTAIN LIST OF SPECIAL INVESTIGATIONS PER PERIOD
OBTAIN LIST OF SPECIAL INVESTIGATIONS PER CALENDAR YEAR
IDENTIFY PRIORITY INCONSISTENCY
IDENTIFY STATUS INCONSISTENCY
IDENTIFY YEAR INCONSISTENCY
ASSIGNMENT OVERDUE
FOLLOW-UP NO FINDING
FOLLOW-UP NO REMINDER
IDENTIFY FINDING RATING INVALID
CUSTOMER SATISFACTION SURVEY
END OF SCRIPT
"""

# SCRIPT WIDE VARIABLES
s_function: str = "B009_ia_lists"


def ia_lists(s_period: str = "curr"):
    """
    Script to build INTERNAL AUDIT lists
    :param s_period: str: The audit season
    :return: Nothing
    """

    """*****************************************************************************
    ENVIRONMENT
    *****************************************************************************"""

    # FUNCTION WIDE VARIABLES
    if s_period == "prev" and funcdatn.get_current_month() >= "10":
        s_year: str = str(int(funcdatn.get_previous_year()) + 1)
    elif s_period == "prev":
        s_year: str = funcdatn.get_previous_year()
    elif s_period == "curr" and funcdatn.get_current_month() >= "10":
        s_year: str = str(int(funcdatn.get_current_year())+1)
    elif s_period == "curr":
        s_year: str = funcdatn.get_current_year()
    else:
        s_year: str = s_period
    s_from: str = str(int(s_year) - 1) + '-10-01'
    s_to: str = s_year + '-09-30'
    # ed_path: str = "S:/_external_data/"  # External data path
    # re_path: str = f"{funcconf.drive_data_results}Internal_audit/{s_year}"
    so_path: str = f"{funcconf.drive_data_raw}Internal_audit/"  # Source database path
    so_file: str = "Web_ia_nwu.sqlite"
    l_debug: bool = False
    # l_mail: bool = funcconf.l_mail_project
    # l_mail: bool = True
    l_mess: bool = funcconf.l_mess_project
    # l_mess: bool = False
    # l_record: bool = False
    # l_export: bool = False
    s_madelein: str = "11987774@nwu.ac.za"
    s_shahed: str = "10933107@nwu.ac.za"
    s_nicolene: str = "12119180@nwu.ac.za"

    # LOG
    funcfile.writelog("Now")
    funcfile.writelog("SCRIPT: " + s_function.upper())
    funcfile.writelog("-" * len("script: "+s_function))
    if l_debug:
        print(s_function.upper())

    # MESSAGE
    if l_mess:
        funcsms.send_telegram("", "administrator", "<b>" + s_function.upper() + "</b>")

    """************************************************************************
    OPEN THE DATABASES
    ************************************************************************"""
    funcfile.writelog("OPEN THE DATABASES")
    if l_debug:
        print("OPEN THE DATABASES")

    # OPEN SQLITE SOURCE table
    if l_debug:
        print("Open sqlite database...")
    with sqlite3.connect(so_path + so_file) as so_conn:
        so_curs = so_conn.cursor()
    funcfile.writelog("OPEN DATABASE: " + so_file)

    """
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
    """

    """************************************************************************
    TEMPORARY AREA
    ************************************************************************"""
    funcfile.writelog("TEMPORARY AREA")
    if l_debug:
        print("TEMPORARY AREA")

    """************************************************************************
    BEGIN OF SCRIPT
    ************************************************************************"""
    funcfile.writelog("BEGIN OF SCRIPT")
    if l_debug:
        print("BEGIN OF SCRIPT")

    """************************************************************************
    OBTAIN LIST OF ASSIGNMENTS
    ************************************************************************"""
    funcfile.writelog("OBTAIN ASSIGNMENTS")
    if l_debug:
        print("OBTAIN ASSIGNMENTS")

    # OBTAIN THE LIST
    if l_debug:
        print("Obtain the list of assignments...")
    sr_file = "X000_Assignment_" + s_period
    s_sql = "CREATE TABLE " + sr_file + " AS" + """
    Select
        assi.ia_assi_auto As File,
        user.name As Auditor,
        assi.ia_assi_year As Year,
        Cast(Case
            When Date(assi.ia_assi_startdate) >= '%from%' And Date(assi.ia_assi_startdate) <= '%to%' 
            Then %year%
            Else '0' 
        End As Integer) As Year_calc,     
        Case
            When assi.ia_assi_year = %year%
            Then 'Current'
            When assi.ia_assi_priority < 9
            Then 'Previous'
            Else 'Period'
        End As Year_indicator,
        cate.ia_assicate_name As Category,
        type.ia_assitype_name As Type,
        Case
            When cate.ia_assicate_name = 'Development'
            Then 'Administration (audit)'     
            When cate.ia_assicate_name = 'Administration (university process)'
            Then 'Administration (process)'        
            When cate.ia_assicate_name = 'Election'
            Then 'Verification audit'
            When cate.ia_assicate_name = 'Reporting'
            Then cate.ia_assicate_name
            When cate.ia_assicate_name = 'Consultation'
            Then cate.ia_assicate_name
            When cate.ia_assicate_name = 'Continuous audit'
            Then cate.ia_assicate_name
            When cate.ia_assicate_name = 'Special investigation'
            Then cate.ia_assicate_name
            Else type.ia_assitype_name
        End As Type_calc,
        assi.ia_assi_priority As Priority_number,
        Case
            When assi.ia_assi_priority = 9
            Then '9-Closed'
            When assi.ia_assi_priority = 8
            Then '8-Continuous'
            When assi.ia_assi_priority = 7
            Then '7-Follow-up'
            When assi.ia_assi_priority = 3
            Then '3-High'
            When assi.ia_assi_priority = 2
            Then '2-Medium'
            When assi.ia_assi_priority = 1
            Then '1-Low'
            Else '0-Inactive'
        End As Priority_word,
        asta.ia_assistat_name As Assignment_status,
        Case
            When SubStr(asta.ia_assistat_name, 1, 2) = '00'
            Then '1-NotStarted'
            When assi.ia_assi_priority = 8
            Then '8-Continuous'
            When assi.ia_assi_priority = 7
            Then '7-Follow-up'
            When Upper(SubStr(asta.ia_assistat_name, 1, 2)) = 'CO'
            Then '9-Completed'
            When Cast(SubStr(asta.ia_assistat_name, 1, 2) As Integer) >= 1 And Cast(SubStr(asta.ia_assistat_name, 1,
                2) As Integer) <= 10
            Then '2-Planning'
            When Cast(SubStr(asta.ia_assistat_name, 1, 2) As Integer) >= 11 And Cast(SubStr(asta.ia_assistat_name,
                1, 2) As Integer) <= 50
            Then '3-FieldworkInitial'
            When Cast(SubStr(asta.ia_assistat_name, 1, 2) As Integer) >= 51 And Cast(SubStr(asta.ia_assistat_name,
                1, 2) As Integer) <= 79
            Then '4-FieldworkFinal'
            When Cast(SubStr(asta.ia_assistat_name, 1, 2) As Integer) >= 80 And Cast(SubStr(asta.ia_assistat_name,
                1, 2) As Integer) <= 89
            Then '5-DraftReport'
            When Cast(SubStr(asta.ia_assistat_name, 1, 2) As Integer) >= 90 And Cast(SubStr(asta.ia_assistat_name,
                1, 2) As Integer) <= 99
            Then '6-FinalReport'
            Else 'Unknown'
        End As Assignment_status_calc,
        assi.ia_assi_name || ' (' || assi.ia_assi_auto || ')' As Assignment,
        assi.ia_assi_startdate As Date_opened,
        assi.ia_assi_si_reportdate As Date_case_reported,
        Case
            When Date(assi.ia_assi_startdate) < '%from%'
            Then StrfTime('%Y', assi.ia_assi_startdate)
            When Date(assi.ia_assi_startdate) < '%to%'
            Then StrfTime('%Y-%m', assi.ia_assi_startdate)
            When Date(assi.ia_assi_startdate) >= '%from%'
            Then StrfTime('%Y', assi.ia_assi_startdate)
            Else StrfTime('%Y-%m', 'now')
        End As Date_opened_month,
        Case
            When Cast((StrfTime("%s", 'now') - StrfTime("%s", assi.ia_assi_startdate)) / 86400.0 As Integer) > 0
            Then Cast((StrfTime("%s", 'now') - StrfTime("%s", assi.ia_assi_startdate)) / 86400.0 As Integer)
            Else 0
        End As Date_opened_days,
        assi.ia_assi_completedate As Date_due,
        assi.ia_assi_si_report2date As Date_due_si,
        assi.ia_assi_proofdate As Date_reported,
        assi.ia_assi_finishdate As Date_closed,
        Case
            When assi.ia_assi_priority = 7
            Then assi.ia_assi_proofdate
            When assi.ia_assi_priority = 8
            Then Date('now')
            Else assi.ia_assi_finishdate
        End As Date_closed_calc,
        Case
            When assi.ia_assi_priority = 7 And Date(assi.ia_assi_proofdate) >= '%from%' And Date(assi.ia_assi_proofdate) <= '%to%'
            Then StrfTime('%Y-%m', assi.ia_assi_proofdate)
            When assi.ia_assi_priority = 7
            Then StrfTime('%Y', assi.ia_assi_proofdate)
            When Date(assi.ia_assi_finishdate) >= '%from%' And Date(assi.ia_assi_finishdate) <= '%to%'
            Then StrfTime('%Y-%m', assi.ia_assi_finishdate)
            Else '00 Unclosed'
        End As Date_closed_month,
        Case
            When Cast((StrfTime("%s", assi.ia_assi_finishdate) - StrfTime("%s", assi.ia_assi_startdate)) / 86400.0
                As Integer) > 0
            Then Cast((StrfTime("%s", assi.ia_assi_finishdate) - StrfTime("%s", assi.ia_assi_startdate)) / 86400.0
                As Integer)
            Else 0
        End As Days_to_close,
        Case
            When assi.ia_assi_priority < 7 And StrfTime("%s", 'now') > StrfTime("%s", assi.ia_assi_completedate)
            Then Cast((StrfTime("%s", 'now') - StrfTime("%s", assi.ia_assi_completedate)) / 86400.0 As Integer)
            Else 0
        End As Days_due,
        Case
            When assi.ia_assi_priority < 7 And StrfTime("%s", 'now') > StrfTime("%s", assi.ia_assi_si_report2date)
            Then Cast((StrfTime("%s", 'now') - StrfTime("%s", assi.ia_assi_si_report2date)) / 86400.0 As Integer)
            Else 0
        End As Days_due_si,
        user.email As ia_user_mail,
        Case
            When user.email == '%madelein%'
            Then ''
            When user.email == '%shahed%'
            Then ''
            When user.email == '%nicolene%'
            Then ''
            When cate.ia_assicate_name = 'Assignment'
            Then '%nicolene%'
            When cate.ia_assicate_name = 'Special investigation'
            Then '%shahed%'
            Else ''
        End As Email_manager1,
        Case
            When user.email == '%madelein%'
            Then ''
            Else '%madelein%'  
        End As Email_manager2
    From
        ia_assignment assi Inner Join
        jm4_users user On user.id = assi.ia_user_sysid Inner Join
        ia_assignment_status asta On asta.ia_assistat_auto = assi.ia_assistat_auto Inner Join
        ia_assignment_category cate On cate.ia_assicate_auto = assi.ia_assicate_auto Inner Join
        ia_assignment_type type On type.ia_assitype_auto = assi.ia_assitype_auto
    Where
        (assi.ia_assi_year = %year% And
            user.block = 0 And
            cate.ia_assicate_private = '0') Or
        (assi.ia_assi_year < %year% And
            assi.ia_assi_priority < 9 And
            user.block = 0 And
            cate.ia_assicate_private = '0') Or
        (assi.ia_assi_finishdate >= '%from%' And
            assi.ia_assi_finishdate <= '%to%' And
            user.block = 0 And
            cate.ia_assicate_private = '0')
    Order By
        user.name,
        cate.ia_assicate_name,
        type.ia_assitype_name,
        assi.ia_assi_name
    ;"""
    s_sql = s_sql.replace("%year%", s_year)
    s_sql = s_sql.replace("%from%", s_from)
    s_sql = s_sql.replace("%to%", s_to)
    s_sql = s_sql.replace("%madelein%", s_madelein)
    s_sql = s_sql.replace("%nicolene%", s_nicolene)
    s_sql = s_sql.replace("%shahed%", s_shahed)
    if l_debug:
        print(s_sql)
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_curs.execute("SELECT File FROM " + sr_file)
    ri_count: int = len(so_curs.fetchall())
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # MESSAGE
    if l_mess:
        funcsms.send_telegram('', 'administrator', '<b>IA Assignments</b> ' + s_year + ' records ' + str(ri_count))

    """************************************************************************
    OBTAIN LIST OF FINDINGS
    ************************************************************************"""
    funcfile.writelog("OBTAIN FINDINGS")
    if l_debug:
        print("OBTAIN FINDINGS")

    # OBTAIN THE LIST
    if l_debug:
        print("Obtain the list of findings...")
    sr_file = "X000_Finding_" + s_period
    s_sql = "CREATE TABLE " + sr_file + " AS" + """
    Select
        user.name As auditor,
        user.block As auditor_active,
        assi.ia_assi_year As year,
        assi.ia_assi_finishdate,
        cate.ia_assicate_name As category,
        cate.ia_assicate_private As category_private,
        type.ia_assitype_name As type,
        type.ia_assitype_private As type_private,
        assi.ia_assi_name || ' (' || assi.ia_assi_auto || ')' As Assignment,
        assi.ia_assi_priority As priority,
        find.ia_find_name || ' (' || find.ia_find_auto || ')' As finding,
        find.ia_find_private As finding_private,
        find.ia_find_appendix As finding_appendix,
        find.ia_find_riskmatrix_toggle As riskmatrix_toggle,
        stat.ia_findstat_name As wstatus,
        stat.ia_findstat_private As wstatus_private,
        rate.ia_findrate_name As rating,
        rate.ia_findrate_impact As rating_value,
        `like`.ia_findlike_name As likelihood,
        `like`.ia_findlike_value As likelihood_value,
        cont.ia_findcont_name As control,
        cont.ia_findcont_value As control_value,
        user.email As ia_user_mail,
        Case
            When user.email == '%madelein%'
            Then ''
            When user.email == '%shahed%'
            Then ''
            When user.email == '%nicolene%'
            Then ''
            When cate.ia_assicate_name = 'Assignment'
            Then '%nicolene%'
            When cate.ia_assicate_name = 'Special investigation'
            Then '%shahed%'
            Else ''
        End As Email_manager1,
        Case
            When user.email == '%madelein%'
            Then ''
            Else '%madelein%'  
        End As Email_manager2        
    From
        ia_finding find Inner Join
        ia_assignment assi On assi.ia_assi_auto = find.ia_assi_auto Inner Join
        jm4_users user On user.id = assi.ia_user_sysid Inner Join
        ia_assignment_category cate On cate.ia_assicate_auto = assi.ia_assicate_auto Inner Join
        ia_assignment_type type On type.ia_assitype_auto = assi.ia_assitype_auto Inner Join
        ia_finding_status stat On stat.ia_findstat_auto = find.ia_findstat_auto Left Join
        ia_finding_rate rate On rate.ia_findrate_auto = find.ia_findrate_auto Left Join
        ia_finding_likelihood `like` On `like`.ia_findlike_auto = find.ia_findlike_auto Left Join
        ia_finding_control cont On cont.ia_findcont_auto = find.ia_findcont_auto
    Where
        (assi.ia_assi_year = '%year%' And
            user.block = 0 And
            cate.ia_assicate_private = '0' And
            type.ia_assitype_private = '0' And
            find.ia_find_private = '0' And
            find.ia_find_appendix = 0 And
            stat.ia_findstat_private = '0') Or
        (assi.ia_assi_year < '%year%' And
            user.block = 0 And
            cate.ia_assicate_private = '0' And
            type.ia_assitype_private = '0' And
            find.ia_find_private = '0' And
            find.ia_find_appendix = 0 And
            stat.ia_findstat_private = '0' And
            assi.ia_assi_priority < 9) Or
        (user.block = 0 And
            cate.ia_assicate_private = '0' And
            type.ia_assitype_private = '0' And
            find.ia_find_private = '0' And
            find.ia_find_appendix = 0 And
            stat.ia_findstat_private = '0' And
            assi.ia_assi_finishdate >= '%from%' And
            assi.ia_assi_finishdate <= '%to%')
    Order By
        auditor,
        year,
        category,
        type
     ;"""
    s_sql = s_sql.replace("%year%", s_year)
    s_sql = s_sql.replace("%from%", s_from)
    s_sql = s_sql.replace("%to%", s_to)
    s_sql = s_sql.replace("%madelein%", s_madelein)
    s_sql = s_sql.replace("%nicolene%", s_nicolene)
    s_sql = s_sql.replace("%shahed%", s_shahed)
    if l_debug:
        print(s_sql)
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_curs.execute("SELECT auditor FROM " + sr_file)
    ri_count: int = len(so_curs.fetchall())
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # MESSAGE
    if l_mess:
        funcsms.send_telegram('', 'administrator', '<b>IA Findings</b> ' + s_year + ' records ' + str(ri_count))

    """************************************************************************
    OBTAIN LIST OF SPECIAL INVESTIGATIONS PER PERIOD
    ************************************************************************"""
    funcfile.writelog("OBTAIN SPECIAL INVESTIGATIONS PERIOD")
    if l_debug:
        print("OBTAIN SPECIAL INVESTIGATIONS PERIOD")

    # OBTAIN THE LIST
    if l_debug:
        print("Obtain the list of investigations...")
    sr_file = "X000_Assignment_si_" + s_period
    s_sql = "CREATE TABLE " + sr_file + " AS" + """
    SELECT
        us.name As Auditor,
        ia.ia_assi_year as Assignment_year,
        Cast(Case
            When Date(ia.ia_assi_startdate) >= '%from%' And Date(ia.ia_assi_startdate) <= '%to%' 
            Then %year%
            Else '0' 
        End As Integer) As Year_calc,     
        Case
            When ia.ia_assi_year = %year%
            Then 'Current'
            When ia.ia_assi_priority < 9
            Then 'Previous'
            Else 'Period'
        End As Year_indicator,
        ia.ia_assi_name || ' (' || ia.ia_assi_auto || ')' as Assignment_name,
        ia.ia_assi_si_caseyear as Case_year,
        ia.ia_assi_si_casenumber as Case_number,
        ia.ia_assi_si_boxyear as Box_year,
        ia.ia_assi_si_boxnumber as Box_number,
        ia.ia_assi_startdate as Start_date,
        Case
            When Date(ia.ia_assi_startdate) < '%from%'
            Then StrfTime('%Y', ia.ia_assi_startdate)
            When Date(ia.ia_assi_startdate) < '%to%'
            Then StrfTime('%Y-%m', ia.ia_assi_startdate)
            When Date(ia.ia_assi_startdate) >= '%from%'
            Then StrfTime('%Y', ia.ia_assi_startdate)
            Else StrfTime('%Y-%m', 'now')
        End As Date_opened_month,
        Case
            When Cast((StrfTime("%s", 'now') - StrfTime("%s", ia.ia_assi_startdate)) / 86400.0 As Integer) > 0
            Then Cast((StrfTime("%s", 'now') - StrfTime("%s", ia.ia_assi_startdate)) / 86400.0 As Integer)
            Else 0
        End As Date_opened_days,
        ia.ia_assi_si_reportdate as Report_date,
        ia.ia_assi_si_report1date as Report1_date,
        ia.ia_assi_si_report2date as Report2_date,
        ia.ia_assi_completedate as Due_date,
        ia.ia_assi_finishdate as Close_date,    
        Case
            When ia.ia_assi_priority = 7
            Then ia.ia_assi_proofdate
            When ia.ia_assi_priority = 8
            Then Date('now')
            Else ia.ia_assi_finishdate
        End As Close_date_calc,
        Case
            When ia.ia_assi_priority = 7 And Date(ia.ia_assi_proofdate) >= '%from%' And Date(ia.ia_assi_proofdate) <= '%to%'
            Then StrfTime('%Y-%m', ia.ia_assi_proofdate)
            When ia.ia_assi_priority = 7
            Then StrfTime('%Y', ia.ia_assi_proofdate)
            When Date(ia.ia_assi_finishdate) >= '%from%' And Date(ia.ia_assi_finishdate) <= '%to%'
            Then StrfTime('%Y-%m', ia.ia_assi_finishdate)
            Else '00 Unclosed'
        End As Close_date_month,
        Case
            When Cast((StrfTime("%s", ia.ia_assi_finishdate) - StrfTime("%s", ia.ia_assi_startdate)) / 86400.0
                As Integer) > 0
            Then Cast((StrfTime("%s", ia.ia_assi_finishdate) - StrfTime("%s", ia.ia_assi_startdate)) / 86400.0
                As Integer)
            Else 0
        End As Days_to_close,
        Case
            When ia.ia_assi_priority < 7 And StrfTime("%s", 'now') > StrfTime("%s", ia.ia_assi_completedate)
            Then Cast((StrfTime("%s", 'now') - StrfTime("%s", ia.ia_assi_completedate)) / 86400.0 As Integer)
            Else 0
        End As Days_due,
        Case
            When ia.ia_assi_priority < 7 And StrfTime("%s", 'now') > StrfTime("%s", ia.ia_assi_si_report2date)
            Then Cast((StrfTime("%s", 'now') - StrfTime("%s", ia.ia_assi_si_report2date)) / 86400.0 As Integer)
            Else 0
        End As Days_due_si,
        at.ia_assitype_name as Assignment_type,
        Case
            When ia.ia_assi_priority == 0 Then '0-Inactive'
            When ia.ia_assi_priority == 1 Then '1-Low'
            When ia.ia_assi_priority == 2 Then '2-Medium'
            When ia.ia_assi_priority == 3 Then '3-High'
            When ia.ia_assi_priority == 7 Then '7-Follow-up'
            When ia.ia_assi_priority == 8 Then '8-Continuous'
            When ia.ia_assi_priority == 9 Then '9-Closed'
            Else '0-Unknown'
        End As Assignment_priority, 
        --ia.ia_assi_si_accused as Accused,
        --ia.ia_assi_si_issue as Issue,
        og.ia_assiorig_name as Assignment_origin,
        co.ia_assicond_name as Conducted_by,
        st.ia_assistat_name as Assignment_status,
        Case
            When SubStr(st.ia_assistat_name, 1, 2) = '00'
            Then '1-NotStarted'
            When ia.ia_assi_priority = 8
            Then '8-Continuous'
            When ia.ia_assi_priority = 7
            Then '7-Follow-up'
            When Upper(SubStr(st.ia_assistat_name, 1, 2)) = 'CO'
            Then '9-Completed'
            When Cast(SubStr(st.ia_assistat_name, 1, 2) As Integer) >= 1 And Cast(SubStr(st.ia_assistat_name, 1,
                2) As Integer) <= 10
            Then '2-Planning'
            When Cast(SubStr(st.ia_assistat_name, 1, 2) As Integer) >= 11 And Cast(SubStr(st.ia_assistat_name,
                1, 2) As Integer) <= 50
            Then '3-FieldworkInitial'
            When Cast(SubStr(st.ia_assistat_name, 1, 2) As Integer) >= 51 And Cast(SubStr(st.ia_assistat_name,
                1, 2) As Integer) <= 79
            Then '4-FieldworkFinal'
            When Cast(SubStr(st.ia_assistat_name, 1, 2) As Integer) >= 80 And Cast(SubStr(st.ia_assistat_name,
                1, 2) As Integer) <= 89
            Then '5-DraftReport'
            When Cast(SubStr(st.ia_assistat_name, 1, 2) As Integer) >= 90 And Cast(SubStr(st.ia_assistat_name,
                1, 2) As Integer) <= 99
            Then '6-FinalReport'
            Else 'Unknown'
        End As Assignment_status_calc,
        ia.ia_assi_si_reference as Reference,
        Cast(ia.ia_assi_si_value As Integer) as Value,
        ia.ia_assi_offi as Note_official,
        --ia.ia_assi_desc as Note_private,
        ia.ia_assi_auto as Assignment_number,
        --ia.ia_assicate_auto as Category_number,
        us.email As User_mail,
        Case
            When us.email == '%madelein%'
            Then ''
            When us.email == '%shahed%'
            Then ''
            When us.email == '%nicolene%'
            Then ''
            When ca.ia_assicate_name = 'Assignment'
            Then '%nicolene%'
            When ca.ia_assicate_name = 'Special investigation'
            Then '%shahed%'
            Else ''
        End As Email_manager1,
        Case
            When us.email == '%madelein%'
            Then ''
            Else '%madelein%'  
        End As Email_manager2,
        Cast('1' As Integer) As Record_counter
    FROM
        ia_assignment ia Left Join
        jm4_users us On us.id = ia.ia_user_sysid Left Join
        ia_assignment_category ca On ca.ia_assicate_auto = ia.ia_assicate_auto Left Join
        ia_assignment_type at On at.ia_assitype_auto = ia.ia_assitype_auto Left Join
        ia_assignment_status st On st.ia_assistat_auto = ia.ia_assistat_auto Left Join
        ia_assignment_conducted co On co.ia_assicond_auto = ia.ia_assicond_auto Left Join
        ia_assignment_origin og On og.ia_assiorig_auto = ia.ia_assiorig_auto
    WHERE
        (ia.ia_assi_year = %year% And
            ia.ia_assicate_auto = 9 And        
            us.block = 0 And
            ca.ia_assicate_private = '0') Or
        (ia.ia_assi_year < %year% And
            ia.ia_assicate_auto = 9 And        
            ia.ia_assi_priority < 9 And
            us.block = 0 And
            ca.ia_assicate_private = '0') Or
        (ia.ia_assi_finishdate >= '%from%' And
            ia.ia_assi_finishdate <= '%to%' And
            ia.ia_assicate_auto = 9 And            
            us.block = 0 And
            ca.ia_assicate_private = '0')    
    ORDER BY
    ia_assi_si_caseyear,
    ia_assi_si_casenumber    
    ;"""
    s_sql = s_sql.replace("%year%", s_year)
    s_sql = s_sql.replace("%from%", s_from)
    s_sql = s_sql.replace("%to%", s_to)
    s_sql = s_sql.replace("%madelein%", s_madelein)
    s_sql = s_sql.replace("%nicolene%", s_nicolene)
    s_sql = s_sql.replace("%shahed%", s_shahed)
    if l_debug:
        print(s_sql)
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_curs.execute("SELECT assignment_number FROM " + sr_file)
    ri_count: int = len(so_curs.fetchall())
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # MESSAGE
    if l_mess:
        funcsms.send_telegram('', 'administrator', '<b>IA Assignments SI</b> ' + s_period + ' records ' + str(ri_count))

    """************************************************************************
    OBTAIN LIST OF SPECIAL INVESTIGATIONS PER CALENDAR YEAR
    ************************************************************************"""
    funcfile.writelog("OBTAIN SPECIAL INVESTIGATIONS PERIOD")
    if l_debug:
        print("OBTAIN SPECIAL INVESTIGATIONS PERIOD")

    # OBTAIN THE LIST
    if l_debug:
        print("Obtain the list of investigations...")
    sr_file = "X000_Assignment_si_" + s_period + "_year"
    s_sql = "CREATE TABLE " + sr_file + " AS" + """
    SELECT
        us.name as Auditor,
        ia.ia_assi_year as Assignment_year,
        Cast(Case
            When Date(ia.ia_assi_startdate) >= '%from%' And Date(ia.ia_assi_startdate) <= '%to%' 
            Then %year%
            Else '0' 
        End As Integer) As Year_calc,     
        Case
            When ia.ia_assi_year = %year%
            Then 'Current'
            When ia.ia_assi_priority < 9
            Then 'Previous'
            Else 'Period'
        End As Year_indicator,
        ia.ia_assi_name || ' (' || ia.ia_assi_auto || ')' as Assignment_name,
        ia.ia_assi_si_caseyear as Case_year,
        ia.ia_assi_si_casenumber as Case_number,
        ia.ia_assi_si_boxyear as Box_year,
        ia.ia_assi_si_boxnumber as Box_number,
        ia.ia_assi_startdate as Start_date,
        Case
            When Date(ia.ia_assi_startdate) < '%from%'
            Then StrfTime('%Y', ia.ia_assi_startdate)
            When Date(ia.ia_assi_startdate) < '%to%'
            Then StrfTime('%Y-%m', ia.ia_assi_startdate)
            When Date(ia.ia_assi_startdate) >= '%from%'
            Then StrfTime('%Y', ia.ia_assi_startdate)
            Else StrfTime('%Y-%m', 'now')
        End As Date_opened_month,
        Case
            When Cast((StrfTime("%s", 'now') - StrfTime("%s", ia.ia_assi_startdate)) / 86400.0 As Integer) > 0
            Then Cast((StrfTime("%s", 'now') - StrfTime("%s", ia.ia_assi_startdate)) / 86400.0 As Integer)
            Else 0
        End As Date_opened_days,
        ia.ia_assi_si_reportdate as Report_date,
        ia.ia_assi_si_report1date as Report1_date,
        ia.ia_assi_si_report2date as Report2_date,
        ia.ia_assi_completedate as Due_date,
        ia.ia_assi_finishdate as Close_date,    
        Case
            When ia.ia_assi_priority = 7
            Then ia.ia_assi_proofdate
            When ia.ia_assi_priority = 8
            Then Date('now')
            Else ia.ia_assi_finishdate
        End As Close_date_calc,
        Case
            When ia.ia_assi_priority = 7 And Date(ia.ia_assi_proofdate) >= '%from%' And Date(ia.ia_assi_proofdate) <= '%to%'
            Then StrfTime('%Y-%m', ia.ia_assi_proofdate)
            When ia.ia_assi_priority = 7
            Then StrfTime('%Y', ia.ia_assi_proofdate)
            When Date(ia.ia_assi_finishdate) >= '%from%' And Date(ia.ia_assi_finishdate) <= '%to%'
            Then StrfTime('%Y-%m', ia.ia_assi_finishdate)
            Else '00 Unclosed'
        End As Close_date_month,
        Case
            When Cast((StrfTime("%s", ia.ia_assi_finishdate) - StrfTime("%s", ia.ia_assi_startdate)) / 86400.0
                As Integer) > 0
            Then Cast((StrfTime("%s", ia.ia_assi_finishdate) - StrfTime("%s", ia.ia_assi_startdate)) / 86400.0
                As Integer)
            Else 0
        End As Days_to_close,
        Case
            When ia.ia_assi_priority < 7 And StrfTime("%s", 'now') > StrfTime("%s", ia.ia_assi_completedate)
            Then Cast((StrfTime("%s", 'now') - StrfTime("%s", ia.ia_assi_completedate)) / 86400.0 As Integer)
            Else 0
        End As Days_due,
        Case
            When ia.ia_assi_priority < 7 And StrfTime("%s", 'now') > StrfTime("%s", ia.ia_assi_si_report2date)
            Then Cast((StrfTime("%s", 'now') - StrfTime("%s", ia.ia_assi_si_report2date)) / 86400.0 As Integer)
            Else 0
        End As Days_due_si,
        at.ia_assitype_name as Assignment_type,
        Case
            When ia.ia_assi_priority == 0 Then '0-Inactive'
            When ia.ia_assi_priority == 1 Then '1-Low'
            When ia.ia_assi_priority == 2 Then '2-Medium'
            When ia.ia_assi_priority == 3 Then '3-High'
            When ia.ia_assi_priority == 7 Then '7-Follow-up'
            When ia.ia_assi_priority == 8 Then '8-Continuous'
            When ia.ia_assi_priority == 9 Then '9-Closed'
            Else '0-Unknown'
        End As Assignment_priority, 
        --ia.ia_assi_si_accused as Accused,
        --ia.ia_assi_si_issue as Issue,
        og.ia_assiorig_name as Assignment_origin,
        co.ia_assicond_name as Conducted_by,
        st.ia_assistat_name as Assignment_status,
        Case
            When SubStr(st.ia_assistat_name, 1, 2) = '00'
            Then '1-NotStarted'
            When ia.ia_assi_priority = 8
            Then '8-Continuous'
            When ia.ia_assi_priority = 7
            Then '7-Follow-up'
            When Upper(SubStr(st.ia_assistat_name, 1, 2)) = 'CO'
            Then '9-Completed'
            When Cast(SubStr(st.ia_assistat_name, 1, 2) As Integer) >= 1 And Cast(SubStr(st.ia_assistat_name, 1,
                2) As Integer) <= 10
            Then '2-Planning'
            When Cast(SubStr(st.ia_assistat_name, 1, 2) As Integer) >= 11 And Cast(SubStr(st.ia_assistat_name,
                1, 2) As Integer) <= 50
            Then '3-FieldworkInitial'
            When Cast(SubStr(st.ia_assistat_name, 1, 2) As Integer) >= 51 And Cast(SubStr(st.ia_assistat_name,
                1, 2) As Integer) <= 79
            Then '4-FieldworkFinal'
            When Cast(SubStr(st.ia_assistat_name, 1, 2) As Integer) >= 80 And Cast(SubStr(st.ia_assistat_name,
                1, 2) As Integer) <= 89
            Then '5-DraftReport'
            When Cast(SubStr(st.ia_assistat_name, 1, 2) As Integer) >= 90 And Cast(SubStr(st.ia_assistat_name,
                1, 2) As Integer) <= 99
            Then '6-FinalReport'
            Else 'Unknown'
        End As Assignment_status_calc,
        ia.ia_assi_si_reference as Reference,
        Cast(ia.ia_assi_si_value As Integer) as Value,
        ia.ia_assi_offi as Note_official,
        --ia.ia_assi_desc as Note_private,
        ia.ia_assi_auto as Assignment_number,
        --ia.ia_assicate_auto as Category_number,
        us.email As User_mail,
        Case
            When us.email == '%madelein%'
            Then ''
            When us.email == '%shahed%'
            Then ''
            When us.email == '%nicolene%'
            Then ''
            When ca.ia_assicate_name = 'Assignment'
            Then '%nicolene%'
            When ca.ia_assicate_name = 'Special investigation'
            Then '%shahed%'
            Else ''
        End As Email_manager1,
        Case
            When us.email == '%madelein%'
            Then ''
            Else '%madelein%'  
        End As Email_manager2,
        Cast('1' As Integer) As Record_counter
    FROM
        ia_assignment ia Left Join
        jm4_users us On us.id = ia.ia_user_sysid Left Join
        ia_assignment_category ca On ca.ia_assicate_auto = ia.ia_assicate_auto Left Join
        ia_assignment_type at On at.ia_assitype_auto = ia.ia_assitype_auto Left Join
        ia_assignment_status st On st.ia_assistat_auto = ia.ia_assistat_auto Left Join
        ia_assignment_conducted co On co.ia_assicond_auto = ia.ia_assicond_auto Left Join
        ia_assignment_origin og On og.ia_assiorig_auto = ia.ia_assiorig_auto
    WHERE
        (ia.ia_assi_si_caseyear = %year% And
            ia.ia_assicate_auto = 9 And        
            us.block = 0 And
            ca.ia_assicate_private = '0')
    ORDER BY
    ia_assi_si_caseyear,
    ia_assi_si_casenumber    
    ;"""
    s_sql = s_sql.replace("%year%", s_year)
    s_sql = s_sql.replace("%from%", s_from)
    s_sql = s_sql.replace("%to%", s_to)
    s_sql = s_sql.replace("%madelein%", s_madelein)
    s_sql = s_sql.replace("%nicolene%", s_nicolene)
    s_sql = s_sql.replace("%shahed%", s_shahed)
    if l_debug:
        print(s_sql)
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_curs.execute("SELECT assignment_number FROM " + sr_file)
    ri_count: int = len(so_curs.fetchall())
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # MESSAGE
    if l_mess:
        funcsms.send_telegram('', 'administrator', '<b>IA Assignments SI</b> ' + s_year + ' records ' + str(ri_count))

    """************************************************************************
    IDENTIFY PRIORITY INCONSISTENCY
    ************************************************************************"""
    funcfile.writelog("IDENTIFY PRIORITY INCONSISTENCY")
    if l_debug:
        print("IDENTIFY PRIORITY INCONSISTENCY")

    # OBTAIN THE LIST
    if l_debug:
        print("Identify priority inconsistency...")
    sr_file = "X001_Test_priority_inconsistent_" + s_period
    s_sql = "CREATE TABLE " + sr_file + " AS" + """
    Select
        'Assignment priority inconsistent' As Test,
        assc.File,
        assc.Auditor,
        assc.Year,
        assc.Category,
        assc.Type,
        assc.Priority_word As AssPriority,
        assc.Assignment_status_calc As AssStatus,
        assc.Date_closed_calc As Date_closed,
        assc.Assignment,
        assc.ia_user_mail,
        assc.Email_manager1,
        assc.Email_manager2
    From
        X000_Assignment_%period% assc
    Where
        assc.Priority_word Not Like ('9%') And
        assc.Assignment_status_calc Like ('9%')
    Order By
        assc.Auditor,
        assc.Category,
        assc.Type,
        assc.Year,
        assc.Assignment
    ;"""
    s_sql = s_sql.replace("%period%", s_period)
    if l_debug:
        print(s_sql)
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_curs.execute("SELECT File FROM " + sr_file)
    ri_count: int = len(so_curs.fetchall())
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # MESSAGE
    if l_mess and ri_count > 0:
        funcsms.send_telegram('', 'administrator', '<b>Priority inconsistencies</b> ' + str(ri_count))

    """************************************************************************
    IDENTIFY STATUS INCONSISTENCY
    ************************************************************************"""
    funcfile.writelog("IDENTIFY STATUS INCONSISTENCY")
    if l_debug:
        print("IDENTIFY STATUS INCONSISTENCY")

    # OBTAIN THE LIST
    if l_debug:
        print("Identify status inconsistency...")
    sr_file = "X001_Test_status_inconsistent_" + s_period
    s_sql = "CREATE TABLE " + sr_file + " AS" + """
    Select
        'Assignment status inconsistent' As Test,
        assc.File,
        assc.Auditor,
        assc.Year,
        assc.Category,
        assc.Type,
        assc.Priority_word As AssPriority,
        assc.Assignment_status_calc As AssStatus,
        assc.Date_closed_calc As Date_closed,
        assc.Assignment,
        assc.ia_user_mail,
        assc.Email_manager1,
        assc.Email_manager2
    From
        X000_Assignment_%period% assc
    Where
        assc.Priority_word Like ('9%') And
        assc.Assignment_status_calc Not Like ('9%')
    Order By
        assc.Auditor,
        assc.Category,
        assc.Type,
        assc.Year,
        assc.Assignment
    ;"""
    s_sql = s_sql.replace("%period%", s_period)
    if l_debug:
        print(s_sql)
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_curs.execute("SELECT File FROM " + sr_file)
    ri_count: int = len(so_curs.fetchall())
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # MESSAGE
    if l_mess and ri_count > 0:
        funcsms.send_telegram('', 'administrator', '<b>Status inconsistencies</b> ' + str(ri_count))

    """************************************************************************
    IDENTIFY YEAR INCONSISTENCY
    ************************************************************************"""
    funcfile.writelog("IDENTIFY YEAR INCONSISTENCY")
    if l_debug:
        print("IDENTIFY YEAR INCONSISTENCY")

    # OBTAIN THE LIST
    if l_debug:
        print("Identify year inconsistency...")
    sr_file = "X001_Test_year_inconsistent_" + s_period
    s_sql = "CREATE TABLE " + sr_file + " AS" + """
    Select
        'Assignment year inconsistent' As Test,
        assc.File,
        assc.Auditor,
        assc.Year,
        assc.Year_calc As "Year should be",
        assc.Date_opened,
        assc.Assignment,
        assc.ia_user_mail,
        assc.Email_manager1,
        assc.Email_manager2
    From
        X000_Assignment_%period% assc
    Where
        assc.Year != assc.Year_calc And
        assc.Year_calc > 0
    Order By
        assc.Auditor,
        assc.Year,
        assc.Assignment
    ;"""
    s_sql = s_sql.replace("%period%", s_period)
    if l_debug:
        print(s_sql)
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_curs.execute("SELECT File FROM " + sr_file)
    ri_count: int = len(so_curs.fetchall())
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # MESSAGE
    if l_mess and ri_count > 0:
        funcsms.send_telegram('', 'administrator', '<b>Year inconsistencies</b> ' + str(ri_count))

    """************************************************************************
    ASSIGNMENT OVERDUE
    ************************************************************************"""
    funcfile.writelog("ASSIGNMENT OVERDUE")
    if l_debug:
        print("ASSIGNMENT OVERDUE")

    # OBTAIN THE LIST
    if l_debug:
        print("Assignment overdue...")
    sr_file = "X001_Test_assignment_overdue_" + s_period
    s_sql = "CREATE TABLE " + sr_file + " AS" + """
    Select
        'Assignment overdue' As Test,
        assc.File,
        assc.Auditor,
        assc.Year,
        assc.Category,
        assc.Type,
        assc.Priority_word As AssPriority,
        assc.Assignment_status_calc As AssStatus,
        Case
            When assc.Date_case_reported <> ''
            Then assc.Date_case_reported
            Else assc.Date_opened
        End As Date_opened,
        Case
            When assc.Date_due_si <> ''
            Then assc.Date_due_si
            Else assc.Date_due
        End As Date_due,
        Case
            When assc.Days_due_si > 0
            Then assc.Days_due_si
            Else assc.Days_due
        End As Days_overdue,
        assc.Assignment,
        assc.ia_user_mail,
        assc.Email_manager1,
        assc.Email_manager2
    From
        X000_Assignment_%period% assc
    Where
        Case
            When assc.Days_due_si > 0
            Then assc.Days_due_si
            When assc.Days_due > 0 And assc.Date_due_si = ''
            Then assc.Days_due
            Else 0
        End > 0
    Order By
        assc.Auditor,
        Days_overdue Desc,
        assc.Assignment
    ;"""
    s_sql = s_sql.replace("%period%", s_period)
    if l_debug:
        print(s_sql)
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_curs.execute("SELECT File FROM " + sr_file)
    ri_count: int = len(so_curs.fetchall())
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # MESSAGE
    if l_mess and ri_count > 0:
        funcsms.send_telegram('', 'administrator', '<b>Assignment overdue</b> ' + str(ri_count))

    """************************************************************************
    FOLLOW-UP NO FINDING
    ************************************************************************"""
    funcfile.writelog("FOLLOW-UP NO FINDING")
    if l_debug:
        print("FOLLOW-UP NO FINDING")

    # OBTAIN THE LIST
    if l_debug:
        print("Follow-up no finding...")
    sr_file = "X001_Test_followup_nofinding_" + s_period
    s_sql = "CREATE TABLE " + sr_file + " AS" + """
    Select
        'Follow-up no finding' As Test,
        assc.File,
        assc.Auditor,
        assc.Year,
        assc.Category,
        assc.Type,
        assc.Priority_word As AssPriority,
        assc.Assignment_status_calc As AssStatus,
        fins.ia_findstat_name As FinStatus,
        assc.Date_reported As Report_final,
        assc.Assignment,
        find.ia_find_name || ' (' || find.ia_find_auto || ')' As Finding,
        assc.ia_user_mail As Mail_user,
        assc.Email_manager1 As Mail_manager1,
        assc.Email_manager2 As Mail_manager2
    From
        X000_Assignment_%period% assc Left Join
        ia_finding find On find.ia_assi_auto = assc.File Inner Join
        ia_finding_status fins On fins.ia_findstat_auto = find.ia_findstat_auto
    Where
        assc.Priority_word Like ('7%') And
        fins.ia_findstat_name Not In ('Closed', 'Request remediation')
    Order By
        assc.Auditor,
        assc.Date_reported,
        assc.Assignment,
        find.ia_find_name
    ;"""
    s_sql = s_sql.replace("%period%", s_period)
    if l_debug:
        print(s_sql)
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_curs.execute("SELECT File FROM " + sr_file)
    ri_count: int = len(so_curs.fetchall())
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # MESSAGE
    if l_mess and ri_count > 0:
        funcsms.send_telegram('', 'administrator', '<b>Follow-up no finding</b> ' + str(ri_count))

    """************************************************************************
    FOLLOW-UP NO REMINDER
    ************************************************************************"""
    funcfile.writelog("FOLLOW-UP NO REMINDER")
    if l_debug:
        print("FOLLOW-UP NO REMINDER")

    # OBTAIN THE LIST
    if l_debug:
        print("Follow-up no reminder...")
    sr_file = "X001_Test_followup_noreminder_" + s_period
    s_sql = "CREATE TABLE " + sr_file + " AS" + """
    Select
        'Follow-up no reminder' As Test,
        assc.File,
        assc.Auditor,
        assc.Year,
        assc.Category,
        assc.Type,
        assc.Priority_word As AssPriority,
        assc.Assignment_status_calc As AssStatus,
        fins.ia_findstat_name As FinStatus,
        assc.Date_reported As Report_final,
        reme.ia_findreme_name As Reminder_to,
        reme.ia_findreme_date_schedule As Remind_date,
        assc.Assignment,
        find.ia_find_name || ' (' || find.ia_find_auto || ')' As Finding,
        assc.ia_user_mail As Mail_user,
        assc.Email_manager1 As Mail_manager1,
        assc.Email_manager2 As Mail_manager2
    From
        X000_Assignment_%period% assc Left Join
        ia_finding find On find.ia_assi_auto = assc.File Inner Join
        ia_finding_status fins On fins.ia_findstat_auto = find.ia_findstat_auto Left Join
        ia_finding_remediation reme On reme.ia_find_auto = find.ia_find_auto
                And reme.ia_findreme_mail_trigger > 0
    Where
        assc.Priority_word Like ('7%') And
        fins.ia_findstat_name = 'Request remediation' And
        reme.ia_findreme_date_schedule < 1        
    Order By
        assc.Auditor,
        assc.Date_reported,
        assc.Assignment,
        find.ia_find_name
    ;"""
    s_sql = s_sql.replace("%period%", s_period)
    if l_debug:
        print(s_sql)
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_curs.execute("SELECT File FROM " + sr_file)
    ri_count: int = len(so_curs.fetchall())
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # MESSAGE
    if l_mess and ri_count > 0:
        funcsms.send_telegram('', 'administrator', '<b>Follow-up no reminder</b> ' + str(ri_count))

    """************************************************************************
    IDENTIFY FINDING RATING INVALID
    ************************************************************************"""
    funcfile.writelog("IDENTIFY PRIORITY INCONSISTENCY")
    if l_debug:
        print("IDENTIFY PRIORITY INCONSISTENCY")

    # OBTAIN THE LIST
    if l_debug:
        print("Identify finding rating invalid...")
    sr_file = "X001_Test_finding_rating_invalid_" + s_period
    s_sql = "CREATE TABLE " + sr_file + " AS" + """
    Select
        'Finding rating invalid' As Test,
        find.auditor,
        find.year,
        find.Assignment,
        find.finding,
        find.wstatus As status,
        find.ia_user_mail,
        find.Email_manager1,
        find.Email_manager2
    From
        X000_Finding_%period% find
    Where
        (find.riskmatrix_toggle in ('a', 'b') and find.rating_value Is Null) Or
        (find.riskmatrix_toggle in ('a', 'b') and find.rating_value = '0') Or
        (find.riskmatrix_toggle in ('a', 'b') and find.likelihood_value Is Null) Or
        (find.riskmatrix_toggle in ('a', 'b') and find.likelihood_value = '0') Or
        (find.riskmatrix_toggle in ('a', 'b') and find.control_value Is Null) Or
        (find.riskmatrix_toggle in ('a', 'b') and find.control_value = '0')
    Order By
        find.auditor,
        find.year,
        find.Assignment,
        find.finding
    ;"""
    s_sql = s_sql.replace("%period%", s_period)
    if l_debug:
        print(s_sql)
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_curs.execute("SELECT auditor FROM " + sr_file)
    ri_count: int = len(so_curs.fetchall())
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # MESSAGE
    if l_mess and ri_count > 0:
        funcsms.send_telegram('', 'administrator', '<b>Finding rating invalid</b> ' + str(ri_count))

    """************************************************************************
    CUSTOMER SATISFACTION SURVEY
    ************************************************************************"""
    funcfile.writelog("CUSTOMER SATISFACTION SURVEY")
    if l_debug:
        print("CUSTOMER SATISFACTION SURVEY")

    # Variables
    audit_finding = 'CUSTOMER SATISFACTION SURVEY'

    # Read the list of words to exclude in the vendor names
    auditor_to_remove = funcstat.stat_tuple(so_curs,
                                           "X000_Own_lookups",
                                           "LOOKUP_DESCRIPTION",
                                           "LOOKUP='REPORT CUSTOMER SATISFACTION SURVEY EXCLUDE AUDITOR'")

    if l_debug:
        print("AUDITOR EMAILS TO EXCLUDE:")
        print(auditor_to_remove)
        pass

    # Build the list of assignments to request customer satisfaction survey
    if l_debug:
        print("Identify assignments due for customer satisfaction survey...")
    sr_file = "X002a_Customer_satisfaction_all"
    s_sql = "CREATE TABLE " + sr_file + " AS" + f"""
    Select
        ass.File,
        fin.ia_find_auto As File_finding,
        rem.ia_findreme_auto As File_remediation,
        Upper(rem.ia_findreme_name) As Audit_client,
        Lower(rem.ia_findreme_mail) As Audit_client_email,
        Upper(ass.Auditor) As Auditor,
        Lower(rem.ia_findreme_auditor_email) As Auditor_email,
        ass.Assignment_status_calc As AssStatus,
        ass.Assignment,
        rem.ia_findreme_mail_trigger As RemStatus,
        rem.ia_findresp_auto As Response,
        rem.ia_findresp_desc As Response_description,
        Case
            When Lower(rem.ia_findreme_mail) In {auditor_to_remove}
            Then 1
            Else 0
        End As Exclude_record
    From
        X000_Assignment_curr ass Inner Join
        ia_finding fin On fin.ia_assi_auto = ass.File Inner Join
        ia_finding_remediation rem On rem.ia_find_auto = fin.ia_find_auto
    Where
        ass.Assignment_status_calc Like ('9%') And
        rem.ia_findreme_mail_trigger = 0 And
        rem.ia_findresp_desc <> ''
    ;"""
    if l_debug:
        # print(s_sql)
        pass
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # Build the list of assignments to request customer satisfaction survey
    if l_debug:
        print("Build the list of assignments to request customer satisfaction survey...")
    sr_file = "X002b_Customer_satisfaction_distribute"
    s_sql = "CREATE TABLE " + sr_file + " AS" + f"""
    Select
        '{audit_finding}' As Audit_finding,
        css.File,        
        css.Audit_client,
        css.Audit_client_email,
        css.Auditor,
        css.Auditor_email,
        css.Assignment,
        '(' || cast(css.File as text) || ')' || css.Audit_client_email As Unique_id
    From
        X002a_Customer_satisfaction_all css
    Where
        css.Exclude_record = 0
    Group by
        '(' || cast(css.File as text) || ')' || css.Audit_client_email
    Order by
        css.File
    ;"""
    if l_debug:
        # print(s_sql)
        pass
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    """************************************************************************
    END OF SCRIPT
    ************************************************************************"""
    funcfile.writelog("END OF SCRIPT")
    if l_debug:
        print("END OF SCRIPT")

    # CLOSE THE DATABASE CONNECTION
    so_conn.commit()
    so_conn.close()

    # CLOSE THE LOG WRITER
    funcfile.writelog("-" * len("completed: "+s_function))
    funcfile.writelog("COMPLETED: " + s_function.upper())

    return


if __name__ == '__main__':
    try:
        ia_lists('curr')
    except Exception as e:
        funcsys.ErrMessage(e)
