""" SCRIPT TO BUILD STUDENTS LIST **********************************************
Create on: 2 May 2019
Copyright: Albert J van Rensburg
*****************************************************************************"""

# IMPORT OWN MODULES
from _my_modules import funcconf
from _my_modules import funcdatn
from _my_modules import funcdatn
from _my_modules import funccsv
from _my_modules import funcfile
from _my_modules import funcsms
from _my_modules import funcsys

""" INDEX **********************************************************************
BUILD STUDENTS
*****************************************************************************"""


def studentlist(so_conn, re_path, s_period='curr', l_export=False):
    """
    Script to build STUDENT list
    :param so_conn: Database connection
    :param re_path: Results path
    :param s_period: Calculation period
    :param s_year: Financial year
    :param l_export: Export results
    :return: Nothing
    """

    # DECLARE VARIABLES
    if s_period == 'curr':
        s_year = funcdatn.get_current_year()
    elif s_period == 'prev':
        s_year = funcdatn.get_previous_year()
    else:
        s_year = s_period
    so_curs = so_conn.cursor()

    """*************************************************************************
    BUILD STUDENTS
    *************************************************************************"""
    print(f"BUILD {s_year} STUDENTS")
    funcfile.writelog(f"BUILD {s_year} YEAR STUDENTS")

    # BUILD STUDENT LIST
    print("Build student list...")
    sr_file = "X001_Student"
    s_sql = "CREATE TABLE " + sr_file + " AS " + f"""
    Select
        STUD.KSTUDBUSENTID,
        STUD.DATEQUALLEVELSTARTED,  
        STUD.DATEENROL,  
        STUD.STARTDATE,
        STUD.ENDDATE,
        STUD.MARKSFINALISEDDATE,
        RESU.RESULTISSUEDATE,
        RESU.RESULTPASSDATE,    
        QUAL.QUALIFICATION,
        QUAL.QUALIFICATION_NAME,
        QUAL.QUALIFICATION_TYPE As QUAL_TYPE,
        Upper(ACTI.LONG) AS ACTIVE_IND,  
        Upper(ENTR.LONG) AS ENTRY_LEVEL,
        Upper(BLAC.LONG) AS BLACKLIST,  
        QUAL.ENROL_CATEGORY As ENROL_CAT,
        QUAL.PRESENT_CATEGORY As PRESENT_CAT,
        QUAL.FINAL_STATUS As STATUS_FINAL,
        QUAL.LEVY_CATEGORY,
        QUAL.CERT_TYPE,
        QUAL.LEVY_TYPE,
        QUAL.FOS_SELECTION,
        QUAL.FOS_SELECTION As LONG,
        QUAL.FBUSINESSENTITYID,
        QUAL.SITEID,
        QUAL.CAMPUS,
        QUAL.ORGUNIT_MANAGER,
        QUAL.ORGUNIT_NAME,
        QUAL.ORGUNIT_TYPE,
        RESU.KSTUDQUALFOSRESULTID,
        RESU.DISCONTINUEDATE,
        RESU.FDISCONTINUECODEID,
        RESU.RESULT,
        RESU.DISCONTINUE_REAS,
        RESU.POSTPONE_REAS,
        RESU.FPOSTPONEMENTCODEID,
        RESU.FGRADUATIONCEREMONYID,
        GRAD.CEREMONY,
        GRAD.CEREMONYDATETIME,    
        QUAL.QUALIFICATIONCODE,
        QUAL.QUALIFICATIONFIELDOFSTUDY,
        QUAL.QUALIFICATIONLEVEL,  
        STUD.ENROLACADEMICYEAR,
        STUD.ENROLHISTORYYEAR,  
        QUAL.MIN,
        QUAL.MIN_UNIT,
        QUAL.MAX,
        QUAL.MAX_UNIT,
        STUD.ISHEMISSUBSIDY,
        STUD.ISMAINQUALLEVEL,
        STUD.ISCONDITIONALREG,
        STUD.ISCUMLAUDE,
        STUD.ISPOSSIBLEGRADUATE,
        STUD.FACCEPTANCETESTCODEID,
        QUAL.ISVERIFICATIONREQUIRED,
        QUAL.EXAMSUBMINIMUM,
        QUAL.ISVATAPPLICABLE,
        QUAL.ISPRESENTEDBEFOREAPPROVAL,
        QUAL.ISDIRECTED,
        QUAL.SITEID As FSITEORGUNITNUMBER,
        STUD.KENROLSTUDID,
        QUAL.FQUALLEVELAPID,
        QUAL.KENROLMENTPRESENTATIONID,
        STUD.FENROLMENTPRESENTATIONID,
        QUAL.FOS_KACADEMICPROGRAMID,
        STUD.FPROGRAMAPID,
        QUAL.FENROLMENTCATEGORYCODEID,
        QUAL.FPRESENTATIONCATEGORYCODEID,
        Case
            When STUD.ENROLHISTORYYEAR > 6 Then 6
            Else STUD.ENROLHISTORYYEAR
        End As FEEHISTORYYEAR,
        strftime("%Y", STUD.STARTDATE) - strftime("%Y", STUD.DATEQUALLEVELSTARTED) + 1 As CALCHISTORYYEAR
    From
        QUALLEVELENROLSTUD STUD Left Join
        VSS.X000_Codedescription BLAC ON BLAC.KCODEDESCID = STUD.FBLACKLISTCODEID Left Join
        VSS.X000_Codedescription ACTI ON ACTI.KCODEDESCID = STUD.FSTUDACTIVECODEID Left Join
        VSS.X000_Codedescription ENTR ON ENTR.KCODEDESCID = STUD.FENTRYLEVELCODEID Left Join
        VSS.X000_Qualifications QUAL On QUAL.KENROLMENTPRESENTATIONID = STUD.FENROLMENTPRESENTATIONID Left Join
        VSS.X000_Student_qualfos_result RESU ON RESU.KBUSINESSENTITYID = STUD.KSTUDBUSENTID And
            RESU.KACADEMICPROGRAMID = QUAL.FOS_KACADEMICPROGRAMID And
            Strftime('%Y', RESU.DISCONTINUEDATE) = '{s_year}' Left Join
        Vss.X000_Gradceremony GRAD On GRAD.KGRADUATIONCEREMONYID = RESU.FGRADUATIONCEREMONYID
    Order By
        STUD.KSTUDBUSENTID
    ;"""
    # s_sql = s_sql.replace("%PERIOD%", s_period)
    # s_sql = s_sql.replace("%YEAR%", s_year)
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # MESSAGE
    if funcconf.l_mess_project:
        i = funcsys.tablerowcount(so_curs, sr_file)
        funcsms.send_telegram("", "administrator", "<b>" + str(i) + "</b> Students")

    # Export the data
    if l_export:
        print("Export students all...")
        sr_filet = sr_file
        sx_path = re_path + s_year + "/"
        sx_file = "Student_001_all_"
        s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)
        funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_file, s_head)
        funcfile.writelog("%t EXPORT DATA: "+sx_path+sx_file)

    """*************************************************************************
    BUILD STUDENT MODULES
    *************************************************************************"""
    print(f"BUILD {s_year} STUDENT MODULES")
    funcfile.writelog(f"BUILD {s_year} YEAR STUDENT MODULES")

    # BUILD STUDENT LIST
    print("Build student list...")
    sr_file = "X001_Student_module"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        MENR.KENROLSTUDID,
        MENR.KSTUDBUSENTID,
        MENR.ACADEMICYEAR,
        MENR.DATEENROL,
        MENR.STARTDATE,
        MENR.ENDDATE,
        STUD.FQUALLEVELAPID,
        STUD.QUALIFICATION,
        MODU.FMODULEAPID,
        MODU.MODULE,
        MODU.MODULE_NAME,
        MODU.FENROLMENTCATEGORYCODEID,
        MODU.ENROL_CATEGORY,
        MODU.FPRESENTATIONCATEGORYCODEID,
        MODU.PRESENT_CATEGORY,
        MODU.FCOURSEGROUPCODEID,
        MODU.COURSEGROUP,
        MENR.FMODULETYPECODEID,
        Upper(TYPE.LONG) As MODULE_TYPE,
        MENR.DATEDISCONTINUED,
        MENR.FCOMPLETEREASONCODEID,
        Upper(REAS.LONG) As COMPLETE_REASON,
        Trim(MODR.PART_RESU) As PART_RESU,
        MODR.DATEACHIEVED As DATE_RESU,
        MODU.FBUSINESSENTITYID,
        MODU.SITEID,
        MODU.CAMPUS,
        MODU.ORGUNIT_TYPE,
        MODU.ORGUNIT_NAME,
        MODU.ORGUNIT_MANAGER,
        MENR.ISCONDITIONALREG,
        MENR.ISNEWENROLMENT,
        MENR.ISPROCESSEDONLINE,
        MENR.ISREPEATINGMODULE,
        MENR.ISEXEMPTION,
        MODU.ISEXAMMODULE,
        MODU.ISRESEARCHMODULE,
        MENR.ISDISCOUNTED,
        MODU.EXAMSUBMINIMUM,
        MENR.FSTUDYCENTREMODAPID,
        MENR.FENROLMENTPRESENTATIONID,
        MENR.FEXAMCENTREMODAPID,
        MENR.FPRESENTATIONLANGUAGEID,
        MENR.FMODPERIODENROLPRESCATID,
        MENR.FACKTYPECODEID,
        MENR.FACKSTUDBUSENTID,
        MENR.FACKENROLSTUDID,
        MENR.FACKMODENROLSTUDID,
        MENR.FACKMODSTUDBUSENTID,
        MENR.AUDITDATETIME As MENROL_AUDITDATETIME,
        MENR.FAUDITSYSTEMFUNCTIONID As MENROL_SYSID,
        MENR.FAUDITUSERCODE As MENROL_USERCODE,
        MENR.REGALLOWED,
        MODU.KENROLMENTPRESENTATIONID,
        MODU.COURSECODE,
        MODU.COURSELEVEL,
        MODU.COURSEMODULE,
        MODU.COURSESEMESTER
    From
        MODULEENROLSTUD MENR Inner Join
        VSS.X000_Modules MODU On MODU.KENROLMENTPRESENTATIONID = MENR.FENROLMENTPRESENTATIONID Left Join
        VSS.X000_Codedescription TYPE On TYPE.KCODEDESCID = MENR.FMODULETYPECODEID Left Join
        VSS.X000_Codedescription REAS On REAS.KCODEDESCID = MENR.FCOMPLETEREASONCODEID Left Join
        X001_Student STUD On STUD.KSTUDBUSENTID = MENR.KSTUDBUSENTID And
            STUD.KENROLSTUDID = MENR.FQUALLEVELENROLSTUDID Left Join
        X000_Student_module_result_participate MODR On MODR.KSTUDBUSENTID = MENR.KSTUDBUSENTID And
            MODR.KENROLSTUDID = MENR.KENROLSTUDID     
    ;"""
    # s_sql = s_sql.replace("%PERIOD%", s_period)
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # MESSAGE
    if funcconf.l_mess_project:
        i = funcsys.tablerowcount(so_curs, sr_file)
        funcsms.send_telegram("", "administrator", "<b>" + str(i) + "</b> Modules")

    return
