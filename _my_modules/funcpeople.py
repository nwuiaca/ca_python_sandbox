"""
PEOPLE Functions
Created on: 18 Jun 2021
Author: Albert J v Rensburg (NWU21162395)
"""

# Import python objects
# import sys

# Import own functions
from _my_modules import funccsv
from _my_modules import funcdatn
from _my_modules import funcdatn
from _my_modules import funcfile
from _my_modules import funcsys

# INDEX
"""
Function to build PEOPLE lists on any given date
Function to build ASSIGNMENTS (X000_Assignment) for different date periods
Function to build ASSIGNMENT for different date periods
Function to build PEOPLE table from different assignments
"""


def people_detail_list(
        so_conn,
        s_table: str = 'X000_PEOPLE',
        s_date: str = funcdatn.get_today_date()
        ) -> int:
    """
    Function to build PEOPLE lists on any given date.

    :param so_conn: object: Table connection object
    :param s_table: str: Table name to create (Default=X000_PEOPLE)
    :param s_date: str: List date (Default=Today)
    :return: int: Table row count
    """

    # IMPORT FUNCTIONS
    from _my_modules import funcpayroll

    # DECLARE VARIABLES
    l_debug: bool = False

    # OPEN THE DATABASE CURSOR
    so_curs = so_conn.cursor()

    # BUILD TOTAL ANNUAL PACKAGE
    i_records = funcpayroll.payroll_element_screen_value(
        so_conn,
        'X000_PACKAGE',
        'nwu total_package',
        'annual amount',
        s_date)
    if l_debug:
        print(i_records)

    # BUILD NRF ALLOWANCE
    i_records = funcpayroll.payroll_element_screen_value(
        so_conn,
        'X000_NRF_ALLOWANCE',
        'nwu allowance nrf',
        'option',
        s_date)
    if l_debug:
        print(i_records)

    # BUILD ACTUAL START DATE
    i_records = funcpayroll.payroll_element_screen_value(
        so_conn,
        'X000_LONG_SERVICE_DATE',
        'nwu long service award',
        'long service date',
        s_date)
    if l_debug:
        print(i_records)

    # BUILD PENSIONABLE SALARY
    i_records = funcpayroll.payroll_element_screen_value(
        so_conn,
        'X000_PENSIONABLE_SALARY',
        'nwu pensionable salary',
        'pension ratio',
        s_date)
    if l_debug:
        print(i_records)

    # BUILD GROUP INSURANCE INFORMATION SPOUSE
    i_records = funcpayroll.payroll_element_screen_value(
        so_conn,
        'X000_GROUPINSURANCE_SPOUSE',
        'nwu group insurance information',
        'spouse',
        s_date)
    if l_debug:
        print(i_records)

    # BUILD CURRENT PEOPLE
    if l_debug:
        print("Build people list...")
        print(s_table)
        print(s_date)
    sr_file = s_table
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        papf.employee_number employee_number,
        papf.full_name name_full,
        '' name_list,
        '' name_address,
        papf.attribute4 preferred_name,
        case
            when upper(titl.meaning) = 'MR.' then 'MR'
            when upper(titl.meaning) = 'MRS.' then 'MRS'
            when upper(titl.meaning) = 'MS.' then 'MS'
            else upper(titl.meaning)
        end as title,
        case
            when instr(papf.middle_names,' ') > 1 then
                substr(papf.first_name,1,1) ||
                 substr(papf.middle_names,1,1) ||
                  trim(substr(papf.middle_names,instr(papf.middle_names,' '),2))
            when length(papf.middle_names) > 0 then
                substr(papf.first_name,1,1) || substr(papf.middle_names,1,1)
            else substr(papf.first_name,1,1)
        end as initials,
        papf.last_name name_last,
        papf.date_of_birth date_of_birth,
        strftime('%Y','%DATE%') - strftime('%Y', papf.date_of_birth) as employee_age,
        upper(sex.meaning) gender,
        upper(race.meaning) race,
        case
            when papf.attribute3 = 'A' then 'AFRIKAANS'
            else 'ENGLISH'
        end as corr_language,
        upper(mar.meaning) marital_status,
        upper(dis.meaning) disabled,
        papf.per_information9 is_foreign,        
        upper(nat.meaning) nationality,
        upper(pan.meaning) nationality_passport,        
        papf.national_identifier national_identifier,
        upper(papf.per_information2) passport,
        upper(papf.per_information3) permit,
        Replace(Substr(papf.per_information8,1,10),'/','-') permit_expire,
        ph.phone_number phone_work,
        ho.phone_number phone_home,        
        mo.phone_number phone_mobile,
        papf.email_address email_address,
        papf.attribute2 internal_box,
        sars.address_sars address_sars,
        post.address_post address_post,
        Replace(Substr(lsd.element_value,1,10),'/','-') date_started,
        papf.effective_start_date people_start_date,
        papf.effective_end_date people_end_date,    
        paaf.effective_start_date assign_start_date,
        paaf.effective_end_date assign_end_date,
        pps.date_start service_start_date,
        pps.actual_termination_date service_end_date,
        pps.leaving_reason end_reason,
        upper(ler.meaning) leaving_reason,
        papf.person_id person_id,
        paaf.assignment_id assignment_id,
        paaf.assignment_number assignment_number,
        upper(cat.meaning) assignment_category,
        case
            when paaf.position_id = 0 then paaf.EMPLOYEE_CATEGORY
            else hrp.acad_supp
        end as employee_category,
        upper(ppt.user_person_type) user_person_type,
        pg.grade grade,
        pg.grade_calc grade_calc,       
        upper(pg.grade_name) grade_name,
        ppg.segment1 leave_code,
        paaf.ass_attribute1 type_of_shift,
        paaf.ass_attribute2 joint_appt,
        paaf.organization_id organization_id,
        upper(hrl.location_code) location,
        upper(hos.division) division,
        upper(hos.faculty) faculty,     
        hro.oe_code oe_code,
        upper(hro.org_name) organization,
        upper(hro.org_type) organization_type,
        upper(hro.org_type_desc) organization_description,
        hro.org_head_person_id org_head_person_id,
        head.employee_number oe_head_number,
        head.full_name oe_head_name_name,
        sup.line_manager supervisor_number,
        '' supervisor_name,
        paaf.position_id position_id,
        hrp.position position,
        hrp.max_persons max_persons,
        upper(hrp.position_name) position_name,
        hrp.parent_position_id parent_position_id,
        upper(pj.job_name) job_name,
        upper(pj.job_segment_name) job_segment_name,
        ppb.name salary_basis,
        cast((pack.element_value*3)/17 As Real) annual_package,
        hrp.attribute1 account_cost,
        upper(hrp.attribute2) account_allocate,
        cast(hrp.attribute3 as int) account_part,
        cast(pes.element_value As Real) pension_ratio,
        nrf.element_value nrf_rated,
        --max(acc.personal_payment_method_id) As account_id,
        acc.org_payment_method_name As account_pay_method,
        acc.acc_type account_type,
        acc.acc_branch account_branch,
        acc.acc_number account_number,
        acc.acc_relation account_relation,
        acc.ppm_information1 account_sars,
        papf.last_update_date people_update_date,
        papf.last_updated_by people_update_by,
        paaf.last_update_date assignment_update_date,
        paaf.last_updated_by assignment_update_by,
        1 As customer
    FROM
        per_all_people_f papf left join
        per_all_assignments_f paaf on paaf.person_id = papf.person_id left join
        per_periods_of_service pps on pps.person_id = papf.person_id and
            paaf.effective_end_date between pps.date_start and
             ifnull(pps.actual_termination_date, '4712-12-31') left join
        X000_positions hrp on hrp.position_id = paaf.position_id and 
            paaf.effective_end_date between hrp.effective_start_date and hrp.effective_end_date left join
        x000_organization hro on hro.organization_id = paaf.organization_id left join
        x000_organization_struct hos on hos.org1 = paaf.organization_id left join 
        hr_locations_all hrl on hrl.location_id = paaf.location_id left join
        -- per_all_people_f sup on sup.person_id = paaf.supervisor_id and
        -- paaf.effective_end_date between sup.effective_start_date and sup.effective_end_date left join
        xxnwu_line_managers sup on sup.employee_number = papf.employee_number left join
        per_all_people_f head on head.person_id = hro.org_head_person_id and
            paaf.effective_end_date between head.effective_start_date and head.effective_end_date left join        
        per_person_type_usages_f pptuf on pptuf.person_id = papf.person_id and
            paaf.effective_end_date between pptuf.effective_start_date and pptuf.effective_end_date left join
        per_person_types ppt on pptuf.person_type_id = ppt.person_type_id left join
        x000_grades pg on pg.grade_id = paaf.grade_id and
            paaf.effective_end_date between pg.date_from and ifnull(pg.date_to, '4712-12-31') left join
        x000_jobs pj on pj.job_id = hrp.job_id and
            hrp.effective_end_date between pj.date_from and ifnull(pj.date_to, '4712-12-31') left join
        pay_people_groups ppg on ppg.people_group_id = paaf.people_group_id and
            ppg.enabled_flag = 'Y' left join
        per_pay_bases ppb on ppb.pay_basis_id = paaf.pay_basis_id left join
        hr_lookups cat on cat.lookup_type = 'EMP_CAT' and cat.lookup_code = paaf.employment_category left join
        hr_lookups titl on titl.lookup_type = 'TITLE' and titl.lookup_code = papf.title left join
        hr_lookups sex on sex.lookup_type = 'SEX' and sex.lookup_code = papf.sex and sex.enabled_flag = 'Y' left join
        hr_lookups nat on nat.lookup_type = 'NATIONALITY' and nat.lookup_code = papf.nationality left join
        hr_lookups pan on pan.lookup_type = 'NATIONALITY' and pan.lookup_code = papf.per_information10 left join        
        hr_lookups race on race.lookup_type = 'ZA_RACE' and race.lookup_code = papf.per_information4 and
         race.enabled_flag = 'Y' left join
        hr_lookups mar on mar.lookup_type = 'MAR_STATUS' and mar.lookup_code = papf.marital_status and
         mar.enabled_flag = 'Y' left join
        hr_lookups dis on dis.lookup_type = 'REGISTERED_DISABLED' and
         dis.lookup_code = papf.registered_disabled_flag left join
        hr_lookups ler on ler.lookup_type = 'LEAV_REAS' and ler.lookup_code = pps.leaving_reason left join
        X000_phone_work_latest ph on ph.parent_id = papf.person_id left join
        X000_phone_home_latest ho on ho.parent_id = papf.person_id left join        
        x000_phone_mobile_latest mo on mo.parent_id = papf.person_id left join
        x000_package pack on pack.employee_number = papf.employee_number left join
        x000_nrf_allowance nrf on nrf.employee_number = papf.employee_number left join
        x000_long_service_date lsd on lsd.employee_number = papf.employee_number left join
        x000_pensionable_salary pes on pes.employee_number = papf.employee_number left join
        x000_pay_accounts_latest acc on acc.assignment_id = paaf.assignment_id left join
        -- acc.org_payment_method_id = 61 and        
        -- paaf.effective_end_date between acc.effective_start_date and acc.effective_end_date left join
        x000_address_sars sars on sars.person_id = papf.person_id and
            paaf.effective_end_date between sars.date_from and sars.date_to left join
        x000_address_post post on post.person_id = papf.person_id and
            paaf.effective_end_date between post.date_from and post.date_to
    WHERE
        paaf.assignment_status_type_id in (1) and
        strftime('%Y-%m-%d', '%DATE%') between paaf.effective_start_date and paaf.effective_end_date and
        paaf.effective_end_date between papf.effective_start_date and papf.effective_end_date and
        ppt.user_person_type != 'Retiree'
    ORDER BY
        papf.employee_number    
    ;"""
    s_sql = s_sql.replace("%DATE%", s_date)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)
    so_curs.execute("Update " + sr_file + " Set name_list = name_last||' '||title||' '||initials;")
    so_conn.commit()
    so_curs.execute("Update " + sr_file + " Set name_address = title||' '||initials||' '||name_last;")
    so_conn.commit()
    # Update the supervisor name
    l_sql = f"""
    Select
        peop.employee_number,
        supe.name_full as supervisor
    From
        {s_table} peop Inner Join
        {s_table} supe On supe.employee_number = peop.supervisor_number
    """
    s_sql = f"Update {sr_file} Set supervisor_name = name.supervisor From ({l_sql}) As name Where {s_table}.employee_number = name.employee_number;"
    so_curs.execute(s_sql)
    so_conn.commit()

    # RETURN VALUE
    i_return = funcsys.tablerowcount(so_curs, s_table)

    return i_return


def people_detail_list2(
        so_conn,
        s_table: str = 'X000_PEOPLE2',
        s_date: str = funcdatn.get_today_date()
        ) -> int:
    """
    Function to build PEOPLE lists on any given date.

    :param so_conn: object: Table connection object
    :param s_table: str: Table name to create (Default=X000_PEOPLE)
    :param s_date: str: List date (Default=Today)
    :return: int: Table row count
    """

    # IMPORT FUNCTIONS
    from _my_modules import funcpayroll

    # DECLARE VARIABLES
    l_debug: bool = False

    # OPEN THE DATABASE CURSOR
    so_curs = so_conn.cursor()

    # BUILD CURRENT PEOPLE
    if l_debug:
        print("Build people list 2...")
        print(s_table)
        print(s_date)
    sr_file = s_table
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        pap.EMPLOYEE_NUMBER As employee_number,
        pap.PERSON_ID As person_id,
        paa.ASSIGNMENT_ID As assignment_id,
        pap.full_name name_full,
        '' name_list,
        '' name_address,
        pap.attribute4 preferred_name,
        case
            when upper(tit.meaning) = 'MR.' then 'MR'
            when upper(tit.meaning) = 'MRS.' then 'MRS'
            when upper(tit.meaning) = 'MS.' then 'MS'
            else upper(tit.meaning)
        end as title,
        case
            when instr(pap.middle_names,' ') > 1 then
                substr(pap.first_name,1,1) ||
                 substr(pap.middle_names,1,1) ||
                  trim(substr(pap.middle_names,instr(pap.middle_names,' '),2))
            when length(pap.middle_names) > 0 then
                substr(pap.first_name,1,1) || substr(pap.middle_names,1,1)
            else substr(pap.first_name,1,1)
        end as initials,
        pap.last_name name_last,
        pap.date_of_birth date_of_birth,
        strftime('%Y','%DATE%') - strftime('%Y', pap.date_of_birth) as employee_age,
        Upper(hrl.LOCATION_CODE) As location,
        Max(pps.ACTUAL_TERMINATION_DATE) As service_end_date,
        Upper(ppt.USER_PERSON_TYPE) As user_person_type,
        Upper(cat.MEANING) As assignment_category,
        Case
            When paa.POSITION_ID = 0
            Then Upper(paa.EMPLOYEE_CATEGORY)
            Else Upper(hrp.ACAD_SUPP)
        End As employee_category,
        Upper(hrp.POSITION_NAME) position_name,
        Upper(nat.meaning) nationality,
        Upper(pan.meaning) nationality_passport,      
        pap.PER_INFORMATION9 As is_foreign,
        pap.NATIONAL_IDENTIFIER As national_identifier,
        Upper(pap.PER_INFORMATION2) passport,
        Upper(pap.PER_INFORMATION3) permit,
        Replace(Substr(pap.PER_INFORMATION8,1,10),'/','-') permit_expire,
        acc.ORG_PAYMENT_METHOD_NAME As account_pay_method,
        pap.last_update_date people_update_date,
        pap.last_updated_by people_update_by,        
        paa.last_update_date assignment_update_date,
        paa.last_updated_by assignment_update_by,
        sup.line_manager supervisor_number,
        '' supervisor_name                   
    From
        PER_ALL_PEOPLE_F pap Left Join
        PER_ALL_ASSIGNMENTS_F paa On paa.PERSON_ID = pap.PERSON_ID
                -- And Date() Between paa.EFFECTIVE_START_DATE And paa.EFFECTIVE_END_DATE
                And '%DATE%' Between paa.EFFECTIVE_START_DATE And paa.EFFECTIVE_END_DATE
                And paa.EFFECTIVE_END_DATE Between pap.EFFECTIVE_START_DATE And pap.EFFECTIVE_END_DATE
                And paa.ASSIGNMENT_STATUS_TYPE_ID = 1 Left Join
        PER_PERIODS_OF_SERVICE pps On pps.PERSON_ID = pap.PERSON_ID Left Join
        PER_PERSON_TYPE_USAGES_F ptu On pap.PERSON_ID = ptu.PERSON_ID
                And paa.EFFECTIVE_END_DATE Between ptu.EFFECTIVE_START_DATE And ptu.EFFECTIVE_END_DATE Left Join
        PER_PERSON_TYPES ppt On ptu.PERSON_TYPE_ID = ppt.PERSON_TYPE_ID Left Join
        HR_LOCATIONS_ALL hrl On hrl.LOCATION_ID = paa.LOCATION_ID Left Join
        HR_LOOKUPS cat On cat.LOOKUP_CODE = paa.EMPLOYMENT_CATEGORY
                And cat.LOOKUP_TYPE = 'EMP_CAT' Left Join
        X000_POSITIONS hrp On hrp.POSITION_ID = paa.POSITION_ID
                And paa.EFFECTIVE_END_DATE Between hrp.EFFECTIVE_START_DATE And hrp.EFFECTIVE_END_DATE Left join
        HR_LOOKUPS tit on tit.lookup_type = 'TITLE' and tit.lookup_code = pap.title Left join    
        HR_LOOKUPS nat on nat.lookup_type = 'NATIONALITY' and nat.lookup_code = pap.nationality Left join
        HR_LOOKUPS pan on pan.lookup_type = 'NATIONALITY' and pan.lookup_code = pap.per_information10 Left join
        X000_pay_accounts_latest acc on acc.assignment_id = paa.assignment_id Left Join
        XXNWU_LINE_MANAGERS sup on sup.employee_number = pap.employee_number
    Where
        (paa.EFFECTIVE_END_DATE Between pps.DATE_START And pps.ACTUAL_TERMINATION_DATE And
            ppt.USER_PERSON_TYPE != 'Retiree') Or
        (ppt.USER_PERSON_TYPE != 'Retiree' And
            pps.ACTUAL_TERMINATION_DATE == pps.DATE_START)
    Group By
        pap.EMPLOYEE_NUMBER
    ;"""
    s_sql = s_sql.replace("%DATE%", s_date)
    so_curs.execute(s_sql)
    so_conn.commit()

    # Update the names
    so_curs.execute("Update " + sr_file + " Set name_list = name_last||' '||title||' '||initials;")
    so_conn.commit()
    so_curs.execute("Update " + sr_file + " Set name_address = title||' '||initials||' '||name_last;")
    so_conn.commit()

    # Update the supervisor name
    l_sql = """
    Select
        peop.employee_number,
        supe.name_full as supervisor
    From
        X000_PEOPLE2 peop Inner Join
        X000_PEOPLE2 supe On supe.employee_number = peop.supervisor_number
    """
    s_sql = "Update " + \
            sr_file + \
            " Set supervisor_name = name.supervisor From (" + l_sql + \
            ") As name Where X000_PEOPLE2.employee_number = name.employee_number;"
    so_curs.execute(s_sql)
    so_conn.commit()

    # RETURN VALUE
    i_return = funcsys.tablerowcount(so_curs, s_table)

    return i_return


def assign01(so_conn, s_table, s_from, s_to, s_on, s_mess):
    """
    Function to build ASSIGNMENTS (X000_Assignment) for different date periods
    :param so_conn: Connection string
    :param s_table: Table name to create
    :param s_from: Period start date
    :param s_to: Period end date
    :param s_on: On which date
    :param s_mess: Print message
    :return:
    """

    # Print and connect
    print(s_mess)
    so_curs = so_conn.cursor()

    # Build the table
    s_sql = "CREATE TABLE " + s_table + " AS" + """
    SELECT
      ASSI.ASS_ID,
      ASSI.PERSON_ID,
      ASSI.ASSIGNMENT_NUMBER As ASS_NUMB,
      ASSI.SERVICE_DATE_START As EMP_START,
      ASSI.EFFECTIVE_START_DATE As ASS_START,
      ASSI.EFFECTIVE_END_DATE As ASS_END,
      ASSI.SERVICE_DATE_ACTUAL_TERMINATION As EMP_END,
      ASSI.LEAVING_REASON,
      ASSI.LEAVE_REASON_DESCRIP,
      ASSI.LOCATION_DESCRIPTION,  
      ASSI.ORG_TYPE_DESC,
      ASSI.OE_CODE,
      ASSI.ORG_NAME,  
      ASSI.FACULTY,
      ASSI.DIVISION,      
      ASSI.GRADE,  
      ASSI.GRADE_NAME,
      ASSI.GRADE_CALC,
      ASSI.POSITION_ID,
      ASSI.POSITION,
      ASSI.POSITION_NAME,
      ASSI.JOB_NAME,
      ASSI.JOB_SEGMENT_NAME,
      ASSI.ACAD_SUPP,
      ASSI.EMPLOYMENT_CATEGORY,
      ASSI.LEAVE_CODE,
      ASSI.SUPERVISOR_ID,  
      ASSI.ASS_WEEK_LEN,
      ASSI.ASS_ATTRIBUTE2,
      ASSI.PRIMARY_FLAG,
      ASSI.MAILTO      
    FROM
      X000_PER_ALL_ASSIGNMENTS ASSI
    WHERE
      (ASSI.EFFECTIVE_END_DATE >= Date('%FROM%') AND
      ASSI.EFFECTIVE_END_DATE <= Date('%TO%')) OR
      (ASSI.EFFECTIVE_START_DATE >= Date('%FROM%') AND
      ASSI.EFFECTIVE_START_DATE <= Date('%TO%')) OR
      (ASSI.EFFECTIVE_END_DATE >= Date('%FROM%') AND
      ASSI.EFFECTIVE_START_DATE <= Date('%TO%'))
    ORDER BY
      ASSI.ASSIGNMENT_NUMBER,
      ASSI.EFFECTIVE_START_DATE
    """
    s_sql = s_sql.replace("%FROM%", s_from)
    s_sql = s_sql.replace("%TO%", s_to)
    so_curs.execute("DROP TABLE IF EXISTS " + s_table)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + s_table)

    # Add column assignment lookup date
    if "DATE_ASS_LOOKUP" not in funccsv.get_colnames_sqlite(so_curs, s_table):
        so_curs.execute("ALTER TABLE " + s_table + " ADD COLUMN DATE_ASS_LOOKUP TEXT;")
        s_sql = "UPDATE " + s_table + """
                        SET DATE_ASS_LOOKUP = 
                        CASE
                           WHEN ASS_END > Date('%TO%') THEN Date('%TO%')
                           ELSE ASS_END
                        END
                        ;"""
        s_sql = s_sql.replace("%TO%", s_on)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t ADD COLUMN: DATE_ASS_LOOKUP")

    # Add column is assignment active
    if "ASS_ACTIVE" not in funccsv.get_colnames_sqlite(so_curs, s_table):
        so_curs.execute("ALTER TABLE " + s_table + " ADD COLUMN ASS_ACTIVE TEXT;")
        s_sql = "UPDATE " + s_table + """
            SET ASS_ACTIVE = 
            CASE
                WHEN ORG_TYPE_DESC = 'Parent Organisation' THEN 'O'
                WHEN INSTR(POSITION_NAME,'Pensioner') > 0 THEN 'P'
                WHEN ASS_START <= Date('%ON%') AND ASS_END >= Date('%ON%') THEN 'Y'                           
                ELSE 'N'
            END
            ;"""
        s_sql = s_sql.replace("%ON%", s_on)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t ADD COLUMN: ASS_ACTIVE")

    # Add column people lookup
    if "DATE_EMP_LOOKUP" not in funccsv.get_colnames_sqlite(so_curs, s_table):
        so_curs.execute("ALTER TABLE " + s_table + " ADD COLUMN DATE_EMP_LOOKUP TEXT;")
        s_sql = "UPDATE " + s_table + """
                        SET DATE_EMP_LOOKUP = 
                        CASE
                           WHEN EMP_START = EMP_END AND LEAVING_REASON = '' THEN Date('%TO%')
                           WHEN EMP_END > Date('%TO%') THEN Date('%TO%')
                           ELSE EMP_END
                        END
                        ;"""
        s_sql = s_sql.replace("%TO%", s_on)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t ADD COLUMN: DATE_EMP_LOOKUP")

    # Add column is people active
    if "EMP_ACTIVE" not in funccsv.get_colnames_sqlite(so_curs, s_table):
        so_curs.execute("ALTER TABLE " + s_table + " ADD COLUMN EMP_ACTIVE TEXT;")
        s_sql = "UPDATE " + s_table + """
            SET EMP_ACTIVE = 
            CASE
                WHEN ASS_ACTIVE = 'O' AND EMP_START <= Date('%ON%') AND DATE_EMP_LOOKUP >= Date('%ON%')
                    THEN 'O'
                WHEN ASS_ACTIVE = 'P' AND EMP_START <= Date('%ON%') AND DATE_EMP_LOOKUP >= Date('%ON%')
                    THEN 'P'
                WHEN ASS_ACTIVE = 'Y' AND EMP_START <= Date('%ON%') AND DATE_EMP_LOOKUP >= Date('%ON%')
                    THEN 'Y'
                ELSE 'N'
            END
            ;"""
        s_sql = s_sql.replace("%ON%", s_on)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t ADD COLUMN: EMP_ACTIVE")

    return


# 18 Build current assignment round 2 ******************************************

def assign02(so_conn, s_table, s_source, s_mess):
    """
    Function to build ASSIGNMENT for different date periods
    :param so_conn: Connection string
    :param s_table: Table name to create
    :param s_source: Source table
    :param s_mess: Print message
    :return: Nothing
    """

    # Print and connect
    print(s_mess)
    so_curs = so_conn.cursor()

    # Build the table
    s_sql = "CREATE TABLE " + s_table + " AS" + """
    SELECT
      X000_PER_ALL_PEOPLE.EMPLOYEE_NUMBER,
      ASSI.ASS_ID,
      ASSI.PERSON_ID,
      ASSI.ASS_NUMB,
      X000_PER_ALL_PEOPLE.FULL_NAME,
      X000_PER_ALL_PEOPLE.KNOWN_NAME,
      X000_PER_ALL_PEOPLE.DATE_OF_BIRTH,
      ASSI.EMP_START,
      ASSI.ASS_START,
      ASSI.ASS_END,
      ASSI.EMP_END,
      ASSI.LEAVING_REASON,
      ASSI.LEAVE_REASON_DESCRIP,
      ASSI.LOCATION_DESCRIPTION,
      ASSI.ORG_TYPE_DESC,
      ASSI.OE_CODE,
      ASSI.GRADE,
      ASSI.GRADE_NAME,
      ASSI.GRADE_CALC,
      ASSI.POSITION_ID,
      ASSI.POSITION,
      ASSI.POSITION_NAME,
      ASSI.ORG_NAME,
      ASSI.JOB_NAME,
      ASSI.JOB_SEGMENT_NAME,
      ASSI.ACAD_SUPP,
      ASSI.FACULTY,
      ASSI.DIVISION,      
      ASSI.EMPLOYMENT_CATEGORY,
      ASSI.LEAVE_CODE,
      ASSI.SUPERVISOR_ID,
      X000_PER_ALL_PEOPLE1.EMPLOYEE_NUMBER As SUPERVISOR,
      ASSI.ASS_WEEK_LEN,
      ASSI.ASS_ATTRIBUTE2,
      X000_PER_ALL_PEOPLE.NATIONALITY,
      X000_PER_ALL_PEOPLE.NATIONALITY_NAME,
      X000_PER_ALL_PEOPLE.USER_PERSON_TYPE,
      ASSI.PRIMARY_FLAG,
      X000_PER_ALL_PEOPLE.CURRENT_EMPLOYEE_FLAG,
      ASSI.DATE_ASS_LOOKUP,
      ASSI.ASS_ACTIVE,
      ASSI.DATE_EMP_LOOKUP,
      ASSI.EMP_ACTIVE,
      X000_COUNTS.COUNT_ASS,
      X000_COUNTS.COUNT_PEO,
      X000_COUNTS.COUNT_POS,
      ASSI.MAILTO,
      BANK.ACC_TYPE,
      BANK.ACC_BRANCH,
      BANK.ACC_NUMBER,
      BANK.ACC_RELATION,
      BANK.PPM_INFORMATION1 As ACC_SARS,
      SEC.SEC_FULLPART_FLAG
    FROM
      %SOURCET% ASSI Left Join
      X000_PER_ALL_PEOPLE ON X000_PER_ALL_PEOPLE.PERSON_ID = ASSI.PERSON_ID AND
        X000_PER_ALL_PEOPLE.EFFECTIVE_START_DATE <= ASSI.DATE_ASS_LOOKUP AND
        X000_PER_ALL_PEOPLE.EFFECTIVE_END_DATE >= ASSI.DATE_ASS_LOOKUP Left Join
      X000_PER_ALL_PEOPLE X000_PER_ALL_PEOPLE1 ON X000_PER_ALL_PEOPLE1.PERSON_ID = ASSI.SUPERVISOR_ID AND
        X000_PER_ALL_PEOPLE1.EFFECTIVE_START_DATE <= ASSI.DATE_ASS_LOOKUP AND
        X000_PER_ALL_PEOPLE1.EFFECTIVE_END_DATE >= ASSI.DATE_ASS_LOOKUP Left Join
      X000_COUNTS ON X000_COUNTS.PERSON_ID = ASSI.PERSON_ID Left Join
      X000_PAY_ACCOUNTS BANK ON BANK.ASSIGNMENT_ID = ASSI.ASS_ID AND
        BANK.ORG_PAYMENT_METHOD_ID = 61 AND
        BANK.EFFECTIVE_START_DATE <= ASSI.DATE_ASS_LOOKUP AND
        BANK.EFFECTIVE_END_DATE >= ASSI.DATE_ASS_LOOKUP Left Join
      X001_ASSIGNMENT_SEC_CURR_YEAR SEC ON SEC.ASSIGNMENT_ID = ASSI.ASS_ID AND
        SEC.SEC_DATE_FROM <= ASSI.DATE_ASS_LOOKUP AND
        SEC.SEC_DATE_TO >= ASSI.DATE_ASS_LOOKUP
    ORDER BY
      X000_PER_ALL_PEOPLE.EMPLOYEE_NUMBER,
      ASSI.EMP_START
    """
    so_curs.execute("DROP TABLE IF EXISTS " + s_table)
    s_sql = s_sql.replace("%SOURCET%", s_source)
    so_curs.execute(s_sql)
    so_conn.commit()
    # so_curs.execute("DROP TABLE IF EXISTS " + s_source)
    funcfile.writelog("%t BUILD TABLE: " + s_table)
    return


def people01(so_conn, s_table, s_source, s_peri, s_mess, s_acti):
    """
    Function to build PEOPLE table from different assignments
    :param so_conn: Connection string
    :param s_table: Table name to create
    :param s_source: Table source
    :param s_peri: For which period
    :param s_mess: Print and log message
    :param s_acti: Should list include only active people = Y (or active assignments = N)
    :return: int: Number of people
    """

    # Print and connect
    print(s_mess)
    so_curs = so_conn.cursor()

    # Use assignment or people date
    if s_acti == "Y":
        s_wher = "ASSI.EMP_ACTIVE = 'Y'"
    else:
        s_wher = "ASSI.ASS_ACTIVE = 'Y'"

    # Create the people table
    s_sql = "CREATE TABLE " + s_table + " As " + """
    Select
      ASSI.EMPLOYEE_NUMBER,
      ASSI.ASS_ID,
      ASSI.PERSON_ID,
      ASSI.ASS_NUMB,
      X000_PER_ALL_PEOPLE.PARTY_ID,
      Upper(ASSI.FULL_NAME) As FULL_NAME,
      '' As NAME_LIST,
      '' As NAME_ADDR,
      Upper(ASSI.KNOWN_NAME) As KNOWN_NAME,
      CASE
         WHEN ORG_NAME IS NULL THEN OE_CODE||': '||POSITION_NAME
         ELSE ORG_NAME||': '||POSITION_NAME
      END AS POSITION_FULL,
      ASSI.DATE_OF_BIRTH,
      Upper(X000_PER_ALL_PEOPLE.NATIONALITY) As NATIONALITY,
      Upper(X000_PER_ALL_PEOPLE.NATIONALITY_NAME) As NATIONALITY_NAME,
      X000_PER_ALL_PEOPLE.NATIONAL_IDENTIFIER As IDNO,
      Upper(X000_PER_ALL_PEOPLE.PER_INFORMATION2) As PASSPORT,
      Upper(X000_PER_ALL_PEOPLE.PER_INFORMATION3) As PERMIT,
      X000_PER_ALL_PEOPLE.PER_INFORMATION8 As PERMIT_EXPIRE,
      X000_PER_ALL_PEOPLE.TAX_NUMBER,
      Case
          When X000_PER_ALL_PEOPLE.SEX = 'F' Then 'FEMALE'
          When X000_PER_ALL_PEOPLE.SEX = 'M' Then 'MALE'
          Else 'OTHER'
      End As SEX,
      X000_PER_ALL_PEOPLE.MARITAL_STATUS,
      X000_PER_ALL_PEOPLE.REGISTERED_DISABLED_FLAG As DISABLED,
      X000_PER_ALL_PEOPLE.RACE_CODE,
      Upper(X000_PER_ALL_PEOPLE.RACE_DESC) As RACE_DESC,
      X000_PER_ALL_PEOPLE.LANG_CODE,
      Upper(X000_PER_ALL_PEOPLE.LANG_DESC) As LANG_DESC,
      X000_PER_ALL_PEOPLE.INT_MAIL,
      Lower(X000_PER_ALL_PEOPLE.EMAIL_ADDRESS) As EMAIL_ADDRESS,
      X000_PER_ALL_PEOPLE.CURRENT_EMPLOYEE_FLAG As CURR_EMPL_FLAG,
      X000_PER_ALL_PEOPLE.USER_PERSON_TYPE,
      ASSI.ASS_START,
      ASSI.ASS_END,
      ASSI.EMP_START,
      ASSI.EMP_END,
      ASSI.LEAVING_REASON,
      Upper(ASSI.LEAVE_REASON_DESCRIP) As LEAVE_REASON_DESCRIP,
      Upper(ASSI.LOCATION_DESCRIPTION) As LOCATION_DESCRIPTION,
      Upper(ASSI.ORG_TYPE_DESC) As ORG_TYPE_DESC,
      Upper(ASSI.OE_CODE) As OE_CODE,
      Upper(ASSI.ORG_NAME) As ORG_NAME,
      ASSI.PRIMARY_FLAG,
      Case
        When Upper(ASSI.ACAD_SUPP) = 'ACADEMIC' Then Upper(ASSI.ACAD_SUPP)
        When Upper(ASSI.ACAD_SUPP) = 'SUPPORT' Then Upper(ASSI.ACAD_SUPP)
        Else 'OTHER'
      End as ACAD_SUPP,
      Upper(ASSI.FACULTY) As FACULTY,
      Upper(ASSI.DIVISION) As DIVISION,
      Case
          When EMPLOYMENT_CATEGORY = 'P' Then 'PERMANENT'
          When EMPLOYMENT_CATEGORY = 'T' Then 'TEMPORARY'
          Else 'OTHER'
      End As EMPLOYMENT_CATEGORY,
      ASSI.ASS_WEEK_LEN,
      ASSI.LEAVE_CODE,
      ASSI.GRADE,
      Upper(ASSI.GRADE_NAME) As GRADE_NAME,
      ASSI.GRADE_CALC,
      ASSI.POSITION,
      Upper(ASSI.POSITION_NAME) As POSITION_NAME,
      Upper(ASSI.JOB_NAME) As JOB_NAME,
      Upper(ASSI.JOB_SEGMENT_NAME) As JOB_SEGMENT_NAME,
      ASSI.SUPERVISOR,
      X000_PER_ALL_PEOPLE.TITLE_FULL,
      X000_PER_ALL_PEOPLE.FIRST_NAME,
      X000_PER_ALL_PEOPLE.MIDDLE_NAMES,
      X000_PER_ALL_PEOPLE.LAST_NAME,
      X000_PHONE_WORK_%PERIOD%_LIST.PHONE_WORK,
      X000_PHONE_MOBI_%PERIOD%_LIST.PHONE_MOBI,
      X000_PHONE_HOME_%PERIOD%_LIST.PHONE_HOME,
      X000_ADDRESS_SARS.ADDRESS_SARS,
      X000_ADDRESS_POST.ADDRESS_POST,
      X000_ADDRESS_HOME.ADDRESS_HOME,
      X000_ADDRESS_OTHE.ADDRESS_OTHE,
      ASSI.COUNT_POS,
      ASSI.COUNT_ASS,
      ASSI.COUNT_PEO,
      ASSI.DATE_ASS_LOOKUP,
      ASSI.ASS_ACTIVE,
      ASSI.DATE_EMP_LOOKUP,
      ASSI.EMP_ACTIVE,      
      ASSI.MAILTO,
      PER_PAY_PROPOSALS.PROPOSED_SALARY_N,
      Upper(X000_PER_PEOPLE_TYPES.USER_PERSON_TYPE) As PERSON_TYPE,
      Upper(ASSI.ACC_TYPE) As ACC_TYPE,
      Upper(ASSI.ACC_BRANCH) As ACC_BRANCH,
      ASSI.ACC_NUMBER,
      Upper(ASSI.ACC_RELATION) As ACC_RELATION,
      ASSI.ACC_SARS,
      ASSI.SEC_FULLPART_FLAG
    FROM
      %SOURCET% ASSI
      LEFT JOIN X000_PER_ALL_PEOPLE ON X000_PER_ALL_PEOPLE.PERSON_ID = ASSI.PERSON_ID AND
        X000_PER_ALL_PEOPLE.EFFECTIVE_START_DATE <= ASSI.DATE_EMP_LOOKUP AND
        X000_PER_ALL_PEOPLE.EFFECTIVE_END_DATE >= ASSI.DATE_EMP_LOOKUP
      LEFT JOIN X000_PHONE_WORK_%PERIOD%_LIST ON X000_PHONE_WORK_%PERIOD%_LIST.PERSON_ID = ASSI.PERSON_ID
      LEFT JOIN X000_PHONE_MOBI_%PERIOD%_LIST ON X000_PHONE_MOBI_%PERIOD%_LIST.PERSON_ID = ASSI.PERSON_ID
      LEFT JOIN X000_PHONE_HOME_%PERIOD%_LIST ON X000_PHONE_HOME_%PERIOD%_LIST.PERSON_ID = ASSI.PERSON_ID
      LEFT JOIN X000_ADDRESS_SARS ON X000_ADDRESS_SARS.PERSON_ID = ASSI.PERSON_ID AND
        X000_ADDRESS_SARS.DATE_FROM <= ASSI.DATE_EMP_LOOKUP AND X000_ADDRESS_SARS.DATE_TO >=
        ASSI.DATE_EMP_LOOKUP
      LEFT JOIN X000_ADDRESS_POST ON X000_ADDRESS_POST.PERSON_ID = ASSI.PERSON_ID AND
        X000_ADDRESS_POST.DATE_FROM <= ASSI.DATE_EMP_LOOKUP AND X000_ADDRESS_POST.DATE_TO >=
        ASSI.DATE_EMP_LOOKUP
      LEFT JOIN X000_ADDRESS_HOME ON X000_ADDRESS_HOME.PERSON_ID = ASSI.PERSON_ID AND
        X000_ADDRESS_HOME.DATE_FROM <= ASSI.DATE_EMP_LOOKUP AND X000_ADDRESS_HOME.DATE_TO >=
        ASSI.DATE_EMP_LOOKUP
      LEFT JOIN X000_ADDRESS_OTHE ON X000_ADDRESS_OTHE.PERSON_ID = ASSI.PERSON_ID AND
        X000_ADDRESS_OTHE.DATE_FROM <= ASSI.DATE_EMP_LOOKUP AND
        X000_ADDRESS_OTHE.DATE_TO >= ASSI.DATE_EMP_LOOKUP
      LEFT JOIN PER_PAY_PROPOSALS ON PER_PAY_PROPOSALS.ASSIGNMENT_ID = ASSI.ASS_ID AND
        PER_PAY_PROPOSALS.CHANGE_DATE <= ASSI.DATE_EMP_LOOKUP AND
        PER_PAY_PROPOSALS.DATE_TO >= ASSI.DATE_EMP_LOOKUP
      LEFT JOIN X000_PER_PEOPLE_TYPES ON X000_PER_PEOPLE_TYPES.PERSON_ID = ASSI.PERSON_ID AND
        X000_PER_PEOPLE_TYPES.EFFECTIVE_START_DATE <= ASSI.DATE_EMP_LOOKUP AND
        X000_PER_PEOPLE_TYPES.EFFECTIVE_END_DATE >= ASSI.DATE_EMP_LOOKUP
    WHERE
    """ + s_wher + """
    GROUP BY
      ASSI.EMPLOYEE_NUMBER
    """
    so_curs.execute("DROP TABLE IF EXISTS " + s_table)
    s_sql = s_sql.replace("%SOURCET%", s_source)
    s_sql = s_sql.replace("%PERIOD%", s_peri)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + s_table)

    # Add column initials
    if "INITIALS" not in funccsv.get_colnames_sqlite(so_curs, s_table):
        so_curs.execute("ALTER TABLE " + s_table + " ADD COLUMN INITIALS TEXT;")
        s_sql = "UPDATE " + s_table + """
        SET INITIALS = 
        CASE
            WHEN INSTR(MIDDLE_NAMES,' ') > 1
                THEN SUBSTR(FIRST_NAME,1,1) ||
                 SUBSTR(MIDDLE_NAMES,1,1) ||
                  TRIM(SUBSTR(MIDDLE_NAMES,INSTR(MIDDLE_NAMES,' '),2))
            WHEN LENGTH(MIDDLE_NAMES) > 0 THEN
                SUBSTR(FIRST_NAME,1,1) || SUBSTR(MIDDLE_NAMES,1,1)
            ELSE SUBSTR(FIRST_NAME,1,1)
        END
        ;"""
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t ADD COLUMN: INITIALS")

    so_curs.execute("UPDATE " + s_table + " SET NAME_LIST = LAST_NAME||' '||TITLE_FULL||' '||INITIALS;")
    so_conn.commit()
    so_curs.execute("UPDATE " + s_table + " SET NAME_ADDR = TITLE_FULL||' '||INITIALS||' '||LAST_NAME;")
    so_conn.commit()

    # Add column age
    if "AGE" not in funccsv.get_colnames_sqlite(so_curs, s_table):
        so_curs.execute("ALTER TABLE " + s_table + " ADD COLUMN AGE INT;")
        s_sql = "UPDATE " + s_table + """
                        SET AGE = strftime('%Y', 'now') - strftime('%Y', DATE_OF_BIRTH)
                        ;"""
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t ADD COLUMN: AGE")

    # Add column month
    if "MONTH" not in funccsv.get_colnames_sqlite(so_curs, s_table):
        so_curs.execute("ALTER TABLE " + s_table + " ADD COLUMN MONTH INT;")
        s_sql = "UPDATE " + s_table + """
                        SET MONTH = cast(strftime('%m', DATE_OF_BIRTH) As int)
                        ;"""
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t ADD COLUMN: MONTH")

    # Add column day
    if "DAY" not in funccsv.get_colnames_sqlite(so_curs, s_table):
        so_curs.execute("ALTER TABLE " + s_table + " ADD COLUMN DAY INT;")
        s_sql = "UPDATE " + s_table + """
                        SET DAY = cast(strftime('%d', DATE_OF_BIRTH) As int)
                        ;"""
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t ADD COLUMN: DAY")

    return funcsys.tablerowcount(so_curs, s_table)
