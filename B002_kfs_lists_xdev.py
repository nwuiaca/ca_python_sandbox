"""
Script to build standard KFS lists
Author: Albert J v Rensburg (NWU:21162395)
Created: 23 May 2018
Updated 6 Mar 2024
"""

# IMPORT PYTHON MODULES
import csv
import sqlite3

# IMPORT OWN MODULES
from _my_modules import funcconf
from _my_modules import funcfile
from _my_modules import funcsms
from _my_modules import funcsys

""" INDEX
ENVIRONMENT
OPEN THE DATABASES
BEGIN OF SCRIPT
END OF SCRIPT
"""


def kfs_lists():
    """
    Script to build standard KFS lists
    :return: Nothing
    """

    """*****************************************************************************
    ENVIRONMENT
    *****************************************************************************"""

    # Debug messages
    l_debug: bool = True

    # Declare variables
    function_name: str = "B002_kfs_lists"  # Function name
    # external_data_path = "S:/_external_data/"  # external data path
    source_file: str = "Kfs.sqlite"  # Source database
    source_path: str = f"{funcconf.drive_data_raw}Kfs/"  # Source database path
    l_message: bool = False
    # l_message: bool = funcconf.l_mess_project
    # l_vacuum: bool = False  # Vacuum database

    # Open the log writer
    s_message = f"SCRIPT: {function_name}"
    if l_debug:
        print(f"{'-' * len(s_message)}\n{s_message}\n{'-' * len(s_message)}")
    funcfile.writelog("Now")
    funcfile.writelog(f"{s_message}\n{'-' * len(s_message)}")

    # Message
    if l_message:
        funcsms.send_telegram("", "administrator", "<b>B002 Kfs lists</b>")

    """*****************************************************************************
    OPEN THE DATABASES
    *****************************************************************************"""
    if l_debug:
        print("OPEN THE DATABASES")
    funcfile.writelog("OPEN THE DATABASES")

    # Open the working database
    with sqlite3.connect(source_path + source_file) as sqlite_connection:
        sqlite_cursor = sqlite_connection.cursor()
    funcfile.writelog("OPEN DATABASE: " + source_file)

    # Attach other data sources
    sqlite_cursor.execute(f"ATTACH DATABASE '{funcconf.drive_data_raw}People/People.sqlite' AS 'PEOPLE'")
    funcfile.writelog("ATTACH DATABASE: PEOPLE.SQLITE")

    """ ****************************************************************************
    BEGIN OF SCRIPT
    *****************************************************************************"""
    if l_debug:
        print("BEGIN OF SCRIPT")
    funcfile.writelog("BEGIN OF SCRIPT")

    """ ****************************************************************************
    VENDOR MASTER FILE 2024
    *****************************************************************************"""
    if l_debug:
        print("BUILD VENDOR MASTER FILE")
    funcfile.writelog("BUILD VENDOR MASTER FILE")

    """ Index
    Build vendor list
    Vendor address list
    Vendor bank list
    Vendor bee list
    Vendor contact list
    Vendor phone list
    """

    # Build vendor list
    if l_debug:
        print("Build vendor list...")
    sr_file = "X000a_Vendor"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        vnd.VNDR_ID,
        vnd.VNDR_NM,
        vnh.VNDR_TYP_CD,
        vnd.VNDR_URL_ADDR,
        vnh.VNDR_TAX_NBR,
        vnh.VNDR_TAX_TYP_CD,
        vnd.COLLECT_TAX_IND,
        vnh.VNDR_OWNR_CD,
        vnd.VNDR_PARENT_IND,
        vnh.VNDR_FRGN_IND,
        vnd.DOBJ_MAINT_CD_ACTV_IND,
        vnd.VNDR_INACTV_REAS_CD,
        vnd.VNDR_PMT_TERM_CD,
        vnd.VNDR_SHP_TTL_CD,
        vnd.VNDR_MIN_ORD_AMT,
        vnh.VNDR_HDR_GNRTD_ID,
        vnd.VNDR_DTL_ASND_ID,
        vnh.OBJ_ID As HDR_OBJ_ID,
        vnd.OBJ_ID As DTL_OBJ_ID,
        vnh.VER_NBR As HDR_VER_NBR,
        vnd.VER_NBR As DTL_VER_NBR
    From
        PUR_VNDR_HDR_T vnh Inner Join
        PUR_VNDR_DTL_T vnd On vnd.VNDR_HDR_GNRTD_ID = vnh.VNDR_HDR_GNRTD_ID
    ;"""
    sqlite_cursor.execute(f"DROP TABLE IF EXISTS {sr_file}")
    sqlite_cursor.execute(s_sql)
    sqlite_connection.commit()
    funcfile.writelog(f"%t BUILD TABLE: {sr_file}")

    # Vendor address list
    if l_debug:
        print("Build vendor address list...")
    sr_file = "X000_Vendor_address"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        ven.VNDR_ID,
        ven.VNDR_NM,
        ven.VNDR_TYP_CD,
        Case
            When arm.VNDR_ADDR_EMAIL_ADDR <> '' Then lower(arm.VNDR_ADDR_EMAIL_ADDR)
            Else ''
        End As EMAIL_REMITTANCE,
        Case
            When apo.VNDR_ADDR_EMAIL_ADDR <> '' And apo.VNDR_ADDR_EMAIL_ADDR = arm.VNDR_ADDR_EMAIL_ADDR Then ''
            When apo.VNDR_ADDR_EMAIL_ADDR <> '' Then lower(apo.VNDR_ADDR_EMAIL_ADDR)
            Else ''
        End As EMAIL_PURCHASE_ORDER,
        Case
            When arm.VNDR_DFLT_ADDR_IND Is Null Then ''
            Else Trim(Upper(arm.VNDR_DFLT_ADDR_IND))
                || '~' || Trim(Upper(arm.VNDR_ATTN_NM))
                || '~' || Trim(Upper(arm.VNDR_LN1_ADDR))
                || '~' || Trim(Upper(arm.VNDR_LN2_ADDR))
                || '~' || Trim(Upper(arm.VNDR_CTY_NM))
                || '~' || Trim(Upper(arm.VNDR_ZIP_CD))
                || '~' || Trim(Upper(arm.VNDR_CNTRY_CD)) 
        End As ADDRESS_REMITTANCE,
        Case
            When apo.VNDR_DFLT_ADDR_IND Is Null Then ''
            When Trim(Upper(apo.VNDR_DFLT_ADDR_IND))
                || '~' || Trim(Upper(apo.VNDR_ATTN_NM))
                || '~' || Trim(Upper(apo.VNDR_LN1_ADDR))
                || '~' || Trim(Upper(apo.VNDR_LN2_ADDR))
                || '~' || Trim(Upper(apo.VNDR_CTY_NM))
                || '~' || Trim(Upper(apo.VNDR_ZIP_CD))
                || '~' || Trim(Upper(apo.VNDR_CNTRY_CD)) = Trim(Upper(arm.VNDR_DFLT_ADDR_IND))
                || '~' || Trim(Upper(arm.VNDR_ATTN_NM))
                || '~' || Trim(Upper(arm.VNDR_LN1_ADDR))
                || '~' || Trim(Upper(arm.VNDR_LN2_ADDR))
                || '~' || Trim(Upper(arm.VNDR_CTY_NM))
                || '~' || Trim(Upper(arm.VNDR_ZIP_CD))
                || '~' || Trim(Upper(arm.VNDR_CNTRY_CD)) Then '' 
            Else Trim(Upper(apo.VNDR_DFLT_ADDR_IND))
                || '~' || Trim(Upper(apo.VNDR_ATTN_NM))
                || '~' || Trim(Upper(apo.VNDR_LN1_ADDR))
                || '~' || Trim(Upper(apo.VNDR_LN2_ADDR))
                || '~' || Trim(Upper(apo.VNDR_CTY_NM))
                || '~' || Trim(Upper(apo.VNDR_ZIP_CD))
                || '~' || Trim(Upper(apo.VNDR_CNTRY_CD)) 
        End As ADDRESS_PURCHASE_ORDER,
        Max(arm.VNDR_ADDR_GNRTD_ID) As VNDR_ADDR_GNRTD_ID_RM,
        Max(apo.VNDR_ADDR_GNRTD_ID) As VNDR_ADDR_GNRTD_ID_PO
    From
        X000a_Vendor ven Left Join
        PUR_VNDR_ADDR_T arm On arm.VNDR_HDR_GNRTD_ID = ven.VNDR_HDR_GNRTD_ID
                And arm.VNDR_DTL_ASND_ID = ven.VNDR_DTL_ASND_ID
                And arm.VNDR_ADDR_TYP_CD = 'RM'
                And arm.DOBJ_MAINT_CD_ACTV_IND = 'Y' Left Join
        PUR_VNDR_ADDR_T apo On apo.VNDR_HDR_GNRTD_ID = ven.VNDR_HDR_GNRTD_ID
                And apo.VNDR_DTL_ASND_ID = ven.VNDR_DTL_ASND_ID
                And apo.VNDR_ADDR_TYP_CD = 'PO'
                And apo.DOBJ_MAINT_CD_ACTV_IND = 'Y'
    Group By
        ven.VNDR_ID
    Order By
        ven.VNDR_ID
    ;"""
    sqlite_cursor.execute(f"DROP TABLE IF EXISTS {sr_file}")
    sqlite_cursor.execute(s_sql)
    sqlite_connection.commit()
    funcfile.writelog(f"%t BUILD TABLE: {sr_file}")

    # Vendor bank list
    if l_debug:
        print("Build vendor bank list...")
    sr_file = "X000_Vendor_bank"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        ven.VNDR_ID,
        ven.VNDR_NM,
        ven.VNDR_TYP_CD,
        acc.PAYEE_ID_TYP_CD,
        ban.BNK_NM As BANK,
        bra.BRANCH_NM As BANK_BRANCH,
        acb.BNK_BRANCH_CD As BANK_BRANCH_NUMBER,
        acc.BNK_ACCT_NBR As BANK_ACCOUNT_NUMBER,
        lower(acc.PAYEE_EMAIL_ADDR) As EMAIL_PAYEE,
        Max(acc.ACH_ACCT_GNRTD_ID) As ACH_ACCT_GNRTD_ID_AC
    From
        X000a_Vendor ven Inner Join
        PDP_PAYEE_ACH_ACCT_T acc On acc.PAYEE_ID_NBR = ven.VNDR_ID
                And acc.ROW_ACTV_IND = 'Y' Inner Join
        PDP_PAYEE_ACH_ACCT_EXT_T acb On acb.ACH_ACCT_GNRTD_ID = acc.ACH_ACCT_GNRTD_ID Inner Join
        FP_BANK_BRANCH_T bra On bra.BRANCH_CD = acb.BNK_BRANCH_CD Inner Join
        FP_ZA_BANK_T ban On ban.BNK_CD = bra.BNK_CD
    Group By
        ven.VNDR_ID
    Order By
        ven.VNDR_ID
    ;"""
    sqlite_cursor.execute(f"DROP TABLE IF EXISTS {sr_file}")
    sqlite_cursor.execute(s_sql)
    sqlite_connection.commit()
    funcfile.writelog(f"%t BUILD TABLE: {sr_file}")

    # Vendor bee list
    if l_debug:
        print("Build vendor bee list...")
    sr_file = "X000_Vendor_bee"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        ven.VNDR_ID,
        ven.VNDR_NM,
        ven.VNDR_TYP_CD,
        bee.BEE_REGISTERED,
        Case
            When Length(bee.BEE_CERT_EXP_DATE) = 10
            Then bee.BEE_CERT_EXP_DATE
            Else ''
        End As BEE_CERT_EXP_DATE,
        Upper(bee.BEE_CNTCT_PRSN_NM) As CONTACT_NAME_BEE,
        bee.BEE_CNTCT_PHN_NBR As PHONE_BEE,
        Lower(bee.BEE_CNTCT_EMAIL_ADDR) As EMAIL_BEE,
        Case
            When Length(bee.BIRTH_DATE) = 10
            Then bee.BIRTH_DATE
            Else ''
        End As BIRTH_DATE,
        bee.NATIONALITY_CD,
        bee.ID_NBR,
        bee.PASSPORT_NBR,
        bee.BEE_CATEGORY,
        bee.BEE_RATING,
        bee.BEE_LEVEL,
        bee.BEE_SIZE_CD,
        bee.BEE_EMPOWERING,
        bee.BO_PERC,
        bee.BWO_PERC,
        bee.BO_DES_SUPPLIER,
        bee.EFF_BLK_SHARE_PERC,
        bee.CURRENCY_FK
    From
        X000a_Vendor ven Inner Join
        PUR_VNDR_DTL_EXT_T bee On bee.VNDR_HDR_GNRTD_ID = ven.VNDR_HDR_GNRTD_ID
                And bee.VNDR_DTL_ASND_ID = ven.VNDR_DTL_ASND_ID
    Where
        bee.BEE_REGISTERED = 'Y'
    Group By
        ven.VNDR_ID
    Order By
        ven.VNDR_ID
    ;"""
    sqlite_cursor.execute(f"DROP TABLE IF EXISTS {sr_file}")
    sqlite_cursor.execute(s_sql)
    sqlite_connection.commit()
    funcfile.writelog(f"%t BUILD TABLE: {sr_file}")

    # Vendor contact list
    if l_debug:
        print("Build vendor contact list...")
    sr_file = "X000_Vendor_contact"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        ven.VNDR_ID,
        ven.VNDR_NM,
        ven.VNDR_TYP_CD,
        con.VNDR_CNTCT_TYP_CD,
        Upper(con.VNDR_CNTCT_NM) As CONTACT_NAME,
        Lower(con.VNDR_CNTCT_EMAIL_ADDR) As CONTACT_EMAIL,
        Case
            When pho.VNDR_PHN_NBR <> '' Then pho.VNDR_PHN_NBR
            Else ''
        End As CONTACT_PHONE,
        --pho.VNDR_PHN_NBR As CONTACT_PHONE,
        Case
            WHen mob.VNDR_PHN_NBR <> '' And mob.VNDR_PHN_NBR = pho.VNDR_PHN_NBR Then '' 
            When mob.VNDR_PHN_NBR <> '' Then mob.VNDR_PHN_NBR
            Else '' 
        End As CONTACT_MOBILE,
        --mob.VNDR_PHN_NBR As CONTACT_MOBILE,
        con.VNDR_CNTCT_CMNT_TXT As CONTACT_COMMENT,
        Max(con.VNDR_CNTCT_GNRTD_ID) As VNDR_CNTCT_GNRTD_ID,
        Max(pho.VNDR_CNTCT_PHN_GNRTD_ID) As VNDR_CNTCT_PHN_GNRTD_ID_PH,
        Max(mob.VNDR_CNTCT_PHN_GNRTD_ID) As VNDR_CNTCT_PHN_GNRTD_ID_MB
    From
        X000a_Vendor ven Inner Join
        PUR_VNDR_CNTCT_T con On con.VNDR_DTL_ASND_ID = ven.VNDR_DTL_ASND_ID
                And con.VNDR_HDR_GNRTD_ID = ven.VNDR_HDR_GNRTD_ID
                And con.DOBJ_MAINT_CD_ACTV_IND = 'Y' Left Join
        PUR_VNDR_CNTCT_PHN_NBR_T pho On pho.VNDR_CNTCT_GNRTD_ID = con.VNDR_CNTCT_GNRTD_ID
                And pho.VNDR_PHN_TYP_CD = 'PH'
                And pho.DOBJ_MAINT_CD_ACTV_IND = 'Y' Left Join
        PUR_VNDR_CNTCT_PHN_NBR_T mob On mob.VNDR_CNTCT_GNRTD_ID = con.VNDR_CNTCT_GNRTD_ID
                And mob.VNDR_PHN_TYP_CD = 'MB'
                And mob.DOBJ_MAINT_CD_ACTV_IND = 'Y'
    Group By
        ven.VNDR_ID
    Order By
        ven.VNDR_ID
    ;"""
    sqlite_cursor.execute(f"DROP TABLE IF EXISTS {sr_file}")
    sqlite_cursor.execute(s_sql)
    sqlite_connection.commit()
    funcfile.writelog(f"%t BUILD TABLE: {sr_file}")

    # Vendor phone list
    if l_debug:
        print("Build vendor phone list...")
    sr_file = "X000_Vendor_phone"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        ven.VNDR_ID,
        ven.VNDR_NM,
        ven.VNDR_TYP_CD,
        Case
            When pph.VNDR_PHN_NBR <> '' Then pph.VNDR_PHN_NBR
            Else '' 
        End As PHONE,
        --pph.VNDR_PHN_NBR As PHONE,
        Case
            When pmb.VNDR_PHN_NBR <> '' And pmb.VNDR_PHN_NBR = pph.VNDR_PHN_NBR Then ''
            When pmb.VNDR_PHN_NBR <> '' Then pmb.VNDR_PHN_NBR
            Else '' 
        End  As MOBILE,
        --pmb.VNDR_PHN_NBR As MOBILE,
        Max(pph.VNDR_PHN_GNRTD_ID) As VNDR_PHN_GNRTD_ID_PH,
        Max(pmb.VNDR_PHN_GNRTD_ID) As VNDR_PHN_GNRTD_ID_MB
    From
        X000a_Vendor ven Left Join
        PUR_VNDR_PHN_NBR_T pmb On pmb.VNDR_HDR_GNRTD_ID = ven.VNDR_HDR_GNRTD_ID
                And pmb.VNDR_DTL_ASND_ID = ven.VNDR_DTL_ASND_ID
                And pmb.VNDR_PHN_TYP_CD = 'MB'
                And pmb.DOBJ_MAINT_CD_ACTV_IND = 'Y'
                And pmb.VNDR_PHN_NBR Not In ('1111111111', '11111111111', '0000000000', '+000000000', '+000000000000')
        Left Join
        PUR_VNDR_PHN_NBR_T pph On pph.VNDR_HDR_GNRTD_ID = ven.VNDR_HDR_GNRTD_ID
                And pph.VNDR_DTL_ASND_ID = ven.VNDR_DTL_ASND_ID
                And pph.VNDR_PHN_TYP_CD = 'PH'
                And pph.DOBJ_MAINT_CD_ACTV_IND = 'Y'
                And pph.VNDR_PHN_NBR Not In ('1111111111', '11111111111', '0000000000', '+000000000', '+000000000000')
    Group By
        ven.VNDR_ID
    Order By
        ven.VNDR_ID
    ;"""
    sqlite_cursor.execute(f"DROP TABLE IF EXISTS {sr_file}")
    sqlite_cursor.execute(s_sql)
    sqlite_connection.commit()
    funcfile.writelog(f"%t BUILD TABLE: {sr_file}")

    # Vendor master file
    if l_debug:
        print("Build vendor master file...")
    sr_file = "X001_Vendor_master"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        ven.VNDR_ID As vendor_id,
        ven.DOBJ_MAINT_CD_ACTV_IND As vendor_active,    
        ven.VNDR_NM As vendor_name,
        Case
            When pep.employee_number Is Not Null Then 'Y'
            Else 'N'
        End As is_employee,
        pep.name_address As employee_name,
        ven.VNDR_TYP_CD As vendor_type,
        ven.VNDR_URL_ADDR As vendor_reg_number,
        ven.VNDR_TAX_NBR As vendor_tax_number,
        ven.VNDR_TAX_TYP_CD As vendor_tax_type,
        ven.COLLECT_TAX_IND As vendor_tax_collect,
        pho.PHONE As phone,
        pho.MOBILE As mobile,
        adr.EMAIL_REMITTANCE As remittance_email,
        adr.EMAIL_PURCHASE_ORDER As purchase_order_email,
        adr.ADDRESS_REMITTANCE As address_remittance,
        adr.ADDRESS_PURCHASE_ORDER As address_purchase_order,
        con.VNDR_CNTCT_TYP_CD As contact_type,
        con.CONTACT_NAME As contact_name,
        con.CONTACT_PHONE As contact_phone,
        con.CONTACT_MOBILE As contact_mobile,
        con.CONTACT_EMAIL As contact_email,
        X000_Vendor_bank.EMAIL_PAYEE As payee_email,
        X000_Vendor_bank.PAYEE_ID_TYP_CD As payee_id_type,
        X000_Vendor_bank.BANK As bank,
        X000_Vendor_bank.BANK_BRANCH As bank_branch_name,
        X000_Vendor_bank.BANK_BRANCH_NUMBER As bank_branch_number,
        X000_Vendor_bank.BANK_ACCOUNT_NUMBER As bank_account_number,
        X000_Vendor_bee.BEE_REGISTERED As bee_registered,
        X000_Vendor_bee.BEE_CERT_EXP_DATE As bee_certificate_expire,
        X000_Vendor_bee.CONTACT_NAME_BEE As bee_contact_name,
        X000_Vendor_bee.PHONE_BEE As bee_phone,
        X000_Vendor_bee.EMAIL_BEE As bee_email,
        X000_Vendor_bee.BIRTH_DATE As bee_birthdate,
        X000_Vendor_bee.NATIONALITY_CD As bee_nationality,
        X000_Vendor_bee.ID_NBR As bee_id,
        X000_Vendor_bee.PASSPORT_NBR As bee_passport,
        X000_Vendor_bee.BEE_CATEGORY As bee_category,
        X000_Vendor_bee.BEE_RATING As bee_rating,
        X000_Vendor_bee.BEE_LEVEL As bee_level,
        X000_Vendor_bee.BEE_SIZE_CD As bee_size,
        X000_Vendor_bee.BEE_EMPOWERING As bee_empowering,
        X000_Vendor_bee.BO_PERC As bee_percentage,
        X000_Vendor_bee.BWO_PERC As bee_bwo_percentage,
        X000_Vendor_bee.BO_DES_SUPPLIER As bee_des_supplier,
        X000_Vendor_bee.EFF_BLK_SHARE_PERC As bee_share_percentage,
        X000_Vendor_bee.CURRENCY_FK As bee_currency,
        con.CONTACT_COMMENT As contact_comment
    From
        X000a_Vendor ven Left Join
        X000_Vendor_phone pho On pho.VNDR_ID = ven.VNDR_ID Left Join
        X000_Vendor_address adr On adr.VNDR_ID = ven.VNDR_ID Left Join
        X000_Vendor_contact con On con.VNDR_ID = ven.VNDR_ID Left Join
        X000_Vendor_bank On X000_Vendor_bank.VNDR_ID = ven.VNDR_ID Left Join
        X000_Vendor_bee On X000_Vendor_bee.VNDR_ID = ven.VNDR_ID Left Join
        PEOPLE.X000_People pep On pep.employee_number = Cast(ven.VNDR_HDR_GNRTD_ID As Text)
    ;"""
    sqlite_cursor.execute(f"DROP TABLE IF EXISTS {sr_file}")
    sqlite_cursor.execute(s_sql)
    sqlite_connection.commit()
    funcfile.writelog(f"%t BUILD TABLE: {sr_file}")

    """ ****************************************************************************
    END OF SCRIPT
    *****************************************************************************"""
    if l_debug:
        print("END OF SCRIPT")
    funcfile.writelog("END OF SCRIPT")

    # Commit
    sqlite_connection.commit()

    # Close the database
    sqlite_connection.close()

    # Close the log writer
    s_message = f"ENDSCRIPT: {function_name}"
    if l_debug:
        print(f"{'-' * len(s_message)}\n{s_message}\n{'-' * len(s_message)}")
    funcfile.writelog("Now")
    funcfile.writelog(f"{s_message}\n{'-' * len(s_message)}")

    return


if __name__ == '__main__':
    try:
        kfs_lists()
    except Exception as e:
        funcsys.ErrMessage(e, funcconf.l_mess_project, "B002_kfs_lists", "B002_kfs_lists")
