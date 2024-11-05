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
IMPORT OWN LOOKUPS
ORGANIZATION MASTER LIST
ACCOUNT MASTER LIST
VENDOR MASTER LIST 2018
VENDOR MASTER FILE 2024
DOCUMENT MASTER LIST
BANKS AND BRANCHES
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
    l_debug: bool = False

    # Declare variables
    function_name: str = "B002_kfs_lists"  # Function name
    # external_data_path = "S:/_external_data/"  # external data path
    source_file: str = "Kfs.sqlite"  # Source database
    source_path: str = f"{funcconf.drive_data_raw}Kfs/"  # Source database path
    # l_message: bool = True
    l_message: bool = funcconf.l_mess_project
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
    ORGANIZATION MASTER LIST
    *****************************************************************************"""
    if l_debug:
        print("ORGANIZATION MASTER LIST")
    funcfile.writelog("ORGANIZATION MASTER LIST")

    # BUILD ORGANIZATION LIST
    if l_debug:
        print("Build organization...")
    sr_file = "X000_Organization"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        ORG.FIN_COA_CD,
        ORG.ORG_CD,
        ORG.ORG_TYP_CD,
        TYP.ORG_TYP_NM,
        ORG.ORG_NM,
        ORG.ORG_BEGIN_DT,
        ORG.ORG_END_DT,
        ORG.OBJ_ID,
        ORG.VER_NBR,
        ORG.ORG_MGR_UNVL_ID,
        ORG.RC_CD,
        ORG.ORG_PHYS_CMP_CD,
        ORG.ORG_DFLT_ACCT_NBR,
        ORG.ORG_LN1_ADDR,
        ORG.ORG_LN2_ADDR,
        ORG.ORG_CITY_NM,
        ORG.ORG_STATE_CD,
        ORG.ORG_ZIP_CD,
        ORG.ORG_CNTRY_CD,
        ORG.RPTS_TO_FIN_COA_CD,
        ORG.RPTS_TO_ORG_CD,
        ORG.ORG_ACTIVE_CD,
        ORG.ORG_IN_FP_CD,
        ORG.ORG_PLNT_ACCT_NBR,
        ORG.CMP_PLNT_ACCT_NBR,
        ORG.ORG_PLNT_COA_CD,
        ORG.CMP_PLNT_COA_CD,
        ORG.ORG_LVL
    From
       CA_ORG_T ORG Left Join
       CA_ORG_TYPE_T TYP ON TYP.ORG_TYP_CD = ORG.ORG_TYP_CD
    Order By
       ORG.FIN_COA_CD,
       ORG.ORG_LVL,
       TYP.ORG_TYP_NM,
       ORG.ORG_NM,
       ORG.ORG_BEGIN_DT,
       ORG.ORG_END_DT
    ;"""
    sqlite_cursor.execute("DROP TABLE IF EXISTS " + sr_file)
    sqlite_cursor.execute(s_sql)
    sqlite_connection.commit()
    funcfile.writelog("%t BUILD TABLE: Organization list")

    # BUILD ORGANIZATION STRUCTURE
    if l_debug:
        print("Build organization structure...")
    sr_file = "X000_Organization_struct"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        ORS.FIN_COA_CD,
        ORS.ORG_CD,
        ORS.ORG_TYP_CD,
        ORS.ORG_TYP_NM,
        ORS.ORG_NM,
        ORS.ORG_MGR_UNVL_ID,
        ORS.ORG_LVL,
        ORA.FIN_COA_CD AS FIN_COA_CD1,
        ORA.ORG_CD AS ORG_CD1,
        ORA.ORG_TYP_CD AS ORG_TYP_CD1,
        ORA.ORG_TYP_NM AS ORG_TYP_NM1,
        ORA.ORG_NM AS ORG_NM1,
        ORA.ORG_MGR_UNVL_ID AS ORG_MGR_UNVL_ID1,
        ORA.ORG_LVL AS ORG_LVL1,
        ORB.FIN_COA_CD AS FIN_COA_CD2,
        ORB.ORG_CD AS ORG_CD2,
        ORB.ORG_TYP_CD AS ORG_TYP_CD2,
        ORB.ORG_TYP_NM AS ORG_TYP_NM2,
        ORB.ORG_NM AS ORG_NM2,
        ORB.ORG_MGR_UNVL_ID AS ORG_MGR_UNVL_ID2,
        ORB.ORG_LVL AS ORG_LVL2,
        ORC.FIN_COA_CD AS FIN_COA_CD3,
        ORC.ORG_CD AS ORG_CD3,
        ORC.ORG_TYP_CD AS ORG_TYP_CD3,
        ORC.ORG_TYP_NM AS ORG_TYP_NM3,
        ORC.ORG_NM AS ORG_NM3,
        ORC.ORG_MGR_UNVL_ID AS ORG_MGR_UNVL_ID3,
        ORC.ORG_LVL AS ORG_LVL3,
        ORD.FIN_COA_CD AS FIN_COA_CD4,
        ORD.ORG_CD AS ORG_CD4,
        ORD.ORG_TYP_CD AS ORG_TYP_CD4,
        ORD.ORG_TYP_NM AS ORG_TYP_NM4,
        ORD.ORG_NM AS ORG_NM4,
        ORD.ORG_MGR_UNVL_ID AS ORG_MGR_UNVL_ID4,
        ORD.ORG_LVL AS ORG_LVL4,
        ORE.FIN_COA_CD AS FIN_COA_CD5,
        ORE.ORG_CD AS ORG_CD5,
        ORE.ORG_TYP_CD AS ORG_TYP_CD5,
        ORE.ORG_TYP_NM AS ORG_TYP_NM5,
        ORE.ORG_NM AS ORG_NM5,
        ORE.ORG_MGR_UNVL_ID AS ORG_MGR_UNVL_ID5,
        ORE.ORG_LVL AS ORG_LVL5,
        ORF.FIN_COA_CD AS FIN_COA_CD6,
        ORF.ORG_CD AS ORG_CD6,
        ORF.ORG_TYP_CD AS ORG_TYP_CD6,
        ORF.ORG_TYP_NM AS ORG_TYP_NM6,
        ORF.ORG_NM AS ORG_NM6,
        ORF.ORG_MGR_UNVL_ID AS ORG_MGR_UNVL_ID6,
        ORF.ORG_LVL AS ORG_LVL6,
        ORG.FIN_COA_CD AS FIN_COA_CD7,
        ORG.ORG_CD AS ORG_CD7,
        ORG.ORG_TYP_CD AS ORG_TYP_CD7,
        ORG.ORG_TYP_NM AS ORG_TYP_NM7,
        ORG.ORG_NM AS ORG_NM7,
        ORG.ORG_MGR_UNVL_ID AS ORG_MGR_UNVL_ID7,
        ORG.ORG_LVL AS ORG_LVL7
    From
        X000_Organization ORS Left Join
        X000_Organization ORA ON ORA.FIN_COA_CD = ORS.RPTS_TO_FIN_COA_CD And
            ORA.ORG_CD = ORS.RPTS_TO_ORG_CD Left Join
        X000_Organization ORB ON ORB.FIN_COA_CD = ORA.RPTS_TO_FIN_COA_CD AND
            ORB.ORG_CD = ORA.RPTS_TO_ORG_CD Left Join
        X000_Organization ORC ON ORC.FIN_COA_CD = ORB.RPTS_TO_FIN_COA_CD AND
            ORC.ORG_CD = ORB.RPTS_TO_ORG_CD Left Join
        X000_Organization ORD ON ORD.FIN_COA_CD = ORC.RPTS_TO_FIN_COA_CD AND
            ORD.ORG_CD = ORC.RPTS_TO_ORG_CD Left Join
        X000_Organization ORE ON ORE.FIN_COA_CD = ORD.RPTS_TO_FIN_COA_CD AND
            ORE.ORG_CD = ORD.RPTS_TO_ORG_CD Left Join
        X000_Organization ORF ON ORF.FIN_COA_CD = ORE.RPTS_TO_FIN_COA_CD AND
            ORF.ORG_CD = ORE.RPTS_TO_ORG_CD Left Join
        X000_Organization ORG ON ORG.FIN_COA_CD = ORF.RPTS_TO_FIN_COA_CD AND
            ORG.ORG_CD = ORF.RPTS_TO_ORG_CD
    Where
        ORS.ORG_BEGIN_DT >= Date("2018-01-01")
    Order By
        ORG.ORG_LVL,
        ORG.ORG_NM
    ;"""
    sqlite_cursor.execute("DROP TABLE IF EXISTS " + sr_file)
    sqlite_cursor.execute(s_sql)
    sqlite_connection.commit()
    funcfile.writelog("%t BUILD TABLE: Organization structure")

    """ ****************************************************************************
    ACCOUNT MASTER LIST
    *****************************************************************************"""
    if l_debug:
        print("ACCOUNT MASTER LIST")
    funcfile.writelog("ACCOUNT MASTER LIST")

    # BUILD ACCOUNT LIST
    if l_debug:
        print("Build account list...")
    sr_file = "X000_Account"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        ACC.FIN_COA_CD,
        ACC.ACCOUNT_NBR,
        TYP.ACCT_TYP_NM,
        ORG.ORG_NM,
        ACC.ACCOUNT_NM,
        ACC.ACCT_FSC_OFC_UID,
        ACC.ACCT_SPVSR_UNVL_ID,
        ACC.ACCT_MGR_UNVL_ID,
        ACC.ORG_CD,
        ACC.ACCT_TYP_CD,
        ACC.ACCT_PHYS_CMP_CD,
        ACC.ACCT_FRNG_BNFT_CD,
        ACC.FIN_HGH_ED_FUNC_CD,
        ACC.SUB_FUND_GRP_CD,
        ACC.ACCT_RSTRC_STAT_CD,
        ACC.ACCT_RSTRC_STAT_DT,
        ACC.ACCT_CITY_NM,
        ACC.ACCT_STATE_CD,
        ACC.ACCT_STREET_ADDR,
        ACC.ACCT_ZIP_CD,
        ACC.RPTS_TO_FIN_COA_CD,
        ACC.RPTS_TO_ACCT_NBR,
        ACC.ACCT_CREATE_DT,
        ACC.ACCT_EFFECT_DT,
        ACC.ACCT_EXPIRATION_DT,
        ACC.CONT_FIN_COA_CD,
        ACC.CONT_ACCOUNT_NBR,
        ACC.ACCT_CLOSED_IND,
        ACC.OBJ_ID,
        ACC.VER_NBR
    From
        CA_ACCOUNT_T ACC Left Join
        X000_Organization ORG ON ORG.FIN_COA_CD = ACC.FIN_COA_CD AND ORG.ORG_CD = ACC.ORG_CD Left Join
        CA_ACCOUNT_TYPE_T TYP ON TYP.ACCT_TYP_CD = ACC.ACCT_TYP_CD
    """
    sqlite_cursor.execute("DROP TABLE IF EXISTS " + sr_file)
    sqlite_cursor.execute(s_sql)
    sqlite_connection.commit()
    funcfile.writelog("%t BUILD TABLE: Account list")

    """ ****************************************************************************
    VENDOR MASTER LIST 2018
    *****************************************************************************"""
    if l_debug:
        print("VENDOR MASTER LIST")
    funcfile.writelog("VENDOR MASTER LIST")

    # BUILD TABLE WITH VENDOR REMITTANCE ADDRESSES
    # BUILD TABLE WITH VENDOR PURCHASE ORDER ADDRESSES
    # JOIN VENDOR RM AND PO ADDRESSES
    # BUILD VENDOR BANK ACCOUNT TABLE
    # BUILD CONTACT NAMES EMAIL PHONE MOBILE LIST
    # BUILD VENDOR PHONE MOBILE LIST
    # BUILD VENDOR MASTER CONTACT LIST
    # BUILD VENDOR MASTER COMBINED CONTACT LIST
    # UPDATE NUMBERS COLUMN WITH MOBILE
    # UPDATE NUMBERS COLUMN WITH PHONEC
    # UPDATE NUMBERS COLUMN WITH MOBILEC
    # UPDATE NUMBERS REMOVE SPECIAL CHARACTERS FROM NUMBERS
    # TRIM UNWANTED CHARACTERS
    # BUILD VENDOR TABLE

    # BUILD TABLE WITH VENDOR REMITTANCE ADDRESSES
    if l_debug:
        print("Build vendor remittance addresses...")
    sr_file = "X001aa_vendor_rm_address"
    s_sql = "CREATE VIEW " + sr_file + " AS " + """
    Select
        CAST(TRIM(UPPER(ADDR.VNDR_HDR_GNRTD_ID))||'-'||TRIM(UPPER(ADDR.VNDR_DTL_ASND_ID)) AS
            TEXT) VENDOR_ID,
        ADDR.VNDR_ST_CD,
        ADDR.VNDR_CNTRY_CD,
        ADDR.VNDR_ADDR_EMAIL_ADDR,
        ADDR.VNDR_B2B_URL_ADDR,
        ADDR.VNDR_FAX_NBR,
        TRIM(UPPER(ADDR.VNDR_DFLT_ADDR_IND))||'~'||
        TRIM(UPPER(ADDR.VNDR_ATTN_NM))||'~'||
        TRIM(UPPER(ADDR.VNDR_LN1_ADDR))||'~'||
        TRIM(UPPER(ADDR.VNDR_LN2_ADDR))||'~'||
        TRIM(UPPER(ADDR.VNDR_CTY_NM))||'~'||
        TRIM(UPPER(ADDR.VNDR_ZIP_CD))||'~'||
        TRIM(UPPER(ADDR.VNDR_CNTRY_CD))
        ADDRESS_RM
    From
        PUR_VNDR_ADDR_T ADDR
    Where
        ADDR.VNDR_ADDR_TYP_CD = 'RM' And
        ADDR.VNDR_DFLT_ADDR_IND = 'Y'
    Group By
        ADDR.VNDR_HDR_GNRTD_ID,
        ADDR.VNDR_DTL_ASND_ID    
    """
    sqlite_cursor.execute("DROP VIEW IF EXISTS " + sr_file)
    sqlite_cursor.execute(s_sql)
    sqlite_connection.commit()
    funcfile.writelog("%t BUILD VIEW: " + sr_file)

    # BUILD TABLE WITH VENDOR PURCHASE ORDER ADDRESSES
    if l_debug:
        print("Build vendor purchase order addresses...")
    sr_file = "X001ab_vendor_po_address"
    s_sql = "CREATE VIEW " + sr_file + " AS " + """
    Select
        CAST(TRIM(UPPER(ADDR.VNDR_HDR_GNRTD_ID))||'-'||TRIM(UPPER(ADDR.VNDR_DTL_ASND_ID))
            AS TEXT) VENDOR_ID,
        ADDR.VNDR_ST_CD,
        ADDR.VNDR_CNTRY_CD,
        ADDR.VNDR_ADDR_EMAIL_ADDR,
        ADDR.VNDR_B2B_URL_ADDR,
        ADDR.VNDR_FAX_NBR,
        TRIM(UPPER(ADDR.VNDR_DFLT_ADDR_IND))||'~'||
        TRIM(UPPER(ADDR.VNDR_ATTN_NM))||'~'||
        TRIM(UPPER(ADDR.VNDR_LN1_ADDR))||'~'||
        TRIM(UPPER(ADDR.VNDR_LN2_ADDR))||'~'||
        TRIM(UPPER(ADDR.VNDR_CTY_NM))||'~'||
        TRIM(UPPER(ADDR.VNDR_ZIP_CD))||'~'||
        TRIM(UPPER(ADDR.VNDR_CNTRY_CD))
        ADDRESS_PO
    From
        PUR_VNDR_ADDR_T ADDR
    Where
        ADDR.VNDR_ADDR_TYP_CD = 'PO' And
        ADDR.VNDR_DFLT_ADDR_IND = 'Y'
    Group By
        ADDR.VNDR_HDR_GNRTD_ID,
        ADDR.VNDR_DTL_ASND_ID    
    """
    sqlite_cursor.execute("DROP VIEW IF EXISTS " + sr_file)
    sqlite_cursor.execute(s_sql)
    sqlite_connection.commit()
    funcfile.writelog("%t BUILD VIEW: " + sr_file)

    # JOIN VENDOR RM AND PO ADDRESSES
    if l_debug:
        print("Build vendor address master file...")
    sr_file = "X001ac_vendor_address_comb"
    s_sql = "CREATE VIEW " + sr_file + " AS " + """
    Select
        VENDOR.VNDR_ID As VENDOR_ID,
        Case
            When ADDRM.VNDR_ST_CD <> '' Then ADDRM.VNDR_ST_CD
            Else ADDPO.VNDR_ST_CD
        End as STATE_CD,
        Case
            When ADDRM.VNDR_CNTRY_CD <> '' Then ADDRM.VNDR_CNTRY_CD
            Else ADDPO.VNDR_CNTRY_CD
        End as COUNTRY_CD,
        Case
            When ADDRM.VNDR_ADDR_EMAIL_ADDR <> '' Then
                Lower(ADDRM.VNDR_ADDR_EMAIL_ADDR)
            Else Lower(ADDPO.VNDR_ADDR_EMAIL_ADDR)
        End as EMAIL,
        Case
            When ADDRM.VNDR_B2B_URL_ADDR <> '' Then Lower(ADDRM.VNDR_B2B_URL_ADDR)
            Else Lower(ADDPO.VNDR_B2B_URL_ADDR)
        End as URL,
        Case
            When ADDRM.VNDR_FAX_NBR <> '' Then ADDRM.VNDR_FAX_NBR
            Else ADDPO.VNDR_FAX_NBR
        End as FAX,
        Case
            When ADDRM.ADDRESS_RM <> '' Then Upper(ADDRM.ADDRESS_RM)
            Else Upper(ADDPO.ADDRESS_PO)
        End as ADDRESS
    From
        PUR_VNDR_DTL_T VENDOR Left Join
        X001aa_vendor_rm_address ADDRM On ADDRM.VENDOR_ID = VENDOR.VNDR_ID Left Join
        X001ab_vendor_po_address ADDPO On ADDPO.VENDOR_ID = VENDOR.VNDR_ID
    """
    sqlite_cursor.execute("DROP VIEW IF EXISTS " + sr_file)
    sqlite_cursor.execute(s_sql)
    sqlite_connection.commit()
    funcfile.writelog("%t BUILD VIEW: " + sr_file)

    # BUILD VENDOR BANK ACCOUNT TABLE
    if l_debug:
        print("Build vendor bank account table...")
    sr_file = "X001ad_vendor_bankacc"
    s_sql = "CREATE VIEW " + sr_file + " AS " + """
    Select Distinct
        BANK.PAYEE_ID_NBR As VENDOR_ID,
        STUD.BNK_ACCT_NBR As STUD_BANK,
        STUDB.BNK_BRANCH_CD As STUD_BRANCH,
        STUD.PAYEE_ID_TYP_CD As STUD_TYPE,
        STUD.PAYEE_EMAIL_ADDR As STUD_MAIL,
        VEND.BNK_ACCT_NBR As VEND_BANK,
        VENDB.BNK_BRANCH_CD As VEND_BRANCH,
        VEND.PAYEE_ID_TYP_CD As VEND_TYPE,
        VEND.PAYEE_EMAIL_ADDR As VEND_MAIL,
        EMPL.BNK_ACCT_NBR As EMPL_BANK,
        EMPLB.BNK_BRANCH_CD As EMPL_BRANCH,
        EMPL.PAYEE_ID_TYP_CD As EMPL_TYPE,
        EMPL.PAYEE_EMAIL_ADDR As EMPL_MAIL
    From
        PDP_PAYEE_ACH_ACCT_T BANK
        Left Join PDP_PAYEE_ACH_ACCT_T STUD On STUD.PAYEE_ID_NBR = BANK.PAYEE_ID_NBR And STUD.PAYEE_ID_TYP_CD = 'S' And
            STUD.ROW_ACTV_IND = 'Y'
        Left Join PDP_PAYEE_ACH_ACCT_EXT_T STUDB On STUDB.ACH_ACCT_GNRTD_ID = STUD.ACH_ACCT_GNRTD_ID
        Left Join PDP_PAYEE_ACH_ACCT_T VEND On VEND.PAYEE_ID_NBR = BANK.PAYEE_ID_NBR And VEND.PAYEE_ID_TYP_CD = 'V' And
            VEND.ROW_ACTV_IND = 'Y'
        Left Join PDP_PAYEE_ACH_ACCT_EXT_T VENDB On VENDB.ACH_ACCT_GNRTD_ID = VEND.ACH_ACCT_GNRTD_ID
        Left Join PDP_PAYEE_ACH_ACCT_T EMPL On EMPL.PAYEE_ID_NBR = BANK.PAYEE_ID_NBR And EMPL.PAYEE_ID_TYP_CD = 'E' And
            EMPL.ROW_ACTV_IND = 'Y'
        Left Join PDP_PAYEE_ACH_ACCT_EXT_T EMPLB On EMPLB.ACH_ACCT_GNRTD_ID = EMPL.ACH_ACCT_GNRTD_ID
    Where
        BANK.ROW_ACTV_IND = 'Y'
    """
    sqlite_cursor.execute("DROP VIEW IF EXISTS " + sr_file)
    sqlite_cursor.execute(s_sql)
    sqlite_connection.commit()
    funcfile.writelog("%t BUILD VIEW: " + sr_file)

    # BUILD CONTACT NAMES EMAIL PHONE MOBILE LIST
    if l_debug:
        print("Build contact email phone and mobile list...")
    sr_file = "X001ae_vendor_contact"
    sqlite_cursor.execute("DROP VIEW IF EXISTS " + sr_file)
    s_sql = "Create View " + sr_file + " As" + """
    Select
        CONT.VNDR_HDR_GNRTD_ID,
        CONT.VNDR_CNTCT_GNRTD_ID,
        CONT.VNDR_CNTCT_TYP_CD As CONTACT_TYPE,
        Trim(Upper(CONT.VNDR_CNTCT_NM)) As CONTACT,
        Trim(Upper(CONT.VNDR_ATTN_NM)) As ATTENTION,
        Lower(CONT.VNDR_CNTCT_EMAIL_ADDR) As EMAIL,
        PHON.VNDR_PHN_NBR As PHONE,
        MOBI.VNDR_PHN_NBR As MOBILE
    From
        PUR_VNDR_CNTCT_T CONT Left Join
        PUR_VNDR_CNTCT_PHN_NBR_T PHON On PHON.VNDR_CNTCT_GNRTD_ID = CONT.VNDR_CNTCT_GNRTD_ID And PHON.VNDR_PHN_TYP_CD = 'PH' Left Join
        PUR_VNDR_CNTCT_PHN_NBR_T MOBI On MOBI.VNDR_CNTCT_GNRTD_ID = CONT.VNDR_CNTCT_GNRTD_ID And MOBI.VNDR_PHN_TYP_CD = 'MB'
    Group By
        CONT.VNDR_HDR_GNRTD_ID
    Order By
        CONT.VNDR_HDR_GNRTD_ID,
        CONTACT_TYPE
    ;"""
    # s_sql = s_sql.replace("%PERIOD%", s_period)
    sqlite_cursor.execute(s_sql)
    funcfile.writelog("%t BUILD VIEW: " + sr_file)

    # BUILD VENDOR PHONE MOBILE LIST
    if l_debug:
        print("Build vendor phone and mobile list...")
    sr_file = "X001af_vendor_phone"
    sqlite_cursor.execute("DROP VIEW IF EXISTS " + sr_file)
    s_sql = "Create View " + sr_file + " As" + """
    Select
        PHON.VNDR_HDR_GNRTD_ID,
        PHON.VNDR_PHN_GNRTD_ID,
        LAND.VNDR_PHN_NBR As PHONE,
        MOBI.VNDR_PHN_NBR As MOBILE
    From
        PUR_VNDR_PHN_NBR_T PHON Left Join
        PUR_VNDR_PHN_NBR_T LAND On LAND.VNDR_PHN_GNRTD_ID = PHON.VNDR_PHN_GNRTD_ID And LAND.VNDR_PHN_TYP_CD = 'PH' Left Join
        PUR_VNDR_PHN_NBR_T MOBI On MOBI.VNDR_PHN_GNRTD_ID = PHON.VNDR_PHN_GNRTD_ID And MOBI.VNDR_PHN_TYP_CD = 'MB'
    Group By
        PHON.VNDR_HDR_GNRTD_ID
    Order By
        PHON.VNDR_HDR_GNRTD_ID
    ;"""
    # s_sql = s_sql.replace("%PERIOD%", s_period)
    sqlite_cursor.execute(s_sql)
    funcfile.writelog("%t BUILD VIEW: " + sr_file)

    # BUILD VENDOR MASTER CONTACT LIST
    if l_debug:
        print("Build vendor master contact list...")
    sr_file = "X001ag_contact_comb"
    sqlite_cursor.execute("DROP VIEW IF EXISTS " + sr_file)
    s_sql = "Create View " + sr_file + " As" + """
    Select
        VEND.VNDR_ID As VENDOR_ID,
        PHON.PHONE,
        PHON.MOBILE,
        CONT.CONTACT,
        CONT.ATTENTION,
        CONT.EMAIL,
        CONT.PHONE As PHONEC,
        CONT.MOBILE As MOBILEC
    From
        PUR_VNDR_DTL_T VEND Left Join
        X001af_vendor_phone PHON On PHON.VNDR_HDR_GNRTD_ID = VEND.VNDR_HDR_GNRTD_ID Left Join
        X001ae_vendor_contact CONT On CONT.VNDR_HDR_GNRTD_ID = VEND.VNDR_HDR_GNRTD_ID
    ;"""
    # s_sql = s_sql.replace("%PERIOD%", s_period)
    sqlite_cursor.execute(s_sql)
    funcfile.writelog("%t BUILD VIEW: " + sr_file)

    # BUILD VENDOR MASTER COMBINED CONTACT LIST
    if l_debug:
        print("Build vendor master combined contact list...")
    sr_file = "X000_Contact"
    sqlite_cursor.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = "Create Table " + sr_file + " As" + """
    Select
        CONT.VENDOR_ID AS VENDOR_ID,
        CONT.CONTACT,
        CONT.ATTENTION,
        CONT.EMAIL,
        CONT.PHONE,
        CONT.MOBILE,
        CONT.PHONEC AS PHONEC,
        CONT.MOBILEC AS MOBILEC,
        CASE
            WHEN CONT.PHONE != '' THEN Replace(Trim(CONT.PHONE),' ','') || '~'
            ELSE ''
        END As NUMBERS
    From
        X001ag_contact_comb CONT
    Order By
        VENDOR_ID    
    ;"""
    # s_sql = s_sql.replace("%PERIOD%", s_period)
    sqlite_cursor.execute(s_sql)
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # UPDATE NUMBERS COLUMN WITH MOBILE
    if l_debug:
        print("Update numbers with mobile...")
    sqlite_cursor.execute("Update X000_Contact " + """
                    Set NUMBERS = 
                    Case
                        When Trim(MOBILE) != '' And Instr(NUMBERS, Replace(Trim(MOBILE), ' ', '')) != 0 THEN NUMBERS
                        When Trim(MOBILE) != '' THEN NUMBERS || Replace(Trim(MOBILE),' ','') || '~'
                        Else NUMBERS
                    End
                    ;""")

    # UPDATE NUMBERS COLUMN WITH PHONEC
    if l_debug:
        print("Update numbers with phonec...")
    sqlite_cursor.execute("Update X000_Contact " + """
                    Set NUMBERS = 
                    Case
                        When Trim(PHONEC) != '' And Instr(NUMBERS, Replace(Trim(PHONEC), ' ', '')) != 0 THEN NUMBERS
                        When Trim(PHONEC) != '' THEN NUMBERS || Replace(Trim(PHONEC),' ','') || '~'
                        Else NUMBERS
                    End
                    ;""")

    # UPDATE NUMBERS COLUMN WITH MOBILEC
    if l_debug:
        print("Update numbers with mobilec...")
    sqlite_cursor.execute("Update X000_Contact " + """
                    Set NUMBERS = 
                    Case
                        When Trim(MOBILEC) != '' And Instr(NUMBERS, Replace(Trim(MOBILEC), ' ', '')) != 0 THEN NUMBERS
                        When Trim(MOBILEC) != '' THEN NUMBERS || Replace(Trim(MOBILEC),' ','') || '~'
                        Else NUMBERS
                    End
                    ;""")

    # UPDATE NUMBERS REMOVE SPECIAL CHARACTERS FROM NUMBERS
    for i in range(5):
        if l_debug:
            print("Remove special characters...")
        sqlite_cursor.execute("Update X000_Contact " + """
                        Set NUMBERS = 
                        Case
                            When NUMBERS Like('%-%') Then Replace(NUMBERS, '-', '')
                            When NUMBERS Like('%(%') Then Replace(NUMBERS, '(', '')
                            When NUMBERS Like('%)%') Then Replace(NUMBERS, ')', '')
                            When NUMBERS Like('%*%') Then Replace(NUMBERS, '*', '')
                            When NUMBERS Like('%;%') Then Replace(NUMBERS, ';', '')                        
                            When NUMBERS Like('%.%') Then Replace(NUMBERS, '.', '')                        
                            When NUMBERS Like('%+27%') Then Replace(NUMBERS, '+27', '0')
                            When NUMBERS Like('%UNKNOWN%') Then Replace(NUMBERS, 'UNKNOWN'||'~', '')
                            When NUMBERS Like('%geennommerbeskikbaar%') Then Replace(NUMBERS, 'geennommerbeskikbaar'||'~', '')
                            When NUMBERS Like('%NONE%') Then Replace(NUMBERS, 'NONE'||'~', '')
                            When NUMBERS Like('%N/A%') Then Replace(NUMBERS, 'N/A'||'~', '')
                            When NUMBERS Like('%Fon:%') Then Replace(NUMBERS, 'Fon:', '')
                            When NUMBERS Like('%Tel:%') Then Replace(NUMBERS, 'Tel:', '')
                            When Trim(EMAIL) != '' And Instr(NUMBERS,EMAIL) > 0 THEN Replace(NUMBERS, EMAIL || '~', '')
                            When NUMBERS Like('%O%') Then Replace(NUMBERS, 'O', '0')
                            Else NUMBERS
                        End
                        ;""")

    # TRIM UNWANTED CHARACTERS
    if l_debug:
        print("Trim unwanted characters...")
    sqlite_cursor.execute("Update X000_Contact " + """
                    Set NUMBERS = Trim(NUMBERS,'~')
                    ;""")

    # BUILD VENDOR TABLE
    if l_debug:
        print("Build vendor master file...")
    sr_file = "X000_Vendor"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        DETAIL.VNDR_ID As VENDOR_ID,
        UPPER(DETAIL.VNDR_NM) AS VNDR_NM,
        DETAIL.VNDR_URL_ADDR,
        HEADER.VNDR_TAX_NBR,
        BANK.VEND_BANK,
        BANK.VEND_BRANCH,
        BANK.VEND_MAIL,
        BANK.EMPL_BANK,
        BANK.STUD_BANK,
        CONT.NUMBERS,
        ADDR.FAX,
        ADDR.EMAIL,
        CONT.CONTACT,
        CONT.ATTENTION,
        CONT.EMAIL AS EMAIL_CONTACT,
        ADDR.ADDRESS,
        ADDR.URL,
        ADDR.STATE_CD,
        ADDR.COUNTRY_CD,
        HEADER.VNDR_TAX_TYP_CD,
        HEADER.VNDR_TYP_CD,
        DETAIL.VNDR_PMT_TERM_CD,
        DETAIL.VNDR_SHP_TTL_CD,
        DETAIL.VNDR_PARENT_IND,
        DETAIL.VNDR_1ST_LST_NM_IND,
        DETAIL.COLLECT_TAX_IND,
        HEADER.VNDR_FRGN_IND,
        DETAIL.VNDR_CNFM_IND,
        DETAIL.VNDR_PRPYMT_IND,
        DETAIL.VNDR_CCRD_IND,
        DETAIL.DOBJ_MAINT_CD_ACTV_IND,
        DETAIL.VNDR_INACTV_REAS_CD
    From
        PUR_VNDR_DTL_T DETAIL Left Join
        PUR_VNDR_HDR_T HEADER On HEADER.VNDR_HDR_GNRTD_ID = DETAIL.VNDR_HDR_GNRTD_ID Left Join
        X001ac_vendor_address_comb ADDR On ADDR.VENDOR_ID = DETAIL.VNDR_ID Left Join
        X001ad_vendor_bankacc BANK On BANK.VENDOR_ID = DETAIL.VNDR_ID Left Join
        X000_Contact CONT On CONT.VENDOR_ID = DETAIL.VNDR_ID
    Order by
        VNDR_NM,
        VENDOR_ID
    """
    sqlite_cursor.execute("DROP TABLE IF EXISTS " + sr_file)
    sqlite_cursor.execute(s_sql)
    sqlite_connection.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

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
    DOCUMENT MASTER LIST
    *****************************************************************************"""
    if l_debug:
        print("DOCUMENTS MASTER LIST")
    funcfile.writelog("DOCUMENTS MASTER LIST")

    # BUILD DOCS MASTER LIST
    if l_debug:
        print("Build docs master list...")
    sr_file = "X000_Document"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        DOC.DOC_HDR_ID,
        DOC.DOC_TYP_ID,
        TYP.DOC_TYP_NM,
        TYP.LBL
    From
        KREW_DOC_HDR_T DOC Inner Join
        KREW_DOC_TYP_T TYP On TYP.DOC_TYP_ID = DOC.DOC_TYP_ID
    """
    sqlite_cursor.execute("DROP TABLE IF EXISTS " + sr_file)
    sqlite_cursor.execute(s_sql)
    sqlite_connection.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    """ ****************************************************************************
    BANKS AND BRANCHES
    *****************************************************************************"""

    # Master table of banks and branches
    if l_debug:
        print("Build banks and branches master table...")
    sr_file = "X000_Banks_branches"
    s_sql = f"CREATE TABLE {sr_file} AS " + """
    Select
        ban.BNK_CD,
        Upper(ban.BNK_NM) As BNK_NM,
        ban.VER_NBR As VER_NBR_BANK,
        bra.BRANCH_CD,
        bra.BRANCH_NM,
        bra.VER_NBR As VER_NBR_BRANCH,
        bra.ACTV_IND
    From
        FP_ZA_BANK_T ban Inner Join
        FP_BANK_BRANCH_T bra On bra.BNK_CD = ban.BNK_CD
    Order By
        BNK_NM,
        bra.BRANCH_NM
    ;"""
    sqlite_cursor.execute(f"DROP TABLE IF EXISTS {sr_file}")
    sqlite_cursor.execute(s_sql)
    sqlite_connection.commit()
    funcfile.writelog(f"%t BUILD TABLE: {sr_file}")

    # NWU Bank accounts detail
    if l_debug:
        print("Build nwu bank accounts detail...")
    sr_file = "X000_NWU_bank_details"
    s_sql = f"CREATE TABLE {sr_file} AS " + """
    Select
        acc.BNK_CD,
        acc.ROW_ACTV_IND,
        bra.BNK_NM,
        bra.BRANCH_NM,
        acb.BNK_BRANCH_CD,
        acc.BNK_ACCT_NBR,
        Upper(acc.BNK_ACCT_DESC) As BNK_ACCT_DESC,
        acc.CSH_OFST_FIN_COA_CD,
        acc.CSH_OFST_ACCT_NBR,
        acc.CSH_OFST_OBJ_CD,
        acc.CONT_BNK_CD,
        acc.BNK_DPST_IND,
        acc.BNK_DISB_IND,
        acc.BNK_ACH_IND,
        acc.BNK_CHK_IND
    From
        FP_BANK_T acc Inner Join
        FP_BANK_EXT_T acb On acb.BNK_CD = acc.BNK_CD Inner Join
        X000_Banks_branches bra On bra.BRANCH_CD = acb.BNK_BRANCH_CD
    Order By
        acc.ROW_ACTV_IND Desc,
        bra.BNK_NM,
        bra.BRANCH_NM
    ;"""
    sqlite_cursor.execute(f"DROP TABLE IF EXISTS {sr_file}")
    sqlite_cursor.execute(s_sql)
    sqlite_connection.commit()
    funcfile.writelog(f"%t BUILD TABLE: {sr_file}")

    # NWU Bank accounts unique
    if l_debug:
        print("Build nwu banks account unique table...")
    sr_file = "X000_NWU_bank_unique"
    s_sql = f"CREATE TABLE {sr_file} AS " + """
    Select
        ban.BNK_NM,
        ban.BRANCH_NM,
        ban.BNK_BRANCH_CD,
        ban.BNK_ACCT_NBR,
        ban.BNK_ACCT_DESC
    From
        X000_NWU_bank_details ban
    Group By
        ban.BNK_BRANCH_CD,
        ban.BNK_ACCT_NBR
    Order By
        ban.BNK_ACCT_NBR
    ;"""
    sqlite_cursor.execute(f"DROP TABLE IF EXISTS {sr_file}")
    sqlite_cursor.execute(s_sql)
    sqlite_connection.commit()
    funcfile.writelog(f"%t BUILD TABLE: {sr_file}")

    # MESSAGE
    if l_message:
        sr_file = "X000_Account"
        i = funcsys.tablerowcount(sqlite_cursor, sr_file)
        funcsms.send_telegram("", "administrator", "<b> " + str(i) + "</b> " + " Accounts")
        sr_file = "X000_Vendor"
        i = funcsys.tablerowcount(sqlite_cursor, sr_file)
        funcsms.send_telegram("", "administrator", "<b> " + str(i) + "</b> " + " Vendors")

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
