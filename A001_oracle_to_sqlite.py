""" ORACLE_TO_SQLITE ***********************************************************
Script to extract raw data from an ODBC data source using lookup tables
Copyright (c) AB Janse van Rensburg 10 Feb 2018
"""

# Import python objects *******************************************************
import csv
import oracledb
import sqlite3

# Import own modules ***********************************************************
from _my_modules import funcconf
from _my_modules import funcdatn
from _my_modules import funcfile
from _my_modules import funcstr
from _my_modules import funcsys
from _my_modules import funcsms


def oracle_to_sqlite(s_table="000b_Table - temp.csv", s_tables="TEMP"):
    """
    Script to import data from oracle.
    :param s_table: Table name of files to convert
    :param s_tables: Table set name
    :return: Nothing
    """

    # DECLARE VARIABLES

    # l_mess: bool = False
    l_mess: bool = funcconf.l_mess_project
    i_mess: int = 0
    l_debug: bool = False

    sl_path: str = funcconf.drive_system

    so_dsn = ""  # Source file name
    oracle_source_usr = ""  # Source file user name
    oracle_source_pwd = ""  # Source file password

    de_fil = ""  # Destination file name
    de_pat = ""  # Destination file path

    tb_own = ""  # Table owner
    tb_nam = ""  # Table name
    tb_whe = ""  # Table filter / where clause
    tb_ord = ""  # Table sort
    tb_alt = ""  # Table alternative name
    tb_sch = ""  # Table schedule

    co_nam = ""  # Column name
    co_ana = ""  # Column alternate name
    co_typ = ""  # Column type
    co_len = 0   # Column width
    co_dec = 0   # Column decimals

    sco_nam = ""  # Column string with actual names
    sco_ana = ""  # Column string with alternate names
    sco_dro = ""  # Column drop table

    ssql_create = ""  # Sql to create a table

    # LOG
    funcfile.writelog("Now")
    funcfile.writelog("SCRIPT: A001_ORACLE_TO_SQLITE")
    funcfile.writelog("-----------------------------")
    funcfile.writelog("TABLESET: " + s_table.upper())

    # MESSAGE TO ADMIN
    if l_mess:
        funcsms.send_telegram('', 'administrator', '<b>A001 ' + s_tables + ' Oracle import</b>')

    # DATABASE from text ***********************************************************

    # Read the database parameters from the 01_Database.csv file

    db = open(sl_path + "000a_Database.csv", newline=None)
    db_reader = csv.reader(db)

    # Read the DATABASE database data

    for row in db_reader:

        # Populate the database variables
        if row[0] == "DESC":
            continue
        else:
            
            funcfile.writelog("OPEN DATABASE: " + row[0])
            print("--------")
            print("DATABASE: " + row[0])
            print("--------")
            so_dsn = row[1]
            oracle_source_usr = row[2]
            oracle_source_pwd = row[3]
            de_fil = row[4]
            de_pat = row[5]
            de_sch = row[6]
            so_dri = row[7]
            oracle_source_dnq = row[8]
            i_mess = 0

        # Open the source ORACLE database
        # print("DSN="+so_dsn+";PWD="+oracle_source_pwd)
        # Driver={Oracle in OraClient11g_home1}; DBQ=myserver.mydomain.com:1521/mySid; Uid=myUsername; Pwd=myPassword;
        # with oracledb.connect(oracle_source_usr, oracle_source_pwd, so_dsn, ) as oracle_connection:
        # with pyodbc.connect("DSN="+so_dsn+"; PWD="+oracle_source_pwd) as oracle_connection:
        # with oracledb.connect(oracle_source_usr, oracle_source_pwd, so_dsn, ) as oracle_connection:
        # print(so_dsn)
        # print(oracle_source_usr)
        # print(oracle_source_pwd)
        # print(so_dri)
        # print(oracle_source_dnq)
        # print(de_pat)
        # print(de_fil)
        # oracle_connect_parameters = "Driver={"+so_dri+"}; DBQ="+oracle_source_dnq+"; UID="+oracle_source_usr+"; PWD="+oracle_source_pwd+";"
        # oracle_connect_parameters = "DSN="+so_dsn+"; PWD="+oracle_source_pwd
        # with pyodbc.connect(oracle_connect_parameters) as oracle_connection:
        #     oracle_cursor = oracle_connection.cursor()
            
        # Open the relevant Oracle database
        # Establish the database connection
        oracle_connection = oracledb.connect(user=oracle_source_usr, password=oracle_source_pwd, dsn=oracle_source_dnq)
        oracle_cursor = oracle_connection.cursor()

        # Open the destination SQLite database
        # print(de_pat+de_fil)
        with sqlite3.connect(de_pat+de_fil) as de_con:
            de_cur = de_con.cursor()

        # TABLE info from text *****************************************************

        # Read the table parameters from the 02_Table.csv file
        tb = open(sl_path + s_table, newline=None)
        tb_reader = csv.reader(tb)

        # Read the TABLE database data
        for row in tb_reader:

            tb_own = ""  # Table owner
            tb_nam = ""  # Table name
            tb_whe = ""  # SQL where clause
            tb_ord = ""  # SQL sort clause
            tb_alt = ""  # Table alternative name
            tb_sch = ""  # Table schedule
            tb_extract = False

            # Populate the table variables
            if row[0] == "DESC":
                continue
            else:
                if row[1] != de_fil:
                    # Ignore the header
                    continue
                elif funcstr.isNotBlank(row[7]):
                    if row[7] == "X":
                        # Do not do
                        continue
                    elif row[7] == funcdatn.get_today_name():
                        # Do if table schedule = day of week
                        tb_extract = True
                    elif row[7] == funcdatn.get_today_day_strip():
                        # Do if table schedule = day of month
                        tb_extract = True                    
                    else:
                        continue
                else:
                    tb_extract = True

                if tb_extract:
                    tb_own = row[2]  # Table owner
                    tb_nam = row[3]  # Table name
                    tb_whe = row[4]  # SQL where clause
                    tb_ord = row[5]  # SQL sort clause
                    tb_alt = row[6]  # Table alternative name
                    tb_sch = row[7]  # Table schedule
                    if l_mess and i_mess == 0:
                        i_mess += 1
                        if l_mess:
                            funcsms.send_telegram('', 'administrator', de_fil.lower())
                else:
                    continue

            # COLUMN info from text ************************************************

            # Read the table parameters from the 02_Table.csv file """
            co = open(sl_path + "000c_Column.csv", newline=None)
            co_reader = csv.reader(co) 

            # Read the COLUMN database data
            sco_nam = ""
            sco_lst = ""
            lco_lst = []

            sty_nam = ""
            sty_lst = ""
            lty_lst = []

            sma_nam = ""
            sma_lst = ""
            lma_lst = []
            
            sco_ana = ""
            sco_dro = ""

            for row in co_reader:

                # Populate the column variables
                if row[0] == "DESC":
                    continue
                else:
                    if row[1] != so_dsn:
                        continue
                    else:
                        if row[2] != tb_nam:
                            continue
                        else:
                            
                            # print("COLUMN: " + row[4])
                            
                            # Populate variables
                            co_nam = row[4]  # Column name
                            co_ana = row[8]  # Column alternate name
                            co_typ = row[5]  # Column type
                            co_len = row[6]  # Column width
                            co_dec = row[7]  # Column decimals
                            
                            # Populate column string with actual names
                            sco_nam += ''.join(row[4]) + ", "
                            sco_lst += row[4] + " "

                            # Populate column string with comlumn type
                            sty_nam += ''.join(row[5]) + ", "
                            sty_lst += row[5] + " "

                            # Populate column string with begin end date marker
                            s_startdate = """
                            STARTDATE*
                            STARTDATETIME*
                            KSTARTDATETIME*
                            DATE_FROM*
                            START_DATE_ACTIVE
                            """
                            s_enddate = """
                            ENDDATE*
                            ENDDATETIME*
                            KSTARTDATETIME*
                            DATE_TO*
                            END_DATE_ACTIVE
                            """
                            if row[4] in s_startdate:
                                sma_nam += "B, "
                                sma_lst += "B "
                            elif row[4] in s_enddate:
                                sma_nam += "E, "
                                sma_lst += "E "
                            else:
                                sma_nam += "N, "
                                sma_lst += "N "
                            
                            # Populate column string with alternate names
                            if co_typ == "NUMBER" and co_dec != "0":
                                sco_ana = sco_ana + ''.join(row[8]) + " REAL,"
                            elif co_typ == "NUMBER":
                                sco_ana = sco_ana + ''.join(row[8]) + " INTEGER,"
                            elif co_typ == "REAL":
                                sco_ana = sco_ana + ''.join(row[8]) + " REAL,"
                            else:
                                sco_ana = sco_ana + ''.join(row[8]) + " TEXT,"
                                
            # Create the sql create table variable
            sco_nam = sco_nam.rstrip(", ")
            lco_lst = sco_lst.split()
            sty_nam = sty_nam.rstrip(", ")
            lty_lst = sty_lst.split()
            sma_nam = sma_nam.rstrip(", ")
            lma_lst = sma_lst.split()

            if tb_alt != "":
                if "%CYEAR%" in tb_alt:
                    tb_alt = tb_alt.replace("%CYEAR%", funcdatn.get_current_year())
                if "%CMONTH%" in tb_alt:
                    tb_alt = tb_alt.replace("%CMONTH%", funcdatn.get_current_month())
                if "%PYEAR%" in tb_alt:
                    tb_alt = tb_alt.replace("%PYEAR%", funcdatn.get_previous_year())
                if "%PMONTH%" in tb_alt:
                    tb_alt = tb_alt.replace("%PMONTH%", funcdatn.get_previous_month())
                ssql_dro = "DROP TABLE IF EXISTS " + tb_alt
                ssql_create = "CREATE TABLE " + tb_alt + "(" + sco_ana + ")"
            else:
                ssql_dro = "DROP TABLE IF EXISTS " + tb_nam
                ssql_create = "CREATE TABLE " + tb_nam + "(" + sco_ana + ")"
            ssql_create = ssql_create.replace(",)", ")", 1)

            # print(ssql_create)

            funcfile.writelog("%t WRITE TABLE: " + tb_nam + "(" + tb_alt + ")")
            print("TABLE: " + tb_nam + "(" + tb_alt + ")")

            co.close()

            # Create the DESTINATION table
            de_cur.execute(ssql_dro)
            de_cur.execute(ssql_create)
              
            # Write the data
            ssql_str = "SELECT "
            ssql_str += sco_nam
            ssql_str += " FROM "
            if tb_own == "":
                ssql_str += tb_nam
            else:
                ssql_str += tb_own + "." + tb_nam
            if tb_whe != "":
                if "%CYEAR%" in tb_whe:
                    tb_whe = tb_whe.replace("%CYEAR%", funcdatn.get_current_year())
                if "%CYEARB%" in tb_whe:
                    tb_whe = tb_whe.replace("%CYEARB%", funcdatn.get_current_year_begin())
                if "%CYEARE%" in tb_whe:
                    tb_whe = tb_whe.replace("%CYEARE%", funcdatn.get_current_year_end())
                if "%CMONTHB%" in tb_whe:
                    tb_whe = tb_whe.replace("%CMONTHB%", funcdatn.get_current_month_begin())
                if "%CMONTHE%" in tb_whe:
                    tb_whe = tb_whe.replace("%CMONTHE%", funcdatn.get_current_month_end())
                if "%PYEAR%" in tb_whe:
                    tb_whe = tb_whe.replace("%PYEAR%", funcdatn.get_previous_year())
                if "%PMONTHB%" in tb_whe:
                    tb_whe = tb_whe.replace("%PMONTHB%", funcdatn.get_previous_month_begin())
                if "%PMONTHE%" in tb_whe:
                    tb_whe = tb_whe.replace("%PMONTHE%", funcdatn.get_previous_month_end())
                if "%PYEARB%" in tb_whe:
                    tb_whe = tb_whe.replace("%PYEARB%", funcdatn.get_previous_year_begin())
                if "%PYEARE%" in tb_whe:
                    tb_whe = tb_whe.replace("%PYEARE%", funcdatn.get_previous_year_end())
                    
                ssql_str = ssql_str + " " + tb_whe        
            if tb_ord != "":
                ssql_str = ssql_str + " ORDER BY " + tb_ord
                
            # print(ssql_str)
            # print("Name")
            # print(lco_lst)
            # print("Type")
            # print(lty_lst)
            # print("Marker")
            # print(lma_lst)

            for result in funcsys.ResultIter(oracle_cursor.execute(ssql_str)):
                c_text = ""
                c_test = ""
                c_data = "("
                i = 0

                # if result[4] == 41775:
                #     print("Result")
                #     print(result)

                for item in result:

                    if lty_lst[i] == "DATE":
                        if str(item) == "None" or str(item) == "":
                            if lma_lst[i] == "B":
                                c_test = "0001-01-01"
                            elif lma_lst[i] == "E":
                                c_test = "4712-12-31"
                        else:
                            c_test = str(item)
                            c_test = c_test[0:10]
                        c_data += "'" + c_test + "',"
                        
                    elif lty_lst[i] == "DATETIME":
                        if str(item) == "None" or str(item) == "":
                            if lma_lst[i] == "B":
                                c_test = "0001-01-01 00:00:00"
                            elif lma_lst[i] == "E":
                                c_test = "4712-12-31 23:59:59"
                        else:
                            c_test = str(item)
                        c_data += "'" + c_test + "',"
                        
                    elif lty_lst[i] == "NUMBER":
                        c_test = str(item)
                        c_test = c_test.replace("None", "0")
                        c_test = c_test.replace(".0", "")
                        c_data += "'" + c_test + "',"
                        
                    else:
                        c_test = str(item)
                        c_test = c_test.replace(",", "")
                        c_test = c_test.replace("'", "")
                        c_test = c_test.replace('"', '')
                        c_test = c_test.replace("None", "")
                        c_data += "'" + c_test + "',"
                        
                    i += 1
                    c_test = ""
                    
                c_data += ")"
                c_data = c_data.replace(",)", ")", 1)

                # if result[4] == 41775:
                #     print("Data")
                #     print(c_data)
                
                if tb_alt != "":
                    c_text = 'INSERT INTO ' + tb_alt + ' VALUES' + c_data
                else:
                    c_text = 'INSERT INTO ' + tb_nam + ' VALUES' + c_data

                if l_debug:
                    print(c_text)
                    
                try:
                    de_cur.execute(c_text)
                except:
                    funcfile.writelog(f"%t ERROR: {c_text}")

                # break

            de_con.commit()

            # Wait a few seconds for log file to close
            # time.sleep(10)

        # Close 02_Table.csv
        tb.close()

        # Display the number of tables in the destination
        for table in de_cur.execute("SELECT name FROM sqlite_master WHERE type = 'table'"):
            print("TABLE in DESTINATION: ", table[0])

        # Display the number of tables in the destination
        for table in de_cur.execute("SELECT name FROM sqlite_master WHERE type = 'view'"):
            print("VIEW in DESTINATION: ", table[0])        

        # Close the destination
        de_con.close()

        # Close the source
        oracle_connection.close()
       
    # Close 01_Database.csv
    db.close()

    # Close the log writer *********************************************************
    funcfile.writelog("--------------------------------")
    funcfile.writelog("COMPLETED: A001_ORACLE_TO_SQLITE")

    return


if __name__ == '__main__':
    try:
        oracle_to_sqlite()
    except Exception as e:
        funcsys.ErrMessage(e)
