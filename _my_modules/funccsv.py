""" FUNCCSV.PY *****************************************************************
Script to write a csv file from tables
Copyright (c) AB Janse van Rensburg 19 Feb 2018
"""

#Import python objects
import csv
import pyodbc
import sqlite3


#Function to read table columns from sqlite3 table
def get_colnames_sqlite(os_cur,s_tab):

    """ Parameter
    os_cur = ODBC Source Cursor
    s_tab = Data table name
    """
    
    s_data = ""
    for row in os_cur.execute("PRAGMA table_info(" + s_tab + ")").fetchall():
        s_data = s_data + row[1] + " "

    return s_data.split() #Return column headers in list format

#Function to read table columns from oracle table
def get_colnames_oracle(os_cur,s_tab):

    """ Parameter
    os_cur = ODBC Source Cursor
    s_tab = Data table name
    """
    
    s_data = ""
    for row in os_cur.execute("SELECT column_name FROM ALL_TAB_COLUMNS WHERE table_name = '" + s_tab + "' ORDER BY column_id").fetchall():
        s_data = s_data + row[1] + " "

    return s_data.split() #Return column headers in list format

#Function to write csv file from ODBC table source
def write_data(os_cur, ss_sch, ss_tab, sd_fol, sd_nam, ld_hea, s_type="w", s_fext=".csv"):
    
    # Extract and read the data
    
    """ Parameter
    os_cur = ODBC Source Cursor
    ss_sch = Source schema
    ss_tab = Data table name
    sd_fol = Data file destination
    sd_nam = Destination file name
    ld_hea = Destination file header record
    """
    
    f = open(sd_fol + sd_nam + s_fext, s_type)

    if s_fext[-3:] == "txt":
        csvf = csv.writer(f, lineterminator='\n')
    else:
        csvf = csv.writer(f, lineterminator='\n', quoting=csv.QUOTE_NONNUMERIC)

    if s_type == "w":
        csvf.writerow(ld_hea)
    for row in os_cur.execute("select * from " + ss_sch + "." + ss_tab).fetchall():
        csvf.writerow(row)
        
    f.close()

def write_header(o_odbc, c_whd, c_wht):
    
    # Write a column header file
    
    """
    Parameter
        whf = The name of the table for which to obtain header record
    """

    f = open(c_whd + "column_names.csv", "w")
    csvf = csv.writer(f, lineterminator='\r', quoting=csv.QUOTE_NONNUMERIC)
    for row in o_odbc.execute("PRAGMA table_info(" + c_wht + ")").fetchall():
        csvf.writerow(row)
    f.close()

def read_header(c_rhd):
    
    # Read the columns from the column header file
    
    header_list = ""
    f = open(c_rhd + "column_names.csv", "r")
    reader = csv.reader(f)
    for row in reader:
        #c_data = row[1]
        #header_list = header_list + ''.join(row[1]) + ","
        header_list = header_list + row[1] + " "
    #header_list = header_list.rstrip(",")
    f.close()
    return header_list.split()
