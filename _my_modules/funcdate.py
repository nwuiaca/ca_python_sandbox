"""
Functions to return all kinds of dates
Created on: 20 Feb 2018
Author: Albert v Rensburg (NWU:21162395)
"""

# Import the system objects
import datetime
from datetime import datetime
from datetime import timedelta
import calendar

""" Notes **********************************************************************
01. All these functions result in STRING returns.
"""

""" Short description of functions *********************************************
cur_day 		This day in DD format ex 01
cur_month		This month in MM format ex 01
cur_monthbegin 		This month begin date in YYYY-MM-DD format ex 2018-01-01
cur_monthend 		This month end date in YYYY-MM-DD format ex 2018-01-31
cur_monthendfile	This month end date in YYYYMMDD format ex 20190131
cur_monthendnext    This month end in YYYY-MM-DD format, but next month end date in x days before current month end. 
cur_monthfile           This month in YYYYMM format ex 201902
cur_year		This year in YYYY format ex 2018
cur_yearbegin		This year begin date in YYYY-MM-DD format ex 2018-01-01
cur_yearend		This year end date in YYYY-MM-DD format ex 2018-12-31
prev_month		Previous month in MM format ex 02
prev_monthbegin		Previous month begin date in YYYY-MM-DD format ex 2017-12-01
prev_monthend		Previous month end in YYYY-MM-DD format exec 2017-12-31
prev_monthendfile       Previous month end in YYYYMMDD format exec 20180228
prev_monthfile          Previous month in YYYYMM format exec 201802
prev_year		Previous year in YYYY format ex 2017
prev_yearbegin		Previous year begin date in YYYY-MM-DD format ex 2017-01-01
prev_yearend		Previous year end date in  YYYY-MM-DD format ex 2017-12-31
today			Todays date in YYYY-MM-DD format ex 2018-02-20
today_dayname		Todays weekday name ex Mon, Tue, Wed, Thu, Fri, Sat, Sun
today_file		Todays date in YYYYMMDD format for filing ex 20180220 
today_filemonth		Todays date in YYYYMM for filing without "-" ex 201801
"""


# Function This day in DD format ex 01
def cur_day():
    """Return today's day in DD format. Example 01"""
    return str(datetime.date.today().strftime("%d"))  # Current day


# Function This day in D format ex 1
def cur_daystrip():
    """Return today's day in D stripped format. Example 1"""
    return str(datetime.date.today().strftime("%e")).strip() #Current day


# Function This month in MM format ex 01
def cur_month():
    """Return today's month in MM format. Example 01"""
    return str(datetime.date.today().strftime("%m"))  # Current month


# Function This month begin date in YYYY-MM-DD format ex 2018-01-01
def cur_monthbegin():
    """Return the first day of the current month in YYYY-MM-DD format. Example 2023-04-01"""
    return cur_year() + "-" + cur_month() + "-" + "01"  # Current month begin


# Function This month end date in YYYY-MM-DD format ex 2018-01-31
def cur_monthend():
    """Return the last day of the current month in YYYY-MM-DD format. Example 2023-04-30"""
    s_retu = cur_year() + "-" + cur_month() + "-"
    if cur_month() in "01z03z05z07z08z10z12":
        s_retu += "31"
    elif cur_month() in "04z06z09z11":
        s_retu += "30"
    else:
        if calendar.isleap(int(cur_year())):
            s_retu += "29"
        else:
            s_retu += "28"
    return s_retu  # Current month end


# Function This month end date in YYYYMMDD format ex 20180131
def cur_monthendfile():
    if cur_month() in "01z03z05z07z08z10z12":
        s_retu = "31"
    elif cur_month() in "04z06z09z11":
        s_retu = "30"
    else:
        if calendar.isleap(int(cur_year())):
            s_retu = "29"
        else:
            s_retu = "28"
    return cur_year() + cur_month() + s_retu #Current month end file


# Function This month end date in YYYY-MM-DD format ex 2018-01-31
def cur_monthendnext(i_days=7):
    """
    Function to return month end date. Return next month if i_days (default=7) before current month end.
    December will return next year January month end date.
    @param i_days: Number of days before month end
    @return: Month end date in YYYY-MM-DD format
    """

    # SET VARIABLES
    s_year = cur_year()
    s_month = cur_month()
    if s_month in "01z03z05z07z08z10z12":
        s_lastd = "31"
    elif s_month in "04z06z09z11":
        s_lastd = "30"
    else:
        if calendar.isleap(int(s_year)):
            s_lastd = "29"
        else:
            s_lastd = "28"
    a_today = datetime.date.today()
    b_month = datetime.date(int(s_year), int(s_month), int(s_lastd))
    diff = b_month - a_today

    # CALCULATION
    if diff.days <= i_days:
        i_year = int(s_year)
        i_month = int(s_month)
        if i_month == 12:
            i_year += 1
            s_year = str(i_year)
            s_month = "01"
        else:
            i_month += 1
            if i_month < 10:
                s_month = "0" + str(i_month)
            else:
                s_month = str(i_month)
        if s_month in "01z03z05z07z08z10z12":
            s_lastd = "31"
        elif s_month in "04z06z09z11":
            s_lastd = "30"
        else:
            if calendar.isleap(int(s_year)):
                s_lastd = "29"
            else:
                s_lastd = "28"
    return s_year + "-" + s_month + "-" + s_lastd  # Return month end


#Function This month date in YYYYMM format ex 201801
def cur_monthfile():
    return cur_year() + cur_month() #Current month file


#Function This year in YYYY format ex 2018
def cur_year():
    return datetime.date.today().strftime("%Y") #Current year


#Function This year begin date in YYYY-MM-DD format ex 2018-01-01
def cur_yearbegin():
    return datetime.date.today().strftime("%Y") + "-01-01" #Current year begin date

#Function This year end date in YYYY-MM-DD format ex 2018-12-31
def cur_yearend():
    return datetime.date.today().strftime("%Y") + "-12-31" #Current year begin date

#Function Previous month in MM format
def prev_month():
    p_month = str(int(cur_month())-1)
    if int(p_month) < 10:
        p_month = "0" + p_month
    if p_month == "00":
        p_month = "01"
    return p_month #Previous month
	
#Function Previous month begin in YYYY-MM-DD format
def prev_monthbegin():
    p_year = cur_year()
    p_month = str(int(cur_month())-1)
    if int(p_month) < 10:
        p_month = "0" + p_month
    if p_month == "00":
        p_month = "12"
        p_year = prev_year()
    return p_year + "-" + p_month + "-01" #Previous month begin

#Function Previous month end in YYYY-MM-DD format
def prev_monthend():
    p_year = cur_year()
    p_month = str(int(cur_month())-1)
    if int(p_month) < 10:
        p_month = "0" + p_month
    if p_month == "00":
        p_month = "12"
        p_year = prev_year()
    if p_month in "01z03z05z07z08z10z12":
        s_retu = "31"
    elif p_month in "04z06z09z11":
        s_retu = "30"
    else:
        if calendar.isleap(int(p_year)):
            s_retu = "29"
        else:
            s_retu = "28"
    
    return p_year + "-" + p_month + "-" + s_retu #Previous month end

#Function Previous month end file in YYYYMMDD format
def prev_monthendfile():
    p_year = cur_year()
    p_month = prev_month()
    if p_month in "01z03z05z07z08z10z12":
        s_retu = "31"
    elif p_month in "04z06z09z11":
        s_retu = "30"
    else:
        if calendar.isleap(int(p_year)):
            s_retu = "29"
        else:
            s_retu = "28"
    return p_year + p_month + s_retu #Previous month end file

#Function Previous month file in YYYYMM format
def prev_monthfile():
    p_year = cur_year()
    p_month = str(int(cur_month())-1)
    if int(p_month) < 10:
        p_month = "0" + p_month
    if p_month == "00":
        p_month = "12"
        p_year = prev_year()
    return p_year + p_month #Previous month file

#Function Previous year in YYYY format
def prev_year():
    s_retu = "" + str(int(cur_year())-1)
    return s_retu #Previous year

#Function Previous year begin date YYYY-MM-DD format
def prev_yearbegin():
    return prev_year() + "-01-01" #Previous year begin

#Function Previous year end date YYYY-MM-DD format
def prev_yearend():
    return prev_year() + "-12-31" #Previous year end

#Function next year in YYYY format
def next_year():
    s_retu = "" + str(int(cur_year())+1)
    return s_retu #Next year


# Function next year begin date YYYY-MM-DD format
def next_yearbegin():
    """
    Return the first date of next year in YYYY-MM-DD format.
    :returns: First date of next year in YYYY-MM-DD format.
    """
    return next_year() + "-01-01"  # Previous year begin


# Function Today's date in YYYY-MM-DD format example 2018-01-01
def today():
    """
    Return today's date in YYYY-MM-DD format.
    :returns: Today's date in YYYY-MM-DD format
    """
    s_return = datetime.date.today().strftime("%Y")  # Current year
    s_return += "-" + datetime.date.today().strftime("%m")  # Current month
    s_return += "-" + datetime.date.today().strftime("%d")  # Current day
    return s_return


# Function to return today weekday name as in Mon, Tue
def today_dayname():
    return datetime.date.today().strftime("%a") #Abbreviated weekday	

# Function Todays date for filing without "-" ex 20180101


def today_file():
    s_retu = datetime.date.today().strftime("%Y") #Current year
    s_retu += datetime.date.today().strftime("%m") #Current month
    s_retu += datetime.date.today().strftime("%d") #Current day
    return s_retu

# Function Now date and time filing without "-" ex 20180101 00:00:00


def now_file():
    now = datetime.now()
    return now.strftime("%Y%m%d %H%M%S")

# Function Todays date for filing without "-" ex 201801


def today_filemonth():
    s_retu = datetime.date.today().strftime("%Y") #Current year
    s_retu += datetime.date.today().strftime("%m") #Current month
    return s_retu

#Function Today plus a number of days format ex 2018-01-01
def today_plusdays(i_days=14):
    s_retu = (datetime.date.today()+timedelta(days=i_days)).strftime("%Y") #Current year
    s_retu += "-" + (datetime.date.today()+timedelta(days=i_days)).strftime("%m") #Current month
    s_retu += "-" + (datetime.date.today()+timedelta(days=i_days)).strftime("%d") #Current day
    return s_retu


def yesterday():
    """
    YESTERDAY date ex 2019-09-06
    :return: str: YYYY-MM-DD
    """
    s_retu = (datetime.date.today()+timedelta(days=-1)).strftime("%Y")  # Current year
    s_retu += "-" + (datetime.date.today()+timedelta(days=-1)).strftime("%m")  # Current month
    s_retu += "-" + (datetime.date.today()+timedelta(days=-1)).strftime("%d")  # Yesterday
    return s_retu


def yesterday_file():
    """
    YESTERDAY date ex 20190906
    :return: str: YYYYMMDD
    """
    s_retu = (datetime.date.today()+timedelta(days=-1)).strftime("%Y")  # Current year
    s_retu += (datetime.date.today()+timedelta(days=-1)).strftime("%m")  # Current month
    s_retu += (datetime.date.today()+timedelta(days=-1)).strftime("%d")  # Yesterday
    return s_retu
