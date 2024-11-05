"""
This module does various date calculations and return them.

It is part of the NWUIACA project, and is used throughout to obtain dates in various formats.

Author: Albert Janse van Rensburg (NWU:21162395)
Date: 2023-12-03
"""

# Import standard libraries
import calendar
from datetime import date, datetime, timedelta

# Import related third party libraries

# Import local application modules

# Metadata
__author__ = 'Albert Janse van Rensburg'
__email__ = '21162395@nwu.ac.za'
__version__ = '1.0.0'
__status__ = 'Development'  # Production

"""
INDEX

get_today_date(): Return today's date in YYYY-MM-DD format.
get_today_date_file(): Return today's date in YYYYMMDD format.
get_today_name(): Return today's weekday name.
get_today_day(): Return today's day as a string in DD format.
get_today_day_strip(): Return today's day as a string in 'D' format.
get_today_plusdays(i_days=14): Return today's date plus the number of days.
get_current_month(): Return today's month as a string in DD format.
get_current_month_begin(): Return the first day of the current month in YYYY-MM-DD format
get_current_month_end(): Return the last day of the current month in YYYY-MM-DD format.
get_current_month_end_next(): Return the end of this or the next month depending on the cutoff_days
get_current_month_file(): Get the current month in YYYYMM format.
get_current_year(): Return today's year as a string in YYYY format.
get_current_year_begin(): Return the date for the first day of this year.
get_current_year_end(): Return the date for the last day of this year.
get_yesterday_date(): Return yesterday's date in YYYY-MM-DD format.
get_yesterday_date_file(): Return yesterday's date in YYYYMMDD format.
get_previous_month(): Return the previous month as a string in MM format.
get_previous_month_begin(): Return the first day of the previous month in YYYY-MM-DD format.
get_previous_month_end(): Return the last day of the previous month as a string in YYYY-MM-DD format.
get_previous_month_end_file(): Return the last day of the previous month as a string in YYYYMMDD format.
get_previous_month_file(): Return the last day of the previous month as a string in YYYYMM format.
get_previous_year(): Return previous year as a string in YYYY format.
get_previous_year_begin(): Return the first day of the previous year as a string in YYYY-MM-DD format.
get_previous_year_end(): Return the last day of the previous year as a string in YYYY-MM-DD format.
get_next_year(): Return next year as a string in YYYY format.
get_next_year_begin(): Return first day of next year as a string in YYYY-MM-DD format.

"""


def get_today_date():
    """
    Today's date.
    :return: str: YYYY-MM-DD
    """
    return date.today().strftime("%Y-%m-%d")


def get_today_date_file():
    """
    Today's date without special characters.
    :return: str: YYYYMMDD
    """
    return date.today().strftime("%Y%m%d")


def get_today_name():
    """
    Today's weekday name. Example Mon, Tue, Wed ...
    :return: str: Weekday
    """
    # Get the current local datetime
    today = datetime.now()
    return today.strftime('%a')


def get_today_day():
    """
    Today's day. Example 01.
    :return: str: DD
    """
    return date.today().strftime("%d")


def get_today_day_strip():
    """
    Today's day. Example 1.
    :return: str: D
    """
    return str(date.today().day)


def get_today_plusdays(i_days=14):
    """
    Today plus a number of days.
    :param i_days: Number of days to add to today's date.
    :return: str: YYYY-MM_DD
    """
    future_date = datetime.today() + timedelta(days=i_days)
    return future_date.strftime("%Y-%m-%d")


def get_now_file():
    """
    The current date and time
    :return: str: YYYY-MM-DD 00:00:00
    """
    now = datetime.now()
    return now.strftime("%Y-%m-%d %H:%M:%S")


def get_current_month():
    """
    The current month. Example 01.
    :return: str: MM
    """
    return date.today().strftime("%m")


def get_current_month_begin():
    """
    The first day of the current month.
    :return: str: YYYY-MM-DD
    """
    today = datetime.today()
    first_day_of_month = datetime(today.year, today.month, 1)
    return first_day_of_month.strftime("%Y-%m-%d")


def get_current_month_end():
    """
    The last day of the current month.
    :return: str: YYYY-MM-DD
    """
    today = datetime.now()
    year = today.year
    month = today.month
    last_day = calendar.monthrange(year, month)[1]
    return f"{year}-{month:02d}-{last_day}"


def get_current_month_end_next(cutoff_days: int = 7):
    """
    Function to return the end of this month or the next month.
    :param cutoff_days: Cutoff days
    :return: str: YYYY-MM-DD
    """

    """
    In essence, this function allows you to determine when the cutoff is for the end of the month, taking into
    account a buffer of a certain number of days specified by cutoff_days. If today’s date is within cutoff_days
    of the end of the current month, it provides the last day of the next month; otherwise, it gives you the last
    day of the current month.
    """

    # Get today's date
    today = date.today()
    year = today.year
    month = today.month

    # Calculate the last day of the current month
    last_day_of_month = calendar.monthrange(year, month)[1]
    # Create a date for the last day of the current month
    end_of_month = date(year, month, last_day_of_month)

    if (end_of_month - today).days <= cutoff_days:
        # If we're within days of the end of this month, find the end of the next month
        if month == 12:
            year += 1
            month = 1
        else:
            month += 1
        last_day_of_next_month = calendar.monthrange(year, month)[1]
        end_of_next_month = date(year, month, last_day_of_next_month)
        return end_of_next_month.strftime("%Y-%m-%d")
    else:
        return end_of_month.strftime("%Y-%m-%d")


def get_current_month_file():
    """
    The current year and month without special characters.
    :return: str: YYYYMM
    """
    # Get the current month in YYYYMM format
    return date.today().strftime("%Y%m")


def get_current_year():
    """
    The current year.
    :return: str: YYYY
    """
    """Return today's year as a string in YYYY format, e.g., '2023' for 2023."""
    return date.today().strftime("%Y")


def get_current_year_begin():
    """
    The first day of the current year.
    :return: str: YYYY-MM-DD
    """
    today = date.today()
    first_day_of_year = date(today.year, 1, 1)
    return first_day_of_year.strftime("%Y-%m-%d")


def get_current_year_end():
    """
    The last day of the current year.
    :return: str: YYYY-MM-DD
    """
    today = date.today()
    first_day_of_year = date(today.year, 12, 31)
    return first_day_of_year.strftime("%Y-%m-%d")


def get_yesterday_date():
    """
    Yesterday's date.
    :return: str: YYYY-MM-DD
    """
    yesterday_date = date.today() - timedelta(days=1)
    return yesterday_date.strftime("%Y-%m-%d")


def get_yesterday_date_file():
    """
    Yesterday's date without special characters.
    :return: str: YYYYMMDD
    """
    yesterday_date = date.today() - timedelta(days=1)
    return yesterday_date.strftime("%Y%m%d")


def get_previous_month():
    """
    The previous month. Example 01 for January.
    :return: str: MM
    """
    today = date.today()
    first_of_this_month = date(today.year, today.month, 1)
    last_day_of_previous_month = first_of_this_month - timedelta(days=1)
    return last_day_of_previous_month.strftime("%m")


def get_previous_month_begin():
    """
    The first day of the previous month.
    :return: str: YYYY-MM-DD
    """
    today = datetime.today()
    first_day_of_current_month = datetime(today.year, today.month, 1)
    last_day_of_previous_month = first_day_of_current_month - timedelta(days=1)
    first_day_of_previous_month = datetime(last_day_of_previous_month.year, last_day_of_previous_month.month, 1)
    return first_day_of_previous_month.strftime("%Y-%m-%d")


def get_previous_month_end():
    """
    The last day of the previous month.
    :return: str: YYYY-MM-DD
    """
    today = date.today()
    first_of_this_month = date(today.year, today.month, 1)
    last_day_of_previous_month = first_of_this_month - timedelta(days=1)
    return last_day_of_previous_month.strftime("%Y-%m-%d")


def get_previous_month_end_file():
    """
    The last day of the previous month without special characters.
    :return: str: YYYYMMDD
    """
    today = date.today()
    first_of_this_month = date(today.year, today.month, 1)
    last_day_of_previous_month = first_of_this_month - timedelta(days=1)
    return last_day_of_previous_month.strftime("%Y%m%d")


def get_previous_month_file():
    """
    The year and previous month without special characters.
    :return: str: YYYYMM
    """
    today = date.today()
    first_of_this_month = date(today.year, today.month, 1)
    last_day_of_previous_month = first_of_this_month - timedelta(days=1)
    return last_day_of_previous_month.strftime("%Y%m")


def get_previous_year():
    """
    The previous year.
    :return: str: YYYY
    """
    last_year = date.today().year - 1
    return str(last_year)


def get_previous_year_begin():
    """
    The first day of the previous year.
    :return: str: YYYY-MM-DD
    """
    last_year = date.today().year - 1
    first_day_of_year = date(last_year, 1, 1)
    return first_day_of_year.strftime("%Y-%m-%d")


def get_previous_year_end():
    """
    The last day of the previous year.
    :return: str: YYYY-MM-DD
    """
    last_year = date.today().year - 1
    first_day_of_year = date(last_year, 12, 31)
    return first_day_of_year.strftime("%Y-%m-%d")


def get_next_year():
    """
    The next year.
    :return: str: YYYY
    """
    last_year = date.today().year + 1
    return str(last_year)


def get_next_year_begin():
    """
    The first day of next year.
    :return: str: YYYY-MM-DD
    """
    last_year = date.today().year + 1
    first_day_of_year = date(last_year, 1, 1)
    return first_day_of_year.strftime("%Y-%m-%d")


if __name__ == '__main__':
    print(get_today_date())
    print(get_today_date_file())
    print(get_today_name())
    print(get_today_day())
    print(get_today_day_strip())
    print(get_today_plusdays(10))
    print(get_today_plusdays(100))
    print(get_now_file())
    print(get_current_month())
    print(get_current_month_begin())
    print(get_current_month_end())
    print(get_current_month_end_next())
    print(get_current_month_end_next(30))
    print(get_current_month_file())
    print(get_current_year())
    print(get_current_year_begin())
    print(get_current_year_end())
    print(get_yesterday_date())
    print(get_yesterday_date_file())
    print(get_previous_month())
    print(get_previous_month_begin())
    print(get_previous_month_end())
    print(get_previous_month_end_file())
    print(get_previous_month_file())
    print(get_previous_year())
    print(get_previous_year_begin())
    print(get_previous_year_end())
    print(get_next_year())
    print(get_next_year_begin())
