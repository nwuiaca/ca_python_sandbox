"""
Script to define global variables
"""

# IMPORT PYTHON PACKAGES
import time
import datetime

# IMPORT OWN MODULES
from _my_modules import funcdatn
from _my_modules import funcdatn  # Own modules

# DECLARE GLOBAL VARIABLES

# TELEGRAM VARIABLES
l_tel_use_nwu: bool = True  # Use NWU or Albert robot

# DRIVE VARIABLES
drive_system: str = "S:/"  # System drive
drive_data_raw: str = "W:/"  # Raw data drive
drive_data_results: str = "R:/"  # Results drive

# CORRESPONDENCE VARIABLES
l_mail_project: bool = True  # Flag to indicate project sending emails.
l_mail_use_nwu: bool = False
l_mail_use_sendgrid: bool = True
l_mail_use_albert: bool = False
l_mess_project: bool = True  # Flag to indicate project sending text messages.

# PROJECT RUN VARIABLES
l_stop_project: bool = False  # Flag to stop system and project
l_run_system: bool = True  # Flag to indicate system running or stopping.
l_run_people_test: bool = True  # Flag to indicate people tests to run
l_run_vss_test: bool = True  # Flag to indicate vss tests to run
l_run_kfs_test: bool = True  # Flag to indicate kfs tests to run

# SCHEDULE TIME VARIABLES
l_run_group4: bool = False
l_run_group5: bool = False
l_run_group6: bool = False
l_run_group7: bool = False

# VACUUM SCHEDULE PROCESS
if time.strftime("%R", time.localtime()) <= "16:45":
    d_run_vacuum: datetime = datetime.datetime.strptime(funcdatn.get_today_date() + " 17:00:00", "%Y-%m-%d %H:%M:%S")
else:
    d_run_vacuum: datetime = datetime.datetime.strptime(funcdatn.get_today_date() + " 17:00:00", "%Y-%m-%d %H:%M:%S") + \
                             datetime.timedelta(days=1)

# LARGE SCHEDULE PROCESS
if time.strftime("%R", time.localtime()) <= "19:55":
    d_run_large: datetime = datetime.datetime.strptime(funcdatn.get_today_date() + " 21:00:00", "%Y-%m-%d %H:%M:%S")
else:
    d_run_large: datetime = datetime.datetime.strptime(funcdatn.get_today_date() + " 21:00:00", "%Y-%m-%d %H:%M:%S") + \
                            datetime.timedelta(days=1)

# SMALL SCHEDULE PROCESS
if time.strftime("%R", time.localtime()) <= "23:59":
    d_run_small: datetime = datetime.datetime.strptime(funcdatn.get_today_date() + " 23:02:00", "%Y-%m-%d %H:%M:%S") + \
                           datetime.timedelta(hours=2)
else:
    d_run_small: datetime = datetime.datetime.strptime(funcdatn.get_today_date() + " 23:02:00", "%Y-%m-%d %H:%M:%S") + \
                             datetime.timedelta(days=1, hours=2)

# TEST SCHEDULE PROCESS
if time.strftime("%R", time.localtime()) <= "23:59":
    d_run_test: datetime = datetime.datetime.strptime(funcdatn.get_today_date() + " 23:00:00", "%Y-%m-%d %H:%M:%S") + \
                           datetime.timedelta(hours=7)
else:
    d_run_test: datetime = datetime.datetime.strptime(funcdatn.get_today_date() + " 23:00:00", "%Y-%m-%d %H:%M:%S") + \
                             datetime.timedelta(days=1, hours=7)
