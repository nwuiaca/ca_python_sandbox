"""
Script to EXECUTE the VSS LISTS script.
"""

# IMPORT OWN MODULES
from _my_modules import funcfile
from _my_modules import funcsys
import B007_vss_period_list

# OPEN THE LOG WRITER
funcfile.writelog("Now")
funcfile.writelog("SCRIPT: B007_RUN_VSS_PERIOD_LIST")
funcfile.writelog("--------------------------------")

# RUN KFS LISTS
try:
    B007_vss_period_list.vss_period_list("prev")
except Exception as e:
    funcsys.ErrMessage(e)

# CLOSE THE LOG WRITER
funcfile.writelog("Now")
funcfile.writelog("COMPLETED: B007_VSS_PERIOD_LIST")
funcfile.writelog("-------------------------------")