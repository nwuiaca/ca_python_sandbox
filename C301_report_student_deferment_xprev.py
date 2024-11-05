
import sys

# OWN MODULE PATH
sys.path.append('../_my_modules')
sys.path.append('../')

# IMPORT MODULES
import funcdatn
import funcfile
import funcsys

# OPEN THE LOG WRITER
funcfile.writelog("Now")
funcfile.writelog("SCRIPT: C301_REPORT_STUDENT_DEFERMENT_PREV")
funcfile.writelog("------------------------------------------")

import C301_report_student_deferment
try:
    C301_report_student_deferment.Studdeb_deferments('prev',funcdatn.get_previous_year())
except Exception as e:
    funcsys.ErrMessage(e)

# CLOSE THE LOG WRITER
funcfile.writelog("Now")
funcfile.writelog("COMPLETED: C301_REPORT_STUDENT_DEFERMENT_PREV")
funcfile.writelog("---------------------------------------------")

