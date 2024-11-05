
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
funcfile.writelog("SCRIPT: C301_REPORT_STUDENT_DEFERMENT_PERI")
funcfile.writelog("------------------------------------------")

# OBTAIN DEFERMENT YEAR
print("Note")
print("----")
print("1. Period Students should exist in VSS for the year.")
print("2. VSS Transactional data should exist in VSS for the year.")
print("")
s_year = input("Deferments year? (yyyy) ")
print("")

funcfile.writelog("DEFERMENT YEAR " + s_year)

import C301_report_student_deferment
try:
    C301_report_student_deferment.Studdeb_deferments('peri',s_year)
except Exception as e:
    funcsys.ErrMessage(e)

# CLOSE THE LOG WRITER
funcfile.writelog("Now")
funcfile.writelog("COMPLETED: C301_REPORT_STUDENT_DEFERMENT_PERI")
funcfile.writelog("---------------------------------------------")

