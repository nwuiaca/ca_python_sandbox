# IMPORT MODULES
import C302_test_student_fee
from _my_modules import funcfile
from _my_modules import funcsys

# OPEN THE LOG WRITER
funcfile.writelog("Now")
funcfile.writelog("SCRIPT: C302_TEST_STUDENT_FEE_RUN_PREV")
funcfile.writelog("--------------------------------------")

try:
    C302_test_student_fee.student_fee("prev")
except Exception as e:
    funcsys.ErrMessage(e)

# CLOSE THE LOG WRITER
funcfile.writelog("Now")
funcfile.writelog("COMPLETED: C302_TEST_STUDENT_FEE_RUN_PREV")
funcfile.writelog("-----------------------------------------")
