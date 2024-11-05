# TEST GROUND FOR SCRIPTS
# CAN BE DELETED - NO PERMANENT CODE STORED HERE

# SYSTEM MODULES

# IMPORT OWN MODULES
from datetime import date

from _my_modules import funcfile
from _my_modules import funcsms

# LOGGING
# s_path = "S:/LOGS/"
# s_file = "Python_log_" + datetime.datetime.now().strftime("%Y%m%d") + ".txt"
# funcfile.writelog("Now", s_path, s_file)
funcfile.writelog("Now","","","w")
# funcfile.writelog("SCRIPT: OPEN PROJECT NWU INTERNAL AUDIT CONTINUOUS AUDIT")
# funcfile.writelog("--------------------------------------------------------")


d0 = date(2020, 3, 27)
d1 = date(2022, 4, 4)
delta = d1 - d0
print(delta. days)
print()

funcsms.send_telegram('Hi','administrator','Test',1111961873)


