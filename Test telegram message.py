# TEST GROUND FOR SCRIPTS
# CAN BE DELETED - NO PERMANENT CODE STORED HERE

# SYSTEM MODULES
import csv

# IMPORT OWN MODULES
from _my_modules import funcsms

# FETCH UPDATES / ALL MESSAGES FROM TELEGRAM
# print(funcsms.get_telegram_all())

# TEST GET USER DETAILS
s_receive = funcsms.get_mobile_user(1111961873, "role", True)
print(s_receive)



