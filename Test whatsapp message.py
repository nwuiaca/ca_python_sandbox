# TEST GROUND FOR SCRIPTS
# CAN BE DELETED - NO PERMANENT CODE STORED HERE

# SYSTEM MODULES
from _my_modules import funcsms

# IMPORT OWN MODULES

# SEND FIRST MESSAGE
funcsms.send_whatsapp()

# ASK FOR NEXT MESSSAGE
s_mess = input("Sms message: ")

# SEND SECOND MESSAGE
funcsms.send_whatsapp(s_mess)



