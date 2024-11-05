"""
FUNCTIONS USED BY THE SYSTEM
29 Sep 2023
"""

# Import python libraries
import traceback

def ResultIter(cursor, size=10000):
    """
    An iterator to keep memory usage down
    :param size: Array size before commit
    :param cursor: Database cursor
    :return: Result of fetchmany
    """
    while True:
        results = cursor.fetchmany(size)
        if not results:
            break
        for result in results:
            yield result


def ErrMessage(e, l_mail: bool = True, s_subject: str = 'NWUIACA Error Message', s_body: str = 'SCRIPT ERROR'):
    """
    Error message handler.
    :param e: Error object
    :param l_mail: Send mail and message
    :param s_subject: Mail subject
    :param s_body: Mail body
    :return: Nothing
    """

    # IMPORT OWN MODULES
    from _my_modules import funcconf
    from _my_modules import funcfile
    from _my_modules import funcmail
    from _my_modules import funcsms

    # DECLARE VARIABLES
    l_debug: bool = False
    if not funcconf.l_mail_project:
        l_mail = False

    # Remove special characters from the error object for messaging purposes
    s_mess = str(e).replace("'", "")
    s_mess = s_mess.replace('"', '')
    s_mess = s_mess.replace(':', '')
    s_mess = s_mess.replace('[', '')
    s_mess = s_mess.replace(']', '')
    s_mess = s_mess.replace('(', '')
    s_mess = s_mess.replace(')', '')
    s_mess = s_mess.replace('< ', 'smaller than sign ')
    s_mess = s_mess.replace(' >', 'greater than sign ')

    # DISPLAY
    if l_debug:
        print('funcsys.ErrMessage')
        print(s_mess)
        print("E: ", e)
        print("TYPE(E): ", type(e))
        print("TYPE(E)NAME: ", type(e).__name__)
        print("JOIN(E.ARGS: ", e.args)
        print('funcsys.ErrMessage.Extended')
    error_message = traceback.format_exc()
    print(error_message)

    # DEFINE THE ERROR TEMPLATE AND MESSAGE
    # template = "Exception type {0} occurred. Arguments:\n{1!r}"
    # message = template.format(type(e).__name__, e.args)
    # print("MESSAGE: ", message)

    # WRITE ERROR TO LOG
    # funcfile.writelog("%t ERROR: " + type(e).__name__)
    # funcfile.writelog("%t ERROR: " + "".join(e.args))
    # funcfile.writelog("%t ERROR: " + s_mess)
    funcfile.writelog('%t ERROR: ' + str(e))

    # SEND MAIL
    if l_mail:
        # s_body1 = s_body + '\n' + type(e).__name__ + '\n' + "".join(e.args)
        # s_body1 = s_body + '\n' + s_mess
        s_body1 = s_body + '\n' + error_message
        funcmail.send_mail('std_fail_nwu', s_subject, s_body1)

    # SEND MESSAGE
    if funcconf.l_mess_project:
        # s_body1 = s_body + ' | ' + type(e).__name__ + ' | ' + "".join(e.args)
        s_body1 = s_body + ' | ' + s_mess
        # s_body1 = s_body + ' | ' + error_message
        funcsms.send_telegram('Dear', 'administrator', s_body1)

    return


def tablerowcount(so_curs, table):
    """
    Count the number of rows in a table.
    :param so_curs: Database cursor
    :param table: Table to count
    :rtype: int
    :return: Number of rows
    """
    so_curs.execute("SELECT COUNT(*) FROM " + table)
    x = so_curs.fetchone()
    return int(x[0])
