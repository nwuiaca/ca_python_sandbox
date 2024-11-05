"""
SCRIPT TO INITIALISE THE SYSTEM LOGGING FUNCTION
Author: Albert Janse van Rensburg (NWU:21162395)
Developed: 25 Sep 2023
"""

# Import python libraries
import configparser
import logging
import datetime
import traceback

# Import own libraries
from _my_modules import funcmail


def handle_exception(e):
    error_message = traceback.format_exc()
    print(error_message)  # print to console or log file
    funcmail.send_mail('std_fail_nwu', 'NWUIACA Error message', error_message)
    funcsms.send_telegram("Dear", "administrator", error_message)


# send_email('Python Application Error', error_message, 'your_email@gmail.com', 'receiver_email@gmail.com', 'smtp.gmail.com', 587, 'your_email@gmail.com', 'your_password')


def setup_logger():

    # Read from the configuration file
    config = configparser.ConfigParser()
    config.read('.config.ini')
    logger_name = config.get('LOGGER', 'name')
    file_path = config.get('LOGGER', 'file_path')
    file_name = config.get('LOGGER', 'file_name')
    file_extension = config.get('LOGGER', 'file_extension')

    # Initialize the logger
    logger = logging.getLogger(f'{logger_name}')
    logger.setLevel(logging.DEBUG)  # Set level to DEBUG

    # Get today's date and build the log file name
    today = datetime.date.today()
    today_str = today.strftime("%Y%m%d")
    log_file = f'{file_path}{file_name}{today_str}{file_extension}'

    # Create file handler which logs even debug messages
    fh = logging.FileHandler(log_file)
    fh.setLevel(logging.DEBUG)

    ch = logging.StreamHandler()
    ch.setLevel(logging.ERROR)

    # Create formatter and add it to the handlers
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)

    # Add the handlers to the logger
    logger.addHandler(fh)
    logger.addHandler(ch)

    return logger
