"""
NWU INTERNAL AUDIT ROBOT USING TELEGRAM MESSAGING
Press Ctrl-C on the command line or send a signal to the process to stop the bot.
Author: Albert B Janse van Rensburg (NWU:21162395)
Date created: 24 September 2023
"""

# IMPORT PYTHON MODULES
from telegram.ext import Updater, CommandHandler, MessageHandler, Filters
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.executors.pool import ThreadPoolExecutor, ProcessPoolExecutor
import configparser
import logging
import datetime

# IMPORT OWN MODULES
from _my_modules import funcbott
from _my_modules import funcconf
from _my_modules import funcmail
from _my_modules import funclogg
from _my_modules import funcsms
from _my_modules import funcsys


def main():
    """
    Script to start the telegram bot and schedule functions
    :return: Nothing
    """

    # Declare variables
    s_function: str = 'A000_nwuiaca_xdev1.main'

    # Setup logging
    logger = funclogg.setup_logger()
    logger.info('Starting the main robot scheduler.')

    # Read from the configuration file
    config = configparser.ConfigParser()
    config.read('.config.ini')

    try:

        # START THE BOT
        if funcconf.l_tel_use_nwu:
            updater = Updater(config.get('API', 'token_telegram_nwu'), use_context=True)  # NWU
        else:
            updater = Updater(config.get('API', 'token_telegram_albert'), use_context=True)  # Albert

        dp = updater.dispatcher

        # ON DIFFERENT COMMANDS - ANSWER IN TELEGRAM
        dp.add_handler(CommandHandler("name", funcbott.name, pass_args=True))
        dp.add_handler(CommandHandler("hi", funcbott.hi))
        dp.add_handler(CommandHandler("helping", funcbott.helping))
        dp.add_handler(CommandHandler("report", funcbott.report, pass_args=True))
        dp.add_handler(CommandHandler("ai", funcbott.ai, pass_args=True))
        dp.add_handler(CommandHandler("run", funcbott.run, pass_args=True))
        dp.add_handler(CommandHandler("set", funcbott.set_schedule, pass_args=True))
        dp.add_handler(CommandHandler("switch", funcbott.switch, pass_args=True))

        # ON NON COMMAND - DO NOT UNDERSTAND MESSAGE
        dp.add_handler(CommandHandler("stop", funcbott.stop))
        dp.add_handler(MessageHandler(Filters.text, funcbott.echo))

        # LOG ALL ERRORS
        dp.add_error_handler(funcbott.error)

        # SEND OPENING MESSAGES
        funcsms.send_telegram("Dear", "administrator", "the <b>server</b> is up and running, and you may talk to me!")

        executors = {
            'default': ThreadPoolExecutor(20),
            'processpool': ProcessPoolExecutor(5)
        }
        job_defaults = {
            'coalesce': False,
            'max_instances': 3
        }

        scheduler = BackgroundScheduler(executors=executors, job_defaults=job_defaults)

        scheduler.add_job(group6, 'cron', hour=1, minute=0)
        scheduler.add_job(group6, 'cron', hour=1, minute=2)
        scheduler.add_job(group7, 'cron', hour=5, minute=0)
        scheduler.add_job(group7, 'cron', hour=5, minute=2)
        scheduler.add_job(group1, 'cron', hour=7, minute=45)
        scheduler.add_job(group2, 'cron', hour=12, minute=55)
        scheduler.add_job(group3, 'cron', hour=16, minute=30)
        scheduler.add_job(group4, 'cron', hour=17, minute=30)
        scheduler.add_job(group4, 'cron', hour=17, minute=32)
        scheduler.add_job(group5, 'cron', hour=17, minute=45)
        scheduler.add_job(group5, 'cron', hour=17, minute=47)

        scheduler.start()

        updater.start_polling()

        updater.idle()

    except Exception as e:
        funcsys.ErrMessage(e)


def group1():
    """
    Script to run workday morning functions
    :return: Nothing
    """

    # Setup variables
    s_function = 'A000_nwuiaca_xdev1.group1'
    funcconf.l_run_group4 = False
    funcconf.l_run_group5 = False
    funcconf.l_run_group6 = False
    funcconf.l_run_group7 = False

    # Send message
    try:
        if datetime.datetime.now().weekday() < 5:
            funcsms.send_telegram("Dear", "administrator", "your working day started, and I'm up and running!")
    except Exception as e:
        funcsys.ErrMessage(e)


def group2():
    """
    Script to run workday noon functions
    :return: Nothing
    """

    # Import function
    import A000_nwuiaca_group2
    funcconf.l_run_group4 = False
    funcconf.l_run_group5 = False
    funcconf.l_run_group6 = False
    funcconf.l_run_group7 = False

    # Send message
    try:
        if datetime.datetime.now().weekday() < 5:
            funcsms.send_telegram("Dear", "administrator", "how about going for a walk, while I'm keeping up!")
    except Exception as e:
        funcsys.ErrMessage(e)

    # Run functions
    try:
        A000_nwuiaca_group2.group2_functions()
    except Exception as e:
        funcsys.ErrMessage(e)


def group3():
    """
    Script to run workday afternoon functions
    :return: Nothing
    """

    # Import function
    import A000_nwuiaca_group3
    funcconf.l_run_group4 = False
    funcconf.l_run_group5 = False
    funcconf.l_run_group6 = False
    funcconf.l_run_group7 = False

    # Send message
    try:
        if datetime.datetime.now().weekday() < 5:
            funcsms.send_telegram("Dear", "administrator", "you've done your part today, while I'm prepping!")
    except Exception as e:
        funcsys.ErrMessage(e)

    # Run functions
    try:
        A000_nwuiaca_group3.group3_functions()
    except Exception as e:
        funcsys.ErrMessage(e)


def group4():
    """
    Script to run workday early evening functions
    :return: Nothing
    """

    # Import function
    import A000_nwuiaca_group4

    # Send message
    try:
        if datetime.datetime.now().weekday() < 5:
            funcsms.send_telegram("Dear", "administrator", "good evening!")
    except Exception as e:
        funcsys.ErrMessage(e)

    # Run functions
    try:
        if not funcconf.l_run_group4:
            funcconf.l_run_group4 = True
            A000_nwuiaca_group4.group4_functions()
    except Exception as e:
        funcsys.ErrMessage(e)


def group5():
    """
    Script to run workday late evening functions
    :return: Nothing
    """

    # Import function
    import A000_nwuiaca_group5

    # Send message
    try:
        if datetime.datetime.now().weekday() < 5:
            funcsms.send_telegram("Dear", "administrator", "good night, sleep tight!")
    except Exception as e:
        funcsys.ErrMessage(e)

    # Run functions
    try:
        if not funcconf.l_run_group5:
            funcconf.l_run_group5 = True
            A000_nwuiaca_group5.group5_functions()
    except Exception as e:
        funcsys.ErrMessage(e)


def group6():
    """
    Script to run midnight functions
    :return: Nothing
    """

    # Import function
    import A000_nwuiaca_group6

    # Send message
    try:
        if datetime.datetime.now().weekday() < 5:
            funcsms.send_telegram("Dear", "administrator", "deep sleep!")
    except Exception as e:
        funcsys.ErrMessage(e)

    # Run functions
    try:
        if not funcconf.l_run_group6:
            funcconf.l_run_group6 = True
            A000_nwuiaca_group6.group6_functions()
    except Exception as e:
        funcsys.ErrMessage(e)


def group7():
    """
    Script to run dawn functions
    :return: Nothing
    """

    # Import function
    import A000_nwuiaca_group7

    # Send message
    try:
        if datetime.datetime.now().weekday() < 5:
            funcsms.send_telegram("Dear", "administrator", "early start!")
    except Exception as e:
        funcsys.ErrMessage(e)

    # Run functions
    try:
        if not funcconf.l_run_group7:
            funcconf.l_run_group7 = True
            A000_nwuiaca_group7.group7_functions()
    except Exception as e:
        funcsys.ErrMessage(e)


if __name__ == '__main__':
    main()
