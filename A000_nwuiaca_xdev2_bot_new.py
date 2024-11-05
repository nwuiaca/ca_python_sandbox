"""
NWU INTERNAL AUDIT ROBOT USING TELEGRAM MESSAGING
Press Ctrl-C on the command line or send a signal to the process to stop the bot.
Author: Albert B Janse van Rensburg (NWU:21162395)
Date created: 24 September 2023

Experimental code for telegram v20 togethet with A000_nwuiaca_xdev2_bot_commands.py

"""

# IMPORT PYTHON MODULES
from telegram import Update
from telegram.ext import filters, MessageHandler, ApplicationBuilder, CommandHandler, ContextTypes
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.executors.pool import ThreadPoolExecutor, ProcessPoolExecutor
from queue import Queue
from A000_nwuiaca_bot_commands import hi, echo, unknown
import logging
import warnings
import configparser
import datetime


def main():

    # IMPORT OWN MODULES
    from _my_modules import funcconf
    from _my_modules import funcmail
    from _my_modules import funclogg
    from _my_modules import funcsms

    # Read from the configuration file
    config = configparser.ConfigParser()
    config.read('.config.ini')

    # Setup logging
    logger = funclogg.setup_logger()
    logger.info('Starting the main robot scheduler.')

    # Ignore all warnings from pytz
    warnings.filterwarnings('ignore', category=UserWarning, module='pytz')

    try:

        # START THE BOT
        queue = Queue()
        if funcconf.l_tel_use_nwu:
            application = ApplicationBuilder().token(config.get('API', 'token_telegram_nwu')).build()
        else:
            application = ApplicationBuilder().token(config.get('API', 'token_telegram_albert')).build()

        # Command handler
        hi_handler = CommandHandler('hi', hi)
        gr_handler = CommandHandler('gr', gr, pass_args=True, pass_user_data=True, pass_chat_data=True)
        application.add_handler(hi_handler)
        # Echo handler
        echo_handler = MessageHandler(filters.TEXT & (~filters.COMMAND), echo)
        application.add_handler(echo_handler)
        # Other handlers
        unknown_handler = MessageHandler(filters.COMMAND, unknown)
        application.add_handler(unknown_handler)

        application.run_polling()

        # SEND OPENING MESSAGES
        # funcsms.send_telegram("Dear", "administrator", "the <b>server</b> is up and running, and you may talk to me!")

        executors = {
            'default': ThreadPoolExecutor(20),
            'processpool': ProcessPoolExecutor(5)
        }
        job_defaults = {
            'coalesce': False,
            'max_instances': 3
        }

        scheduler = BackgroundScheduler(executors=executors, job_defaults=job_defaults)

        scheduler.add_job(group1, 'cron', hour=9, minute=45)
        scheduler.add_job(group2, 'cron', hour=12, minute=55)
        scheduler.add_job(group3, 'cron', hour=16, minute=30)
        scheduler.add_job(group4, 'cron', hour=17, minute=0)
        scheduler.add_job(group5, 'cron', hour=22, minute=0)
        scheduler.add_job(group6, 'cron', hour=1, minute=0)
        scheduler.add_job(group7, 'cron', hour=5, minute=0)

        scheduler.start()

        # updater.start_polling()

        updater.idle()

    except Exception as e:
        logger.error(f"Error in main robot scheduler: {e}")

'''
async def hi(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await context.bot.send_message(chat_id=update.effective_chat.id, text="I'm a bot, please talk to me!")


async def echo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await context.bot.send_message(chat_id=update.effective_chat.id, text=update.message.text)


async def unknown(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await context.bot.send_message(chat_id=update.effective_chat.id, text="Sorry, I didn't understand that command.")
'''

def group1():

    # Run at start of workday
    import datetime
    # import A000_nwuiaca_group1
    from _my_modules import funcsms
    # from _my_modules import funclogg

    # Setup logger
    # logger = funclogg.setup_logger()
    logger.info('Executing group1 from the scheduler.')

    try:
        # Only on workdays
        if datetime.datetime.now().weekday() < '5':
            funcsms.send_telegram("Dear", "administrator", "your working day started, and I'm up and running!")
    except Exception as e:
        logger.error(f"Error in group1 message: {e}")


def group2():

    # Run at noon
    import datetime
    import A000_nwuiaca_group2
    from _my_modules import funcsms
    from _my_modules import funclogg

    # Setup logger
    logger = funclogg.setup_logger()
    logger.info('Executing group2 from the scheduler.')

    try:
        # Only on workdays
        if datetime.datetime.now().weekday() < 5:
            funcsms.send_telegram("Dear", "administrator", "how about going for a walk, while I'm keeping up!")
    except Exception as e:
        logger.error(f"Error in group2 message: {e}")

    try:
        # Only on workdays
        A000_nwuiaca_group2.group2_functions()
    except Exception as e:
        logger.error(f"Error in group2 functions: {e}")


def group3():

    # Run at end of working day
    import datetime
    import A000_nwuiaca_group3
    from _my_modules import funcsms
    from _my_modules import funclogg

    # Setup logger
    logger = funclogg.setup_logger()
    logger.info('Executing group3 from the scheduler.')

    # MESSAGE AT THE END OF THE WORKDAY
    try:
        # Only on workdays
        if datetime.datetime.now().weekday() < 5:
            funcsms.send_telegram("Dear", "administrator", "you've done your part today, while I'm prepping!")
    except Exception as e:
        logger.error(f"Error in group3 message: {e}")

    # RUN GROUP3 FUNCTIONS
    try:
        # Only on workdays
        A000_nwuiaca_group3.group3_functions()
    except Exception as e:
        logger.error(f"Error in group3 functions: {e}")


def group4():

    # Run at end of working day
    import datetime
    import A000_nwuiaca_group4
    from _my_modules import funcsms
    from _my_modules import funclogg

    # Setup logger
    logger = funclogg.setup_logger()
    logger.info('Executing group4 from the scheduler.')

    # RUN GROUP4 FUNCTIONS
    try:
        A000_nwuiaca_group4.group4_functions()
    except Exception as e:
        logger.error(f"Error in group4 functions: {e}")


def group5():

    # Run at end of working day
    import datetime
    import A000_nwuiaca_group5
    from _my_modules import funcsms
    from _my_modules import funclogg

    # Setup logger
    logger = funclogg.setup_logger()
    logger.info('Executing group5 from the scheduler.')

    # RUN GROUP5 FUNCTIONS
    try:
        A000_nwuiaca_group5.group5_functions()
    except Exception as e:
        logger.error(f"Error in group5 functions: {e}")


def group6():

    # Run at end of working day
    import datetime
    import A000_nwuiaca_group6
    from _my_modules import funcsms
    from _my_modules import funclogg

    # Setup logger
    logger = funclogg.setup_logger()
    logger.info('Executing group6 from the scheduler.')

    # RUN GROUP6 FUNCTIONS
    try:
        A000_nwuiaca_group6.group6_functions()
    except Exception as e:
        logger.error(f"Error in group6 functions: {e}")


def group7():

    # Run at end of working day
    import datetime
    import A000_nwuiaca_group7
    from _my_modules import funcsms
    from _my_modules import funclogg

    # Setup logger
    logger = funclogg.setup_logger()
    logger.info('Executing group7 from the scheduler.')

    # RUN GROUP7 FUNCTIONS
    try:
        A000_nwuiaca_group7.group7_functions()
    except Exception as e:
        logger.error(f"Error in group7 functions: {e}")


if __name__ == '__main__':
    main()
